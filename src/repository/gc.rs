// mapache is an incremental backup tool
// Copyright (C) 2025  Javier Lancha Vázquez <javier.lancha@gmail.com>
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

use std::{
    collections::{BTreeSet, HashMap, HashSet},
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use anyhow::{Result, bail};
use colored::Colorize;
use indicatif::{ProgressBar, ProgressStyle};
use rayon::iter::{IntoParallelIterator, IntoParallelRefIterator, ParallelIterator};

use crate::{
    global::{
        self, FileType, ID, SaveID,
        defaults::{DEFAULT_MIN_PACK_SIZE_FACTOR, DEFAULT_PACK_SIZE},
    },
    repository::{repo::Repository, snapshot::SnapshotStreamer, streamers::SerializedNodeStreamer},
    ui::{self, PROGRESS_REFRESH_RATE_HZ, SPINNER_TICK_CHARS, default_bar_draw_target},
};

/// The cleanup plan. This struct contains lists of items that are valid, unused or need some work.
/// A plan can be executed to complete the garbage collection process. Once executed, the plan
/// object is consumed and cannot be used again. This is an intended safety measure.
pub struct Plan {
    pub repo: Arc<Repository>,
    pub total_packs: usize, // Total number of blobs in the repository
    pub referenced_blobs: HashSet<ID>, // Blobs referenced by existing snapshots
    pub referenced_packs: HashSet<ID>, // Packs referenced by the referenced blobs
    pub obsolete_packs: BTreeSet<ID>, // Packs containing non-referenced blobs
    pub small_packs: BTreeSet<ID>, // Small packs marked to be repacked (to merge)
    pub tolerated_packs: BTreeSet<ID>, // Packs containing garbage, but keep due to tolerance
    pub unused_packs: BTreeSet<ID>, // Packs not referenced by any snapshot or index
    pub index_ids: BTreeSet<ID>, // Current index IDs
}

/// Scan the repository and make a plan of what needs to be cleaned.
pub fn scan(repo: Arc<Repository>, tolerance: f32) -> Result<Plan> {
    let (referenced_blobs, referenced_packs) = get_referenced_blobs_and_packs(repo.clone())?;

    let mut keep_packs: BTreeSet<ID> = repo.list_objects()?;
    let mut unused_packs = keep_packs.clone();

    keep_packs.retain(|id| referenced_packs.contains(id));
    unused_packs.retain(|id| !referenced_packs.contains(id));

    let mut plan = Plan {
        repo: repo.clone(),
        total_packs: keep_packs.len(),
        referenced_blobs,
        referenced_packs,
        obsolete_packs: BTreeSet::new(),
        tolerated_packs: BTreeSet::new(),
        unused_packs,
        index_ids: repo.index().read().ids(),
        small_packs: BTreeSet::new(),
    };

    // Count garbage bytes in each pack
    let mut kept_pack_size: HashMap<ID, u64> = HashMap::new();
    let mut pack_garbage: HashMap<ID, u64> = HashMap::new();

    // Find obsolete packs and blobs in index
    let spinner = ProgressBar::new_spinner();
    spinner.set_draw_target(default_bar_draw_target());
    spinner.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner:.cyan} Finding obsolete blobs: {pos}")
            .unwrap()
            .tick_chars(SPINNER_TICK_CHARS),
    );
    spinner.enable_steady_tick(Duration::from_millis(
        (1000.0f32 / PROGRESS_REFRESH_RATE_HZ as f32) as u64,
    ));
    for (id, locator) in repo.index().read().iter_ids() {
        kept_pack_size
            .entry(locator.pack_id.clone())
            .and_modify(|size| {
                *size += locator.length as u64;
            })
            .or_default();

        if !plan.referenced_blobs.contains(id) {
            pack_garbage
                .entry(locator.pack_id)
                .and_modify(|size| *size += locator.length as u64)
                .or_insert(locator.length as u64);
            spinner.inc(1);
        }
    }

    // Find small packs to repack
    for (pack_id, size) in kept_pack_size {
        if (size as f32 / DEFAULT_PACK_SIZE as f32) < DEFAULT_MIN_PACK_SIZE_FACTOR {
            plan.small_packs.insert(pack_id);
        }
    }

    spinner.finish_and_clear();
    ui::cli::log!(
        "Found {} obsolete blobs in {} packs",
        spinner.position(),
        pack_garbage.len()
    );

    // Check garbage levels
    let spinner = ProgressBar::new_spinner();
    spinner.set_draw_target(default_bar_draw_target());
    spinner.set_length(pack_garbage.len() as u64);
    spinner.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner:.cyan} Checking garbage levels ({pos} / {len} packs)")
            .unwrap()
            .tick_chars(SPINNER_TICK_CHARS),
    );
    spinner.enable_steady_tick(Duration::from_millis(
        (1000.0f32 / PROGRESS_REFRESH_RATE_HZ as f32) as u64,
    ));
    for (pack_id, garbage_bytes) in pack_garbage.into_iter() {
        if (garbage_bytes as f32 / DEFAULT_PACK_SIZE as f32) > tolerance {
            keep_packs.remove(&pack_id);
            plan.obsolete_packs.insert(pack_id);
        } else {
            plan.tolerated_packs.insert(pack_id);
        }
        spinner.inc(1);
    }
    spinner.finish_and_clear();

    Ok(plan)
}

impl Plan {
    /// Execute the plan. Calling this method consumes the plan so it cannot be
    /// executed more than once.
    pub fn execute(mut self) -> Result<i64> {
        let mut deleted_size = 0;
        let mut added_size = 0;

        // Append small packs to the obsolete pack list. Do this only if there are
        // at least 2 packs that can be merged.
        // Small packs will not always be merged with other packs. If all other packs
        // are full merging will result in a new pack with the leftovers. If two small
        // packs contain different types of blobs (data and tree), they will be repacked
        // in separate pack files.
        if self.small_packs.len() > 1 {
            self.obsolete_packs.append(&mut self.small_packs);
        }

        deleted_size += self.delete_unused_packs()?;

        // No need to repack and rewrite the indices if there are no obsolete packs
        if !self.obsolete_packs.is_empty() {
            self.repo
                .init_pack_saver(global::defaults::DEFAULT_WRITE_CONCURRENCY);

            added_size += self.repack()?;
            let (_, encoded) = self.repo.flush()?;
            self.repo.finalize_pack_saver();

            added_size += encoded;

            deleted_size += self.delete_old_indices()?;
            deleted_size += self.delete_obsolete_packs()?;
        }

        Ok((deleted_size - added_size) as i64)
    }

    /// Delete packs that contain no referenced blobs.
    fn delete_unused_packs(&self) -> Result<u64> {
        let unused_pack_delete_bar = ProgressBar::with_draw_target(
            Some(self.unused_packs.len() as u64),
            default_bar_draw_target(),
        )
        .with_style(
            ProgressStyle::default_bar()
                .template("[{bar:25.cyan/white}] Deleting unused packs: {pos}/{len}")
                .unwrap()
                .progress_chars("=> "),
        );

        let mut deleted_size = 0;
        for id in &self.unused_packs {
            deleted_size += self.repo.delete_file(FileType::Pack, id)?;
            unused_pack_delete_bar.inc(1);
        }
        unused_pack_delete_bar.finish_and_clear();
        ui::cli::log!("Deleted {} unused packs", unused_pack_delete_bar.position());

        Ok(deleted_size)
    }

    /// Repack referenced blobs from obsolete packs to new packs
    fn repack(&mut self) -> Result<u64> {
        // Collect information about the blobs to repack. Since we will rewrite the index, we will
        // lose this information.
        let repack_bar = ProgressBar::with_draw_target(
            Some(self.referenced_blobs.len() as u64),
            default_bar_draw_target(),
        )
        .with_style(
            ProgressStyle::default_bar()
                .template("[{bar:25.cyan/white}] Finding blobs to repack: {pos}/{len}")
                .unwrap()
                .progress_chars("=> "),
        );
        let mut repack_blob_info = HashMap::new();
        for referenced_blob_id in &self.referenced_blobs {
            if let Some((pack_id, blob_type, offset, length, raw_length)) =
                self.repo.index().read().get(referenced_blob_id)
            {
                if self.obsolete_packs.contains(&pack_id) {
                    repack_blob_info.insert(
                        referenced_blob_id,
                        (pack_id, blob_type, offset, raw_length, length),
                    );
                }
            }
            repack_bar.inc(1);
        }
        repack_bar.finish_and_clear();

        // Rewrite index (remove obsolete packs) and repack.
        // We read the blobs we need to repack and pass them to the repository.
        // Since they are no longer in the index, this is like doing a backup of those blobs,
        // without creating the snapshot.
        self.repo
            .index()
            .write()
            .cleanup(Some(&self.obsolete_packs));

        let repack_bar = ProgressBar::with_draw_target(
            Some(repack_blob_info.len() as u64),
            default_bar_draw_target(),
        )
        .with_style(
            ProgressStyle::default_bar()
                .template("[{bar:25.cyan/white}] Repacking blobs: {pos}/{len}")
                .unwrap()
                .progress_chars("=> "),
        );

        let added_size = AtomicU64::new(0);

        const REPACK_CONCURRENCY: usize = 4;
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(REPACK_CONCURRENCY)
            .build()
            .expect("Failed to build thread pool");
        let process_result: Result<()> = pool.install(|| {
            repack_blob_info.into_par_iter().try_for_each(
                |(blob_id, (pack_id, blob_type, offset, _raw_length, length))| {
                    // Reencoding the blob to repack it might seem unnecessary, and it is,
                    // but this can serve as a validation mechanism and I also don't want
                    // to leave any option to code paths that lead to any unencrypted repacked blob.
                    let data = self.repo.read_from_file_and_decode(
                        FileType::Pack,
                        &pack_id,
                        offset as u64,
                        length as u64,
                    )?;
                    let (_id, (_raw_length, _encoded_length), (_raw_meta, encoded_meta)) = self
                        .repo
                        .encode_and_save_blob(blob_type, data, SaveID::WithID(blob_id.clone()))?;
                    added_size.fetch_add(length as u64 + encoded_meta, Ordering::AcqRel);

                    repack_bar.inc(1);
                    Ok(())
                },
            )
        });
        repack_bar.finish_and_clear();
        ui::cli::log!("Repacked {} blobs", repack_bar.position());

        if let Err(e) = process_result {
            bail!("An error occurred during repacking: {}", e);
        }

        Ok(added_size.load(Ordering::Relaxed))
    }

    /// Delete old index files
    /// This operation must be performed after the master index has been cleaned up
    /// and all referenced packs have been repacked.
    fn delete_old_indices(&mut self) -> Result<u64> {
        // Delete obsolete index files
        // Make sure that the new index files don't overlap the files to delete.
        // This can happen if an index did not change while repacking.
        let new_index_ids = self.repo.index().read().ids();
        self.index_ids.retain(|id| !new_index_ids.contains(id));

        let index_delete_bar = ProgressBar::with_draw_target(
            Some(self.index_ids.len() as u64),
            default_bar_draw_target(),
        )
        .with_style(
            ProgressStyle::default_bar()
                .template("[{bar:25.cyan/white}] Deleting old index files: {pos}/{len}")
                .unwrap()
                .progress_chars("=> "),
        );

        let deleted_size = AtomicU64::new(0);
        self.index_ids.par_iter().for_each(|id| {
            let size_res = self.repo.delete_file(FileType::Index, id);
            deleted_size.fetch_add(size_res.unwrap_or(0), Ordering::AcqRel);
            index_delete_bar.inc(1);
        });
        index_delete_bar.finish_and_clear();
        ui::cli::log!(
            "Deleted {} obsolete index files",
            index_delete_bar.position()
        );

        Ok(deleted_size.load(Ordering::Relaxed))
    }

    /// Delete all pack files marked as obsolete.
    fn delete_obsolete_packs(&self) -> Result<u64> {
        // Delete obsolete pack files
        let obsolete_pack_delete_bar = ProgressBar::with_draw_target(
            Some(self.obsolete_packs.len() as u64),
            default_bar_draw_target(),
        )
        .with_style(
            ProgressStyle::default_bar()
                .template("[{bar:25.cyan/white}] Deleting obsolete pack files: {pos}/{len}")
                .unwrap()
                .progress_chars("=> "),
        );

        let deleted_size = AtomicU64::new(0);
        self.obsolete_packs.par_iter().for_each(|id| {
            let size_res = self.repo.delete_file(FileType::Pack, id);
            deleted_size.fetch_add(size_res.unwrap_or(0), Ordering::AcqRel);
            obsolete_pack_delete_bar.inc(1);
        });
        obsolete_pack_delete_bar.finish_and_clear();
        ui::cli::log!(
            "Deleted {} obsolete packs",
            obsolete_pack_delete_bar.position()
        );

        Ok(deleted_size.load(Ordering::Relaxed))
    }
}

/// Returns all blobs and packs referenced by all existing snapshots in the repository.
fn get_referenced_blobs_and_packs(repo: Arc<Repository>) -> Result<(HashSet<ID>, HashSet<ID>)> {
    let mut referenced_blobs = HashSet::new();
    let mut referenced_packs = HashSet::new();
    let index = repo.index();

    let snapshot_streamer = SnapshotStreamer::new(repo.clone())?;

    let spinner = ProgressBar::new_spinner();
    spinner.set_draw_target(default_bar_draw_target());
    spinner.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner:.cyan} Searching referenced blobs: {pos}")
            .unwrap()
            .tick_chars(SPINNER_TICK_CHARS),
    );
    spinner.enable_steady_tick(Duration::from_millis(
        (1000.0_f32 / PROGRESS_REFRESH_RATE_HZ as f32) as u64,
    ));

    for (_snapshot_id, snapshot) in snapshot_streamer {
        let tree_id = snapshot.tree.clone();

        // Tree blob of the snapshot
        if referenced_blobs.insert(tree_id.clone()) {
            spinner.set_position(referenced_blobs.len() as u64);
        }

        match index.read().get(&tree_id) {
            Some((pack_id, _, _, _, _)) => {
                referenced_packs.insert(pack_id);
            }
            None => {
                ui::cli::warning!(
                    "Snapshot tree {} is referenced but not found in index",
                    tree_id
                );
            }
        }

        // Stream all nodes in the snapshot
        let node_streamer =
            SerializedNodeStreamer::new(repo.clone(), Some(tree_id), PathBuf::new(), None, None)?;

        let mut missing_tree_blobs = 0;
        let mut missing_data_blobs = 0;

        for node_res in node_streamer {
            match node_res {
                Ok((_path, stream_node)) => {
                    let node = &stream_node.node;

                    // Tree blobs
                    if let Some(tree) = &node.tree {
                        if referenced_blobs.insert(tree.clone()) {
                            spinner.set_position(referenced_blobs.len() as u64);
                        }

                        match index.read().get(tree) {
                            Some((pack_id, _, _, _, _)) => {
                                referenced_packs.insert(pack_id);
                            }
                            None => {
                                missing_tree_blobs += 1;
                            }
                        }
                    }

                    // Data blobs
                    if let Some(blobs) = &node.blobs {
                        for blob_id in blobs {
                            if referenced_blobs.insert(blob_id.clone()) {
                                spinner.set_position(referenced_blobs.len() as u64);
                            }

                            match index.read().get(blob_id) {
                                Some((pack_id, _, _, _, _)) => {
                                    referenced_packs.insert(pack_id);
                                }
                                None => {
                                    missing_data_blobs += 1;
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    ui::cli::warning!("Error parsing node: {e}");
                }
            }
        }

        if missing_tree_blobs > 0 {
            ui::cli::warning!(
                "{} tree blobs referenced in snapshot are missing in the index",
                missing_tree_blobs.to_string().bold()
            );
        }

        if missing_data_blobs > 0 {
            ui::cli::warning!(
                "{} data blobs referenced in snapshot are missing in the index",
                missing_data_blobs.to_string().bold()
            );
        }
    }

    spinner.finish_and_clear();
    ui::cli::log!(
        "Found {} referenced blobs and {} packs",
        referenced_blobs.len(),
        referenced_packs.len()
    );

    Ok((referenced_blobs, referenced_packs))
}
