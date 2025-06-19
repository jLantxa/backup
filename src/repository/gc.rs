// [backup] is an incremental backup tool
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
    collections::{HashMap, HashSet},
    path::PathBuf,
    sync::Arc,
};

use anyhow::Result;

use crate::{
    global::{ID, defaults::MAX_PACK_SIZE},
    repository::{
        RepositoryBackend, snapshot::SnapshotStreamer, streamers::SerializedNodeStreamer,
    },
    ui,
};

pub struct Plan {
    pub repo: Arc<dyn RepositoryBackend>,
    pub referenced_blobs: HashSet<ID>, // Blobs to keep
    pub obsolete_packs: HashSet<ID>,   // Packs to repack
}

pub fn plan(repo: Arc<dyn RepositoryBackend>, tolerance: f32) -> Result<Plan> {
    let mut plan = Plan {
        repo: repo.clone(),
        referenced_blobs: get_referenced_blobs(repo.clone())?,
        obsolete_packs: HashSet::new(),
    };

    let mut pack_garbage: HashMap<ID, u64> = HashMap::new();

    // Find obsolete packs and blobs in index
    let master_index = repo.index();
    let index_guard = master_index.lock().unwrap();
    for (id, locator) in index_guard.iter_ids() {
        if !plan.referenced_blobs.contains(id) {
            pack_garbage
                .entry(locator.pack_id)
                .and_modify(|size| *size += locator.length)
                .or_insert(locator.length);
        }
    }

    for (pack_id, garbage_bytes) in pack_garbage.into_iter() {
        if (garbage_bytes as f32 / MAX_PACK_SIZE as f32) > tolerance {
            plan.obsolete_packs.insert(pack_id);
        }
    }

    Ok(plan)
}

impl Plan {
    pub fn execute(&self) -> Result<()> {
        todo!()
    }
}

fn get_referenced_blobs(repo: Arc<dyn RepositoryBackend>) -> Result<HashSet<ID>> {
    let mut referenced_blobs = HashSet::new();

    let snapshot_streamer = SnapshotStreamer::new(repo.clone())?;
    for (_snapshot_id, snapshot) in snapshot_streamer {
        let tree_id = snapshot.tree;
        referenced_blobs.insert(tree_id.clone());

        let node_streamer =
            SerializedNodeStreamer::new(repo.clone(), Some(tree_id), PathBuf::new(), None, None)?;

        for node_res in node_streamer {
            match node_res {
                Ok((_path, stream_node)) => {
                    if let Some(tree) = stream_node.node.tree {
                        referenced_blobs.insert(tree);
                    } else if let Some(blobs) = stream_node.node.blobs {
                        for blob_id in blobs {
                            referenced_blobs.insert(blob_id);
                        }
                    }
                }
                Err(e) => ui::cli::warning!("Error parsing node: {}", e.to_string()),
            }
        }
    }

    Ok(referenced_blobs)
}
