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
    time::Instant,
};

use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::{
    global::{self, BlobType, ID},
    repository::repo::Repository,
    utils::indexset::IndexSet,
};

use super::packer::PackedBlobDescriptor;

/// Represents the location and size of a blob within a pack file.
#[derive(Debug, Clone)]
struct BlobLocation {
    /// The index into the `pack_ids` `IndexSet` for the pack containing this blob. See Index.
    pub pack_array_index: u32,
    /// The offset of the blob within its pack file.
    pub offset: u32,
    /// The length of the blob within its pack file.
    pub length: u32,
    /// The raw sized (uncompressed, unencrypted) of the blob
    pub raw_length: u32,
}

/// Represents the location and size of a blob within a pack file.
/// This struct contains the full pack ID. This is suited for iterating.
#[derive(Debug, Clone)]
pub struct BlobLocator {
    pub pack_id: ID,
    pub offset: u32,
    pub length: u32,
    pub raw_length: u32,
}

/// Manages the mapping of blob IDs to their locations within pack files.
/// An `Index` can be in a 'pending' state, indicating it's still being built.
#[derive(Debug, Clone)]
pub struct Index {
    /// blob ID -> BlobLocation map. This is the core lookup table.
    data_ids: HashMap<ID, BlobLocation>,
    tree_ids: HashMap<ID, BlobLocation>,

    /// The Pack IDs referenced in this index. Using an `IndexSet` allows us
    /// to store a small `usize` index in `BlobLocation` instead of the full `ID`,
    /// significantly reducing memory usage.
    pack_ids: IndexSet<ID>,

    /// If an index is pending, it is still receiving entries from packs and is not yet finalized.
    is_pending: bool,

    create_time: Instant,

    // The ID of this index, if it is finalized and serialized
    id: Option<ID>,
}

impl Default for Index {
    fn default() -> Self {
        Self::new()
    }
}

impl Index {
    pub fn new() -> Self {
        Self {
            data_ids: HashMap::new(),
            tree_ids: HashMap::new(),
            pack_ids: IndexSet::new(),
            is_pending: true,
            create_time: Instant::now(),
            id: None,
        }
    }

    /// Marks the index as finalized. A finalized index no longer accepts new entries
    /// and is typically ready for persistence or read-only operations.
    #[inline]
    pub fn finalize(&mut self) {
        self.is_pending = false;
    }

    /// Marks the index as pending.
    #[inline]
    pub fn set_pending(&mut self) {
        self.is_pending = true;
        self.id = None;
    }

    /// Returns the id of this index
    #[inline]
    pub fn id(&self) -> Option<ID> {
        self.id.clone()
    }

    /// Sets the index ID
    #[inline]
    pub fn set_id(&mut self, id: ID) {
        self.id = Some(id);
    }

    /// Returns `true` if the index is currently pending (still receiving entries).
    #[inline]
    pub fn is_pending(&self) -> bool {
        self.is_pending
    }

    /// Returns true if the index contains enough blobs to be considered full
    #[inline]
    pub fn is_full(&self) -> bool {
        self.num_blobs() >= global::defaults::BLOBS_PER_INDEX_FILE
            || self.create_time.elapsed() >= global::defaults::INDEX_FLUSH_TIMEOUT
    }

    /// Creates an `Index` from a serialized `IndexFile`.
    /// The created index is *not* pending, as it represents a complete, loaded file.
    pub fn from_index_file(index_file: IndexFile) -> Self {
        let mut index = Self::new();
        // An index loaded from a file is considered complete and not pending.
        index.is_pending = false;

        for pack in index_file.packs {
            let pack_index = index.pack_ids.insert(pack.id.clone());
            for blob in pack.blobs {
                let map = match blob.blob_type {
                    BlobType::Data => &mut index.data_ids,
                    BlobType::Tree => &mut index.tree_ids,
                    BlobType::Padding => continue,
                };

                map.insert(
                    blob.id,
                    BlobLocation {
                        pack_array_index: pack_index as u32,
                        offset: blob.offset,
                        length: blob.length,
                        raw_length: blob.raw_length,
                    },
                );
            }
        }
        index
    }

    /// Checks if the index contains the given object ID.
    #[inline]
    pub fn contains(&self, id: &ID) -> bool {
        self.data_ids.contains_key(id) || self.tree_ids.contains_key(id)
    }

    /// Retrieves the pack ID, offset, and length for a given blob ID, if it exists.
    /// Returns `None` if the blob ID is not found.
    pub fn get(&self, id: &ID) -> Option<(ID, BlobType, u32, u32, u32)> {
        self.data_ids
            .get(id)
            .map(|location| {
                let pack_id = self
                    .pack_ids
                    .get_value(location.pack_array_index as usize)
                    .expect("pack_index should always be valid for an existing blob");
                (
                    pack_id.clone(),
                    BlobType::Data,
                    location.offset,
                    location.length,
                    location.raw_length,
                )
            })
            .or_else(|| {
                self.tree_ids.get(id).map(|location| {
                    let pack_id = self
                        .pack_ids
                        .get_value(location.pack_array_index as usize)
                        .expect("pack_index should always be valid for an existing blob");
                    (
                        pack_id.clone(),
                        BlobType::Tree,
                        location.offset,
                        location.length,
                        location.raw_length,
                    )
                })
            })
    }

    /// Adds all blob descriptors from a specific pack to the index.
    /// This method is optimized for adding multiple blobs from the same pack,
    /// as it only needs to look up the pack ID once.
    pub fn add_pack(&mut self, pack_id: &ID, packed_blob_descriptors: &[PackedBlobDescriptor]) {
        let pack_index = self.pack_ids.insert(pack_id.clone());
        for blob in packed_blob_descriptors {
            let map = match blob.blob_type {
                BlobType::Data => &mut self.data_ids,
                BlobType::Tree => &mut self.tree_ids,
                BlobType::Padding => continue,
            };

            map.insert(
                blob.id.clone(),
                BlobLocation {
                    pack_array_index: pack_index as u32,
                    offset: blob.offset,
                    length: blob.length,
                    raw_length: blob.raw_length,
                },
            );
        }
    }

    /// Saves the index to the repository.
    /// Returns the total uncompressed and compressed sizes of the saved index files.
    pub fn finalize_and_save(&mut self, repo: &Repository) -> Result<(u64, u64)> {
        self.finalize();

        // Don't do anything if the index is empty.
        if self.data_ids.is_empty() && self.tree_ids.is_empty() {
            return Ok((0, 0));
        }

        let mut packs_with_blobs: HashMap<usize, Vec<IndexFileBlob>> = HashMap::new();
        for (blob_id, location) in &self.data_ids {
            let entry = packs_with_blobs
                .entry(location.pack_array_index as usize)
                .or_default();
            entry.push(IndexFileBlob {
                id: blob_id.clone(),
                blob_type: BlobType::Data,
                offset: location.offset,
                length: location.length,
                raw_length: location.raw_length,
            });
        }
        for (blob_id, location) in &self.tree_ids {
            let entry = packs_with_blobs
                .entry(location.pack_array_index as usize)
                .or_default();
            entry.push(IndexFileBlob {
                id: blob_id.clone(),
                blob_type: BlobType::Tree,
                offset: location.offset,
                length: location.length,
                raw_length: location.raw_length,
            });
        }

        let mut index_file = IndexFile::default();

        // Iterate through packs in the order they were inserted into `pack_ids`.
        // This ensures a consistent ordering of packs in the generated index files.
        for (pack_index, pack_id) in self.pack_ids.iter().enumerate() {
            if let Some(blobs) = packs_with_blobs.remove(&pack_index) {
                let index_pack_file = IndexFilePack {
                    id: pack_id.clone(),
                    blobs,
                };
                index_file.packs.push(index_pack_file);
            }
        }

        let (id, raw_size, encoded_size) = repo.save_file(
            global::FileType::Index,
            serde_json::to_string(&index_file)?.as_bytes(),
        )?;
        self.id = Some(id);

        Ok((raw_size, encoded_size))
    }

    #[inline]
    pub fn num_blobs(&self) -> usize {
        self.data_ids.len() + self.tree_ids.len()
    }

    #[inline]
    pub fn num_packs(&self) -> usize {
        self.pack_ids.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.num_blobs() == 0 && self.num_packs() == 0
    }

    pub fn iter_ids(&self) -> impl Iterator<Item = (&ID, BlobLocator)> {
        self.data_ids
            .iter()
            .chain(self.tree_ids.iter())
            .map(|(id, loc)| {
                (
                    id,
                    BlobLocator {
                        pack_id: self
                            .pack_ids
                            .get_value(loc.pack_array_index as usize)
                            .unwrap()
                            .clone(),
                        offset: loc.offset,
                        length: loc.length,
                        raw_length: loc.raw_length,
                    },
                )
            })
    }

    fn remove_pack(&mut self, target_pack_id: &ID) {
        let mut blobs_to_remove = Vec::new();

        for (blob_id, blob_location) in self.data_ids.iter().chain(self.tree_ids.iter()) {
            if let Some(pack_id) = self
                .pack_ids
                .get_value(blob_location.pack_array_index as usize)
            {
                if target_pack_id == pack_id {
                    blobs_to_remove.push(blob_id.clone());
                }
            }
        }

        // Clean up
        for blob_id in blobs_to_remove.into_iter() {
            self.data_ids.remove(&blob_id);
            self.tree_ids.remove(&blob_id);
        }
        self.pack_ids.remove(target_pack_id);
    }
}

/// Manages a collection of `Index` instances, providing a unified view
/// over all known blobs in the repository.
#[derive(Debug, Clone)]
pub struct MasterIndex {
    /// A list of individual indices, some of which might be pending.
    indices: Vec<Index>,

    /// Stores the IDs of blobs that are waiting to be serialized into a pack file.
    pending_blobs: HashSet<ID>,
}

impl Default for MasterIndex {
    fn default() -> Self {
        Self::new()
    }
}

impl MasterIndex {
    /// Creates a new, empty `MasterIndex`.
    pub fn new() -> Self {
        Self {
            indices: Vec::with_capacity(1),
            pending_blobs: HashSet::new(),
        }
    }

    /// Returns `true` if the object ID is known either in a finalized index
    /// or is currently a pending blob.
    pub fn contains(&self, id: &ID) -> bool {
        // Check finalized indices first
        self.indices
            .iter()
            .any(|idx| !idx.is_pending && idx.contains(id))
            || self.pending_blobs.contains(id) // Then check pending blobs
    }

    /// Retrieves an entry for a given blob ID by searching through finalized indices.
    /// Pending blobs (those not yet packed) cannot be retrieved via this method.
    pub fn get(&self, id: &ID) -> Option<(ID, BlobType, u32, u32, u32)> {
        self.indices
            .iter()
            .find_map(|idx| if !idx.is_pending { idx.get(id) } else { None })
    }

    /// Adds a fully constructed `Index` to the master index.
    /// This is typically used for adding loaded, finalized indices.
    pub fn add_index(&mut self, index: Index) {
        self.indices.push(index);
    }

    /// Adds a blob ID to the set of blobs that are waiting to be packed.
    /// Returns `true` if the ID did not exist in the set and was inserted; `false` otherwise.
    pub fn add_pending_blob(&mut self, id: ID) -> bool {
        self.pending_blobs.insert(id)
    }

    /// Processes a newly created pack of blobs. It removes these blobs from the
    /// `pending_blobs` set and adds them to all currently pending `Index` instances.
    ///
    /// It's assumed that there is at least one pending index that should receive these blobs,
    /// or that a new one will be created as part of the overall backup process if needed.
    pub fn add_pack(
        &mut self,
        repo: &Repository,
        pack_id: &ID,
        packed_blob_descriptors: Vec<PackedBlobDescriptor>, // Take ownership as it's consumed
    ) -> Result<(u64, u64)> {
        // Remove processed blobs from the pending set
        for blob in &packed_blob_descriptors {
            self.pending_blobs.remove(&blob.id);
        }

        // Add the pack's blobs to all currently pending indices.
        for idx in &mut self.indices {
            if idx.is_pending() {
                idx.add_pack(pack_id, &packed_blob_descriptors);

                return match idx.is_full() {
                    true => idx.finalize_and_save(repo),
                    false => Ok((0, 0)), // Nothing was added to the repository
                };
            }
        }

        // There were no pending indices. Create a new empty pending index and add the pack.
        let mut new_pending_index = Index::new();
        new_pending_index.add_pack(pack_id, &packed_blob_descriptors);
        self.indices.push(new_pending_index);

        Ok((0, 0)) // Nothing was added to the repository
    }

    /// Saves all pending indices managed by the `MasterIndex` to the repository.
    /// Finalized indices are not saved again.
    ///
    /// Returns the total raw and encoded sizes of the saved index files.
    pub fn save(&mut self, repo: &Repository) -> Result<(u64, u64)> {
        let mut uncompressed_size: u64 = 0;
        let mut compressed_size: u64 = 0;

        for idx in &mut self.indices {
            if idx.is_pending() {
                let (uncompressed, compressed) = idx.finalize_and_save(repo)?;
                uncompressed_size += uncompressed;
                compressed_size += compressed;
            }
        }

        Ok((uncompressed_size, compressed_size))
    }

    pub fn iter_ids(&self) -> impl Iterator<Item = (&ID, BlobLocator)> {
        // We start with an "empty" chain or the first iterator
        let mut chained_iterator: Box<dyn Iterator<Item = (&ID, BlobLocator)>> =
            Box::new(std::iter::empty());

        for index in &self.indices {
            // Chain each index's iterator to the accumulating chained_iterator
            chained_iterator = Box::new(chained_iterator.chain(index.iter_ids()));
        }
        chained_iterator
    }

    /// Returns the IDs of all finalized (serialized) indices
    pub fn ids(&self) -> BTreeSet<ID> {
        let mut ids = BTreeSet::new();
        for idx in &self.indices {
            if idx.is_pending() {
                continue;
            }
            if let Some(id) = idx.id() {
                ids.insert(id);
            }
        }
        ids
    }

    /// Removes obsolete packs from all indices
    pub fn cleanup(&mut self, obsolete_packs: Option<&BTreeSet<ID>>) {
        if let Some(packs_to_remove) = obsolete_packs {
            for idx in &mut self.indices {
                idx.set_pending();
                for pack_id in packs_to_remove {
                    idx.remove_pack(pack_id);
                }
            }
        }
        self.merge_index();
    }

    /// Merges all current indices into a new collection of full indices.
    /// This function can be used to defragment the current master index into
    /// a small set of full index files then it becomes fragmented into many
    /// small files.
    fn merge_index(&mut self) {
        let mut new_indices = Vec::new();
        let mut pack_ids = BTreeSet::new();

        let mut current_index = Index::new();
        for idx in &mut self.indices {
            for pack_id in idx.pack_ids.iter() {
                if pack_ids.contains(pack_id) {
                    continue;
                }

                if current_index.is_full() {
                    // Important: The index is not saved yet, so it must not be finalized
                    new_indices.push(current_index);
                    current_index = Index::new();
                }

                pack_ids.insert(pack_id);
                let mut packed_blob_descriptors = Vec::new();

                let mut process_blobs =
                    |blob_map: &HashMap<ID, BlobLocation>, blob_type: BlobType| {
                        for (blob_id, _) in blob_map.iter() {
                            let (blob_pack_id, _, offset, length, raw_length) =
                                idx.get(blob_id).unwrap();
                            if blob_pack_id == *pack_id {
                                let blob_descriptor = PackedBlobDescriptor {
                                    id: blob_id.clone(),
                                    blob_type: blob_type.clone(),
                                    offset,
                                    length,
                                    raw_length,
                                };
                                packed_blob_descriptors.push(blob_descriptor);
                            }
                        }
                    };

                process_blobs(&idx.data_ids, BlobType::Data);
                process_blobs(&idx.tree_ids, BlobType::Tree);
                current_index.add_pack(pack_id, &packed_blob_descriptors);
            }
        }

        if !current_index.is_empty() {
            new_indices.push(current_index);
        }

        self.indices.clear();
        self.indices = new_indices;
    }
}

/// Represents the on-disk format for an index file.
/// This structure is used for serialization and deserialization of index data.
#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct IndexFile {
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub packs: Vec<IndexFilePack>,
}

/// Represents a pack's entry within an `IndexFile`.
#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct IndexFilePack {
    pub id: ID,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub blobs: Vec<IndexFileBlob>,
}

/// Represents a blob's entry within an `IndexFilePack`.
#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct IndexFileBlob {
    pub id: ID,
    #[serde(rename = "type")]
    pub blob_type: BlobType,
    pub offset: u32,
    pub length: u32,
    pub raw_length: u32,
}
