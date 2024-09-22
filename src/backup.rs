/*
 * Copyright (C) 2024 Javier Lancha Vázquez
 *
 * This program is free software: you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation, either version 3 of the License, or (at your option)
 * any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of  MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 * more details.
 *
 * You should have received a copy of the GNU General Public License along with
 * this program.  If not, see <http://www.gnu.org/licenses/>.
 */

use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};

use crate::{io::SecureStorage, storage};

/// Type of snapshot
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum SnapshotKind {
    Full,
    Delta,
}

/// Retention policy for snapshots in the repository.
#[derive(Debug, Serialize, Deserialize, Default)]
pub enum RetentionPolicy {
    #[default]
    KeepAll,
}

/// Compression level for objects in the repository.
#[derive(Debug, Serialize, Deserialize)]
pub enum CompressionLevel {
    LOW,
    MID,
    HIGH,
    MAX,
}

impl Default for CompressionLevel {
    fn default() -> Self {
        CompressionLevel::LOW
    }
}

impl CompressionLevel {
    /// Convert CompressionLevel to integer.
    fn to_i32(&self) -> i32 {
        match self {
            CompressionLevel::LOW => 3,
            CompressionLevel::MID => 9,
            CompressionLevel::HIGH => 15,
            CompressionLevel::MAX => 22,
        }
    }
}

const DATA_DIR: &str = "data";
const SNAPSHOTS_DIR: &str = "snapshots";
const REFS_PATH: &str = "refs";
const CONFIG_PATH: &str = "config";

struct Paths {
    pub config: PathBuf,
    pub refs: PathBuf,
    pub snapshots: PathBuf,
    pub data: PathBuf,
}

/// File metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileMetadata {
    pub path: String,
    pub delta: Delta,
    pub file_size: usize,
    pub modify_date: String,
}

/// Snapshot metadata.
#[derive(Debug, Serialize, Deserialize)]
pub struct Snapshot {
    pub id: String,
    pub kind: SnapshotKind,
    pub timestamp: String,
    pub previous_snapshot_id: Option<String>,
    pub files: HashMap<String, FileMetadata>,
}

/// Quick reference of all snapshots in the repo.
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct SnapshotsRef {
    pub snapshots: Vec<(String, SnapshotKind, String)>, // (id, kind, timestamp)
}

/// Repository settings.
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Settings {
    pub retention_policy: RetentionPolicy,
    pub compression_level: CompressionLevel,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Delta {
    Chunks(Vec<String>),
    Deleted,
}

#[derive(Debug)]
pub enum ErrorKind {
    RepoInitError,
    MetadataError,
    StorageError,
}

#[derive(Debug)]
pub struct Error {
    pub kind: ErrorKind,
    pub error: String,
}

impl Error {
    pub fn new(kind: ErrorKind, error: &str) -> Self {
        Self {
            kind,
            error: String::from(error),
        }
    }
}

/// Repo handles backup repository operations.
pub struct Repo {
    repo_path: PathBuf,
    paths: Paths,
    secure_storage: SecureStorage,
    config: Settings,
    refs: SnapshotsRef,
}

impl Repo {
    pub fn new(path: &Path, password: &str) -> Result<Self, Error> {
        let secure_storage = SecureStorage {};
        let repo_path = path.to_path_buf();
        let paths = Paths {
            config: repo_path.join(CONFIG_PATH),
            refs: repo_path.join(REFS_PATH),
            snapshots: repo_path.join(SNAPSHOTS_DIR),
            data: repo_path.join(DATA_DIR),
        };

        Self::create_dirs(&repo_path);

        let repo = Self {
            repo_path: repo_path,
            paths,
            secure_storage,
            config: Default::default(),
            refs: Default::default(),
        };

        repo.persist_meta();

        Ok(repo)
    }

    pub fn from_existing(path: &Path, password: &str) -> Result<Self, Error> {
        let secure_storage = SecureStorage {};
        let repo_path = path.to_path_buf();
        let paths = Paths {
            config: repo_path.join(CONFIG_PATH),
            refs: repo_path.join(REFS_PATH),
            snapshots: repo_path.join(SNAPSHOTS_DIR),
            data: repo_path.join(DATA_DIR),
        };

        let config: Settings = secure_storage.load_json(&paths.config).unwrap();
        let refs: SnapshotsRef = secure_storage.load_json(&paths.refs).unwrap();

        Ok(Self {
            repo_path: repo_path,
            paths,
            secure_storage,
            config: config,
            refs: refs,
        })
    }

    /// Create the repository chunks directory.
    fn create_dirs(path: &Path) -> std::io::Result<()> {
        let data_path = path.join(DATA_DIR);
        std::fs::create_dir_all(&data_path);

        let snapshots_path = path.join(SNAPSHOTS_DIR);
        std::fs::create_dir_all(&snapshots_path);

        (0x00..=0xff)
            .map(|i| std::fs::create_dir_all(data_path.join(format!("{:02x}", i))))
            .collect::<Result<_, _>>()
    }

    /// Initialize all metadata.
    fn init_meta(&mut self) -> std::io::Result<()> {
        self.config = self.secure_storage.load_json(&self.paths.config)?;
        self.refs = self.secure_storage.load_json(&self.paths.snapshots)?;
        Ok(())
    }

    /// Save metadata.
    fn persist_meta(&self) -> std::io::Result<()> {
        let compression_level = self.config.compression_level.to_i32();
        self.secure_storage
            .save_json(&self.paths.config, &self.config, compression_level)?;
        self.secure_storage
            .save_json(&self.paths.refs, &self.refs, compression_level)?;
        Ok(())
    }

    /// Set the repo retention policy.
    pub fn retention_policy(&mut self, retention_policy: RetentionPolicy) {
        self.config.retention_policy = retention_policy;
    }

    /// Set the repo compression level.
    pub fn compression_level(&mut self, level: CompressionLevel) {
        self.config.compression_level = level;
    }

    fn get_all_files_recursive(directory: &Path) -> Vec<PathBuf> {
        let mut files = Vec::new();
        if let Ok(entries) = std::fs::read_dir(directory) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_file() {
                    files.push(path);
                } else if path.is_dir() {
                    files.extend(Self::get_all_files_recursive(&path));
                }
            }
        }
        files
    }

    /// Backup a directory to the repo.
    pub fn backup(&mut self, src_dir: &Path) -> Result<(), Error> {
        let snapshot_kind = self.determine_snapshot_kind();
        let source_files = Self::get_all_files_recursive(src_dir);
        let last_snapshot_files = self.get_last_snapshot_files()?;
        let compression_level = self.config.compression_level.to_i32();

        let snapshot_files =
            self.process_source_files(source_files, &last_snapshot_files, compression_level)?;

        let snapshot_id = self.create_snapshot(snapshot_kind, snapshot_files)?;
        Ok(())
    }

    fn determine_snapshot_kind(&self) -> SnapshotKind {
        if self.refs.snapshots.is_empty() {
            SnapshotKind::Full
        } else {
            let num_deltas = self
                .refs
                .snapshots
                .iter()
                .rev()
                .take_while(|(_, kind, _)| *kind == SnapshotKind::Delta)
                .count();
            if num_deltas < 10 {
                SnapshotKind::Delta
            } else {
                SnapshotKind::Full
            }
        }
    }

    fn get_last_snapshot_files(&self) -> Result<HashMap<String, FileMetadata>, Error> {
        match self.refs.snapshots.last() {
            None => Ok(HashMap::new()),
            Some((id, _, _)) => self.calculate_status_at_snapshot(id),
        }
    }

    fn process_source_files(
        &self,
        source_files: Vec<PathBuf>,
        last_snapshot_files: &HashMap<String, FileMetadata>,
        compression_level: i32,
    ) -> Result<HashMap<String, FileMetadata>, Error> {
        let mut snapshot_files = HashMap::new();

        for source_file in source_files {
            let storage_result = self.store_and_compress_file(&source_file, compression_level)?;
            let has_changed =
                self.file_has_changed(&source_file, &storage_result, last_snapshot_files);

            if has_changed {
                let file_metadata = FileMetadata {
                    path: source_file.to_string_lossy().to_string(),
                    delta: Delta::Chunks(storage_result.chunk_hashes),
                    file_size: 0,                      // TODO: Update with real size
                    modify_date: String::from("todo"), // TODO: Update with real modify date
                };
                snapshot_files.insert(source_file.to_string_lossy().to_string(), file_metadata);
            }
        }

        Ok(snapshot_files)
    }

    fn store_and_compress_file(
        &self,
        source_file: &PathBuf,
        compression_level: i32,
    ) -> Result<storage::StorageResult, Error> {
        storage::store_file(
            source_file,
            &self.paths.data,
            &self.secure_storage,
            compression_level,
        )
        .map_err(|_| Error::new(ErrorKind::StorageError, "Could not store file"))
    }

    fn file_has_changed(
        &self,
        source_file: &PathBuf,
        storage_result: &storage::StorageResult,
        last_snapshot_files: &HashMap<String, FileMetadata>,
    ) -> bool {
        let file_meta = last_snapshot_files.get(source_file.to_string_lossy().as_ref());

        match file_meta {
            None => true,
            Some(meta) => match &meta.delta {
                Delta::Deleted => true,
                Delta::Chunks(chunks) => *chunks != storage_result.chunk_hashes,
            },
        }
    }

    fn create_snapshot(
        &mut self,
        snapshot_kind: SnapshotKind,
        snapshot_files: HashMap<String, FileMetadata>,
    ) -> Result<String, Error> {
        let snapshot_id = self.refs.snapshots.len().to_string();
        let previous_snapshot_id = self.refs.snapshots.last().map(|(id, _, _)| id.clone());

        let snapshot = Snapshot {
            id: snapshot_id.clone(),
            kind: snapshot_kind.clone(),
            timestamp: "timestamp".to_string(), // TODO: Update with real timestamp
            previous_snapshot_id,
            files: snapshot_files,
        };

        let snapshot_meta_path = self.paths.snapshots.join(&snapshot_id);
        self.secure_storage
            .save_json(
                &snapshot_meta_path,
                &snapshot,
                self.config.compression_level.to_i32(),
            )
            .map_err(|_| Error::new(ErrorKind::StorageError, "Could not store snapshot metadata"));

        self.refs
            .snapshots
            .push((snapshot_id.clone(), snapshot_kind, "timestamp".to_string()));
        self.persist_meta();

        Ok(snapshot_id)
    }

    /// Restore a snapshot.
    pub fn restore_snapshot(&self, snapshot_id: &str, dst_path: &Path) -> Result<(), Error> {
        todo!()
    }

    /// Cleanup the repository.
    pub fn cleanup(&mut self) -> Result<(), Error> {
        let mut referenced_chunks = HashSet::new();

        for (snapshot_id, _, _) in &self.refs.snapshots {
            let snapshot_path = &self.paths.snapshots.join(snapshot_id);
            let snapshot: Snapshot = self
                .secure_storage
                .load_json(&snapshot_path)
                .map_err(|_| Error::new(ErrorKind::MetadataError, "Could not load snapshot"))?;

            for file_meta in snapshot.files.values() {
                if let Delta::Chunks(chunks) = &file_meta.delta {
                    referenced_chunks.extend(chunks.iter().cloned());
                }
            }
        }

        // To-Do: Iterate over all stored chunks and remove those not in the set.
        todo!()
    }

    /// Return a sequence of snapshot IDs from the last full snapshot or up to a specific snapshot.
    fn get_delta_segment<F>(&self, condition: F) -> Option<Vec<String>>
    where
        F: Fn(&String, &SnapshotKind) -> bool,
    {
        let mut segment = Vec::new();

        for (id, kind, _) in self.refs.snapshots.iter().rev() {
            segment.push(id.clone());
            if condition(id, kind) {
                break;
            }
        }

        if segment.is_empty() {
            None
        } else {
            Some(segment.into_iter().rev().collect())
        }
    }

    /// Return a sequence of snapshot IDs from the last full snapshot to a specific snapshot.
    fn get_last_delta_segment_from_snapshot(&self, segment_id: &str) -> Option<Vec<String>> {
        self.get_delta_segment(|id, kind| kind == &SnapshotKind::Full)
    }

    /// Return a sequence of snapshot IDs from the last full snapshot.
    fn get_last_delta_segment(&self) -> Option<Vec<String>> {
        self.refs
            .snapshots
            .last()
            .and_then(|(id, _, _)| self.get_last_delta_segment_from_snapshot(id))
    }

    /// Compute the status of the file system at a specific snapshot.
    fn calculate_status_at_snapshot(
        &self,
        snapshot_id: &str,
    ) -> Result<HashMap<String, FileMetadata>, Error> {
        let Some(segment) = self.get_last_delta_segment_from_snapshot(snapshot_id) else {
            return Err(Error::new(
                ErrorKind::MetadataError,
                "Snapshot id not found",
            ));
        };

        let mut files = HashMap::new();

        for snapshot_id in segment {
            let snapshot: Snapshot = self
                .secure_storage
                .load_json(&self.paths.snapshots.join(&snapshot_id))
                .map_err(|_| Error::new(ErrorKind::MetadataError, "Could not load snapshot"))?;
            for (filename, file_metadata) in snapshot.files.iter() {
                if matches!(file_metadata.delta, Delta::Deleted) {
                    files.remove(filename);
                } else {
                    files.insert(filename.clone(), file_metadata.clone());
                }
            }
        }

        Ok(files)
    }
}
