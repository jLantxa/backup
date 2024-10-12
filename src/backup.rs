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

use crate::{io::SecureStorage, storage};
use chrono::{Local, TimeZone, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io;
use std::path::{Path, PathBuf};
use thiserror::Error;

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
    fn to_i32(&self) -> i32 {
        match self {
            CompressionLevel::LOW => 3,
            CompressionLevel::MID => 9,
            CompressionLevel::HIGH => 15,
            CompressionLevel::MAX => 22,
        }
    }
}

/// Paths structure for repository directories
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
    pub utc_timestamp: i64,
    pub previous_snapshot_id: Option<String>,
    pub files: HashMap<String, FileMetadata>,
}

/// Reference for all snapshots in the repo.
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct SnapshotsRef {
    pub snapshots: Vec<(String, SnapshotKind, i64)>, // (id, kind, timestamp)
}

/// Repository settings.
#[derive(Debug, Serialize, Deserialize)]
pub struct Config {
    pub retention_policy: RetentionPolicy,
    pub compression_level: CompressionLevel,
    pub max_consecutive_deltas: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            retention_policy: Default::default(),
            compression_level: Default::default(),
            max_consecutive_deltas: 10,
        }
    }
}

/// Delta types for file changes in a snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Delta {
    Chunks(Vec<String>),
    Deleted,
}

/// Custom error types for repository operations.
#[derive(Error, Debug)]
pub enum RepoError {
    #[error("Repository initialization failed")]
    RepoInitError,

    #[error("Failed to load metadata: {0}")]
    MetadataError(String),

    #[error("Storage error: {0}")]
    StorageError(String),

    #[error("File system error")]
    FileSystemError(#[from] std::io::Error),

    #[error("Invalid snapshot ID: {0}")]
    InvalidSnapshotId(String),

    #[error("Failed to store file: {0}")]
    StoreFileError(String),

    #[error("Unexpected error: {0}")]
    Unknown(String),
}

/// Repo handles backup repository operations.
pub struct Repo {
    repo_path: PathBuf,
    paths: Paths,
    secure_storage: SecureStorage,
    config: Config,
    refs: SnapshotsRef,
}

impl Repo {
    /// Create a new repository
    pub fn new(path: &Path, password: &str) -> Result<Self, RepoError> {
        let secure_storage = SecureStorage {};
        let repo_path = path.to_path_buf();
        let paths = Self::build_paths(&repo_path);

        Self::create_dirs(&repo_path)?;

        let repo = Self {
            repo_path,
            paths,
            secure_storage,
            config: Default::default(),
            refs: Default::default(),
        };

        repo.persist_meta()?;

        Ok(repo)
    }

    /// Load an existing repository
    pub fn from_existing(path: &Path, password: &str) -> Result<Self, RepoError> {
        let secure_storage = SecureStorage {};
        let repo_path = path.to_path_buf();
        let paths = Self::build_paths(&repo_path);

        let config: Config = secure_storage
            .load_json(&paths.config)
            .map_err(|e| RepoError::MetadataError(format!("Failed to load config: {}", e)))?;

        let refs: SnapshotsRef = secure_storage
            .load_json(&paths.refs)
            .map_err(|e| RepoError::MetadataError(format!("Failed to load refs: {}", e)))?;

        Ok(Self {
            repo_path,
            paths,
            secure_storage,
            config,
            refs,
        })
    }

    fn build_paths(repo_path: &Path) -> Paths {
        Paths {
            config: repo_path.join("config"),
            refs: repo_path.join("refs"),
            snapshots: repo_path.join("snapshots"),
            data: repo_path.join("data"),
        }
    }

    /// Create repository directories
    fn create_dirs(path: &Path) -> io::Result<()> {
        let data_path = path.join("data");
        let snapshots_path = path.join("snapshots");

        std::fs::create_dir_all(&data_path)?;
        std::fs::create_dir_all(&snapshots_path)?;

        (0x00..=0xff)
            .map(|i| std::fs::create_dir_all(data_path.join(format!("{:02x}", i))))
            .collect::<Result<_, _>>()
    }

    /// Persist repository metadata to disk
    fn persist_meta(&self) -> Result<(), RepoError> {
        let compression_level = self.config.compression_level.to_i32();
        self.secure_storage
            .save_json(&self.paths.config, &self.config, compression_level)
            .map_err(|e| RepoError::StorageError(format!("Failed to save config: {}", e)))?;

        self.secure_storage
            .save_json(&self.paths.refs, &self.refs, compression_level)
            .map_err(|e| RepoError::StorageError(format!("Failed to save refs: {}", e)))?;

        Ok(())
    }

    /// Backup a directory to the repository.
    pub fn backup(&mut self, src_dir: &Path) -> Result<(), RepoError> {
        let snapshot_kind = self.determine_snapshot_kind();

        let source_files = Self::get_all_files_recursive(src_dir)?;

        let last_snapshot_files = self.get_last_snapshot_files().map_err(|e| {
            RepoError::MetadataError(format!("Failed to retrieve last snapshot files: {}", e))
        })?;

        let compression_level = self.config.compression_level.to_i32();

        let snapshot_files = self
            .process_source_files(
                src_dir,
                source_files,
                &last_snapshot_files,
                compression_level,
            )
            .map_err(|e| {
                RepoError::StoreFileError(format!("Failed to process source files: {}", e))
            })?;

        self.create_snapshot(snapshot_kind, snapshot_files)
            .map_err(|e| RepoError::StorageError(format!("Failed to create snapshot: {}", e)))?;

        Ok(())
    }

    /// Restore a snapshot from the repository.
    pub fn restore_snapshot(&self, snapshot_id: &str, dst_path: &Path) -> Result<(), RepoError> {
        let files = self
            .calculate_status_at_snapshot(snapshot_id)
            .map_err(|_| {
                RepoError::InvalidSnapshotId(format!("Failed to restore snapshot: {}", snapshot_id))
            })?;

        for (repo_filename, file_metadata) in files {
            let file_dst_path = dst_path.join(&repo_filename);
            storage::restore_file(
                &file_metadata,
                &self.repo_path,
                &file_dst_path,
                &self.secure_storage,
            )
            .map_err(|e| RepoError::StoreFileError(format!("Failed to restore file: {}", e)))?;
        }

        Ok(())
    }

    pub fn restore_last_snapshot(&self, dst_path: &Path) -> Result<(), RepoError> {
        if let Some((id, _, _)) = self.refs.snapshots.last() {
            return self.restore_snapshot(id, dst_path);
        }

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

            if num_deltas < self.config.max_consecutive_deltas {
                SnapshotKind::Delta
            } else {
                SnapshotKind::Full
            }
        }
    }

    fn get_last_snapshot_files(&self) -> Result<HashMap<String, FileMetadata>, RepoError> {
        match self.refs.snapshots.last() {
            None => Ok(HashMap::new()),
            Some((id, _, _)) => self.calculate_status_at_snapshot(id),
        }
    }

    fn process_source_files(
        &self,
        src_dir: &Path,
        source_files: Vec<PathBuf>,
        last_snapshot_files: &HashMap<String, FileMetadata>,
        compression_level: i32,
    ) -> Result<HashMap<String, FileMetadata>, RepoError> {
        let mut snapshot_files = HashMap::new();

        for source_file in source_files {
            let path_relative_path = source_file.strip_prefix(src_dir).unwrap().to_owned();
            let storage_result = self.store_and_compress_file(&source_file, compression_level)?;
            if self.file_has_changed(src_dir, &source_file, &storage_result, last_snapshot_files) {
                snapshot_files.insert(
                    path_relative_path.to_string_lossy().to_string(),
                    FileMetadata {
                        path: path_relative_path.to_string_lossy().to_string(),
                        delta: Delta::Chunks(storage_result.chunk_hashes),
                        file_size: 0,                      // TODO: Add file size
                        modify_date: String::from("todo"), // TODO: Add modify date
                    },
                );
            }
        }

        Ok(snapshot_files)
    }

    fn store_and_compress_file(
        &self,
        source_file: &PathBuf,
        compression_level: i32,
    ) -> Result<storage::StorageResult, RepoError> {
        storage::store_file(
            source_file,
            &self.paths.data,
            &self.secure_storage,
            compression_level,
        )
        .map_err(|e| RepoError::StoreFileError(format!("Could not store file: {}", e)))
    }

    fn file_has_changed(
        &self,
        src_dir: &Path,
        source_file: &PathBuf,
        storage_result: &storage::StorageResult,
        last_snapshot_files: &HashMap<String, FileMetadata>,
    ) -> bool {
        let repo_relative_path = source_file.strip_prefix(src_dir).unwrap();
        match last_snapshot_files.get(repo_relative_path.to_string_lossy().as_ref()) {
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
    ) -> Result<String, RepoError> {
        let snapshot_id = self.refs.snapshots.len().to_string();
        let previous_snapshot_id = self.refs.snapshots.last().map(|(id, _, _)| id.clone());
        let utc_timestamp = Self::get_utc_timestamp();

        let snapshot = Snapshot {
            id: snapshot_id.clone(),
            kind: snapshot_kind.clone(),
            utc_timestamp: utc_timestamp,
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
            .map_err(|e| RepoError::StorageError(format!("Failed to save snapshot: {}", e)))?;

        self.refs
            .snapshots
            .push((snapshot_id.clone(), snapshot_kind, utc_timestamp));

        self.persist_meta()?;
        Ok(snapshot_id)
    }

    /// Retrieve all files recursively from a directory.
    fn get_all_files_recursive(directory: &Path) -> Result<Vec<PathBuf>, RepoError> {
        let mut files = Vec::new();
        let entries = std::fs::read_dir(directory).map_err(|e| RepoError::FileSystemError(e))?;

        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_file() {
                files.push(path);
            } else if path.is_dir() {
                files.extend(Self::get_all_files_recursive(&path)?);
            }
        }

        Ok(files)
    }

    fn calculate_status_at_snapshot(
        &self,
        snapshot_id: &str,
    ) -> Result<HashMap<String, FileMetadata>, RepoError> {
        let segment = self.get_delta_segment(snapshot_id).ok_or_else(|| {
            RepoError::InvalidSnapshotId(format!("Snapshot ID not found: {}", snapshot_id))
        })?;

        let mut files = HashMap::new();

        for snapshot_id in segment {
            let snapshot: Snapshot = self
                .secure_storage
                .load_json(&self.paths.snapshots.join(&snapshot_id))
                .map_err(|e| RepoError::MetadataError(format!("Failed to load snapshot: {}", e)))?;

            for (filename, file_metadata) in snapshot.files {
                match file_metadata.delta {
                    Delta::Deleted => {
                        files.remove(&filename);
                    }
                    _ => {
                        files.insert(filename, file_metadata);
                    }
                }
            }
        }

        Ok(files)
    }

    fn get_delta_segment(&self, snapshot_id: &str) -> Option<Vec<String>> {
        let index = self
            .refs
            .snapshots
            .iter()
            .position(|(id, _, _)| id == snapshot_id)?;

        let mut segment = Vec::new();

        for (id, kind, _) in self.refs.snapshots[..=index].iter().rev() {
            segment.push(id.clone());
            if *kind == SnapshotKind::Full {
                break;
            }
        }

        if segment.is_empty() {
            None
        } else {
            Some(segment.into_iter().rev().collect())
        }
    }

    pub fn list_snapshots(&self) -> Vec<(String, SnapshotKind, String)> {
        let mut snapshots = Vec::new();

        for (id, kind, utc_timestamp) in &self.refs.snapshots {
            let local_timestamp = Self::utc_to_local_format(*utc_timestamp);
            snapshots.push((id.clone(), kind.clone(), local_timestamp));
        }

        snapshots
    }

    /// Get the current UTC timestamp in Unix time (seconds since the epoch).
    fn get_utc_timestamp() -> i64 {
        Utc::now().timestamp()
    }

    /// Convert a given UTC timestamp to a human-readable time in the user's local timezone.
    fn utc_to_local_format(utc_timestamp: i64) -> String {
        let local_time = Local.timestamp_opt(utc_timestamp, 0).unwrap();

        local_time.format("%Y-%m-%d %H:%M:%S").to_string()
    }
}
