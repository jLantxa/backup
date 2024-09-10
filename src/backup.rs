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
    path::{Path, PathBuf},
    process::exit,
};

use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::{
    io::SecureStorage,
    meta::{self},
};

#[derive(Debug, Serialize, Deserialize)]
pub enum SnapshotKind {
    Full,        // Record the status of all existing files, but only changes are stored
    Incremental, // Record only the files that changed from the previous snapshot
}

#[derive(Debug, Serialize, Deserialize)]
pub enum RetentionPolicy {
    KeepAll, // Keep all snapshots
}

impl Default for RetentionPolicy {
    fn default() -> Self {
        RetentionPolicy::KeepAll
    }
}

#[derive(Debug)]
pub enum ErrorKind {}

#[derive(Debug)]
pub struct Error {
    pub kind: ErrorKind,
    pub error: String,
}

pub struct BackupManager {
    repo_path: PathBuf,
    secure_storage: SecureStorage,
    settings: meta::SettingsMetadata,
    snapshots_refs: meta::SnapshotsRef,

    settings_path: PathBuf,
    snapshots_ref_path: PathBuf,
}

impl BackupManager {
    pub fn init_repo(path: &Path, password: &str) -> std::io::Result<()> {
        let manager = Self::new(path, password);

        Self::create_dirs(&path)?;
        manager.persist_meta()?;

        Ok(())
    }

    fn create_dirs(path: &Path) -> std::io::Result<()> {
        for i in 0..=0xff {
            let folder_name = format!("{:02x}", i);
            let dir = path.join(String::from(folder_name));
            std::fs::create_dir_all(dir)?;
        }

        Ok(())
    }

    pub fn new(path: &Path, password: &str) -> Self {
        let repo_path = Path::new(path).to_owned();
        let secure_storage = SecureStorage {};
        let settings_path = repo_path
            .join(meta::META_PATH)
            .join(meta::SETTINGS_REF_PATH);
        let snapshots_ref_path = repo_path
            .join(meta::META_PATH)
            .join(meta::SNAPSHOTS_REF_PATH);

        Self {
            repo_path: repo_path,
            secure_storage: secure_storage,
            settings: Default::default(),
            snapshots_refs: Default::default(),
            settings_path: settings_path,
            snapshots_ref_path: snapshots_ref_path,
        }
    }

    pub fn from_repo(path: &Path, password: &str) -> Self {
        if !path.exists() {
            eprintln!(r#"Error: Repo '{}' does not exist"#, path.to_str().unwrap());
            exit(1);
        }

        let mut manager = BackupManager::new(path, password);
        manager.init_meta();

        manager
    }

    fn load_json<T: DeserializeOwned>(&self, path: &Path) -> std::io::Result<T> {
        let data = self
            .secure_storage
            .load_from_file(path)
            .expect("Could not load metadata file");

        let text = String::from_utf8(data).expect("Could not read from file stream");

        Ok(serde_json::from_str(&text).expect("Could not deserialize JSON data"))
    }

    fn save_json<T: Serialize>(&self, path: &Path, metadata: &T) -> std::io::Result<()> {
        let serialized_txt =
            serde_json::to_string(metadata).expect("Could not serialize data to JSON");
        let data = serialized_txt.as_bytes().to_vec();

        const METADATA_COMPRESSION_LEVEL: i32 = 22;
        self.secure_storage
            .save_to_file(path, &data, METADATA_COMPRESSION_LEVEL)?;

        Ok(())
    }

    fn init_meta(&mut self) -> std::io::Result<()> {
        self.settings = self.load_json(&self.settings_path)?;
        self.snapshots_refs = self.load_json(&self.snapshots_ref_path)?;

        Ok(())
    }

    fn persist_meta(&self) -> std::io::Result<()> {
        std::fs::create_dir_all(Path::new(&self.repo_path).join(meta::META_PATH))?;

        self.save_json(&self.settings_path, &self.settings)?;
        self.save_json(&self.snapshots_ref_path, &self.snapshots_refs)?;

        Ok(())
    }
}
