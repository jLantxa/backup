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

use serde::{Deserialize, Serialize};

use crate::{io::SecureStorage, meta};

#[derive(Debug, Serialize, Deserialize)]
pub enum SnapshotKind {
    Full,        // Record the status of all existing files, but only changes are stored
    Incremental, // Record only the files that changed from the previous snapshot
}

#[derive(Debug, Serialize, Deserialize)]
pub enum RetentionPolicy {
    KeepAll, // Keep all snapshots
}

#[derive(Debug)]
pub enum ErrorKind {}

#[derive(Debug)]
pub struct Error {
    pub kind: ErrorKind,
    pub error: String,
}

pub struct BackupManager {
    path: String,
    secure_storage: SecureStorage,
    snapshots: meta::SnapshotsRef,
}

impl BackupManager {
    pub fn from_repo(path: &str, password: &str) -> Self {
        Self {
            path: String::from(path),
            secure_storage: todo!(),
            snapshots: todo!(),
        }
    }
}
