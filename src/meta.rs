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

use crate::backup;

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct FileMetadata {
    pub path: String,
    pub chunks: Vec<String>,
    pub file_size: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SnapshotMetadata {
    pub id: String,
    pub kind: backup::SnapshotKind,
    pub timestamp: String,
    pub previous_snapshot_id: Option<String>,
    pub files: HashMap<String, FileMetadata>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct SnapshotsRef {
    pub snapshots: Vec<(String, backup::SnapshotKind, String)>, // (id, kind, timestamp)
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SettingsMetadata {
    pub retention_policy: backup::RetentionPolicy,
}
