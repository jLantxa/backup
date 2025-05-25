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

use std::path::PathBuf;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::ID;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Snapshot {
    /// The snapshot timestamp is the UTC time at which the snapshot was created
    pub timestamp: DateTime<Utc>,

    /// Hash ID for the tree object root.
    pub tree: ID,

    /// Snapshot size in bytes
    pub size: u64,

    /// Snapshot root path
    pub root: PathBuf,

    /// Absolute paths to the targets
    pub paths: Vec<PathBuf>,

    /// Description of the snapshot.
    pub description: Option<String>,
}
