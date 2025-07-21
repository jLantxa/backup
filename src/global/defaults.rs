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

use std::time::Duration;

use crate::utils::size;

// -- Concurrency --
pub(crate) const DEFAULT_READ_CONCURRENCY: usize = 4;
pub(crate) const DEFAULT_WRITE_CONCURRENCY: usize = 5;

// -- Index --
pub(crate) const INDEX_FLUSH_TIMEOUT: Duration = Duration::from_secs(10 * 60);
pub(crate) const BLOBS_PER_INDEX_FILE: usize = 65535;

// -- Packing --
/// Minimum pack size before flushing to the backend.
pub const DEFAULT_DEFAULT_PACK_SIZE_MIB: f32 = 16.0;
pub const DEFAULT_PACK_SIZE: u64 = (DEFAULT_DEFAULT_PACK_SIZE_MIB * size::MiB as f32) as u64;
pub const DEFAULT_MAX_PACK_SIZE_MIB: f32 = 4.0 * 1024.0;
pub const DEFAULT_MAX_PACK_SIZE: u64 = (DEFAULT_MAX_PACK_SIZE_MIB * size::MiB as f32) as u64 - 1;

pub(crate) const HEADER_BLOB_MULTIPLE: usize = 64;

// -- Chunking --
/// Minimum chunk size
pub(crate) const MIN_CHUNK_SIZE: u64 = 512 * size::KiB;
/// Average chunk size
pub(crate) const AVG_CHUNK_SIZE: u64 = size::MiB;
/// Maximum chunk size
pub(crate) const MAX_CHUNK_SIZE: u64 = 8 * size::MiB;

// -- Display --
/// Display length for the repository ID in bytes
pub(crate) const SHORT_REPO_ID_LEN: usize = 5;

/// Display length for a Snapshot ID in bytes
pub(crate) const SHORT_SNAPSHOT_ID_LEN: usize = 4;

pub(crate) const DEFAULT_VERBOSITY: u32 = 1;

// -- Garbage collection --
/// Percentage of garbage to tolerate per pack
pub(crate) const DEFAULT_GC_TOLERANCE: f32 = 0.0; // [0 - 1]

/// Repack files smaller than this factor of the max pack size
pub(crate) const DEFAULT_MIN_PACK_SIZE_FACTOR: f32 = 0.05;
