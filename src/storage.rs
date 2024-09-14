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

use crate::{backup::FileMetadata, hashing, io::SecureStorage};
use std::{fs::File, io::Read, path::Path};

const CHUNK_SIZE: usize = 1024 * 1024;

pub struct StorageResult {
    pub chunk_hashes: Vec<String>,
    pub bytes_read: usize,
    pub bytes_stored: usize,
}

/// Store a file in the repository.
pub fn store_file(
    src_path: &Path,
    repo_path: &Path,
    secure_storage: &SecureStorage,
    compression_level: i32,
) -> std::io::Result<StorageResult> {
    let mut file = File::open(src_path)?;
    let mut buffer = [0_u8; CHUNK_SIZE];

    let mut chunks = Vec::new();
    let (mut bytes_total, mut bytes_stored): (usize, usize) = (0, 0);

    while let Ok(bytes_read) = file.read(&mut buffer) {
        if bytes_read == 0 {
            break;
        }

        let chunk = &buffer[..bytes_read];

        // Skip storing chunks filled with zeros
        if chunk.iter().all(|&b| b == 0) {
            continue;
        }

        let hash_str = hashing::calculate_hash(chunk);
        let (dir_name, file_name) = (&hash_str[0..2], &hash_str[2..]);

        let chunk_path = repo_path.join(dir_name).join(file_name);
        chunks.push(hash_str.clone());

        // Create directory and store the chunk if it doesn't exist
        if !chunk_path.exists() {
            std::fs::create_dir_all(repo_path.join(dir_name))?;
            let bytes_processed =
                secure_storage.save_to_file(&chunk_path, chunk, compression_level)?;
            bytes_stored += bytes_processed;
        }

        bytes_total += chunk.len();
    }

    Ok(StorageResult {
        chunk_hashes: chunks,
        bytes_read: bytes_total,
        bytes_stored,
    })
}

/// Restore a file from the repository
pub fn restore_file(file: &FileMetadata, secure_storage: &SecureStorage) -> std::io::Result<()> {
    // Placeholder function - To be implemented
    todo!()
}
