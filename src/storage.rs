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

use crate::{
    backup::{Delta, FileMetadata},
    hashing,
    io::SecureStorage,
};
use std::{
    fs::File,
    io::{Read, Write},
    path::Path,
};

const CHUNK_SIZE: usize = 16 * 1024 * 1024; // 1MB

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
    let mut file = File::open(src_path).map_err(|e| {
        std::io::Error::new(
            e.kind(),
            format!("Failed to open source file {:?}: {}", src_path, e),
        )
    })?;

    let mut buffer = vec![0_u8; CHUNK_SIZE];
    let mut chunks = Vec::new();
    let (mut bytes_total, mut bytes_stored): (usize, usize) = (0, 0);

    loop {
        let bytes_read = file.read(&mut buffer).map_err(|e| {
            std::io::Error::new(e.kind(), format!("Failed to read from file: {}", e))
        })?;
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

        // Store the chunk if it doesn't exist
        if !chunk_path.exists() {
            let dir_path = repo_path.join(dir_name);
            if !dir_path.exists() {
                std::fs::create_dir_all(&dir_path).map_err(|e| {
                    std::io::Error::new(
                        e.kind(),
                        format!("Failed to create directory {:?}: {}", dir_path, e),
                    )
                })?;
            }

            let bytes_processed = secure_storage
                .save_to_file(&chunk_path, chunk, compression_level)
                .map_err(|e| {
                    std::io::Error::new(
                        e.kind(),
                        format!("Failed to store chunk at {:?}: {}", chunk_path, e),
                    )
                })?;
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

/// Restore a file from the repository.
pub fn restore_file(
    file: &FileMetadata,
    repo_path: &Path,
    dst_path: &Path,
    secure_storage: &SecureStorage,
) -> std::io::Result<()> {
    // TODO: Don't create the file before checking that all chunks exist.
    std::fs::create_dir_all(dst_path.parent().unwrap()).unwrap();
    let mut output_file = File::create(dst_path).map_err(|e| {
        std::io::Error::new(
            e.kind(),
            format!("Failed to create destination file {:?}: {}", dst_path, e),
        )
    })?;

    if let Delta::Chunks(chunks) = &file.delta {
        for hash_str in chunks {
            let (dir_name, file_name) = (&hash_str[0..2], &hash_str[2..]);
            let chunk_path = repo_path.join("data").join(dir_name).join(file_name);

            if chunk_path.exists() {
                let chunk_data = secure_storage.load_from_file(&chunk_path).map_err(|e| {
                    std::io::Error::new(
                        e.kind(),
                        format!("Failed to load chunk {:?}: {}", chunk_path, e),
                    )
                })?;

                output_file.write_all(&chunk_data).map_err(|e| {
                    std::io::Error::new(
                        e.kind(),
                        format!("Failed to write chunk to file {:?}: {}", dst_path, e),
                    )
                })?;
            } else {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!("Chunk {} not found", hash_str),
                ));
            }
        }
    } else {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "File is marked as deleted or contains no data",
        ));
    }

    Ok(())
}
