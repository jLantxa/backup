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

pub struct StorageResult {
    pub chunk_hashes: Vec<String>,
    pub bytes_read: usize,
    pub bytes_stored: usize,
}

/// Assign a chunk size to every file size.
fn get_chunk_size(file_size: usize) -> usize {
    static CHUNK_SIZES: [(usize, usize); 8] = [
        (4 * 1024 * 1024 * 1024, 256 * 1024 * 1024), // > 4 GB => 256 MB
        (1 * 1024 * 1024 * 1024, 64 * 1024 * 1024),  // 1 GB - 4 GB => 64 MB
        (256 * 1024 * 1024, 16 * 1024 * 1024),       // 256 MB - 1 GB => 16 MB
        (64 * 1024 * 1024, 4 * 1024 * 1024),         // 64 MB - 256 MB => 4 MB
        (16 * 1024 * 1024, 1 * 1024 * 1024),         // 16 MB - 64 MB => 1 MB
        (4 * 1024 * 1024, 256 * 1024),               // 4 MB - 16 MB => 256 KB
        (1 * 1024 * 1024, 64 * 1024),                // 1 MB - 4 MB => 64 KB
        (256 * 1024, 16 * 1024),                     // 256 KB - 1 MB => 16 KB
    ];

    for (table_file_size, table_chunk_size) in CHUNK_SIZES {
        if file_size > table_file_size {
            return table_chunk_size;
        }
    }

    4 * 1024 // 0 - 4 GB => 4 KB
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

    let file_metadata = std::fs::metadata(src_path).unwrap();
    let chunk_size = get_chunk_size(file_metadata.len() as usize);

    let mut buffer = vec![0_u8; chunk_size];
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

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    /// Test different file sizes to verify the correct chunk size is returned
    fn test_get_chunk_size() {
        // Test very small file sizes
        assert_eq!(get_chunk_size(0), 4 * 1024); // File size: 0 bytes -> Chunk size: 4 KB
        assert_eq!(get_chunk_size(100 * 1024), 4 * 1024); // File size: 100 KB -> Chunk size: 4 KB
        assert_eq!(get_chunk_size(200 * 1024), 4 * 1024); // File size: 200 KB -> Chunk size: 4 KB

        // Test small file sizes in the next range
        assert_eq!(get_chunk_size(300 * 1024), 16 * 1024); // File size: 300 KB -> Chunk size: 16 KB
        assert_eq!(get_chunk_size(1 * 1024 * 1024), 16 * 1024); // File size: 1 MB -> Chunk size: 16 KB
        assert_eq!(get_chunk_size(3 * 1024 * 1024), 64 * 1024); // File size: 3 MB -> Chunk size: 64 KB

        // Test medium file sizes
        assert_eq!(get_chunk_size(10 * 1024 * 1024), 256 * 1024); // File size: 10 MB -> Chunk size: 256 KB
        assert_eq!(get_chunk_size(30 * 1024 * 1024), 1 * 1024 * 1024); // File size: 30 MB -> Chunk size: 1 MB

        // Test large file sizes
        assert_eq!(get_chunk_size(100 * 1024 * 1024), 4 * 1024 * 1024); // File size: 100 MB -> Chunk size: 4 MB
        assert_eq!(get_chunk_size(300 * 1024 * 1024), 16 * 1024 * 1024); // File size: 300 MB -> Chunk size: 16 MB

        // Test very large file sizes
        assert_eq!(get_chunk_size(2 * 1024 * 1024 * 1024), 64 * 1024 * 1024); // File size: 2 GB -> Chunk size: 64 MB
        assert_eq!(get_chunk_size(5 * 1024 * 1024 * 1024), 256 * 1024 * 1024); // File size: 5 GB -> Chunk size: 256 MB
    }
}
