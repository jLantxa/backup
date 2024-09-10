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

use crate::hashing;
use crate::{io::SecureStorage, meta::FileMetadata};
use std::fs::File;
use std::io::Read;
use std::path::Path;

const CHUNK_SIZE: usize = 1024 * 1024;

pub fn store_file(
    src_path: &Path,
    repo_path: &Path,
    secure_storage: &SecureStorage,
    compression_level: i32,
) -> std::io::Result<f64> {
    let mut file = File::open(src_path)?;
    let mut buffer = [0_u8; CHUNK_SIZE];

    let mut bytes_compressed: f64 = 0.0;
    let mut bytes_total: f64 = 0.0;

    loop {
        let bytes_read = file.read(&mut buffer)?;
        if bytes_read == 0 {
            break;
        }

        let chunk = &buffer[..bytes_read];

        if chunk.iter().all(|&b| b == 0) {
            continue;
        }

        let hash_str = hashing::calculate_hash(chunk);
        let dir_name = &hash_str[0..2];
        let file_name = &hash_str[2..];

        let chunk_dir = repo_path.join(dir_name);
        let chunk_path = chunk_dir.join(file_name);

        std::fs::create_dir_all(&chunk_dir)?;
        let ratio = secure_storage.save_to_file(&chunk_path, &chunk.to_vec(), compression_level)?;
        bytes_compressed += chunk.len() as f64 / ratio;
        bytes_total += chunk.len() as f64;
    }

    let file_compression_ratio = bytes_total / bytes_compressed;
    Ok(file_compression_ratio)
}

pub fn restore_file(file: &FileMetadata, secure_storage: &SecureStorage) -> std::io::Result<()> {
    todo!()
}
