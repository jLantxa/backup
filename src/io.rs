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

use serde::de::DeserializeOwned;
use serde::Serialize;
use std::io::{Read, Write};
use std::path::Path;
use zstd::stream::read::Decoder as ZstdDecoder;
use zstd::stream::write::Encoder as ZstdEncoder;

/// Secure storage is an abstraction for file IO that handles compression and encryption.
pub struct SecureStorage;

impl SecureStorage {
    /// Load a file previously saved with SecureStorage
    pub fn load_from_file(&self, path: &Path) -> std::io::Result<Vec<u8>> {
        let data = std::fs::read(path)?;
        Self::decompress(&data)
    }

    /// Save data to a file with SecureStorage
    pub fn save_to_file(
        &self,
        path: &Path,
        data: &[u8],
        compression_level: i32,
    ) -> std::io::Result<usize> {
        let compressed_data = Self::compress(data, compression_level)?;
        std::fs::write(path, &compressed_data)?;
        Ok(compressed_data.len())
    }

    /// Deserialize a JSON metadata file.
    pub fn load_json<T: DeserializeOwned>(&self, path: &Path) -> std::io::Result<T> {
        let data = self.load_from_file(path)?;
        let text = String::from_utf8(data).map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, "Invalid data parsing json")
        })?;
        serde_json::from_str(&text)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
    }

    /// Serialize a JSON metadata file.
    pub fn save_json<T: Serialize>(
        &self,
        path: &Path,
        metadata: &T,
        compression_level: i32,
    ) -> std::io::Result<()> {
        let serialized_txt = serde_json::to_string(metadata)?;
        let data = serialized_txt.as_bytes().to_vec();
        self.save_to_file(path, &data, compression_level)?;
        Ok(())
    }

    /// Compress a stream of bytes
    fn compress(data: &[u8], compression_level: i32) -> std::io::Result<Vec<u8>> {
        let mut compressed = Vec::new();
        let mut encoder = ZstdEncoder::new(&mut compressed, compression_level)?;
        encoder.write_all(data)?;
        encoder.finish()?;
        Ok(compressed)
    }

    /// Decompress a stream of bytes
    fn decompress(data: &[u8]) -> std::io::Result<Vec<u8>> {
        let mut decompressed = Vec::new();
        let mut decoder = ZstdDecoder::new(data)?;
        decoder.read_to_end(&mut decompressed)?;
        Ok(decompressed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compression_and_decompression() {
        let original_data = br#"
             Lorem ipsum dolor sit amet, consectetur adipisici elit, sed eiusmod tempor incidunt
             ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation
             ullamco laboris nisi ut aliquid ex ea commodi consequat. Quis aute iure reprehenderit in
             voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint obcaecat
             cupiditat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.
             "#;

        let compression_levels = [0, 10, 22];

        for &compression_level in &compression_levels {
            let compressed_data =
                SecureStorage::compress(original_data, compression_level).unwrap();
            let decompressed_data = SecureStorage::decompress(&compressed_data).unwrap();

            assert_eq!(*original_data, *decompressed_data);

            let ratio = original_data.len() as f64 / compressed_data.len() as f64;
            println!(
                "Compression level {}: Ratio = {:.2}",
                compression_level, ratio
            );
        }
    }
}
