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

use std::io::{Read, Write};
use std::path::Path;

use zstd::stream::read::Decoder as zstdDecoder;
use zstd::stream::write::Encoder as zstdEncoder;

pub struct SecureStorage {}

impl SecureStorage {
    pub fn load_from_file(&self, path: &Path) -> std::io::Result<Vec<u8>> {
        let data = std::fs::read(path)?;
        let decompressed_data = Self::decompress(&data)?;
        Ok(decompressed_data)
    }

    pub fn save_to_file(
        &self,
        path: &Path,
        data: &Vec<u8>,
        compression_level: i32,
    ) -> std::io::Result<()> {
        let compressed_data = Self::compress(data, compression_level)?;
        std::fs::write(path, &compressed_data)?;
        Ok(())
    }

    fn compress(data: &Vec<u8>, compression_level: i32) -> std::io::Result<Vec<u8>> {
        let mut compressed = Vec::new();
        let mut encoder = zstdEncoder::new(&mut compressed, compression_level)?;
        encoder.write_all(data)?;
        encoder.finish()?;
        Ok(compressed)
    }

    fn decompress(data: &Vec<u8>) -> std::io::Result<Vec<u8>> {
        let mut decompressed = Vec::new();
        let mut decoder = zstdDecoder::new(data.as_slice())?;
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

        for compression_level in compression_levels {
            let compressed_data =
                SecureStorage::compress(&original_data.to_vec(), compression_level).unwrap();
            let decompressed_data = SecureStorage::decompress(&compressed_data).unwrap();

            assert_eq!(*original_data, *decompressed_data);

            let compression_ratio = original_data.len() as f64 / compressed_data.len() as f64;
            println!(
                "Compression level {}: Ratio = {:.2}",
                compression_level, compression_ratio
            );
        }
    }
}
