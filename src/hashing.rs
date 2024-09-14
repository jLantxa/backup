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

use sha2::{Digest, Sha256};

/// Calculate the SHA-256 hash of a stream of bytes.
pub fn calculate_hash(data: &[u8]) -> String {
    let hash = Sha256::digest(data);
    format!("{:x}", hash)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Test the hashing function to ensure it produces consistent results.
    #[test]
    fn test_calculate_hash() {
        let data = b"Lorem ipsum dolor sit amet";
        let expected_hash = "16aba5393ad72c0041f5600ad3c2c52ec437a2f0c7fc08fadfc3c0fe9641d7a3";
        let hash = calculate_hash(data);
        assert_eq!(hash, expected_hash);
    }
}
