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

use secrecy::Secret;

pub struct SecureStorage {
    key: Secret<[u8; 32]>,
}

impl SecureStorage {
    pub fn new(password: &str) -> Self {
        Self { key: todo!() }
    }

    pub fn load(&self, path: &str) -> std::io::Result<Vec<u8>> {
        todo!()
    }

    pub fn save(&self, path: &str, data: &Vec<u8>) -> std::io::Result<()> {
        todo!()
    }
}
