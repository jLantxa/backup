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

use std::{
    io::{Read, Seek, SeekFrom},
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};

use crate::backend::FileAttr;

use super::StorageBackend;

/// A local file system
#[derive(Default)]
pub struct LocalFS {
    repo_path: PathBuf,
}

impl LocalFS {
    pub fn new(repo_path: PathBuf) -> Self {
        Self { repo_path }
    }

    fn full_path(&self, path: &Path) -> PathBuf {
        self.repo_path.join(path)
    }

    fn exists_exact(&self, path: &Path) -> bool {
        std::fs::exists(path).unwrap_or_default()
    }
}

impl StorageBackend for LocalFS {
    fn create(&self) -> Result<()> {
        // Create the repo root folder
        std::fs::create_dir_all(&self.repo_path)
            .with_context(|| "Could not create repository backend root")
    }

    #[inline]
    fn root_exists(&self) -> bool {
        self.exists_exact(&self.repo_path)
    }

    fn read(&self, path: &Path) -> Result<Vec<u8>> {
        let full_path = self.full_path(path);
        let data = std::fs::read(full_path)
            .with_context(|| format!("Could not read \'{}\' from local backend", path.display()))?;
        Ok(data)
    }

    fn seek_read(&self, path: &Path, offset: u64, length: u64) -> Result<Vec<u8>> {
        let full_path = self.full_path(path);
        let mut file = std::fs::File::open(full_path).with_context(|| {
            format!(
                "Could not open file {} for range reading from local filesystem",
                path.display()
            )
        })?;

        // Seek to the specified offset
        file.seek(SeekFrom::Start(offset))
            .with_context(|| format!("Could not seek to offset {offset} in local file {path:?}"))?;

        // Read the specified number of bytes
        let mut buffer = vec![0; length as usize];
        file.read_exact(&mut buffer).with_context(|| {
            format!(
                "Could not read {} bytes from offset {} in local file {}",
                length,
                offset,
                path.display()
            )
        })?;

        Ok(buffer)
    }

    fn write(&self, path: &Path, contents: &[u8]) -> Result<()> {
        let full_path = self.full_path(path);
        std::fs::write(full_path, contents)
            .with_context(|| format!("Could not write to \'{}\' in local backend", path.display()))
    }

    fn rename(&self, from: &Path, to: &Path) -> Result<()> {
        let fullpath_from = self.full_path(from);
        let fullpath_to = self.full_path(to);
        std::fs::rename(fullpath_from, fullpath_to).with_context(|| {
            format!(
                "Could not rename \'{}\' to \'{}\' in local backend",
                from.display(),
                to.display()
            )
        })
    }

    fn remove_file(&self, file_path: &Path) -> Result<()> {
        let full_path = self.full_path(file_path);
        std::fs::remove_file(full_path).with_context(|| {
            format!(
                "Could not remove file \'{}\' from local backend",
                file_path.display()
            )
        })
    }

    fn create_dir(&self, path: &Path) -> Result<()> {
        let full_path = self.full_path(path);
        std::fs::create_dir(full_path).with_context(|| {
            format!(
                "Could not create directory \'{}\' in local backend",
                path.display()
            )
        })
    }

    fn create_dir_all(&self, path: &Path) -> Result<()> {
        let full_path = self.full_path(path);
        std::fs::create_dir_all(full_path).with_context(|| {
            format!(
                "Could not create directory \'{}\' in local backend",
                path.display()
            )
        })
    }

    fn remove_dir(&self, path: &Path) -> Result<()> {
        let full_path = self.full_path(path);
        std::fs::remove_dir(full_path).with_context(|| {
            format!(
                "Could not remove directory \'{}\' in local backend",
                path.display()
            )
        })
    }

    fn remove_dir_all(&self, path: &Path) -> Result<()> {
        let full_path = self.full_path(path);
        std::fs::remove_dir_all(full_path).with_context(|| {
            format!(
                "Could not remove directory \'{}\' in local backend",
                path.display()
            )
        })
    }

    fn exists(&self, path: &Path) -> bool {
        let full_path = self.full_path(path);
        self.exists_exact(&full_path)
    }

    fn read_dir(&self, path: &Path) -> Result<Vec<PathBuf>> {
        let full_path = self.full_path(path);
        let mut paths = Vec::new();
        for entry in std::fs::read_dir(full_path).with_context(|| {
            format!(
                "Could not list directory \'{}\' in local backend",
                path.display()
            )
        })? {
            let entry = entry?;
            paths.push(
                entry
                    .path()
                    .strip_prefix(&self.repo_path)
                    .unwrap()
                    .to_path_buf(),
            );
        }

        Ok(paths)
    }

    fn is_file(&self, path: &Path) -> bool {
        let full_path = self.full_path(path);
        full_path.is_file()
    }

    fn is_dir(&self, path: &Path) -> bool {
        let full_path = self.full_path(path);
        full_path.is_dir()
    }

    fn seek_read_from_end(&self, path: &Path, offset: i64, length: u64) -> Result<Vec<u8>> {
        let full_path = self.full_path(path);
        let mut file = std::fs::File::open(full_path).with_context(|| {
            format!(
                "Could not open file {} for range reading from local filesystem",
                path.display()
            )
        })?;

        // Seek to the specified offset
        file.seek(SeekFrom::End(offset)).with_context(|| {
            format!("Could not seek to offset (from End) {offset} in local file {path:?}")
        })?;

        // Read the specified number of bytes
        let mut buffer = vec![0; length as usize];
        file.read_exact(&mut buffer).with_context(|| {
            format!(
                "Could not read {} bytes from offset (from End) {} in local file {}",
                length,
                offset,
                path.display()
            )
        })?;

        Ok(buffer)
    }

    fn lstat(&self, path: &Path) -> Result<super::FileAttr> {
        let full_path = self.full_path(path);
        let meta = std::fs::symlink_metadata(&full_path)?;

        Ok(FileAttr {
            size: Some(meta.len()),
            uid: None,
            gid: None,
            perm: None,
            atime: Some(meta.accessed()?),
            mtime: Some(meta.modified()?),
        })
    }
}

#[cfg(test)]
mod tests {

    use tempfile::tempdir;

    use super::*;

    #[test]

    fn test_local_fs() -> Result<()> {
        let temp_dir = tempdir()?;
        let temp_dir = temp_dir.path();
        let local_fs = Box::new(LocalFS::new(temp_dir.to_path_buf()));

        let write_path = Path::new("file.txt");
        local_fs.write(write_path, b"Mapachito")?;
        let read_content = local_fs.read(write_path)?;

        assert!(local_fs.exists(write_path));
        assert_eq!(read_content, b"Mapachito");

        let dir0 = Path::new("dir0");
        let intermediate = dir0.join("intermediate");
        let dir1 = intermediate.join("dir1");
        local_fs.create_dir(dir0)?;
        local_fs.create_dir_all(&dir1)?;
        assert!(local_fs.exists(dir0));
        assert!(local_fs.exists(&intermediate));
        assert!(local_fs.exists(&dir1));

        local_fs.remove_dir(&dir1)?;
        assert!(!local_fs.exists(&dir1));
        local_fs.remove_dir_all(dir0)?;
        assert!(!local_fs.exists(dir0));
        assert!(!local_fs.exists(&intermediate));
        assert!(!local_fs.exists(&dir1));

        let invalid_path = Path::new("fake_path");
        assert!(!local_fs.exists(invalid_path));
        assert!(local_fs.read(invalid_path).is_err());

        // Read range
        let seek_path = Path::new("seek.txt.");
        local_fs.write(seek_path, b"I am just looking for a word in this sentence.")?;
        let range_str = local_fs.seek_read(seek_path, 10, 7)?;
        assert_eq!(range_str, b"looking");

        Ok(())
    }
}
