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

use std::sync::Arc;
use std::time::SystemTime;

use {
    anyhow::{Context, Result},
    filetime::{FileTime, set_file_times},
    std::{
        fs::{self, OpenOptions},
        io::Write,
        path::Path,
    },
};

use crate::{
    repository::{
        repo::Repository,
        tree::{Node, NodeType},
    },
    ui::{self, restore_progress::RestoreProgressReporter},
};

#[cfg(unix)]
use {
    anyhow::bail,
    std::{fs::Permissions, os::unix::fs::PermissionsExt},
};

/// Restores a node to the specified destination path.
/// This function does not restore file times for directory nodes. This must be
/// done in a reparate pass.
pub(crate) fn restore_node_to_path(
    repo: &Repository,
    progress_reporter: Arc<RestoreProgressReporter>,
    node: &Node,
    dst_path: &Path,
    dry_run: bool,
) -> Result<()> {
    match node.node_type {
        NodeType::File => {
            let blocks = node
                .blobs
                .as_ref()
                .expect("File Node must have contents (even if empty)");

            let dst_file = if !dry_run {
                if let Some(parent) = dst_path.parent() {
                    fs::create_dir_all(parent).with_context(|| {
                        format!(
                            "Could not create parent directories for file '{}'",
                            dst_path.display()
                        )
                    })?;
                }

                Some(
                    OpenOptions::new()
                        .create(true)
                        .truncate(true)
                        .write(true)
                        .open(dst_path)
                        .with_context(|| {
                            format!("Could not create destination file '{}'", dst_path.display())
                        })?,
                )
            } else {
                None
            };

            for (index, blob_id) in blocks.iter().enumerate() {
                let chunk_data = repo.load_blob(blob_id).with_context(|| {
                    format!(
                        "Could not load block #{} ({}) for restoring file '{}'",
                        index + 1,
                        blob_id,
                        dst_path.display()
                    )
                })?;

                let chunk_size = chunk_data.len() as u64;

                if !dry_run {
                    dst_file
                        .as_ref()
                        .expect("Destination file should exist")
                        .write_all(&chunk_data)
                        .with_context(|| {
                            format!(
                                "Could not restore block #{} ({}) to file '{}'",
                                index + 1,
                                blob_id,
                                dst_path.display()
                            )
                        })?;
                }

                progress_reporter.processed_bytes(chunk_size);
            }

            // Restore metadata after content is written
            if !dry_run {
                restore_node_metadata(node, dst_path)?;
            }
        }

        NodeType::Directory => {
            if !dry_run {
                std::fs::create_dir_all(dst_path).with_context(|| {
                    format!("Could not create directory '{}'", dst_path.display())
                })?;

                // We don't restore metadata for directories now, as the filetimes
                // will change if we touch any children nodes. We will restore the
                // directory metadata in a second, dedicated bottom-up pass.
            }
        }

        NodeType::Symlink => {
            let symlink_info = node.symlink_info.as_ref();

            // Show a warning if the symlink metadata is missing and return.
            if symlink_info.is_none() {
                ui::cli::warning!("Symlink {} does not have a target path", dst_path.display());
                return Ok(());
            }
            let symlink_info = symlink_info.unwrap();

            #[cfg(unix)]
            {
                if !dry_run
                    && let Err(e) = std::os::unix::fs::symlink(&symlink_info.target_path, dst_path)
                {
                    ui::cli::warning!(
                        "Could not create symlink '{}' pointing to '{}' : {}",
                        dst_path.display(),
                        symlink_info.target_path.display(),
                        e.to_string()
                    );
                }
            }
            #[cfg(windows)]
            {
                // Windows distinguishes symlinks to files and symlinks to dirs
                match symlink_info.target_type {
                    // Directory symlink
                    Some(NodeType::Directory) => {
                        if !dry_run
                            && let Err(e) = std::os::windows::fs::symlink_dir(
                                dst_path,
                                &symlink_info.target_path,
                            )
                        {
                            ui::cli::warning!(
                                "Could not create symlink '{}' pointing to '{}' : {}",
                                dst_path.display(),
                                symlink_info.target_path.display(),
                                e.to_string()
                            );
                        }
                    }
                    // Everything else (not a directory)
                    Some(_) => {
                        if !dry_run
                            && let Err(e) = std::os::windows::fs::symlink_file(
                                dst_path,
                                &symlink_info.target_path,
                            )
                        {
                            ui::cli::warning!(
                                "Could not create symlink '{}' pointing to '{}' : {}",
                                dst_path.display(),
                                symlink_info.target_path.display(),
                                e.to_string()
                            );
                        }
                    }
                    // No type info. Show warning.
                    None => {
                        ui::cli::warning!("Symlink {} has no type info", dst_path.display());
                    }
                }
            }

            // TODO:
            // Restoring symlink metadata is a bit special, so let's skip it for now.
        }

        NodeType::BlockDevice => {
            #[cfg(unix)]
            ui::cli::warning!(
                "Restoration of block device '{}' not supported yet.",
                dst_path.display()
            );
            #[cfg(not(unix))]
            ui::cli::warning!(
                "Block device restoration not supported on this operating system: '{}'",
                dst_path.display()
            );
        }

        NodeType::CharDevice => {
            #[cfg(unix)]
            ui::cli::warning!(
                "Restoration of character device '{}' not supported yet.",
                dst_path.display()
            );
            #[cfg(not(unix))]
            ui::cli::warning!(
                "Character device restoration not supported on this operating system: '{}'",
                dst_path.display()
            );
        }

        NodeType::Fifo => {
            #[cfg(unix)]
            ui::cli::warning!(
                "Restoration of FIFO (named pipe) '{}' not supported yet.",
                dst_path.display()
            );
            #[cfg(not(unix))]
            ui::cli::warning!(
                "FIFO restoration not supported on this operating system: '{}'",
                dst_path.display()
            );
        }

        NodeType::Socket => {
            #[cfg(unix)]
            ui::cli::warning!(
                "Restoration of socket '{}' not supported yet.",
                dst_path.display()
            );
            #[cfg(not(unix))]
            ui::cli::warning!(
                "Socket restoration not supported on this operating system: '{}'",
                dst_path.display()
            );
        }
    }

    Ok(())
}

/// Restores the metadata of a node to the specified destination path.
fn restore_node_metadata(node: &Node, dst_path: &Path) -> Result<()> {
    // Set file times
    restore_times(
        dst_path,
        node.metadata.accessed_time.as_ref(),
        node.metadata.modified_time.as_ref(),
    )?;

    // Unix-specific metadata (mode, uid, gid)
    #[cfg(unix)]
    {
        // Set file permissions (mode)
        if !node.is_symlink()
            && let Some(mode) = node.metadata.mode
        {
            let permissions = Permissions::from_mode(mode);
            if let Err(e) = std::fs::set_permissions(dst_path, permissions) {
                bail!(
                    "Could not set permissions for '{}': {}. This may not be supported for all node types (e.g. symlinks).",
                    dst_path.display(),
                    e.to_string()
                );
            }
        }

        if !node.is_symlink() {
            // Set owner (uid) and group (gid)
            let uid = node.metadata.owner_uid;
            let gid = node.metadata.owner_gid;

            if uid.is_some() || gid.is_some() {
                if let Err(e) = std::os::unix::fs::chown(dst_path, uid, gid) {
                    bail!(
                        "Could not set owner/group for '{}': {}. This operation often requires elevated privileges (e.g., root) and may not be supported for all node types (e.g. symlinks).",
                        dst_path.display(),
                        e.to_string()
                    );
                }
            }
        }
    }

    Ok(())
}

/// Restores file times
pub fn restore_times(
    dst_path: &Path,
    atime: Option<&SystemTime>,
    mtime: Option<&SystemTime>,
) -> Result<()> {
    if let Some(modified_time) = mtime {
        let ft_mtime = FileTime::from(*modified_time);
        let ft_atime = atime.map_or(ft_mtime, |atime| FileTime::from(*atime));

        set_file_times(dst_path, ft_atime, ft_mtime)
            .with_context(|| format!("Could not set file times for '{}'", dst_path.display()))?;
    }

    Ok(())
}

#[cfg(test)]
#[allow(unused_imports)]
mod tests {
    use {
        chrono::{Duration, Local},
        std::time::SystemTime,
    };

    use super::*;

    #[test]
    fn test_restore_mtime() -> Result<()> {
        use std::fs::File;

        use tempfile::tempdir;

        let temp_dir = tempdir()?;
        let temp_path = temp_dir.path();

        let file_path = temp_path.join("file.txt");
        std::fs::write(&file_path, b"Mapachito").expect("Expected to write to file");
        let node = Node::from_path(&file_path)?;

        // Change mtime to 1 day before now
        let prev_mtime: SystemTime = (Local::now() - Duration::days(1)).into();
        let ft_mtime = FileTime::from(prev_mtime);
        let ft_atime = node.metadata.accessed_time.map_or(ft_mtime, FileTime::from);

        set_file_times(&file_path, ft_atime, ft_mtime).with_context(|| {
            format!("Could not set modified time for '{}'", file_path.display())
        })?;

        restore_node_metadata(&node, &file_path)?;

        assert_eq!(
            node.metadata.modified_time.unwrap(),
            file_path.symlink_metadata().unwrap().modified().unwrap()
        );

        Ok(())
    }
}
