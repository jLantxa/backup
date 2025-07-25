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

#![cfg(test)]

mod tests {
    use std::path::PathBuf;

    use anyhow::{Context, Result};
    use mapache::{
        commands::{self, GlobalArgs, UseSnapshot, cmd_restore, cmd_snapshot},
        global::{defaults::DEFAULT_DEFAULT_PACK_SIZE_MIB, set_global_opts_with_args},
        restorer::Resolution,
    };

    use tempfile::tempdir;

    use crate::{
        integration_tests::{BACKUP_DATA_PATH, init_repo},
        test_utils::{self},
    };

    #[test]
    fn test_snapshot() -> Result<()> {
        let tmp_dir = tempdir()?;
        let tmp_path = tmp_dir.path();
        let password = "mapachito";
        let password_path = tmp_path.join("password");
        std::fs::write(&password_path, password)?;

        let backup_data_path = test_utils::get_test_data_path(BACKUP_DATA_PATH);
        let backup_data_tmp_path = tmp_path.join("backup");
        test_utils::extract_tar_xz_archive(&backup_data_path, &backup_data_tmp_path)?;

        let repo = String::from("repo");
        let repo_path = tmp_path.join(&repo);

        let global = GlobalArgs {
            repo: repo_path.to_string_lossy().to_string(),
            password_file: Some(password_path),
            key: None,
            quiet: true,
            verbosity: None,
            ssh_pubkey: None,
            ssh_privatekey: None,
            pack_size_mib: DEFAULT_DEFAULT_PACK_SIZE_MIB,
        };
        set_global_opts_with_args(&global);

        // Init repo
        init_repo(password, repo_path.clone())?;

        // Run snapshot
        let snapshot_args = cmd_snapshot::CmdArgs {
            paths: vec![
                backup_data_tmp_path.join("0"),
                backup_data_tmp_path.join("1"),
                backup_data_tmp_path.join("2"),
                backup_data_tmp_path.join("file.txt"),
            ],
            as_root: false,
            exclude: None,
            tags_str: String::new(),
            description: None,
            rescan: false,
            parent: UseSnapshot::Latest,
            read_concurrency: 2,
            write_concurrency: 5,
            dry_run: false,
        };
        commands::cmd_snapshot::run(&global, &snapshot_args)
            .with_context(|| "Failed to run cmd_snapshot")?;

        // Run restore
        let restore_path = tmp_path.join("restore");
        let restore_args = cmd_restore::CmdArgs {
            target: restore_path.clone(),
            snapshot: UseSnapshot::Latest,
            dry_run: false,
            include: None,
            exclude: None,
            strip_prefix: false,
            resolution: mapache::restorer::Resolution::Skip,
            no_verify: false,
        };
        commands::cmd_restore::run(&global, &restore_args)
            .with_context(|| "Failed to run cmd_restore")?;

        let paths = vec![
            PathBuf::from("0"),
            PathBuf::from("0/file0.txt"),
            PathBuf::from("0/00"),
            PathBuf::from("0/00/file00.txt"),
            PathBuf::from("0/01"),
            PathBuf::from("0/l01"),
            PathBuf::from("0/01/file01a.txt"),
            PathBuf::from("0/01/file01b.txt"),
            PathBuf::from("1"),
            PathBuf::from("1/10"),
            PathBuf::from("1/10/file10.txt"),
            PathBuf::from("1/10/lfile10.txt"),
            PathBuf::from("2"),
            PathBuf::from("file.txt"),
        ];

        for path in &paths {
            let backup_path = backup_data_tmp_path.join(path);
            let restored_path = restore_path.join(path);
            assert!(restored_path.exists());

            let restored_meta = restored_path.symlink_metadata()?;
            let backup_meta = backup_path.symlink_metadata()?;

            assert_eq!(restored_meta.len(), backup_meta.len());

            if !restored_path.is_symlink() {
                assert_eq!(restored_meta.modified()?, backup_meta.modified()?);
            }

            if restored_path.is_file() {
                assert_eq!(std::fs::read(&restored_path)?, std::fs::read(&backup_path)?);
            }
        }

        Ok(())
    }

    #[test]
    fn test_snapshot_dry_run() -> Result<()> {
        let tmp_dir = tempdir()?;
        let tmp_path = tmp_dir.path();
        let password = "mapachito";
        let password_path = tmp_path.join("password");
        std::fs::write(&password_path, password)?;

        let backup_data_path = test_utils::get_test_data_path(BACKUP_DATA_PATH);
        let backup_data_tmp_path = tmp_path.join("backup");
        test_utils::extract_tar_xz_archive(&backup_data_path, &backup_data_tmp_path)?;

        let repo = String::from("repo");
        let repo_path = tmp_path.join(&repo);

        let global = GlobalArgs {
            repo: repo_path.to_string_lossy().to_string(),
            password_file: Some(password_path),
            key: None,
            quiet: true,
            verbosity: None,
            ssh_pubkey: None,
            ssh_privatekey: None,
            pack_size_mib: DEFAULT_DEFAULT_PACK_SIZE_MIB,
        };
        set_global_opts_with_args(&global);

        // Init repo
        init_repo(password, repo_path.clone())?;

        // Run snapshot
        let snapshot_args = cmd_snapshot::CmdArgs {
            paths: vec![
                backup_data_tmp_path.join("0"),
                backup_data_tmp_path.join("1"),
                backup_data_tmp_path.join("2"),
                backup_data_tmp_path.join("file.txt"),
            ],
            as_root: false,
            exclude: None,
            tags_str: String::new(),
            description: None,
            rescan: false,
            parent: UseSnapshot::Latest,
            read_concurrency: 2,
            write_concurrency: 5,
            dry_run: true,
        };
        commands::cmd_snapshot::run(&global, &snapshot_args)
            .with_context(|| "Failed to run cmd_snapshot")?;

        // `snapshots` directory should be empty
        let snapshots_read_dir = repo_path.join("snapshots").read_dir()?;
        assert_eq!(0, snapshots_read_dir.into_iter().count());

        // `index` directory should be empty
        let index_read_dir = repo_path.join("index").read_dir()?;
        assert_eq!(0, index_read_dir.into_iter().count());

        // Run restore
        let restore_path = tmp_path.join("restore");
        let restore_args = cmd_restore::CmdArgs {
            target: restore_path.clone(),
            snapshot: UseSnapshot::Latest,
            dry_run: false,
            include: None,
            exclude: None,
            strip_prefix: false,
            resolution: mapache::restorer::Resolution::Skip,
            no_verify: false,
        };

        let restore_result = commands::cmd_restore::run(&global, &restore_args);
        assert!(restore_result.is_err());

        Ok(())
    }

    #[test]
    fn test_snapshot_with_exclude() -> Result<()> {
        let tmp_dir = tempdir()?;
        let tmp_path = tmp_dir.path();
        let password = "mapachito";
        let password_path = tmp_path.join("password");
        std::fs::write(&password_path, password)?;

        let backup_data_path = test_utils::get_test_data_path(BACKUP_DATA_PATH);
        let backup_data_tmp_path = tmp_path.join("backup");
        test_utils::extract_tar_xz_archive(&backup_data_path, &backup_data_tmp_path)?;

        let repo = String::from("repo");
        let repo_path = tmp_path.join(&repo);

        let global = GlobalArgs {
            repo: repo_path.to_string_lossy().to_string(),
            password_file: Some(password_path),
            key: None,
            quiet: true,
            verbosity: None,
            ssh_pubkey: None,
            ssh_privatekey: None,
            pack_size_mib: DEFAULT_DEFAULT_PACK_SIZE_MIB,
        };
        set_global_opts_with_args(&global);

        // Init repo
        init_repo(password, repo_path.clone())?;

        // Run snapshot
        let snapshot_args = cmd_snapshot::CmdArgs {
            paths: vec![
                backup_data_tmp_path.join("0"),
                backup_data_tmp_path.join("1"),
                backup_data_tmp_path.join("2"),
                backup_data_tmp_path.join("file.txt"),
            ],
            as_root: false,
            exclude: Some(vec![backup_data_tmp_path.join("0/01")]),
            tags_str: String::new(),
            description: None,
            rescan: false,
            parent: UseSnapshot::Latest,
            read_concurrency: 2,
            write_concurrency: 5,
            dry_run: false,
        };
        commands::cmd_snapshot::run(&global, &snapshot_args)
            .with_context(|| "Failed to run cmd_snapshot")?;

        // Run restore
        let restore_path = tmp_path.join("restore");
        let restore_args = cmd_restore::CmdArgs {
            target: restore_path.clone(),
            snapshot: UseSnapshot::Latest,
            dry_run: false,
            include: None,
            exclude: None,
            strip_prefix: false,
            resolution: Resolution::Skip,
            no_verify: false,
        };
        commands::cmd_restore::run(&global, &restore_args)
            .with_context(|| "Failed to run cmd_restore")?;

        let paths = vec![
            PathBuf::from("0"),
            PathBuf::from("0/file0.txt"),
            PathBuf::from("0/00"),
            PathBuf::from("0/00/file00.txt"),
            PathBuf::from("1"),
            PathBuf::from("1/10"),
            PathBuf::from("1/10/file10.txt"),
            PathBuf::from("1/10/lfile10.txt"),
            PathBuf::from("2"),
            PathBuf::from("file.txt"),
        ];

        for path in &paths {
            let backup_path = backup_data_tmp_path.join(path);
            let restored_path = restore_path.join(path);
            assert!(restored_path.exists());

            let restored_meta = restored_path.symlink_metadata()?;
            let backup_meta = backup_path.symlink_metadata()?;

            if restored_path.is_file() {
                assert_eq!(std::fs::read(&restored_path)?, std::fs::read(&backup_path)?);
            }

            if !restore_path.is_dir() {
                // Excluded paths decrease the size of parent directories.
                // We only test the size of files in this case
                assert_eq!(restored_meta.len(), backup_meta.len());
            }
            if !restored_path.is_symlink() {
                assert_eq!(restored_meta.modified()?, backup_meta.modified()?);
            }
        }

        Ok(())
    }

    #[test]
    fn test_snapshot_twice() -> Result<()> {
        let tmp_dir = tempdir()?;
        let tmp_path = tmp_dir.path();
        let password = "mapachito";
        let password_path = tmp_path.join("password");
        std::fs::write(&password_path, password)?;

        let backup_data_path = test_utils::get_test_data_path(BACKUP_DATA_PATH);
        let backup_data_tmp_path = tmp_path.join("backup");
        test_utils::extract_tar_xz_archive(&backup_data_path, &backup_data_tmp_path)?;

        let repo = String::from("repo");
        let repo_path = tmp_path.join(&repo);

        let global = GlobalArgs {
            repo: repo_path.to_string_lossy().to_string(),
            password_file: Some(password_path),
            key: None,
            quiet: true,
            verbosity: None,
            ssh_pubkey: None,
            ssh_privatekey: None,
            pack_size_mib: DEFAULT_DEFAULT_PACK_SIZE_MIB,
        };
        set_global_opts_with_args(&global);

        // Init repo
        init_repo(password, repo_path.clone())?;

        // Run snapshot (1st)
        let snapshot_args = cmd_snapshot::CmdArgs {
            paths: vec![
                backup_data_tmp_path.join("0"),
                backup_data_tmp_path.join("1"),
                backup_data_tmp_path.join("2"),
                backup_data_tmp_path.join("file.txt"),
            ],
            as_root: false,
            exclude: Some(vec![backup_data_tmp_path.join("0/01")]),
            tags_str: String::new(),
            description: None,
            rescan: false,
            parent: UseSnapshot::Latest,
            read_concurrency: 2,
            write_concurrency: 5,
            dry_run: false,
        };
        commands::cmd_snapshot::run(&global, &snapshot_args)
            .with_context(|| "Failed to run cmd_snapshot")?;

        let snapshot_args = cmd_snapshot::CmdArgs {
            paths: vec![
                backup_data_tmp_path.join("0"),
                backup_data_tmp_path.join("1"),
                backup_data_tmp_path.join("2"),
                backup_data_tmp_path.join("file.txt"),
            ],
            as_root: false,
            exclude: None,
            tags_str: String::new(),
            description: None,
            rescan: false,
            parent: UseSnapshot::Latest,
            read_concurrency: 2,
            write_concurrency: 5,
            dry_run: false,
        };
        commands::cmd_snapshot::run(&global, &snapshot_args)
            .with_context(|| "Failed to run cmd_snapshot")?;

        let restore_path = tmp_path.join("restore");
        let restore_args = cmd_restore::CmdArgs {
            target: restore_path.clone(),
            snapshot: UseSnapshot::Latest,
            dry_run: false,
            include: None,
            exclude: None,
            strip_prefix: false,
            resolution: mapache::restorer::Resolution::Skip,
            no_verify: false,
        };
        commands::cmd_restore::run(&global, &restore_args)
            .with_context(|| "Failed to run cmd_restore")?;

        let paths = vec![
            PathBuf::from("0"),
            PathBuf::from("0/file0.txt"),
            PathBuf::from("0/00"),
            PathBuf::from("0/00/file00.txt"),
            PathBuf::from("0/01"),
            PathBuf::from("0/01/file01a.txt"),
            PathBuf::from("0/01/file01b.txt"),
            PathBuf::from("1"),
            PathBuf::from("1/10"),
            PathBuf::from("2"),
            PathBuf::from("file.txt"),
        ];

        for path in &paths {
            let backup_path = backup_data_tmp_path.join(path);
            let restored_path = restore_path.join(path);
            assert!(restored_path.exists());

            let restored_meta = restored_path.symlink_metadata()?;
            let backup_meta = backup_path.symlink_metadata()?;

            assert_eq!(restored_meta.len(), backup_meta.len());
            assert_eq!(restored_meta.modified()?, backup_meta.modified()?);

            if restored_path.is_file() {
                assert_eq!(std::fs::read(&restored_path)?, std::fs::read(&backup_path)?);
            }
        }

        Ok(())
    }
}
