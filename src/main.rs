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

use std::path::Path;

use backup::Repo;

mod backup;
mod io;
mod storage;
mod utils;

enum Action {
    CreateNew,
    Backup,
    RestoreLast,
    Restore(String),
    List,
}
fn main() {
    let args: Vec<String> = std::env::args().collect();

    let action = match args[1].as_str() {
        "new" => Action::CreateNew,
        "delta" => Action::Backup,
        "restorelast" => Action::RestoreLast,
        "restoren" => Action::Restore(args[5].clone()),
        "list" => Action::List,
        _ => panic!("Unexpected action"),
    };

    let repo_path = Path::new(&args[2]).to_owned();
    let src_path = Path::new(&args[3]).to_owned();
    let restore_path = Path::new(&args[4]).to_owned();
    let password = &args[6];

    match action {
        Action::CreateNew => {
            let mut repo = Repo::new(&repo_path, &password).unwrap();
            repo.backup(&src_path).unwrap();
        }
        Action::Backup => {
            let mut repo = Repo::from_existing(&repo_path, &password).unwrap();
            repo.backup(&src_path).unwrap();
        }
        Action::RestoreLast => {
            let repo = Repo::from_existing(&repo_path, &password).unwrap();
            repo.restore_last_snapshot(&restore_path).unwrap();
        }
        Action::Restore(id) => {
            let repo = Repo::from_existing(&repo_path, &password).unwrap();
            repo.restore_snapshot(&id, &restore_path).unwrap();
        }
        Action::List => {
            let repo = Repo::from_existing(&repo_path, &password).unwrap();
            let snapshots = repo.list_snapshots();
        }
    }
}
