// [backup] is an incremental backup tool
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

use std::{path::Path, sync::Arc};

use anyhow::Result;
use clap::Args;

use crate::{
    cli::{self, GlobalArgs},
    repository::{self},
    storage_backend::localfs::LocalFS,
};

#[derive(Args, Debug)]
pub struct CmdArgs {}

pub fn run(global: &GlobalArgs, _args: &CmdArgs) -> Result<()> {
    let password = cli::request_password();
    let repo_path = Path::new(&global.repo);

    let backend = Arc::new(LocalFS::new());

    let repo = repository::backend::open(backend, &repo_path, password)?;

    let _snapshots = repo.get_snapshots_sorted()?;

    todo!()
}
