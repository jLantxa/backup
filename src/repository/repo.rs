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
    collections::BTreeSet,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{Context, Result, bail};
use chrono::Utc;
use parking_lot::RwLock;
use zstd::DEFAULT_COMPRESSION_LEVEL;

use crate::{
    backend::StorageBackend,
    global::{
        self, BlobType, FileType, ID, SaveID,
        defaults::{DEFAULT_PACK_SIZE, SHORT_REPO_ID_LEN},
    },
    repository::{
        keys::{generate_key_file, generate_new_master_key, retrieve_master_key},
        packer::{PackSaver, Packer},
        storage::SecureStorage,
    },
    ui::{self, cli},
};

use super::{
    index::{Index, IndexFile, MasterIndex},
    keys,
    manifest::Manifest,
    snapshot::Snapshot,
};

pub const THIS_REPOSITORY_VERSION: u32 = 1;

const OBJECTS_DIR: &str = "objects";
const SNAPSHOTS_DIR: &str = "snapshots";
const INDEX_DIR: &str = "index";
pub(crate) const MANIFEST_PATH: &str = "manifest";
pub(crate) const KEYS_DIR: &str = "keys";

const OBJECTS_DIR_FANOUT: usize = 2;

#[derive(Debug)]
pub struct RepoConfig {
    pub pack_size: u64,
}

impl Default for RepoConfig {
    fn default() -> Self {
        Self {
            pack_size: DEFAULT_PACK_SIZE,
        }
    }
}

pub struct Repository {
    backend: Arc<dyn StorageBackend>,

    objects_path: PathBuf,
    snapshot_path: PathBuf,
    index_path: PathBuf,
    keys_path: PathBuf,

    secure_storage: Arc<SecureStorage>,

    // Packers.
    // By design, we pack blobs and trees separately so we can potentially cache trees
    // separately.
    max_packer_size: u64,
    data_packer: Arc<RwLock<Packer>>,
    tree_packer: Arc<RwLock<Packer>>,
    pack_saver: Arc<RwLock<Option<PackSaver>>>,

    index: Arc<RwLock<MasterIndex>>,
}

impl Repository {
    /// Create and initialize a new repository
    pub fn init(
        password: Option<String>,
        keyfile_path: Option<&PathBuf>,
        backend: Arc<dyn StorageBackend>,
    ) -> Result<()> {
        let timestamp = Utc::now();

        let pass = match password {
            Some(p) => p,
            None => ui::cli::request_password_with_confirmation(
                "Enter new password for repository",
                "Confirm password",
                "Passwords don't match",
            ),
        };

        // Create the repository root
        if backend.root_exists() {
            bail!("Could not initialize a repository because a directory already exists");
        }

        backend
            .create()
            .with_context(|| "Could not create root directory")?;

        let keys_path = PathBuf::from(KEYS_DIR);
        backend.create_dir(&keys_path)?;

        // Create new key
        let master_key = generate_new_master_key();
        let keyfile = generate_key_file(&pass, master_key.clone())
            .with_context(|| "Could not generate key")?;
        let secure_storage = Arc::new(
            SecureStorage::build()
                .with_compression(DEFAULT_COMPRESSION_LEVEL)
                .with_key(master_key),
        );

        let keyfile_json = serde_json::to_string_pretty(&keyfile)?;
        let keyfile_json =
            SecureStorage::compress(keyfile_json.as_bytes(), DEFAULT_COMPRESSION_LEVEL)?;
        let keyfile_id = ID::from_content(&keyfile_json);
        match keyfile_path {
            Some(p) => {
                std::fs::write(p, &keyfile_json)?;
            }
            None => {
                let p = keys_path.join(keyfile_id.to_hex());

                backend.write(&p, &keyfile_json)?;
            }
        }

        let repo_id = ID::new_random();

        // Init repository structure
        let objects_path = PathBuf::from(OBJECTS_DIR);
        let snapshot_path = PathBuf::from(SNAPSHOTS_DIR);
        let index_path = PathBuf::from(INDEX_DIR);

        // Save new manifest
        let manifest = Manifest {
            version: THIS_REPOSITORY_VERSION,
            id: repo_id.clone(),
            created_time: timestamp,
        };

        let manifest_path = Path::new(MANIFEST_PATH);
        let manifest = serde_json::to_string_pretty(&manifest)?;
        let manifest = secure_storage.encode(manifest.as_bytes())?;
        backend.write(manifest_path, &manifest)?;

        backend.create_dir(&objects_path)?;
        let num_folders: usize = 1 << (4 * OBJECTS_DIR_FANOUT);
        for n in 0x00..num_folders {
            backend.create_dir(&objects_path.join(format!("{n:0>OBJECTS_DIR_FANOUT$x}")))?;
        }

        backend.create_dir(&snapshot_path)?;
        backend.create_dir(&index_path)?;

        ui::cli::log!(
            "Created repo with id {}",
            repo_id.to_short_hex(SHORT_REPO_ID_LEN)
        );

        Ok(())
    }

    /// Try to open a repository.
    /// This function prompts for a password to retrieve a master key.
    pub fn try_open(
        mut password: Option<String>,
        key_file_path: Option<&PathBuf>,
        backend: Arc<dyn StorageBackend>,
        config: RepoConfig,
    ) -> Result<(Arc<Repository>, Arc<SecureStorage>)> {
        if !backend.root_exists() {
            bail!("Could not open a repository. The path does not exist.");
        }

        const MAX_PASSWORD_RETRIES: u32 = 3;
        let mut password_try_count = 0;

        let master_key = {
            if let Some(p) = password.take() {
                retrieve_master_key(&p, key_file_path, backend.clone())
                    .with_context(|| "Incorrect password.")?
            } else {
                loop {
                    let pass_from_console = ui::cli::request_password("Enter repository password");

                    if let Ok(key) =
                        retrieve_master_key(&pass_from_console, key_file_path, backend.clone())
                    {
                        break key;
                    } else {
                        password_try_count += 1;
                        if password_try_count < MAX_PASSWORD_RETRIES {
                            ui::cli::log!("Incorrect password. Try again.");
                            continue;
                        } else {
                            bail!("Wrong password or no KeyFile found.");
                        }
                    }
                }
            }
        };

        let secure_storage = Arc::new(
            SecureStorage::build()
                .with_compression(DEFAULT_COMPRESSION_LEVEL)
                .with_key(master_key),
        );

        let manifest_path = Path::new(MANIFEST_PATH);

        let manifest = backend
            .read(manifest_path)
            .with_context(|| "Could not load manifest file")?;
        let manifest = secure_storage
            .decode(&manifest)
            .with_context(|| "Could not decode the manifest file")?;
        let manifest: Manifest = serde_json::from_slice(&manifest)?;

        let version = manifest.version;

        if version == 1 {
            let repo = Repository::open(backend, secure_storage.clone(), config)?;
            Ok((repo, secure_storage))
        } else {
            bail!("Invalid repository version \'{}\'", version);
        }
    }

    /// Open an existing repository from a directory
    fn open(
        backend: Arc<dyn StorageBackend>,
        secure_storage: Arc<SecureStorage>,
        config: RepoConfig,
    ) -> Result<Arc<Self>> {
        let objects_path = PathBuf::from(OBJECTS_DIR);
        let snapshot_path = PathBuf::from(SNAPSHOTS_DIR);
        let index_path = PathBuf::from(INDEX_DIR);

        let data_packer = Arc::new(RwLock::new(Packer::new()));
        let tree_packer = Arc::new(RwLock::new(Packer::new()));

        let index = Arc::new(RwLock::new(MasterIndex::new()));

        let mut repo = Repository {
            backend,
            objects_path,
            snapshot_path,
            index_path,
            keys_path: PathBuf::from(KEYS_DIR),
            secure_storage,
            max_packer_size: config.pack_size,
            data_packer,
            tree_packer,
            pack_saver: Arc::new(RwLock::new(None)),
            index,
        };

        repo.load_master_index()?;

        Ok(Arc::new(repo))
    }

    /// Encodes and saves a blob in the repository. This blob can be packed with other blobs in an pack file.
    /// Returns a tuple (`ID`, (raw_data_size, encoded_data_size), (raw_meta_size, encoded_meta_size))
    #[allow(clippy::type_complexity)]
    pub fn encode_and_save_blob(
        &self,
        blob_type: BlobType,
        data: Vec<u8>,
        save_id: SaveID,
    ) -> Result<(ID, (u64, u64), (u64, u64))> {
        let packer = match blob_type {
            BlobType::Data => &self.data_packer,
            BlobType::Tree => &self.tree_packer,
            BlobType::Padding => panic!("Internal error: blob type not allowed"),
        };

        // The ID of a blob is the hash of its plaintext content.
        // It has to be like that because the encoding appends a random 12-byte
        // Nonce which would change the ID every time, ruining the deduplication.
        let id = match save_id {
            SaveID::CalculateID => ID::from_content(&data),
            SaveID::WithID(id) => id,
        };

        let mut index_wlock = self.index.write();
        let blob_exists = index_wlock.contains(&id) || !index_wlock.add_pending_blob(id.clone());
        drop(index_wlock);

        // If the blob was already pending, return early, as we are finished here.
        if blob_exists {
            return Ok((id, (0, 0), (0, 0)));
        }

        let raw_length = data.len() as u64;
        let data = self.secure_storage.encode(&data)?;
        let encoded_length = data.len() as u64;

        packer
            .write()
            .add_blob(id.clone(), blob_type, data, raw_length, encoded_length);

        // Flush if the packer is considered full
        let packer_meta_size = if packer.read().size() > self.max_packer_size {
            self.flush_packer(packer)?
        } else {
            (0, 0)
        };

        Ok((id, (raw_length, encoded_length), packer_meta_size))
    }

    /// Loads a blob from the repository.
    pub fn load_blob(&self, id: &ID) -> Result<Vec<u8>> {
        let blob_entry = self.index.read().get(id);
        match blob_entry {
            Some((pack_id, _blob_type, offset, length, _raw_length)) => {
                self.load_from_pack(&pack_id, offset, length)
            }
            None => bail!("Could not find blob {:?} in index", id),
        }
    }

    /// Saves a file to the repository
    pub fn save_file(&self, file_type: FileType, data: &[u8]) -> Result<(ID, u64, u64)> {
        assert_ne!(file_type, FileType::Key);
        assert_ne!(file_type, FileType::Manifest);

        let raw_size = data.len() as u64;
        let data = self.secure_storage.encode(data)?;
        let encoded_size = data.len() as u64;
        let id = ID::from_content(&data);
        let path = self.get_path(file_type, &id);
        self.save_with_rename(&path, &data)?;

        Ok((id, raw_size, encoded_size))
    }

    /// Loads a file to the repository
    pub fn load_file(&self, file_type: FileType, id: &ID) -> Result<Vec<u8>> {
        assert_ne!(file_type, FileType::Key);
        assert_ne!(file_type, FileType::Manifest);

        let path = self.get_path(file_type, id);
        let data = self.backend.read(&path)?;

        if file_type != FileType::Pack {
            return self.secure_storage.decode(&data);
        }

        Ok(data)
    }

    /// Deletes a file from the repository
    pub fn delete_file(&self, file_type: FileType, id: &ID) -> Result<u64> {
        assert_ne!(file_type, FileType::Key);
        assert_ne!(file_type, FileType::Manifest);

        let path = self.get_path(file_type, id);
        let size = self.backend.lstat(&path)?.size;
        self.backend.remove_file(&path)?;

        Ok(size.unwrap_or(0))
    }

    /// Removes a snapshot from the repository, if it exists.
    pub fn remove_snapshot(&self, id: &ID) -> Result<()> {
        let snapshot_path = self.snapshot_path.join(id.to_hex());

        if !self.backend.exists(&snapshot_path) {
            bail!("Snapshot {} doesn't exist", id)
        }

        self.backend
            .remove_file(&snapshot_path)
            .with_context(|| format!("Could not remove snapshot {id}"))
    }

    /// Loads a snapshot by ID
    pub fn load_snapshot(&self, id: &ID) -> Result<Snapshot> {
        let snapshot = self
            .load_file(FileType::Snapshot, id)
            .with_context(|| format!("No snapshot with ID \'{id}\' exists"))?;
        let snapshot: Snapshot = serde_json::from_slice(&snapshot)?;
        Ok(snapshot)
    }

    /// Lists all snapshot IDs
    pub fn list_snapshot_ids(&self) -> Result<Vec<ID>> {
        let mut ids = Vec::new();

        let paths = self
            .backend
            .read_dir(&self.snapshot_path)
            .with_context(|| "Could not read snapshots")?;

        for path in paths {
            if self.backend.is_file(&path) {
                if let Some(file_name) = path.file_name().and_then(|s| s.to_str()) {
                    ids.push(ID::from_hex(file_name)?);
                }
            }
        }

        Ok(ids)
    }

    /// Flushes all pending data and saves it.
    /// Returns a tuple (raw_size, encoded_size)
    pub fn flush(&self) -> Result<(u64, u64)> {
        let data_packer_meta_size = self.flush_packer(&self.data_packer)?;
        let tree_packer_meta_size = self.flush_packer(&self.tree_packer)?;

        let (index_raw_size, index_encoded_size) = self.index.write().save(self)?;

        Ok((
            data_packer_meta_size.1 + tree_packer_meta_size.0 + index_raw_size,
            data_packer_meta_size.1 + tree_packer_meta_size.1 + index_encoded_size,
        ))
    }

    /// Loads a pack.
    pub fn load_object(&self, id: &ID) -> Result<Vec<u8>> {
        self.load_file(FileType::Pack, id)
    }

    /// Loads an index file.
    pub fn load_index(&self, id: &ID) -> Result<IndexFile> {
        let index: Vec<u8> = self
            .load_file(FileType::Index, id)
            .with_context(|| format!("Could not load index {}", id.to_hex()))?;
        let index = serde_json::from_slice(&index)?;
        Ok(index)
    }

    /// Loads the repository manifest.
    pub fn load_manifest(&self) -> Result<Manifest> {
        let manifest = self.backend.read(Path::new(MANIFEST_PATH))?;
        let manifest = self.secure_storage.decode(&manifest)?;
        let manifest = serde_json::from_slice(&manifest)?;
        Ok(manifest)
    }

    /// Loads a KeyFile.
    pub fn load_key(&self, id: &ID) -> Result<keys::KeyFile> {
        let key_path = self.keys_path.join(id.to_hex());
        let key = self.backend.read(&key_path)?;
        let key = SecureStorage::decompress(&key)?;
        let key = serde_json::from_slice(&key)?;
        Ok(key)
    }

    /// Finds a file in the repository using an ID prefix
    pub fn find(&self, file_type: FileType, prefix: &str) -> Result<(ID, PathBuf)> {
        if prefix.len() > 2 * global::ID_LENGTH {
            // A hex string has 2 characters per byte.
            bail!(
                "Invalid prefix length. The prefix must not be longer than the ID ({} chars)",
                2 * global::ID_LENGTH
            );
        } else if prefix.is_empty() {
            // Although it is technically posible to use an empty prefix, which would find a match
            // if only one file of the type exists. let's consider this invalid as it can be
            // potentially ambiguous or lead to errors.
            bail!("Prefix cannot be empty");
        }

        let type_files = self.list_files(file_type)?;
        let mut matches = Vec::new();

        for file_path in type_files {
            let filename = match file_path.file_name() {
                Some(os_str) => os_str.to_string_lossy().into_owned(),
                None => bail!("Failed to list file for type {}", file_type),
            };

            if !filename.starts_with(prefix) {
                continue;
            }

            if matches.is_empty() {
                matches.push((filename, file_path));
            } else {
                bail!("Prefix {} is ambiguous", prefix);
            }
        }

        if matches.is_empty() {
            bail!(
                "File type {} with prefix {} doesn't exist",
                file_type,
                prefix
            );
        }

        let (filename, filepath) = matches.pop().unwrap();
        let id = ID::from_hex(&filename)?;

        Ok((id, filepath))
    }

    pub fn init_pack_saver(&self, concurrency: usize) {
        let backend = self.backend.clone();
        let objects_path = self.objects_path.clone();

        let pack_saver = PackSaver::new(
            concurrency,
            Arc::new(move |data, id| {
                let path = Self::get_object_path(&objects_path, &id);
                if let Err(e) = backend.write(&path, &data) {
                    cli::error!("Could not save pack {}: {}", id.to_hex(), e);
                }
            }),
        );
        self.pack_saver.write().replace(pack_saver);
    }

    pub fn finalize_pack_saver(&self) {
        if let Some(pack_saver) = self.pack_saver.write().take() {
            pack_saver.finish();
        }
    }

    pub fn index(&self) -> Arc<RwLock<MasterIndex>> {
        self.index.clone()
    }

    /// Reads from a repository file with offset and length.
    /// This function does not decode the data.
    pub fn read_from_file(
        &self,
        file_type: FileType,
        id: &ID,
        offset: u64,
        length: u64,
    ) -> Result<Vec<u8>> {
        assert_ne!(file_type, FileType::Key);
        assert_ne!(file_type, FileType::Manifest);

        let path = self.get_path(file_type, id);
        self.backend.seek_read(&path, offset, length)
    }

    /// Reads from a repository file with offset and length.
    /// This function decodes the data.
    pub fn read_from_file_and_decode(
        &self,
        file_type: FileType,
        id: &ID,
        offset: u64,
        length: u64,
    ) -> Result<Vec<u8>> {
        let data = self.read_from_file(file_type, id, offset, length)?;
        self.secure_storage.decode(&data)
    }

    /// Lists all packs in the repository.
    pub fn list_objects(&self) -> Result<BTreeSet<ID>> {
        let mut list = BTreeSet::new();

        let num_folders: usize = 1 << (4 * OBJECTS_DIR_FANOUT);
        for n in 0..num_folders {
            let dir = self
                .objects_path
                .join(format!("{n:0>OBJECTS_DIR_FANOUT$x}"));

            let files = self.backend.read_dir(&dir)?;
            for path in files {
                let filename = path.file_name().unwrap().to_string_lossy().to_string();
                if let Ok(id) = ID::from_hex(&filename) {
                    list.insert(id);
                }
            }
        }

        Ok(list)
    }

    /// Returns the path to an object with a given hash in the repository.
    fn get_object_path(objects_path: &Path, id: &ID) -> PathBuf {
        let id_hex = id.to_hex();
        objects_path
            .join(&id_hex[..OBJECTS_DIR_FANOUT])
            .join(&id_hex)
    }

    pub fn get_path(&self, file_type: FileType, id: &ID) -> PathBuf {
        let id_hex = id.to_hex();
        match file_type {
            FileType::Pack => Self::get_object_path(&self.objects_path, id),
            FileType::Snapshot => self.snapshot_path.join(id_hex),
            FileType::Index => self.index_path.join(id_hex),
            FileType::Key => self.keys_path.join(id_hex),
            FileType::Manifest => PathBuf::from(MANIFEST_PATH),
        }
    }

    /// Lists all paths belonging to a file type (objects, snapshots, indices, etc.).
    pub fn list_files(&self, file_type: FileType) -> Result<Vec<PathBuf>> {
        match file_type {
            FileType::Snapshot => self.backend.read_dir(&self.snapshot_path),
            FileType::Key => self.backend.read_dir(&self.keys_path),
            FileType::Index => self.backend.read_dir(&self.index_path),
            FileType::Manifest => Ok(vec![PathBuf::from(MANIFEST_PATH)]),
            FileType::Pack => {
                let mut files = Vec::new();
                for n in 0x00..(1 << (4 * OBJECTS_DIR_FANOUT)) {
                    let dir_name = self
                        .objects_path
                        .join(format!("{n:0>OBJECTS_DIR_FANOUT$x}"));

                    let sub_files = self.backend.read_dir(&dir_name)?;
                    for file_path in sub_files.into_iter() {
                        files.push(file_path);
                    }
                }

                Ok(files)
            }
        }
    }

    fn save_with_rename(&self, path: &Path, data: &[u8]) -> Result<usize> {
        let tmp_path = path.with_extension("tmp");
        self.backend.write(&tmp_path, data)?;
        self.backend.rename(&tmp_path, path)?;
        Ok(data.len())
    }

    fn flush_packer(&self, packer: &Arc<RwLock<Packer>>) -> Result<(u64, u64)> {
        match packer.write().flush(&self.secure_storage)? {
            None => Ok((0, 0)),
            Some(flushed_pack) => {
                if let Some(pack_saver) = self.pack_saver.write().as_ref() {
                    pack_saver
                        .save_pack(flushed_pack.data, SaveID::WithID(flushed_pack.id.clone()))?;
                } else {
                    bail!("PackSaver is not initialized. Call `init_pack_saver` first.");
                }

                let (index_raw, index_encoded) = self.index.write().add_pack(
                    self,
                    &flushed_pack.id,
                    flushed_pack.descriptors,
                )?;

                Ok((
                    flushed_pack.meta_size + index_raw,
                    flushed_pack.meta_size + index_encoded,
                ))
            }
        }
    }

    fn load_master_index(&mut self) -> Result<()> {
        let files = self.backend.read_dir(&self.index_path)?;
        let num_index_files = files.len();

        for file in files {
            let file_name = file
                .file_name()
                .expect("Could not read index file name")
                .to_string_lossy()
                .clone();
            let id = ID::from_hex(&file_name)?;
            let index_file = self.backend.read(&file)?;
            let index_file = self.secure_storage.decode(&index_file)?;
            let index_file = match serde_json::from_slice(&index_file) {
                Ok(idx_file) => idx_file,
                Err(e) => bail!("Failed to load index file {}: {}", id.to_short_hex(4), e),
            };

            let mut index = Index::from_index_file(index_file);
            index.finalize();
            index.set_id(id);

            self.index.write().add_index(index);
        }

        ui::cli::verbose_1!("Loaded {} index files", num_index_files);

        Ok(())
    }

    pub fn load_from_pack(&self, id: &ID, offset: u32, length: u32) -> Result<Vec<u8>> {
        let object_path = Self::get_object_path(&self.objects_path, id);
        let data = self
            .backend
            .seek_read(&object_path, offset as u64, length as u64)?;
        self.secure_storage.decode(&data)
    }
}

impl Drop for Repository {
    fn drop(&mut self) {
        let _ = self.flush();
    }
}

#[cfg(test)]
mod tests {
    use base64::{Engine, engine::general_purpose};
    use tempfile::tempdir;

    use crate::{backend::localfs::LocalFS, utils};

    use super::*;

    /// Test init a repo with password and open it
    #[test]
    fn test_init_and_open_with_password() -> Result<()> {
        let temp_repo_dir = tempdir()?;
        let temp_repo_path = temp_repo_dir.path().join("repo");

        let password = Some(String::from("mapachito"));
        let backend = Arc::new(LocalFS::new(temp_repo_path.to_owned()));

        Repository::init(password.clone(), None, backend.to_owned())?;
        Repository::try_open(password, None, backend, RepoConfig::default())?;

        Ok(())
    }

    /// Test init a repo with password and open it using a password stored in a file
    #[test]
    fn test_init_and_open_with_password_from_file() -> Result<()> {
        let temp_dir = tempdir()?;
        let temp_path = temp_dir.path();
        let temp_repo_path = temp_path.join("repo");
        let password_file_path = temp_path.join("repo_password");

        // Write password to file
        std::fs::write(&password_file_path, "mapachito")?;

        let password = utils::get_password_from_file(&Some(password_file_path))?;
        let backend = Arc::new(LocalFS::new(temp_repo_path.to_owned()));

        Repository::init(password.clone(), None, backend.to_owned())?;
        Repository::try_open(password, None, backend, RepoConfig::default())?;

        Ok(())
    }

    /// Test generation of master keys
    #[test]
    fn test_generate_key_file() -> Result<()> {
        let master_key = generate_new_master_key();
        let keyfile = generate_key_file("mapachito", master_key.clone())?;

        let salt = general_purpose::STANDARD.decode(keyfile.salt)?;
        let encrypted_key = general_purpose::STANDARD.decode(keyfile.encrypted_key)?;

        let intermediate_key = SecureStorage::derive_key("mapachito", &salt);
        let decrypted_key = SecureStorage::decrypt_with_key(&intermediate_key, &encrypted_key)?;

        assert_eq!(master_key, decrypted_key.as_slice());

        Ok(())
    }
}
