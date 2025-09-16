use log::trace;
use sha2::{Digest, Sha256};
use std::fs::{File, OpenOptions};

use crate::storage_backend::StorageBackend;

fn hash_file_name(key: &str) -> String {
    let hash = Sha256::digest(key);
    base16ct::lower::encode_string(&hash)
}

pub struct FolderBackend {
    base_path: std::path::PathBuf,
}

impl FolderBackend {
    pub fn new(base_path: std::path::PathBuf) -> Self {
        Self { base_path }
    }
}

impl StorageBackend for FolderBackend {
    type Error = std::io::Error;
    fn reader(&self, key: &str) -> Result<impl std::io::Read, Self::Error> {
        let name = hash_file_name(key);
        let path = self.base_path.join(name);
        let file = File::open(path)?;
        file.lock_shared()?;
        Ok(file)
    }
    fn writer(&self, key: &str) -> Result<impl std::io::Write, Self::Error> {
        let name = hash_file_name(key);
        let path = self.base_path.join(name);
        trace!("Writing to path {:?}", path);
        if let Some(parent) = path.parent() {
            trace!("Creating parent directory {:?}", parent);
            std::fs::create_dir_all(parent)?;
        }

        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)?;
        // Get an exclusive lock to make sure no reads are happening
        file.lock()?;
        // Truncate the file
        file.set_len(0)?;

        Ok(Box::new(file))
    }
    fn exists(&self, key: &str) -> Result<bool, Self::Error> {
        let name = hash_file_name(key);
        let path = self.base_path.join(name);
        Ok(path.exists())
    }
}
