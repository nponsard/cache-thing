use log::trace;
use std::fs::{File, OpenOptions};

use crate::storage_backend::StorageBackend;

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
        let path = self.base_path.join(key);
        let file = File::open(path)?;
        file.lock_shared()?;
        Ok(file)
    }
    fn writer(&self, key: &str) -> Result<impl std::io::Write, Self::Error> {
        let path = self.base_path.join(key);
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
        let path = self.base_path.join(key);
        Ok(path.exists())
    }
}
