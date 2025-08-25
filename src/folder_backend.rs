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
        let file = std::fs::File::open(path)?;
        Ok(Box::new(file))
    }
    fn writer(&self, key: &str) -> Result<impl std::io::Write, Self::Error> {
        let path = self.base_path.join(key);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file = std::fs::File::create(path)?;
        Ok(Box::new(file))
    }
    fn exists(&self, key: &str) -> Result<bool, Self::Error> {
        let path = self.base_path.join(key);
        Ok(path.exists())
    }
}
