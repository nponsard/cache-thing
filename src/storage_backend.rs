pub trait StorageBackend {
    type Error: std::error::Error + Send + Sync;
    fn writer(&self, key: &str) -> Result<impl std::io::Write, Self::Error>;
    fn reader(&self, key: &str) -> Result<impl std::io::Read, Self::Error>;
}
