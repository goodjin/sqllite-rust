use thiserror::Error;

#[derive(Error, Debug)]
pub enum PagerError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Invalid database format")]
    InvalidFormat,

    #[error("Page not found: {0}")]
    PageNotFound(u32),

    #[error("Permission denied")]
    PermissionDenied,

    #[error("Cache full")]
    CacheFull,

    #[error("WAL error: {0}")]
    WalError(String),

    /// P3-6: Page checksum verification failed
    #[error("Page {page_id} checksum mismatch: stored={stored_checksum:08X}, calculated={calculated_checksum:08X}")]
    CorruptedPage {
        page_id: u32,
        stored_checksum: u32,
        calculated_checksum: u32,
    },

    /// P3-6: General corruption error
    #[error("Corruption detected: {0}")]
    Corrupted(String),
}

impl From<crate::storage::StorageError> for PagerError {
    fn from(err: crate::storage::StorageError) -> Self {
        // Convert StorageError to PagerError without causing cycles
        PagerError::WalError(err.to_string())
    }
}

pub type Result<T> = std::result::Result<T, PagerError>;
