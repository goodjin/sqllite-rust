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
}

impl From<crate::storage::StorageError> for PagerError {
    fn from(err: crate::storage::StorageError) -> Self {
        // Convert StorageError to PagerError without causing cycles
        PagerError::WalError(err.to_string())
    }
}

pub type Result<T> = std::result::Result<T, PagerError>;
