use thiserror::Error;

pub type Result<T> = std::result::Result<T, IndexError>;

#[derive(Error, Debug)]
pub enum IndexError {
    #[error("Key not found")]
    KeyNotFound,

    #[error("Duplicate key")]
    DuplicateKey,

    #[error("Index corruption: {0}")]
    Corruption(String),

    #[error("Storage error: {0}")]
    StorageError(#[from] crate::storage::StorageError),

    #[error("Pager error: {0}")]
    PagerError(#[from] crate::pager::PagerError),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}
