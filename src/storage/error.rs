use thiserror::Error;

pub type Result<T> = std::result::Result<T, StorageError>;

#[derive(Error, Debug)]
pub enum StorageError {
    #[error("Record not found: {0}")]
    RecordNotFound(u64),

    #[error("Record too large: {0} bytes")]
    RecordTooLarge(usize),

    #[error("Invalid record format")]
    InvalidRecordFormat,

    #[error("B-tree error: {0}")]
    BTreeError(String),

    #[error("Key not found")]
    KeyNotFound,

    #[error("Duplicate key")]
    DuplicateKey,

    #[error("Pager error: {0}")]
    PagerError(#[from] crate::pager::PagerError),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Page is full")]
    PageFull,

    #[error("Corrupted data: {0}")]
    Corrupted(String),
}
