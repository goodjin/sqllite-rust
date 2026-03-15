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
}

pub type Result<T> = std::result::Result<T, PagerError>;
