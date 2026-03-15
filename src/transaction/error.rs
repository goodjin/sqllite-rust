use thiserror::Error;

pub type Result<T> = std::result::Result<T, TransactionError>;

#[derive(Error, Debug)]
pub enum TransactionError {
    #[error("Transaction not active")]
    NotActive,

    #[error("Transaction already active")]
    AlreadyActive,

    #[error("Write conflict detected")]
    WriteConflict,

    #[error("Deadlock detected")]
    Deadlock,

    #[error("WAL error: {0}")]
    WalError(String),

    #[error("Pager error: {0}")]
    PagerError(#[from] crate::pager::PagerError),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}
