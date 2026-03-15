use thiserror::Error;

pub type Result<T> = std::result::Result<T, VMError>;

#[derive(Error, Debug)]
pub enum VMError {
    #[error("Invalid opcode: {0}")]
    InvalidOpcode(u8),

    #[error("Stack overflow")]
    StackOverflow,

    #[error("Stack underflow")]
    StackUnderflow,

    #[error("Register index out of bounds: {0}")]
    RegisterOutOfBounds(u8),

    #[error("Execution error: {0}")]
    ExecutionError(String),

    #[error("Storage error: {0}")]
    StorageError(#[from] crate::storage::StorageError),

    #[error("Pager error: {0}")]
    PagerError(#[from] crate::pager::PagerError),
}
