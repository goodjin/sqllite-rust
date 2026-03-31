use thiserror::Error;

#[derive(Error, Debug)]
pub enum FtsError {
    #[error("FTS table already exists: {0}")]
    AlreadyExists(String),
    
    #[error("FTS table not found: {0}")]
    NotFound(String),
    
    #[error("Document not found: {0}")]
    DocumentNotFound(u64),
    
    #[error("Invalid query: {0}")]
    InvalidQuery(String),
    
    #[error("Tokenizer error: {0}")]
    TokenizerError(String),
    
    #[error("Column mismatch: expected {expected}, got {actual}")]
    ColumnMismatch { expected: usize, actual: usize },
}

pub type Result<T> = std::result::Result<T, FtsError>;
