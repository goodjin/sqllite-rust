use crate::storage::StorageError;

pub type Result<T> = std::result::Result<T, ExecutorError>;

#[derive(Debug)]
pub enum ExecutorError {
    Storage(StorageError),
    TableNotFound(String),
    ColumnNotFound(String),
    IndexNotFound(String),
    ValueCountMismatch { expected: usize, actual: usize },
    NotImplemented(String),
    InvalidOperation(String),
    ParseError(String),
}

impl std::fmt::Display for ExecutorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExecutorError::Storage(e) => write!(f, "Storage error: {:?}", e),
            ExecutorError::TableNotFound(name) => write!(f, "Table not found: {}", name),
            ExecutorError::ColumnNotFound(name) => write!(f, "Column not found: {}", name),
            ExecutorError::IndexNotFound(name) => write!(f, "Index not found: {}", name),
            ExecutorError::ValueCountMismatch { expected, actual } => write!(f, "Value count mismatch: expected {}, got {}", expected, actual),
            ExecutorError::NotImplemented(feature) => write!(f, "Not implemented: {}", feature),
            ExecutorError::InvalidOperation(msg) => write!(f, "Invalid operation: {}", msg),
            ExecutorError::ParseError(msg) => write!(f, "Parse error: {}", msg),
        }
    }
}

impl std::error::Error for ExecutorError {}

impl From<StorageError> for ExecutorError {
    fn from(err: StorageError) -> Self {
        ExecutorError::Storage(err)
    }
}
