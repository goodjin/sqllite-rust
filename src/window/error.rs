use thiserror::Error;

#[derive(Error, Debug)]
pub enum WindowError {
    #[error("Invalid window specification: {0}")]
    InvalidSpec(String),
    
    #[error("Column not found: {0}")]
    ColumnNotFound(String),
    
    #[error("Invalid frame bound: {0}")]
    InvalidFrameBound(String),
    
    #[error("Window function evaluation failed: {0}")]
    EvaluationFailed(String),
}

pub type Result<T> = std::result::Result<T, WindowError>;
