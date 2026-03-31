use thiserror::Error;

#[derive(Error, Debug)]
pub enum TriggerError {
    #[error("Trigger already exists: {0}")]
    AlreadyExists(String),
    
    #[error("Trigger not found: {0}")]
    NotFound(String),
    
    #[error("Invalid trigger timing: {0}")]
    InvalidTiming(String),
    
    #[error("Invalid trigger event: {0}")]
    InvalidEvent(String),
    
    #[error("Trigger execution failed: {0}")]
    ExecutionFailed(String),
    
    #[error("Serialization error: {0}")]
    Serialization(String),
}

pub type Result<T> = std::result::Result<T, TriggerError>;
