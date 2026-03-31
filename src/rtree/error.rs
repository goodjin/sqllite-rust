use thiserror::Error;

#[derive(Error, Debug)]
pub enum RtreeError {
    #[error("R-Tree index already exists: {0}")]
    AlreadyExists(String),
    
    #[error("R-Tree index not found: {0}")]
    NotFound(String),
    
    #[error("Object not found: {0}")]
    ObjectNotFound(u64),
    
    #[error("Invalid bounding box: {0}")]
    InvalidBoundingBox(String),
    
    #[error("Invalid node: {0}")]
    InvalidNode(u64),
    
    #[error("Node overflow")]
    NodeOverflow,
    
    #[error("Node underflow")]
    NodeUnderflow,
}

pub type Result<T> = std::result::Result<T, RtreeError>;
