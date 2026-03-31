use thiserror::Error;

#[derive(Error, Debug)]
pub enum JsonError {
    #[error("Invalid JSON syntax: {0}")]
    InvalidSyntax(String),
    
    #[error("Unexpected end of input")]
    UnexpectedEof,
    
    #[error("Expected character '{expected}' at position {position}")]
    ExpectedChar { expected: char, position: usize },
    
    #[error("Invalid number: {0}")]
    InvalidNumber(String),
    
    #[error("Invalid escape sequence")]
    InvalidEscape,
    
    #[error("Invalid UTF-8 sequence")]
    InvalidUtf8,
    
    #[error("Path not found: {0}")]
    PathNotFound(String),
}

pub type Result<T> = std::result::Result<T, JsonError>;
