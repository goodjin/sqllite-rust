use thiserror::Error;

#[derive(Error, Debug, Clone, PartialEq)]
pub enum ParseError {
    #[error("Unexpected token: {0:?}")]
    UnexpectedToken(String),

    #[error("Expected token {expected}, found {found}")]
    ExpectedToken { expected: String, found: String },

    #[error("Expected identifier")]
    ExpectedIdentifier,

    #[error("Expected semicolon")]
    ExpectedSemicolon,

    #[error("Invalid number: {0}")]
    InvalidNumber(String),

    #[error("Unterminated string")]
    UnterminatedString,

    #[error("Empty input")]
    EmptyInput,
}

pub type Result<T> = std::result::Result<T, ParseError>;
