//! Error Handling Boundary Tests
//!
//! Tests for error handling edge cases and boundary conditions

use sqllite_rust::storage::StorageError;
use sqllite_rust::sql::error::ParseError;
use sqllite_rust::transaction::TransactionError;
use sqllite_rust::pager::error::PagerError;

// ============================================================================
// Storage Error Tests
// ============================================================================

#[test]
fn test_storage_error_variants() {
    let errors = vec![
        StorageError::Corrupted("test".to_string()),
        StorageError::PageNotFound(1),
        StorageError::InvalidPageSize(100),
    ];
    
    for err in errors {
        let _ = format!("{}", err);
    }
}

#[test]
fn test_storage_error_display() {
    let err = StorageError::PageNotFound(42);
    let msg = format!("{}", err);
    assert!(msg.contains("42"));
}

// ============================================================================
// Parse Error Tests
// ============================================================================

#[test]
fn test_parse_error_variants() {
    let errors = vec![
        ParseError::UnexpectedToken("test".to_string()),
        ParseError::ExpectedSemicolon,
        ParseError::InvalidNumber("abc".to_string()),
        ParseError::UnterminatedString,
        ParseError::UnexpectedEof,
    ];
    
    for err in errors {
        let _ = format!("{}", err);
    }
}

// ============================================================================
// Transaction Error Tests
// ============================================================================

#[test]
fn test_transaction_error_variants() {
    let errors = vec![
        TransactionError::AlreadyActive,
        TransactionError::NotActive,
        TransactionError::WalError("test".to_string()),
        TransactionError::Other("test".to_string()),
    ];
    
    for err in errors {
        let _ = format!("{}", err);
    }
}

// ============================================================================
// Pager Error Tests
// ============================================================================

#[test]
fn test_pager_error_variants() {
    let errors = vec![
        PagerError::InvalidPageNumber(0),
        PagerError::PageOutOfBounds(100, 10),
        PagerError::ChecksumMismatch { expected: 1, actual: 2 },
    ];
    
    for err in errors {
        let _ = format!("{}", err);
    }
}

// ============================================================================
// Error Conversion Tests
// ============================================================================

#[test]
fn test_error_from_io() {
    let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
    let storage_err: StorageError = io_err.into();
    let _ = format!("{}", storage_err);
}
