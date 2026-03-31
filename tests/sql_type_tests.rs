//! SQL Type Boundary Tests
//!
//! Tests for SQL type edge cases and boundary conditions

use sqllite_rust::sql::ast::DataType;

// ============================================================================
// Data Type Tests
// ============================================================================

#[test]
fn test_integer_type() {
    let types = vec![
        DataType::Integer,
        DataType::Int,
        DataType::SmallInt,
        DataType::BigInt,
    ];
    
    for _ in types {
        // Just verify they exist
    }
}

#[test]
fn test_real_type() {
    let types = vec![
        DataType::Real,
        DataType::Float,
        DataType::Double,
        DataType::Decimal(10, 2),
        DataType::Decimal(38, 10),
    ];
    
    for _ in types {
        // Just verify they exist
    }
}

#[test]
fn test_text_type() {
    let types = vec![
        DataType::Text,
        DataType::Char(1),
        DataType::Char(255),
        DataType::VarChar(100),
        DataType::VarChar(65535),
    ];
    
    for _ in types {
        // Just verify they exist
    }
}

#[test]
fn test_blob_type() {
    let types = vec![
        DataType::Blob,
        DataType::Binary(100),
        DataType::VarBinary(1000),
    ];
    
    for _ in types {
        // Just verify they exist
    }
}

#[test]
fn test_datetime_type() {
    let types = vec![
        DataType::Date,
        DataType::Time,
        DataType::DateTime,
        DataType::Timestamp,
    ];
    
    for _ in types {
        // Just verify they exist
    }
}

#[test]
fn test_other_types() {
    let types = vec![
        DataType::Boolean,
        DataType::Numeric,
        DataType::Null,
    ];
    
    for _ in types {
        // Just verify they exist
    }
}

// ============================================================================
// Type Value Tests
// ============================================================================

use sqllite_rust::sql::ast::Value;

#[test]
fn test_null_value() {
    let val = Value::Null;
    let _ = val;
}

#[test]
fn test_integer_values() {
    let values = vec![
        Value::Integer(0),
        Value::Integer(1),
        Value::Integer(-1),
        Value::Integer(i64::MAX),
        Value::Integer(i64::MIN),
    ];
    
    for val in values {
        let _ = val;
    }
}

#[test]
fn test_real_values() {
    let values = vec![
        Value::Real(0.0),
        Value::Real(1.0),
        Value::Real(-1.0),
        Value::Real(f64::MAX),
        Value::Real(f64::MIN),
        Value::Real(f64::NAN),
        Value::Real(f64::INFINITY),
    ];
    
    for val in values {
        let _ = val;
    }
}

#[test]
fn test_text_values() {
    let values = vec![
        Value::Text("".to_string()),
        Value::Text("a".to_string()),
        Value::Text("hello".to_string()),
        Value::Text("a".repeat(1000)),
    ];
    
    for val in values {
        let _ = val;
    }
}

#[test]
fn test_blob_values() {
    let values = vec![
        Value::Blob(vec![]),
        Value::Blob(vec![0]),
        Value::Blob(vec![0; 100]),
        Value::Blob(vec![255; 1000]),
    ];
    
    for val in values {
        let _ = val;
    }
}

#[test]
fn test_boolean_values() {
    let values = vec![
        Value::Boolean(true),
        Value::Boolean(false),
    ];
    
    for val in values {
        let _ = val;
    }
}
