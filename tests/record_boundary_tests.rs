//! Record Boundary Tests
//!
//! Tests for record format edge cases and boundary conditions

use sqllite_rust::storage::record::{Record, Value};

// ============================================================================
// Empty Record Tests
// ============================================================================

#[test]
fn test_empty_record() {
    let record = Record::new(vec![]);
    assert!(record.columns().is_empty());
    
    let serialized = record.serialize();
    let deserialized = Record::deserialize(&serialized).unwrap();
    assert!(deserialized.columns().is_empty());
}

// ============================================================================
// Null Value Tests
// ============================================================================

#[test]
fn test_all_null_record() {
    let record = Record::new(vec![
        Value::Null,
        Value::Null,
        Value::Null,
    ]);
    
    assert_eq!(record.columns().len(), 3);
    
    let serialized = record.serialize();
    let deserialized = Record::deserialize(&serialized).unwrap();
    assert_eq!(deserialized.columns().len(), 3);
}

#[test]
fn test_mixed_null_values() {
    let record = Record::new(vec![
        Value::Null,
        Value::Integer(1),
        Value::Null,
        Value::Text("test".to_string()),
        Value::Null,
    ]);
    
    assert_eq!(record.columns().len(), 5);
}

// ============================================================================
// Integer Boundary Tests
// ============================================================================

#[test]
fn test_integer_boundaries() {
    let values = vec![
        i64::MIN,
        i64::MIN + 1,
        -1000,
        -1,
        0,
        1,
        1000,
        i64::MAX - 1,
        i64::MAX,
    ];
    
    for value in values {
        let record = Record::new(vec![Value::Integer(value)]);
        let serialized = record.serialize();
        let deserialized = Record::deserialize(&serialized).unwrap();
        
        assert_eq!(deserialized.columns().len(), 1);
    }
}

// ============================================================================
// Real Number Boundary Tests
// ============================================================================

#[test]
fn test_real_boundaries() {
    let values = vec![
        f64::NEG_INFINITY,
        f64::MIN,
        -1000.0,
        -1.0,
        -0.0,
        0.0,
        1.0,
        1000.0,
        f64::MAX,
        f64::INFINITY,
        f64::NAN,
        f64::MIN_POSITIVE,
        f64::EPSILON,
    ];
    
    for value in values {
        let record = Record::new(vec![Value::Real(value)]);
        let serialized = record.serialize();
        let deserialized = Record::deserialize(&serialized).unwrap();
        
        assert_eq!(deserialized.columns().len(), 1);
    }
}

// ============================================================================
// Text Boundary Tests
// ============================================================================

#[test]
fn test_text_boundaries() {
    let strings = vec![
        "".to_string(),
        "a".to_string(),
        " ".to_string(),
        "hello".to_string(),
        "hello world".to_string(),
        "a".repeat(100),
        "a".repeat(1000),
        "a".repeat(10000),
        "special chars: !@#$%^&*()".to_string(),
        "unicode: 你好世界 🎉".to_string(),
        "new\nline\ttab".to_string(),
        "null\0char".to_string(),
    ];
    
    for s in strings {
        let record = Record::new(vec![Value::Text(s.clone())]);
        let serialized = record.serialize();
        let deserialized = Record::deserialize(&serialized).unwrap();
        
        assert_eq!(deserialized.columns().len(), 1);
    }
}

// ============================================================================
// Blob Boundary Tests
// ============================================================================

#[test]
fn test_blob_boundaries() {
    let blobs = vec![
        vec![],
        vec![0],
        vec![255],
        vec![0, 1, 2, 3],
        vec![0; 100],
        vec![255; 1000],
        vec![0; 10000],
        (0..256).map(|i| i as u8).collect(),
    ];
    
    for blob in blobs {
        let record = Record::new(vec![Value::Blob(blob.clone())]);
        let serialized = record.serialize();
        let deserialized = Record::deserialize(&serialized).unwrap();
        
        assert_eq!(deserialized.columns().len(), 1);
    }
}

// ============================================================================
// Many Column Tests
// ============================================================================

#[test]
fn test_many_columns() {
    let counts = vec![1, 10, 50, 100, 500, 1000];
    
    for count in counts {
        let values: Vec<Value> = (0..count)
            .map(|i| Value::Integer(i as i64))
            .collect();
        
        let record = Record::new(values);
        assert_eq!(record.columns().len(), count);
        
        let serialized = record.serialize();
        let deserialized = Record::deserialize(&serialized).unwrap();
        assert_eq!(deserialized.columns().len(), count);
    }
}

// ============================================================================
// Mixed Type Tests
// ============================================================================

#[test]
fn test_mixed_types() {
    let record = Record::new(vec![
        Value::Null,
        Value::Integer(42),
        Value::Real(3.14),
        Value::Text("hello".to_string()),
        Value::Blob(vec![1, 2, 3]),
    ]);
    
    assert_eq!(record.columns().len(), 5);
    
    let serialized = record.serialize();
    let deserialized = Record::deserialize(&serialized).unwrap();
    assert_eq!(deserialized.columns().len(), 5);
}

#[test]
fn test_alternating_types() {
    let values: Vec<Value> = (0..100)
        .map(|i| match i % 5 {
            0 => Value::Null,
            1 => Value::Integer(i as i64),
            2 => Value::Real(i as f64),
            3 => Value::Text(format!("value{}", i)),
            _ => Value::Blob(vec![i as u8]),
        })
        .collect();
    
    let record = Record::new(values);
    assert_eq!(record.columns().len(), 100);
}

// ============================================================================
// Large Record Tests
// ============================================================================

#[test]
fn test_large_record() {
    // Large string column
    let large_string = "x".repeat(100000);
    let record = Record::new(vec![
        Value::Integer(1),
        Value::Text(large_string),
        Value::Integer(2),
    ]);
    
    let serialized = record.serialize();
    let deserialized = Record::deserialize(&serialized).unwrap();
    
    assert_eq!(deserialized.columns().len(), 3);
}

#[test]
fn test_large_blob_record() {
    // Large blob column
    let large_blob = vec![0u8; 100000];
    let record = Record::new(vec![
        Value::Integer(1),
        Value::Blob(large_blob),
        Value::Integer(2),
    ]);
    
    let serialized = record.serialize();
    let deserialized = Record::deserialize(&serialized).unwrap();
    
    assert_eq!(deserialized.columns().len(), 3);
}

// ============================================================================
// Serialization Roundtrip Tests
// ============================================================================

#[test]
fn test_serialization_roundtrip() {
    let records = vec![
        Record::new(vec![]),
        Record::new(vec![Value::Null]),
        Record::new(vec![Value::Integer(42)]),
        Record::new(vec![Value::Real(3.14)]),
        Record::new(vec![Value::Text("test".to_string())]),
        Record::new(vec![Value::Blob(vec![1, 2, 3])]),
        Record::new(vec![
            Value::Null,
            Value::Integer(1),
            Value::Real(2.0),
            Value::Text("three".to_string()),
            Value::Blob(vec![4, 5]),
        ]),
    ];
    
    for record in records {
        let serialized = record.serialize();
        let deserialized = Record::deserialize(&serialized).unwrap();
        
        assert_eq!(record.columns().len(), deserialized.columns().len());
    }
}

// ============================================================================
// Corruption Handling Tests
// ============================================================================

#[test]
fn test_deserialize_empty() {
    let result = Record::deserialize(&[]);
    // Should handle gracefully (may error or return empty)
}

#[test]
fn test_deserialize_invalid() {
    let invalid_data = vec![0xFF, 0xFF, 0xFF, 0xFF];
    let result = Record::deserialize(&invalid_data);
    // Should handle gracefully
}
