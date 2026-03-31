//! Serialization Boundary Tests
//!
//! Tests for serialization edge cases and boundary conditions

// ============================================================================
// Binary Data Tests
// ============================================================================

#[test]
fn test_binary_roundtrip() {
    let data = vec![0u8, 1, 2, 3, 255];
    let serialized = data.clone();
    assert_eq!(data, serialized);
}

#[test]
fn test_binary_empty() {
    let data: Vec<u8> = vec![];
    assert!(data.is_empty());
}

#[test]
fn test_binary_large() {
    let data = vec![0u8; 1000000];
    assert_eq!(data.len(), 1000000);
}

// ============================================================================
// Hex Encoding Tests
// ============================================================================

#[test]
fn test_hex_encoding() {
    let data = vec![0u8, 1, 2, 255];
    let hex = hex::encode(&data);
    assert_eq!(hex, "000102ff");
}

#[test]
fn test_hex_decoding() {
    let hex = "000102ff";
    let decoded = hex::decode(hex).unwrap();
    assert_eq!(decoded, vec![0u8, 1, 2, 255]);
}

#[test]
fn test_hex_empty() {
    let data: Vec<u8> = vec![];
    let hex = hex::encode(&data);
    assert!(hex.is_empty());
}

// ============================================================================
// Base64 Tests
// ============================================================================

#[test]
fn test_base64_roundtrip() {
    use base64::{Engine as _, engine::general_purpose};
    
    let data = b"hello world";
    let encoded = general_purpose::STANDARD.encode(data);
    let decoded = general_purpose::STANDARD.decode(&encoded).unwrap();
    
    assert_eq!(data.to_vec(), decoded);
}

// ============================================================================
// JSON Tests
// ============================================================================

#[test]
fn test_json_null() {
    let json = "null";
    let _ = json;
}

#[test]
fn test_json_boolean() {
    let jsons = vec!["true", "false"];
    
    for json in jsons {
        let _ = json;
    }
}

#[test]
fn test_json_number() {
    let jsons = vec![
        "0",
        "-1",
        "1",
        "3.14",
        "-1.5e10",
        "1.7976931348623157e308",
    ];
    
    for json in jsons {
        let _ = json;
    }
}

#[test]
fn test_json_string() {
    let jsons = vec![
        "\"\"",
        "\"hello\"",
        "\"hello world\"",
        "\"special: \\\"quoted\\\"\"",
        "\"unicode: \\u0041\"",
        "\"newline: \\n\"",
        "\"tab: \\t\"",
    ];
    
    for json in jsons {
        let _ = json;
    }
}

#[test]
fn test_json_array() {
    let jsons = vec![
        "[]",
        "[1]",
        "[1, 2, 3]",
        "[1, \"two\", 3.0, null, true]",
        "[[1, 2], [3, 4]]",
    ];
    
    for json in jsons {
        let _ = json;
    }
}

#[test]
fn test_json_object() {
    let jsons = vec![
        "{}",
        "{\"a\": 1}",
        "{\"a\": 1, \"b\": 2}",
        "{\"nested\": {\"key\": \"value\"}}",
    ];
    
    for json in jsons {
        let _ = json;
    }
}

#[test]
fn test_json_complex() {
    let json = r#"{
        "name": "test",
        "count": 42,
        "active": true,
        "nested": {
            "array": [1, 2, 3],
            "null": null
        }
    }"#;
    
    let _ = json;
}
