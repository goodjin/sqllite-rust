#![no_main]

use libfuzzer_sys::fuzz_target;

// Record encoding/decoding fuzzing target
// Tests various record formats and serialization

fuzz_target!(|data: &[u8]| {
    if data.is_empty() {
        return;
    }
    
    // Test record serialization
    let _ = fuzz_record_serialization(data);
    
    // Test value encoding
    let _ = fuzz_value_encoding(data);
    
    // Test fixed-width vs variable-width encoding
    let _ = fuzz_encoding_modes(data);
    
    // Test record parsing with corruption
    let _ = fuzz_corrupted_records(data);
});

#[derive(Debug, Clone, PartialEq)]
enum ValueType {
    Null,
    Bool(bool),
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Float32(f32),
    Float64(f64),
    String(Vec<u8>),
    Blob(Vec<u8>),
}

fn fuzz_record_serialization(data: &[u8]) -> Result<(), ()> {
    // Parse data into values
    let values = parse_values(data);
    
    // Serialize record
    let serialized = serialize_record(&values);
    
    // Deserialize and verify
    let deserialized = deserialize_record(&serialized)?;
    
    // Verify round-trip
    assert_eq!(values.len(), deserialized.len(), "Record length mismatch");
    
    for (orig, recov) in values.iter().zip(deserialized.iter()) {
        match (orig, recov) {
            (ValueType::Null, ValueType::Null) => {}
            (ValueType::Bool(a), ValueType::Bool(b)) => assert_eq!(a, b),
            (ValueType::Int8(a), ValueType::Int8(b)) => assert_eq!(a, b),
            (ValueType::Int16(a), ValueType::Int16(b)) => assert_eq!(a, b),
            (ValueType::Int32(a), ValueType::Int32(b)) => assert_eq!(a, b),
            (ValueType::Int64(a), ValueType::Int64(b)) => assert_eq!(a, b),
            (ValueType::Float32(a), ValueType::Float32(b)) => {
                assert!((a - b).abs() < f32::EPSILON, "Float32 mismatch");
            }
            (ValueType::Float64(a), ValueType::Float64(b)) => {
                assert!((a - b).abs() < f64::EPSILON, "Float64 mismatch");
            }
            (ValueType::String(a), ValueType::String(b)) => assert_eq!(a, b),
            (ValueType::Blob(a), ValueType::Blob(b)) => assert_eq!(a, b),
            _ => panic!("Type mismatch in deserialization"),
        }
    }
    
    Ok(())
}

fn parse_values(data: &[u8]) -> Vec<ValueType> {
    let mut values = Vec::new();
    
    for (i, &byte) in data.iter().enumerate() {
        let value = match byte % 10 {
            0 => ValueType::Null,
            1 => ValueType::Bool(byte % 2 == 0),
            2 => ValueType::Int8(byte as i8),
            3 => ValueType::Int16(
                i16::from_le_bytes([byte, data.get(i+1).copied().unwrap_or(0)])
            ),
            4 => {
                let bytes = [
                    byte,
                    data.get(i+1).copied().unwrap_or(0),
                    data.get(i+2).copied().unwrap_or(0),
                    data.get(i+3).copied().unwrap_or(0),
                ];
                ValueType::Int32(i32::from_le_bytes(bytes))
            }
            5 => {
                let bytes = [
                    byte,
                    data.get(i+1).copied().unwrap_or(0),
                    data.get(i+2).copied().unwrap_or(0),
                    data.get(i+3).copied().unwrap_or(0),
                    data.get(i+4).copied().unwrap_or(0),
                    data.get(i+5).copied().unwrap_or(0),
                    data.get(i+6).copied().unwrap_or(0),
                    data.get(i+7).copied().unwrap_or(0),
                ];
                ValueType::Int64(i64::from_le_bytes(bytes))
            }
            6 => {
                let bytes = [
                    byte,
                    data.get(i+1).copied().unwrap_or(0),
                    data.get(i+2).copied().unwrap_or(0),
                    data.get(i+3).copied().unwrap_or(0),
                ];
                ValueType::Float32(f32::from_le_bytes(bytes))
            }
            7 => {
                let bytes = [
                    byte,
                    data.get(i+1).copied().unwrap_or(0),
                    data.get(i+2).copied().unwrap_or(0),
                    data.get(i+3).copied().unwrap_or(0),
                    data.get(i+4).copied().unwrap_or(0),
                    data.get(i+5).copied().unwrap_or(0),
                    data.get(i+6).copied().unwrap_or(0),
                    data.get(i+7).copied().unwrap_or(0),
                ];
                ValueType::Float64(f64::from_le_bytes(bytes))
            }
            8 => {
                let len = (byte % 16) as usize;
                let s: Vec<u8> = data.iter().skip(i+1).take(len).copied().collect();
                ValueType::String(s)
            }
            9 => {
                let len = (byte % 32) as usize;
                let b: Vec<u8> = data.iter().skip(i+1).take(len).copied().collect();
                ValueType::Blob(b)
            }
            _ => ValueType::Null,
        };
        values.push(value);
        
        if values.len() >= 20 {
            break;
        }
    }
    
    values
}

fn serialize_record(values: &[ValueType]) -> Vec<u8> {
    let mut result = Vec::new();
    
    // Header: number of values
    result.push(values.len() as u8);
    
    // Type codes
    for value in values {
        let type_code: u8 = match value {
            ValueType::Null => 0,
            ValueType::Bool(_) => 1,
            ValueType::Int8(_) => 2,
            ValueType::Int16(_) => 3,
            ValueType::Int32(_) => 4,
            ValueType::Int64(_) => 5,
            ValueType::Float32(_) => 6,
            ValueType::Float64(_) => 7,
            ValueType::String(s) => 0x80 | (s.len().min(127) as u8),
            ValueType::Blob(b) => 0xC0 | (b.len().min(63) as u8),
        };
        result.push(type_code);
    }
    
    // Values
    for value in values {
        match value {
            ValueType::Null => {}
            ValueType::Bool(b) => result.push(*b as u8),
            ValueType::Int8(i) => result.push(*i as u8),
            ValueType::Int16(i) => result.extend_from_slice(&i.to_le_bytes()),
            ValueType::Int32(i) => result.extend_from_slice(&i.to_le_bytes()),
            ValueType::Int64(i) => result.extend_from_slice(&i.to_le_bytes()),
            ValueType::Float32(f) => result.extend_from_slice(&f.to_le_bytes()),
            ValueType::Float64(f) => result.extend_from_slice(&f.to_le_bytes()),
            ValueType::String(s) => result.extend_from_slice(s),
            ValueType::Blob(b) => result.extend_from_slice(b),
        }
    }
    
    result
}

fn deserialize_record(data: &[u8]) -> Result<Vec<ValueType>, ()> {
    if data.is_empty() {
        return Ok(Vec::new());
    }
    
    let num_values = data[0] as usize;
    let mut values = Vec::with_capacity(num_values);
    
    if data.len() < 1 + num_values {
        return Err(());
    }
    
    let mut offset = 1 + num_values; // After header and type codes
    
    for i in 0..num_values {
        let type_code = data[1 + i];
        
        let value = match type_code {
            0 => ValueType::Null,
            1 => {
                if offset < data.len() {
                    let v = ValueType::Bool(data[offset] != 0);
                    offset += 1;
                    v
                } else {
                    ValueType::Null
                }
            }
            2 => {
                if offset < data.len() {
                    let v = ValueType::Int8(data[offset] as i8);
                    offset += 1;
                    v
                } else {
                    ValueType::Null
                }
            }
            3 => {
                if offset + 1 < data.len() {
                    let bytes = [data[offset], data[offset+1]];
                    let v = ValueType::Int16(i16::from_le_bytes(bytes));
                    offset += 2;
                    v
                } else {
                    ValueType::Null
                }
            }
            4 => {
                if offset + 3 < data.len() {
                    let bytes = [data[offset], data[offset+1], data[offset+2], data[offset+3]];
                    let v = ValueType::Int32(i32::from_le_bytes(bytes));
                    offset += 4;
                    v
                } else {
                    ValueType::Null
                }
            }
            5 => {
                if offset + 7 < data.len() {
                    let bytes = [
                        data[offset], data[offset+1], data[offset+2], data[offset+3],
                        data[offset+4], data[offset+5], data[offset+6], data[offset+7]
                    ];
                    let v = ValueType::Int64(i64::from_le_bytes(bytes));
                    offset += 8;
                    v
                } else {
                    ValueType::Null
                }
            }
            6 => {
                if offset + 3 < data.len() {
                    let bytes = [data[offset], data[offset+1], data[offset+2], data[offset+3]];
                    let v = ValueType::Float32(f32::from_le_bytes(bytes));
                    offset += 4;
                    v
                } else {
                    ValueType::Null
                }
            }
            7 => {
                if offset + 7 < data.len() {
                    let bytes = [
                        data[offset], data[offset+1], data[offset+2], data[offset+3],
                        data[offset+4], data[offset+5], data[offset+6], data[offset+7]
                    ];
                    let v = ValueType::Float64(f64::from_le_bytes(bytes));
                    offset += 8;
                    v
                } else {
                    ValueType::Null
                }
            }
            n if n & 0x80 != 0 => {
                // String or blob
                let len = (n & 0x3F) as usize;
                if offset + len <= data.len() {
                    let bytes = data[offset..offset+len].to_vec();
                    offset += len;
                    if n & 0x40 != 0 {
                        ValueType::Blob(bytes)
                    } else {
                        ValueType::String(bytes)
                    }
                } else {
                    ValueType::Null
                }
            }
            _ => ValueType::Null,
        };
        
        values.push(value);
    }
    
    Ok(values)
}

fn fuzz_value_encoding(data: &[u8]) -> Result<(), ()> {
    // Test specific value encoding edge cases
    
    // Integer encoding
    test_integer_encoding(0i64);
    test_integer_encoding(-1i64);
    test_integer_encoding(1i64);
    test_integer_encoding(i64::MAX);
    test_integer_encoding(i64::MIN);
    
    // Float encoding
    test_float_encoding(0.0f64);
    test_float_encoding(-0.0f64);
    test_float_encoding(f64::NAN);
    test_float_encoding(f64::INFINITY);
    test_float_encoding(f64::NEG_INFINITY);
    test_float_encoding(f64::MIN_POSITIVE);
    test_float_encoding(f64::MAX);
    test_float_encoding(f64::MIN);
    
    // String encoding with special characters
    let special_strings = vec![
        vec![],
        vec![0],
        vec![0xFF],
        vec![b'\n', b'\r', b'\t'],
        vec![b'"', b'\'', b'\\'],
    ];
    
    for s in special_strings {
        let encoded = serialize_record(&[ValueType::String(s.clone())]);
        let decoded = deserialize_record(&encoded)?;
        assert_eq!(decoded[0], ValueType::String(s));
    }
    
    Ok(())
}

fn test_integer_encoding(val: i64) {
    let bytes = val.to_be_bytes();
    let recovered = i64::from_be_bytes(bytes);
    assert_eq!(val, recovered);
}

fn test_float_encoding(val: f64) {
    let bytes = val.to_be_bytes();
    let recovered = f64::from_be_bytes(bytes);
    
    if val.is_nan() {
        assert!(recovered.is_nan());
    } else {
        assert_eq!(val, recovered);
    }
}

fn fuzz_encoding_modes(data: &[u8]) -> Result<(), ()> {
    // Test fixed-width vs variable-width encoding trade-offs
    
    let test_values = vec![
        ValueType::Int32(42),
        ValueType::Int32(1000000),
        ValueType::String(vec![b'a'; 10]),
        ValueType::String(vec![b'b'; 100]),
    ];
    
    // Fixed-width encoding
    let fixed_width = test_values.iter().map(|v| {
        match v {
            ValueType::Int32(_) => 4usize,
            ValueType::String(s) => s.len().max(100),
            _ => 8,
        }
    }).sum::<usize>();
    
    // Variable-width encoding
    let variable_width = serialize_record(&test_values).len();
    
    // Variable-width should be more compact
    assert!(variable_width <= fixed_width, "Variable-width encoding should be more compact");
    
    // Verify round-trip for all modes
    let round_trip = deserialize_record(&serialize_record(&test_values))?;
    assert_eq!(test_values.len(), round_trip.len());
    
    Ok(())
}

fn fuzz_corrupted_records(data: &[u8]) -> Result<(), ()> {
    // Test handling of corrupted/malformed records
    
    // Create a valid record
    let valid_values = vec![
        ValueType::Int32(42),
        ValueType::String(b"test".to_vec()),
    ];
    let mut corrupted = serialize_record(&valid_values);
    
    // Apply corruptions
    if !corrupted.is_empty() && !data.is_empty() {
        for (i, &byte) in data.iter().enumerate().take(corrupted.len().min(10)) {
            corrupted[i] = corrupted[i].wrapping_add(byte);
        }
    }
    
    // Should not panic, might return error
    let _ = deserialize_record(&corrupted);
    
    // Test truncated record
    for len in 0..corrupted.len().min(5) {
        let _ = deserialize_record(&corrupted[..len]);
    }
    
    // Test with garbage data
    let _ = deserialize_record(data);
    
    Ok(())
}
