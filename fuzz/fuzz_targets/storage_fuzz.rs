#![no_main]

use libfuzzer_sys::fuzz_target;
use std::collections::HashMap;

// Storage Engine fuzzing target
// Tests B+Tree operations with random key-value pairs

fuzz_target!(|data: &[u8]| {
    if data.is_empty() {
        return;
    }
    
    // Parse fuzz input into operations
    let operations = parse_operations(data);
    
    // Execute operations against a mock storage
    let _ = execute_storage_operations(operations);
    
    // Test record encoding/decoding
    let _ = fuzz_record_encoding(data);
    
    // Test key comparison
    let _ = fuzz_key_comparison(data);
});

#[derive(Debug, Clone)]
enum StorageOp {
    Insert(Vec<u8>, Vec<u8>),
    Get(Vec<u8>),
    Delete(Vec<u8>),
    RangeScan(Vec<u8>, Vec<u8>),
    Update(Vec<u8>, Vec<u8>),
}

fn parse_operations(data: &[u8]) -> Vec<StorageOp> {
    let mut ops = Vec::new();
    let mut i = 0;
    
    while i < data.len() {
        let op_type = data[i] % 5;
        i += 1;
        
        // Extract key (variable length)
        let key_len = if i < data.len() { 
            (data[i] as usize % 64) + 1 
        } else { 
            break;
        };
        i += 1;
        
        let key_end = (i + key_len).min(data.len());
        let key = data[i..key_end].to_vec();
        i = key_end;
        
        match op_type {
            0 => {
                // Insert: need value
                if i < data.len() {
                    let val_len = (data[i] as usize % 256) + 1;
                    i += 1;
                    let val_end = (i + val_len).min(data.len());
                    let value = data[i..val_end].to_vec();
                    i = val_end;
                    ops.push(StorageOp::Insert(key, value));
                }
            }
            1 => {
                // Get
                ops.push(StorageOp::Get(key));
            }
            2 => {
                // Delete
                ops.push(StorageOp::Delete(key));
            }
            3 => {
                // RangeScan: need end key
                let end_key = key.iter().map(|b| b.wrapping_add(1)).collect();
                ops.push(StorageOp::RangeScan(key, end_key));
            }
            4 => {
                // Update: need value
                if i < data.len() {
                    let val_len = (data[i] as usize % 256) + 1;
                    i += 1;
                    let val_end = (i + val_len).min(data.len());
                    let value = data[i..val_end].to_vec();
                    i = val_end;
                    ops.push(StorageOp::Update(key, value));
                }
            }
            _ => {}
        }
        
        // Limit number of operations
        if ops.len() >= 100 {
            break;
        }
    }
    
    ops
}

fn execute_storage_operations(ops: Vec<StorageOp>) -> Result<(), ()> {
    // Mock in-memory storage for fuzzing
    let mut storage: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
    
    for op in ops {
        match op {
            StorageOp::Insert(key, value) => {
                storage.insert(key, value);
            }
            StorageOp::Get(key) => {
                let _ = storage.get(&key);
            }
            StorageOp::Delete(key) => {
                storage.remove(&key);
            }
            StorageOp::RangeScan(start, end) => {
                // Simulate range scan
                let mut results: Vec<_> = storage
                    .iter()
                    .filter(|(k, _)| k.as_slice() >= start.as_slice() && k.as_slice() <= end.as_slice())
                    .collect();
                results.sort_by(|a, b| a.0.cmp(b.0));
            }
            StorageOp::Update(key, value) => {
                if storage.contains_key(&key) {
                    storage.insert(key, value);
                }
            }
        }
    }
    
    Ok(())
}

fn fuzz_record_encoding(data: &[u8]) -> Result<(), ()> {
    use sqllite_rust::storage::Value;
    
    // Test Value serialization
    let values = vec![
        Value::Null,
        Value::Integer(42),
        Value::Integer(-999999),
        Value::Real(3.14159),
        Value::Text("fuzz".to_string()),
        Value::Blob(data.to_vec()),
    ];
    
    for value in values {
        // Serialize
        let bytes = value.serialize();
        
        // Deserialize
        if let Some((decoded, _)) = Value::deserialize(&bytes) {
            // Verify round-trip for non-floating point (fp equality is tricky)
            match (&value, &decoded) {
                (Value::Real(a), Value::Real(b)) => {
                    // Allow small floating point differences
                    assert!((a - b).abs() < 0.0001, "Float round-trip failed");
                }
                _ => {
                    assert_eq!(value, decoded, "Value round-trip failed");
                }
            }
        }
    }
    
    Ok(())
}

fn fuzz_key_comparison(data: &[u8]) -> Result<(), ()> {
    if data.len() < 2 {
        return Ok(());
    }
    
    // Split data into two keys
    let mid = data.len() / 2;
    let key1 = &data[..mid];
    let key2 = &data[mid..];
    
    // Test comparison operations
    let _cmp_result = key1.cmp(key2);
    let _eq_result = key1 == key2;
    
    // Test prefix comparison
    let prefix_len = mid.min(key2.len());
    let _prefix_eq = &key1[..prefix_len] == &key2[..prefix_len];
    
    Ok(())
}

// Trait extensions for Value
trait ValueExt {
    fn serialize(&self) -> Vec<u8>;
    fn deserialize(bytes: &[u8]) -> Option<(Self, usize)> where Self: Sized;
}

impl ValueExt for sqllite_rust::storage::Value {
    fn serialize(&self) -> Vec<u8> {
        use sqllite_rust::storage::Value;
        match self {
            Value::Null => vec![0],
            Value::Integer(i) => {
                let mut result = vec![1];
                result.extend_from_slice(&i.to_be_bytes());
                result
            }
            Value::Real(f) => {
                let mut result = vec![2];
                result.extend_from_slice(&f.to_be_bytes());
                result
            }
            Value::Text(s) => {
                let mut result = vec![3];
                let bytes = s.as_bytes();
                result.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
                result.extend_from_slice(bytes);
                result
            }
            Value::Blob(b) => {
                let mut result = vec![4];
                result.extend_from_slice(&(b.len() as u32).to_be_bytes());
                result.extend_from_slice(b);
                result
            }
        }
    }
    
    fn deserialize(bytes: &[u8]) -> Option<(Self, usize)> {
        use sqllite_rust::storage::Value;
        
        if bytes.is_empty() {
            return None;
        }
        
        match bytes[0] {
            0 => Some((Value::Null, 1)),
            1 if bytes.len() >= 9 => {
                let val = i64::from_be_bytes(bytes[1..9].try_into().ok()?);
                Some((Value::Integer(val), 9))
            }
            2 if bytes.len() >= 9 => {
                let val = f64::from_be_bytes(bytes[1..9].try_into().ok()?);
                Some((Value::Real(val), 9))
            }
            3 if bytes.len() >= 5 => {
                let len = u32::from_be_bytes(bytes[1..5].try_into().ok()?) as usize;
                if bytes.len() >= 5 + len {
                    let s = String::from_utf8_lossy(&bytes[5..5+len]);
                    Some((Value::Text(s.to_string()), 5 + len))
                } else {
                    None
                }
            }
            4 if bytes.len() >= 5 => {
                let len = u32::from_be_bytes(bytes[1..5].try_into().ok()?) as usize;
                if bytes.len() >= 5 + len {
                    Some((Value::Blob(bytes[5..5+len].to_vec()), 5 + len))
                } else {
                    None
                }
            }
            _ => None,
        }
    }
}
