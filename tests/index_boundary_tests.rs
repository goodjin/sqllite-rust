//! Index Boundary Tests
//!
//! Tests for index edge cases and boundary conditions

use sqllite_rust::storage::btree::BPlusTreeIndex;
use sqllite_rust::storage::record::Value;
use sqllite_rust::index::btree::BTreeIndex;

// ============================================================================
// B+Tree Index Boundary Tests
// ============================================================================

#[test]
fn test_empty_index() {
    let index = BPlusTreeIndex::new(
        "idx_test".to_string(),
        "test_table".to_string(),
        "col".to_string(),
    );
    
    assert!(index.lookup(&Value::Integer(1)).is_none());
    let scan = index.range_scan(&Value::Integer(0), &Value::Integer(100));
    assert!(scan.is_empty());
}

#[test]
fn test_single_entry() {
    let mut index = BPlusTreeIndex::new(
        "idx_test".to_string(),
        "test_table".to_string(),
        "col".to_string(),
    );
    
    index.insert(Value::Integer(42), 1).unwrap();
    
    let result = index.lookup(&Value::Integer(42));
    assert!(result.is_some());
    assert_eq!(result.unwrap(), &vec![1]);
}

#[test]
fn test_many_entries() {
    let mut index = BPlusTreeIndex::new(
        "idx_test".to_string(),
        "test_table".to_string(),
        "col".to_string(),
    );
    
    // Insert many entries
    for i in 0..10000 {
        index.insert(Value::Integer(i), i as u64).unwrap();
    }
    
    // Verify all
    for i in 0..10000 {
        let result = index.lookup(&Value::Integer(i));
        assert!(result.is_some());
        assert!(result.unwrap().contains(&(i as u64)));
    }
}

#[test]
fn test_duplicate_keys_many() {
    let mut index = BPlusTreeIndex::new(
        "idx_test".to_string(),
        "test_table".to_string(),
        "col".to_string(),
    );
    
    // Insert many duplicates
    for i in 0..1000 {
        index.insert(Value::Integer(1), i).unwrap();
    }
    
    let result = index.lookup(&Value::Integer(1));
    assert!(result.is_some());
    assert_eq!(result.unwrap().len(), 1000);
}

#[test]
fn test_range_scan_empty_range() {
    let mut index = BPlusTreeIndex::new(
        "idx_test".to_string(),
        "test_table".to_string(),
        "col".to_string(),
    );
    
    // Insert some values
    for i in 0..100 {
        index.insert(Value::Integer(i * 10), i as u64).unwrap();
    }
    
    // Scan empty range
    let result = index.range_scan(&Value::Integer(5), &Value::Integer(9));
    assert!(result.is_empty());
}

#[test]
fn test_range_scan_full_range() {
    let mut index = BPlusTreeIndex::new(
        "idx_test".to_string(),
        "test_table".to_string(),
        "col".to_string(),
    );
    
    // Insert values
    for i in 0..100 {
        index.insert(Value::Integer(i), i as u64).unwrap();
    }
    
    // Full range scan
    let result = index.range_scan(&Value::Integer(0), &Value::Integer(100));
    assert_eq!(result.len(), 100);
}

#[test]
fn test_range_scan_partial_range() {
    let mut index = BPlusTreeIndex::new(
        "idx_test".to_string(),
        "test_table".to_string(),
        "col".to_string(),
    );
    
    // Insert values
    for i in 0..1000 {
        index.insert(Value::Integer(i), i as u64).unwrap();
    }
    
    // Partial scan
    let result = index.range_scan(&Value::Integer(100), &Value::Integer(200));
    assert_eq!(result.len(), 100);
}

#[test]
fn test_range_scan_boundary_values() {
    let mut index = BPlusTreeIndex::new(
        "idx_test".to_string(),
        "test_table".to_string(),
        "col".to_string(),
    );
    
    index.insert(Value::Integer(0), 1).unwrap();
    index.insert(Value::Integer(i64::MAX), 2).unwrap();
    index.insert(Value::Integer(i64::MIN), 3).unwrap();
    
    // Scan around boundaries
    let result = index.range_scan(&Value::Integer(i64::MIN), &Value::Integer(1));
    assert_eq!(result.len(), 2);
    
    let result = index.range_scan(&Value::Integer(i64::MAX - 1), &Value::Integer(i64::MAX));
    assert_eq!(result.len(), 1);
}

#[test]
fn test_delete_nonexistent() {
    let mut index = BPlusTreeIndex::new(
        "idx_test".to_string(),
        "test_table".to_string(),
        "col".to_string(),
    );
    
    // Delete non-existent key
    let result = index.delete(&Value::Integer(999), 1);
    assert!(result.is_ok());
}

#[test]
fn test_delete_all_duplicates() {
    let mut index = BPlusTreeIndex::new(
        "idx_test".to_string(),
        "test_table".to_string(),
        "col".to_string(),
    );
    
    // Insert duplicates
    for i in 0..10 {
        index.insert(Value::Integer(1), i).unwrap();
    }
    
    // Delete all
    for i in 0..10 {
        index.delete(&Value::Integer(1), i).unwrap();
    }
    
    // Key should be removed
    let result = index.lookup(&Value::Integer(1));
    assert!(result.is_none());
}

#[test]
fn test_delete_partial_duplicates() {
    let mut index = BPlusTreeIndex::new(
        "idx_test".to_string(),
        "test_table".to_string(),
        "col".to_string(),
    );
    
    // Insert duplicates
    for i in 0..10 {
        index.insert(Value::Integer(1), i).unwrap();
    }
    
    // Delete half
    for i in 0..5 {
        index.delete(&Value::Integer(1), i).unwrap();
    }
    
    // Key should still exist with remaining
    let result = index.lookup(&Value::Integer(1));
    assert!(result.is_some());
    assert_eq!(result.unwrap().len(), 5);
}

// ============================================================================
// Iterator Tests
// ============================================================================

#[test]
fn test_iterator_empty() {
    let index = BPlusTreeIndex::new(
        "idx_test".to_string(),
        "test_table".to_string(),
        "col".to_string(),
    );
    
    let count = index.iter().count();
    assert_eq!(count, 0);
}

#[test]
fn test_iterator_many() {
    let mut index = BPlusTreeIndex::new(
        "idx_test".to_string(),
        "test_table".to_string(),
        "col".to_string(),
    );
    
    for i in 0..1000 {
        index.insert(Value::Integer(i), i as u64).unwrap();
    }
    
    let count = index.iter().count();
    assert_eq!(count, 1000);
}

// ============================================================================
// From Iterator Tests
// ============================================================================

#[test]
fn test_from_iterator_empty() {
    let entries: Vec<(Value, Vec<u64>)> = vec![];
    
    let index = BPlusTreeIndex::from_iter(
        "idx_test".to_string(),
        "test_table".to_string(),
        "col".to_string(),
        entries.into_iter(),
    );
    
    assert_eq!(index.iter().count(), 0);
}

#[test]
fn test_from_iterator_many() {
    let entries: Vec<(Value, Vec<u64>)> = (0..1000)
        .map(|i| (Value::Integer(i), vec![i as u64]))
        .collect();
    
    let index = BPlusTreeIndex::from_iter(
        "idx_test".to_string(),
        "test_table".to_string(),
        "col".to_string(),
        entries.into_iter(),
    );
    
    assert_eq!(index.iter().count(), 1000);
}

// ============================================================================
// String Key Tests
// ============================================================================

#[test]
fn test_string_keys() {
    let mut index = BPlusTreeIndex::new(
        "idx_test".to_string(),
        "test_table".to_string(),
        "col".to_string(),
    );
    
    let keys = vec![
        "".to_string(),
        "a".to_string(),
        "z".to_string(),
        "aa".to_string(),
        "az".to_string(),
        "zz".to_string(),
        "aaa".to_string(),
    ];
    
    for (i, key) in keys.iter().enumerate() {
        index.insert(Value::Text(key.clone()), i as u64).unwrap();
    }
    
    for key in &keys {
        let result = index.lookup(&Value::Text(key.clone()));
        assert!(result.is_some());
    }
}

#[test]
fn test_string_range_scan() {
    let mut index = BPlusTreeIndex::new(
        "idx_test".to_string(),
        "test_table".to_string(),
        "col".to_string(),
    );
    
    // Insert alphabet
    for c in 'a'..='z' {
        index.insert(Value::Text(c.to_string()), (c as u64) - ('a' as u64)).unwrap();
    }
    
    // Range scan [c, f)
    let result = index.range_scan(
        &Value::Text("c".to_string()),
        &Value::Text("f".to_string()),
    );
    assert_eq!(result.len(), 3); // c, d, e
}

// ============================================================================
// Real Number Key Tests
// ============================================================================

#[test]
fn test_real_keys() {
    let mut index = BPlusTreeIndex::new(
        "idx_test".to_string(),
        "test_table".to_string(),
        "col".to_string(),
    );
    
    let values = vec![
        0.0,
        0.1,
        0.5,
        1.0,
        1.5,
        2.0,
        10.0,
        100.0,
        f64::MIN_POSITIVE,
    ];
    
    for (i, val) in values.iter().enumerate() {
        index.insert(Value::Real(*val), i as u64).unwrap();
    }
    
    for val in &values {
        let result = index.lookup(&Value::Real(*val));
        assert!(result.is_some());
    }
}

#[test]
fn test_real_range_scan() {
    let mut index = BPlusTreeIndex::new(
        "idx_test".to_string(),
        "test_table".to_string(),
        "col".to_string(),
    );
    
    for i in 0..100 {
        index.insert(Value::Real(i as f64 * 0.1), i as u64).unwrap();
    }
    
    let result = index.range_scan(&Value::Real(1.0), &Value::Real(5.0));
    assert!(!result.is_empty());
}

// ============================================================================
// Null Key Tests
// ============================================================================

#[test]
fn test_null_keys() {
    let mut index = BPlusTreeIndex::new(
        "idx_test".to_string(),
        "test_table".to_string(),
        "col".to_string(),
    );
    
    index.insert(Value::Null, 1).unwrap();
    index.insert(Value::Null, 2).unwrap();
    
    let result = index.lookup(&Value::Null);
    assert!(result.is_some());
    assert_eq!(result.unwrap().len(), 2);
}

// ============================================================================
// Mixed Type Tests
// ============================================================================

#[test]
fn test_mixed_types() {
    let mut index = BPlusTreeIndex::new(
        "idx_test".to_string(),
        "test_table".to_string(),
        "col".to_string(),
    );
    
    // Insert different types
    index.insert(Value::Null, 1).unwrap();
    index.insert(Value::Integer(1), 2).unwrap();
    index.insert(Value::Real(1.0), 3).unwrap();
    index.insert(Value::Text("1".to_string()), 4).unwrap();
    index.insert(Value::Blob(vec![1]), 5).unwrap();
    
    // Each should be findable
    assert!(index.lookup(&Value::Null).is_some());
    assert!(index.lookup(&Value::Integer(1)).is_some());
    assert!(index.lookup(&Value::Real(1.0)).is_some());
    assert!(index.lookup(&Value::Text("1".to_string())).is_some());
    assert!(index.lookup(&Value::Blob(vec![1])).is_some());
}

// ============================================================================
// Blob Key Tests
// ============================================================================

#[test]
fn test_blob_keys() {
    let mut index = BPlusTreeIndex::new(
        "idx_test".to_string(),
        "test_table".to_string(),
        "col".to_string(),
    );
    
    let blobs = vec![
        vec![],
        vec![0],
        vec![255],
        vec![0, 1, 2, 3],
        vec![0; 100],
        vec![255; 100],
    ];
    
    for (i, blob) in blobs.iter().enumerate() {
        index.insert(Value::Blob(blob.clone()), i as u64).unwrap();
    }
    
    for blob in &blobs {
        let result = index.lookup(&Value::Blob(blob.clone()));
        assert!(result.is_some());
    }
}

// ============================================================================
// Stress Tests
// ============================================================================

#[test]
fn test_index_stress_insert_delete() {
    let mut index = BPlusTreeIndex::new(
        "idx_test".to_string(),
        "test_table".to_string(),
        "col".to_string(),
    );
    
    // Insert many
    for i in 0..5000 {
        index.insert(Value::Integer(i), i as u64).unwrap();
    }
    
    // Delete half
    for i in 0..2500 {
        index.delete(&Value::Integer(i), i as u64).unwrap();
    }
    
    // Insert more
    for i in 5000..10000 {
        index.insert(Value::Integer(i), i as u64).unwrap();
    }
    
    // Verify
    for i in 0..2500 {
        assert!(index.lookup(&Value::Integer(i)).is_none());
    }
    for i in 2500..10000 {
        assert!(index.lookup(&Value::Integer(i)).is_some());
    }
}

#[test]
fn test_index_stress_duplicates() {
    let mut index = BPlusTreeIndex::new(
        "idx_test".to_string(),
        "test_table".to_string(),
        "col".to_string(),
    );
    
    // Insert many duplicates
    for i in 0..10000 {
        index.insert(Value::Integer(i % 10), i as u64).unwrap();
    }
    
    // Each key should have 1000 entries
    for i in 0..10 {
        let result = index.lookup(&Value::Integer(i));
        assert!(result.is_some());
        assert_eq!(result.unwrap().len(), 1000);
    }
}

// ============================================================================
// Concurrent Access Tests
// ============================================================================

#[test]
fn test_concurrent_reads() {
    use std::sync::{Arc, Mutex};
    use std::thread;
    
    let mut index = BPlusTreeIndex::new(
        "idx_test".to_string(),
        "test_table".to_string(),
        "col".to_string(),
    );
    
    // Populate
    for i in 0..1000 {
        index.insert(Value::Integer(i), i as u64).unwrap();
    }
    
    let index = Arc::new(Mutex::new(index));
    let mut handles = vec![];
    
    for thread_id in 0..10 {
        let index = Arc::clone(&index);
        let handle = thread::spawn(move || {
            let idx = index.lock().unwrap();
            for i in 0..100 {
                let key = (thread_id * 100 + i) as i64;
                assert!(idx.lookup(&Value::Integer(key)).is_some());
            }
        });
        handles.push(handle);
    }
    
    for handle in handles {
        handle.join().unwrap();
    }
}

// ============================================================================
// Ascending/Descending Insert Tests
// ============================================================================

#[test]
fn test_ascending_insert() {
    let mut index = BPlusTreeIndex::new(
        "idx_test".to_string(),
        "test_table".to_string(),
        "col".to_string(),
    );
    
    // Ascending insert
    for i in 0..1000 {
        index.insert(Value::Integer(i), i as u64).unwrap();
    }
    
    // Verify
    for i in 0..1000 {
        assert!(index.lookup(&Value::Integer(i)).is_some());
    }
}

#[test]
fn test_descending_insert() {
    let mut index = BPlusTreeIndex::new(
        "idx_test".to_string(),
        "test_table".to_string(),
        "col".to_string(),
    );
    
    // Descending insert
    for i in (0..1000).rev() {
        index.insert(Value::Integer(i), i as u64).unwrap();
    }
    
    // Verify
    for i in 0..1000 {
        assert!(index.lookup(&Value::Integer(i)).is_some());
    }
}

#[test]
fn test_random_insert() {
    use rand::seq::SliceRandom;
    
    let mut index = BPlusTreeIndex::new(
        "idx_test".to_string(),
        "test_table".to_string(),
        "col".to_string(),
    );
    
    let mut order: Vec<i64> = (0..1000).collect();
    order.shuffle(&mut rand::thread_rng());
    
    // Random insert
    for i in &order {
        index.insert(Value::Integer(*i), *i as u64).unwrap();
    }
    
    // Verify
    for i in 0..1000 {
        assert!(index.lookup(&Value::Integer(i)).is_some());
    }
}

// ============================================================================
// Index Metadata Tests
// ============================================================================

#[test]
fn test_index_metadata() {
    let index = BPlusTreeIndex::new(
        "idx_users_name".to_string(),
        "users".to_string(),
        "name".to_string(),
    );
    
    assert_eq!(index.name, "idx_users_name");
    assert_eq!(index.table, "users");
    assert_eq!(index.column, "name");
}

// ============================================================================
// Large Value Tests
// ============================================================================

#[test]
fn test_large_string_keys() {
    let mut index = BPlusTreeIndex::new(
        "idx_test".to_string(),
        "test_table".to_string(),
        "col".to_string(),
    );
    
    let sizes = vec![100, 1000, 10000];
    
    for (i, size) in sizes.iter().enumerate() {
        let key = "a".repeat(*size);
        index.insert(Value::Text(key.clone()), i as u64).unwrap();
        
        let result = index.lookup(&Value::Text(key));
        assert!(result.is_some());
    }
}

#[test]
fn test_large_blob_keys() {
    let mut index = BPlusTreeIndex::new(
        "idx_test".to_string(),
        "test_table".to_string(),
        "col".to_string(),
    );
    
    let sizes = vec![100, 1000, 10000];
    
    for (i, size) in sizes.iter().enumerate() {
        let key = vec![0u8; *size];
        index.insert(Value::Blob(key.clone()), i as u64).unwrap();
        
        let result = index.lookup(&Value::Blob(key));
        assert!(result.is_some());
    }
}
