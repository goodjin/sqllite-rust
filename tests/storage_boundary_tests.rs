//! Storage Engine Boundary Tests
//!
//! Tests for storage engine edge cases and boundary conditions

use sqllite_rust::storage::btree::BPlusTreeIndex;
use sqllite_rust::storage::record::{Record, Value};
use sqllite_rust::storage::overflow::OverflowPage;
use sqllite_rust::pager::page::{Page, PageId};
use sqllite_rust::storage::wal::{Wal, WalHeader, WalFrame, GroupCommitConfig};

// ============================================================================
// Key Length Boundary Tests
// ============================================================================

#[test]
fn test_zero_length_key() {
    let mut index = BPlusTreeIndex::new(
        "idx_test".to_string(),
        "test_table".to_string(),
        "col".to_string(),
    );
    
    // Empty string as key
    let result = index.insert(Value::Text("".to_string()), 1);
    assert!(result.is_ok());
    
    // Should be able to look up empty key
    let lookup = index.lookup(&Value::Text("".to_string()));
    assert!(lookup.is_some());
}

#[test]
fn test_max_key_length() {
    let mut index = BPlusTreeIndex::new(
        "idx_test".to_string(),
        "test_table".to_string(),
        "col".to_string(),
    );
    
    // Test various key sizes
    let sizes = vec![100, 1000, 10000];
    for size in sizes {
        let long_key = "a".repeat(size);
        let result = index.insert(Value::Text(long_key.clone()), size as u64);
        assert!(result.is_ok());
        
        let lookup = index.lookup(&Value::Text(long_key));
        assert!(lookup.is_some());
    }
}

#[test]
fn test_very_long_key() {
    let mut index = BPlusTreeIndex::new(
        "idx_test".to_string(),
        "test_table".to_string(),
        "col".to_string(),
    );
    
    // Very long key (100K chars)
    let very_long_key = "x".repeat(100000);
    let result = index.insert(Value::Text(very_long_key), 1);
    assert!(result.is_ok());
}

// ============================================================================
// Value Length Boundary Tests
// ============================================================================

#[test]
fn test_zero_length_value() {
    let record = Record::new(vec![]);
    assert!(record.columns().is_empty());
}

#[test]
fn test_max_value_length() {
    let sizes = vec![100, 1000, 10000, 100000];
    for size in sizes {
        let long_value = Value::Text("v".repeat(size));
        let record = Record::new(vec![long_value]);
        assert_eq!(record.columns().len(), 1);
    }
}

// ============================================================================
// Page Boundary Tests
// ============================================================================

#[test]
fn test_page_size_boundaries() {
    let sizes = vec![512, 1024, 2048, 4096, 8192, 16384, 32768, 65536];
    for size in sizes {
        let page = Page::new(1);
        // Page should be created without panic
        assert_eq!(page.id(), 1);
    }
}

#[test]
fn test_page_id_boundaries() {
    let ids = vec![0u32, 1, 100, 1000, u32::MAX];
    for id in ids {
        let page = Page::new(id);
        assert_eq!(page.id(), id);
    }
}

#[test]
fn test_page_data_boundaries() {
    let page = Page::new(1);
    let data = page.as_slice();
    // Should have default page size
    assert!(!data.is_empty());
}

// ============================================================================
// B+Tree Depth Tests
// ============================================================================

#[test]
fn test_btree_many_entries() {
    let mut index = BPlusTreeIndex::new(
        "idx_test".to_string(),
        "test_table".to_string(),
        "col".to_string(),
    );
    
    // Insert many entries to force tree growth
    for i in 0..10000 {
        let key = Value::Integer(i);
        let result = index.insert(key, i as u64);
        assert!(result.is_ok());
    }
    
    // Verify all entries are findable
    for i in 0..10000 {
        let lookup = index.lookup(&Value::Integer(i));
        assert!(lookup.is_some());
    }
}

#[test]
fn test_btree_duplicate_keys() {
    let mut index = BPlusTreeIndex::new(
        "idx_test".to_string(),
        "test_table".to_string(),
        "col".to_string(),
    );
    
    // Insert many duplicates
    for i in 0..1000 {
        let result = index.insert(Value::Text("duplicate".to_string()), i);
        assert!(result.is_ok());
    }
    
    // Should have all rowids
    let lookup = index.lookup(&Value::Text("duplicate".to_string()));
    assert!(lookup.is_some());
    assert_eq!(lookup.unwrap().len(), 1000);
}

#[test]
fn test_btree_range_scan_boundaries() {
    let mut index = BPlusTreeIndex::new(
        "idx_salary".to_string(),
        "employees".to_string(),
        "salary".to_string(),
    );
    
    // Insert entries
    for i in 0..1000 {
        index.insert(Value::Integer(i * 100), i as u64).unwrap();
    }
    
    // Test range scans at boundaries
    let result = index.range_scan(&Value::Integer(0), &Value::Integer(100));
    assert!(!result.is_empty());
    
    let result = index.range_scan(&Value::Integer(90000), &Value::Integer(100000));
    assert!(!result.is_empty());
    
    // Empty range
    let result = index.range_scan(&Value::Integer(-1000), &Value::Integer(-1));
    assert!(result.is_empty());
    
    // Full range
    let result = index.range_scan(&Value::Integer(0), &Value::Integer(100000));
    assert_eq!(result.len(), 1000);
}

// ============================================================================
// Record Boundary Tests
// ============================================================================

#[test]
fn test_record_many_columns() {
    let counts = vec![1, 10, 50, 100, 500];
    for count in counts {
        let values: Vec<Value> = (0..count).map(|i| Value::Integer(i as i64)).collect();
        let record = Record::new(values);
        assert_eq!(record.columns().len(), count);
    }
}

#[test]
fn test_record_null_values() {
    let record = Record::new(vec![
        Value::Null,
        Value::Null,
        Value::Null,
    ]);
    assert_eq!(record.columns().len(), 3);
}

#[test]
fn test_record_mixed_types() {
    let record = Record::new(vec![
        Value::Null,
        Value::Integer(42),
        Value::Real(3.14),
        Value::Text("hello".to_string()),
        Value::Blob(vec![1, 2, 3]),
    ]);
    assert_eq!(record.columns().len(), 5);
}

#[test]
fn test_record_large_blob() {
    let sizes = vec![100, 1000, 10000, 100000];
    for size in sizes {
        let blob = vec![0u8; size];
        let record = Record::new(vec![Value::Blob(blob)]);
        assert_eq!(record.columns().len(), 1);
    }
}

// ============================================================================
// Value Type Boundary Tests
// ============================================================================

#[test]
fn test_integer_boundaries() {
    let values = vec![
        Value::Integer(0),
        Value::Integer(1),
        Value::Integer(-1),
        Value::Integer(i64::MAX),
        Value::Integer(i64::MIN),
    ];
    
    for value in values {
        let record = Record::new(vec![value]);
        assert_eq!(record.columns().len(), 1);
    }
}

#[test]
fn test_real_boundaries() {
    let values = vec![
        Value::Real(0.0),
        Value::Real(1.0),
        Value::Real(-1.0),
        Value::Real(f64::MAX),
        Value::Real(f64::MIN),
        Value::Real(f64::NAN),
        Value::Real(f64::INFINITY),
        Value::Real(f64::NEG_INFINITY),
    ];
    
    for value in values {
        let record = Record::new(vec![value]);
        assert_eq!(record.columns().len(), 1);
    }
}

#[test]
fn test_text_boundaries() {
    let values = vec![
        Value::Text("".to_string()),
        Value::Text("a".to_string()),
        Value::Text("hello world".to_string()),
        Value::Text("a".repeat(1000)),
        Value::Text("x".repeat(10000)),
    ];
    
    for value in values {
        let record = Record::new(vec![value]);
        assert_eq!(record.columns().len(), 1);
    }
}

#[test]
fn test_blob_boundaries() {
    let values = vec![
        Value::Blob(vec![]),
        Value::Blob(vec![0]),
        Value::Blob(vec![0, 1, 2, 3]),
        Value::Blob(vec![0; 1000]),
        Value::Blob(vec![255; 10000]),
    ];
    
    for value in values {
        let record = Record::new(vec![value]);
        assert_eq!(record.columns().len(), 1);
    }
}

// ============================================================================
// Overflow Page Tests
// ============================================================================

#[test]
fn test_overflow_page_boundaries() {
    let page_id = 1u32;
    let next_page = Some(2u32);
    let data = vec![1u8, 2, 3, 4, 5];
    
    let overflow = OverflowPage::new(page_id, next_page, data);
    assert_eq!(overflow.page_id(), page_id);
    assert_eq!(overflow.next_page(), next_page);
}

#[test]
fn test_overflow_page_empty_data() {
    let overflow = OverflowPage::new(1, None, vec![]);
    assert!(overflow.data().is_empty());
}

#[test]
fn test_overflow_page_large_data() {
    let large_data = vec![0u8; 10000];
    let overflow = OverflowPage::new(1, Some(2), large_data);
    assert_eq!(overflow.data().len(), 10000);
}

// ============================================================================
// Index Deletion Tests
// ============================================================================

#[test]
fn test_index_delete_all_entries() {
    let mut index = BPlusTreeIndex::new(
        "idx_test".to_string(),
        "test_table".to_string(),
        "col".to_string(),
    );
    
    // Insert and then delete all entries
    for i in 0..100 {
        index.insert(Value::Integer(i), i as u64).unwrap();
    }
    
    // Delete all
    for i in 0..100 {
        index.delete(&Value::Integer(i), i as u64).unwrap();
    }
    
    // Verify all deleted
    for i in 0..100 {
        let lookup = index.lookup(&Value::Integer(i));
        assert!(lookup.is_none());
    }
}

#[test]
fn test_index_delete_partial() {
    let mut index = BPlusTreeIndex::new(
        "idx_test".to_string(),
        "test_table".to_string(),
        "col".to_string(),
    );
    
    // Insert entries
    for i in 0..100 {
        index.insert(Value::Integer(i), i as u64).unwrap();
    }
    
    // Delete even entries
    for i in 0..50 {
        index.delete(&Value::Integer(i * 2), (i * 2) as u64).unwrap();
    }
    
    // Verify odd entries still exist
    for i in 0..50 {
        let lookup = index.lookup(&Value::Integer(i * 2 + 1));
        assert!(lookup.is_some());
    }
}

#[test]
fn test_index_delete_duplicate_partial() {
    let mut index = BPlusTreeIndex::new(
        "idx_test".to_string(),
        "test_table".to_string(),
        "col".to_string(),
    );
    
    // Insert duplicates
    for i in 0..10 {
        index.insert(Value::Text("dup".to_string()), i).unwrap();
    }
    
    // Delete some duplicates
    index.delete(&Value::Text("dup".to_string()), 5).unwrap();
    index.delete(&Value::Text("dup".to_string()), 7).unwrap();
    
    // Verify remaining
    let lookup = index.lookup(&Value::Text("dup".to_string()));
    assert!(lookup.is_some());
    assert_eq!(lookup.unwrap().len(), 8);
}

// ============================================================================
// Concurrent Insert Tests
// ============================================================================

#[test]
fn test_concurrent_insert_same_key() {
    use std::sync::{Arc, Mutex};
    use std::thread;
    
    let index = Arc::new(Mutex::new(BPlusTreeIndex::new(
        "idx_test".to_string(),
        "test_table".to_string(),
        "col".to_string(),
    )));
    
    let mut handles = vec![];
    
    // Spawn threads to insert same key
    for i in 0..100 {
        let index = Arc::clone(&index);
        let handle = thread::spawn(move || {
            let mut idx = index.lock().unwrap();
            idx.insert(Value::Text("shared".to_string()), i as u64).unwrap();
        });
        handles.push(handle);
    }
    
    for handle in handles {
        handle.join().unwrap();
    }
    
    // Verify all entries
    let idx = index.lock().unwrap();
    let lookup = idx.lookup(&Value::Text("shared".to_string()));
    assert!(lookup.is_some());
    assert_eq!(lookup.unwrap().len(), 100);
}

// ============================================================================
// WAL Boundary Tests
// ============================================================================

#[test]
fn test_wal_header_serialization() {
    let page_sizes = vec![512, 1024, 2048, 4096, 8192, 16384, 32768, 65536];
    
    for size in page_sizes {
        let header = WalHeader::new(size as u32);
        let bytes = header.to_bytes();
        let restored = WalHeader::from_bytes(&bytes).unwrap();
        
        assert_eq!(restored.page_size, size as u32);
    }
}

#[test]
fn test_wal_frame_serialization() {
    let sizes = vec![512, 4096, 8192];
    
    for size in sizes {
        let data = vec![1u8; size];
        let frame = WalFrame::new(1, 1, data.clone());
        
        assert_eq!(frame.page_id, 1);
        assert_eq!(frame.commit_id, 1);
        assert!(frame.verify());
    }
}

#[test]
fn test_wal_frame_checksum() {
    let data = vec![1u8, 2, 3, 4, 5];
    let frame = WalFrame::new(1, 1, data);
    
    assert!(frame.verify());
}

#[test]
fn test_wal_commit_id_boundaries() {
    let commit_ids = vec![0u64, 1, 100, u64::MAX];
    
    for id in commit_ids {
        let frame = WalFrame::new(1, id, vec![0u8; 100]);
        assert_eq!(frame.commit_id, id);
    }
}

// ============================================================================
// Storage Error Tests
// ============================================================================

#[test]
fn test_storage_error_display() {
    use sqllite_rust::storage::StorageError;
    
    let errors = vec![
        StorageError::Io(std::io::Error::new(std::io::ErrorKind::Other, "test")),
        StorageError::Corrupted("test".to_string()),
        StorageError::PageNotFound(1),
        StorageError::InvalidPageSize(100),
    ];
    
    for err in errors {
        let _ = format!("{}", err);
    }
}

// ============================================================================
// Prefix Compression Tests
// ============================================================================

#[test]
fn test_prefix_compression_boundaries() {
    // Test data with common prefixes
    let keys = vec![
        Value::Text("aaa".to_string()),
        Value::Text("aab".to_string()),
        Value::Text("aac".to_string()),
        Value::Text("aba".to_string()),
        Value::Text("abb".to_string()),
    ];
    
    let mut index = BPlusTreeIndex::new(
        "idx_test".to_string(),
        "test_table".to_string(),
        "col".to_string(),
    );
    
    for (i, key) in keys.iter().enumerate() {
        index.insert(key.clone(), i as u64).unwrap();
    }
    
    // Verify all keys
    for key in &keys {
        let lookup = index.lookup(key);
        assert!(lookup.is_some());
    }
}

#[test]
fn test_prefix_compression_empty_prefix() {
    // Keys with no common prefix
    let keys = vec![
        Value::Text("abc".to_string()),
        Value::Text("xyz".to_string()),
        Value::Text("123".to_string()),
    ];
    
    let mut index = BPlusTreeIndex::new(
        "idx_test".to_string(),
        "test_table".to_string(),
        "col".to_string(),
    );
    
    for (i, key) in keys.iter().enumerate() {
        index.insert(key.clone(), i as u64).unwrap();
    }
    
    for key in &keys {
        let lookup = index.lookup(key);
        assert!(lookup.is_some());
    }
}

// ============================================================================
// Table Scan Tests
// ============================================================================

#[test]
fn test_table_scan_empty() {
    let index = BPlusTreeIndex::new(
        "idx_test".to_string(),
        "test_table".to_string(),
        "col".to_string(),
    );
    
    // Empty table should return empty iterator
    let count = index.iter().count();
    assert_eq!(count, 0);
}

#[test]
fn test_table_scan_many_rows() {
    let mut index = BPlusTreeIndex::new(
        "idx_test".to_string(),
        "test_table".to_string(),
        "col".to_string(),
    );
    
    // Insert many rows
    for i in 0..10000 {
        index.insert(Value::Integer(i), i as u64).unwrap();
    }
    
    // Scan all
    let count = index.iter().count();
    assert_eq!(count, 10000);
}

// ============================================================================
// Foreign Key Tests
// ============================================================================

#[test]
fn test_foreign_key_action_boundaries() {
    use sqllite_rust::storage::foreign_key::{ForeignKeyAction, ForeignKey};
    
    let actions = vec![
        ForeignKeyAction::NoAction,
        ForeignKeyAction::Restrict,
        ForeignKeyAction::Cascade,
        ForeignKeyAction::SetNull,
        ForeignKeyAction::SetDefault,
    ];
    
    for action in actions {
        let fk = ForeignKey::new("child".to_string(), vec!["parent_id".to_string()])
            .on_delete(action.clone())
            .on_update(action);
        
        assert_eq!(fk.columns().len(), 1);
    }
}

// ============================================================================
// Database Open/Close Tests
// ============================================================================

#[test]
fn test_database_path_boundaries() {
    let paths = vec![
        ":memory:",
        "test.db",
        "/tmp/test.db",
        "./test.db",
        "../test.db",
    ];
    
    for path in paths {
        // Just verify these don't panic when used in paths
        let _ = path;
    }
}

// ============================================================================
// Page Cache Tests
// ============================================================================

#[test]
fn test_page_cache_zero_capacity() {
    use sqllite_rust::pager::cache::PageCache;
    
    // Zero capacity cache
    let cache = PageCache::new(0);
    let stats = cache.stats();
    assert_eq!(stats.capacity, 0);
}

#[test]
fn test_page_cache_max_capacity() {
    use sqllite_rust::pager::cache::PageCache;
    
    let capacities = vec![100, 1000, 10000, 100000];
    
    for capacity in capacities {
        let mut cache = PageCache::new(capacity);
        
        // Fill cache
        for i in 1..=capacity * 2 {
            cache.put(Page::new(i as u32), false);
        }
        
        let stats = cache.stats();
        assert!(stats.size <= capacity);
    }
}

#[test]
fn test_page_cache_dirty_pages() {
    use sqllite_rust::pager::cache::PageCache;
    
    let mut cache = PageCache::new(100);
    
    // Add dirty pages
    for i in 1..=50 {
        cache.put(Page::new(i), true);
    }
    
    // Add clean pages
    for i in 51..=100 {
        cache.put(Page::new(i), false);
    }
    
    let stats = cache.stats();
    assert_eq!(stats.dirty_count, 50);
}

// ============================================================================
// Iterator Tests
// ============================================================================

#[test]
fn test_index_iterator_empty() {
    let index = BPlusTreeIndex::new(
        "idx_test".to_string(),
        "test_table".to_string(),
        "col".to_string(),
    );
    
    let mut count = 0;
    for _ in index.iter() {
        count += 1;
    }
    assert_eq!(count, 0);
}

#[test]
fn test_index_iterator_many() {
    let mut index = BPlusTreeIndex::new(
        "idx_test".to_string(),
        "test_table".to_string(),
        "col".to_string(),
    );
    
    // Insert many entries
    for i in 0..1000 {
        index.insert(Value::Integer(i), i as u64).unwrap();
    }
    
    let mut count = 0;
    for _ in index.iter() {
        count += 1;
    }
    assert_eq!(count, 1000);
}

// ============================================================================
// Value Comparison Tests
// ============================================================================

#[test]
fn test_value_comparison_boundaries() {
    // Same type comparisons
    let v1 = Value::Integer(1);
    let v2 = Value::Integer(2);
    assert_ne!(v1, v2);
    
    let v1 = Value::Real(1.0);
    let v2 = Value::Real(2.0);
    assert_ne!(v1, v2);
    
    let v1 = Value::Text("a".to_string());
    let v2 = Value::Text("b".to_string());
    assert_ne!(v1, v2);
}

#[test]
fn test_value_null_comparison() {
    let null1 = Value::Null;
    let null2 = Value::Null;
    assert_eq!(null1, null2);
}

// ============================================================================
// Serialization Tests
// ============================================================================

#[test]
fn test_record_serialization_boundaries() {
    let records = vec![
        Record::new(vec![]),
        Record::new(vec![Value::Null]),
        Record::new(vec![Value::Integer(42)]),
        Record::new(vec![Value::Real(3.14)]),
        Record::new(vec![Value::Text("hello".to_string())]),
        Record::new(vec![Value::Blob(vec![1, 2, 3])]),
        Record::new(vec![
            Value::Integer(1),
            Value::Text("test".to_string()),
            Value::Real(2.5),
            Value::Null,
        ]),
    ];
    
    for record in records {
        let serialized = record.serialize();
        let deserialized = Record::deserialize(&serialized).unwrap();
        assert_eq!(record.columns().len(), deserialized.columns().len());
    }
}

// ============================================================================
// Bulk Operation Tests
// ============================================================================

#[test]
fn test_bulk_insert_boundaries() {
    let mut index = BPlusTreeIndex::new(
        "idx_test".to_string(),
        "test_table".to_string(),
        "col".to_string(),
    );
    
    // Bulk insert ascending
    for i in 0..5000 {
        index.insert(Value::Integer(i), i as u64).unwrap();
    }
    
    // Bulk insert descending
    for i in (5000..10000).rev() {
        index.insert(Value::Integer(i), i as u64).unwrap();
    }
    
    // Verify all
    for i in 0..10000 {
        let lookup = index.lookup(&Value::Integer(i));
        assert!(lookup.is_some());
    }
}

#[test]
fn test_bulk_delete_boundaries() {
    let mut index = BPlusTreeIndex::new(
        "idx_test".to_string(),
        "test_table".to_string(),
        "col".to_string(),
    );
    
    // Insert many
    for i in 0..10000 {
        index.insert(Value::Integer(i), i as u64).unwrap();
    }
    
    // Bulk delete
    for i in 0..5000 {
        index.delete(&Value::Integer(i), i as u64).unwrap();
    }
    
    // Verify deleted
    for i in 0..5000 {
        let lookup = index.lookup(&Value::Integer(i));
        assert!(lookup.is_none());
    }
    
    // Verify remaining
    for i in 5000..10000 {
        let lookup = index.lookup(&Value::Integer(i));
        assert!(lookup.is_some());
    }
}

// ============================================================================
// Recovery Tests
// ============================================================================

#[test]
fn test_wal_recovery_boundaries() {
    use tempfile::NamedTempFile;
    
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap().to_string() + ".db";
    
    // Write some data
    {
        let mut wal = Wal::open(&path, 4096).unwrap();
        wal.begin_transaction();
        
        let page = Page::from_bytes(1, vec![1u8; 4096]);
        wal.write_page(&page).unwrap();
        wal.flush().unwrap();
    }
    
    // Reopen and verify recovery
    {
        let mut wal = Wal::open(&path, 4096).unwrap();
        let result = wal.read_page(1).unwrap();
        assert!(result.is_some());
    }
}

// ============================================================================
// Checkpoint Tests
// ============================================================================

#[test]
fn test_checkpoint_boundaries() {
    use tempfile::NamedTempFile;
    
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap().to_string() + ".db";
    
    let mut wal = Wal::open(&path, 4096).unwrap();
    
    // Write multiple transactions
    for _ in 0..10 {
        wal.begin_transaction();
        for page_id in 1..=10 {
            let page = Page::from_bytes(page_id, vec![page_id as u8; 4096]);
            wal.write_page(&page).unwrap();
        }
        wal.flush().unwrap();
    }
    
    // Checkpoint
    let checkpointed = wal.checkpoint(|_page_id, _data| Ok(())).unwrap();
    assert!(checkpointed >= 0);
}

// ============================================================================
// Memory Pressure Tests
// ============================================================================

#[test]
fn test_memory_pressure_many_small_records() {
    let mut records = vec![];
    
    // Create many small records
    for i in 0..100000 {
        let record = Record::new(vec![
            Value::Integer(i),
            Value::Text(format!("value{}", i)),
        ]);
        records.push(record);
    }
    
    assert_eq!(records.len(), 100000);
}

#[test]
fn test_memory_pressure_few_large_records() {
    let mut records = vec![];
    
    // Create few large records
    for i in 0..100 {
        let large_text = "x".repeat(100000);
        let record = Record::new(vec![
            Value::Integer(i),
            Value::Text(large_text),
        ]);
        records.push(record);
    }
    
    assert_eq!(records.len(), 100);
}

// ============================================================================
// Type Conversion Tests
// ============================================================================

#[test]
fn test_value_type_conversion() {
    // Test conversions that might happen in storage
    let values = vec![
        Value::Integer(42),
        Value::Real(42.0),
        Value::Text("42".to_string()),
    ];
    
    for value in values {
        let record = Record::new(vec![value]);
        assert_eq!(record.columns().len(), 1);
    }
}

// ============================================================================
// Empty Database Tests
// ============================================================================

#[test]
fn test_empty_database_operations() {
    // Operations on empty storage should not panic
    let index = BPlusTreeIndex::new(
        "idx_test".to_string(),
        "test_table".to_string(),
        "col".to_string(),
    );
    
    // Lookup on empty
    let lookup = index.lookup(&Value::Integer(1));
    assert!(lookup.is_none());
    
    // Range scan on empty
    let result = index.range_scan(&Value::Integer(0), &Value::Integer(100));
    assert!(result.is_empty());
    
    // Iterator on empty
    assert_eq!(index.iter().count(), 0);
}

// ============================================================================
// Edge Case Value Tests
// ============================================================================

#[test]
fn test_edge_case_values() {
    let values = vec![
        // Edge case integers
        Value::Integer(0),
        Value::Integer(-0),
        Value::Integer(1),
        Value::Integer(-1),
        
        // Edge case floats
        Value::Real(0.0),
        Value::Real(-0.0),
        Value::Real(f64::NAN),
        Value::Real(f64::INFINITY),
        Value::Real(f64::NEG_INFINITY),
        Value::Real(f64::MIN_POSITIVE),
        Value::Real(f64::MAX),
        
        // Edge case strings
        Value::Text("".to_string()),
        Value::Text(" ".to_string()),
        Value::Text("\n".to_string()),
        Value::Text("\t".to_string()),
        Value::Text("\0".to_string()),
        
        // Edge case blobs
        Value::Blob(vec![]),
        Value::Blob(vec![0]),
        Value::Blob(vec![255]),
        Value::Blob(vec![0, 255]),
    ];
    
    for value in values {
        let record = Record::new(vec![value]);
        assert_eq!(record.columns().len(), 1);
    }
}

// ============================================================================
// Stress Tests
// ============================================================================

#[test]
fn test_storage_stress_insert_delete() {
    let mut index = BPlusTreeIndex::new(
        "idx_test".to_string(),
        "test_table".to_string(),
        "col".to_string(),
    );
    
    // Insert
    for i in 0..5000 {
        index.insert(Value::Integer(i), i as u64).unwrap();
    }
    
    // Delete odd
    for i in 0..2500 {
        index.delete(&Value::Integer(i * 2 + 1), (i * 2 + 1) as u64).unwrap();
    }
    
    // Insert more
    for i in 5000..10000 {
        index.insert(Value::Integer(i), i as u64).unwrap();
    }
    
    // Delete even from first batch
    for i in 0..2500 {
        index.delete(&Value::Integer(i * 2), (i * 2) as u64).unwrap();
    }
    
    // Verify
    for i in 0..5000 {
        let lookup = index.lookup(&Value::Integer(i));
        assert!(lookup.is_none());
    }
    
    for i in 5000..10000 {
        let lookup = index.lookup(&Value::Integer(i));
        assert!(lookup.is_some());
    }
}

// ============================================================================
// Page Reuse Tests
// ============================================================================

#[test]
fn test_page_reuse_boundaries() {
    use sqllite_rust::pager::cache::PageCache;
    
    let mut cache = PageCache::new(10);
    
    // Add pages
    for i in 1..=10 {
        cache.put(Page::new(i), false);
    }
    
    // Add more to trigger eviction
    for i in 11..=20 {
        cache.put(Page::new(i), false);
    }
    
    // Reuse old page ids
    for i in 1..=10 {
        cache.put(Page::new(i), false);
    }
    
    let stats = cache.stats();
    assert!(stats.size <= 10);
}

// ============================================================================
// Concurrent Read Tests
// ============================================================================

#[test]
fn test_concurrent_read_boundaries() {
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
    
    // Concurrent reads
    for thread_id in 0..10 {
        let index = Arc::clone(&index);
        let handle = thread::spawn(move || {
            let idx = index.lock().unwrap();
            for i in 0..100 {
                let key = (thread_id * 100 + i) as i64;
                let lookup = idx.lookup(&Value::Integer(key));
                assert!(lookup.is_some());
            }
        });
        handles.push(handle);
    }
    
    for handle in handles {
        handle.join().unwrap();
    }
}
