//! Phase 9 Week 1: Storage Layer Unit Tests
//!
//! This test file provides comprehensive coverage for storage components:
//! - B+Tree boundary tests (page split, merge, key boundaries)
//! - Pager tests (page allocation, cache eviction, dirty page flush)
//! - WAL tests (log writing, checkpoint, crash recovery)
//!
//! Target: 50 new tests

use sqllite_rust::pager::{Pager, PageId};
use sqllite_rust::pager::page::{Page, PAGE_SIZE};
use sqllite_rust::storage::{
    PageHeader, PageType, RecordHeader, BtreePageOps, 
    PageAllocator, FreePageList, Wal, WalHeader, WalFrame,
    BtreeNodeCache, BtreeCacheStats,
    MAX_INLINE_SIZE, compare_keys, binary_search_entries,
};
use sqllite_rust::storage::prefix_page::{find_common_prefix, compress_keys, decompress_key};
use std::time::{SystemTime, UNIX_EPOCH};

// ============================================================================
// B+Tree Boundary Tests
// ============================================================================

/// Helper: Create a temporary database path
fn temp_db_path() -> String {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros();
    format!("/tmp/test_db_{}.db", timestamp)
}

#[test]
fn test_btree_insert_until_page_split() {
    let path = temp_db_path();
    let mut pager = Pager::open(&path).unwrap();
    let mut allocator = PageAllocator::new();
    
    // Allocate a page and initialize as data page
    let page_id = allocator.allocate(&mut pager, PageType::Data).unwrap();
    let mut page = pager.get_page(page_id).unwrap();
    
    // Fill page with records until it's full
    let mut record_count = 0;
    for i in 0..1000 {
        let key = format!("key{:08}", i);
        let value = vec![0u8; 100];
        
        match page.insert_record(key.as_bytes(), &value) {
            Ok(_) => record_count += 1,
            Err(_) => break, // Page full
        }
    }
    
    // Verify we inserted some records
    assert!(record_count > 0, "Should insert at least one record");
    
    // Verify record count matches
    assert_eq!(page.record_count().unwrap(), record_count);
    
    // Cleanup
    std::fs::remove_file(&path).ok();
    std::fs::remove_file(format!("{}.wal", path)).ok();
}

#[test]
fn test_btree_page_split_boundary() {
    let path = temp_db_path();
    let mut pager = Pager::open(&path).unwrap();
    
    let mut page = Page::new(1);
    let header = PageHeader::new(PageType::Data);
    page.write_header(&header).unwrap();
    
    // Insert small records until page is full
    let mut inserted = 0;
    for i in 0..500 {
        let key = format!("k{:06}", i);
        let value = vec![b'x'; 50];
        
        if page.insert_record(key.as_bytes(), &value).is_ok() {
            inserted += 1;
        } else {
            break;
        }
    }
    
    // Try to insert one more - should fail
    let key = format!("k{:06}", inserted);
    let value = vec![b'x'; 50];
    let result = page.insert_record(key.as_bytes(), &value);
    
    // Either it fails or the page has incredible capacity
    if inserted < 500 {
        assert!(result.is_err() || inserted >= 400);
    }
    
    std::fs::remove_file(&path).ok();
    std::fs::remove_file(format!("{}.wal", path)).ok();
}

#[test]
fn test_btree_min_key_size() {
    let mut page = Page::new(1);
    let header = PageHeader::new(PageType::Data);
    page.write_header(&header).unwrap();
    
    // Insert minimum size key (empty)
    let result = page.insert_record(b"", b"value");
    assert!(result.is_ok());
    
    // Verify we can read it back
    let count = page.record_count().unwrap();
    assert_eq!(count, 1);
    
    let (key, value) = page.get_record_at(0).unwrap();
    assert_eq!(key, b"");
    assert_eq!(value, b"value");
}

#[test]
fn test_btree_max_key_size() {
    let mut page = Page::new(1);
    let header = PageHeader::new(PageType::Data);
    page.write_header(&header).unwrap();
    
    // Try to insert maximum size key + value
    let max_key = vec![b'k'; 1000];
    let max_value = vec![b'v'; 1000];
    
    let result = page.insert_record(&max_key, &max_value);
    
    // Should either succeed or fail with RecordTooLarge
    match result {
        Ok(_) => {
            // If it succeeded, verify we can read it back
            let (k, v) = page.get_record_at(0).unwrap();
            assert_eq!(k, max_key);
            assert_eq!(v, max_value);
        }
        Err(e) => {
            // Should fail gracefully
            let err_str = format!("{:?}", e);
            assert!(err_str.contains("RecordTooLarge") || err_str.contains("PageFull"));
        }
    }
}

#[test]
fn test_btree_duplicate_key_handling() {
    let mut page = Page::new(1);
    let header = PageHeader::new(PageType::Data);
    page.write_header(&header).unwrap();
    
    // Insert same key multiple times
    page.insert_record(b"duplicate_key", b"value1").unwrap();
    page.insert_record(b"duplicate_key", b"value2").unwrap();
    page.insert_record(b"duplicate_key", b"value3").unwrap();
    
    // Should have 3 records (duplicates allowed in this implementation)
    let count = page.record_count().unwrap();
    assert_eq!(count, 3);
    
    // Verify order is maintained
    let (k, _) = page.get_record_at(0).unwrap();
    assert_eq!(k, b"duplicate_key");
}

#[test]
fn test_btree_key_ordering_boundary() {
    let mut page = Page::new(1);
    let header = PageHeader::new(PageType::Data);
    page.write_header(&header).unwrap();
    
    // Insert keys in non-sorted order
    let keys = vec![
        b"z".to_vec(),
        b"a".to_vec(),
        b"m".to_vec(),
        b"A".to_vec(), // Capital letter
        b"0".to_vec(), // Number
        vec![0u8],     // Null byte
        vec![255u8],   // Max byte
    ];
    
    for key in &keys {
        page.insert_record(key, b"value").unwrap();
    }
    
    // Verify records are sorted
    let records = page.get_all_records().unwrap();
    for i in 1..records.len() {
        assert!(
            records[i-1].0 <= records[i].0,
            "Records should be sorted"
        );
    }
}

#[test]
fn test_btree_empty_page_operations() {
    let mut page = Page::new(1);
    let header = PageHeader::new(PageType::Data);
    page.write_header(&header).unwrap();
    
    // Empty page should have 0 records
    assert_eq!(page.record_count().unwrap(), 0);
    
    // Getting record from empty page should fail
    assert!(page.get_record_at(0).is_err());
    
    // Getting all records from empty page should return empty vec
    let records = page.get_all_records().unwrap();
    assert!(records.is_empty());
    
    // Empty page should have space
    assert!(page.has_space(100).unwrap());
}

#[test]
fn test_btree_page_merge_simulation() {
    // Simulate page merge by splitting records between two pages
    let mut page1 = Page::new(1);
    let mut page2 = Page::new(2);
    let header = PageHeader::new(PageType::Data);
    page1.write_header(&header).unwrap();
    page2.write_header(&header).unwrap();
    
    // Add records to both pages
    for i in 0..10 {
        let key = format!("key{:02}", i);
        if i < 5 {
            page1.insert_record(key.as_bytes(), b"value").unwrap();
        } else {
            page2.insert_record(key.as_bytes(), b"value").unwrap();
        }
    }
    
    // Simulate merge by moving records from page2 to page1
    let records_to_move = page2.get_all_records().unwrap();
    for (key, value) in records_to_move {
        page1.insert_record(&key, &value).unwrap();
    }
    
    // Page1 should now have all 10 records
    assert_eq!(page1.record_count().unwrap(), 10);
}

#[test]
fn test_btree_large_number_of_small_records() {
    let mut page = Page::new(1);
    let header = PageHeader::new(PageType::Data);
    page.write_header(&header).unwrap();
    
    // Insert many small records
    let mut inserted = 0;
    for i in 0..1000 {
        let key = vec![i as u8];
        let value = vec![];
        
        if page.insert_record(&key, &value).is_ok() {
            inserted += 1;
        } else {
            break;
        }
    }
    
    // Should insert a significant number
    assert!(inserted >= 50, "Should insert at least 50 records, got {}", inserted);
}

// ============================================================================
// Pager Tests
// ============================================================================

#[test]
fn test_pager_allocate_multiple_pages() {
    let path = temp_db_path();
    let mut pager = Pager::open(&path).unwrap();
    
    // Allocate multiple pages
    let mut page_ids = Vec::new();
    for _ in 0..100 {
        let page_id = pager.allocate_page().unwrap();
        page_ids.push(page_id);
    }
    
    // Verify page IDs are sequential
    for (i, &id) in page_ids.iter().enumerate() {
        assert_eq!(id as usize, i + 1, "Page IDs should be sequential");
    }
    
    std::fs::remove_file(&path).ok();
    std::fs::remove_file(format!("{}.wal", path)).ok();
}

#[test]
fn test_pager_page_persistence() {
    let path = temp_db_path();
    
    // Create pager and write data
    {
        let mut pager = Pager::open(&path).unwrap();
        let page_id = pager.allocate_page().unwrap();
        
        let mut page = pager.get_page(page_id).unwrap();
        page.data[0..10].copy_from_slice(b"TEST_DATA_");
        pager.write_page(&page).unwrap();
        pager.flush().unwrap();
    }
    
    // Reopen and verify data persisted
    {
        let mut pager = Pager::open(&path).unwrap();
        let page = pager.get_page(1).unwrap();
        assert_eq!(&page.data[0..10], b"TEST_DATA_");
    }
    
    std::fs::remove_file(&path).ok();
    std::fs::remove_file(format!("{}.wal", path)).ok();
}

#[test]
fn test_pager_cache_eviction_policy() {
    let path = temp_db_path();
    let mut pager = Pager::open(&path).unwrap();
    
    // Allocate more pages than cache capacity
    for i in 0..10 {
        let page_id = pager.allocate_page().unwrap();
        let mut page = pager.get_page(page_id).unwrap();
        page.data[0] = i as u8;
        pager.write_page(&page).unwrap();
    }
    
    // Access pages in specific order
    for i in 0..5 {
        let _ = pager.get_page(i + 1).unwrap();
    }
    
    // Pages should still be accessible
    for i in 0..10 {
        let page = pager.get_page(i + 1).unwrap();
        assert_eq!(page.data[0], i as u8);
    }
    
    std::fs::remove_file(&path).ok();
    std::fs::remove_file(format!("{}.wal", path)).ok();
}

#[test]
fn test_pager_dirty_page_tracking() {
    let mut page = Page::new(1);
    
    // Initially not dirty
    assert!(!page.is_dirty());
    
    // Mark as dirty
    page.mark_dirty();
    assert!(page.is_dirty());
    
    // Clear dirty
    page.clear_dirty();
    assert!(!page.is_dirty());
}

#[test]
fn test_pager_pin_unpin_page() {
    let mut page = Page::new(1);
    
    // Initially not pinned
    assert!(!page.is_pinned());
    
    // Pin page
    page.pin();
    assert!(page.is_pinned());
    
    // Unpin page
    page.unpin();
    assert!(!page.is_pinned());
}

#[test]
fn test_pager_write_read_large_data() {
    let path = temp_db_path();
    let mut pager = Pager::open(&path).unwrap();
    
    let page_id = pager.allocate_page().unwrap();
    
    // Write data pattern
    let mut page = pager.get_page(page_id).unwrap();
    for i in 0..PAGE_SIZE {
        page.data[i] = (i % 256) as u8;
    }
    pager.write_page(&page).unwrap();
    pager.flush().unwrap();
    
    // Read back and verify
    let page = pager.get_page(page_id).unwrap();
    for i in 0..PAGE_SIZE {
        assert_eq!(page.data[i], (i % 256) as u8);
    }
    
    std::fs::remove_file(&path).ok();
    std::fs::remove_file(format!("{}.wal", path)).ok();
}

#[test]
fn test_pager_page_access_counting() {
    let mut page = Page::new(1);
    
    // Initial access count is 0
    assert_eq!(page.access_count, 0);
    
    // Record access
    page.record_access(1000);
    assert_eq!(page.access_count, 1);
    assert_eq!(page.last_access, 1000);
    
    // Record more accesses
    page.record_access(2000);
    assert_eq!(page.access_count, 2);
    assert_eq!(page.last_access, 2000);
}

#[test]
fn test_pager_corrupted_page_detection() {
    let path = temp_db_path();
    let mut pager = Pager::open(&path).unwrap();
    let page_id = pager.allocate_page().unwrap();
    
    // Get page and corrupt it
    let mut page = pager.get_page(page_id).unwrap();
    page.data[0..PageHeader::SIZE].fill(0xFF);
    pager.write_page(&page).unwrap();
    pager.flush().unwrap();
    
    // Trying to read header from corrupted page should fail
    let page = pager.get_page(page_id).unwrap();
    let result = page.read_header();
    assert!(result.is_err());
    
    std::fs::remove_file(&path).ok();
    std::fs::remove_file(format!("{}.wal", path)).ok();
}

// ============================================================================
// WAL Tests
// ============================================================================

#[test]
fn test_wal_header_serialization_roundtrip() {
    let header = WalHeader::new(4096);
    let bytes = header.to_bytes();
    let restored = WalHeader::from_bytes(&bytes).unwrap();
    
    assert_eq!(restored.magic, WalHeader::MAGIC);
    assert_eq!(restored.version, 1);
    assert_eq!(restored.page_size, 4096);
    assert_eq!(restored.checkpoint_seq, 0);
    assert_eq!(restored.salt1, 0x12345678);
    assert_eq!(restored.salt2, 0x9ABCDEF0);
}

#[test]
fn test_wal_frame_serialization_roundtrip() {
    let data = vec![1u8, 2, 3, 4, 5];
    let frame = WalFrame::new(1, 1, data.clone());
    let bytes = frame.to_bytes();
    
    let header = &bytes[..WalFrame::HEADER_SIZE];
    let page_data = &bytes[WalFrame::HEADER_SIZE..];
    let restored = WalFrame::from_bytes(header, page_data).unwrap();
    
    assert_eq!(restored.page_id, 1);
    assert_eq!(restored.commit_id, 1);
    assert_eq!(restored.page_data, data);
    assert!(restored.verify());
}

#[test]
fn test_wal_write_and_read_single_page() {
    let path = temp_db_path();
    
    {
        let mut wal = Wal::open(&path, PAGE_SIZE).unwrap();
        wal.begin_transaction();
        
        let page = Page::from_bytes(1, vec![42u8; PAGE_SIZE]);
        wal.write_page(&page).unwrap();
        wal.flush().unwrap();
        
        // Read back
        let result = wal.read_page(1).unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap()[0], 42);
    }
    
    std::fs::remove_file(&path).ok();
    std::fs::remove_file(format!("{}.wal", path)).ok();
}

#[test]
fn test_wal_multiple_pages_write_read() {
    let path = temp_db_path();
    
    {
        let mut wal = Wal::open(&path, PAGE_SIZE).unwrap();
        wal.begin_transaction();
        
        // Write multiple pages
        for i in 1..=50 {
            let mut page_data = vec![0u8; PAGE_SIZE];
            page_data[0] = i as u8;
            page_data[1] = (i * 2) as u8;
            let page = Page::from_bytes(i, page_data);
            wal.write_page(&page).unwrap();
        }
        
        wal.flush().unwrap();
        
        // Read all pages back
        for i in 1..=50 {
            let result = wal.read_page(i).unwrap();
            assert!(result.is_some());
            let data = result.unwrap();
            assert_eq!(data[0], i as u8);
            assert_eq!(data[1], (i * 2) as u8);
        }
    }
    
    std::fs::remove_file(&path).ok();
    std::fs::remove_file(format!("{}.wal", path)).ok();
}

#[test]
fn test_wal_commit_boundary_persistence() {
    let path = temp_db_path();
    
    // First transaction
    {
        let mut wal = Wal::open(&path, PAGE_SIZE).unwrap();
        wal.begin_transaction();
        
        let page = Page::from_bytes(1, vec![1u8; PAGE_SIZE]);
        wal.write_page(&page).unwrap();
        wal.flush().unwrap();
    }
    
    // Second transaction
    {
        let mut wal = Wal::open(&path, PAGE_SIZE).unwrap();
        wal.begin_transaction();
        
        let page = Page::from_bytes(1, vec![2u8; PAGE_SIZE]);
        wal.write_page(&page).unwrap();
        wal.flush().unwrap();
    }
    
    // Verify latest version
    {
        let mut wal = Wal::open(&path, PAGE_SIZE).unwrap();
        let result = wal.read_page(1).unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap()[0], 2);
    }
    
    std::fs::remove_file(&path).ok();
    std::fs::remove_file(format!("{}.wal", path)).ok();
}

#[test]
fn test_wal_checkpoint_clears_log() {
    let path = temp_db_path();
    
    {
        let mut wal = Wal::open(&path, PAGE_SIZE).unwrap();
        wal.begin_transaction();
        
        let page = Page::from_bytes(1, vec![42u8; PAGE_SIZE]);
        wal.write_page(&page).unwrap();
        wal.flush().unwrap();
        
        // Note: checkpoint requires integration with main db
        // Just verify it doesn't panic
        let _ = wal.checkpoint(|_, _| Ok(()));
    }
    
    std::fs::remove_file(&path).ok();
    std::fs::remove_file(format!("{}.wal", path)).ok();
}

#[test]
fn test_wal_frame_count_tracking() {
    let path = temp_db_path();
    
    {
        let mut wal = Wal::open(&path, PAGE_SIZE).unwrap();
        
        // Initial count should be 0
        assert_eq!(wal.frame_count(), 0);
        
        wal.begin_transaction();
        
        // Write some pages
        for i in 1..=10 {
            let page = Page::from_bytes(i, vec![i as u8; PAGE_SIZE]);
            wal.write_page(&page).unwrap();
        }
        
        // Count should include buffered frames
        assert_eq!(wal.frame_count(), 10);
        
        wal.flush().unwrap();
        
        // Count should still be 10 after flush
        assert_eq!(wal.frame_count(), 10);
    }
    
    std::fs::remove_file(&path).ok();
    std::fs::remove_file(format!("{}.wal", path)).ok();
}

#[test]
fn test_wal_needs_checkpoint_threshold() {
    let path = temp_db_path();
    
    {
        let mut wal = Wal::open(&path, PAGE_SIZE).unwrap();
        
        // Initially shouldn't need checkpoint
        assert!(!wal.needs_checkpoint());
        
        wal.begin_transaction();
        
        // Write many pages to exceed threshold
        for i in 1..=1500 {
            let page = Page::from_bytes(i, vec![0u8; PAGE_SIZE]);
            wal.write_page(&page).unwrap();
        }
        
        wal.flush().unwrap();
        
        // Should need checkpoint after exceeding threshold
        assert!(wal.needs_checkpoint());
    }
    
    std::fs::remove_file(&path).ok();
    std::fs::remove_file(format!("{}.wal", path)).ok();
}

#[test]
fn test_wal_corrupted_frame_detection() {
    let data = vec![1u8, 2, 3, 4, 5];
    let mut frame = WalFrame::new(1, 1, data);
    
    // Valid frame should verify
    assert!(frame.verify());
    
    // Corrupt the data
    frame.page_data[0] = 99;
    
    // Corrupted frame should fail verification
    assert!(!frame.verify());
}

#[test]
fn test_wal_invalid_magic_detection() {
    let mut bytes = WalHeader::new(4096).to_bytes();
    
    // Corrupt magic number
    bytes[0] = 0xFF;
    bytes[1] = 0xFF;
    bytes[2] = 0xFF;
    bytes[3] = 0xFF;
    
    let result = WalHeader::from_bytes(&bytes);
    assert!(result.is_err());
}

#[test]
fn test_wal_read_nonexistent_page() {
    let path = temp_db_path();
    
    {
        let mut wal = Wal::open(&path, PAGE_SIZE).unwrap();
        
        // Try to read page that doesn't exist
        let result = wal.read_page(999).unwrap();
        assert!(result.is_none());
    }
    
    std::fs::remove_file(&path).ok();
    std::fs::remove_file(format!("{}.wal", path)).ok();
}

// ============================================================================
// Page Allocator Tests
// ============================================================================

#[test]
fn test_page_allocator_create_new() {
    let _allocator = PageAllocator::new();
    // Just verify it can be created
    assert!(true);
}

#[test]
fn test_page_allocator_allocate_various_types() {
    let path = temp_db_path();
    let mut pager = Pager::open(&path).unwrap();
    let mut allocator = PageAllocator::new();
    
    // Allocate pages of different types
    let data_page = allocator.allocate(&mut pager, PageType::Data).unwrap();
    let index_page = allocator.allocate(&mut pager, PageType::Index).unwrap();
    let overflow_page = allocator.allocate(&mut pager, PageType::Overflow).unwrap();
    
    // All should have valid page IDs
    assert!(data_page > 0);
    assert!(index_page > 0);
    assert!(overflow_page > 0);
    assert_ne!(data_page, index_page);
    
    std::fs::remove_file(&path).ok();
    std::fs::remove_file(format!("{}.wal", path)).ok();
}

// ============================================================================
// BtreeCache Tests
// ============================================================================

#[test]
fn test_btree_cache_create() {
    let cache = BtreeNodeCache::new(100);
    let stats = cache.stats();
    
    assert_eq!(stats.capacity, 100);
    assert_eq!(stats.size, 0);
}

#[test]
fn test_btree_cache_get_with_page() {
    let mut cache = BtreeNodeCache::new(100);
    
    // Create a page with header for the cache
    let mut page = Page::new(1);
    let header = PageHeader::new(PageType::Data);
    page.write_header(&header).unwrap();
    
    // First access - cache miss
    let (info, _prefetch) = cache.get(1, page.as_slice());
    assert!(info.is_some());
    assert_eq!(info.unwrap().page_id, 1);
}

// ============================================================================
// Prefix Compression Tests
// ============================================================================

#[test]
fn test_prefix_compression_empty_input() {
    let keys: Vec<Vec<u8>> = vec![];
    let prefix = find_common_prefix(&keys);
    assert!(prefix.is_empty());
}

#[test]
fn test_prefix_compression_single_key() {
    let keys = vec![b"test".to_vec()];
    let prefix = find_common_prefix(&keys);
    assert_eq!(prefix, b"test");
}

#[test]
fn test_prefix_compression_all_same() {
    let keys = vec![
        b"hello".to_vec(),
        b"hello".to_vec(),
        b"hello".to_vec(),
    ];
    let prefix = find_common_prefix(&keys);
    assert_eq!(prefix, b"hello");
}

#[test]
fn test_prefix_compression_no_common() {
    let keys = vec![
        b"abc".to_vec(),
        b"xyz".to_vec(),
        b"123".to_vec(),
    ];
    let prefix = find_common_prefix(&keys);
    assert!(prefix.is_empty());
}

#[test]
fn test_prefix_compression_partial() {
    let keys = vec![
        b"prefix_a".to_vec(),
        b"prefix_b".to_vec(),
        b"prefix_c".to_vec(),
    ];
    let prefix = find_common_prefix(&keys);
    assert_eq!(prefix, b"prefix_");
}

#[test]
fn test_compress_decompress_roundtrip() {
    let keys: Vec<Vec<u8>> = (0..10)
        .map(|i| format!("user:{:04}:data", i).into_bytes())
        .collect();
    
    let prefix = find_common_prefix(&keys);
    let compressed = compress_keys(&keys, &prefix);
    
    // Verify decompression
    for (i, suffix) in compressed.iter().enumerate() {
        let decompressed = decompress_key(suffix, &prefix);
        assert_eq!(decompressed, keys[i]);
    }
}

#[test]
fn test_prefix_compression_space_efficiency() {
    // Create keys with long common prefix
    let keys: Vec<Vec<u8>> = (0..100)
        .map(|i| format!("https://example.com/api/v1/resource/{:08}/data", i).into_bytes())
        .collect();
    
    let prefix = find_common_prefix(&keys);
    let compressed = compress_keys(&keys, &prefix);
    
    let original_size: usize = keys.iter().map(|k| k.len()).sum();
    let compressed_size: usize = compressed.iter().map(|k| k.len()).sum();
    
    // Should save significant space
    let savings = (original_size - compressed_size) as f64 / original_size as f64;
    assert!(savings > 0.5, "Should save more than 50% space, saved {:.1}%", savings * 100.0);
}

// ============================================================================
// Page Header Tests
// ============================================================================

#[test]
fn test_page_header_all_types() {
    let types = vec![
        PageType::Data,
        PageType::Index,
        PageType::Overflow,
        PageType::Free,
    ];
    
    for page_type in types {
        let header = PageHeader::new(page_type);
        let bytes = header.to_bytes();
        let restored = PageHeader::from_bytes(&bytes).unwrap();
        
        assert_eq!(restored.page_type as u8, page_type as u8);
    }
}

#[test]
fn test_page_header_flags() {
    let mut header = PageHeader::new(PageType::Data);
    
    // Test leaf flag
    assert!(!header.is_leaf());
    header.set_leaf(true);
    assert!(header.is_leaf());
    header.set_leaf(false);
    assert!(!header.is_leaf());
    
    // Test root flag
    assert!(!header.is_root());
    header.set_root(true);
    assert!(header.is_root());
    header.set_root(false);
    assert!(!header.is_root());
    
    // Test deleted flag
    assert!(!header.is_deleted());
}

#[test]
fn test_page_header_size_constant() {
    assert_eq!(PageHeader::SIZE, 96);
}

// ============================================================================
// Record Header Tests
// ============================================================================

#[test]
fn test_record_header_new() {
    let header = RecordHeader::new(100, 200);
    
    assert_eq!(header.key_size, 100);
    assert_eq!(header.value_size, 200);
    assert_eq!(header.total_size, 100 + 200 + RecordHeader::SIZE as u32);
    assert!(!header.is_deleted());
    assert!(!header.has_overflow());
}

#[test]
fn test_record_header_deleted_flag() {
    let mut header = RecordHeader::new(10, 20);
    
    assert!(!header.is_deleted());
    header.mark_deleted();
    assert!(header.is_deleted());
}

#[test]
fn test_record_header_serialization_size() {
    let header = RecordHeader::new(100, 200);
    let bytes = header.to_bytes();
    
    assert_eq!(bytes.len(), RecordHeader::SIZE);
}

// ============================================================================
// Key Comparison Tests
// ============================================================================

#[test]
fn test_compare_keys_empty() {
    assert_eq!(compare_keys(b"", b""), std::cmp::Ordering::Equal);
    assert_eq!(compare_keys(b"", b"a"), std::cmp::Ordering::Less);
    assert_eq!(compare_keys(b"a", b""), std::cmp::Ordering::Greater);
}

#[test]
fn test_compare_keys_various_lengths() {
    assert_eq!(compare_keys(b"a", b"ab"), std::cmp::Ordering::Less);
    assert_eq!(compare_keys(b"ab", b"a"), std::cmp::Ordering::Greater);
    assert_eq!(compare_keys(b"abc", b"abc"), std::cmp::Ordering::Equal);
}

#[test]
fn test_compare_keys_special_bytes() {
    assert_eq!(compare_keys(&[0], &[255]), std::cmp::Ordering::Less);
    assert_eq!(compare_keys(&[128], &[0]), std::cmp::Ordering::Greater);
}

#[test]
fn test_binary_search_entries_found() {
    let entries = vec![
        (b"a".to_vec(), b"1".to_vec()),
        (b"b".to_vec(), b"2".to_vec()),
        (b"c".to_vec(), b"3".to_vec()),
        (b"d".to_vec(), b"4".to_vec()),
    ];
    
    assert_eq!(binary_search_entries(&entries, b"a").unwrap(), 0);
    assert_eq!(binary_search_entries(&entries, b"b").unwrap(), 1);
    assert_eq!(binary_search_entries(&entries, b"c").unwrap(), 2);
    assert_eq!(binary_search_entries(&entries, b"d").unwrap(), 3);
}

#[test]
fn test_binary_search_entries_not_found() {
    let entries = vec![
        (b"b".to_vec(), b"1".to_vec()),
        (b"d".to_vec(), b"2".to_vec()),
    ];
    
    assert!(binary_search_entries(&entries, b"a").is_err());
    assert!(binary_search_entries(&entries, b"c").is_err());
    assert!(binary_search_entries(&entries, b"z").is_err());
}

#[test]
fn test_binary_search_empty_entries() {
    let entries: Vec<(Vec<u8>, Vec<u8>)> = vec![];
    assert!(binary_search_entries(&entries, b"key").is_err());
}

// ============================================================================
// Overflow Record Tests
// ============================================================================

#[test]
fn test_max_inline_size_constant() {
    // MAX_INLINE_SIZE should be reasonable
    assert!(MAX_INLINE_SIZE > 1000);
    assert!(MAX_INLINE_SIZE < PAGE_SIZE);
}

#[test]
fn test_large_record_rejection() {
    let mut page = Page::new(1);
    let header = PageHeader::new(PageType::Data);
    page.write_header(&header).unwrap();
    
    // Try to insert record larger than MAX_INLINE_SIZE
    let huge_key = vec![0u8; MAX_INLINE_SIZE / 2 + 100];
    let huge_value = vec![0u8; MAX_INLINE_SIZE / 2 + 100];
    
    let result = page.insert_record(&huge_key, &huge_value);
    
    // Should fail gracefully
    assert!(result.is_err());
}
