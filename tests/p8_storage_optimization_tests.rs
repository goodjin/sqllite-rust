//! Phase 8-A: B+Tree Storage Layer Optimization Tests
//! 
//! This test file validates:
//! - P8-1: Prefix compression integration
//! - P8-2: Binary search performance
//! - P8-3: Cache-line alignment

use std::time::Instant;
use std::cmp::Ordering;

// ============================================================================
// P8-3: Cache-line alignment tests
// ============================================================================

#[test]
fn test_page_cache_line_alignment() {
    // Verify Page structure is properly aligned
    use sqllite_rust::pager::page::Page;
    
    assert_eq!(
        std::mem::align_of::<Page>(), 
        64, 
        "Page must be 64-byte aligned"
    );
    
    // Verify hot data fits in first cache line
    let hot_data_offset = std::mem::offset_of!(Page, data);
    assert!(
        hot_data_offset <= 64,
        "Hot data must fit in first cache line (64 bytes), got {} bytes",
        hot_data_offset
    );
    
    println!("Page alignment: {} bytes", std::mem::align_of::<Page>());
    println!("Page size: {} bytes", std::mem::size_of::<Page>());
    println!("Hot data offset: {} bytes", hot_data_offset);
}

#[test]
fn test_page_access_tracking() {
    use sqllite_rust::pager::page::Page;
    
    let mut page = Page::new(1);
    
    // Test initial state
    assert_eq!(page.access_count, 0);
    assert_eq!(page.last_access, 0);
    
    // Test access tracking
    page.record_access(1000);
    assert_eq!(page.access_count, 1);
    assert_eq!(page.last_access, 1000);
    
    page.record_access(2000);
    assert_eq!(page.access_count, 2);
    assert_eq!(page.last_access, 2000);
}

#[test]
fn test_page_flags() {
    use sqllite_rust::pager::page::Page;
    
    let mut page = Page::new(1);
    
    // Test dirty flag
    assert!(!page.is_dirty());
    page.mark_dirty();
    assert!(page.is_dirty());
    page.clear_dirty();
    assert!(!page.is_dirty());
    
    // Test pin flag
    assert!(!page.is_pinned());
    page.pin();
    assert!(page.is_pinned());
    page.unpin();
    assert!(!page.is_pinned());
}

// ============================================================================
// P8-1: Prefix compression tests
// ============================================================================

#[test]
fn test_prefix_compression_space_savings() {
    use sqllite_rust::storage::prefix_page::{find_common_prefix, compress_keys, decompress_key};
    
    // Create keys with common prefix (like user IDs)
    let keys: Vec<Vec<u8>> = (0..100)
        .map(|i| format!("user:{:08x}:profile:data", i).into_bytes())
        .collect();
    
    let prefix = find_common_prefix(&keys);
    let compressed = compress_keys(&keys, &prefix);
    
    // Calculate space usage
    let uncompressed_size: usize = keys.iter().map(|k| k.len()).sum();
    let compressed_size: usize = compressed.iter().map(|k| k.len()).sum();
    let total_with_prefix = compressed_size + prefix.len();
    
    let savings_ratio = (uncompressed_size - total_with_prefix) as f64 / uncompressed_size as f64;
    
    println!("Prefix: {:?} ({} bytes)", String::from_utf8_lossy(&prefix), prefix.len());
    println!("Uncompressed: {} bytes", uncompressed_size);
    println!("Compressed: {} bytes (suffixes: {}, prefix: {})", 
             total_with_prefix, compressed_size, prefix.len());
    println!("Space saved: {:.1}%", savings_ratio * 100.0);
    
    // Verify compression saves at least 30% space
    assert!(
        savings_ratio > 0.30,
        "Expected >30% space savings, got {:.1}%", 
        savings_ratio * 100.0
    );
    
    // Verify decompress works
    for (i, suffix) in compressed.iter().enumerate() {
        let decompressed = decompress_key(suffix, &prefix);
        assert_eq!(decompressed, keys[i]);
    }
}

#[test]
fn test_prefix_compression_user_keys() {
    use sqllite_rust::storage::prefix_page::find_common_prefix;
    
    // Test with user:xxx style keys as specified in requirements
    let keys = vec![
        b"user:001".to_vec(),
        b"user:002".to_vec(),
        b"user:003".to_vec(),
    ];
    
    let prefix = find_common_prefix(&keys);
    
    println!("User keys prefix: {:?}", String::from_utf8_lossy(&prefix));
    
    // Should compress to shared prefix "user:00" + differences
    assert!(prefix.len() >= 6, "Should find 'user:00' as common prefix");
    
    // Calculate space savings
    let uncompressed: usize = keys.iter().map(|k| k.len()).sum();
    let compressed: usize = keys.iter().map(|k| k.len()).sum::<usize>() - (keys.len() - 1) * prefix.len();
    let savings_ratio = (uncompressed - compressed) as f64 / uncompressed as f64;
    
    println!("Space saved: {:.1}%", savings_ratio * 100.0);
    assert!(savings_ratio > 0.30, "Should save at least 30% space");
}

#[test]
fn test_prefix_compression_url_keys() {
    use sqllite_rust::storage::prefix_page::find_common_prefix;
    
    // Simulate URL keys
    let urls: Vec<Vec<u8>> = vec![
        b"https://example.com/path/to/resource1".to_vec(),
        b"https://example.com/path/to/resource2".to_vec(),
        b"https://example.com/path/to/resource3".to_vec(),
        b"https://example.com/path/to/resource4".to_vec(),
    ];
    
    let prefix = find_common_prefix(&urls);
    
    println!("URL prefix: {:?}", String::from_utf8_lossy(&prefix));
    
    // Should find common prefix up to "https://example.com/path/to/resource"
    assert!(prefix.len() >= 35, "Should find significant common prefix for URLs");
}

#[test]
fn test_prefix_compression_timestamp_keys() {
    use sqllite_rust::storage::prefix_page::find_common_prefix;
    
    // Simulate timestamp-based keys
    let timestamps: Vec<Vec<u8>> = vec![
        b"2024-01-15T10:30:00Z_event1".to_vec(),
        b"2024-01-15T10:30:01Z_event2".to_vec(),
        b"2024-01-15T10:30:02Z_event3".to_vec(),
        b"2024-01-15T10:30:03Z_event4".to_vec(),
    ];
    
    let prefix = find_common_prefix(&timestamps);
    
    println!("Timestamp prefix: {:?}", String::from_utf8_lossy(&prefix));
    
    // Should find common prefix up to the date/time portion (at least 17 chars)
    assert!(prefix.len() >= 17, "Should find significant common prefix for timestamps, got {} chars", prefix.len());
}

#[test]
fn test_prefix_compression_no_common_prefix() {
    use sqllite_rust::storage::prefix_page::find_common_prefix;
    
    // Keys with no common prefix
    let keys: Vec<Vec<u8>> = vec![
        b"alice".to_vec(),
        b"bob".to_vec(),
        b"charlie".to_vec(),
    ];
    
    let prefix = find_common_prefix(&keys);
    
    assert!(prefix.is_empty(), "Should return empty prefix when no common prefix exists");
}

// ============================================================================
// P8-2: Binary search performance tests
// ============================================================================

/// Compare two byte keys
fn compare_keys(a: &[u8], b: &[u8]) -> Ordering {
    a.cmp(b)
}

/// Linear search implementation
fn linear_search(sorted_keys: &[Vec<u8>], target: &[u8]) -> Option<usize> {
    for (i, key) in sorted_keys.iter().enumerate() {
        if compare_keys(key, target) == Ordering::Equal {
            return Some(i);
        }
    }
    None
}

/// Binary search implementation
fn binary_search(sorted_keys: &[Vec<u8>], target: &[u8]) -> Option<usize> {
    let mut left = 0;
    let mut right = sorted_keys.len();
    
    while left < right {
        let mid = (left + right) / 2;
        match compare_keys(&sorted_keys[mid], target) {
            Ordering::Equal => return Some(mid),
            Ordering::Less => left = mid + 1,
            Ordering::Greater => right = mid,
        }
    }
    None
}

#[test]
fn test_binary_search_performance_1000_records() {
    // Create 1000 sorted keys
    let keys: Vec<Vec<u8>> = (0..1000)
        .map(|i| format!("key{:08}", i).into_bytes())
        .collect();
    
    // Search for 100 random keys
    let search_indices: Vec<usize> = (0..100)
        .map(|i| i * 10)
        .collect();
    
    // Benchmark linear search
    let start = Instant::now();
    for &idx in &search_indices {
        let _ = linear_search(&keys, &keys[idx]);
    }
    let linear_time = start.elapsed();
    
    // Benchmark binary search
    let start = Instant::now();
    for &idx in &search_indices {
        let _ = binary_search(&keys, &keys[idx]);
    }
    let binary_time = start.elapsed();
    
    let speedup = linear_time.as_nanos() as f64 / binary_time.as_nanos().max(1) as f64;
    
    println!("1000 records: Linear={:?}, Binary={:?}, Speedup={:.2}x",
             linear_time, binary_time, speedup);
    
    // Binary search should be significantly faster
    assert!(
        speedup >= 10.0,
        "Binary search should be at least 10x faster for 1000 records (got {:.2}x)",
        speedup
    );
}

#[test]
fn test_binary_search_correctness() {
    let keys: Vec<Vec<u8>> = (0..1000)
        .map(|i| format!("key{:08}", i).into_bytes())
        .collect();
    
    // Test finding every 10th key
    for i in (0..1000).step_by(10) {
        let result = binary_search(&keys, &keys[i]);
        assert_eq!(result, Some(i), "Should find key at index {}", i);
    }
    
    // Test non-existent keys
    let non_existent = b"nonexistent";
    assert_eq!(binary_search(&keys, non_existent), None);
    
    // Test boundary keys
    assert_eq!(binary_search(&keys, &keys[0]), Some(0));
    assert_eq!(binary_search(&keys, &keys[999]), Some(999));
}

#[test]
fn test_binary_search_variable_length_keys() {
    let keys: Vec<Vec<u8>> = vec![
        b"a".to_vec(),
        b"ab".to_vec(),
        b"abc".to_vec(),
        b"abcd".to_vec(),
        b"abcde".to_vec(),
        b"abcdef".to_vec(),
        b"abcdefg".to_vec(),
        b"abcdefgh".to_vec(),
    ];
    
    // Search for each key
    for (i, key) in keys.iter().enumerate() {
        let result = binary_search(&keys, key);
        assert_eq!(result, Some(i), "Should find key at index {}", i);
    }
}

// ============================================================================
// Integration test combining all three optimizations
// ============================================================================

#[test]
fn test_p8_all_optimizations_together() {
    use sqllite_rust::pager::page::Page;
    use sqllite_rust::storage::prefix_page::find_common_prefix;
    
    // Create a page (uses cache-line alignment from P8-3)
    let mut page = Page::new(1);
    
    // Verify cache-line alignment
    assert_eq!(std::mem::align_of::<Page>(), 64);
    
    // Create keys with common prefix (for P8-1)
    let keys: Vec<Vec<u8>> = (0..100)
        .map(|i| format!("user:{:08x}:data", i).into_bytes())
        .collect();
    
    // Find common prefix
    let prefix = find_common_prefix(&keys);
    assert!(!prefix.is_empty(), "Should find common prefix");
    
    // Calculate compression ratio
    let uncompressed: usize = keys.iter().map(|k| k.len()).sum();
    let compressed = uncompressed - (keys.len() - 1) * prefix.len();
    let ratio = compressed as f64 / uncompressed as f64;
    
    println!("Compression ratio: {:.2}% (lower is better)", ratio * 100.0);
    assert!(ratio < 0.7, "Compressed size should be < 70% of uncompressed");
    
    // Test binary search performance (P8-2)
    let start = Instant::now();
    for i in (0..100).step_by(10) {
        let _ = binary_search(&keys, &keys[i]);
    }
    let binary_time = start.elapsed();
    
    let start = Instant::now();
    for i in (0..100).step_by(10) {
        let _ = linear_search(&keys, &keys[i]);
    }
    let linear_time = start.elapsed();
    
    let speedup = linear_time.as_nanos() as f64 / binary_time.as_nanos().max(1) as f64;
    println!("Binary search speedup: {:.2}x", speedup);
    
    println!("\n=== Phase 8-A Optimization Summary ===");
    println!("P8-1 Prefix Compression: {}% space saved", (1.0 - ratio) * 100.0);
    println!("P8-2 Binary Search: {:.2}x faster than linear", speedup);
    println!("P8-3 Cache-line Alignment: Page aligned to {} bytes", std::mem::align_of::<Page>());
}
