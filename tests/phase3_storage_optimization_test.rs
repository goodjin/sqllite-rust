//! Phase 3 - Storage Engine Optimization Tests
//!
//! Tests for:
//! - P3-1: B+Tree Prefix Compression
//! - P3-2: Page Prefetch Optimization
//! - P3-3: Adaptive Cache
//! - P3-4: Index Pushdown Filter
//! - P3-6: Page Checksum

use sqllite_rust::storage::prefix_page::{
    find_common_prefix, compress_keys, decompress_key, 
    KeyDistribution, BtreeConfig, GlobalCompressionStats,
    PrefixCompressionOps, PrefixCompressionStats
};
use sqllite_rust::pager::cache::{PageCache, CacheStats, AdaptiveCacheConfig};
use sqllite_rust::pager::prefetch::{PrefetchConfig, SequentialScanDetector, AccessPattern};
use sqllite_rust::pager::checksum::{calculate_crc32, verify_crc32, PageChecksumOps, ChecksumConfig};
use sqllite_rust::index::pushdown::{IndexFilter, extract_index_filter, IndexPushdownOptimizer};
use sqllite_rust::storage::Value;
use sqllite_rust::sql::ast::{Expression, BinaryOp};

// ============================================================================
// P3-1: Prefix Compression Tests
// ============================================================================

#[test]
fn test_p31_key_distribution_analysis() {
    // Keys with good common prefix
    let good_keys: Vec<Vec<u8>> = (0..50)
        .map(|i| format!("user:{:08x}:profile", i).into_bytes())
        .collect();

    let distribution = KeyDistribution::analyze(&good_keys);
    
    println!("Good keys distribution: {:?}", distribution);
    
    assert!(distribution.prefix_ratio >= 0.5, "Should have high prefix ratio");
    assert!(distribution.compression_score > 0.3, "Should have good compression score");
    
    // Keys with no common prefix
    let bad_keys: Vec<Vec<u8>> = vec![
        b"alice".to_vec(),
        b"bob".to_vec(),
        b"charlie".to_vec(),
    ];

    let bad_distribution = KeyDistribution::analyze(&bad_keys);
    
    println!("Bad keys distribution: {:?}", bad_distribution);
    
    assert_eq!(bad_distribution.prefix_ratio, 0.0);
    assert_eq!(bad_distribution.compression_score, 0.0);
}

#[test]
fn test_p31_adaptive_compression_decision() {
    let config = BtreeConfig::default();
    
    // Good keys should be compressed
    let good_keys: Vec<Vec<u8>> = (0..20)
        .map(|i| format!("user:{:08x}:data", i).into_bytes())
        .collect();
    
    let distribution = KeyDistribution::analyze(&good_keys);
    assert!(distribution.should_compress(&config), "Should enable compression for keys with common prefix");

    // Bad keys should not be compressed
    let bad_keys: Vec<Vec<u8>> = vec![
        b"alice".to_vec(),
        b"bob".to_vec(),
        b"charlie".to_vec(),
    ];

    let bad_distribution = KeyDistribution::analyze(&bad_keys);
    assert!(!bad_distribution.should_compress(&config), "Should not enable compression for keys without common prefix");
}

#[test]
fn test_p31_btree_config_presets() {
    let default = BtreeConfig::default();
    assert!(default.enable_prefix_compression);
    assert!(default.adaptive_compression);
    
    let conservative = BtreeConfig::conservative();
    assert!(conservative.enable_prefix_compression);
    assert!(conservative.min_prefix_ratio > default.min_prefix_ratio);
    
    let aggressive = BtreeConfig::aggressive();
    assert!(aggressive.enable_prefix_compression);
    assert!(aggressive.min_prefix_ratio < default.min_prefix_ratio);
    
    let disabled = BtreeConfig::disabled();
    assert!(!disabled.enable_prefix_compression);
}

#[test]
fn test_p31_prefix_compression_space_savings() {
    // Create test keys with common prefix
    let keys: Vec<Vec<u8>> = (0..100)
        .map(|i| format!("user:{:08x}:profile:data", i).into_bytes())
        .collect();
    
    let prefix = find_common_prefix(&keys);
    
    // Calculate space savings
    let total_uncompressed: usize = keys.iter().map(|k| k.len()).sum();
    let compressed_suffixes = compress_keys(&keys, &prefix);
    let total_compressed: usize = compressed_suffixes.iter().map(|s| s.len()).sum();
    
    let savings_ratio = (total_uncompressed - total_compressed) as f64 / total_uncompressed as f64;
    
    println!("Prefix: {:?} ({} bytes)", String::from_utf8_lossy(&prefix), prefix.len());
    println!("Uncompressed: {} bytes", total_uncompressed);
    println!("Compressed: {} bytes (suffixes: {}, prefix: {})", 
             total_compressed + prefix.len(), total_compressed, prefix.len());
    println!("Space saved: {:.1}%", savings_ratio * 100.0);
    
    // Should save at least 30%
    assert!(savings_ratio > 0.30, "Expected >30% space savings, got {:.1}%", savings_ratio * 100.0);
    
    // Verify decompress works
    for (i, suffix) in compressed_suffixes.iter().enumerate() {
        let decompressed = decompress_key(suffix, &prefix);
        assert_eq!(decompressed, keys[i]);
    }
}

// ============================================================================
// P3-2: Page Prefetch Tests
// ============================================================================

#[test]
fn test_p32_prefetch_config() {
    let default = PrefetchConfig::default();
    assert!(default.enabled);
    assert_eq!(default.prefetch_distance, 4);
    assert!(default.adaptive_window);
    
    let conservative = PrefetchConfig::conservative();
    assert!(conservative.enabled);
    assert_eq!(conservative.prefetch_distance, 2);
    
    let aggressive = PrefetchConfig::aggressive();
    assert!(aggressive.enabled);
    assert_eq!(aggressive.prefetch_distance, 8);
    
    let disabled = PrefetchConfig::disabled();
    assert!(!disabled.enabled);
}

#[test]
fn test_p32_sequential_scan_detector() {
    let mut detector = SequentialScanDetector::new(3);

    assert!(!detector.is_sequential());

    // Add sequential accesses
    detector.record_access(1);
    detector.record_access(2);
    assert!(!detector.is_sequential()); // Need 3

    detector.record_access(3);
    assert!(detector.is_sequential()); // Now sequential

    // Continue sequential
    detector.record_access(4);
    assert!(detector.is_sequential());

    // Break pattern
    detector.record_access(100);
    assert!(!detector.is_sequential());
}

#[test]
fn test_p32_access_pattern_detection() {
    use sqllite_rust::pager::prefetch::{PagePrefetcher, PageCache};
    
    let cache = PageCache::new(100);
    let config = PrefetchConfig::default();
    let mut prefetcher = PagePrefetcher::new(cache, config);

    // Initially unknown
    assert_eq!(prefetcher.access_pattern(), AccessPattern::Unknown);

    // Sequential pattern
    for i in 1..=5 {
        prefetcher.record_access(i);
    }
    assert_eq!(prefetcher.access_pattern(), AccessPattern::Sequential);
}

// ============================================================================
// P3-3: Adaptive Cache Tests
// ============================================================================

#[test]
fn test_p33_cache_temperature() {
    use sqllite_rust::pager::cache::CacheTemperature;
    
    assert_eq!(CacheTemperature::from_access_count(0), CacheTemperature::Cold);
    assert_eq!(CacheTemperature::from_access_count(1), CacheTemperature::Cold);
    assert_eq!(CacheTemperature::from_access_count(2), CacheTemperature::Cool);
    assert_eq!(CacheTemperature::from_access_count(5), CacheTemperature::Cool);
    assert_eq!(CacheTemperature::from_access_count(6), CacheTemperature::Warm);
    assert_eq!(CacheTemperature::from_access_count(20), CacheTemperature::Warm);
    assert_eq!(CacheTemperature::from_access_count(21), CacheTemperature::Hot);
}

#[test]
fn test_p33_adaptive_cache_config() {
    let config = AdaptiveCacheConfig::default();
    assert!(config.adaptive_sizing);
    assert!(config.target_hit_rate > 0.0);
    
    let cache = PageCache::with_config(config);
    assert!(cache.config().adaptive_sizing);
}

#[test]
fn test_p33_hit_rate_calculation() {
    let mut cache = PageCache::new(10);
    
    // Insert a page
    use sqllite_rust::pager::Page;
    cache.put(Page::new(1), false);
    
    // Multiple hits
    for _ in 0..5 {
        cache.get(1);
    }
    
    // Some misses
    for _ in 0..5 {
        cache.get(999);
    }
    
    let stats = cache.stats();
    assert!(stats.hit_rate > 0.0);
    assert!(stats.hit_rate < 1.0);
}

// ============================================================================
// P3-4: Index Pushdown Tests
// ============================================================================

#[test]
fn test_p34_index_filter_evaluate() {
    // Equality
    let eq_filter = IndexFilter::Eq { value: Value::Integer(10) };
    assert!(eq_filter.evaluate(&Value::Integer(10)));
    assert!(!eq_filter.evaluate(&Value::Integer(20)));

    // Range
    let range_filter = IndexFilter::Range {
        low: Value::Integer(10),
        high: Value::Integer(20),
        inclusive_low: true,
        inclusive_high: false,
    };
    assert!(range_filter.evaluate(&Value::Integer(10)));
    assert!(range_filter.evaluate(&Value::Integer(15)));
    assert!(!range_filter.evaluate(&Value::Integer(20)));
    assert!(!range_filter.evaluate(&Value::Integer(5)));

    // IN list
    let in_filter = IndexFilter::In {
        values: vec![Value::Integer(1), Value::Integer(2), Value::Integer(3)],
    };
    assert!(in_filter.evaluate(&Value::Integer(2)));
    assert!(!in_filter.evaluate(&Value::Integer(4)));

    // IS NULL
    let null_filter = IndexFilter::IsNull;
    assert!(null_filter.evaluate(&Value::Null));
    assert!(!null_filter.evaluate(&Value::Integer(1)));

    // AND
    let and_filter = IndexFilter::And(
        Box::new(IndexFilter::Gt { value: Value::Integer(10) }),
        Box::new(IndexFilter::Lt { value: Value::Integer(20) }),
    );
    assert!(!and_filter.evaluate(&Value::Integer(5)));
    assert!(!and_filter.evaluate(&Value::Integer(25)));
    assert!(and_filter.evaluate(&Value::Integer(15)));
}

#[test]
fn test_p34_extract_index_filter() {
    // Simple equality
    let expr = Expression::Binary {
        left: Box::new(Expression::Column("age".to_string())),
        op: BinaryOp::Equal,
        right: Box::new(Expression::Integer(25)),
    };

    let filter = extract_index_filter(&expr, "age");
    assert!(filter.is_some());
    
    if let Some(IndexFilter::Eq { value }) = filter {
        assert_eq!(value, Value::Integer(25));
    } else {
        panic!("Expected Eq filter");
    }

    // Different column - should not match
    let filter2 = extract_index_filter(&expr, "name");
    assert!(filter2.is_none());

    // Range condition
    let range_expr = Expression::Binary {
        left: Box::new(Expression::Column("age".to_string())),
        op: BinaryOp::Greater,
        right: Box::new(Expression::Integer(18)),
    };

    let range_filter = extract_index_filter(&range_expr, "age");
    assert!(matches!(range_filter, Some(IndexFilter::Gt { .. })));
}

#[test]
fn test_p34_pushdown_benefit() {
    let benefit = IndexPushdownOptimizer::estimate_benefit(
        10000,  // table rows
        0.5,    // index selects 50%
        0.2,    // filter keeps 20%
    );

    // Without pushdown: 5000 lookups
    // With pushdown: 1000 lookups
    // Saved: 4000 lookups
    assert_eq!(benefit.rows_saved, 4000);
    assert!((benefit.lookup_reduction_ratio - 0.8).abs() < 0.01);
    assert!(benefit.recommended);
}

// ============================================================================
// P3-6: Page Checksum Tests
// ============================================================================

#[test]
fn test_p36_crc32_calculation() {
    // Test vector: "123456789" should give 0xCBF43926
    let data = b"123456789";
    let checksum = calculate_crc32(data);
    assert_eq!(checksum, 0xCBF43926);
}

#[test]
fn test_p36_crc32_empty() {
    let checksum = calculate_crc32(b"");
    assert_eq!(checksum, 0x00000000);
}

#[test]
fn test_p36_crc32_verification() {
    let data = b"Hello, World!";
    let checksum = calculate_crc32(data);
    assert!(verify_crc32(data, checksum));
    assert!(!verify_crc32(data, checksum + 1));
}

#[test]
fn test_p36_page_checksum_ops() {
    use sqllite_rust::pager::Page;
    
    let mut page = Page::new(1);
    
    // Fill with some data
    for i in 0..100 {
        page.data[i + 4] = (i % 256) as u8;
    }
    
    // Calculate checksum
    let checksum = page.calculate_checksum();
    
    // Verify checksum is stored
    assert_eq!(page.get_checksum(), checksum);
    
    // Verify passes
    assert!(page.has_valid_checksum());
    assert!(page.verify_checksum().is_ok());
}

#[test]
fn test_p36_page_checksum_corruption() {
    use sqllite_rust::pager::Page;
    
    let mut page = Page::new(1);
    
    // Fill with data and calculate checksum
    for i in 0..100 {
        page.data[i + 4] = (i % 256) as u8;
    }
    page.calculate_checksum();
    
    // Corrupt the data
    page.data[10] ^= 0xFF;
    
    // Verification should fail
    assert!(!page.has_valid_checksum());
    assert!(page.verify_checksum().is_err());
}

#[test]
fn test_p36_checksum_config_presets() {
    let strict = ChecksumConfig::strict();
    assert!(strict.verify_on_read);
    assert!(strict.calculate_on_write);
    
    let disabled = ChecksumConfig::disabled();
    assert!(!disabled.verify_on_read);
    assert!(!disabled.calculate_on_write);
}

#[test]
fn test_p36_data_integrity() {
    // Test that different data produces different checksums
    let data1 = b"Hello, World!";
    let data2 = b"Hello, World?";
    
    let crc1 = calculate_crc32(data1);
    let crc2 = calculate_crc32(data2);
    
    assert_ne!(crc1, crc2);
    
    // Same data should produce same checksum
    let crc1_again = calculate_crc32(data1);
    assert_eq!(crc1, crc1_again);
}

// ============================================================================
// Integration Test: Phase 3 Complete
// ============================================================================

#[test]
fn test_phase3_all_features_summary() {
    println!("\n========================================");
    println!("Phase 3 - Storage Engine Optimization");
    println!("========================================\n");
    
    // P3-1: Prefix Compression
    println!("P3-1: Prefix Compression");
    let keys: Vec<Vec<u8>> = (0..100)
        .map(|i| format!("user:{:08x}:data", i).into_bytes())
        .collect();
    let prefix = find_common_prefix(&keys);
    let savings = {
        let uncompressed: usize = keys.iter().map(|k| k.len()).sum();
        let compressed = compress_keys(&keys, &prefix);
        let compressed_size: usize = compressed.iter().map(|s| s.len()).sum();
        (uncompressed - compressed_size) as f64 / uncompressed as f64 * 100.0
    };
    println!("  - Prefix length: {} bytes", prefix.len());
    println!("  - Space savings: {:.1}%", savings);
    assert!(savings > 30.0, "Should save at least 30% space");
    
    // P3-2: Page Prefetch
    println!("\nP3-2: Page Prefetch");
    let config = PrefetchConfig::default();
    println!("  - Prefetch distance: {}", config.prefetch_distance);
    println!("  - Adaptive window: {}", config.adaptive_window);
    assert!(config.adaptive_window);
    
    // P3-3: Adaptive Cache
    println!("\nP3-3: Adaptive Cache");
    let cache_config = AdaptiveCacheConfig::default();
    println!("  - Target hit rate: {:.0}%", cache_config.target_hit_rate * 100.0);
    println!("  - Adaptive sizing: {}", cache_config.adaptive_sizing);
    assert!(cache_config.target_hit_rate >= 0.8);
    
    // P3-4: Index Pushdown
    println!("\nP3-4: Index Pushdown");
    let benefit = IndexPushdownOptimizer::estimate_benefit(10000, 0.5, 0.2);
    println!("  - Estimated rows saved: {}", benefit.rows_saved);
    println!("  - Lookup reduction: {:.1}%", benefit.lookup_reduction_ratio * 100.0);
    assert!(benefit.rows_saved > 0);
    
    // P3-6: Page Checksum
    println!("\nP3-6: Page Checksum");
    let checksum = calculate_crc32(b"test data");
    println!("  - CRC32 implementation: OK");
    println!("  - Sample checksum: 0x{:08X}", checksum);
    assert!(verify_crc32(b"test data", checksum));
    
    println!("\n========================================");
    println!("Phase 3: All features verified!");
    println!("========================================");
}
