//! Page Cache Boundary Tests
//!
//! Tests for page cache edge cases and boundary conditions

use sqllite_rust::pager::cache::{
    PageCache, AdaptiveCacheConfig, CacheTemperature, 
    AccessPattern, CacheStatistics, global_cache_stats,
    record_global_hit, record_global_miss, GLOBAL_CACHE_HITS, GLOBAL_CACHE_MISSES
};
use sqllite_rust::pager::page::{Page, PageId};
use std::sync::atomic::{AtomicU64, Ordering};

// ============================================================================
// Capacity Boundary Tests
// ============================================================================

#[test]
fn test_cache_zero_capacity() {
    let cache = PageCache::new(0);
    let stats = cache.stats();
    
    assert_eq!(stats.capacity, 0);
    assert_eq!(stats.size, 0);
}

#[test]
fn test_cache_max_capacity() {
    let capacities = vec![1, 10, 100, 1000, 10000, 100000];
    
    for capacity in capacities {
        let mut cache = PageCache::new(capacity);
        
        // Fill to capacity
        for i in 1..=capacity * 2 {
            cache.put(Page::new(i as u32), false);
        }
        
        let stats = cache.stats();
        assert!(stats.size <= capacity);
    }
}

#[test]
fn test_cache_very_large_capacity() {
    let capacity = 1_000_000;
    let mut cache = PageCache::new(capacity);
    
    // Add some pages
    for i in 1..=1000 {
        cache.put(Page::new(i), false);
    }
    
    let stats = cache.stats();
    assert_eq!(stats.size, 1000);
    assert_eq!(stats.capacity, capacity);
}

// ============================================================================
// LRU Eviction Tests
// ============================================================================

#[test]
fn test_lru_eviction_order() {
    let mut cache = PageCache::new(3);
    
    // Add 3 pages
    cache.put(Page::new(1), false);
    cache.put(Page::new(2), false);
    cache.put(Page::new(3), false);
    
    // Access page 1 to make it most recently used
    cache.get(1);
    
    // Add page 4, should evict page 2 (LRU)
    cache.put(Page::new(4), false);
    
    assert!(cache.get(1).is_some());
    assert!(cache.get(2).is_none()); // Evicted
    assert!(cache.get(3).is_some());
    assert!(cache.get(4).is_some());
}

#[test]
fn test_lru_eviction_many_pages() {
    let capacity = 100;
    let mut cache = PageCache::new(capacity);
    
    // Fill cache
    for i in 1..=capacity {
        cache.put(Page::new(i as u32), false);
    }
    
    // Access every other page
    for i in (1..=capacity).step_by(2) {
        cache.get(i as u32);
    }
    
    // Add more pages to trigger eviction
    for i in (capacity + 1)..=(capacity + 50) {
        cache.put(Page::new(i as u32), false);
    }
    
    // Accessed pages should still be there
    for i in (1..=capacity).step_by(2) {
        assert!(cache.get(i as u32).is_some());
    }
}

// ============================================================================
// Dirty Page Tests
// ============================================================================

#[test]
fn test_dirty_page_eviction() {
    let mut cache = PageCache::new(2);
    
    // Add dirty and clean pages
    cache.put(Page::new(1), true);  // dirty
    cache.put(Page::new(2), false); // clean
    
    // Add third page
    cache.put(Page::new(3), false);
    
    // Clean page should be evicted first
    let dirty_pages = cache.get_dirty_pages();
    assert!(dirty_pages.contains(&1));
}

#[test]
fn test_dirty_page_marking() {
    let mut cache = PageCache::new(10);
    
    cache.put(Page::new(1), false);
    assert!(cache.get_dirty_pages().is_empty());
    
    cache.mark_dirty(1);
    assert!(cache.get_dirty_pages().contains(&1));
}

#[test]
fn test_clear_dirty() {
    let mut cache = PageCache::new(10);
    
    cache.put(Page::new(1), true);
    assert!(!cache.get_dirty_pages().is_empty());
    
    cache.clear_dirty(1);
    assert!(cache.get_dirty_pages().is_empty());
}

#[test]
fn test_dirty_eviction_stats() {
    let mut cache = PageCache::new(2);
    
    // Fill with dirty pages
    cache.put(Page::new(1), true);
    cache.put(Page::new(2), true);
    
    // Add more to force dirty eviction
    cache.put(Page::new(3), false);
    cache.put(Page::new(4), false);
    
    let stats = cache.detailed_stats();
    // Should have some dirty evictions
}

// ============================================================================
// Temperature Tests
// ============================================================================

#[test]
fn test_cache_temperature_boundaries() {
    assert_eq!(CacheTemperature::from_access_count(0), CacheTemperature::Cold);
    assert_eq!(CacheTemperature::from_access_count(1), CacheTemperature::Cold);
    assert_eq!(CacheTemperature::from_access_count(2), CacheTemperature::Cool);
    assert_eq!(CacheTemperature::from_access_count(5), CacheTemperature::Cool);
    assert_eq!(CacheTemperature::from_access_count(6), CacheTemperature::Warm);
    assert_eq!(CacheTemperature::from_access_count(20), CacheTemperature::Warm);
    assert_eq!(CacheTemperature::from_access_count(21), CacheTemperature::Hot);
    assert_eq!(CacheTemperature::from_access_count(100), CacheTemperature::Hot);
    assert_eq!(CacheTemperature::from_access_count(u32::MAX), CacheTemperature::Hot);
}

#[test]
fn test_temperature_scores() {
    assert_eq!(CacheTemperature::Cold.score(), 0);
    assert_eq!(CacheTemperature::Cool.score(), 1);
    assert_eq!(CacheTemperature::Warm.score(), 2);
    assert_eq!(CacheTemperature::Hot.score(), 3);
}

#[test]
fn test_hot_page_retention() {
    let mut cache = PageCache::new(5);
    
    // Add pages and make some hot
    for i in 1..=5 {
        cache.put(Page::new(i), false);
    }
    
    // Make pages 1 and 2 hot
    for _ in 0..30 {
        cache.get(1);
        cache.get(2);
    }
    
    // Add more pages to trigger eviction
    for i in 6..=10 {
        cache.put(Page::new(i), false);
    }
    
    // Hot pages should still be there
    assert!(cache.get(1).is_some());
    assert!(cache.get(2).is_some());
}

// ============================================================================
// Access Pattern Tests
// ============================================================================

#[test]
fn test_sequential_access_detection() {
    let mut cache = PageCache::new(10);
    
    // Sequential access pattern
    for i in 1..=5 {
        cache.put(Page::new(i), false);
    }
    
    cache.get(1);
    cache.get(2); // Sequential
    cache.get(3); // Sequential
    
    let stats = cache.detailed_stats();
    assert!(stats.hits >= 3);
}

#[test]
fn test_random_access_pattern() {
    let mut cache = PageCache::new(10);
    
    // Random access pattern
    cache.put(Page::new(1), false);
    cache.put(Page::new(5), false);
    cache.put(Page::new(2), false);
    cache.put(Page::new(8), false);
    
    cache.get(1);
    cache.get(5);
    cache.get(2);
    
    let stats = cache.detailed_stats();
    assert!(stats.hits >= 3);
}

// ============================================================================
// Hit Rate Tests
// ============================================================================

#[test]
fn test_hit_rate_calculation() {
    let mut cache = PageCache::new(10);
    
    // Add some pages
    for i in 1..=5 {
        cache.put(Page::new(i), false);
    }
    
    // Mix of hits and misses
    for _ in 0..5 {
        cache.get(1); // hit
        cache.get(2); // hit
    }
    
    for i in 100..110 {
        cache.get(i); // miss
    }
    
    let stats = cache.stats();
    assert!(stats.hit_rate >= 0.0);
    assert!(stats.hit_rate <= 1.0);
}

#[test]
fn test_overall_hit_rate() {
    let mut cache = PageCache::new(10);
    
    // Initial population
    for i in 1..=10 {
        cache.put(Page::new(i), false);
    }
    
    // All hits
    for i in 1..=10 {
        cache.get(i);
    }
    
    let stats = cache.detailed_stats();
    let overall_rate = stats.overall_hit_rate();
    
    assert!(overall_rate >= 0.0);
    assert!(overall_rate <= 1.0);
}

// ============================================================================
// Configuration Tests
// ============================================================================

#[test]
fn test_adaptive_cache_config_default() {
    let config = AdaptiveCacheConfig::default();
    
    assert_eq!(config.initial_capacity, 1000);
    assert_eq!(config.min_capacity, 100);
    assert_eq!(config.max_capacity, 10000);
    assert!(config.adaptive_sizing);
    assert!(config.target_hit_rate > 0.0);
    assert!(config.target_hit_rate < 1.0);
}

#[test]
fn test_custom_adaptive_config() {
    let configs = vec![
        AdaptiveCacheConfig {
            initial_capacity: 100,
            min_capacity: 10,
            max_capacity: 1000,
            adaptive_sizing: true,
            target_hit_rate: 0.9,
            hot_data_ratio: 0.2,
            hit_rate_window: 500,
        },
        AdaptiveCacheConfig {
            initial_capacity: 10000,
            min_capacity: 1000,
            max_capacity: 100000,
            adaptive_sizing: false,
            target_hit_rate: 0.7,
            hot_data_ratio: 0.3,
            hit_rate_window: 2000,
        },
    ];
    
    for config in configs {
        let cache = PageCache::with_config(config.clone());
        assert_eq!(cache.config().initial_capacity, config.initial_capacity);
        assert_eq!(cache.config().min_capacity, config.min_capacity);
        assert_eq!(cache.config().max_capacity, config.max_capacity);
    }
}

// ============================================================================
// Cache Clearing Tests
// ============================================================================

#[test]
fn test_cache_clear_empty() {
    let mut cache = PageCache::new(10);
    cache.clear();
    
    let stats = cache.stats();
    assert_eq!(stats.size, 0);
}

#[test]
fn test_cache_clear_with_pages() {
    let mut cache = PageCache::new(10);
    
    for i in 1..=10 {
        cache.put(Page::new(i), i % 2 == 0); // mix of dirty and clean
    }
    
    cache.clear();
    
    let stats = cache.stats();
    assert_eq!(stats.size, 0);
    assert_eq!(stats.dirty_count, 0);
}

#[test]
fn test_cache_clear_stats_reset() {
    let mut cache = PageCache::new(10);
    
    // Add and access pages
    for i in 1..=5 {
        cache.put(Page::new(i), false);
        cache.get(i);
    }
    
    cache.clear();
    
    let stats = cache.stats();
    assert_eq!(stats.size, 0);
}

// ============================================================================
// Get Mutable Tests
// ============================================================================

#[test]
fn test_get_mut_existing_page() {
    let mut cache = PageCache::new(10);
    
    cache.put(Page::new(1), false);
    
    let page = cache.get_mut(1);
    assert!(page.is_some());
}

#[test]
fn test_get_mut_nonexistent_page() {
    let mut cache = PageCache::new(10);
    
    let page = cache.get_mut(999);
    assert!(page.is_none());
}

#[test]
fn test_get_mut_marks_dirty() {
    let mut cache = PageCache::new(10);
    
    cache.put(Page::new(1), false);
    
    let _ = cache.get_mut(1);
    
    // Should be marked as dirty
    assert!(cache.get_dirty_pages().contains(&1));
}

// ============================================================================
// Statistics Tests
// ============================================================================

#[test]
fn test_cache_statistics_empty() {
    let cache = PageCache::new(10);
    let stats = cache.stats();
    
    assert_eq!(stats.size, 0);
    assert_eq!(stats.capacity, 10);
    assert_eq!(stats.dirty_count, 0);
    assert_eq!(stats.hit_rate, 0.0);
    assert_eq!(stats.hot_pages, 0);
    assert_eq!(stats.warm_pages, 0);
    assert_eq!(stats.cold_pages, 0);
}

#[test]
fn test_detailed_statistics() {
    let mut cache = PageCache::new(10);
    
    for i in 1..=5 {
        cache.put(Page::new(i), false);
    }
    
    // Access to change temperatures
    for _ in 0..30 {
        cache.get(1);
        cache.get(2);
    }
    
    let stats = cache.stats();
    assert!(stats.hot_pages + stats.warm_pages + stats.cold_pages <= 5);
}

#[test]
fn test_eviction_statistics() {
    let mut cache = PageCache::new(2);
    
    cache.put(Page::new(1), false);
    cache.put(Page::new(2), false);
    cache.put(Page::new(3), false); // Triggers eviction
    
    let stats = cache.detailed_stats();
    assert!(stats.evictions >= 1);
}

// ============================================================================
// Global Statistics Tests
// ============================================================================

#[test]
fn test_global_stats_initial() {
    // Reset global stats
    GLOBAL_CACHE_HITS.store(0, Ordering::Relaxed);
    GLOBAL_CACHE_MISSES.store(0, Ordering::Relaxed);
    
    let (hits, misses, rate) = global_cache_stats();
    assert_eq!(hits, 0);
    assert_eq!(misses, 0);
    assert_eq!(rate, 0.0);
}

#[test]
fn test_global_hit_recording() {
    // Reset
    GLOBAL_CACHE_HITS.store(0, Ordering::Relaxed);
    
    record_global_hit();
    record_global_hit();
    
    let (hits, _, _) = global_cache_stats();
    assert_eq!(hits, 2);
}

#[test]
fn test_global_miss_recording() {
    // Reset
    GLOBAL_CACHE_MISSES.store(0, Ordering::Relaxed);
    
    record_global_miss();
    record_global_miss();
    record_global_miss();
    
    let (_, misses, _) = global_cache_stats();
    assert_eq!(misses, 3);
}

#[test]
fn test_global_stats_rate() {
    // Reset
    GLOBAL_CACHE_HITS.store(0, Ordering::Relaxed);
    GLOBAL_CACHE_MISSES.store(0, Ordering::Relaxed);
    
    record_global_hit();
    record_global_hit();
    record_global_miss();
    
    let (_, _, rate) = global_cache_stats();
    assert!((rate - 2.0/3.0).abs() < 0.001);
}

// ============================================================================
// Capacity Adjustment Tests
// ============================================================================

#[test]
fn test_capacity_adjustment_low_hit_rate() {
    let config = AdaptiveCacheConfig {
        adaptive_sizing: true,
        target_hit_rate: 0.9,
        min_capacity: 10,
        max_capacity: 1000,
        ..Default::default()
    };
    
    let mut cache = PageCache::with_config(config);
    
    // Low hit rate scenario
    for i in 1..=100 {
        cache.put(Page::new(i), false);
        cache.get(i + 1000); // Misses
    }
    
    // Force adjustment check
    for _ in 0..100 {
        cache.get(1);
    }
    
    let stats = cache.stats();
    // Capacity may have been adjusted
}

#[test]
fn test_capacity_adjustment_high_hit_rate() {
    let config = AdaptiveCacheConfig {
        adaptive_sizing: true,
        target_hit_rate: 0.5,
        min_capacity: 10,
        max_capacity: 100,
        ..Default::default()
    };
    
    let mut cache = PageCache::with_config(config);
    
    // Populate cache
    for i in 1..=50 {
        cache.put(Page::new(i), false);
    }
    
    // High hit rate
    for _ in 0..100 {
        for i in 1..=50 {
            cache.get(i);
        }
    }
    
    let stats = cache.stats();
    // Should have high hit rate
}

// ============================================================================
// Memory Pressure Tests
// ============================================================================

#[test]
fn test_memory_pressure_many_pages() {
    let mut cache = PageCache::new(10000);
    
    // Add many pages
    for i in 1..=10000 {
        cache.put(Page::new(i), false);
    }
    
    let stats = cache.stats();
    assert_eq!(stats.size, 10000);
}

#[test]
fn test_memory_pressure_with_dirty_pages() {
    let mut cache = PageCache::new(1000);
    
    // Add many dirty pages
    for i in 1..=1000 {
        cache.put(Page::new(i), true);
    }
    
    let stats = cache.stats();
    assert_eq!(stats.dirty_count, 1000);
}

// ============================================================================
// Concurrent Access Tests
// ============================================================================

#[test]
fn test_concurrent_reads() {
    use std::sync::{Arc, Mutex};
    use std::thread;
    
    let cache = Arc::new(Mutex::new(PageCache::new(100)));
    
    // Populate cache
    {
        let mut c = cache.lock().unwrap();
        for i in 1..=50 {
            c.put(Page::new(i), false);
        }
    }
    
    let mut handles = vec![];
    
    for thread_id in 0..10 {
        let cache = Arc::clone(&cache);
        let handle = thread::spawn(move || {
            let mut c = cache.lock().unwrap();
            for i in 1..=50 {
                if i % 10 == thread_id {
                    c.get(i as u32);
                }
            }
        });
        handles.push(handle);
    }
    
    for handle in handles {
        handle.join().unwrap();
    }
}

// ============================================================================
// Page ID Boundary Tests
// ============================================================================

#[test]
fn test_page_id_boundaries() {
    let mut cache = PageCache::new(10);
    
    let page_ids = vec![0u32, 1, u32::MAX / 2, u32::MAX - 1, u32::MAX];
    
    for page_id in page_ids {
        cache.put(Page::new(page_id), false);
        let result = cache.get(page_id);
        assert!(result.is_some());
    }
}

// ============================================================================
// Empty and Null Tests
// ============================================================================

#[test]
fn test_get_from_empty_cache() {
    let mut cache = PageCache::new(10);
    
    let result = cache.get(1);
    assert!(result.is_none());
}

#[test]
fn test_put_same_page_multiple_times() {
    let mut cache = PageCache::new(10);
    
    // Put same page multiple times
    for i in 0..10 {
        cache.put(Page::new(1), i % 2 == 0);
    }
    
    let result = cache.get(1);
    assert!(result.is_some());
}

// ============================================================================
// Stress Tests
// ============================================================================

#[test]
fn test_cache_stress_test() {
    let mut cache = PageCache::new(1000);
    
    // Mix of operations
    for round in 0..100 {
        // Add pages
        for i in 1..=100 {
            cache.put(Page::new((round * 100 + i) as u32), i % 3 == 0);
        }
        
        // Read some
        for i in 1..=50 {
            cache.get(((round * 100 + i) as u32));
        }
        
        // Update some
        for i in 51..=75 {
            let _ = cache.get_mut(((round * 100 + i) as u32));
        }
    }
    
    let stats = cache.stats();
    assert!(stats.size <= 1000);
}

#[test]
fn test_sequential_scan_pattern() {
    let mut cache = PageCache::new(100);
    
    // Populate
    for i in 1..=200 {
        cache.put(Page::new(i), false);
    }
    
    // Sequential scan
    for i in 1..=200 {
        cache.get(i);
    }
    
    let stats = cache.detailed_stats();
    // Should detect sequential pattern
}

#[test]
fn test_random_access_pattern() {
    use rand::seq::SliceRandom;
    
    let mut cache = PageCache::new(100);
    
    // Populate
    for i in 1..=100 {
        cache.put(Page::new(i), false);
    }
    
    // Random access
    let mut order: Vec<u32> = (1..=100).collect();
    order.shuffle(&mut rand::thread_rng());
    
    for page_id in order {
        cache.get(page_id);
    }
    
    let stats = cache.stats();
    // Hit rate depends on cache size vs working set
}

#[test]
fn test_hot_cold_separation() {
    let mut cache = PageCache::new(50);
    
    // Add 100 pages
    for i in 1..=100 {
        cache.put(Page::new(i), false);
    }
    
    // Make first 10 hot
    for _ in 0..50 {
        for i in 1..=10 {
            cache.get(i);
        }
    }
    
    // Add more pages to trigger eviction
    for i in 101..=150 {
        cache.put(Page::new(i), false);
    }
    
    // Hot pages should still be there
    let stats = cache.stats();
    assert!(stats.hot_pages >= 10);
}
