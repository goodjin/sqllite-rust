//! Pager Boundary Tests
//!
//! Tests for pager edge cases and boundary conditions

use sqllite_rust::pager::page::{Page, PageId};
use sqllite_rust::pager::cache::PageCache;

// ============================================================================
// Page Creation Tests
// ============================================================================

#[test]
fn test_page_creation_zero() {
    let page = Page::new(0);
    assert_eq!(page.id(), 0);
}

#[test]
fn test_page_creation_normal() {
    let page = Page::new(1);
    assert_eq!(page.id(), 1);
}

#[test]
fn test_page_creation_max() {
    let page = Page::new(u32::MAX);
    assert_eq!(page.id(), u32::MAX);
}

// ============================================================================
// Page Data Tests
// ============================================================================

#[test]
fn test_page_data_access() {
    let page = Page::new(1);
    let data = page.as_slice();
    assert!(!data.is_empty());
}

#[test]
fn test_page_data_mutable() {
    let mut page = Page::new(1);
    let data = page.as_mut_slice();
    data[0] = 42;
    assert_eq!(page.as_slice()[0], 42);
}

// ============================================================================
// Page from Bytes Tests
// ============================================================================

#[test]
fn test_page_from_bytes() {
    let sizes = vec![512, 1024, 2048, 4096, 8192];
    
    for size in sizes {
        let data = vec![0u8; size];
        let page = Page::from_bytes(1, data);
        assert_eq!(page.id(), 1);
    }
}

#[test]
fn test_page_from_bytes_empty() {
    let page = Page::from_bytes(1, vec![]);
    assert_eq!(page.id(), 1);
}

// ============================================================================
// Cache Tests
// ============================================================================

#[test]
fn test_cache_empty() {
    let mut cache = PageCache::new(10);
    assert!(cache.get(1).is_none());
}

#[test]
fn test_cache_single_page() {
    let mut cache = PageCache::new(10);
    cache.put(Page::new(1), false);
    assert!(cache.get(1).is_some());
}

#[test]
fn test_cache_eviction() {
    let mut cache = PageCache::new(2);
    
    cache.put(Page::new(1), false);
    cache.put(Page::new(2), false);
    cache.put(Page::new(3), false); // Should evict page 1
    
    assert!(cache.get(1).is_none());
    assert!(cache.get(2).is_some());
    assert!(cache.get(3).is_some());
}

#[test]
fn test_cache_dirty_pages() {
    let mut cache = PageCache::new(10);
    
    cache.put(Page::new(1), true);
    cache.put(Page::new(2), false);
    
    let dirty = cache.get_dirty_pages();
    assert!(dirty.contains(&1));
    assert!(!dirty.contains(&2));
}

#[test]
fn test_cache_clear() {
    let mut cache = PageCache::new(10);
    
    for i in 1..=10 {
        cache.put(Page::new(i), i % 2 == 0);
    }
    
    cache.clear();
    
    let stats = cache.stats();
    assert_eq!(stats.size, 0);
}

// ============================================================================
// Page ID Tests
// ============================================================================

#[test]
fn test_page_id_various() {
    let ids = vec![
        0u32,
        1,
        100,
        1000,
        10000,
        u32::MAX / 2,
        u32::MAX - 1,
        u32::MAX,
    ];
    
    for id in ids {
        let page = Page::new(id);
        assert_eq!(page.id(), id);
    }
}
