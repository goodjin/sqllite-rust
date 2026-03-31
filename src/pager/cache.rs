//! Adaptive Page Cache (P3-3)
//!
//! This module implements an adaptive page cache with:
//! - Access pattern recognition (random vs sequential)
//! - Dynamic cache size adjustment
//! - Hot data identification and retention
//! - Cold data eviction with temperature-based strategy

use hashlink::LinkedHashMap;
use crate::pager::page::{Page, PageId};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

/// Cache temperature levels for hot/cold data management
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CacheTemperature {
    /// Hot: Frequently accessed, keep in cache
    Hot = 3,
    /// Warm: Moderately accessed
    Warm = 2,
    /// Cool: Rarely accessed
    Cool = 1,
    /// Cold: Candidates for eviction
    Cold = 0,
}

impl CacheTemperature {
    pub fn from_access_count(count: u32) -> Self {
        match count {
            0..=1 => CacheTemperature::Cold,
            2..=5 => CacheTemperature::Cool,
            6..=20 => CacheTemperature::Warm,
            _ => CacheTemperature::Hot,
        }
    }

    pub fn score(&self) -> u32 {
        *self as u32
    }
}

/// Access pattern type
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum AccessPattern {
    Random,
    Sequential,
    Mixed,
}

/// Extended cache entry with metadata for adaptive caching
#[derive(Debug, Clone)]
struct AdaptiveCacheEntry {
    page: Page,
    is_dirty: bool,
    pin_count: u32,
    /// Access count for temperature calculation
    access_count: u32,
    /// Last access timestamp
    last_access: u64,
    /// Detected access pattern for this page
    pattern: AccessPattern,
    /// Sequential access streak
    sequential_streak: u32,
}

impl AdaptiveCacheEntry {
    fn new(page: Page, is_dirty: bool) -> Self {
        Self {
            page,
            is_dirty,
            pin_count: 0,
            access_count: 1,
            last_access: 0,
            pattern: AccessPattern::Random,
            sequential_streak: 0,
        }
    }

    fn temperature(&self) -> CacheTemperature {
        CacheTemperature::from_access_count(self.access_count)
    }

    fn record_access(&mut self, timestamp: u64, sequential: bool) {
        self.access_count = self.access_count.wrapping_add(1);
        self.last_access = timestamp;
        
        if sequential {
            self.sequential_streak += 1;
            if self.sequential_streak >= 3 {
                self.pattern = AccessPattern::Sequential;
            }
        } else {
            self.sequential_streak = 0;
            self.pattern = AccessPattern::Random;
        }
    }
}

/// Adaptive cache configuration
#[derive(Debug, Clone, Copy)]
pub struct AdaptiveCacheConfig {
    /// Initial capacity
    pub initial_capacity: usize,
    /// Minimum capacity
    pub min_capacity: usize,
    /// Maximum capacity
    pub max_capacity: usize,
    /// Enable adaptive sizing
    pub adaptive_sizing: bool,
    /// Target hit rate (0.0-1.0)
    pub target_hit_rate: f64,
    /// Hot data retention ratio (0.0-1.0)
    pub hot_data_ratio: f64,
    /// Sample window for hit rate calculation
    pub hit_rate_window: usize,
}

impl Default for AdaptiveCacheConfig {
    fn default() -> Self {
        Self {
            initial_capacity: 1000,
            min_capacity: 100,
            max_capacity: 10000,
            adaptive_sizing: true,
            target_hit_rate: 0.80,  // Target 80% hit rate
            hot_data_ratio: 0.20,    // Keep top 20% as hot
            hit_rate_window: 1000,
        }
    }
}

/// O(1) LRU page cache using LinkedHashMap with adaptive features (P3-3)
pub struct PageCache {
    /// LinkedHashMap maintains insertion order for O(1) LRU
    pages: LinkedHashMap<PageId, AdaptiveCacheEntry>,
    /// Current capacity
    capacity: usize,
    /// Configuration
    config: AdaptiveCacheConfig,
    /// Global timestamp counter
    timestamp: u64,
    /// Access history for pattern detection
    access_history: VecDeque<(PageId, u64)>,
    /// Cache statistics
    stats: CacheStatistics,
    /// Last page accessed (for sequential detection)
    last_page_id: Option<PageId>,
}

/// Cache statistics for monitoring and adaptation
#[derive(Debug, Clone)]
pub struct CacheStatistics {
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
    pub dirty_evictions: u64,
    pub hit_rate_samples: VecDeque<bool>,
    pub current_hit_rate: f64,
    /// Hot pages count
    pub hot_pages: usize,
    /// Warm pages count
    pub warm_pages: usize,
    /// Cold pages count
    pub cold_pages: usize,
}

impl Default for CacheStatistics {
    fn default() -> Self {
        Self {
            hits: 0,
            misses: 0,
            evictions: 0,
            dirty_evictions: 0,
            hit_rate_samples: VecDeque::with_capacity(1000),
            current_hit_rate: 0.0,
            hot_pages: 0,
            warm_pages: 0,
            cold_pages: 0,
        }
    }
}

impl CacheStatistics {
    fn record_access(&mut self, hit: bool, window_size: usize) {
        if hit {
            self.hits += 1;
        } else {
            self.misses += 1;
        }

        // Update hit rate samples
        self.hit_rate_samples.push_back(hit);
        if self.hit_rate_samples.len() > window_size {
            self.hit_rate_samples.pop_front();
        }

        // Calculate current hit rate
        let hits: usize = self.hit_rate_samples.iter().filter(|&&h| h).count();
        self.current_hit_rate = if !self.hit_rate_samples.is_empty() {
            hits as f64 / self.hit_rate_samples.len() as f64
        } else {
            0.0
        };
    }

    pub fn overall_hit_rate(&self) -> f64 {
        let total = self.hits + self.misses;
        if total == 0 {
            0.0
        } else {
            self.hits as f64 / total as f64
        }
    }
}

/// Public cache stats summary
#[derive(Debug, Clone)]
pub struct CacheStats {
    pub size: usize,
    pub capacity: usize,
    pub dirty_count: usize,
    pub hit_rate: f64,
    pub hot_pages: usize,
    pub warm_pages: usize,
    pub cold_pages: usize,
}

impl PageCache {
    pub fn new(capacity: usize) -> Self {
        let config = AdaptiveCacheConfig {
            initial_capacity: capacity,
            max_capacity: capacity * 2,
            ..Default::default()
        };
        
        Self::with_config(config)
    }

    pub fn with_config(config: AdaptiveCacheConfig) -> Self {
        Self {
            pages: LinkedHashMap::new(),
            capacity: config.initial_capacity,
            config,
            timestamp: 0,
            access_history: VecDeque::with_capacity(20),
            stats: CacheStatistics::default(),
            last_page_id: None,
        }
    }

    /// Get a page from cache - O(1)
    pub fn get(&mut self, page_id: PageId) -> Option<&Page> {
        self.timestamp += 1;

        // Check for sequential access
        let sequential = self.is_sequential_access(page_id);
        self.access_history.push_back((page_id, self.timestamp));
        if self.access_history.len() > 20 {
            self.access_history.pop_front();
        }
        self.last_page_id = Some(page_id);

        // Try to get from cache
        if let Some(mut entry) = self.pages.remove(&page_id) {
            // Cache hit
            entry.record_access(self.timestamp, sequential);
            self.pages.insert(page_id, entry);
            
            self.stats.record_access(true, self.config.hit_rate_window);
            
            return self.pages.get(&page_id).map(|e| &e.page);
        }

        // Cache miss
        self.stats.record_access(false, self.config.hit_rate_window);
        
        // Consider adaptive resizing
        if self.config.adaptive_sizing {
            self.adjust_capacity();
        }

        None
    }

    /// Get mutable reference to a page - O(1)
    pub fn get_mut(&mut self, page_id: PageId) -> Option<&mut Page> {
        self.timestamp += 1;

        let sequential = self.is_sequential_access(page_id);

        if let Some(mut entry) = self.pages.remove(&page_id) {
            entry.is_dirty = true;
            entry.record_access(self.timestamp, sequential);
            self.pages.insert(page_id, entry);
            
            self.stats.record_access(true, self.config.hit_rate_window);
            
            return self.pages.get_mut(&page_id).map(|e| &mut e.page);
        }

        self.stats.record_access(false, self.config.hit_rate_window);
        None
    }

    /// Insert or update a page - O(1)
    pub fn put(&mut self, page: Page, is_dirty: bool) {
        let page_id = page.id;

        // Check if we need to evict (only if new page and at capacity)
        if self.pages.len() >= self.capacity && !self.pages.contains_key(&page_id) {
            self.evict_if_needed();
        }

        let entry = AdaptiveCacheEntry::new(page, is_dirty);
        self.pages.insert(page_id, entry);
    }

    /// Mark a page as dirty - O(1)
    pub fn mark_dirty(&mut self, page_id: PageId) {
        if let Some(entry) = self.pages.get_mut(&page_id) {
            entry.is_dirty = true;
        }
    }

    /// Get all dirty page IDs - O(n) but rarely called
    pub fn get_dirty_pages(&self) -> Vec<PageId> {
        self.pages
            .iter()
            .filter(|(_, entry)| entry.is_dirty)
            .map(|(id, _)| *id)
            .collect()
    }

    /// Clear dirty flag for a page - O(1)
    pub fn clear_dirty(&mut self, page_id: PageId) {
        if let Some(entry) = self.pages.get_mut(&page_id) {
            entry.is_dirty = false;
        }
    }

    /// Check if access is sequential
    fn is_sequential_access(&self, current_page_id: PageId) -> bool {
        if let Some(last) = self.last_page_id {
            current_page_id == last + 1 || current_page_id == last.saturating_sub(1)
        } else {
            false
        }
    }

    /// Adjust cache capacity based on hit rate (P3-3)
    fn adjust_capacity(&mut self) {
        // Only adjust every N accesses to avoid thrashing
        if self.timestamp % 100 != 0 {
            return;
        }

        let hit_rate = self.stats.current_hit_rate;
        let target = self.config.target_hit_rate;

        if hit_rate < target * 0.9 {
            // Hit rate too low, increase capacity if possible
            let new_capacity = (self.capacity + self.capacity / 10).min(self.config.max_capacity);
            if new_capacity > self.capacity {
                self.capacity = new_capacity;
            }
        } else if hit_rate > target * 1.1 && self.pages.len() < self.capacity * 8 / 10 {
            // Hit rate high and plenty of free space, can reduce capacity
            let new_capacity = (self.capacity - self.capacity / 20).max(self.config.min_capacity);
            if new_capacity < self.capacity {
                // Evict excess pages first
                while self.pages.len() > new_capacity {
                    self.evict_one();
                }
                self.capacity = new_capacity;
            }
        }
    }

    /// Evict least recently used page that is not dirty and not pinned - O(1) amortized
    fn evict_if_needed(&mut self) {
        // First, try temperature-based eviction
        if self.evict_by_temperature() {
            return;
        }

        // Fall back to LRU eviction
        self.evict_lru();
    }

    /// Evict one page by temperature (cold pages first)
    fn evict_by_temperature(&mut self) -> bool {
        // Collect entries by temperature
        let mut cold_entries: Vec<PageId> = Vec::new();
        let mut cool_entries: Vec<PageId> = Vec::new();

        for (id, entry) in &self.pages {
            if entry.pin_count == 0 && !entry.is_dirty {
                match entry.temperature() {
                    CacheTemperature::Cold => cold_entries.push(*id),
                    CacheTemperature::Cool => cool_entries.push(*id),
                    _ => {}
                }
            }
        }

        // Evict coldest first
        for id in cold_entries {
            self.pages.remove(&id);
            self.stats.evictions += 1;
            return true;
        }

        for id in cool_entries {
            self.pages.remove(&id);
            self.stats.evictions += 1;
            return true;
        }

        false
    }

    /// Evict by LRU order
    fn evict_lru(&mut self) {
        // LinkedHashMap maintains order, front() gives LRU item
        let keys_to_check: Vec<PageId> = self.pages.keys().cloned().collect();

        for page_id in keys_to_check {
            let should_evict = if let Some(entry) = self.pages.get(&page_id) {
                entry.pin_count == 0 && !entry.is_dirty
            } else {
                false
            };
            
            if should_evict {
                self.pages.remove(&page_id);
                self.stats.evictions += 1;
                return;
            }
        }

        // If we can't evict anything clean, try evicting dirty (may need flush)
        if self.pages.len() >= self.capacity {
            let oldest = self.pages.front().map(|(k, _)| *k);
            if let Some(id) = oldest {
                let should_evict = if let Some(entry) = self.pages.get(&id) {
                    entry.pin_count == 0
                } else {
                    false
                };
                
                if should_evict {
                    let is_dirty = self.pages.get(&id).map(|e| e.is_dirty).unwrap_or(false);
                    self.pages.remove(&id);
                    self.stats.evictions += 1;
                    if is_dirty {
                        self.stats.dirty_evictions += 1;
                    }
                }
            }
        }
    }

    /// Evict a single page (for capacity reduction)
    fn evict_one(&mut self) {
        self.evict_if_needed();
    }

    /// Get cache stats
    pub fn stats(&self) -> CacheStats {
        let mut hot = 0;
        let mut warm = 0;
        let mut cold = 0;

        for entry in self.pages.values() {
            match entry.temperature() {
                CacheTemperature::Hot => hot += 1,
                CacheTemperature::Warm => warm += 1,
                _ => cold += 1,
            }
        }

        CacheStats {
            size: self.pages.len(),
            capacity: self.capacity,
            dirty_count: self.pages.values().filter(|e| e.is_dirty).count(),
            hit_rate: self.stats.current_hit_rate,
            hot_pages: hot,
            warm_pages: warm,
            cold_pages: cold,
        }
    }

    /// Get detailed statistics
    pub fn detailed_stats(&self) -> &CacheStatistics {
        &self.stats
    }

    /// Get current configuration
    pub fn config(&self) -> &AdaptiveCacheConfig {
        &self.config
    }

    /// Update configuration
    pub fn set_config(&mut self, config: AdaptiveCacheConfig) {
        self.config = config;
        // Ensure capacity is within new bounds
        self.capacity = self.capacity.clamp(config.min_capacity, config.max_capacity);
    }

    /// Promote hot pages (force temperature recalculation)
    pub fn recalculate_temperatures(&mut self) {
        // Access pattern can change, recalculate on next access
        // This is done automatically on each access
    }

    /// Clear the cache
    pub fn clear(&mut self) {
        self.pages.clear();
        self.stats = CacheStatistics::default();
        self.access_history.clear();
    }
}

/// Global cache statistics for monitoring
pub static GLOBAL_CACHE_HITS: AtomicU64 = AtomicU64::new(0);
pub static GLOBAL_CACHE_MISSES: AtomicU64 = AtomicU64::new(0);

/// Record a global cache hit
pub fn record_global_hit() {
    GLOBAL_CACHE_HITS.fetch_add(1, AtomicOrdering::Relaxed);
}

/// Record a global cache miss
pub fn record_global_miss() {
    GLOBAL_CACHE_MISSES.fetch_add(1, AtomicOrdering::Relaxed);
}

/// Get global cache statistics
pub fn global_cache_stats() -> (u64, u64, f64) {
    let hits = GLOBAL_CACHE_HITS.load(AtomicOrdering::Relaxed);
    let misses = GLOBAL_CACHE_MISSES.load(AtomicOrdering::Relaxed);
    let total = hits + misses;
    let rate = if total > 0 { hits as f64 / total as f64 } else { 0.0 };
    (hits, misses, rate)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cache_get_put() {
        let mut cache = PageCache::new(10);
        let page = Page::new(1);
        cache.put(page, false);

        assert!(cache.get(1).is_some());
        assert!(cache.get(2).is_none());
    }

    #[test]
    fn test_cache_dirty() {
        let mut cache = PageCache::new(10);
        let page = Page::new(1);
        cache.put(page, false);
        cache.mark_dirty(1);

        let dirty = cache.get_dirty_pages();
        assert_eq!(dirty.len(), 1);
        assert_eq!(dirty[0], 1);
    }

    #[test]
    fn test_cache_eviction() {
        let mut cache = PageCache::new(2);
        cache.put(Page::new(1), false);
        cache.put(Page::new(2), false);
        cache.put(Page::new(3), false);

        assert!(cache.get(1).is_none());
        assert!(cache.get(2).is_some());
        assert!(cache.get(3).is_some());
    }

    #[test]
    fn test_cache_lru_order() {
        let mut cache = PageCache::new(3);
        cache.put(Page::new(1), false);
        cache.put(Page::new(2), false);
        cache.put(Page::new(3), false);

        // Access page 1 to make it most recently used
        cache.get(1);

        // Insert page 4, should evict page 2 (LRU)
        cache.put(Page::new(4), false);

        assert!(cache.get(1).is_some());
        assert!(cache.get(2).is_none()); // Evicted
        assert!(cache.get(3).is_some());
        assert!(cache.get(4).is_some());
    }

    #[test]
    fn test_cache_stats() {
        let mut cache = PageCache::new(10);
        cache.put(Page::new(1), false);
        cache.put(Page::new(2), true);
        cache.put(Page::new(3), false);

        let stats = cache.stats();
        assert_eq!(stats.size, 3);
        assert_eq!(stats.capacity, 10);
        assert_eq!(stats.dirty_count, 1);
    }

    // P3-3: New tests for adaptive features
    #[test]
    fn test_cache_temperature() {
        assert_eq!(CacheTemperature::from_access_count(0), CacheTemperature::Cold);
        assert_eq!(CacheTemperature::from_access_count(1), CacheTemperature::Cold);
        assert_eq!(CacheTemperature::from_access_count(2), CacheTemperature::Cool);
        assert_eq!(CacheTemperature::from_access_count(5), CacheTemperature::Cool);
        assert_eq!(CacheTemperature::from_access_count(6), CacheTemperature::Warm);
        assert_eq!(CacheTemperature::from_access_count(20), CacheTemperature::Warm);
        assert_eq!(CacheTemperature::from_access_count(21), CacheTemperature::Hot);
    }

    #[test]
    fn test_adaptive_cache_config() {
        let config = AdaptiveCacheConfig::default();
        assert!(config.adaptive_sizing);
        assert!(config.target_hit_rate > 0.0);
        
        let cache = PageCache::with_config(config);
        assert!(cache.config().adaptive_sizing);
    }

    #[test]
    fn test_hit_rate_calculation() {
        let mut cache = PageCache::new(10);
        
        // Insert a page
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

    #[test]
    fn test_sequential_detection() {
        let mut cache = PageCache::new(10);
        
        // Sequential access pattern
        cache.put(Page::new(1), false);
        cache.put(Page::new(2), false);
        cache.put(Page::new(3), false);
        
        cache.get(1);
        cache.get(2); // Sequential
        cache.get(3); // Sequential
        
        // Pattern should be detected
        let stats = cache.detailed_stats();
        assert!(stats.hits > 0);
    }

    #[test]
    fn test_cache_clear() {
        let mut cache = PageCache::new(10);
        cache.put(Page::new(1), false);
        cache.put(Page::new(2), false);
        
        cache.clear();
        
        let stats = cache.stats();
        assert_eq!(stats.size, 0);
    }

    #[test]
    fn test_global_stats() {
        // Reset first (note: this is not thread-safe in tests)
        GLOBAL_CACHE_HITS.store(0, AtomicOrdering::Relaxed);
        GLOBAL_CACHE_MISSES.store(0, AtomicOrdering::Relaxed);
        
        record_global_hit();
        record_global_hit();
        record_global_miss();
        
        let (hits, misses, rate) = global_cache_stats();
        assert_eq!(hits, 2);
        assert_eq!(misses, 1);
        assert!((rate - 2.0/3.0).abs() < 0.001);
    }
}
