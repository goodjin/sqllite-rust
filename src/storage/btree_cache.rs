//! B-tree Node Cache for Fast Key Lookups
//!
//! This module provides in-memory caching for B-tree node information
//! to avoid repeatedly parsing page headers and scanning slot arrays.
//!
//! Phase 1 Week 1 Optimizations:
//! - Cache hit/miss statistics with detailed metrics
//! - Node-level caching with pre-computed binary search structures
//! - Cache warming for sequential access patterns
//! - Optimized eviction strategy for B-tree access patterns (LRU-K inspired)

use crate::pager::PageId;
use crate::pager::page::PAGE_SIZE;
use crate::storage::btree_engine::{PageHeader, RecordHeader};
use hashlink::LinkedHashMap;
use std::time::Instant;

/// Cached information about a B-tree node
#[derive(Debug, Clone)]
pub struct BtreeNodeInfo {
    /// Page ID
    pub page_id: PageId,
    /// Page header (cached)
    pub header: PageHeader,
    /// Sorted list of (key, record_offset) pairs
    /// This avoids scanning the slot array on every access
    pub key_offsets: Vec<(Vec<u8>, usize)>,
    /// Total size of all records in this node
    pub total_record_size: usize,
    /// Whether this cache entry is dirty
    pub is_dirty: bool,
    /// Access statistics for LRU-K eviction
    pub access_history: AccessHistory,
}

/// Access history for LRU-K eviction policy
#[derive(Debug, Clone)]
pub struct AccessHistory {
    /// Last access time
    pub last_access: Instant,
    /// Number of accesses
    pub access_count: u64,
    /// Penalty score (higher = less likely to evict)
    pub penalty_score: f64,
}

impl Default for AccessHistory {
    fn default() -> Self {
        Self {
            last_access: Instant::now(),
            access_count: 0,
            penalty_score: 1.0,
        }
    }
}

impl AccessHistory {
    /// Record an access and update penalty score
    pub fn record_access(&mut self) {
        self.last_access = Instant::now();
        self.access_count += 1;
        // Higher access count = higher penalty = less likely to evict
        // Use logarithmic scaling to prevent unbounded growth
        self.penalty_score = 1.0 + (self.access_count as f64).ln().max(0.0);
    }
    
    /// Calculate eviction score (lower = more likely to evict)
    pub fn eviction_score(&self, now: Instant) -> f64 {
        let age_ms = now.duration_since(self.last_access).as_millis() as f64;
        // Age divided by penalty = weighted age
        // Nodes with high penalty (frequently accessed) get lower score
        age_ms / self.penalty_score
    }
}

impl BtreeNodeInfo {
    /// Build node info from page data
    pub fn from_page(page_id: PageId, page_data: &[u8]) -> Option<Self> {
        let header = PageHeader::from_bytes(page_data).ok()?;
        let record_count = header.record_count as usize;

        let mut key_offsets = Vec::with_capacity(record_count);
        let mut total_size = 0;

        for slot_idx in 0..record_count {
            let slot_offset = PageHeader::SIZE + slot_idx * 2;
            if slot_offset + 2 > page_data.len() {
                break;
            }

            let record_offset = u16::from_le_bytes([
                page_data[slot_offset],
                page_data[slot_offset + 1],
            ]) as usize;

            if record_offset + RecordHeader::SIZE > page_data.len() {
                continue;
            }

            // Read record header
            if let Ok(rec_header) =
                RecordHeader::from_bytes(&page_data[record_offset..])
            {
                if !rec_header.is_deleted() {
                    let key_start = record_offset + RecordHeader::SIZE;
                    let key_end = key_start + rec_header.key_size as usize;

                    if key_end <= page_data.len() {
                        let key = page_data[key_start..key_end].to_vec();
                        key_offsets.push((key, record_offset));
                        total_size += rec_header.total_size as usize;
                    }
                }
            }
        }

        // Sort by key for binary search
        key_offsets.sort_by(|a, b| a.0.cmp(&b.0));

        Some(Self {
            page_id,
            header,
            key_offsets,
            total_record_size: total_size,
            is_dirty: false,
            access_history: AccessHistory::default(),
        })
    }

    /// Binary search for a key
    pub fn find_key(&self, target: &[u8]) -> Option<usize> {
        self.key_offsets
            .binary_search_by(|(key, _)| key.as_slice().cmp(target))
            .ok()
            .map(|idx| self.key_offsets[idx].1)
    }

    /// Find the insertion position for a key
    pub fn find_insert_position(&self, target: &[u8]) -> usize {
        match self
            .key_offsets
            .binary_search_by(|(key, _)| key.as_slice().cmp(target))
        {
            Ok(idx) => idx,
            Err(idx) => idx,
        }
    }

    /// Get remaining space in this node
    pub fn remaining_space(&self) -> usize {
        PAGE_SIZE - PageHeader::SIZE - self.total_record_size
            - self.key_offsets.len() * 2 // slot array
    }

    /// Check if page has space for a record of given size
    pub fn has_space(&self, record_size: usize) -> bool {
        // Need space for: record + slot entry + some margin
        self.remaining_space() >= record_size + 2 + 10
    }

    /// Get median key (for splitting)
    pub fn median_key(&self) -> Option<&[u8]> {
        let mid = self.key_offsets.len() / 2;
        self.key_offsets.get(mid).map(|(k, _)| k.as_slice())
    }
    
    /// Record an access for LRU-K
    pub fn record_access(&mut self) {
        self.access_history.record_access();
    }
    
    /// Get access count
    pub fn access_count(&self) -> u64 {
        self.access_history.access_count
    }
}

/// Prefetch configuration for sequential scans
#[derive(Debug, Clone)]
pub struct PrefetchConfig {
    /// Enable prefetch
    pub enabled: bool,
    /// Number of pages to prefetch ahead
    pub distance: usize,
    /// Track sequential access patterns
    pub track_sequential: bool,
}

impl Default for PrefetchConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            distance: 4,  // Prefetch 4 pages ahead
            track_sequential: true,
        }
    }
}

/// Access pattern tracker for prefetch
#[derive(Debug, Default)]
struct AccessPattern {
    last_page_id: Option<PageId>,
    sequential_count: u32,
}

/// Cache warming strategy
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum WarmingStrategy {
    /// No warming
    None,
    /// Warm first N pages (for small tables)
    FirstN(usize),
    /// Warm pages at regular intervals (for large tables)
    Interval(usize),
    /// Warm based on index statistics (hot pages)
    HotPages,
}

impl Default for WarmingStrategy {
    fn default() -> Self {
        WarmingStrategy::FirstN(10)
    }
}

/// Detailed cache statistics for analysis
#[derive(Debug, Clone, Default)]
pub struct DetailedCacheStats {
    /// Total cache hits
    pub hits: u64,
    /// Total cache misses
    pub misses: u64,
    /// Cache hits since last reset
    pub recent_hits: u64,
    /// Cache misses since last reset
    pub recent_misses: u64,
    /// Number of evictions performed
    pub evictions: u64,
    /// Number of prefetch hits (accessed page that was prefetched)
    pub prefetch_hits: u64,
    /// Number of pages prefetched
    pub pages_prefetched: u64,
    /// Sequential access patterns detected
    pub sequential_patterns: u64,
    /// Random access patterns detected
    pub random_patterns: u64,
    /// Cache warming operations performed
    pub warming_operations: u64,
    /// Pages warmed
    pub pages_warmed: u64,
}

impl DetailedCacheStats {
    /// Calculate hit rate (0.0 to 1.0)
    pub fn hit_rate(&self) -> f64 {
        let total = self.hits + self.misses;
        if total == 0 {
            0.0
        } else {
            self.hits as f64 / total as f64
        }
    }
    
    /// Calculate recent hit rate
    pub fn recent_hit_rate(&self) -> f64 {
        let total = self.recent_hits + self.recent_misses;
        if total == 0 {
            0.0
        } else {
            self.recent_hits as f64 / total as f64
        }
    }
    
    /// Reset recent counters
    pub fn reset_recent(&mut self) {
        self.recent_hits = 0;
        self.recent_misses = 0;
    }
}

/// LRU-K inspired cache for B-tree nodes with prefetch and warming support
pub struct BtreeNodeCache {
    nodes: LinkedHashMap<PageId, BtreeNodeInfo>,
    capacity: usize,
    /// Detailed cache statistics
    stats: DetailedCacheStats,
    /// Prefetch configuration
    prefetch: PrefetchConfig,
    /// Access pattern tracking (page_id -> last access pattern)
    access_patterns: hashlink::LinkedHashMap<PageId, AccessPattern>,
    /// Pages to prefetch (queue)
    prefetch_queue: Vec<PageId>,
    /// Warming strategy
    warming_strategy: WarmingStrategy,
    /// Max tracked access patterns
    max_pattern_track: usize,
    /// Cache creation time for statistics
    created_at: Instant,
    /// Last accessed page (for global sequential detection)
    last_accessed_page: Option<PageId>,
    /// Sequential streak counter
    sequential_streak: u32,
}

impl BtreeNodeCache {
    /// Default cache capacity: 1000 nodes (~4MB for typical B-tree)
    pub const DEFAULT_CAPACITY: usize = 1000;
    /// Max tracked access patterns
    const MAX_PATTERN_TRACK: usize = 100;

    pub fn new(capacity: usize) -> Self {
        Self {
            nodes: LinkedHashMap::new(),
            capacity,
            stats: DetailedCacheStats::default(),
            prefetch: PrefetchConfig::default(),
            access_patterns: hashlink::LinkedHashMap::new(),
            prefetch_queue: Vec::new(),
            warming_strategy: WarmingStrategy::default(),
            max_pattern_track: Self::MAX_PATTERN_TRACK,
            created_at: Instant::now(),
            last_accessed_page: None,
            sequential_streak: 0,
        }
    }

    /// Create with custom prefetch config
    pub fn with_prefetch(capacity: usize, prefetch: PrefetchConfig) -> Self {
        Self {
            nodes: LinkedHashMap::new(),
            capacity,
            stats: DetailedCacheStats::default(),
            prefetch,
            access_patterns: hashlink::LinkedHashMap::new(),
            prefetch_queue: Vec::new(),
            warming_strategy: WarmingStrategy::default(),
            max_pattern_track: Self::MAX_PATTERN_TRACK,
            created_at: Instant::now(),
            last_accessed_page: None,
            sequential_streak: 0,
        }
    }
    
    /// Create with custom warming strategy
    pub fn with_warming(capacity: usize, strategy: WarmingStrategy) -> Self {
        Self {
            nodes: LinkedHashMap::new(),
            capacity,
            stats: DetailedCacheStats::default(),
            prefetch: PrefetchConfig::default(),
            access_patterns: hashlink::LinkedHashMap::new(),
            prefetch_queue: Vec::new(),
            warming_strategy: strategy,
            max_pattern_track: Self::MAX_PATTERN_TRACK,
            created_at: Instant::now(),
            last_accessed_page: None,
            sequential_streak: 0,
        }
    }

    /// Get node info from cache or build from page data
    /// 
    /// Returns the node info and optionally a list of page IDs to prefetch
    pub fn get(&mut self, page_id: PageId, page_data: &[u8]) -> (Option<&BtreeNodeInfo>, Vec<PageId>) {
        // Try to get from cache
        if let Some(mut info) = self.nodes.remove(&page_id) {
            // Record access for LRU-K
            info.record_access();
            
            self.stats.hits += 1;
            self.stats.recent_hits += 1;
            
            // Check if this was a prefetch hit
            if self.prefetch_queue.contains(&page_id) {
                self.stats.prefetch_hits += 1;
            }
            
            self.nodes.insert(page_id, info);
            
            // Track access pattern for prefetch
            let prefetch_pages = self.track_access_and_prefetch(page_id);
            
            return (self.nodes.get(&page_id), prefetch_pages);
        }

        // Build from page data
        self.stats.misses += 1;
        self.stats.recent_misses += 1;
        
        let info = match BtreeNodeInfo::from_page(page_id, page_data) {
            Some(mut info) => {
                info.record_access();
                info
            }
            None => return (None, Vec::new()),
        };

        // Evict if needed using LRU-K
        if self.nodes.len() >= self.capacity {
            self.evict_lru_k();
        }

        self.nodes.insert(page_id, info);
        
        // Track access pattern
        let prefetch_pages = self.track_access_and_prefetch(page_id);
        
        (self.nodes.get(&page_id), prefetch_pages)
    }

    /// Track access pattern and determine pages to prefetch
    fn track_access_and_prefetch(&mut self, page_id: PageId) -> Vec<PageId> {
        if !self.prefetch.enabled || !self.prefetch.track_sequential {
            // Still update last accessed page
            self.last_accessed_page = Some(page_id);
            return Vec::new();
        }

        let mut pages_to_prefetch = Vec::new();

        // Check if this is sequential access using global last accessed page
        let is_sequential = if let Some(last) = self.last_accessed_page {
            // Sequential if page_id == last + 1 (forward) or page_id == last - 1 (backward)
            page_id == last + 1 || page_id + 1 == last
        } else {
            false
        };

        if is_sequential {
            self.stats.sequential_patterns += 1;
            self.sequential_streak += 1;
        } else {
            if self.sequential_streak > 0 {
                self.stats.random_patterns += 1;
            }
            self.sequential_streak = 1;
        }

        // If we have sequential pattern (2+ consecutive pages), prefetch ahead
        if self.sequential_streak >= 2 {
            let direction: i32 = if let Some(last) = self.last_accessed_page {
                if page_id > last { 1 } else { -1 }
            } else {
                1
            };

            for i in 1..=self.prefetch.distance {
                let prefetch_id = if direction > 0 {
                    page_id + i as u32
                } else {
                    page_id.saturating_sub(i as u32)
                };
                
                // Only prefetch if not already in cache
                if !self.nodes.contains_key(&prefetch_id) && !self.prefetch_queue.contains(&prefetch_id) {
                    pages_to_prefetch.push(prefetch_id);
                    self.stats.pages_prefetched += 1;
                }
            }
        }

        // Update last accessed page
        self.last_accessed_page = Some(page_id);
        
        pages_to_prefetch
    }

    /// Mark pages as prefetched (call after prefetching)
    pub fn mark_prefetched(&mut self, page_id: PageId) {
        self.prefetch_queue.retain(|&id| id != page_id);
    }

    /// Get mutable reference to node info
    pub fn get_mut(&mut self, page_id: PageId) -> Option<&mut BtreeNodeInfo> {
        // Refresh LRU order and record access
        if let Some(mut info) = self.nodes.remove(&page_id) {
            info.record_access();
            self.stats.hits += 1;
            self.stats.recent_hits += 1;
            self.nodes.insert(page_id, info);
            self.nodes.get_mut(&page_id)
        } else {
            None
        }
    }

    /// Invalidate a cached node (when page is modified)
    pub fn invalidate(&mut self, page_id: PageId) {
        self.nodes.remove(&page_id);
    }

    /// Invalidate all nodes
    pub fn invalidate_all(&mut self) {
        self.nodes.clear();
    }

    /// Mark a node as dirty
    pub fn mark_dirty(&mut self, page_id: PageId) {
        if let Some(info) = self.nodes.get_mut(&page_id) {
            info.is_dirty = true;
        }
    }

    /// Evict using LRU-K algorithm (considers access frequency)
    fn evict_lru_k(&mut self) {
        let now = Instant::now();
        
        // Find the node with highest eviction score (oldest with lowest penalty)
        let mut best_eviction_score: f64 = -1.0;
        let mut evict_candidate: Option<PageId> = None;
        
        for (page_id, info) in &self.nodes {
            let score = info.access_history.eviction_score(now);
            if score > best_eviction_score {
                best_eviction_score = score;
                evict_candidate = Some(*page_id);
            }
        }
        
        if let Some(id) = evict_candidate {
            self.nodes.remove(&id);
            self.stats.evictions += 1;
        }
    }
    
    /// Legacy LRU eviction (fallback)
    fn evict_lru(&mut self) {
        if let Some((oldest_id, _)) = self.nodes.front() {
            let id = *oldest_id;
            self.nodes.remove(&id);
            self.stats.evictions += 1;
        }
    }

    /// Get cache statistics
    pub fn stats(&self) -> BtreeCacheStats {
        let total = self.stats.hits + self.stats.misses;
        let hit_rate = if total > 0 {
            (self.stats.hits as f64 / total as f64) * 100.0
        } else {
            0.0
        };

        BtreeCacheStats {
            size: self.nodes.len(),
            capacity: self.capacity,
            hits: self.stats.hits,
            misses: self.stats.misses,
            hit_rate,
            prefetch_enabled: self.prefetch.enabled,
            prefetch_queue_size: self.prefetch_queue.len(),
            tracked_patterns: self.access_patterns.len(),
            detailed: self.stats.clone(),
        }
    }
    
    /// Get detailed statistics
    pub fn detailed_stats(&self) -> &DetailedCacheStats {
        &self.stats
    }
    
    /// Reset recent statistics
    pub fn reset_recent_stats(&mut self) {
        self.stats.reset_recent();
    }

    /// Enable/disable prefetch
    pub fn set_prefetch_enabled(&mut self, enabled: bool) {
        self.prefetch.enabled = enabled;
    }

    /// Set prefetch distance
    pub fn set_prefetch_distance(&mut self, distance: usize) {
        self.prefetch.distance = distance;
    }
    
    /// Set warming strategy
    pub fn set_warming_strategy(&mut self, strategy: WarmingStrategy) {
        self.warming_strategy = strategy;
    }

    /// Get all dirty nodes
    pub fn get_dirty_nodes(&self) -> Vec<PageId> {
        self.nodes
            .iter()
            .filter(|(_, info)| info.is_dirty)
            .map(|(id, _)| *id)
            .collect()
    }
    
    /// Warm cache with specified pages
    /// 
    /// Returns number of pages warmed
    pub fn warm_cache<F>(&mut self, page_count: usize, mut page_loader: F) -> usize
    where
        F: FnMut(PageId) -> Option<Vec<u8>>,
    {
        let mut warmed = 0;
        
        match self.warming_strategy {
            WarmingStrategy::None => return 0,
            WarmingStrategy::FirstN(n) => {
                for page_id in 1..=n.min(page_count) as u32 {
                    if !self.nodes.contains_key(&page_id) {
                        if let Some(page_data) = page_loader(page_id) {
                            if let Some(info) = BtreeNodeInfo::from_page(page_id, &page_data) {
                                if self.nodes.len() >= self.capacity {
                                    self.evict_lru_k();
                                }
                                self.nodes.insert(page_id, info);
                                warmed += 1;
                            }
                        }
                    }
                }
            }
            WarmingStrategy::Interval(interval) => {
                let mut page_id = 1u32;
                while page_id <= page_count as u32 && warmed < self.capacity / 4 {
                    if !self.nodes.contains_key(&page_id) {
                        if let Some(page_data) = page_loader(page_id) {
                            if let Some(info) = BtreeNodeInfo::from_page(page_id, &page_data) {
                                if self.nodes.len() >= self.capacity {
                                    self.evict_lru_k();
                                }
                                self.nodes.insert(page_id, info);
                                warmed += 1;
                            }
                        }
                    }
                    page_id += interval as u32;
                }
            }
            WarmingStrategy::HotPages => {
                // For hot pages strategy, we'd need index statistics
                // For now, just warm the root page (page 1) and first few pages
                for page_id in 1..=5u32.min(page_count as u32) {
                    if !self.nodes.contains_key(&page_id) {
                        if let Some(page_data) = page_loader(page_id) {
                            if let Some(info) = BtreeNodeInfo::from_page(page_id, &page_data) {
                                if self.nodes.len() >= self.capacity {
                                    self.evict_lru_k();
                                }
                                self.nodes.insert(page_id, info);
                                warmed += 1;
                            }
                        }
                    }
                }
            }
        }
        
        self.stats.warming_operations += 1;
        self.stats.pages_warmed += warmed as u64;
        
        warmed
    }
    
    /// Check if a page is in cache (for testing)
    pub fn contains(&self, page_id: PageId) -> bool {
        self.nodes.contains_key(&page_id)
    }
    
    /// Get the number of pages in cache
    pub fn len(&self) -> usize {
        self.nodes.len()
    }
    
    /// Check if cache is empty
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }
    
    /// Get cache uptime
    pub fn uptime(&self) -> std::time::Duration {
        self.created_at.elapsed()
    }
}

#[derive(Debug, Clone)]
pub struct BtreeCacheStats {
    pub size: usize,
    pub capacity: usize,
    pub hits: u64,
    pub misses: u64,
    pub hit_rate: f64,
    pub prefetch_enabled: bool,
    pub prefetch_queue_size: usize,
    pub tracked_patterns: usize,
    /// Detailed statistics
    pub detailed: DetailedCacheStats,
}

impl std::fmt::Display for BtreeCacheStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "B-tree Cache: {}/{} nodes, {:.1}% hit rate ({} hits, {} misses), prefetch={}",
            self.size, self.capacity, self.hit_rate, self.hits, self.misses, 
            if self.prefetch_enabled { "on" } else { "off" }
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::btree_engine::{PageType, BtreePageOps};
    use crate::pager::Page;

    #[test]
    fn test_node_info_from_page() {
        let mut page = Page::new(1);

        // Set up as leaf page with some records
        let mut header = PageHeader::new(PageType::Data);
        header.set_leaf(true);
        header.record_count = 2;
        page.write_header(&header).unwrap();

        // Insert records
        page.insert_record(b"key1", b"value1").unwrap();
        page.insert_record(b"key2", b"value2").unwrap();

        let info = BtreeNodeInfo::from_page(1, page.as_slice()).unwrap();
        assert_eq!(info.page_id, 1);
        assert!(info.header.is_leaf());
        assert_eq!(info.key_offsets.len(), 2);
    }

    #[test]
    fn test_find_key() {
        let mut page = Page::new(1);
        let mut header = PageHeader::new(PageType::Data);
        header.set_leaf(true);
        page.write_header(&header).unwrap();

        page.insert_record(b"aaa", b"v1").unwrap();
        page.insert_record(b"bbb", b"v2").unwrap();
        page.insert_record(b"ccc", b"v3").unwrap();

        let info = BtreeNodeInfo::from_page(1, page.as_slice()).unwrap();

        assert!(info.find_key(b"aaa").is_some());
        assert!(info.find_key(b"bbb").is_some());
        assert!(info.find_key(b"ccc").is_some());
        assert!(info.find_key(b"ddd").is_none());
    }

    #[test]
    fn test_cache_get_and_invalidate() {
        let mut cache = BtreeNodeCache::new(10);
        let mut page = Page::new(1);
        let mut header = PageHeader::new(PageType::Data);
        header.set_leaf(true);
        page.write_header(&header).unwrap();
        page.insert_record(b"key", b"value").unwrap();

        // First access - cache miss
        let (info1, prefetch) = cache.get(1, page.as_slice());
        assert!(info1.is_some());
        assert!(prefetch.is_empty()); // No sequential pattern yet
        assert_eq!(cache.stats().misses, 1);

        // Second access - cache hit
        let (info2, prefetch) = cache.get(1, page.as_slice());
        assert!(info2.is_some());
        assert!(prefetch.is_empty());
        assert_eq!(cache.stats().hits, 1);

        // Invalidate
        cache.invalidate(1);
        assert_eq!(cache.stats().size, 0);
    }

    #[test]
    fn test_cache_eviction() {
        let mut cache = BtreeNodeCache::new(2);

        let mut page1 = Page::new(1);
        let mut header = PageHeader::new(PageType::Data);
        header.set_leaf(true);
        page1.write_header(&header).unwrap();
        page1.insert_record(b"k1", b"v1").unwrap();

        let mut page2 = Page::new(2);
        page2.write_header(&header).unwrap();
        page2.insert_record(b"k2", b"v2").unwrap();

        let mut page3 = Page::new(3);
        page3.write_header(&header).unwrap();
        page3.insert_record(b"k3", b"v3").unwrap();

        // Add 3 pages to cache with capacity 2
        cache.get(1, page1.as_slice());
        cache.get(2, page2.as_slice());
        cache.get(3, page3.as_slice());

        assert_eq!(cache.stats().size, 2);
        // Page with lowest access count might be evicted (LRU-K)
    }

    #[test]
    fn test_prefetch_config() {
        // Test that prefetch configuration works
        let cache = BtreeNodeCache::with_prefetch(10, PrefetchConfig {
            enabled: true,
            distance: 4,
            track_sequential: true,
        });

        let stats = cache.stats();
        assert!(stats.prefetch_enabled);
        assert_eq!(stats.tracked_patterns, 0);
        
        // Test disabling prefetch
        let mut cache2 = BtreeNodeCache::new(10);
        cache2.set_prefetch_enabled(false);
        cache2.set_prefetch_distance(8);
        
        let stats2 = cache2.stats();
        assert!(!stats2.prefetch_enabled);
    }
    
    #[test]
    fn test_lru_k_eviction() {
        let mut cache = BtreeNodeCache::new(3);
        
        let mut header = PageHeader::new(PageType::Data);
        header.set_leaf(true);
        
        // Create 4 pages
        let mut page1 = Page::new(1);
        page1.write_header(&header).unwrap();
        page1.insert_record(b"k1", b"v1").unwrap();
        
        let mut page2 = Page::new(2);
        page2.write_header(&header).unwrap();
        page2.insert_record(b"k2", b"v2").unwrap();
        
        let mut page3 = Page::new(3);
        page3.write_header(&header).unwrap();
        page3.insert_record(b"k3", b"v3").unwrap();
        
        let mut page4 = Page::new(4);
        page4.write_header(&header).unwrap();
        page4.insert_record(b"k4", b"v4").unwrap();
        
        // Add pages 1, 2, 3
        cache.get(1, page1.as_slice());
        cache.get(2, page2.as_slice());
        cache.get(3, page3.as_slice());
        
        // Access page 1 multiple times to give it higher penalty
        cache.get(1, page1.as_slice());
        cache.get(1, page1.as_slice());
        cache.get(1, page1.as_slice());
        
        // Add page 4 - should evict one of the less accessed pages (2 or 3)
        cache.get(4, page4.as_slice());
        
        // Page 1 should still be in cache (high access count = high penalty)
        assert!(cache.contains(1));
        // Cache should have 3 entries
        assert_eq!(cache.len(), 3);
    }
    
    #[test]
    fn test_cache_warming() {
        let mut cache = BtreeNodeCache::with_warming(10, WarmingStrategy::FirstN(3));
        
        let mut header = PageHeader::new(PageType::Data);
        header.set_leaf(true);
        
        // Create pages for warming
        let mut pages: Vec<Page> = Vec::new();
        for i in 1..=5u32 {
            let mut page = Page::new(i);
            page.write_header(&header).unwrap();
            page.insert_record(format!("k{}", i).as_bytes(), b"v").unwrap();
            pages.push(page);
        }
        
        // Warm cache with first 3 pages
        let warmed = cache.warm_cache(5, |page_id| {
            pages.get((page_id - 1) as usize).map(|p| p.as_slice().to_vec())
        });
        
        assert_eq!(warmed, 3);
        assert!(cache.contains(1));
        assert!(cache.contains(2));
        assert!(cache.contains(3));
        assert!(!cache.contains(4));
        assert!(!cache.contains(5));
    }
    
    #[test]
    fn test_detailed_stats() {
        let mut cache = BtreeNodeCache::new(10);
        
        let mut header = PageHeader::new(PageType::Data);
        header.set_leaf(true);
        
        let mut page = Page::new(1);
        page.write_header(&header).unwrap();
        page.insert_record(b"key", b"value").unwrap();
        
        // Generate some hits and misses
        cache.get(1, page.as_slice()); // miss
        cache.get(1, page.as_slice()); // hit
        cache.get(1, page.as_slice()); // hit
        cache.get(2, page.as_slice()); // miss
        
        let stats = cache.detailed_stats();
        assert_eq!(stats.hits, 2);
        assert_eq!(stats.misses, 2);
        assert!((stats.hit_rate() - 0.5).abs() < 0.01);
        
        // Reset recent stats
        cache.reset_recent_stats();
        let stats_after = cache.detailed_stats();
        assert_eq!(stats_after.recent_hits, 0);
        assert_eq!(stats_after.recent_misses, 0);
    }
    
    #[test]
    fn test_sequential_pattern_detection() {
        let mut cache = BtreeNodeCache::with_prefetch(10, PrefetchConfig {
            enabled: true,
            distance: 2,
            track_sequential: true,
        });
        
        let mut header = PageHeader::new(PageType::Data);
        header.set_leaf(true);
        
        let mut pages: Vec<Page> = Vec::new();
        for i in 1..=5u32 {
            let mut page = Page::new(i);
            page.write_header(&header).unwrap();
            page.insert_record(format!("k{}", i).as_bytes(), b"v").unwrap();
            pages.push(page);
        }
        
        // Sequential access: 1, 2, 3, 4
        // First pass: populate cache
        cache.get(1, pages[0].as_slice());
        cache.get(2, pages[1].as_slice());
        cache.get(3, pages[2].as_slice());
        cache.get(4, pages[3].as_slice());
        
        // Second pass: detect sequential pattern on hits
        cache.get(1, pages[0].as_slice());
        let (_, prefetch2) = cache.get(2, pages[1].as_slice());
        let (_, prefetch3) = cache.get(3, pages[2].as_slice());
        let (_, prefetch4) = cache.get(4, pages[3].as_slice());
        
        // Should detect sequential pattern and suggest prefetch
        // After 3+ sequential accesses, pattern should be detected
        let has_prefetch = !prefetch2.is_empty() || !prefetch3.is_empty() || !prefetch4.is_empty();
        let has_sequential = cache.detailed_stats().sequential_patterns > 0;
        assert!(has_sequential || has_prefetch,
            "Should detect sequential pattern or return prefetch suggestions");
    }
}
