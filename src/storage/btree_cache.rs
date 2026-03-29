//! B-tree Node Cache for Fast Key Lookups
//!
//! This module provides in-memory caching for B-tree node information
//! to avoid repeatedly parsing page headers and scanning slot arrays.

use crate::pager::PageId;
use crate::pager::page::PAGE_SIZE;
use crate::storage::btree_engine::{PageHeader, RecordHeader};
use hashlink::LinkedHashMap;

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

/// LRU cache for B-tree nodes with prefetch support
pub struct BtreeNodeCache {
    nodes: LinkedHashMap<PageId, BtreeNodeInfo>,
    capacity: usize,
    /// Cache hit/miss statistics
    hits: u64,
    misses: u64,
    /// Prefetch configuration
    prefetch: PrefetchConfig,
    /// Access pattern tracking (page_id -> last access pattern)
    access_patterns: hashlink::LinkedHashMap<PageId, AccessPattern>,
    /// Pages to prefetch (queue)
    prefetch_queue: Vec<PageId>,
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
            hits: 0,
            misses: 0,
            prefetch: PrefetchConfig::default(),
            access_patterns: hashlink::LinkedHashMap::new(),
            prefetch_queue: Vec::new(),
        }
    }

    /// Create with custom prefetch config
    pub fn with_prefetch(capacity: usize, prefetch: PrefetchConfig) -> Self {
        Self {
            nodes: LinkedHashMap::new(),
            capacity,
            hits: 0,
            misses: 0,
            prefetch,
            access_patterns: hashlink::LinkedHashMap::new(),
            prefetch_queue: Vec::new(),
        }
    }

    /// Get node info from cache or build from page data
    /// 
    /// Returns the node info and optionally a list of page IDs to prefetch
    pub fn get(&mut self, page_id: PageId, page_data: &[u8]) -> (Option<&BtreeNodeInfo>, Vec<PageId>) {
        // Try to get from cache
        if let Some(info) = self.nodes.remove(&page_id) {
            self.hits += 1;
            self.nodes.insert(page_id, info);
            
            // Track access pattern for prefetch
            let prefetch_pages = self.track_access_and_prefetch(page_id);
            
            return (self.nodes.get(&page_id), prefetch_pages);
        }

        // Build from page data
        self.misses += 1;
        let info = match BtreeNodeInfo::from_page(page_id, page_data) {
            Some(info) => info,
            None => return (None, Vec::new()),
        };

        // Evict if needed
        if self.nodes.len() >= self.capacity {
            self.evict_lru();
        }

        self.nodes.insert(page_id, info);
        
        // Track access pattern
        let prefetch_pages = self.track_access_and_prefetch(page_id);
        
        (self.nodes.get(&page_id), prefetch_pages)
    }

    /// Track access pattern and determine pages to prefetch
    fn track_access_and_prefetch(&mut self, page_id: PageId) -> Vec<PageId> {
        if !self.prefetch.enabled || !self.prefetch.track_sequential {
            return Vec::new();
        }

        let mut pages_to_prefetch = Vec::new();

        // Update access pattern
        let pattern = self.access_patterns.remove(&page_id).unwrap_or_default();
        
        // Check if this is sequential access
        let is_sequential = if let Some(last) = pattern.last_page_id {
            // Sequential if page_id == last + 1 (forward) or page_id == last - 1 (backward)
            page_id == last + 1 || page_id == last.saturating_sub(1)
        } else {
            false
        };

        let new_sequential_count = if is_sequential {
            pattern.sequential_count + 1
        } else {
            1
        };

        // If we have sequential pattern, prefetch ahead
        if new_sequential_count >= 2 {
            let direction: i32 = if let Some(last) = pattern.last_page_id {
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
                }
            }
        }

        // Update pattern
        let new_pattern = AccessPattern {
            last_page_id: Some(page_id),
            sequential_count: new_sequential_count,
        };

        // Limit tracked patterns
        if self.access_patterns.len() >= Self::MAX_PATTERN_TRACK {
            if let Some((oldest_id, _)) = self.access_patterns.front() {
                let id = *oldest_id;
                self.access_patterns.remove(&id);
            }
        }

        self.access_patterns.insert(page_id, new_pattern);
        
        pages_to_prefetch
    }

    /// Mark pages as prefetched (call after prefetching)
    pub fn mark_prefetched(&mut self, page_id: PageId) {
        self.prefetch_queue.retain(|&id| id != page_id);
    }

    /// Get mutable reference to node info
    pub fn get_mut(&mut self, page_id: PageId) -> Option<&mut BtreeNodeInfo> {
        // Refresh LRU order
        if let Some(info) = self.nodes.remove(&page_id) {
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

    /// Evict least recently used node
    fn evict_lru(&mut self) {
        if let Some((oldest_id, _)) = self.nodes.front() {
            let id = *oldest_id;
            self.nodes.remove(&id);
        }
    }

    /// Get cache statistics
    pub fn stats(&self) -> BtreeCacheStats {
        let total = self.hits + self.misses;
        let hit_rate = if total > 0 {
            (self.hits as f64 / total as f64) * 100.0
        } else {
            0.0
        };

        BtreeCacheStats {
            size: self.nodes.len(),
            capacity: self.capacity,
            hits: self.hits,
            misses: self.misses,
            hit_rate,
            prefetch_enabled: self.prefetch.enabled,
            prefetch_queue_size: self.prefetch_queue.len(),
            tracked_patterns: self.access_patterns.len(),
        }
    }

    /// Enable/disable prefetch
    pub fn set_prefetch_enabled(&mut self, enabled: bool) {
        self.prefetch.enabled = enabled;
    }

    /// Set prefetch distance
    pub fn set_prefetch_distance(&mut self, distance: usize) {
        self.prefetch.distance = distance;
    }

    /// Get all dirty nodes
    pub fn get_dirty_nodes(&self) -> Vec<PageId> {
        self.nodes
            .iter()
            .filter(|(_, info)| info.is_dirty)
            .map(|(id, _)| *id)
            .collect()
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
        // Page 1 should be evicted (LRU)
        assert!(cache.nodes.get(&1).is_none());
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
}
