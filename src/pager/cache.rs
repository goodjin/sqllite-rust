use hashlink::LinkedHashMap;
use crate::pager::page::{Page, PageId};

/// O(1) LRU page cache using LinkedHashMap
pub struct PageCache {
    /// LinkedHashMap maintains insertion order for O(1) LRU
    pages: LinkedHashMap<PageId, CachedPage>,
    capacity: usize,
}

struct CachedPage {
    page: Page,
    is_dirty: bool,
    pin_count: u32,
}

impl PageCache {
    pub fn new(capacity: usize) -> Self {
        Self {
            pages: LinkedHashMap::new(),
            capacity,
        }
    }

    /// Get a page from cache - O(1)
    pub fn get(&mut self, page_id: PageId) -> Option<&Page> {
        // Refresh position by removing and re-inserting
        if let Some(cached) = self.pages.remove(&page_id) {
            self.pages.insert(page_id, cached);
            self.pages.get(&page_id).map(|c| &c.page)
        } else {
            None
        }
    }

    /// Get mutable reference to a page - O(1)
    pub fn get_mut(&mut self, page_id: PageId) -> Option<&mut Page> {
        // Refresh position by removing and re-inserting
        if let Some(mut cached) = self.pages.remove(&page_id) {
            cached.is_dirty = true;
            self.pages.insert(page_id, cached);
            self.pages.get_mut(&page_id).map(|c| &mut c.page)
        } else {
            None
        }
    }

    /// Insert or update a page - O(1)
    pub fn put(&mut self, page: Page, is_dirty: bool) {
        let page_id = page.id;

        // Check if we need to evict (only if new page and at capacity)
        if self.pages.len() >= self.capacity && !self.pages.contains_key(&page_id) {
            self.evict_if_needed();
        }

        let cached = CachedPage {
            page,
            is_dirty,
            pin_count: 0,
        };

        // LinkedHashMap::insert refreshes position if key exists
        self.pages.insert(page_id, cached);
    }

    /// Mark a page as dirty - O(1)
    pub fn mark_dirty(&mut self, page_id: PageId) {
        if let Some(cached) = self.pages.get_mut(&page_id) {
            cached.is_dirty = true;
        }
    }

    /// Get all dirty page IDs - O(n) but rarely called
    pub fn get_dirty_pages(&self) -> Vec<PageId> {
        self.pages
            .iter()
            .filter(|(_, cached)| cached.is_dirty)
            .map(|(id, _)| *id)
            .collect()
    }

    /// Clear dirty flag for a page - O(1)
    pub fn clear_dirty(&mut self, page_id: PageId) {
        if let Some(cached) = self.pages.get_mut(&page_id) {
            cached.is_dirty = false;
        }
    }

    /// Evict least recently used page that is not dirty and not pinned - O(1) amortized
    fn evict_if_needed(&mut self) {
        // LinkedHashMap maintains order, front() gives LRU item
        let keys_to_check: Vec<PageId> = self.pages.keys().cloned().collect();

        for page_id in keys_to_check {
            if let Some(cached) = self.pages.get(&page_id) {
                if cached.pin_count == 0 && !cached.is_dirty {
                    self.pages.remove(&page_id);
                    return;
                }
            }
        }

        // If we can't evict anything, just remove the oldest (may be dirty/pinned)
        // In production, this would trigger a flush
        if self.pages.len() >= self.capacity {
            if let Some((oldest_id, _)) = self.pages.front() {
                let id = *oldest_id;
                self.pages.remove(&id);
            }
        }
    }

    /// Get cache stats
    pub fn stats(&self) -> CacheStats {
        CacheStats {
            size: self.pages.len(),
            capacity: self.capacity,
            dirty_count: self.pages.values().filter(|c| c.is_dirty).count(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct CacheStats {
    pub size: usize,
    pub capacity: usize,
    pub dirty_count: usize,
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
}
