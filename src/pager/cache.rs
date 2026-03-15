use std::collections::{HashMap, VecDeque};
use crate::pager::page::{Page, PageId};

pub struct PageCache {
    pages: HashMap<PageId, CachedPage>,
    lru: VecDeque<PageId>,
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
            pages: HashMap::new(),
            lru: VecDeque::new(),
            capacity,
        }
    }

    pub fn get(&mut self, page_id: PageId) -> Option<&Page> {
        if self.pages.contains_key(&page_id) {
            self.update_lru(page_id);
            self.pages.get(&page_id).map(|cached| &cached.page)
        } else {
            None
        }
    }

    pub fn get_mut(&mut self, page_id: PageId) -> Option<&mut Page> {
        if self.pages.contains_key(&page_id) {
            self.update_lru(page_id);
            self.pages.get_mut(&page_id).map(|cached| &mut cached.page)
        } else {
            None
        }
    }

    pub fn put(&mut self, page: Page, is_dirty: bool) {
        let page_id = page.id;

        if self.pages.len() >= self.capacity && !self.pages.contains_key(&page_id) {
            self.evict_if_needed();
        }

        let cached = CachedPage {
            page,
            is_dirty,
            pin_count: 0,
        };

        self.pages.insert(page_id, cached);
        self.update_lru(page_id);
    }

    pub fn mark_dirty(&mut self, page_id: PageId) {
        if let Some(cached) = self.pages.get_mut(&page_id) {
            cached.is_dirty = true;
        }
    }

    pub fn get_dirty_pages(&self) -> Vec<PageId> {
        self.pages
            .iter()
            .filter(|(_, cached)| cached.is_dirty)
            .map(|(id, _)| *id)
            .collect()
    }

    pub fn clear_dirty(&mut self, page_id: PageId) {
        if let Some(cached) = self.pages.get_mut(&page_id) {
            cached.is_dirty = false;
        }
    }

    fn update_lru(&mut self, page_id: PageId) {
        if let Some(pos) = self.lru.iter().position(|&id| id == page_id) {
            self.lru.remove(pos);
        }
        self.lru.push_back(page_id);
    }

    fn evict_if_needed(&mut self) {
        while let Some(page_id) = self.lru.pop_front() {
            if let Some(cached) = self.pages.get(&page_id) {
                if cached.pin_count == 0 && !cached.is_dirty {
                    self.pages.remove(&page_id);
                    return;
                }
            }
        }
    }
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
}
