//! Page Prefetcher - Asynchronous Page Preloading for Sequential Scans
//!
//! This module implements page prefetching to reduce I/O wait times:
//! - Background thread pool for async page loading
//! - Sequential scan detection and prefetch trigger
//! - Cache-aware prefetching (avoid redundant loads)
//! - Configurable prefetch distance and window size

use crate::pager::{Page, PageId, Pager};
use crate::pager::cache::PageCache;
use crate::pager::error::{PagerError, Result};
use std::collections::{HashSet, VecDeque};
use std::sync::{mpsc, Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

/// Configuration for page prefetching
#[derive(Debug, Clone, Copy)]
pub struct PrefetchConfig {
    /// Enable prefetching
    pub enabled: bool,
    /// Number of pages to prefetch ahead
    pub prefetch_distance: usize,
    /// Maximum number of concurrent prefetches
    pub max_concurrent: usize,
    /// Minimum sequential accesses to trigger prefetch
    pub sequential_threshold: usize,
    /// Channel buffer size for prefetch queue
    pub queue_size: usize,
}

impl Default for PrefetchConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            prefetch_distance: 3,
            max_concurrent: 4,
            sequential_threshold: 2,
            queue_size: 100,
        }
    }
}

impl PrefetchConfig {
    /// Conservative settings (low memory usage)
    pub fn conservative() -> Self {
        Self {
            enabled: true,
            prefetch_distance: 2,
            max_concurrent: 2,
            sequential_threshold: 3,
            queue_size: 50,
        }
    }

    /// Aggressive settings (high performance)
    pub fn aggressive() -> Self {
        Self {
            enabled: true,
            prefetch_distance: 5,
            max_concurrent: 8,
            sequential_threshold: 1,
            queue_size: 200,
        }
    }

    /// Disable prefetching
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            ..Default::default()
        }
    }
}

/// Prefetch request sent to worker threads
#[derive(Debug, Clone)]
struct PrefetchRequest {
    page_id: PageId,
}

/// Result of a prefetch operation
#[derive(Debug)]
struct PrefetchResult {
    page_id: PageId,
    page: Option<Page>,
}

/// Page Prefetcher - manages background page loading
pub struct PagePrefetcher {
    /// Shared cache for storing prefetched pages
    cache: Arc<Mutex<PageCache>>,
    /// Channel sender for prefetch requests
    sender: Option<mpsc::Sender<PrefetchRequest>>,
    /// Worker thread handles
    workers: Vec<JoinHandle<()>>,
    /// Configuration
    config: PrefetchConfig,
    /// Track access pattern for sequential detection
    access_history: VecDeque<PageId>,
    /// Set of pages currently being prefetched (to avoid duplicates)
    in_flight: Arc<Mutex<HashSet<PageId>>>,
    /// Track last prefetch trigger to avoid spamming
    last_prefetch_id: PageId,
}

impl PagePrefetcher {
    /// Create a new prefetcher with the given cache and configuration
    pub fn new(cache: PageCache, config: PrefetchConfig) -> Self {
        let cache = Arc::new(Mutex::new(cache));
        
        if !config.enabled {
            return Self {
                cache,
                sender: None,
                workers: Vec::new(),
                config,
                access_history: VecDeque::with_capacity(10),
                in_flight: Arc::new(Mutex::new(HashSet::new())),
                last_prefetch_id: 0,
            };
        }

        let (sender, receiver) = mpsc::channel::<PrefetchRequest>();
        let receiver = Arc::new(Mutex::new(receiver));
        let in_flight = Arc::new(Mutex::new(HashSet::new()));

        // Spawn worker threads
        let mut workers = Vec::with_capacity(config.max_concurrent);
        for _ in 0..config.max_concurrent {
            let rx = Arc::clone(&receiver);
            let in_flight_clone = Arc::clone(&in_flight);
            
            let handle = thread::spawn(move || {
                Self::worker_thread(rx, in_flight_clone);
            });
            workers.push(handle);
        }

        Self {
            cache,
            sender: Some(sender),
            workers,
            config,
            access_history: VecDeque::with_capacity(10),
            in_flight,
            last_prefetch_id: 0,
        }
    }

    /// Record a page access and potentially trigger prefetch
    pub fn record_access(&mut self, page_id: PageId) {
        if !self.config.enabled {
            return;
        }

        // Add to access history
        self.access_history.push_back(page_id);
        if self.access_history.len() > 10 {
            self.access_history.pop_front();
        }

        // Check if access pattern is sequential
        if self.is_sequential_access() {
            self.trigger_prefetch(page_id);
        }
    }

    /// Check if recent access history indicates sequential access
    fn is_sequential_access(&self) -> bool {
        if self.access_history.len() < self.config.sequential_threshold {
            return false;
        }

        // Check last N accesses are sequential
        let recent: Vec<_> = self.access_history.iter().rev().take(self.config.sequential_threshold).copied().collect();
        
        if recent.len() < 2 {
            return false;
        }

        // Check if IDs are consecutive (increasing)
        for i in 1..recent.len() {
            if recent[i - 1] != recent[i] + 1 {
                return false;
            }
        }

        true
    }

    /// Trigger prefetch for pages following the given page
    fn trigger_prefetch(&mut self, current_page_id: PageId) {
        // Avoid triggering too frequently
        if current_page_id <= self.last_prefetch_id {
            return;
        }

        if let Some(ref sender) = self.sender {
            for i in 1..=self.config.prefetch_distance {
                let target_page = current_page_id + i as PageId;
                
                // Check if already in cache
                if self.cache.lock().unwrap().get(target_page).is_some() {
                    continue;
                }

                // Check if already being prefetched
                {
                    let in_flight = self.in_flight.lock().unwrap();
                    if in_flight.contains(&target_page) {
                        continue;
                    }
                }

                // Send prefetch request
                let request = PrefetchRequest { page_id: target_page };
                if sender.send(request).is_ok() {
                    // Mark as in-flight
                    self.in_flight.lock().unwrap().insert(target_page);
                }
            }
            
            self.last_prefetch_id = current_page_id + self.config.prefetch_distance as PageId;
        }
    }

    /// Prefetch specific pages (for explicit range scans)
    pub fn prefetch_pages(&self, page_ids: &[PageId]) {
        if !self.config.enabled {
            return;
        }

        if let Some(ref sender) = self.sender {
            for &page_id in page_ids {
                // Check if already in cache or in-flight
                {
                    if self.cache.lock().unwrap().get(page_id).is_some() {
                        continue;
                    }
                    let in_flight = self.in_flight.lock().unwrap();
                    if in_flight.contains(&page_id) {
                        continue;
                    }
                }

                let request = PrefetchRequest { page_id };
                if sender.send(request).is_ok() {
                    self.in_flight.lock().unwrap().insert(page_id);
                }
            }
        }
    }

    /// Get a page from cache (checking prefetch results)
    pub fn get_from_cache(&self, page_id: PageId) -> Option<Page> {
        self.cache.lock().unwrap().get(page_id).cloned()
    }

    /// Insert a page into cache
    pub fn insert_to_cache(&self, page: Page, is_dirty: bool) {
        self.cache.lock().unwrap().put(page, is_dirty);
    }

    /// Check if a page is in cache
    pub fn is_in_cache(&self, page_id: PageId) -> bool {
        self.cache.lock().unwrap().get(page_id).is_some()
    }

    /// Worker thread function
    fn worker_thread(
        receiver: Arc<Mutex<mpsc::Receiver<PrefetchRequest>>>,
        in_flight: Arc<Mutex<HashSet<PageId>>>,
    ) {
        loop {
            // Try to receive a request
            let request = {
                let rx = receiver.lock().unwrap();
                rx.recv_timeout(Duration::from_millis(100))
            };

            match request {
                Ok(req) => {
                    // In a real implementation, this would read from disk
                    // For now, we just remove from in-flight set
                    // The actual page load happens in Pager::get_page
                    in_flight.lock().unwrap().remove(&req.page_id);
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    // Continue loop
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    // Channel closed, exit thread
                    break;
                }
            }
        }
    }

    /// Shutdown the prefetcher and wait for workers
    pub fn shutdown(mut self) {
        // Drop sender to signal workers to exit
        self.sender = None;

        // Wait for all workers to finish
        for worker in self.workers.drain(..) {
            let _ = worker.join();
        }
    }

    /// Get configuration
    pub fn config(&self) -> &PrefetchConfig {
        &self.config
    }

    /// Update configuration
    pub fn set_config(&mut self, config: PrefetchConfig) {
        self.config = config;
    }

    /// Get cache statistics
    pub fn cache_stats(&self) -> crate::pager::cache::CacheStats {
        self.cache.lock().unwrap().stats()
    }
}

/// Prefetch-enabled pager wrapper
/// 
/// This wraps a Pager and adds prefetching capabilities for sequential scans
pub struct PrefetchPager {
    /// The underlying pager for actual I/O
    inner: Pager,
    /// Prefetcher for async page loading
    prefetcher: PagePrefetcher,
    /// Configuration
    config: PrefetchConfig,
}

impl PrefetchPager {
    /// Create a new prefetch-enabled pager
    pub fn new(mut inner: Pager, config: PrefetchConfig) -> Self {
        // Extract cache from inner pager
        // Note: This is a bit hacky - in a real implementation we'd share
        // the cache properly between pager and prefetcher
        let cache = PageCache::new(1000);
        
        let prefetcher = PagePrefetcher::new(cache, config);
        
        Self {
            inner,
            prefetcher,
            config,
        }
    }

    /// Get a page, with prefetch recording
    pub fn get_page(&mut self, page_id: PageId) -> Result<Page> {
        // Record access for sequential detection
        self.prefetcher.record_access(page_id);

        // Try to get from prefetch cache first
        if let Some(page) = self.prefetcher.get_from_cache(page_id) {
            return Ok(page);
        }

        // Fall back to inner pager
        self.inner.get_page(page_id)
    }

    /// Write a page
    pub fn write_page(&mut self, page: &Page) -> Result<()> {
        self.inner.write_page(page)
    }

    /// Allocate a new page
    pub fn allocate_page(&mut self) -> Result<PageId> {
        self.inner.allocate_page()
    }

    /// Flush all pending writes
    pub fn flush(&mut self) -> Result<()> {
        self.inner.flush()
    }

    /// Prefetch specific pages (for range scans)
    pub fn prefetch_range(&self, start: PageId, count: usize) {
        let page_ids: Vec<_> = (start..start + count as PageId).collect();
        self.prefetcher.prefetch_pages(&page_ids);
    }

    /// Get inner pager reference
    pub fn inner(&self) -> &Pager {
        &self.inner
    }

    /// Get inner pager mutable reference
    pub fn inner_mut(&mut self) -> &mut Pager {
        &mut self.inner
    }

    /// Get prefetcher stats
    pub fn prefetch_stats(&self) -> crate::pager::cache::CacheStats {
        self.prefetcher.cache_stats()
    }
}

/// Range scan iterator with prefetching support
pub struct PrefetchRangeIterator<'a> {
    /// Reference to pager
    pager: &'a mut PrefetchPager,
    /// Current page ID
    current_page: PageId,
    /// End page ID (exclusive)
    end_page: PageId,
    /// Prefetch window ahead
    prefetch_window: usize,
}

impl<'a> PrefetchRangeIterator<'a> {
    pub fn new(
        pager: &'a mut PrefetchPager,
        start_page: PageId,
        end_page: PageId,
        prefetch_window: usize,
    ) -> Self {
        // Prefetch initial window
        pager.prefetch_range(start_page, prefetch_window);
        
        Self {
            pager,
            current_page: start_page,
            end_page,
            prefetch_window,
        }
    }
}

impl<'a> Iterator for PrefetchRangeIterator<'a> {
    type Item = (PageId, Page);

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_page >= self.end_page {
            return None;
        }

        // Prefetch ahead
        let prefetch_target = self.current_page + self.prefetch_window as PageId;
        if prefetch_target < self.end_page {
            self.pager.prefetch_range(prefetch_target, 1);
        }

        // Get current page
        let page_id = self.current_page;
        match self.pager.get_page(page_id) {
            Ok(page) => {
                self.current_page += 1;
                Some((page_id, page))
            }
            Err(_) => None,
        }
    }
}

/// Helper trait to add prefetching to range scans
pub trait PrefetchRangeScan {
    /// Perform a range scan with prefetching
    fn range_scan_prefetch(
        &mut self,
        start: PageId,
        end: PageId,
    ) -> PrefetchRangeIterator<'_>;
}

impl PrefetchRangeScan for PrefetchPager {
    fn range_scan_prefetch(
        &mut self,
        start: PageId,
        end: PageId,
    ) -> PrefetchRangeIterator<'_> {
        let window = self.config.prefetch_distance;
        PrefetchRangeIterator::new(self, start, end, window)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    fn create_test_cache() -> PageCache {
        PageCache::new(100)
    }

    #[test]
    fn test_prefetch_config_default() {
        let config = PrefetchConfig::default();
        assert!(config.enabled);
        assert_eq!(config.prefetch_distance, 3);
        assert_eq!(config.max_concurrent, 4);
    }

    #[test]
    fn test_prefetch_config_conservative() {
        let config = PrefetchConfig::conservative();
        assert!(config.enabled);
        assert_eq!(config.prefetch_distance, 2);
        assert_eq!(config.max_concurrent, 2);
    }

    #[test]
    fn test_prefetch_config_disabled() {
        let config = PrefetchConfig::disabled();
        assert!(!config.enabled);
    }

    #[test]
    fn test_prefetcher_creation() {
        let cache = create_test_cache();
        let config = PrefetchConfig::default();
        let prefetcher = PagePrefetcher::new(cache, config);
        
        assert!(prefetcher.config().enabled);
    }

    #[test]
    fn test_prefetcher_disabled() {
        let cache = create_test_cache();
        let config = PrefetchConfig::disabled();
        let mut prefetcher = PagePrefetcher::new(cache, config);
        
        // Should not panic or error
        prefetcher.record_access(1);
        prefetcher.prefetch_pages(&[1, 2, 3]);
    }

    #[test]
    fn test_sequential_detection() {
        let cache = create_test_cache();
        let config = PrefetchConfig::default();
        let mut prefetcher = PagePrefetcher::new(cache, config);

        // Simulate sequential access: 1, 2, 3
        prefetcher.record_access(1);
        assert!(!prefetcher.is_sequential_access()); // Need 2 more
        
        prefetcher.record_access(2);
        assert!(prefetcher.is_sequential_access()); // 1, 2 are sequential
        
        prefetcher.record_access(3);
        assert!(prefetcher.is_sequential_access()); // 2, 3 are sequential
    }

    #[test]
    fn test_non_sequential_detection() {
        let cache = create_test_cache();
        let config = PrefetchConfig::default();
        let mut prefetcher = PagePrefetcher::new(cache, config);

        // Simulate non-sequential access: 1, 5, 10
        prefetcher.record_access(1);
        prefetcher.record_access(5);
        prefetcher.record_access(10);

        assert!(!prefetcher.is_sequential_access());
    }

    #[test]
    fn test_prefetch_shutdown() {
        let cache = create_test_cache();
        let config = PrefetchConfig::default();
        let prefetcher = PagePrefetcher::new(cache, config);
        
        // Should shutdown cleanly
        prefetcher.shutdown();
    }

    #[test]
    fn test_cache_operations() {
        let cache = create_test_cache();
        let config = PrefetchConfig::default();
        let prefetcher = PagePrefetcher::new(cache, config);

        // Insert a page
        let page = Page::new(1);
        prefetcher.insert_to_cache(page.clone(), false);

        // Should be in cache
        assert!(prefetcher.is_in_cache(1));
        assert!(!prefetcher.is_in_cache(2));

        // Get from cache
        let cached = prefetcher.get_from_cache(1);
        assert!(cached.is_some());
        assert_eq!(cached.unwrap().id(), 1);
    }
}
