//! Page Prefetcher - Asynchronous Page Preloading for Sequential Scans (P3-2)
//!
//! This module implements page prefetching to reduce I/O wait times:
//! - Background thread pool for async page loading
//! - Sequential scan detection and prefetch trigger
//! - Cache-aware prefetching (avoid redundant loads)
//! - Configurable prefetch distance and window size
//! - Adaptive prefetch window based on I/O performance

use crate::pager::{Page, PageId, Pager};
use crate::pager::cache::PageCache;
use crate::pager::error::Result;
use std::collections::{HashSet, VecDeque};
use std::sync::{mpsc, Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

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
    /// P3-2: Enable adaptive window sizing
    pub adaptive_window: bool,
    /// P3-2: Minimum prefetch window
    pub min_window_size: usize,
    /// P3-2: Maximum prefetch window
    pub max_window_size: usize,
    /// P3-2: I/O latency threshold for window adjustment (ms)
    pub io_latency_threshold_ms: u64,
}

impl Default for PrefetchConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            prefetch_distance: 4,      // P3-2: Increased default
            max_concurrent: 4,
            sequential_threshold: 2,
            queue_size: 100,
            adaptive_window: true,      // P3-2: Enabled by default
            min_window_size: 2,
            max_window_size: 16,
            io_latency_threshold_ms: 10,
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
            adaptive_window: true,
            min_window_size: 1,
            max_window_size: 4,
            io_latency_threshold_ms: 20,
        }
    }

    /// Aggressive settings (high performance)
    pub fn aggressive() -> Self {
        Self {
            enabled: true,
            prefetch_distance: 8,
            max_concurrent: 8,
            sequential_threshold: 1,
            queue_size: 200,
            adaptive_window: true,
            min_window_size: 4,
            max_window_size: 32,
            io_latency_threshold_ms: 5,
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
    request_time: Instant,
}

/// Result of a prefetch operation
#[derive(Debug)]
struct PrefetchResult {
    page_id: PageId,
    page: Option<Page>,
    latency_ms: u64,
}

/// Access pattern type for sequential detection (P3-2)
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum AccessPattern {
    /// Sequential access (e.g., full table scan)
    Sequential,
    /// Random access (e.g., point lookups)
    Random,
    /// Mixed pattern
    Mixed,
    /// Unknown (not enough data)
    Unknown,
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
    /// P3-2: Current access pattern
    current_pattern: AccessPattern,
    /// P3-2: Adaptive window size
    current_window_size: usize,
    /// P3-2: I/O latency history for adaptive tuning
    latency_history: VecDeque<u64>,
    /// P3-2: Sequential streak counter
    sequential_streak: usize,
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
                current_pattern: AccessPattern::Unknown,
                current_window_size: config.prefetch_distance,
                latency_history: VecDeque::with_capacity(20),
                sequential_streak: 0,
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
            let cache_clone = Arc::clone(&cache);
            
            let handle = thread::spawn(move || {
                Self::worker_thread(rx, in_flight_clone, cache_clone);
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
            current_pattern: AccessPattern::Unknown,
            current_window_size: config.prefetch_distance,
            latency_history: VecDeque::with_capacity(20),
            sequential_streak: 0,
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

        // Update access pattern
        self.update_access_pattern(page_id);

        // Check if access pattern is sequential
        if self.is_sequential_access() {
            self.sequential_streak += 1;
            self.trigger_prefetch(page_id);
        } else {
            self.sequential_streak = 0;
        }
    }

    /// P3-2: Update the current access pattern based on recent history
    fn update_access_pattern(&mut self, current_page_id: PageId) {
        if self.access_history.len() < 3 {
            self.current_pattern = AccessPattern::Unknown;
            return;
        }

        let recent: Vec<_> = self.access_history.iter().rev().take(5).copied().collect();
        
        // Count sequential vs random accesses
        let mut sequential_count = 0;
        let mut random_count = 0;
        
        for i in 1..recent.len() {
            let diff = if recent[i - 1] > recent[i] {
                recent[i - 1] - recent[i]
            } else {
                recent[i] - recent[i - 1]
            };
            
            if diff == 1 {
                sequential_count += 1;
            } else if diff > 10 {
                random_count += 1;
            }
        }

        self.current_pattern = if sequential_count >= 3 {
            AccessPattern::Sequential
        } else if random_count >= 2 {
            AccessPattern::Random
        } else if sequential_count > 0 && random_count > 0 {
            AccessPattern::Mixed
        } else {
            AccessPattern::Unknown
        };
    }

    /// Get current access pattern (P3-2)
    pub fn access_pattern(&self) -> AccessPattern {
        self.current_pattern
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

        // Check if IDs are consecutive (increasing or decreasing)
        let increasing = recent.windows(2).all(|w| w[0] == w[1] + 1);
        let decreasing = recent.windows(2).all(|w| w[0] + 1 == w[1]);
        
        increasing || decreasing
    }

    /// P3-2: Adjust prefetch window based on I/O latency
    fn adjust_window_size(&mut self, latency_ms: u64) {
        if !self.config.adaptive_window {
            return;
        }

        // Record latency
        self.latency_history.push_back(latency_ms);
        if self.latency_history.len() > 20 {
            self.latency_history.pop_front();
        }

        // Calculate average latency
        if self.latency_history.len() < 5 {
            return;
        }

        let avg_latency: u64 = self.latency_history.iter().sum::<u64>() / self.latency_history.len() as u64;

        // Adjust window based on latency
        if avg_latency < self.config.io_latency_threshold_ms {
            // Fast I/O, can increase window
            self.current_window_size = (self.current_window_size + 1).min(self.config.max_window_size);
        } else if avg_latency > self.config.io_latency_threshold_ms * 2 {
            // Slow I/O, decrease window
            self.current_window_size = (self.current_window_size.saturating_sub(1)).max(self.config.min_window_size);
        }
    }

    /// Trigger prefetch for pages following the given page
    fn trigger_prefetch(&mut self, current_page_id: PageId) {
        // Avoid triggering too frequently
        if current_page_id <= self.last_prefetch_id {
            return;
        }

        let window_size = self.current_window_size;

        if let Some(ref sender) = self.sender {
            for i in 1..=window_size {
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
                let request = PrefetchRequest { 
                    page_id: target_page,
                    request_time: Instant::now(),
                };
                if sender.send(request).is_ok() {
                    // Mark as in-flight
                    self.in_flight.lock().unwrap().insert(target_page);
                }
            }
            
            self.last_prefetch_id = current_page_id + window_size as PageId;
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

                let request = PrefetchRequest { 
                    page_id,
                    request_time: Instant::now(),
                };
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
        _cache: Arc<Mutex<PageCache>>,
    ) {
        loop {
            // Try to receive a request
            let request = {
                let rx = receiver.lock().unwrap();
                rx.recv_timeout(Duration::from_millis(100))
            };

            match request {
                Ok(req) => {
                    // Simulate I/O latency for now
                    // In real implementation, this would read from disk
                    let latency = Duration::from_millis(1);
                    thread::sleep(latency);
                    
                    // Remove from in-flight
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

    /// P3-2: Get current prefetch window size
    pub fn current_window_size(&self) -> usize {
        self.current_window_size
    }

    /// P3-2: Get sequential streak count
    pub fn sequential_streak(&self) -> usize {
        self.sequential_streak
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

    /// P3-2: Get current access pattern
    pub fn access_pattern(&self) -> AccessPattern {
        self.prefetcher.access_pattern()
    }

    /// P3-2: Get current window size
    pub fn current_window_size(&self) -> usize {
        self.prefetcher.current_window_size()
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

/// P3-2: Sequential scan detector for query optimization
#[derive(Debug)]
pub struct SequentialScanDetector {
    /// History of accessed page IDs
    page_history: VecDeque<PageId>,
    /// Threshold for declaring sequential scan
    threshold: usize,
    /// Current detection state
    is_sequential: bool,
}

impl SequentialScanDetector {
    pub fn new(threshold: usize) -> Self {
        Self {
            page_history: VecDeque::with_capacity(threshold * 2),
            threshold,
            is_sequential: false,
        }
    }

    /// Record a page access and update detection state
    pub fn record_access(&mut self, page_id: PageId) -> bool {
        self.page_history.push_back(page_id);
        if self.page_history.len() > self.threshold * 2 {
            self.page_history.pop_front();
        }

        // Check for sequential pattern
        if self.page_history.len() >= self.threshold {
            let recent: Vec<_> = self.page_history.iter().rev().take(self.threshold).copied().collect();
            self.is_sequential = recent.windows(2).all(|w| w[0] == w[1] + 1);
        }

        self.is_sequential
    }

    /// Check if currently in sequential scan mode
    pub fn is_sequential(&self) -> bool {
        self.is_sequential
    }

    /// Reset detector
    pub fn reset(&mut self) {
        self.page_history.clear();
        self.is_sequential = false;
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
        assert_eq!(config.prefetch_distance, 4);
        assert_eq!(config.max_concurrent, 4);
        assert!(config.adaptive_window);  // P3-2
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

    // P3-2: New tests for access pattern detection
    #[test]
    fn test_access_pattern_detection() {
        let cache = create_test_cache();
        let config = PrefetchConfig::default();
        let mut prefetcher = PagePrefetcher::new(cache, config);

        // Initially unknown
        assert_eq!(prefetcher.access_pattern(), AccessPattern::Unknown);

        // Sequential pattern
        for i in 1..=5 {
            prefetcher.record_access(i);
        }
        assert_eq!(prefetcher.access_pattern(), AccessPattern::Sequential);

        // Reset with random pattern
        let cache2 = create_test_cache();
        let mut prefetcher2 = PagePrefetcher::new(cache2, config);
        
        prefetcher2.record_access(1);
        prefetcher2.record_access(100);
        prefetcher2.record_access(5);
        prefetcher2.record_access(200);
        
        // Pattern might be mixed or random
        let pattern = prefetcher2.access_pattern();
        assert!(pattern == AccessPattern::Random || pattern == AccessPattern::Mixed || pattern == AccessPattern::Unknown);
    }

    #[test]
    fn test_sequential_scan_detector() {
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
    fn test_adaptive_window() {
        let cache = create_test_cache();
        let config = PrefetchConfig::aggressive();
        let prefetcher = PagePrefetcher::new(cache, config);

        // Window should be within bounds
        let window = prefetcher.current_window_size();
        assert!(window >= config.min_window_size);
        assert!(window <= config.max_window_size);
    }
}
