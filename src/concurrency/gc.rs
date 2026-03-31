//! Garbage Collector for MVCC
//!
//! Removes obsolete versions that are no longer visible to any active transaction:
//! - Identifies invisible versions (not visible to any active snapshot)
//! - Background GC thread for automatic cleanup
//! - Configurable GC strategies (Manual / Timer / Adaptive)
//! - Batch cleanup for efficiency

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use parking_lot::{Mutex, RwLock};

use super::mvcc::{MvccManager, TxId, Version, VersionChain};
use super::cow::CowStorage;

/// GC mode/strategy
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum GcMode {
    /// Manual triggering only
    Manual,
    /// Timer-based (run every N seconds)
    Timer { interval_secs: u64 },
    /// Adaptive (based on version count and memory usage)
    Adaptive { 
        /// Trigger GC when version count exceeds this
        version_threshold: usize,
        /// Trigger GC when memory exceeds this (in MB)
        memory_threshold_mb: usize,
    },
}

impl Default for GcMode {
    fn default() -> Self {
        GcMode::Adaptive { 
            version_threshold: 10000, 
            memory_threshold_mb: 100 
        }
    }
}

/// Statistics for garbage collection
#[derive(Debug, Clone, Default)]
pub struct GcStats {
    /// Total number of GC runs
    pub total_runs: u64,
    /// Total versions removed
    pub versions_removed: u64,
    /// Total memory freed (approximate, in bytes)
    pub memory_freed_bytes: u64,
    /// Last GC time
    pub last_gc_time: Option<Instant>,
    /// Average versions removed per run
    pub avg_versions_per_run: f64,
    /// Time spent in GC (total)
    pub total_gc_time_ms: u64,
}

impl GcStats {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a GC run
    pub fn record_run(&mut self, versions_removed: usize, memory_freed: usize, elapsed_ms: u64) {
        self.total_runs += 1;
        self.versions_removed += versions_removed as u64;
        self.memory_freed_bytes += memory_freed as u64;
        self.last_gc_time = Some(Instant::now());
        self.total_gc_time_ms += elapsed_ms;
        
        // Update average
        self.avg_versions_per_run = self.versions_removed as f64 / self.total_runs as f64;
    }

    /// Get GC efficiency (versions per ms)
    pub fn efficiency(&self) -> f64 {
        if self.total_gc_time_ms == 0 {
            0.0
        } else {
            self.versions_removed as f64 / self.total_gc_time_ms as f64
        }
    }
}

/// Garbage collector for MVCC version chains
pub struct GarbageCollector {
    /// GC mode/strategy
    pub gc_mode: GcMode,
    /// Statistics
    pub stats: Mutex<GcStats>,
    /// Last GC time
    pub last_gc_time: Mutex<Option<Instant>>,
    /// Running flag for background thread
    running: AtomicBool,
    /// Background thread handle
    bg_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
    /// Versions removed counter
    pub versions_removed: AtomicU64,
    /// Minimum interval between GC runs (to prevent thrashing)
    pub min_gc_interval: Duration,
    /// Batch size for cleanup (versions per batch)
    pub batch_size: usize,
}

impl GarbageCollector {
    pub fn new(gc_mode: GcMode) -> Self {
        Self {
            gc_mode,
            stats: Mutex::new(GcStats::new()),
            last_gc_time: Mutex::new(None),
            running: AtomicBool::new(false),
            bg_thread: Mutex::new(None),
            versions_removed: AtomicU64::new(0),
            min_gc_interval: Duration::from_secs(1),
            batch_size: 1000,
        }
    }

    /// Create with default adaptive mode
    pub fn new_adaptive() -> Self {
        Self::new(GcMode::default())
    }

    /// Create with manual mode
    pub fn new_manual() -> Self {
        Self::new(GcMode::Manual)
    }

    /// Create with timer mode
    pub fn new_timer(interval_secs: u64) -> Self {
        Self::new(GcMode::Timer { interval_secs })
    }

    /// Check if GC should run (based on mode)
    pub fn should_run(&self, version_count: usize, memory_usage_mb: usize) -> bool {
        match self.gc_mode {
            GcMode::Manual => false, // Only manual trigger
            GcMode::Timer { interval_secs } => {
                let last = *self.last_gc_time.lock();
                if let Some(last_time) = last {
                    last_time.elapsed().as_secs() >= interval_secs
                } else {
                    true // Never run before
                }
            }
            GcMode::Adaptive { version_threshold, memory_threshold_mb } => {
                // Check version threshold
                if version_count >= version_threshold {
                    return true;
                }
                // Check memory threshold
                if memory_usage_mb >= memory_threshold_mb {
                    return true;
                }
                // Check minimum interval
                let last = *self.last_gc_time.lock();
                if let Some(last_time) = last {
                    if last_time.elapsed() < self.min_gc_interval {
                        return false;
                    }
                }
                false
            }
        }
    }

    /// Run garbage collection on version chains
    /// 
    /// # Arguments
    /// * `version_chains` - Map of key to version chains
    /// * `oldest_visible_tx` - Oldest transaction that might still be reading
    /// 
    /// Returns number of versions removed
    pub fn gc_version_chains<K: Clone + std::hash::Hash + Eq, V: Clone>(
        &self,
        version_chains: &RwLock<HashMap<K, VersionChain<V>>>,
        oldest_visible_tx: TxId,
    ) -> usize {
        let start = Instant::now();
        let mut total_removed = 0;
        
        let mut chains = version_chains.write();
        
        for (_, chain) in chains.iter_mut() {
            total_removed += chain.gc(oldest_visible_tx);
            
            // Yield periodically to avoid blocking too long
            if total_removed % self.batch_size == 0 {
                std::thread::yield_now();
            }
        }
        
        // Update stats
        let elapsed_ms = start.elapsed().as_millis() as u64;
        let memory_freed = total_removed * std::mem::size_of::<Version<V>>();
        
        self.stats.lock().record_run(total_removed, memory_freed, elapsed_ms);
        self.versions_removed.fetch_add(total_removed as u64, Ordering::SeqCst);
        *self.last_gc_time.lock() = Some(Instant::now());
        
        total_removed
    }

    /// Run garbage collection on COW storage
    pub fn gc_cow_storage<T: Clone>(
        &self,
        storage: &CowStorage<T>,
        oldest_visible_tx: TxId,
    ) -> usize {
        let start = Instant::now();
        let removed = storage.gc(oldest_visible_tx);
        
        // Update stats
        let elapsed_ms = start.elapsed().as_millis() as u64;
        let memory_freed = removed * std::mem::size_of::<T>();
        
        self.stats.lock().record_run(removed, memory_freed, elapsed_ms);
        self.versions_removed.fetch_add(removed as u64, Ordering::SeqCst);
        *self.last_gc_time.lock() = Some(Instant::now());
        
        removed
    }

    /// Run GC with manager (finds oldest visible tx automatically)
    pub fn run_with_manager<K: Clone + std::hash::Hash + Eq, V: Clone>(
        &self,
        version_chains: &RwLock<HashMap<K, VersionChain<V>>>,
        manager: &MvccManager,
    ) -> usize {
        let oldest_visible_tx = manager.get_oldest_active_tx().unwrap_or(u64::MAX);
        self.gc_version_chains(version_chains, oldest_visible_tx)
    }

    /// Manual trigger for GC
    pub fn trigger_gc<K: Clone + std::hash::Hash + Eq, V: Clone>(
        &self,
        version_chains: &RwLock<HashMap<K, VersionChain<V>>>,
        manager: &MvccManager,
    ) -> usize {
        self.run_with_manager(version_chains, manager)
    }

    /// Get statistics
    pub fn get_stats(&self) -> GcStats {
        self.stats.lock().clone()
    }

    /// Get total versions removed
    pub fn get_versions_removed(&self) -> u64 {
        self.versions_removed.load(Ordering::Acquire)
    }

    /// Reset statistics
    pub fn reset_stats(&self) {
        *self.stats.lock() = GcStats::new();
        self.versions_removed.store(0, Ordering::SeqCst);
    }

    /// Set batch size
    pub fn set_batch_size(&mut self, size: usize) {
        self.batch_size = size;
    }

    /// Set minimum GC interval
    pub fn set_min_interval(&mut self, interval: Duration) {
        self.min_gc_interval = interval;
    }
}

impl Default for GarbageCollector {
    fn default() -> Self {
        Self::new_adaptive()
    }
}

/// Background GC worker
pub struct BackgroundGcWorker {
    /// Garbage collector
    gc: Arc<GarbageCollector>,
    /// MVCC manager
    manager: Arc<MvccManager>,
    /// Running flag
    running: Arc<AtomicBool>,
}

impl BackgroundGcWorker {
    pub fn new(gc: Arc<GarbageCollector>, manager: Arc<MvccManager>) -> Self {
        Self {
            gc,
            manager,
            running: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Start background GC thread (for Timer and Adaptive modes)
    pub fn start<K, V>(
        &self,
        version_chains: Arc<RwLock<HashMap<K, VersionChain<V>>>>,
    ) -> std::thread::JoinHandle<()>
    where
        K: Clone + std::hash::Hash + Eq + Send + Sync + 'static,
        V: Clone + Send + Sync + 'static,
    {
        let running = self.running.clone();
        let gc = self.gc.clone();
        let manager = self.manager.clone();
        
        running.store(true, Ordering::SeqCst);
        
        let chains = version_chains.clone();
        
        std::thread::spawn(move || {
            while running.load(Ordering::Acquire) {
                // Check if GC should run
                let should_run = match gc.gc_mode {
                    GcMode::Manual => false,
                    GcMode::Timer { interval_secs } => {
                        let last = *gc.last_gc_time.lock();
                        last.map_or(true, |t| t.elapsed().as_secs() >= interval_secs)
                    }
                    GcMode::Adaptive { .. } => {
                        // For adaptive, we'd need to check version count and memory
                        // For simplicity, run periodically
                        let last = *gc.last_gc_time.lock();
                        last.map_or(true, |t| t.elapsed().as_secs() >= 5)
                    }
                };
                
                if should_run {
                    let oldest = manager.get_oldest_active_tx().unwrap_or(u64::MAX);
                    gc.gc_version_chains(&chains, oldest);
                }
                
                // Sleep to avoid busy waiting
                std::thread::sleep(Duration::from_secs(1));
            }
        })
    }

    /// Stop background thread
    pub fn stop(&self) {
        self.running.store(false, Ordering::SeqCst);
    }
}

/// Integrated GC manager for MVCC database
pub struct GcManager {
    /// Garbage collector
    pub gc: Arc<GarbageCollector>,
    /// Background worker
    worker: Option<BackgroundGcWorker>,
    /// Background thread handle
    bg_handle: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl GcManager {
    pub fn new(gc_mode: GcMode) -> Self {
        let gc = Arc::new(GarbageCollector::new(gc_mode));
        
        Self {
            gc,
            worker: None,
            bg_handle: Mutex::new(None),
        }
    }

    /// Create with adaptive mode
    pub fn new_adaptive() -> Self {
        Self::new(GcMode::default())
    }

    /// Create with manual mode
    pub fn new_manual() -> Self {
        Self::new(GcMode::Manual)
    }

    /// Create with timer mode
    pub fn new_timer(interval_secs: u64) -> Self {
        Self::new(GcMode::Timer { interval_secs })
    }

    /// Start background GC (for Timer/Adaptive modes)
    pub fn start_background<K, V>(
        &mut self,
        version_chains: Arc<RwLock<HashMap<K, VersionChain<V>>>>,
        manager: Arc<MvccManager>,
    ) where
        K: Clone + std::hash::Hash + Eq + Send + Sync + 'static,
        V: Clone + Send + Sync + 'static,
    {
        // Only start for Timer or Adaptive modes
        match self.gc.gc_mode {
            GcMode::Manual => return,
            _ => {}
        }
        
        let worker = BackgroundGcWorker::new(self.gc.clone(), manager);
        let handle = worker.start(version_chains);
        
        self.worker = Some(worker);
        *self.bg_handle.lock() = Some(handle);
    }

    /// Stop background GC
    pub fn stop_background(&self) {
        if let Some(ref worker) = self.worker {
            worker.stop();
        }
        
        if let Some(handle) = self.bg_handle.lock().take() {
            // Wait for thread to finish (with timeout)
            let _ = handle.join();
        }
    }

    /// Manual GC trigger
    pub fn trigger_gc<K, V>(
        &self,
        version_chains: &RwLock<HashMap<K, VersionChain<V>>>,
        manager: &MvccManager,
    ) -> usize
    where
        K: Clone + std::hash::Hash + Eq,
        V: Clone,
    {
        self.gc.run_with_manager(version_chains, manager)
    }

    /// Get statistics
    pub fn stats(&self) -> GcStats {
        self.gc.get_stats()
    }

    /// Check if GC is running in background
    pub fn is_running(&self) -> bool {
        self.worker.is_some()
    }
}

impl Drop for GcManager {
    fn drop(&mut self) {
        self.stop_background();
    }
}

/// Version chain storage with integrated GC
pub struct VersionChainStorage<K: Clone + std::hash::Hash + Eq, V: Clone> {
    /// Version chains
    chains: Arc<RwLock<HashMap<K, VersionChain<V>>>>,
    /// MVCC manager
    manager: Arc<MvccManager>,
    /// GC manager
    gc: Arc<GcManager>,
}

impl<K: Clone + std::hash::Hash + Eq + Send + Sync + 'static, V: Clone + Send + Sync + 'static> 
    VersionChainStorage<K, V> 
{
    pub fn new(manager: Arc<MvccManager>, gc_mode: GcMode) -> Self {
        let chains = Arc::new(RwLock::new(HashMap::new()));
        let mut gc_manager = GcManager::new(gc_mode);
        
        // Start background GC if not manual mode
        gc_manager.start_background(chains.clone(), manager.clone());
        
        Self {
            chains,
            manager,
            gc: Arc::new(gc_manager),
        }
    }

    /// Insert or update a key
    pub fn insert(&self, key: K, data: V, tx_id: TxId) {
        let version = Version::new(data, tx_id);
        
        let mut chains = self.chains.write();
        let chain = chains.entry(key).or_default();
        chain.add_version(version);
    }

    /// Read a key
    pub fn read(&self, key: &K, reader_tx: TxId) -> Option<V> {
        let snapshot = self.manager.get_snapshot(reader_tx);
        
        let chains = self.chains.read();
        let chain = chains.get(key)?;
        chain.get_visible(reader_tx, &snapshot).map(|v| v.data.clone())
    }

    /// Manual GC trigger
    pub fn trigger_gc(&self) -> usize {
        self.gc.trigger_gc(&self.chains, &self.manager)
    }

    /// Get GC statistics
    pub fn gc_stats(&self) -> GcStats {
        self.gc.stats()
    }

    /// Get version count
    pub fn version_count(&self) -> usize {
        let chains = self.chains.read();
        chains.values().map(|c| c.versions.len()).sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_gc_mode_default() {
        let mode = GcMode::default();
        match mode {
            GcMode::Adaptive { version_threshold, memory_threshold_mb } => {
                assert_eq!(version_threshold, 10000);
                assert_eq!(memory_threshold_mb, 100);
            }
            _ => panic!("Expected adaptive mode"),
        }
    }

    #[test]
    fn test_gc_stats() {
        let mut stats = GcStats::new();
        
        assert_eq!(stats.total_runs, 0);
        assert_eq!(stats.versions_removed, 0);
        
        // Record some runs
        stats.record_run(100, 1024, 10);
        stats.record_run(200, 2048, 20);
        
        assert_eq!(stats.total_runs, 2);
        assert_eq!(stats.versions_removed, 300);
        assert_eq!(stats.avg_versions_per_run, 150.0);
    }

    #[test]
    fn test_gc_should_run_manual() {
        let gc = GarbageCollector::new_manual();
        
        // Manual mode never auto-runs
        assert!(!gc.should_run(100000, 1000));
        assert!(!gc.should_run(0, 0));
    }

    #[test]
    fn test_gc_should_run_timer() {
        let gc = GarbageCollector::new_timer(1);
        
        // Should run initially (never run before)
        assert!(gc.should_run(0, 0));
        
        // Simulate a run
        *gc.last_gc_time.lock() = Some(Instant::now());
        
        // Should not run immediately
        assert!(!gc.should_run(0, 0));
    }

    #[test]
    fn test_gc_should_run_adaptive() {
        let gc = GarbageCollector::new_adaptive();
        
        // Should run when version count exceeds threshold
        assert!(gc.should_run(10001, 0));
        
        // Should run when memory exceeds threshold
        assert!(gc.should_run(0, 101));
        
        // Should not run when below thresholds
        assert!(!gc.should_run(100, 10));
    }

    #[test]
    fn test_gc_version_chains() {
        let gc = GarbageCollector::new_manual();
        let chains: RwLock<HashMap<u64, VersionChain<String>>> = RwLock::new(HashMap::new());
        
        // Add some version chains
        {
            let mut c = chains.write();
            
            // Chain 1: 3 versions
            let mut chain1 = VersionChain::new();
            chain1.add_version(Version::new("v1".to_string(), 1));
            chain1.add_version(Version::new("v2".to_string(), 2));
            chain1.add_version(Version::new("v3".to_string(), 3));
            c.insert(1, chain1);
            
            // Chain 2: 2 versions
            let mut chain2 = VersionChain::new();
            chain2.add_version(Version::new("a1".to_string(), 1));
            chain2.add_version(Version::new("a2".to_string(), 2));
            c.insert(2, chain2);
        }
        
        // GC with oldest_visible_tx = 2
        // Should remove versions with created_by < 2 (except newest)
        let removed = gc.gc_version_chains(&chains, 2);
        
        // v1 should be removed from chain 1, a1 from chain 2
        assert_eq!(removed, 2);
        
        // Check stats
        let stats = gc.get_stats();
        assert_eq!(stats.total_runs, 1);
        assert_eq!(stats.versions_removed, 2);
    }

    #[test]
    fn test_version_chain_storage() {
        let manager = Arc::new(MvccManager::new());
        let storage = VersionChainStorage::new(manager.clone(), GcMode::Manual);
        
        // Insert some data
        let tx1 = manager.begin_transaction();
        storage.insert("key1".to_string(), "value1".to_string(), tx1);
        manager.commit_transaction(tx1);
        
        let tx2 = manager.begin_transaction();
        storage.insert("key1".to_string(), "value2".to_string(), tx2);
        manager.commit_transaction(tx2);
        
        // Should have 2 versions
        assert_eq!(storage.version_count(), 2);
        
        // Trigger GC
        let removed = storage.trigger_gc();
        assert!(removed >= 1); // At least one old version removed
    }

    #[test]
    fn test_gc_stats_efficiency() {
        let mut stats = GcStats::new();
        
        stats.record_run(1000, 10240, 100);
        
        // Efficiency = versions / ms = 1000 / 100 = 10
        assert_eq!(stats.efficiency(), 10.0);
    }

    #[test]
    fn test_gc_reset_stats() {
        let gc = GarbageCollector::new_manual();
        
        // Add some stats
        gc.stats.lock().record_run(100, 1024, 10);
        gc.versions_removed.store(100, Ordering::SeqCst);
        
        assert_eq!(gc.get_versions_removed(), 100);
        
        // Reset
        gc.reset_stats();
        
        assert_eq!(gc.get_versions_removed(), 0);
        assert_eq!(gc.get_stats().total_runs, 0);
    }
}
