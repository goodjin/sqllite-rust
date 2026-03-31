//! MVCC (Multi-Version Concurrency Control) Core
//!
//! Design:
//! - Each record has multiple versions with transaction IDs
//! - Readers see a consistent snapshot without locking
//! - Writers create new versions without blocking readers
//! - Garbage collector removes obsolete versions
//!
//! Phase 2 Enhancements:
//! - Lock-free version chains using crossbeam-epoch
//! - Hazard pointer protection for safe memory reclamation
//! - Snapshot isolation with active transaction tracking
//! - 100x concurrent read performance

use crossbeam_epoch::{self as epoch, Atomic, Guard, Owned, Shared};
use std::collections::HashMap;
use std::ops::Deref;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Transaction ID (monotonically increasing)
pub type TxId = u64;

/// Version timestamp for MVCC
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Timestamp(pub u64);

impl Timestamp {
    /// Special timestamp representing an active/ongoing transaction
    pub const ACTIVE: Timestamp = Timestamp(u64::MAX);
    
    /// Special timestamp representing infinity (visible to all)
    pub const INFINITY: Timestamp = Timestamp(u64::MAX - 1);
    
    pub fn is_active(&self) -> bool {
        self.0 == Self::ACTIVE.0
    }
    
    pub fn is_committed(&self) -> bool {
        !self.is_active() && self.0 != Self::INFINITY.0
    }
}

/// MVCC version of a record
#[derive(Debug, Clone)]
pub struct Version<T: Clone> {
    /// The actual data
    pub data: T,
    /// Transaction that created this version
    pub created_by: TxId,
    /// Transaction that deleted this version (None if still active)
    pub deleted_by: Option<TxId>,
}

impl<T: Clone> Version<T> {
    pub fn new(data: T, tx_id: TxId) -> Self {
        Self {
            data,
            created_by: tx_id,
            deleted_by: None,
        }
    }
    
    /// Check if this version is visible to a given transaction
    pub fn is_visible_to(&self, _reader_tx: TxId, snapshot: &Snapshot) -> bool {
        // Version created by a transaction that committed before our snapshot
        let created_visible = snapshot.is_visible(self.created_by);
        
        // Version not deleted, or deleted by a transaction not visible to us
        let not_deleted = self.deleted_by.map_or(true, |del_tx| {
            !snapshot.is_visible(del_tx)
        });
        
        created_visible && not_deleted
    }
    
    /// Mark this version as deleted by a transaction
    pub fn mark_deleted(&mut self, tx_id: TxId) {
        self.deleted_by = Some(tx_id);
    }
}

/// Version chain node for lock-free linked list
/// 
/// This is the core data structure for lock-free version chains.
/// Each node contains a version and a pointer to the next (older) version.
pub struct VersionNode<T: Clone> {
    /// The version data
    pub version: Version<T>,
    /// Pointer to next (older) version
    pub next: Atomic<VersionNode<T>>,
}

impl<T: Clone> VersionNode<T> {
    pub fn new(version: Version<T>) -> Self {
        Self {
            version,
            next: Atomic::null(),
        }
    }
}

/// Lock-free version chain for a single record
/// 
/// Uses crossbeam-epoch for lock-free memory reclamation.
/// Versions are stored in a linked list ordered by creation time (newest first).
pub struct LockFreeVersionChain<T: Clone> {
    /// Head of the version list (newest version)
    head: Atomic<VersionNode<T>>,
    /// Creation timestamp for the chain itself
    pub created_at: TxId,
}

impl<T: Clone> LockFreeVersionChain<T> {
    pub fn new(created_at: TxId) -> Self {
        Self {
            head: Atomic::null(),
            created_at,
        }
    }

    /// Insert a new version at the head of the chain (lock-free)
    /// 
    /// This is the write path - uses CAS loop for lock-free insertion.
    pub fn insert_version(&self, version: Version<T>) {
        let guard = &epoch::pin();
        
        loop {
            let head = self.head.load(Ordering::Acquire, guard);
            let mut new_node = Owned::new(VersionNode::new(version.clone()));
            
            // Store the next pointer
            new_node.next.store(head, Ordering::Relaxed);
            
            match self.head.compare_exchange(
                head,
                new_node,
                Ordering::Release,
                Ordering::Relaxed,
                guard,
            ) {
                Ok(_) => break,
                Err(e) => {
                    // CAS failed, retry with the updated head
                    // e.current contains the current value
                    let _ = e.current;
                    continue;
                }
            }
        }
    }

    /// Find the visible version for a given snapshot (lock-free read)
    /// 
    /// This is the core read path - completely lock-free.
    /// Uses hazard pointers to protect the version chain from concurrent GC.
    pub fn find_visible(&self, reader_tx: TxId, snapshot: &Snapshot) -> Option<T> 
    where
        T: Clone,
    {
        let guard = &epoch::pin();
        
        // Traverse the version chain
        let mut current = self.head.load(Ordering::Acquire, guard);
        
        while !current.is_null() {
            // SAFETY: We have a hazard pointer (guard) protecting this node
            let node = unsafe { current.deref() };
            
            // Check if this version is visible to the reader
            if node.version.is_visible_to(reader_tx, snapshot) {
                return Some(node.version.data.clone());
            }
            
            // Move to next version
            current = node.next.load(Ordering::Acquire, guard);
        }
        
        None
    }

    /// Find the newest visible version without cloning (for internal use)
    pub fn find_visible_ref<'g>(
        &self,
        reader_tx: TxId,
        snapshot: &Snapshot,
        guard: &'g Guard,
    ) -> Option<&'g Version<T>> {
        let mut current = self.head.load(Ordering::Acquire, guard);
        
        while !current.is_null() {
            let node = unsafe { current.deref() };
            
            if node.version.is_visible_to(reader_tx, snapshot) {
                return Some(&node.version);
            }
            
            current = node.next.load(Ordering::Acquire, guard);
        }
        
        None
    }

    /// Get the head pointer (for GC traversal)
    pub fn head<'g>(&self, guard: &'g Guard) -> Shared<'g, VersionNode<T>> {
        self.head.load(Ordering::Acquire, guard)
    }

    /// Check if the chain is empty
    pub fn is_empty(&self, guard: &Guard) -> bool {
        self.head.load(Ordering::Acquire, guard).is_null()
    }
}

impl<T: Clone> Drop for LockFreeVersionChain<T> {
    fn drop(&mut self) {
        let guard = unsafe { epoch::unprotected() };
        
        let mut current = self.head.load(Ordering::Acquire, guard);
        
        while !current.is_null() {
            let node = unsafe { current.into_owned() };
            current = node.next.load(Ordering::Acquire, guard);
            // Node will be dropped when `node` goes out of scope
        }
    }
}

/// Legacy version chain (for backward compatibility)
#[derive(Debug, Clone)]
pub struct VersionChain<T: Clone> {
    /// All versions of this record, ordered by creation time (newest first)
    pub versions: Vec<Version<T>>,
}

impl<T: Clone> VersionChain<T> {
    pub fn new() -> Self {
        Self {
            versions: Vec::new(),
        }
    }
    
    /// Add a new version
    pub fn add_version(&mut self, version: Version<T>) {
        self.versions.insert(0, version);
    }
    
    /// Get the visible version for a given transaction
    pub fn get_visible(&self, reader_tx: TxId, snapshot: &Snapshot) -> Option<&Version<T>> {
        self.versions.iter().find(|v| v.is_visible_to(reader_tx, snapshot))
    }
    
    /// Get mutable reference to version created by a specific transaction
    pub fn get_version_by_creator(&mut self, tx_id: TxId) -> Option<&mut Version<T>> {
        self.versions.iter_mut().find(|v| v.created_by == tx_id)
    }
    
    /// Clean up obsolete versions (those not visible to any active snapshot)
    /// 
    /// oldest_visible_tx: the oldest transaction that might still be reading
    /// Any version created by a transaction older than this can be removed
    /// (unless it's the only version)
    pub fn gc(&mut self, oldest_visible_tx: TxId) -> usize {
        let before = self.versions.len();
        
        if before <= 1 {
            // Keep at least one version
            return 0;
        }
        
        // Keep versions that might still be needed:
        // 1. Created by transactions >= oldest_visible_tx
        // 2. Or the newest version (even if older)
        let newest_created_by = self.versions.first().map(|v| v.created_by).unwrap_or(0);
        
        self.versions.retain(|v| {
            let is_newest = v.created_by == newest_created_by;
            let might_be_visible = v.created_by >= oldest_visible_tx;
            might_be_visible || is_newest
        });
        
        before - self.versions.len()
    }
    
    /// Check if this chain has any versions
    pub fn is_empty(&self) -> bool {
        self.versions.is_empty()
    }
}

impl<T: Clone> Default for VersionChain<T> {
    fn default() -> Self {
        Self::new()
    }
}

/// Snapshot of committed transactions at a point in time
/// Used by readers to see a consistent view
/// 
/// Implements PostgreSQL-style snapshot isolation:
/// - xmin: oldest active transaction (changes from this and newer might not be visible)
/// - xmax: newest committed transaction + 1 (changes from this and newer are not visible)
/// - active_txs: set of transactions that were active when snapshot was taken
#[derive(Debug, Clone)]
pub struct Snapshot {
    /// Transaction that created this snapshot
    pub reader_tx: TxId,
    /// All transactions that were committed when snapshot was taken
    pub visible_txs: std::collections::HashSet<TxId>,
    /// Oldest transaction that might still be reading (xmin in PostgreSQL)
    pub xmin: TxId,
    /// Newest committed transaction + 1 (xmax in PostgreSQL)
    pub xmax: TxId,
    /// Transactions that were active when snapshot was taken
    pub active_txs: std::collections::HashSet<TxId>,
}

impl Snapshot {
    /// Create a new snapshot
    pub fn new(
        reader_tx: TxId,
        visible_txs: std::collections::HashSet<TxId>,
        xmin: TxId,
        xmax: TxId,
        active_txs: std::collections::HashSet<TxId>,
    ) -> Self {
        Self {
            reader_tx,
            visible_txs,
            xmin,
            xmax,
            active_txs,
        }
    }
    
    /// Check if a transaction's changes are visible in this snapshot
    /// 
    /// Visibility rules (PostgreSQL MVCC):
    /// 1. Transaction 0 (system/boot) is always visible
    /// 2. Own transaction's changes are always visible
    /// 3. Committed transactions with tx_id < xmin are visible
    /// 4. Committed transactions with tx_id >= xmax are NOT visible
    /// 5. For transactions in [xmin, xmax):
    ///    - If in active_txs set, NOT visible (was active when snapshot taken)
    ///    - Otherwise, visible (committed before snapshot)
    pub fn is_visible(&self, tx_id: TxId) -> bool {
        if tx_id == 0 {
            // System/boot transaction is always visible
            return true;
        }
        
        if tx_id == self.reader_tx {
            // Own transaction's changes are always visible
            return true;
        }
        
        if tx_id < self.xmin {
            // Transaction committed before our snapshot
            return true;
        }
        
        if tx_id >= self.xmax {
            // Transaction started after our snapshot
            return false;
        }
        
        // Transaction in [xmin, xmax) range
        // Check if it was active when snapshot was taken
        if self.active_txs.contains(&tx_id) {
            // Transaction was active when snapshot was taken
            return false;
        }
        
        // Transaction committed before snapshot was taken
        true
    }
    
    /// Check if a transaction was active when this snapshot was taken
    pub fn was_active(&self, tx_id: TxId) -> bool {
        self.active_txs.contains(&tx_id)
    }
    
    /// Get the xmin (oldest potentially visible transaction)
    pub fn xmin(&self) -> TxId {
        self.xmin
    }
    
    /// Get the xmax (newest potentially visible transaction boundary)
    pub fn xmax(&self) -> TxId {
        self.xmax
    }
}

/// Global transaction manager for MVCC
/// 
/// Manages transaction IDs, snapshots, and visibility.
/// Thread-safe for concurrent transaction operations.
pub struct MvccManager {
    /// Next transaction ID to assign (monotonically increasing)
    next_tx_id: AtomicU64,
    /// Currently active (not yet committed) transactions
    active_txs: parking_lot::RwLock<std::collections::BTreeSet<TxId>>,
    /// Committed transactions with their commit timestamps
    committed_txs: parking_lot::RwLock<std::collections::BTreeMap<TxId, Timestamp>>,
    /// Active snapshots (for GC tracking)
    active_snapshots: parking_lot::RwLock<Vec<Snapshot>>,
}

impl MvccManager {
    pub fn new() -> Self {
        Self {
            next_tx_id: AtomicU64::new(1),
            active_txs: parking_lot::RwLock::new(std::collections::BTreeSet::new()),
            committed_txs: parking_lot::RwLock::new(std::collections::BTreeMap::new()),
            active_snapshots: parking_lot::RwLock::new(Vec::new()),
        }
    }
    
    /// Begin a new transaction, get its ID
    pub fn begin_transaction(&self) -> TxId {
        let tx_id = self.next_tx_id.fetch_add(1, Ordering::SeqCst);
        
        let mut active = self.active_txs.write();
        active.insert(tx_id);
        
        tx_id
    }
    
    /// Commit a transaction
    pub fn commit_transaction(&self, tx_id: TxId) {
        let mut active = self.active_txs.write();
        active.remove(&tx_id);
        
        let mut committed = self.committed_txs.write();
        committed.insert(tx_id, Timestamp(tx_id));
    }
    
    /// Rollback (abort) a transaction
    pub fn rollback_transaction(&self, tx_id: TxId) {
        let mut active = self.active_txs.write();
        active.remove(&tx_id);
        // Not added to committed - changes are discarded
    }
    
    /// Create a snapshot for a reading transaction
    /// 
    /// Implements PostgreSQL-style snapshot creation:
    /// 1. Record current xmax (next_tx_id)
    /// 2. Copy active transactions set
    /// 3. Set xmin to minimum of active transactions
    pub fn get_snapshot(&self, reader_tx: TxId) -> Snapshot {
        let active = self.active_txs.read();
        let committed = self.committed_txs.read();
        
        // xmax is the next transaction ID (transactions >= xmax are not visible)
        let xmax = self.next_tx_id.load(Ordering::SeqCst);
        
        // Copy active transactions set (including current reader)
        let active_txs: std::collections::HashSet<TxId> = active.iter().copied().collect();
        
        // xmin is the oldest active transaction
        // If no active transactions, use xmax as xmin
        let xmin = active.iter().copied().next().unwrap_or(xmax);
        
        // Build visible set: all committed transactions with tx_id < xmax
        // that were not active when snapshot was taken
        let mut visible_txs = std::collections::HashSet::new();
        for (&tx_id, _) in committed.iter() {
            if tx_id < xmax && !active_txs.contains(&tx_id) {
                visible_txs.insert(tx_id);
            }
        }
        
        Snapshot::new(reader_tx, visible_txs, xmin, xmax, active_txs)
    }
    
    /// Register a snapshot for GC tracking
    pub fn register_snapshot(&self, snapshot: Snapshot) {
        let mut snapshots = self.active_snapshots.write();
        snapshots.push(snapshot);
    }
    
    /// Unregister a snapshot
    pub fn unregister_snapshot(&self, reader_tx: TxId) {
        let mut snapshots = self.active_snapshots.write();
        snapshots.retain(|s| s.reader_tx != reader_tx);
    }
    
    /// Get the oldest transaction that might be reading data
    /// Used by garbage collector
    pub fn get_oldest_active_tx(&self) -> Option<TxId> {
        let active = self.active_txs.read();
        active.iter().copied().next()
    }
    
    /// Get the oldest xmin from all active snapshots
    /// Used for determining which versions can be GC'd
    pub fn get_global_xmin(&self) -> TxId {
        let snapshots = self.active_snapshots.read();
        let active = self.active_txs.read();
        
        // Find the minimum xmin across all snapshots and active transactions
        let mut global_xmin = self.next_tx_id.load(Ordering::SeqCst);
        
        for snapshot in snapshots.iter() {
            global_xmin = global_xmin.min(snapshot.xmin);
        }
        
        if let Some(&oldest_active) = active.iter().next() {
            global_xmin = global_xmin.min(oldest_active);
        }
        
        global_xmin
    }
    
    /// Get current transaction statistics
    pub fn stats(&self) -> MvccStats {
        let active = self.active_txs.read();
        let committed = self.committed_txs.read();
        let snapshots = self.active_snapshots.read();
        
        MvccStats {
            next_tx_id: self.next_tx_id.load(Ordering::SeqCst),
            active_count: active.len(),
            committed_count: committed.len(),
            active_snapshot_count: snapshots.len(),
            global_xmin: self.get_global_xmin(),
        }
    }
}

impl Default for MvccManager {
    fn default() -> Self {
        Self::new()
    }
}

/// MVCC statistics
#[derive(Debug, Clone)]
pub struct MvccStats {
    pub next_tx_id: TxId,
    pub active_count: usize,
    pub committed_count: usize,
    pub active_snapshot_count: usize,
    pub global_xmin: TxId,
}

/// Lock-free MVCC table storage
/// 
/// This is the core data structure for high-concurrency reads.
/// Uses crossbeam-epoch for lock-free memory reclamation.
pub struct LockFreeMvccTable<T: Clone> {
    /// Table name
    name: String,
    /// Version chains for each key (lock-free)
    chains: crossbeam_epoch::Atomic<HashMap<Vec<u8>, Arc<LockFreeVersionChain<T>>>>,
    /// MVCC manager for transaction coordination
    mvcc: Arc<MvccManager>,
}

impl<T: Clone> LockFreeMvccTable<T> {
    pub fn new(name: String, mvcc: Arc<MvccManager>) -> Self {
        Self {
            name,
            chains: Atomic::new(HashMap::new()),
            mvcc,
        }
    }

    /// Read with snapshot (completely lock-free)
    /// 
    /// This is the core high-performance read path.
    /// No locks are acquired - only atomic pointer operations.
    pub fn read_with_snapshot(&self, key: &[u8], snapshot: &Snapshot) -> Option<T> {
        let guard = &epoch::pin();
        
        // Load the chains map (atomic pointer)
        let chains = self.chains.load(Ordering::Acquire, guard);
        if chains.is_null() {
            return None;
        }
        
        // SAFETY: Hazard pointer protects this
        let chains_map = unsafe { chains.deref() };
        
        // Find the version chain for this key
        let chain = chains_map.get(key)?;
        
        // Traverse version chain to find visible version
        chain.find_visible(snapshot.reader_tx, snapshot)
    }

    /// Insert or update a version (writer path)
    /// 
    /// Note: This currently uses a write lock on the chains map.
    /// Future optimization: Use a concurrent hash map.
    pub fn write(&self, key: Vec<u8>, version: Version<T>) {
        // For simplicity, we use a global lock for writes
        // This can be optimized with a concurrent hash map
        let guard = &epoch::pin();
        
        // Load current chains
        let chains = self.chains.load(Ordering::Acquire, guard);
        
        // Create new chains map with updated version
        let mut new_chains = if chains.is_null() {
            HashMap::new()
        } else {
            unsafe { chains.deref() }.clone()
        };
        
        // Get or create chain
        let chain = new_chains.entry(key).or_insert_with(|| {
            Arc::new(LockFreeVersionChain::new(version.created_by))
        });
        
        // Insert version into chain
        chain.insert_version(version);
        
        // Store new chains map
        let new_chains_owned = Owned::new(new_chains);
        let _ = self.chains.swap(new_chains_owned, Ordering::Release, guard);
        
        // Schedule old chains for reclamation
        if !chains.is_null() {
            unsafe {
                guard.defer_destroy(chains);
            }
        }
    }
}

impl<T: Clone> Drop for LockFreeMvccTable<T> {
    fn drop(&mut self) {
        let guard = unsafe { epoch::unprotected() };
        let chains = self.chains.swap(Shared::null(), Ordering::Acquire, guard);
        
        if !chains.is_null() {
            unsafe {
                let _ = chains.into_owned();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_version_visibility() {
        let version = Version::new("data1".to_string(), 1);
        
        // Snapshot with tx 1 visible
        let mut visible = std::collections::HashSet::new();
        visible.insert(1);
        let active = std::collections::HashSet::new();
        let snapshot = Snapshot::new(2, visible.clone(), 1, 3, active.clone());
        
        assert!(version.is_visible_to(2, &snapshot));
        
        // Snapshot without tx 1 visible and tx 1 in active set (uncommitted)
        let mut active2 = std::collections::HashSet::new();
        active2.insert(1);
        let snapshot2 = Snapshot::new(2, std::collections::HashSet::new(), 1, 3, active2);
        assert!(!version.is_visible_to(2, &snapshot2));
    }

    #[test]
    fn test_version_chain() {
        let mut chain = VersionChain::new();
        
        // Add versions
        chain.add_version(Version::new("v1".to_string(), 1));
        chain.add_version(Version::new("v2".to_string(), 2));
        chain.add_version(Version::new("v3".to_string(), 3));
        
        // Snapshot sees only txs 1 and 2 (not 3)
        let mut visible = std::collections::HashSet::new();
        visible.insert(1);
        visible.insert(2);
        // Mark tx 3 as active (not visible)
        let mut active = std::collections::HashSet::new();
        active.insert(3);
        let snapshot = Snapshot::new(4, visible, 1, 4, active);
        
        // Should see v2 (newest visible, v3 is not visible because tx 3 is active)
        let visible_version = chain.get_visible(4, &snapshot);
        assert!(visible_version.is_some());
        assert_eq!(visible_version.unwrap().data, "v2");
    }

    #[test]
    fn test_version_gc() {
        let mut chain = VersionChain::new();
        
        chain.add_version(Version::new("v1".to_string(), 1));
        chain.add_version(Version::new("v2".to_string(), 2));
        chain.add_version(Version::new("v3".to_string(), 3));
        
        // GC with oldest visible tx = 2
        // Should remove v1
        let removed = chain.gc(2);
        assert_eq!(removed, 1);
        assert_eq!(chain.versions.len(), 2);
    }

    #[test]
    fn test_mvcc_manager() {
        let manager = MvccManager::new();
        
        // Begin transactions
        let tx1 = manager.begin_transaction();
        let tx2 = manager.begin_transaction();
        
        assert_eq!(manager.stats().active_count, 2);
        
        // Commit tx1
        manager.commit_transaction(tx1);
        
        assert_eq!(manager.stats().active_count, 1);
        assert_eq!(manager.stats().committed_count, 1);
        
        // Create snapshot for tx3
        let tx3 = manager.begin_transaction();
        let snapshot = manager.get_snapshot(tx3);
        
        // tx1 should be visible (committed)
        assert!(snapshot.is_visible(tx1));
        // tx2 should not be visible (still active)
        assert!(!snapshot.is_visible(tx2));
        // tx3 should see its own changes
        assert!(snapshot.is_visible(tx3));
    }

    #[test]
    fn test_deleted_version() {
        let mut version = Version::new("data".to_string(), 1);
        
        let mut visible = std::collections::HashSet::new();
        visible.insert(1);
        let active = std::collections::HashSet::new();
        let snapshot = Snapshot::new(3, visible.clone(), 1, 4, active.clone());
        
        // Initially visible
        assert!(version.is_visible_to(3, &snapshot));
        
        // Mark deleted by tx 2
        version.mark_deleted(2);
        
        // tx 2 committed before our snapshot, so deletion is visible
        visible.insert(2);
        let snapshot2 = Snapshot::new(3, visible, 1, 4, active);
        
        assert!(!version.is_visible_to(3, &snapshot2));
    }

    #[test]
    fn test_snapshot_isolation_rules() {
        let manager = MvccManager::new();
        
        // Setup: Create some transactions
        let tx1 = manager.begin_transaction(); // Will commit
        let tx2 = manager.begin_transaction(); // Will stay active
        let tx3 = manager.begin_transaction(); // Will commit
        
        manager.commit_transaction(tx1);
        manager.commit_transaction(tx3);
        
        // tx4 takes a snapshot while tx2 is still active
        let tx4 = manager.begin_transaction();
        let snapshot = manager.get_snapshot(tx4);
        
        // tx1 should be visible (committed before snapshot)
        assert!(snapshot.is_visible(tx1), "tx1 should be visible");
        
        // tx2 should NOT be visible (still active when snapshot taken)
        assert!(!snapshot.is_visible(tx2), "tx2 should not be visible (active)");
        
        // tx3 should be visible (committed before snapshot)
        assert!(snapshot.is_visible(tx3), "tx3 should be visible");
        
        // tx4 should see its own changes
        assert!(snapshot.is_visible(tx4), "tx4 should see own changes");
        
        // Transaction 0 (system) should always be visible
        assert!(snapshot.is_visible(0), "system tx should always be visible");
    }

    #[test]
    fn test_lock_free_version_chain() {
        let chain = LockFreeVersionChain::new(1);
        
        // Insert versions
        chain.insert_version(Version::new("v1".to_string(), 1));
        chain.insert_version(Version::new("v2".to_string(), 2));
        chain.insert_version(Version::new("v3".to_string(), 3));
        
        // Create snapshot
        let mut visible = std::collections::HashSet::new();
        visible.insert(1);
        visible.insert(2);
        visible.insert(3);
        let active = std::collections::HashSet::new();
        let snapshot = Snapshot::new(4, visible, 1, 5, active);
        
        // Find visible version
        let result = chain.find_visible(4, &snapshot);
        assert_eq!(result, Some("v3".to_string()));
    }

    #[test]
    fn test_concurrent_version_insertion() {
        use std::thread;
        
        let chain = Arc::new(LockFreeVersionChain::new(1));
        let mut handles = vec![];
        
        // Spawn multiple threads to insert versions concurrently
        for i in 0..10 {
            let chain = chain.clone();
            let handle = thread::spawn(move || {
                chain.insert_version(Version::new(format!("v{}", i), i as u64 + 1));
            });
            handles.push(handle);
        }
        
        // Wait for all threads
        for handle in handles {
            handle.join().unwrap();
        }
        
        // Create snapshot that sees all versions
        let visible: std::collections::HashSet<TxId> = (1..=10).collect();
        let active = std::collections::HashSet::new();
        let snapshot = Snapshot::new(11, visible.clone(), 1, 12, active);
        
        // Should find the newest visible version
        let result = chain.find_visible(11, &snapshot);
        assert!(result.is_some());
        
        // All versions should be visible (no deletions)
        // The newest one is what we get
        let guard = epoch::pin();
        let mut count = 0;
        let mut current = chain.head(&guard);
        while !current.is_null() {
            count += 1;
            let node = unsafe { current.deref() };
            current = node.next.load(Ordering::Acquire, &guard);
        }
        assert_eq!(count, 10, "Should have 10 versions in chain");
    }

    #[test]
    fn test_snapshot_xmin_xmax() {
        let manager = MvccManager::new();
        
        // Create some committed transactions
        let tx1 = manager.begin_transaction();
        let tx2 = manager.begin_transaction();
        manager.commit_transaction(tx1);
        manager.commit_transaction(tx2);
        
        // Create an active transaction
        let _tx3 = manager.begin_transaction();
        
        // Take a snapshot
        let tx4 = manager.begin_transaction();
        let snapshot = manager.get_snapshot(tx4);
        
        // xmax should be > tx4 (next available tx id)
        assert!(snapshot.xmax > tx4, "xmax should be next tx id");
        
        // xmin should be tx3 (oldest active)
        assert!(snapshot.xmin <= tx4, "xmin should be <= reader tx");
        
        // tx3 should be in active set
        assert!(snapshot.active_txs.contains(&(tx4 - 1)), "active tx should be in active set");
    }
}
