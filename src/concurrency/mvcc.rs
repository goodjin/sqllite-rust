//! MVCC (Multi-Version Concurrency Control) Core
//!
//! Design:
//! - Each record has multiple versions with transaction IDs
//! - Readers see a consistent snapshot without locking
//! - Writers create new versions without blocking readers
//! - Garbage collector removes obsolete versions

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

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
    pub fn is_visible_to(&self, reader_tx: TxId, snapshot: &Snapshot) -> bool {
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

/// Version chain for a single record (multiple versions)
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
#[derive(Debug, Clone)]
pub struct Snapshot {
    /// Transaction that created this snapshot
    pub reader_tx: TxId,
    /// All transactions that were committed when snapshot was taken
    pub visible_txs: std::collections::HashSet<TxId>,
    /// Oldest transaction that might still be reading
    pub xmin: TxId,
}

impl Snapshot {
    /// Create a new snapshot
    pub fn new(reader_tx: TxId, visible_txs: std::collections::HashSet<TxId>, xmin: TxId) -> Self {
        Self {
            reader_tx,
            visible_txs,
            xmin,
        }
    }
    
    /// Check if a transaction's changes are visible in this snapshot
    pub fn is_visible(&self, tx_id: TxId) -> bool {
        if tx_id == 0 {
            // System/boot transaction is always visible
            return true;
        }
        
        // A transaction is visible if:
        // 1. It's in our visible set (committed before we started)
        // 2. It's our own transaction
        self.visible_txs.contains(&tx_id) || tx_id == self.reader_tx
    }
    
    /// Get the xmin (oldest potentially visible transaction)
    pub fn xmin(&self) -> TxId {
        self.xmin
    }
}

/// Global transaction manager for MVCC
pub struct MvccManager {
    /// Next transaction ID to assign
    next_tx_id: AtomicU64,
    /// Currently active (not yet committed) transactions
    active_txs: std::sync::Mutex<std::collections::HashSet<TxId>>,
    /// Committed transactions (for snapshot creation)
    committed_txs: std::sync::Mutex<std::collections::BTreeSet<TxId>>,
}

impl MvccManager {
    pub fn new() -> Self {
        Self {
            next_tx_id: AtomicU64::new(1),
            active_txs: std::sync::Mutex::new(std::collections::HashSet::new()),
            committed_txs: std::sync::Mutex::new(std::collections::BTreeSet::new()),
        }
    }
    
    /// Begin a new transaction, get its ID
    pub fn begin_transaction(&self) -> TxId {
        let tx_id = self.next_tx_id.fetch_add(1, Ordering::SeqCst);
        
        let mut active = self.active_txs.lock().unwrap();
        active.insert(tx_id);
        
        tx_id
    }
    
    /// Commit a transaction
    pub fn commit_transaction(&self, tx_id: TxId) {
        let mut active = self.active_txs.lock().unwrap();
        active.remove(&tx_id);
        
        let mut committed = self.committed_txs.lock().unwrap();
        committed.insert(tx_id);
    }
    
    /// Rollback (abort) a transaction
    pub fn rollback_transaction(&self, tx_id: TxId) {
        let mut active = self.active_txs.lock().unwrap();
        active.remove(&tx_id);
        // Not added to committed - changes are discarded
    }
    
    /// Create a snapshot for a reading transaction
    pub fn get_snapshot(&self, reader_tx: TxId) -> Snapshot {
        let active = self.active_txs.lock().unwrap();
        let committed = self.committed_txs.lock().unwrap();
        
        // Visible transactions: all committed except those that started after us
        // For simplicity: all committed txs are visible
        let visible_txs: std::collections::HashSet<TxId> = committed.iter().copied().collect();
        
        // xmin: oldest active transaction (or reader_tx if none)
        let xmin = active.iter().copied().min().unwrap_or(reader_tx);
        
        Snapshot::new(reader_tx, visible_txs, xmin)
    }
    
    /// Get the oldest transaction that might be reading data
    /// Used by garbage collector
    pub fn get_oldest_active_tx(&self) -> Option<TxId> {
        let active = self.active_txs.lock().unwrap();
        active.iter().copied().min()
    }
    
    /// Get current transaction statistics
    pub fn stats(&self) -> MvccStats {
        let active = self.active_txs.lock().unwrap();
        let committed = self.committed_txs.lock().unwrap();
        
        MvccStats {
            next_tx_id: self.next_tx_id.load(Ordering::SeqCst),
            active_count: active.len(),
            committed_count: committed.len(),
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
        let snapshot = Snapshot::new(2, visible, 1);
        
        assert!(version.is_visible_to(2, &snapshot));
        
        // Snapshot without tx 1 visible
        let snapshot2 = Snapshot::new(2, std::collections::HashSet::new(), 1);
        assert!(!version.is_visible_to(2, &snapshot2));
    }

    #[test]
    fn test_version_chain() {
        let mut chain = VersionChain::new();
        
        // Add versions
        chain.add_version(Version::new("v1".to_string(), 1));
        chain.add_version(Version::new("v2".to_string(), 2));
        chain.add_version(Version::new("v3".to_string(), 3));
        
        // Snapshot sees txs 1 and 2
        let mut visible = std::collections::HashSet::new();
        visible.insert(1);
        visible.insert(2);
        let snapshot = Snapshot::new(4, visible, 1);
        
        // Should see v2 (newest visible)
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
        let snapshot = Snapshot::new(3, visible, 1);
        
        // Initially visible
        assert!(version.is_visible_to(3, &snapshot));
        
        // Mark deleted by tx 2
        version.mark_deleted(2);
        
        // tx 2 committed before our snapshot, so deletion is visible
        let mut visible2 = std::collections::HashSet::new();
        visible2.insert(1);
        visible2.insert(2);
        let snapshot2 = Snapshot::new(3, visible2, 1);
        
        assert!(!version.is_visible_to(3, &snapshot2));
    }
}
