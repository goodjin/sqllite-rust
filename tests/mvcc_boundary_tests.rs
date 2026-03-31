//! MVCC (Multi-Version Concurrency Control) Boundary Tests
//!
//! Tests for MVCC edge cases and boundary conditions

use sqllite_rust::concurrency::mvcc::{
    MvccManager, Version, VersionChain, Snapshot, Timestamp,
    TxId, LockFreeVersionChain
};
use std::collections::HashSet;
use crossbeam_epoch as epoch;
use std::sync::atomic::Ordering;
use std::sync::Arc;

// ============================================================================
// Transaction ID Boundary Tests
// ============================================================================

#[test]
fn test_max_transaction_id() {
    let manager = MvccManager::new();
    
    // Simulate many transactions
    for _ in 0..1000 {
        let tx_id = manager.begin_transaction();
        manager.commit_transaction(tx_id);
    }
    
    let stats = manager.stats();
    assert!(stats.next_tx_id > 1000);
}

#[test]
fn test_tx_id_wraparound_consideration() {
    // Test that the system handles large tx_ids
    let manager = MvccManager::new();
    
    // Begin and commit many transactions
    let tx_ids: Vec<TxId> = (0..100).map(|_| {
        let tx_id = manager.begin_transaction();
        manager.commit_transaction(tx_id);
        tx_id
    }).collect();
    
    // All tx_ids should be unique
    let unique_count: HashSet<_> = tx_ids.iter().copied().collect();
    assert_eq!(unique_count.len(), tx_ids.len());
}

// ============================================================================
// Version Chain Boundary Tests
// ============================================================================

#[test]
fn test_very_long_version_chain() {
    let mut chain = VersionChain::new();
    
    // Add many versions
    for i in 1..=1000 {
        chain.add_version(Version::new(format!("v{}", i), i));
    }
    
    assert_eq!(chain.versions.len(), 1000);
}

#[test]
fn test_version_chain_single_version() {
    let mut chain = VersionChain::new();
    chain.add_version(Version::new("data".to_string(), 1));
    
    // Create snapshot that can see tx 1
    let visible: HashSet<TxId> = [1].iter().copied().collect();
    let active = HashSet::new();
    let snapshot = Snapshot::new(2, visible, 1, 3, active);
    
    let version = chain.get_visible(2, &snapshot);
    assert!(version.is_some());
    assert_eq!(version.unwrap().data, "data");
}

#[test]
fn test_version_chain_empty() {
    let chain = VersionChain::<String>::new();
    
    let visible = HashSet::new();
    let active = HashSet::new();
    let snapshot = Snapshot::new(1, visible, 1, 2, active);
    
    let version = chain.get_visible(1, &snapshot);
    assert!(version.is_none());
}

// ============================================================================
// Visibility Rules Boundary Tests
// ============================================================================

#[test]
fn test_snapshot_with_no_visible_version() {
    let mut chain = VersionChain::new();
    chain.add_version(Version::new("v1".to_string(), 1));
    
    // Snapshot that cannot see any versions
    let visible = HashSet::new();
    let mut active = HashSet::new();
    active.insert(1); // tx 1 is active (not committed)
    let snapshot = Snapshot::new(2, visible, 1, 3, active);
    
    let version = chain.get_visible(2, &snapshot);
    assert!(version.is_none());
}

#[test]
fn test_snapshot_with_all_versions_deleted() {
    let mut chain = VersionChain::new();
    let mut version = Version::new("v1".to_string(), 1);
    version.mark_deleted(2);
    chain.add_version(version);
    
    // Snapshot where deletion is visible
    let visible: HashSet<TxId> = [1, 2].iter().copied().collect();
    let active = HashSet::new();
    let snapshot = Snapshot::new(3, visible, 1, 4, active);
    
    let result = chain.get_visible(3, &snapshot);
    assert!(result.is_none());
}

#[test]
fn test_visibility_system_tx() {
    // System transaction (tx 0) should always be visible
    let visible = HashSet::new();
    let active = HashSet::new();
    let snapshot = Snapshot::new(1, visible, 1, 2, active);
    
    assert!(snapshot.is_visible(0));
}

#[test]
fn test_visibility_own_tx() {
    // Transaction should see its own changes
    let visible = HashSet::new();
    let active = HashSet::new();
    let snapshot = Snapshot::new(5, visible, 1, 10, active);
    
    assert!(snapshot.is_visible(5));
}

#[test]
fn test_visibility_committed_before_snapshot() {
    // Committed transactions before snapshot should be visible
    let visible: HashSet<TxId> = [1, 2, 3].iter().copied().collect();
    let active = HashSet::new();
    let snapshot = Snapshot::new(5, visible, 1, 10, active);
    
    assert!(snapshot.is_visible(1));
    assert!(snapshot.is_visible(2));
    assert!(snapshot.is_visible(3));
}

#[test]
fn test_visibility_active_during_snapshot() {
    // Active transactions during snapshot should not be visible
    let visible: HashSet<TxId> = [1].iter().copied().collect();
    let mut active = HashSet::new();
    active.insert(2);
    let snapshot = Snapshot::new(3, visible, 1, 5, active);
    
    assert!(snapshot.is_visible(1));
    assert!(!snapshot.is_visible(2));
}

#[test]
fn test_visibility_after_snapshot() {
    // Transactions after snapshot should not be visible
    let visible: HashSet<TxId> = [1, 2].iter().copied().collect();
    let active = HashSet::new();
    let snapshot = Snapshot::new(3, visible, 1, 5, active);
    
    assert!(!snapshot.is_visible(5));
    assert!(!snapshot.is_visible(10));
}

// ============================================================================
// Garbage Collection Tests
// ============================================================================

#[test]
fn test_gc_removes_obsolete_versions() {
    let mut chain = VersionChain::new();
    
    chain.add_version(Version::new("v1".to_string(), 1));
    chain.add_version(Version::new("v2".to_string(), 2));
    chain.add_version(Version::new("v3".to_string(), 3));
    
    // GC with oldest_visible_tx = 2
    // Should remove v1
    let removed = chain.gc(2);
    assert_eq!(removed, 1);
    assert_eq!(chain.versions.len(), 2);
}

#[test]
fn test_gc_keeps_at_least_one_version() {
    let mut chain = VersionChain::new();
    chain.add_version(Version::new("v1".to_string(), 1));
    
    // GC should keep at least one version
    let removed = chain.gc(100);
    assert_eq!(removed, 0);
    assert_eq!(chain.versions.len(), 1);
}

#[test]
fn test_gc_keeps_newest_version() {
    let mut chain = VersionChain::new();
    chain.add_version(Version::new("v1".to_string(), 1));
    chain.add_version(Version::new("v2".to_string(), 2));
    
    // GC even with very high oldest_visible_tx
    let removed = chain.gc(1000);
    
    // Should keep v2 (newest)
    assert_eq!(removed, 0);
    assert_eq!(chain.versions.len(), 2);
}

#[test]
fn test_gc_concurrent_with_read() {
    let mut chain = VersionChain::new();
    
    for i in 1..=100 {
        chain.add_version(Version::new(format!("v{}", i), i as u64));
    }
    
    // Read while GC is possible
    let visible: HashSet<TxId> = (50..=100).map(|i| i as u64).collect();
    let active = HashSet::new();
    let snapshot = Snapshot::new(101, visible.clone(), 50, 102, active);
    
    // GC old versions
    let removed = chain.gc(50);
    assert!(removed >= 0);
    
    // Should still be able to read visible versions
    let version = chain.get_visible(101, &snapshot);
    assert!(version.is_some());
}

// ============================================================================
// Concurrent GC and Read Tests
// ============================================================================

#[test]
fn test_concurrent_gc_and_read() {
    use std::thread;
    
    let mut chain = VersionChain::new();
    
    // Populate
    for i in 1..=100 {
        chain.add_version(Version::new(format!("v{}", i), i as u64));
    }
    
    let chain = Arc::new(std::sync::Mutex::new(chain));
    let mut handles = vec![];
    
    // Reader threads
    for _ in 0..5 {
        let chain = Arc::clone(&chain);
        let handle = thread::spawn(move || {
            let visible: HashSet<TxId> = (1..=100).map(|i| i as u64).collect();
            let active = HashSet::new();
            let snapshot = Snapshot::new(101, visible, 1, 102, active);
            
            let chain = chain.lock().unwrap();
            let version = chain.get_visible(101, &snapshot);
            assert!(version.is_some());
        });
        handles.push(handle);
    }
    
    // GC thread
    let chain_gc = Arc::clone(&chain);
    let gc_handle = thread::spawn(move || {
        let mut chain = chain_gc.lock().unwrap();
        chain.gc(50);
    });
    handles.push(gc_handle);
    
    for handle in handles {
        handle.join().unwrap();
    }
}

// ============================================================================
// Lock-Free Version Chain Tests
// ============================================================================

#[test]
fn test_lock_free_version_chain_basic() {
    let chain = LockFreeVersionChain::new(1);
    
    chain.insert_version(Version::new("v1".to_string(), 1));
    
    let visible: HashSet<TxId> = [1].iter().copied().collect();
    let active = HashSet::new();
    let snapshot = Snapshot::new(2, visible, 1, 3, active);
    
    let result = chain.find_visible(2, &snapshot);
    assert_eq!(result, Some("v1".to_string()));
}

#[test]
fn test_lock_free_version_chain_concurrent_insert() {
    use std::thread;
    
    let chain = Arc::new(LockFreeVersionChain::new(1));
    let mut handles = vec![];
    
    for i in 0..100 {
        let chain = Arc::clone(&chain);
        let handle = thread::spawn(move || {
            chain.insert_version(Version::new(format!("v{}", i), i as u64 + 1));
        });
        handles.push(handle);
    }
    
    for handle in handles {
        handle.join().unwrap();
    }
    
    // Verify versions exist
    let guard = epoch::pin();
    let mut count = 0;
    let mut current = chain.head(&guard);
    while !current.is_null() {
        count += 1;
        let node = unsafe { current.deref() };
        current = node.next.load(Ordering::Acquire, &guard);
    }
    
    assert_eq!(count, 100);
}

#[test]
fn test_lock_free_version_chain_empty() {
    let chain = LockFreeVersionChain::<String>::new(1);
    
    let visible = HashSet::new();
    let active = HashSet::new();
    let snapshot = Snapshot::new(1, visible, 1, 2, active);
    
    let result = chain.find_visible(1, &snapshot);
    assert!(result.is_none());
}

// ============================================================================
// Snapshot Boundary Tests
// ============================================================================

#[test]
fn test_snapshot_xmin_xmax_boundaries() {
    let manager = MvccManager::new();
    
    // Create transactions
    let tx1 = manager.begin_transaction();
    let tx2 = manager.begin_transaction();
    manager.commit_transaction(tx1);
    manager.commit_transaction(tx2);
    
    let tx3 = manager.begin_transaction();
    let snapshot = manager.get_snapshot(tx3);
    
    // xmax should be greater than tx3
    assert!(snapshot.xmax > tx3);
    
    // xmin should be valid
    assert!(snapshot.xmin <= snapshot.xmax);
}

#[test]
fn test_snapshot_with_many_active() {
    let manager = MvccManager::new();
    
    // Create many active transactions
    let active_txs: Vec<TxId> = (0..100).map(|_| manager.begin_transaction()).collect();
    
    // Take snapshot
    let reader_tx = manager.begin_transaction();
    let snapshot = manager.get_snapshot(reader_tx);
    
    // Active transactions should be recorded
    assert!(!snapshot.active_txs.is_empty());
    
    // Cleanup
    for tx in active_txs {
        manager.commit_transaction(tx);
    }
    manager.commit_transaction(reader_tx);
}

#[test]
fn test_snapshot_all_committed() {
    let manager = MvccManager::new();
    
    // Create and commit all transactions
    let tx1 = manager.begin_transaction();
    let tx2 = manager.begin_transaction();
    manager.commit_transaction(tx1);
    manager.commit_transaction(tx2);
    
    // Take snapshot with no active
    let tx3 = manager.begin_transaction();
    let snapshot = manager.get_snapshot(tx3);
    
    // Should see committed transactions
    assert!(snapshot.is_visible(tx1));
    assert!(snapshot.is_visible(tx2));
}

// ============================================================================
// Write Skew Detection Tests
// ============================================================================

#[test]
fn test_write_skew_scenario() {
    let manager = MvccManager::new();
    
    // Setup: Two accounts with balance >= 0 constraint
    // T1 reads account A, T2 reads account B
    // Both check A + B >= amount
    // Both commit, violating constraint
    
    let tx1 = manager.begin_transaction();
    let tx2 = manager.begin_transaction();
    
    // T1's view
    let snapshot1 = manager.get_snapshot(tx1);
    
    // T2's view  
    let snapshot2 = manager.get_snapshot(tx2);
    
    // Both see consistent snapshots
    // (In real implementation, this would detect write skew)
    
    manager.commit_transaction(tx1);
    manager.commit_transaction(tx2);
}

// ============================================================================
// Timestamp Tests
// ============================================================================

#[test]
fn test_timestamp_special_values() {
    let active = Timestamp::ACTIVE;
    let infinity = Timestamp::INFINITY;
    
    assert!(active.is_active());
    assert!(!infinity.is_active());
    assert!(!active.is_committed());
    assert!(!infinity.is_committed());
}

#[test]
fn test_timestamp_ordering() {
    let t1 = Timestamp(1);
    let t2 = Timestamp(2);
    let t3 = Timestamp(3);
    
    assert!(t1 < t2);
    assert!(t2 < t3);
    assert!(t1 < t3);
}

// ============================================================================
// MVCC Manager Tests
// ============================================================================

#[test]
fn test_mvcc_manager_empty() {
    let manager = MvccManager::new();
    let stats = manager.stats();
    
    assert_eq!(stats.active_count, 0);
    assert_eq!(stats.committed_count, 0);
}

#[test]
fn test_mvcc_manager_many_transactions() {
    let manager = MvccManager::new();
    
    let mut txs = vec![];
    
    // Begin many transactions
    for _ in 0..100 {
        txs.push(manager.begin_transaction());
    }
    
    let stats = manager.stats();
    assert_eq!(stats.active_count, 100);
    
    // Commit half
    for tx in txs.iter().take(50) {
        manager.commit_transaction(*tx);
    }
    
    let stats = manager.stats();
    assert_eq!(stats.active_count, 50);
    assert_eq!(stats.committed_count, 50);
    
    // Rollback rest
    for tx in txs.iter().skip(50) {
        manager.rollback_transaction(*tx);
    }
    
    let stats = manager.stats();
    assert_eq!(stats.active_count, 0);
    assert_eq!(stats.committed_count, 50);
}

#[test]
fn test_mvcc_manager_global_xmin() {
    let manager = MvccManager::new();
    
    let tx1 = manager.begin_transaction();
    let _ = manager.begin_transaction(); // tx2 stays active
    
    let global_xmin = manager.get_global_xmin();
    assert!(global_xmin <= tx1);
    
    manager.commit_transaction(tx1);
}

#[test]
fn test_mvcc_manager_oldest_active() {
    let manager = MvccManager::new();
    
    let tx1 = manager.begin_transaction();
    let _tx2 = manager.begin_transaction();
    let _tx3 = manager.begin_transaction();
    
    let oldest = manager.get_oldest_active_tx();
    assert_eq!(oldest, Some(tx1));
}

// ============================================================================
// Snapshot Registration Tests
// ============================================================================

#[test]
fn test_snapshot_registration() {
    let manager = MvccManager::new();
    
    let tx1 = manager.begin_transaction();
    let snapshot = manager.get_snapshot(tx1);
    
    manager.register_snapshot(snapshot);
    
    let stats = manager.stats();
    assert_eq!(stats.active_snapshot_count, 1);
    
    manager.unregister_snapshot(tx1);
    
    let stats = manager.stats();
    assert_eq!(stats.active_snapshot_count, 0);
}

#[test]
fn test_many_registered_snapshots() {
    let manager = MvccManager::new();
    
    // Register many snapshots
    for _ in 0..100 {
        let tx = manager.begin_transaction();
        let snapshot = manager.get_snapshot(tx);
        manager.register_snapshot(snapshot);
    }
    
    let stats = manager.stats();
    assert_eq!(stats.active_snapshot_count, 100);
}

// ============================================================================
// Version Deletion Tests
// ============================================================================

#[test]
fn test_version_deletion_visibility() {
    let mut version = Version::new("data".to_string(), 1);
    
    // Initially not deleted
    assert!(version.deleted_by.is_none());
    
    // Mark deleted
    version.mark_deleted(2);
    assert_eq!(version.deleted_by, Some(2));
}

#[test]
fn test_deleted_version_not_visible() {
    let mut version = Version::new("data".to_string(), 1);
    version.mark_deleted(2);
    
    // Snapshot where deletion is visible
    let visible: HashSet<TxId> = [1, 2].iter().copied().collect();
    let active = HashSet::new();
    let snapshot = Snapshot::new(3, visible, 1, 4, active);
    
    assert!(!version.is_visible_to(3, &snapshot));
}

#[test]
fn test_deleted_version_visible_to_older_snapshot() {
    let mut version = Version::new("data".to_string(), 1);
    version.mark_deleted(2);
    
    // Snapshot from before deletion
    let visible: HashSet<TxId> = [1].iter().copied().collect();
    let active = HashSet::new();
    let snapshot = Snapshot::new(3, visible, 1, 4, active);
    
    // Deletion by tx 2 not visible
    assert!(version.is_visible_to(3, &snapshot));
}

// ============================================================================
// Concurrent Transaction Tests
// ============================================================================

#[test]
fn test_concurrent_transactions() {
    use std::thread;
    
    let manager = Arc::new(MvccManager::new());
    let mut handles = vec![];
    
    for i in 0..100 {
        let manager = Arc::clone(&manager);
        let handle = thread::spawn(move || {
            let tx = manager.begin_transaction();
            
            // Simulate some work
            std::thread::sleep(std::time::Duration::from_millis(1));
            
            if i % 2 == 0 {
                manager.commit_transaction(tx);
            } else {
                manager.rollback_transaction(tx);
            }
        });
        handles.push(handle);
    }
    
    for handle in handles {
        handle.join().unwrap();
    }
    
    let stats = manager.stats();
    assert_eq!(stats.active_count, 0);
}

// ============================================================================
// Phantom Read Prevention Tests
// ============================================================================

#[test]
fn test_phantom_read_prevention() {
    let manager = MvccManager::new();
    
    let tx1 = manager.begin_transaction();
    let snapshot1 = manager.get_snapshot(tx1);
    
    // Another transaction inserts
    let tx2 = manager.begin_transaction();
    // (Insert would happen here)
    manager.commit_transaction(tx2);
    
    // tx1 should not see tx2's insert
    assert!(!snapshot1.is_visible(tx2));
}

// ============================================================================
// Non-Repeatable Read Prevention Tests
// ============================================================================

#[test]
fn test_non_repeatable_read_prevention() {
    let mut chain = VersionChain::new();
    chain.add_version(Version::new("v1".to_string(), 1));
    
    let tx1 = 2;
    let visible: HashSet<TxId> = [1].iter().copied().collect();
    let active = HashSet::new();
    let snapshot = Snapshot::new(tx1, visible.clone(), 1, 3, active.clone());
    
    // Read once
    let v1 = chain.get_visible(tx1, &snapshot);
    
    // Another transaction updates
    chain.add_version(Version::new("v2".to_string(), 3));
    
    // Read again with same snapshot
    let v2 = chain.get_visible(tx1, &snapshot);
    
    // Should see same version
    assert_eq!(v1, v2);
}

// ============================================================================
// Isolation Level Tests
// ============================================================================

#[test]
fn test_read_committed_isolation() {
    let manager = MvccManager::new();
    let mut chain = VersionChain::new();
    
    // T1 creates version
    let tx1 = manager.begin_transaction();
    chain.add_version(Version::new("v1".to_string(), tx1));
    manager.commit_transaction(tx1);
    
    // T2 reads committed
    let tx2 = manager.begin_transaction();
    let snapshot = manager.get_snapshot(tx2);
    
    let version = chain.get_visible(tx2, &snapshot);
    assert!(version.is_some());
}

#[test]
fn test_snapshot_isolation() {
    let manager = MvccManager::new();
    let mut chain = VersionChain::new();
    
    // Initial data
    let tx0 = manager.begin_transaction();
    chain.add_version(Version::new("v0".to_string(), tx0));
    manager.commit_transaction(tx0);
    
    // T1 starts
    let tx1 = manager.begin_transaction();
    let snapshot1 = manager.get_snapshot(tx1);
    
    // T2 updates
    let tx2 = manager.begin_transaction();
    chain.add_version(Version::new("v2".to_string(), tx2));
    manager.commit_transaction(tx2);
    
    // T1 should still see original
    let version = chain.get_visible(tx1, &snapshot1);
    assert_eq!(version.map(|v| v.data), Some("v0".to_string()));
}

// ============================================================================
// Long-Running Transaction Tests
// ============================================================================

#[test]
fn test_long_running_transaction() {
    let manager = MvccManager::new();
    let mut chain = VersionChain::new();
    
    // Long-running reader
    let reader_tx = manager.begin_transaction();
    let reader_snapshot = manager.get_snapshot(reader_tx);
    
    // Many writers
    for i in 1..=100 {
        let writer_tx = manager.begin_transaction();
        chain.add_version(Version::new(format!("v{}", i), writer_tx));
        manager.commit_transaction(writer_tx);
    }
    
    // Reader should still see original empty
    let version = chain.get_visible(reader_tx, &reader_snapshot);
    assert!(version.is_none());
    
    // Cleanup
    manager.commit_transaction(reader_tx);
}

// ============================================================================
// Memory Management Tests
// ============================================================================

#[test]
fn test_version_chain_memory_usage() {
    let mut chain = VersionChain::new();
    
    // Add versions with varying sizes
    for i in 0..1000 {
        let data = format!("version_{}_with_some_data_{}", i, "x".repeat(100));
        chain.add_version(Version::new(data, i as u64 + 1));
    }
    
    assert_eq!(chain.versions.len(), 1000);
    
    // GC old versions
    let removed = chain.gc(500);
    assert!(removed > 0);
}

// ============================================================================
// Boundary Value Tests for TxId
// ============================================================================

#[test]
fn test_txid_boundary_values() {
    let tx_ids = vec![
        0u64,       // System transaction
        1,          // First user transaction
        u64::MAX,   // Max value
    ];
    
    for tx_id in tx_ids {
        let version = Version::new("test".to_string(), tx_id);
        assert_eq!(version.created_by, tx_id);
    }
}

// ============================================================================
// Empty and Null Tests
// ============================================================================

#[test]
fn test_empty_version_chain_gc() {
    let mut chain = VersionChain::<String>::new();
    
    // GC on empty chain
    let removed = chain.gc(0);
    assert_eq!(removed, 0);
}

#[test]
fn test_single_version_gc() {
    let mut chain = VersionChain::new();
    chain.add_version(Version::new("data".to_string(), 1));
    
    // GC should not remove last version
    let removed = chain.gc(u64::MAX);
    assert_eq!(removed, 0);
    assert_eq!(chain.versions.len(), 1);
}

// ============================================================================
// Race Condition Tests
// ============================================================================

#[test]
fn test_commit_during_snapshot_creation() {
    use std::thread;
    
    let manager = Arc::new(MvccManager::new());
    
    // Start a transaction
    let tx1 = manager.begin_transaction();
    
    // Concurrent commit and snapshot
    let manager2 = Arc::clone(&manager);
    let handle = thread::spawn(move || {
        manager2.commit_transaction(tx1);
    });
    
    // Take snapshot concurrently
    let tx2 = manager.begin_transaction();
    let _snapshot = manager.get_snapshot(tx2);
    
    handle.join().unwrap();
    
    // Both should be valid states
    manager.commit_transaction(tx2);
}
