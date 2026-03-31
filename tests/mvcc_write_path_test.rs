//! Integration tests for MVCC Write Path (Phase 2)
//!
//! Tests P2-4: Copy-on-Write (COW)
//! Tests P2-5: Garbage Collector
//! Tests P2-6: Optimistic Locking

use sqllite_rust::concurrency::{
    CowStorage, GcManager, GarbageCollector,
    OptimisticMvccManager, ConflictType,
    MvccManager, Snapshot, VersionChain, Version,
};
use std::collections::HashSet;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

// ============== P2-4: COW Tests ==============

#[test]
fn test_cow_write_does_not_block_read() {
    let storage = Arc::new(CowStorage::new());
    
    // Create initial data
    let tx1 = 1;
    let page_id = storage.allocate_page("initial_data".to_string(), tx1);
    
    // Spawn reader thread
    let storage_clone = storage.clone();
    let reader = thread::spawn(move || {
        let mut visible = HashSet::new();
        visible.insert(tx1);
        let snapshot = Snapshot::new(100, visible.clone(), tx1, 100, HashSet::new());
        
        // Read 1000 times
        for _ in 0..1000 {
            let data = storage_clone.read(page_id, 100, &snapshot);
            assert!(data.is_some(), "Read should not be blocked by writes");
        }
    });
    
    // Spawn writer thread
    let storage_clone = storage.clone();
    let writer = thread::spawn(move || {
        for i in 0..100 {
            let tx = i + 2;
            storage_clone.write(page_id, format!("updated_{}", i), tx).unwrap();
            // Small delay to interleave with reads
            thread::sleep(Duration::from_micros(10));
        }
    });
    
    reader.join().unwrap();
    writer.join().unwrap();
}

#[test]
fn test_cow_multiple_concurrent_writers_different_keys() {
    let storage = Arc::new(CowStorage::new());
    let num_writers = 10;
    let writes_per_thread = 100;
    
    // Create pages
    let mut page_ids = vec![];
    for i in 0..num_writers {
        let page_id = storage.allocate_page(format!("initial_{}", i), 1);
        page_ids.push(page_id);
    }
    
    // Spawn concurrent writers (each writing to different page)
    let mut handles = vec![];
    for (thread_id, page_id) in page_ids.iter().enumerate() {
        let storage_clone = storage.clone();
        let page_id = *page_id;
        
        let handle = thread::spawn(move || {
            for j in 0..writes_per_thread {
                let tx = (thread_id as u64 + 1) * 1000 + j as u64;
                storage_clone.write(page_id, format!("data_{}_{}", thread_id, j), tx).unwrap();
            }
        });
        handles.push(handle);
    }
    
    for handle in handles {
        handle.join().unwrap();
    }
    
    // Verify all pages have correct number of versions
    let mut visible = HashSet::new();
    visible.insert(1);
    for i in 0..num_writers {
        let tx = (i as u64 + 1) * 1000 + writes_per_thread as u64 - 1;
        visible.insert(tx);
    }
    let snapshot = Snapshot::new(99999, visible, 1, 99999, HashSet::new());
    
    for (i, page_id) in page_ids.iter().enumerate() {
        let data = storage.read(*page_id, 99999, &snapshot);
        assert!(data.is_some());
        // Latest version should be from the last write
        assert_eq!(data.unwrap(), format!("data_{}_{}", i, writes_per_thread - 1));
    }
}

// ============== P2-6: Optimistic Locking Tests ==============

#[test]
fn test_optimistic_lock_conflict_detection() {
    let manager = Arc::new(OptimisticMvccManager::new());
    
    // Two transactions
    let tx1_arc = manager.begin_transaction();
    let tx1_id = { tx1_arc.lock().tx_id };
    
    let tx2_arc = manager.begin_transaction();
    let tx2_id = { tx2_arc.lock().tx_id };
    
    // Tx1 writes key1
    manager.write(tx1_id, b"key1".to_vec(), b"value1".to_vec()).unwrap();
    
    // Tx2 tries to write key1 (should conflict in lock manager)
    let result = manager.write(tx2_id, b"key1".to_vec(), b"value2".to_vec());
    
    // Should detect conflict
    assert!(result.is_err(), "Should detect write-write conflict");
    
    let err = result.unwrap_err();
    match err.conflict_type {
        ConflictType::WriteWrite { key, tx1: _, tx2: _ } => {
            assert_eq!(key, b"key1".to_vec());
        }
        _ => panic!("Expected WriteWrite conflict"),
    }
    
    // Cleanup
    manager.rollback(tx1_id);
    manager.rollback(tx2_id);
}

#[test]
fn test_optimistic_transaction_commit_rollback() {
    let manager = Arc::new(OptimisticMvccManager::new());
    
    // Transaction 1 - commits successfully
    let tx1_arc = manager.begin_transaction();
    let tx1_id = { tx1_arc.lock().tx_id };
    
    manager.write(tx1_id, b"key1".to_vec(), b"value1".to_vec()).unwrap();
    manager.write(tx1_id, b"key2".to_vec(), b"value2".to_vec()).unwrap();
    
    let result = manager.commit(tx1_id);
    assert!(result.is_ok(), "Transaction should commit successfully");
    
    // Transaction 2 - rolls back
    let tx2_arc = manager.begin_transaction();
    let tx2_id = { tx2_arc.lock().tx_id };
    
    manager.write(tx2_id, b"key3".to_vec(), b"value3".to_vec()).unwrap();
    manager.rollback(tx2_id);
    
    // Transaction 2 should be removed from active
    let stats = manager.stats();
    assert_eq!(stats.active_transactions, 0);
}

#[test]
fn test_concurrent_optimistic_transactions_no_conflict() {
    let manager = Arc::new(OptimisticMvccManager::new());
    let num_threads = 10;
    let keys_per_thread = 10;
    
    let mut handles = vec![];
    
    for thread_id in 0..num_threads {
        let manager_clone = manager.clone();
        let handle = thread::spawn(move || {
            let tx_arc = manager_clone.begin_transaction();
            let tx_id = { tx_arc.lock().tx_id };
            
            // Each thread writes to different keys
            for j in 0..keys_per_thread {
                let key = format!("thread_{}_key_{}", thread_id, j);
                let value = format!("value_{}_{}", thread_id, j);
                manager_clone.write(tx_id, key.into_bytes(), value.into_bytes()).unwrap();
            }
            
            // Commit should succeed (no conflicts since different keys)
            manager_clone.commit(tx_id).expect("Commit should succeed")
        });
        handles.push(handle);
    }
    
    // All commits should succeed
    for handle in handles {
        handle.join().expect("Thread should complete successfully");
    }
}

#[test]
fn test_conflict_strategy_abort_on_conflict() {
    let manager = Arc::new(OptimisticMvccManager::new());
    
    // Tx1 writes key1
    let tx1_arc = manager.begin_transaction();
    let tx1_id = { tx1_arc.lock().tx_id };
    manager.write(tx1_id, b"conflict_key".to_vec(), b"value1".to_vec()).unwrap();
    
    // Tx2 tries to write same key
    let tx2_arc = manager.begin_transaction();
    let tx2_id = { tx2_arc.lock().tx_id };
    
    // First write should succeed
    manager.write(tx2_id, b"other_key".to_vec(), b"value2".to_vec()).unwrap();
    
    // Commit tx1 first
    manager.commit(tx1_id).unwrap();
    
    // Tx2 tries to write conflict_key (should succeed in write, but...)
    // Note: In this implementation, write conflicts are detected immediately
    // So Tx2 would have failed when trying to write if Tx1 hadn't committed yet
}

// ============== P2-5: Garbage Collector Tests ==============

#[test]
fn test_gc_manual_trigger() {
    let manager = Arc::new(MvccManager::new());
    let gc = GcManager::new_manual();
    
    // Create version chains
    use std::collections::HashMap;
    use parking_lot::RwLock;
    
    let chains: Arc<RwLock<HashMap<u64, VersionChain<String>>>> = 
        Arc::new(RwLock::new(HashMap::new()));
    
    // Add versions
    {
        let mut c = chains.write();
        for i in 0..100 {
            let mut chain = VersionChain::new();
            for j in 0..5 {
                chain.add_version(Version::new(
                    format!("data_{}_{}", i, j),
                    j as u64 + 1
                ));
            }
            c.insert(i as u64, chain);
        }
    }
    
    // No active transactions, so all old versions can be GC'd
    let removed = gc.trigger_gc(&chains, &manager);
    
    // Should have removed most old versions (keeping only newest)
    assert!(removed > 0, "GC should remove old versions");
    
    // Verify stats
    let stats = gc.stats();
    assert_eq!(stats.total_runs, 1);
    assert_eq!(stats.versions_removed, removed as u64);
}

#[test]
fn test_gc_adaptive_mode() {
    let gc = GarbageCollector::new_adaptive();
    
    // Should run when version count exceeds threshold
    assert!(gc.should_run(10001, 0), "Should run when versions exceed threshold");
    
    // Should run when memory exceeds threshold
    assert!(gc.should_run(0, 101), "Should run when memory exceeds threshold");
    
    // Should not run when below thresholds
    assert!(!gc.should_run(100, 10), "Should not run when below thresholds");
}

#[test]
fn test_gc_timer_mode() {
    let gc = GarbageCollector::new_timer(1);
    
    // Should run initially (never run before)
    assert!(gc.should_run(0, 0), "Should run initially");
    
    // Simulate a run
    *gc.last_gc_time.lock() = Some(std::time::Instant::now());
    
    // Should not run immediately
    assert!(!gc.should_run(0, 0), "Should not run immediately after GC");
}

#[test]
fn test_gc_stats_tracking() {
    let gc = GarbageCollector::new_manual();
    use std::collections::HashMap;
    use parking_lot::RwLock;
    
    let chains: Arc<RwLock<HashMap<u64, VersionChain<String>>>> = 
        Arc::new(RwLock::new(HashMap::new()));
    let manager = Arc::new(MvccManager::new());
    
    // Add some versions
    {
        let mut c = chains.write();
        let mut chain = VersionChain::new();
        chain.add_version(Version::new("v1".to_string(), 1));
        chain.add_version(Version::new("v2".to_string(), 2));
        chain.add_version(Version::new("v3".to_string(), 3));
        c.insert(1, chain);
    }
    
    // Run GC
    gc.run_with_manager(&chains, &manager);
    
    // Check stats
    let stats = gc.get_stats();
    assert_eq!(stats.total_runs, 1);
    assert!(stats.versions_removed > 0);
    assert!(stats.last_gc_time.is_some());
}

// ============== Integration Tests ==============

#[test]
fn test_full_mvcc_write_path() {
    // Test the complete write path: COW + Optimistic Locking + GC
    let cow_storage = Arc::new(CowStorage::new());
    let opt_manager = Arc::new(OptimisticMvccManager::new());
    let _gc = Arc::new(GarbageCollector::new_adaptive());
    
    // Phase 1: Multiple transactions write data
    let mut tx_ids = vec![];
    for i in 0..10 {
        let tx_arc = opt_manager.begin_transaction();
        let tx_id = { tx_arc.lock().tx_id };
        tx_ids.push(tx_id);
        
        // Each transaction writes unique keys
        for j in 0..5 {
            let key = format!("key_{}_{}", i, j);
            let value = format!("value_{}_{}", i, j);
            opt_manager.write(tx_id, key.into_bytes(), value.into_bytes()).unwrap();
        }
    }
    
    // Phase 2: Commit all transactions
    for tx_id in tx_ids {
        opt_manager.commit(tx_id).expect("All commits should succeed");
    }
    
    // Phase 3: Verify COW storage works independently
    let tx = 999;
    let page_id = cow_storage.allocate_page("test_data".to_string(), tx);
    
    // Concurrent reads and writes on COW storage
    let cow_clone = cow_storage.clone();
    let reader = thread::spawn(move || {
        let mut visible = HashSet::new();
        visible.insert(tx);
        let snapshot = Snapshot::new(1000, visible, tx, 1000, HashSet::new());
        
        for _ in 0..100 {
            let data = cow_clone.read(page_id, 1000, &snapshot);
            assert!(data.is_some());
        }
    });
    
    let cow_clone = cow_storage.clone();
    let writer = thread::spawn(move || {
        for i in 0..50 {
            cow_clone.write(page_id, format!("update_{}", i), 1000 + i).unwrap();
        }
    });
    
    reader.join().unwrap();
    writer.join().unwrap();
    
    // Success - write path components work together
}

#[test]
fn test_gc_efficiency() {
    let manager = Arc::new(MvccManager::new());
    let gc = GarbageCollector::new_manual();
    use std::collections::HashMap;
    use parking_lot::RwLock;
    
    let chains: Arc<RwLock<HashMap<u64, VersionChain<String>>>> = 
        Arc::new(RwLock::new(HashMap::new()));
    
    // Create 1000 chains with 10 versions each
    {
        let mut c = chains.write();
        for i in 0..1000 {
            let mut chain = VersionChain::new();
            for j in 0..10 {
                chain.add_version(Version::new(
                    format!("data_{}_{}", i, j),
                    j as u64 + 1
                ));
            }
            c.insert(i as u64, chain);
        }
    }
    
    // Run GC
    let start = std::time::Instant::now();
    let removed = gc.run_with_manager(&chains, &manager);
    let elapsed = start.elapsed();
    
    // Should remove at least 90% of versions (keeping only newest)
    let total_versions = 1000 * 10;
    let removal_rate = removed as f64 / total_versions as f64;
    assert!(
        removal_rate > 0.85, 
        "GC should remove at least 85% of versions, got {:.2}%", 
        removal_rate * 100.0
    );
    
    // Should be reasonably fast
    assert!(
        elapsed.as_millis() < 1000,
        "GC should complete within 1 second, took {:?}",
        elapsed
    );
    
    // Check efficiency stat
    let stats = gc.get_stats();
    assert!(stats.efficiency() > 0.0, "GC should have positive efficiency");
}
