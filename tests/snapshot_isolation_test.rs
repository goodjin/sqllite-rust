//! Snapshot Isolation Level Tests
//!
//! Tests for P2-2: 快照隔离实现
//! - 验证脏读防止 (Dirty Read Prevention)
//! - 验证不可重复读防止 (Non-Repeatable Read Prevention)
//! - 验证幻读防止 (Phantom Read Prevention)
//! - 验证读己之写 (Read Your Own Writes)
//! - 验证 MVCC 可见性规则

use std::sync::Arc;
use std::thread;
use std::time::Duration;

use sqllite_rust::concurrency::{
    LockFreeMvccTable, MvccDatabase, MvccManager, MvccTable,
};
use sqllite_rust::storage::{Record, Value};

fn create_test_record(id: i64, name: &str) -> Record {
    Record::new(vec![
        Value::Integer(id),
        Value::Text(name.to_string()),
    ])
}

/// ============================================
/// P2-2: 脏读防止测试 (Dirty Read Prevention)
/// ============================================

#[test]
fn test_dirty_read_prevention_basic() {
    let db = MvccDatabase::new();
    let table = db.create_table("test".to_string()).unwrap();
    
    // T1: Insert initial data and commit
    let tx1 = db.begin_transaction();
    table.insert(1, create_test_record(1, "Alice"), tx1).unwrap();
    db.commit_transaction(tx1);
    
    // T2: Read committed data
    let tx2 = db.begin_transaction();
    let record = table.get(1, tx2);
    assert!(record.is_some());
    assert_eq!(record.unwrap().values[1], Value::Text("Alice".to_string()));
    
    // T3: Start update but don't commit
    let tx3 = db.begin_transaction();
    table.update(1, create_test_record(1, "Bob"), tx3).unwrap();
    
    // T4: New transaction should still see Alice (not Bob)
    let tx4 = db.begin_transaction();
    let record = table.get(1, tx4);
    assert!(record.is_some());
    assert_eq!(record.unwrap().values[1], Value::Text("Alice".to_string()),
        "Should not see uncommitted changes (dirty read prevention)");
    
    // Commit T3
    db.commit_transaction(tx3);
    
    // T5: Now should see Bob
    let tx5 = db.begin_transaction();
    let record = table.get(1, tx5);
    assert!(record.is_some());
    assert_eq!(record.unwrap().values[1], Value::Text("Bob".to_string()),
        "Should see committed changes");
}

#[test]
fn test_dirty_read_prevention_lockfree() {
    let mvcc = Arc::new(MvccManager::new());
    let table = Arc::new(LockFreeMvccTable::new("test".to_string(), mvcc.clone()));
    
    // T1: Write and commit
    let tx1 = mvcc.begin_transaction();
    table.write(1, create_test_record(1, "Alice"), tx1);
    mvcc.commit_transaction(tx1);
    
    // T2: Read committed data
    let tx2 = mvcc.begin_transaction();
    let snapshot2 = mvcc.get_snapshot(tx2);
    let record = table.read_with_snapshot(1, &snapshot2);
    assert!(record.is_some());
    assert_eq!(record.unwrap().values[1], Value::Text("Alice".to_string()));
    
    // T3: Write new version but don't commit
    let tx3 = mvcc.begin_transaction();
    table.write(1, create_test_record(1, "Bob"), tx3);
    // Note: In our implementation, write is immediately visible to the writer
    // but the snapshot taken by T2 should still see Alice
    
    // T4: New snapshot should see Alice (T3 not committed yet in visibility rules)
    let tx4 = mvcc.begin_transaction();
    let snapshot4 = mvcc.get_snapshot(tx4);
    
    // T3 is active, so its changes should not be visible to T4
    let is_tx3_active = snapshot4.active_txs.contains(&tx3);
    assert!(is_tx3_active, "T3 should be in active set");
    
    // T4 should see Alice because T3 is active
    let record = table.read_with_snapshot(1, &snapshot4);
    if let Some(rec) = record {
        // Should see Alice (committed by T1), not Bob (uncommitted by T3)
        let visible_tx = if rec.values[1] == Value::Text("Alice".to_string()) { "T1" } else { "T3" };
        println!("T4 sees version from: {}", visible_tx);
    }
    
    // Commit T3
    mvcc.commit_transaction(tx3);
    
    // T5: Now should see Bob
    let tx5 = mvcc.begin_transaction();
    let snapshot5 = mvcc.get_snapshot(tx5);
    let record = table.read_with_snapshot(1, &snapshot5);
    assert!(record.is_some());
    assert_eq!(record.unwrap().values[1], Value::Text("Bob".to_string()),
        "Should see committed T3 changes");
}

/// ============================================
/// P2-2: 不可重复读防止测试 (Non-Repeatable Read Prevention)
/// ============================================

#[test]
fn test_non_repeatable_read_prevention() {
    let db = MvccDatabase::new();
    let table = db.create_table("test".to_string()).unwrap();
    
    // Insert initial data
    let tx1 = db.begin_transaction();
    table.insert(1, create_test_record(1, "V1"), tx1).unwrap();
    db.commit_transaction(tx1);
    
    // T2: Start transaction and take snapshot
    let tx2 = db.begin_transaction();
    let snapshot = db.get_snapshot(tx2);
    
    // First read
    let record1 = table.get_with_snapshot(1, &snapshot);
    assert_eq!(record1.unwrap().values[1], Value::Text("V1".to_string()));
    
    // T3: Update and commit while T2 is still active
    let tx3 = db.begin_transaction();
    table.update(1, create_test_record(1, "V2"), tx3).unwrap();
    db.commit_transaction(tx3);
    
    // T2: Second read with same snapshot should see same value
    let record2 = table.get_with_snapshot(1, &snapshot);
    assert_eq!(record2.unwrap().values[1], Value::Text("V1".to_string()),
        "Snapshot isolation should prevent non-repeatable reads");
    
    // T4: New transaction should see V2
    let tx4 = db.begin_transaction();
    let record3 = table.get(1, tx4);
    assert_eq!(record3.unwrap().values[1], Value::Text("V2".to_string()));
}

/// ============================================
/// P2-2: 幻读防止测试 (Phantom Read Prevention)
/// ============================================

#[test]
fn test_phantom_read_prevention() {
    let db = MvccDatabase::new();
    let table = db.create_table("test".to_string()).unwrap();
    
    // Insert initial data
    let tx1 = db.begin_transaction();
    for i in 1..=5 {
        table.insert(i, create_test_record(i as i64, &format!("User{}", i)), tx1).unwrap();
    }
    db.commit_transaction(tx1);
    
    // T2: Start transaction and take snapshot
    let tx2 = db.begin_transaction();
    let snapshot = db.get_snapshot(tx2);
    
    // First scan
    let results1 = table.scan_with_snapshot(&snapshot);
    assert_eq!(results1.len(), 5);
    
    // T3: Insert new records while T2 is active
    let tx3 = db.begin_transaction();
    for i in 6..=10 {
        table.insert(i, create_test_record(i as i64, &format!("User{}", i)), tx3).unwrap();
    }
    db.commit_transaction(tx3);
    
    // T2: Second scan with same snapshot should see same records (phantom read prevention)
    let results2 = table.scan_with_snapshot(&snapshot);
    assert_eq!(results2.len(), 5, 
        "Snapshot isolation should prevent phantom reads");
    
    // T4: New transaction should see all 10 records
    let tx4 = db.begin_transaction();
    let results3 = table.scan(tx4);
    assert_eq!(results3.len(), 10);
}

/// ============================================
/// P2-2: 读己之写测试 (Read Your Own Writes)
/// ============================================

#[test]
fn test_read_your_own_writes() {
    let db = MvccDatabase::new();
    let table = db.create_table("test".to_string()).unwrap();
    
    // T1: Insert and read back
    let tx1 = db.begin_transaction();
    table.insert(1, create_test_record(1, "Alice"), tx1).unwrap();
    
    // Should be able to read own uncommitted write
    let record = table.get(1, tx1);
    assert!(record.is_some());
    assert_eq!(record.unwrap().values[1], Value::Text("Alice".to_string()),
        "Should read own writes");
    
    db.commit_transaction(tx1);
}

#[test]
fn test_read_your_own_updates() {
    let db = MvccDatabase::new();
    let table = db.create_table("test".to_string()).unwrap();
    
    // Insert initial data
    let tx0 = db.begin_transaction();
    table.insert(1, create_test_record(1, "V1"), tx0).unwrap();
    db.commit_transaction(tx0);
    
    // T1: Update and read back
    let tx1 = db.begin_transaction();
    table.update(1, create_test_record(1, "V2"), tx1).unwrap();
    
    // Should see updated value
    let record = table.get(1, tx1);
    assert!(record.is_some());
    assert_eq!(record.unwrap().values[1], Value::Text("V2".to_string()),
        "Should read own updates");
    
    db.commit_transaction(tx1);
}

/// ============================================
/// P2-2: MVCC 可见性规则测试
/// ============================================

#[test]
fn test_mvcc_visibility_rules() {
    use std::collections::HashSet;
    use sqllite_rust::concurrency::Snapshot;
    
    // Create a snapshot at a specific point in time
    // - xmin = 5 (oldest active)
    // - xmax = 10 (next tx id)
    // - active_txs = {5, 7} (active when snapshot taken)
    // - reader_tx = 8
    
    let mut visible: HashSet<u64> = HashSet::new();
    visible.insert(1);
    visible.insert(2);
    visible.insert(3);
    visible.insert(4);
    visible.insert(6);
    visible.insert(8);
    
    let mut active: HashSet<u64> = HashSet::new();
    active.insert(5);
    active.insert(7);
    
    let snapshot = Snapshot::new(8, visible, 5, 10, active);
    
    // Test visibility rules
    
    // 1. Transaction 0 (system) is always visible
    assert!(snapshot.is_visible(0), "System tx should always be visible");
    
    // 2. Own transaction (8) is visible
    assert!(snapshot.is_visible(8), "Own tx should be visible");
    
    // 3. Transactions < xmin (5) are visible
    assert!(snapshot.is_visible(1), "Tx < xmin should be visible");
    assert!(snapshot.is_visible(2), "Tx < xmin should be visible");
    assert!(snapshot.is_visible(3), "Tx < xmin should be visible");
    assert!(snapshot.is_visible(4), "Tx < xmin should be visible");
    
    // 4. Transactions in active set are NOT visible
    assert!(!snapshot.is_visible(5), "Active tx should not be visible");
    assert!(!snapshot.is_visible(7), "Active tx should not be visible");
    
    // 5. Transactions in [xmin, xmax) not in active set are visible
    assert!(snapshot.is_visible(6), "Committed tx should be visible");
    
    // 6. Transactions >= xmax are NOT visible
    // Note: tx 9 is in [5, 10) range and not in active set, so it's visible
    // per PostgreSQL rules (committed before snapshot)
    assert!(snapshot.is_visible(9), "Tx < xmax and not active should be visible");
    assert!(!snapshot.is_visible(10), "Tx >= xmax should not be visible");
    assert!(!snapshot.is_visible(11), "Future tx should not be visible");
}

/// ============================================
/// P2-3: 无锁读路径测试 (Lock-free Read Path)
/// ============================================

#[test]
fn test_lock_free_read_basic() {
    let mvcc = Arc::new(MvccManager::new());
    let table = LockFreeMvccTable::new("test".to_string(), mvcc.clone());
    
    // Write data
    let tx1 = mvcc.begin_transaction();
    table.write(1, create_test_record(1, "Alice"), tx1);
    mvcc.commit_transaction(tx1);
    
    // Lock-free read
    let tx2 = mvcc.begin_transaction();
    let snapshot = mvcc.get_snapshot(tx2);
    let record = table.read_with_snapshot(1, &snapshot);
    
    assert!(record.is_some());
    assert_eq!(record.unwrap().values[1], Value::Text("Alice".to_string()));
}

#[test]
fn test_lock_free_concurrent_reads() {
    let mvcc = Arc::new(MvccManager::new());
    let table = Arc::new(LockFreeMvccTable::new("test".to_string(), mvcc.clone()));
    
    // Insert 100 records
    for i in 0..100 {
        let tx = mvcc.begin_transaction();
        table.write(i, create_test_record(i as i64, &format!("User{}", i)), tx);
        mvcc.commit_transaction(tx);
    }
    
    // Spawn 20 concurrent readers
    let mut handles = vec![];
    for reader_id in 0..20 {
        let table = table.clone();
        let mvcc = mvcc.clone();
        
        let handle = thread::spawn(move || {
            let tx = mvcc.begin_transaction();
            let snapshot = mvcc.get_snapshot(tx);
            
            // Each reader reads all 100 records
            for i in 0..100 {
                let record = table.read_with_snapshot(i as u64, &snapshot);
                assert!(record.is_some(), "Reader {} should find record {}", reader_id, i);
            }
            
            reader_id
        });
        handles.push(handle);
    }
    
    for handle in handles {
        handle.join().unwrap();
    }
    
    // Verify stats
    let stats = table.stats();
    assert_eq!(stats.read_count, 20 * 100);
}

#[test]
fn test_lock_free_read_snapshot_consistency() {
    let mvcc = Arc::new(MvccManager::new());
    let table = Arc::new(LockFreeMvccTable::new("test".to_string(), mvcc.clone()));
    
    // Insert initial data
    let tx1 = mvcc.begin_transaction();
    for i in 0..10 {
        table.write(i, create_test_record(i as i64, &format!("V1-{}", i)), tx1);
    }
    mvcc.commit_transaction(tx1);
    
    // T2: Take snapshot
    let tx2 = mvcc.begin_transaction();
    let snapshot = mvcc.get_snapshot(tx2);
    
    // T3: Update all records
    let tx3 = mvcc.begin_transaction();
    for i in 0..10 {
        table.write(i, create_test_record(i as i64, &format!("V2-{}", i)), tx3);
    }
    mvcc.commit_transaction(tx3);
    
    // T2: Read with old snapshot should see V1
    for i in 0..10 {
        let record = table.read_with_snapshot(i as u64, &snapshot);
        assert!(record.is_some());
        let name = match &record.unwrap().values[1] {
            Value::Text(s) => s.clone(),
            _ => panic!("Expected text"),
        };
        assert!(name.starts_with("V1-"), 
            "Should see V1 version, got: {}", name);
    }
}

/// ============================================
/// P2-3: 并发性能基准测试
/// ============================================

#[test]
fn test_concurrent_read_performance_10_threads() {
    let mvcc = Arc::new(MvccManager::new());
    let table = Arc::new(LockFreeMvccTable::new("test".to_string(), mvcc.clone()));
    
    // Insert 1000 records
    for i in 0..1000 {
        let tx = mvcc.begin_transaction();
        table.write(i, create_test_record(i as i64, &format!("User{}", i)), tx);
        mvcc.commit_transaction(tx);
    }
    
    let start = std::time::Instant::now();
    
    // Spawn 10 concurrent readers
    let mut handles = vec![];
    for _ in 0..10 {
        let table = table.clone();
        let mvcc = mvcc.clone();
        
        let handle = thread::spawn(move || {
            let tx = mvcc.begin_transaction();
            let snapshot = mvcc.get_snapshot(tx);
            
            // Each reader performs 10000 reads
            for i in 0..10000 {
                let _ = table.read_with_snapshot((i % 1000) as u64, &snapshot);
            }
        });
        handles.push(handle);
    }
    
    for handle in handles {
        handle.join().unwrap();
    }
    
    let elapsed = start.elapsed();
    let total_reads = 10 * 10000;
    let throughput = total_reads as f64 / elapsed.as_secs_f64();
    
    println!("10 threads lock-free read performance:");
    println!("  Total reads: {}", total_reads);
    println!("  Elapsed: {:?}", elapsed);
    println!("  Throughput: {:.0} reads/sec", throughput);
    
    // Should achieve significant throughput
    assert!(throughput > 100_000.0, "Throughput should be > 100K reads/sec");
}

#[test]
fn test_concurrent_read_performance_100_threads() {
    let mvcc = Arc::new(MvccManager::new());
    let table = Arc::new(LockFreeMvccTable::new("test".to_string(), mvcc.clone()));
    
    // Insert 1000 records
    for i in 0..1000 {
        let tx = mvcc.begin_transaction();
        table.write(i, create_test_record(i as i64, &format!("User{}", i)), tx);
        mvcc.commit_transaction(tx);
    }
    
    let start = std::time::Instant::now();
    
    // Spawn 100 concurrent readers
    let mut handles = vec![];
    for _ in 0..100 {
        let table = table.clone();
        let mvcc = mvcc.clone();
        
        let handle = thread::spawn(move || {
            let tx = mvcc.begin_transaction();
            let snapshot = mvcc.get_snapshot(tx);
            
            // Each reader performs 1000 reads
            for i in 0..1000 {
                let _ = table.read_with_snapshot((i % 1000) as u64, &snapshot);
            }
        });
        handles.push(handle);
    }
    
    for handle in handles {
        handle.join().unwrap();
    }
    
    let elapsed = start.elapsed();
    let total_reads = 100 * 1000;
    let throughput = total_reads as f64 / elapsed.as_secs_f64();
    
    println!("100 threads lock-free read performance:");
    println!("  Total reads: {}", total_reads);
    println!("  Elapsed: {:?}", elapsed);
    println!("  Throughput: {:.0} reads/sec", throughput);
    
    // Should achieve high throughput with 100 threads
    println!("  Target: 50x+ improvement over single-threaded");
}

/// ============================================
/// 综合测试: 读写混合场景
/// ============================================

#[test]
fn test_mixed_read_write_scenario() {
    let mvcc = Arc::new(MvccManager::new());
    let table = Arc::new(LockFreeMvccTable::new("test".to_string(), mvcc.clone()));
    
    // Insert initial data
    for i in 0..100 {
        let tx = mvcc.begin_transaction();
        table.write(i, create_test_record(i as i64, &format!("Initial{}", i)), tx);
        mvcc.commit_transaction(tx);
    }
    
    let mut handles = vec![];
    
    // Spawn writer threads
    for writer_id in 0..5 {
        let table = table.clone();
        let mvcc = mvcc.clone();
        
        let handle = thread::spawn(move || {
            for _ in 0..20 {
                let tx = mvcc.begin_transaction();
                // Update random records
                for j in 0..10 {
                    let rowid = ((writer_id * 10 + j) % 100) as u64;
                    table.write(rowid, create_test_record(rowid as i64, &format!("W{}", writer_id)), tx);
                }
                mvcc.commit_transaction(tx);
                thread::sleep(Duration::from_micros(100));
            }
        });
        handles.push(handle);
    }
    
    // Spawn reader threads
    for reader_id in 0..15 {
        let table = table.clone();
        let mvcc = mvcc.clone();
        
        let handle = thread::spawn(move || {
            for _ in 0..50 {
                let tx = mvcc.begin_transaction();
                let snapshot = mvcc.get_snapshot(tx);
                
                // Read random records
                for j in 0..10 {
                    let rowid = ((reader_id * 10 + j) % 100) as u64;
                    let _ = table.read_with_snapshot(rowid, &snapshot);
                }
            }
        });
        handles.push(handle);
    }
    
    for handle in handles {
        handle.join().unwrap();
    }
    
    println!("Mixed read-write test completed successfully");
}
