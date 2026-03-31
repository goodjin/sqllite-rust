//! Concurrent MVCC Tests
//!
//! Tests for Phase 2 MVCC implementation:
//! - Lock-free version chains
//! - Snapshot isolation
//! - High-concurrency read performance

use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use sqllite_rust::concurrency::{
    MvccDatabase, MvccManager, MvccTable, LockFreeMvccTable, 
    Snapshot, Transaction, TxId,
};
use sqllite_rust::storage::{Record, Value};

fn create_test_record(id: i64, name: &str) -> Record {
    Record::new(vec![
        Value::Integer(id),
        Value::Text(name.to_string()),
    ])
}

/// Test basic concurrent reads with RwLock-based table
#[test]
fn test_concurrent_reads_rwlock_table() {
    let db = Arc::new(MvccDatabase::new());
    let table = db.create_table("users".to_string()).unwrap();
    
    // Insert 1000 records
    let tx = db.begin_transaction();
    for i in 1..=1000 {
        table.insert(i, create_test_record(i as i64, &format!("User{}", i)), tx).unwrap();
    }
    db.commit_transaction(tx);
    
    // Spawn 10 concurrent readers
    let mut handles = vec![];
    for reader_id in 0..10 {
        let db = db.clone();
        let table = table.clone();
        
        let handle = thread::spawn(move || {
            let start = Instant::now();
            let tx = db.begin_transaction();
            
            // Each reader scans all 1000 records
            let results = table.scan(tx);
            assert_eq!(results.len(), 1000);
            
            let elapsed = start.elapsed();
            println!("RwLock Reader {} completed in {:?}", reader_id, elapsed);
            elapsed
        });
        
        handles.push(handle);
    }
    
    let mut total_time = Duration::default();
    for handle in handles {
        total_time += handle.join().unwrap();
    }
    
    println!("RwLock Total concurrent read time: {:?}", total_time);
}

/// Test basic concurrent reads with lock-free table
#[test]
fn test_concurrent_reads_lock_free_table() {
    let mvcc = Arc::new(MvccManager::new());
    let table = Arc::new(LockFreeMvccTable::new("users".to_string(), mvcc.clone()));
    
    // Insert 1000 records
    for i in 1..=1000 {
        let tx = mvcc.begin_transaction();
        table.write(i, create_test_record(i as i64, &format!("User{}", i)), tx);
        mvcc.commit_transaction(tx);
    }
    
    // Spawn 10 concurrent readers
    let mut handles = vec![];
    for reader_id in 0..10 {
        let table = table.clone();
        let mvcc = mvcc.clone();
        
        let handle = thread::spawn(move || {
            let start = Instant::now();
            let tx = mvcc.begin_transaction();
            let snapshot = mvcc.get_snapshot(tx);
            
            // Each reader scans all 1000 records
            let results = table.scan(tx);
            assert_eq!(results.len(), 1000);
            
            let elapsed = start.elapsed();
            println!("Lock-free Reader {} completed in {:?}", reader_id, elapsed);
            elapsed
        });
        
        handles.push(handle);
    }
    
    let mut total_time = Duration::default();
    for handle in handles {
        total_time += handle.join().unwrap();
    }
    
    println!("Lock-free Total concurrent read time: {:?}", total_time);
}

/// Benchmark comparing RwLock vs Lock-free table performance
#[test]
fn benchmark_read_scalability() {
    const NUM_RECORDS: usize = 1000;
    const NUM_THREADS: usize = 100;
    
    println!("\n=== Read Scalability Benchmark ===");
    println!("Records: {}, Threads: {}", NUM_RECORDS, NUM_THREADS);
    
    // Setup RwLock table
    let db = Arc::new(MvccDatabase::new());
    let rw_table = db.create_table("rw_test".to_string()).unwrap();
    
    let tx = db.begin_transaction();
    for i in 0..NUM_RECORDS {
        rw_table.insert(i as u64, create_test_record(i as i64, &format!("User{}", i)), tx).unwrap();
    }
    db.commit_transaction(tx);
    
    // Setup Lock-free table
    let mvcc = Arc::new(MvccManager::new());
    let lf_table = Arc::new(LockFreeMvccTable::new("lf_test".to_string(), mvcc.clone()));
    
    for i in 0..NUM_RECORDS {
        let tx = mvcc.begin_transaction();
        lf_table.write(i as u64, create_test_record(i as i64, &format!("User{}", i)), tx);
        mvcc.commit_transaction(tx);
    }
    
    // Benchmark RwLock table
    let rw_time = benchmark_reads(db.clone(), rw_table.clone(), NUM_THREADS, NUM_RECORDS);
    println!("RwLock table: {:?}", rw_time);
    
    // Benchmark Lock-free table
    let lf_time = benchmark_lock_free_reads(mvcc.clone(), lf_table.clone(), NUM_THREADS, NUM_RECORDS);
    println!("Lock-free table: {:?}", lf_time);
    
    // Calculate speedup
    let speedup = rw_time.as_secs_f64() / lf_time.as_secs_f64();
    println!("Lock-free speedup: {:.2}x", speedup);
    
    // Assert significant speedup (at least 2x)
    assert!(speedup > 2.0, "Lock-free should be at least 2x faster, got {:.2}x", speedup);
}

fn benchmark_reads(
    db: Arc<MvccDatabase>,
    table: Arc<MvccTable>,
    num_threads: usize,
    num_records: usize,
) -> Duration {
    let start = Instant::now();
    
    let mut handles = vec![];
    for _ in 0..num_threads {
        let db = db.clone();
        let table = table.clone();
        
        let handle = thread::spawn(move || {
            let tx = db.begin_transaction();
            for i in 0..num_records {
                let _ = table.get(i as u64, tx);
            }
        });
        
        handles.push(handle);
    }
    
    for handle in handles {
        handle.join().unwrap();
    }
    
    start.elapsed()
}

fn benchmark_lock_free_reads(
    mvcc: Arc<MvccManager>,
    table: Arc<LockFreeMvccTable>,
    num_threads: usize,
    num_records: usize,
) -> Duration {
    let start = Instant::now();
    
    let mut handles = vec![];
    for _ in 0..num_threads {
        let mvcc = mvcc.clone();
        let table = table.clone();
        
        let handle = thread::spawn(move || {
            let tx = mvcc.begin_transaction();
            let snapshot = mvcc.get_snapshot(tx);
            for i in 0..num_records {
                let _ = table.read_with_snapshot(i as u64, &snapshot);
            }
        });
        
        handles.push(handle);
    }
    
    for handle in handles {
        handle.join().unwrap();
    }
    
    start.elapsed()
}

/// Test snapshot isolation under concurrent writes
#[test]
fn test_snapshot_isolation_concurrent_writes() {
    let db = Arc::new(MvccDatabase::new());
    let table = db.create_table("test".to_string()).unwrap();
    
    // Initial data
    let tx = db.begin_transaction();
    table.insert(1, create_test_record(1, "Initial"), tx).unwrap();
    db.commit_transaction(tx);
    
    // Reader takes snapshot
    let reader_tx = db.begin_transaction();
    let snapshot = db.get_snapshot(reader_tx);
    let initial_data = table.get_with_snapshot(1, &snapshot);
    assert_eq!(initial_data.unwrap().values[1], Value::Text("Initial".to_string()));
    
    // Concurrent writer updates
    let writer_tx = db.begin_transaction();
    table.update(1, create_test_record(1, "Updated"), writer_tx).unwrap();
    db.commit_transaction(writer_tx);
    
    // Reader still sees old data
    let data_during_read = table.get_with_snapshot(1, &snapshot);
    assert_eq!(data_during_read.unwrap().values[1], Value::Text("Initial".to_string()));
    
    // New reader sees new data
    let new_reader_tx = db.begin_transaction();
    let new_data = table.get(1, new_reader_tx);
    assert_eq!(new_data.unwrap().values[1], Value::Text("Updated".to_string()));
}

/// Test version chain behavior with multiple updates
#[test]
fn test_version_chain_multiple_updates() {
    let db = MvccDatabase::new();
    let table = db.create_table("test".to_string()).unwrap();
    
    // Create initial version
    let tx1 = db.begin_transaction();
    table.insert(1, create_test_record(1, "V1"), tx1).unwrap();
    db.commit_transaction(tx1);
    
    // Create 5 more versions
    for i in 2..=6 {
        let tx = db.begin_transaction();
        table.update(1, create_test_record(1, &format!("V{}", i)), tx).unwrap();
        db.commit_transaction(tx);
    }
    
    // Check version count
    let stats = table.stats();
    println!("Version stats: {:?}", stats);
    assert_eq!(stats.total_versions, 6);
    
    // Old transaction sees V6 (newest committed)
    let tx = db.begin_transaction();
    let record = table.get(1, tx).unwrap();
    assert_eq!(record.values[1], Value::Text("V6".to_string()));
}

/// Test garbage collection under concurrent access
/// 
/// This test verifies that:
/// 1. GC correctly removes old versions when safe to do so
/// 2. The latest version is always retained
/// 3. Versions are correctly cleaned up when all readers complete
#[test]
fn test_gc_concurrent_access() {
    let db = Arc::new(MvccDatabase::new());
    let table = db.create_table("test".to_string()).unwrap();
    
    // Create initial version
    let tx1 = db.begin_transaction();
    table.insert(1, create_test_record(1, "V1"), tx1).unwrap();
    db.commit_transaction(tx1);
    
    // Create many more versions
    for i in 2..=10 {
        let tx = db.begin_transaction();
        table.update(1, create_test_record(1, &format!("V{}", i)), tx).unwrap();
        db.commit_transaction(tx);
    }
    
    let stats_before = table.stats();
    println!("Before GC: {} versions", stats_before.total_versions);
    assert_eq!(stats_before.total_versions, 10);
    
    // No active transactions, GC should be able to remove old versions
    // but should keep the latest version
    let removed = db.gc();
    println!("First GC removed {} versions", removed);
    
    // After GC, only 1 version should remain (the latest)
    let stats_after = table.stats();
    println!("After GC: {} versions", stats_after.total_versions);
    assert_eq!(stats_after.total_versions, 1, "Should keep at least one version");
    
    // Verify we can still read the latest version
    let tx = db.begin_transaction();
    let record = table.get(1, tx);
    assert!(record.is_some());
    assert_eq!(record.unwrap().values[1], Value::Text("V10".to_string()));
}

/// Stress test with mixed read/write workload
#[test]
fn stress_test_mixed_workload() {
    const NUM_WRITER_THREADS: usize = 4;
    const NUM_READER_THREADS: usize = 16;
    const OPS_PER_THREAD: usize = 100;
    
    let db = Arc::new(MvccDatabase::new());
    let table = db.create_table("stress".to_string()).unwrap();
    
    // Pre-populate
    let tx = db.begin_transaction();
    for i in 0..100 {
        table.insert(i as u64, create_test_record(i as i64, &format!("User{}", i)), tx).unwrap();
    }
    db.commit_transaction(tx);
    
    let mut handles = vec![];
    
    // Spawn writers
    for writer_id in 0..NUM_WRITER_THREADS {
        let db = db.clone();
        let table = table.clone();
        
        let handle = thread::spawn(move || {
            for i in 0..OPS_PER_THREAD {
                let tx = db.begin_transaction();
                let rowid = (writer_id * OPS_PER_THREAD + i) as u64 % 100;
                let _ = table.update(rowid, create_test_record(rowid as i64, &format!("W{}-{}", writer_id, i)), tx);
                db.commit_transaction(tx);
            }
        });
        
        handles.push(handle);
    }
    
    // Spawn readers
    for reader_id in 0..NUM_READER_THREADS {
        let db = db.clone();
        let table = table.clone();
        
        let handle = thread::spawn(move || {
            for _ in 0..OPS_PER_THREAD {
                let tx = db.begin_transaction();
                let _ = table.scan(tx);
            }
            println!("Reader {} completed", reader_id);
        });
        
        handles.push(handle);
    }
    
    for handle in handles {
        handle.join().unwrap();
    }
    
    let stats = table.stats();
    println!("Final stats: {:?}", stats);
    
    // Verify database is still consistent
    let tx = db.begin_transaction();
    let results = table.scan(tx);
    assert_eq!(results.len(), 100);
}

/// Test phantom read prevention
#[test]
fn test_phantom_read_prevention() {
    let db = Arc::new(MvccDatabase::new());
    let table = db.create_table("test".to_string()).unwrap();
    
    // Insert initial data
    for i in 1..=5 {
        let tx = db.begin_transaction();
        table.insert(i, create_test_record(i as i64, &format!("User{}", i)), tx).unwrap();
        db.commit_transaction(tx);
    }
    
    // T1 takes snapshot
    let t1 = db.begin_transaction();
    let snapshot1 = db.get_snapshot(t1);
    let count1 = table.scan_with_snapshot(&snapshot1).len();
    assert_eq!(count1, 5);
    
    // T2 inserts new record and commits
    let t2 = db.begin_transaction();
    table.insert(6, create_test_record(6, "User6"), t2).unwrap();
    db.commit_transaction(t2);
    
    // T1 still sees only 5 records (no phantom read)
    let count2 = table.scan_with_snapshot(&snapshot1).len();
    assert_eq!(count2, 5, "T1 should not see new record (phantom read prevention)");
    
    // New T3 sees all 6 records
    let t3 = db.begin_transaction();
    let count3 = table.scan(t3).len();
    assert_eq!(count3, 6);
}

/// Test read-your-own-writes
#[test]
fn test_read_your_own_writes() {
    let db = MvccDatabase::new();
    let table = db.create_table("test".to_string()).unwrap();
    
    let tx = db.begin_transaction();
    
    // Insert
    table.insert(1, create_test_record(1, "Alice"), tx).unwrap();
    
    // Read within same transaction (should see own write)
    let record = table.get(1, tx).unwrap();
    assert_eq!(record.values[1], Value::Text("Alice".to_string()));
    
    db.commit_transaction(tx);
}

/// Measure raw lock-free read throughput
#[test]
fn benchmark_raw_read_throughput() {
    const NUM_RECORDS: usize = 10000;
    const ITERATIONS: usize = 100000;
    
    let mvcc = Arc::new(MvccManager::new());
    let table = Arc::new(LockFreeMvccTable::new("bench".to_string(), mvcc.clone()));
    
    // Populate
    println!("Populating {} records...", NUM_RECORDS);
    for i in 0..NUM_RECORDS {
        let tx = mvcc.begin_transaction();
        table.write(i as u64, create_test_record(i as i64, &format!("User{}", i)), tx);
        mvcc.commit_transaction(tx);
    }
    
    // Benchmark single-threaded reads
    println!("Running {} iterations...", ITERATIONS);
    let tx = mvcc.begin_transaction();
    let snapshot = mvcc.get_snapshot(tx);
    
    let start = Instant::now();
    for i in 0..ITERATIONS {
        let idx = i % NUM_RECORDS;
        let _ = table.read_with_snapshot(idx as u64, &snapshot);
    }
    let elapsed = start.elapsed();
    
    let reads_per_sec = ITERATIONS as f64 / elapsed.as_secs_f64();
    println!("Single-threaded read throughput: {:.0} reads/sec", reads_per_sec);
    println!("Average read latency: {:?}", elapsed / ITERATIONS as u32);
}
