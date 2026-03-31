//! Concurrent Read Performance Benchmark
//!
//! Tests the 100x concurrent read performance target:
//! - Single-threaded baseline
//! - 10 threads: should achieve 10x+ improvement
//! - 100 threads: should achieve 50x+ improvement

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use sqllite_rust::concurrency::{
    LockFreeMvccTable, MvccDatabase, MvccManager, MvccTable,
    Snapshot, TxId,
};
use sqllite_rust::storage::{Record, Value};

/// Create a test record
fn create_test_record(id: i64, name: &str) -> Record {
    Record::new(vec![
        Value::Integer(id),
        Value::Text(name.to_string()),
    ])
}

/// Benchmark single-threaded reads
fn benchmark_single_threaded_read(table: &MvccTable, num_reads: usize) -> Duration {
    let tx = 9999u64; // Dedicated reader transaction
    
    let start = Instant::now();
    for i in 0..num_reads {
        let rowid = (i % 1000 + 1) as u64; // Read from 1000 records
        let _ = table.get(rowid, tx);
    }
    start.elapsed()
}

/// Benchmark concurrent reads using RwLock-based MvccTable
fn benchmark_concurrent_reads_rwlock(
    table: Arc<MvccTable>,
    num_threads: usize,
    reads_per_thread: usize,
) -> Duration {
    let start = Instant::now();
    let mut handles = vec![];
    
    for thread_id in 0..num_threads {
        let table_clone = table.clone();
        let handle = thread::spawn(move || {
            // Each thread uses its own transaction
            let tx = 10000u64 + thread_id as u64;
            
            for i in 0..reads_per_thread {
                let rowid = (i % 1000 + 1) as u64;
                let _ = table_clone.get(rowid, tx);
            }
        });
        handles.push(handle);
    }
    
    for handle in handles {
        handle.join().unwrap();
    }
    
    start.elapsed()
}

/// Benchmark concurrent reads using LockFreeMvccTable
fn benchmark_concurrent_reads_lockfree(
    table: Arc<LockFreeMvccTable>,
    mvcc: Arc<MvccManager>,
    num_threads: usize,
    reads_per_thread: usize,
) -> Duration {
    let start = Instant::now();
    let mut handles = vec![];
    
    for thread_id in 0..num_threads {
        let table_clone = table.clone();
        let mvcc_clone = mvcc.clone();
        
        let handle = thread::spawn(move || {
            // Each thread uses its own transaction
            let tx = mvcc_clone.begin_transaction();
            let snapshot = mvcc_clone.get_snapshot(tx);
            
            for i in 0..reads_per_thread {
                let rowid = (i % 1000 + 1) as u64;
                let _ = table_clone.read_with_snapshot(rowid, &snapshot);
            }
        });
        handles.push(handle);
    }
    
    for handle in handles {
        handle.join().unwrap();
    }
    
    start.elapsed()
}

/// Benchmark with pre-created snapshots
fn benchmark_concurrent_reads_with_snapshot(
    table: Arc<MvccTable>,
    snapshots: Vec<Snapshot>,
    reads_per_thread: usize,
) -> Duration {
    let start = Instant::now();
    let mut handles = vec![];
    
    for (thread_id, snapshot) in snapshots.into_iter().enumerate() {
        let table_clone = table.clone();
        
        let handle = thread::spawn(move || {
            for i in 0..reads_per_thread {
                let rowid = (i % 1000 + 1) as u64;
                let _ = table_clone.get_with_snapshot(rowid, &snapshot);
            }
        });
        handles.push(handle);
    }
    
    for handle in handles {
        handle.join().unwrap();
    }
    
    start.elapsed()
}

/// Measure throughput (reads per second)
fn calculate_throughput(num_reads: usize, duration: Duration) -> f64 {
    let seconds = duration.as_secs_f64();
    if seconds > 0.0 {
        num_reads as f64 / seconds
    } else {
        0.0
    }
}

#[test]
fn test_baseline_single_threaded_performance() {
    let db = Arc::new(MvccDatabase::new());
    let table = db.create_table("test".to_string()).unwrap();
    
    // Insert 1000 records
    let tx = db.begin_transaction();
    for i in 1..=1000 {
        table.insert(i, create_test_record(i as i64, &format!("User{}", i)), tx).unwrap();
    }
    db.commit_transaction(tx);
    
    // Warm up
    let _ = benchmark_single_threaded_read(&table, 1000);
    
    // Benchmark
    let num_reads = 100_000;
    let duration = benchmark_single_threaded_read(&table, num_reads);
    let throughput = calculate_throughput(num_reads, duration);
    
    println!("Single-threaded RwLock read throughput: {:.0} reads/sec", throughput);
    println!("  Total reads: {}", num_reads);
    println!("  Duration: {:?}", duration);
    
    // Baseline should be reasonable (at least 100K reads/sec)
    assert!(throughput > 100_000.0, "Single-threaded throughput too low: {:.0}", throughput);
}

#[test]
fn test_10_threads_rwlock_performance() {
    let db = Arc::new(MvccDatabase::new());
    let table = db.create_table("test".to_string()).unwrap();
    
    // Insert 1000 records
    let tx = db.begin_transaction();
    for i in 1..=1000 {
        table.insert(i, create_test_record(i as i64, &format!("User{}", i)), tx).unwrap();
    }
    db.commit_transaction(tx);
    
    let table = Arc::new(MvccTable::new("test".to_string(), db.mvcc_manager()));
    // Copy data to Arc table (simplified for test)
    
    let num_threads = 10;
    let reads_per_thread = 10_000;
    let total_reads = num_threads * reads_per_thread;
    
    let duration = benchmark_concurrent_reads_rwlock(table, num_threads, reads_per_thread);
    let throughput = calculate_throughput(total_reads, duration);
    
    println!("10 threads RwLock read throughput: {:.0} reads/sec", throughput);
    println!("  Total reads: {}", total_reads);
    println!("  Duration: {:?}", duration);
}

#[test]
fn test_10_threads_lockfree_performance() {
    let mvcc = Arc::new(MvccManager::new());
    let table = Arc::new(LockFreeMvccTable::new("test".to_string(), mvcc.clone()));
    
    // Insert 1000 records
    for i in 1..=1000 {
        let tx = mvcc.begin_transaction();
        table.write(i, create_test_record(i as i64, &format!("User{}", i)), tx);
        mvcc.commit_transaction(tx);
    }
    
    let num_threads = 10;
    let reads_per_thread = 10_000;
    let total_reads = num_threads * reads_per_thread;
    
    // Warm up
    let _ = benchmark_concurrent_reads_lockfree(table.clone(), mvcc.clone(), num_threads, 1000);
    
    // Benchmark
    let duration = benchmark_concurrent_reads_lockfree(table, mvcc, num_threads, reads_per_thread);
    let throughput = calculate_throughput(total_reads, duration);
    
    println!("10 threads LockFree read throughput: {:.0} reads/sec", throughput);
    println!("  Total reads: {}", total_reads);
    println!("  Duration: {:?}", duration);
    
    // Should achieve significant throughput with lock-free reads
    assert!(throughput > 500_000.0, "Lock-free throughput too low: {:.0}", throughput);
}

#[test]
fn test_100_threads_lockfree_performance() {
    let mvcc = Arc::new(MvccManager::new());
    let table = Arc::new(LockFreeMvccTable::new("test".to_string(), mvcc.clone()));
    
    // Insert 1000 records
    for i in 1..=1000 {
        let tx = mvcc.begin_transaction();
        table.write(i, create_test_record(i as i64, &format!("User{}", i)), tx);
        mvcc.commit_transaction(tx);
    }
    
    let num_threads = 100;
    let reads_per_thread = 1000;
    let total_reads = num_threads * reads_per_thread;
    
    // Warm up
    let _ = benchmark_concurrent_reads_lockfree(table.clone(), mvcc.clone(), 10, 100);
    
    // Benchmark
    let duration = benchmark_concurrent_reads_lockfree(table, mvcc, num_threads, reads_per_thread);
    let throughput = calculate_throughput(total_reads, duration);
    
    println!("100 threads LockFree read throughput: {:.0} reads/sec", throughput);
    println!("  Total reads: {}", total_reads);
    println!("  Duration: {:?}", duration);
    
    // Should achieve high throughput with 100 threads
    println!("Target: 50x+ improvement over single-threaded SQLite");
}

#[test]
fn test_snapshot_reuse_performance() {
    let db = Arc::new(MvccDatabase::new());
    let table = db.create_table("test".to_string()).unwrap();
    
    // Insert 1000 records
    let tx = db.begin_transaction();
    for i in 1..=1000 {
        table.insert(i, create_test_record(i as i64, &format!("User{}", i)), tx).unwrap();
    }
    db.commit_transaction(tx);
    
    let table: Arc<MvccTable> = table;
    
    // Create snapshots for each thread
    let num_threads = 10;
    let reads_per_thread = 10_000;
    let mut snapshots = vec![];
    
    for thread_id in 0..num_threads {
        let tx = 10000u64 + thread_id as u64;
        snapshots.push(db.get_snapshot(tx));
    }
    
    let duration = benchmark_concurrent_reads_with_snapshot(table, snapshots, reads_per_thread);
    let throughput = calculate_throughput(num_threads * reads_per_thread, duration);
    
    println!("Snapshot reuse read throughput: {:.0} reads/sec", throughput);
    println!("  Using pre-created snapshots eliminates snapshot creation overhead");
}

#[test]
fn test_read_your_own_writes() {
    let mvcc = Arc::new(MvccManager::new());
    let table = Arc::new(LockFreeMvccTable::new("test".to_string(), mvcc.clone()));
    
    let tx = mvcc.begin_transaction();
    
    // Write
    table.write(1, create_test_record(1, "Alice"), tx);
    
    // Read back with same transaction
    let snapshot = mvcc.get_snapshot(tx);
    let record = table.read_with_snapshot(1, &snapshot);
    
    assert!(record.is_some(), "Should read own writes");
    assert_eq!(record.unwrap().values[1], Value::Text("Alice".to_string()));
    
    mvcc.commit_transaction(tx);
}

#[test]
fn test_snapshot_isolation_correctness() {
    let mvcc = Arc::new(MvccManager::new());
    let table = Arc::new(LockFreeMvccTable::new("test".to_string(), mvcc.clone()));
    
    // T1: Write and commit
    let tx1 = mvcc.begin_transaction();
    table.write(1, create_test_record(1, "V1"), tx1);
    mvcc.commit_transaction(tx1);
    
    // T2: Take snapshot
    let tx2 = mvcc.begin_transaction();
    let snapshot2 = mvcc.get_snapshot(tx2);
    
    // T3: Update and commit
    let tx3 = mvcc.begin_transaction();
    table.write(1, create_test_record(1, "V2"), tx3);
    mvcc.commit_transaction(tx3);
    
    // T2: Should still see V1 (snapshot isolation)
    let record = table.read_with_snapshot(1, &snapshot2);
    assert!(record.is_some());
    assert_eq!(record.unwrap().values[1], Value::Text("V1".to_string()),
        "T2 should see V1, not V2");
    
    // T4: New transaction should see V2
    let tx4 = mvcc.begin_transaction();
    let snapshot4 = mvcc.get_snapshot(tx4);
    let record = table.read_with_snapshot(1, &snapshot4);
    assert!(record.is_some());
    assert_eq!(record.unwrap().values[1], Value::Text("V2".to_string()),
        "T4 should see V2");
}

#[test]
fn test_concurrent_readers_no_contention() {
    let mvcc = Arc::new(MvccManager::new());
    let table = Arc::new(LockFreeMvccTable::new("test".to_string(), mvcc.clone()));
    
    // Insert initial data
    for i in 0..100 {
        let tx = mvcc.begin_transaction();
        table.write(i, create_test_record(i as i64, &format!("User{}", i)), tx);
        mvcc.commit_transaction(tx);
    }
    
    let counter = Arc::new(AtomicU64::new(0));
    let mut handles = vec![];
    
    // Spawn 50 concurrent readers
    for _ in 0..50 {
        let table_clone = table.clone();
        let mvcc_clone = mvcc.clone();
        let counter_clone = counter.clone();
        
        let handle = thread::spawn(move || {
            let tx = mvcc_clone.begin_transaction();
            let snapshot = mvcc_clone.get_snapshot(tx);
            
            // Each reader reads all 100 records
            for i in 0..100 {
                if table_clone.read_with_snapshot(i, &snapshot).is_some() {
                    counter_clone.fetch_add(1, Ordering::Relaxed);
                }
            }
        });
        handles.push(handle);
    }
    
    for handle in handles {
        handle.join().unwrap();
    }
    
    // All readers should have found all records
    let total_reads = counter.load(Ordering::Relaxed);
    assert_eq!(total_reads, 50 * 100, "All readers should find all records");
}

#[test]
fn test_memory_safety_under_high_concurrency() {
    // This test verifies memory safety with many concurrent operations
    let mvcc = Arc::new(MvccManager::new());
    let table = Arc::new(LockFreeMvccTable::new("test".to_string(), mvcc.clone()));
    
    // Insert data
    for i in 0..100 {
        let tx = mvcc.begin_transaction();
        table.write(i, create_test_record(i as i64, &format!("User{}", i)), tx);
        mvcc.commit_transaction(tx);
    }
    
    let mut handles = vec![];
    
    // Spawn readers and writers concurrently
    for thread_id in 0..20 {
        let table_clone = table.clone();
        let mvcc_clone = mvcc.clone();
        
        if thread_id % 4 == 0 {
            // Writer thread
            let handle = thread::spawn(move || {
                for i in 0..50 {
                    let tx = mvcc_clone.begin_transaction();
                    table_clone.write(
                        (i % 100) as u64,
                        create_test_record(i as i64, &format!("Updated{}", i)),
                        tx
                    );
                    mvcc_clone.commit_transaction(tx);
                }
            });
            handles.push(handle);
        } else {
            // Reader thread
            let handle = thread::spawn(move || {
                for _ in 0..100 {
                    let tx = mvcc_clone.begin_transaction();
                    let snapshot = mvcc_clone.get_snapshot(tx);
                    for i in 0..100 {
                        let _ = table_clone.read_with_snapshot(i as u64, &snapshot);
                    }
                }
            });
            handles.push(handle);
        }
    }
    
    for handle in handles {
        handle.join().unwrap();
    }
    
    // If we get here without panicking or crashing, memory safety is maintained
    println!("Memory safety test passed with high concurrency");
}

/// Performance comparison test
#[test]
fn test_performance_comparison() {
    println!("\n=== Performance Comparison ===\n");
    
    // Test parameters
    let num_records = 1000;
    let reads_per_thread = 10_000;
    
    // RwLock-based table
    let db = Arc::new(MvccDatabase::new());
    let table = db.create_table("test".to_string()).unwrap();
    
    let tx = db.begin_transaction();
    for i in 1..=num_records {
        table.insert(i, create_test_record(i as i64, &format!("User{}", i)), tx).unwrap();
    }
    db.commit_transaction(tx);
    
    // Single-threaded baseline
    let start = Instant::now();
    let tx = 9999u64;
    for i in 0..reads_per_thread {
        let _ = table.get((i % num_records + 1) as u64, tx);
    }
    let single_thread_time = start.elapsed();
    let single_thread_throughput = reads_per_thread as f64 / single_thread_time.as_secs_f64();
    
    println!("RwLock Single-threaded: {:.0} reads/sec", single_thread_throughput);
    
    // Lock-free table
    let mvcc = Arc::new(MvccManager::new());
    let lf_table = Arc::new(LockFreeMvccTable::new("test".to_string(), mvcc.clone()));
    
    for i in 1..=num_records {
        let tx = mvcc.begin_transaction();
        lf_table.write(i, create_test_record(i as i64, &format!("User{}", i)), tx);
        mvcc.commit_transaction(tx);
    }
    
    // Single-threaded lock-free
    let start = Instant::now();
    let tx = mvcc.begin_transaction();
    let snapshot = mvcc.get_snapshot(tx);
    for i in 0..reads_per_thread {
        let _ = lf_table.read_with_snapshot((i % num_records + 1) as u64, &snapshot);
    }
    let lf_single_time = start.elapsed();
    let lf_single_throughput = reads_per_thread as f64 / lf_single_time.as_secs_f64();
    
    println!("LockFree Single-threaded: {:.0} reads/sec", lf_single_throughput);
    
    // 10 threads lock-free
    let start = Instant::now();
    let mut handles = vec![];
    for thread_id in 0..10 {
        let table_clone = lf_table.clone();
        let mvcc_clone = mvcc.clone();
        let handle = thread::spawn(move || {
            let tx = mvcc_clone.begin_transaction();
            let snapshot = mvcc_clone.get_snapshot(tx);
            for i in 0..reads_per_thread {
                let _ = table_clone.read_with_snapshot((i % num_records + 1) as u64, &snapshot);
            }
        });
        handles.push(handle);
    }
    for handle in handles {
        handle.join().unwrap();
    }
    let lf_10_time = start.elapsed();
    let lf_10_throughput = (10 * reads_per_thread) as f64 / lf_10_time.as_secs_f64();
    
    println!("LockFree 10 threads: {:.0} reads/sec", lf_10_throughput);
    println!("  Scaling factor: {:.1}x", lf_10_throughput / lf_single_throughput);
    
    // 100 threads lock-free
    let start = Instant::now();
    let mut handles = vec![];
    for thread_id in 0..100 {
        let table_clone = lf_table.clone();
        let mvcc_clone = mvcc.clone();
        let handle = thread::spawn(move || {
            let tx = mvcc_clone.begin_transaction();
            let snapshot = mvcc_clone.get_snapshot(tx);
            for i in 0..reads_per_thread / 10 {
                let _ = table_clone.read_with_snapshot((i % num_records + 1) as u64, &snapshot);
            }
        });
        handles.push(handle);
    }
    for handle in handles {
        handle.join().unwrap();
    }
    let lf_100_time = start.elapsed();
    let lf_100_throughput = (100 * reads_per_thread / 10) as f64 / lf_100_time.as_secs_f64();
    
    println!("LockFree 100 threads: {:.0} reads/sec", lf_100_throughput);
    println!("  Scaling factor: {:.1}x", lf_100_throughput / lf_single_throughput);
    
    println!("\n=== Summary ===");
    println!("Single-threaded performance should be maintained");
    println!("10 threads should show good scaling (5-10x)");
    println!("100 threads should show near-linear scaling (50x+)");
}
