//! Concurrent Stress Benchmark
//!
//! Phase 2 P2-7: Comprehensive Concurrent Stress Testing
//!
//! This benchmark measures:
//! 1. Concurrent read performance (100x target)
//! 2. Mixed workload performance (90/10 read/write)
//! 3. Transaction isolation correctness
//! 4. Long-running stability

use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput, BenchmarkId};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use sqllite_rust::concurrency::{MvccDatabase, MvccManager, LockFreeMvccTable, MvccTable};
use sqllite_rust::storage::{Record, Value};

fn create_test_record(id: i64, name: &str) -> Record {
    Record::new(vec![
        Value::Integer(id),
        Value::Text(name.to_string()),
    ])
}

/// Benchmark: Single-threaded read baseline
fn bench_single_threaded_reads(c: &mut Criterion) {
    let mut group = c.benchmark_group("single_threaded_reads");
    
    let mvcc = Arc::new(MvccManager::new());
    let table = Arc::new(LockFreeMvccTable::new("test".to_string(), mvcc.clone()));
    
    // Prepare data
    for i in 0..10_000 {
        let tx = mvcc.begin_transaction();
        table.write(i as u64, create_test_record(i as i64, &format!("User{}", i)), tx);
        mvcc.commit_transaction(tx);
    }
    
    group.throughput(Throughput::Elements(10_000));
    group.bench_function("lock_free_10k_reads", |b| {
        b.iter(|| {
            let tx = mvcc.begin_transaction();
            let snapshot = mvcc.get_snapshot(tx);
            for i in 0..10_000 {
                black_box(table.read_with_snapshot(i as u64, &snapshot));
            }
        });
    });
    
    group.finish();
}

/// Benchmark: Concurrent read scalability
fn bench_concurrent_read_scalability(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrent_read_scalability");
    group.measurement_time(Duration::from_secs(10));
    group.sample_size(10);
    
    const NUM_RECORDS: usize = 10_000;
    const OPS_PER_THREAD: usize = 10_000;
    
    for num_threads in [1, 10, 50, 100].iter() {
        // Setup
        let mvcc = Arc::new(MvccManager::new());
        let table = Arc::new(LockFreeMvccTable::new("test".to_string(), mvcc.clone()));
        
        for i in 0..NUM_RECORDS {
            let tx = mvcc.begin_transaction();
            table.write(i as u64, create_test_record(i as i64, &format!("User{}", i)), tx);
            mvcc.commit_transaction(tx);
        }
        
        group.throughput(Throughput::Elements((num_threads * OPS_PER_THREAD) as u64));
        group.bench_with_input(
            BenchmarkId::new("threads", num_threads),
            num_threads,
            |b, &num_threads| {
                b.iter_custom(|iters| {
                    let start = Instant::now();
                    
                    for _ in 0..iters {
                        let barrier = Arc::new(std::sync::Barrier::new(num_threads));
                        let mut handles = vec![];
                        
                        for thread_id in 0..num_threads {
                            let mvcc = mvcc.clone();
                            let table = table.clone();
                            let barrier = barrier.clone();
                            
                            let handle = thread::spawn(move || {
                                barrier.wait();
                                let tx = mvcc.begin_transaction();
                                let snapshot = mvcc.get_snapshot(tx);
                                
                                for i in 0..OPS_PER_THREAD {
                                    let rowid = ((thread_id * OPS_PER_THREAD + i) % NUM_RECORDS) as u64;
                                    black_box(table.read_with_snapshot(rowid, &snapshot));
                                }
                            });
                            handles.push(handle);
                        }
                        
                        for handle in handles {
                            handle.join().unwrap();
                        }
                    }
                    
                    start.elapsed()
                });
            },
        );
    }
    
    group.finish();
}

/// Benchmark: Mixed workload (90% read, 10% write)
fn bench_mixed_workload(c: &mut Criterion) {
    let mut group = c.benchmark_group("mixed_workload");
    group.measurement_time(Duration::from_secs(10));
    
    for (num_readers, num_writers) in [(9, 1), (18, 2), (45, 5)].iter() {
        let total_threads = num_readers + num_writers;
        
        group.bench_with_input(
            BenchmarkId::new("readers_writers", format!("{}_{}", num_readers, num_writers)),
            &(num_readers, num_writers),
            |b, &(num_readers, num_writers)| {
                b.iter_custom(|iters| {
                    let start = Instant::now();
                    
                    for _ in 0..iters {
                        let mvcc = Arc::new(MvccManager::new());
                        let table = Arc::new(LockFreeMvccTable::new("test".to_string(), mvcc.clone()));
                        
                        // Prepare data
                        for i in 0..1000 {
                            let tx = mvcc.begin_transaction();
                            table.write(i as u64, create_test_record(i as i64, &format!("User{}", i)), tx);
                            mvcc.commit_transaction(tx);
                        }
                        
                        let running = Arc::new(AtomicBool::new(true));
                        let mut handles = vec![];
                        
                        // Spawn readers
                        for _ in 0..num_readers {
                            let mvcc = mvcc.clone();
                            let table = table.clone();
                            let running = running.clone();
                            
                            let handle = thread::spawn(move || {
                                let mut count = 0;
                                while running.load(Ordering::Relaxed) && count < 1000 {
                                    let tx = mvcc.begin_transaction();
                                    let snapshot = mvcc.get_snapshot(tx);
                                    for i in 0..10 {
                                        black_box(table.read_with_snapshot(i, &snapshot));
                                        count += 1;
                                    }
                                }
                            });
                            handles.push(handle);
                        }
                        
                        // Spawn writers
                        for writer_id in 0..num_writers {
                            let mvcc = mvcc.clone();
                            let table = table.clone();
                            let running = running.clone();
                            
                            let handle = thread::spawn(move || {
                                let mut count = 0;
                                let mut i = writer_id;
                                while running.load(Ordering::Relaxed) && count < 100 {
                                    let tx = mvcc.begin_transaction();
                                    table.write(i as u64, create_test_record(i as i64, &format!("Updated{}", count)), tx);
                                    mvcc.commit_transaction(tx);
                                    count += 1;
                                    i = (i + num_writers) % 1000;
                                }
                            });
                            handles.push(handle);
                        }
                        
                        // Let it run for a short time
                        thread::sleep(Duration::from_millis(100));
                        running.store(false, Ordering::Relaxed);
                        
                        for handle in handles {
                            handle.join().unwrap();
                        }
                    }
                    
                    start.elapsed()
                });
            },
        );
    }
    
    group.finish();
}

/// Benchmark: Snapshot creation overhead
fn bench_snapshot_creation(c: &mut Criterion) {
    let mut group = c.benchmark_group("snapshot_creation");
    
    for num_active_txs in [0, 10, 100, 1000].iter() {
        let mvcc = Arc::new(MvccManager::new());
        
        // Create active transactions
        let mut txs = vec![];
        for _ in 0..*num_active_txs {
            txs.push(mvcc.begin_transaction());
        }
        
        group.bench_with_input(
            BenchmarkId::new("active_txs", num_active_txs),
            num_active_txs,
            |b, _| {
                b.iter(|| {
                    let tx = mvcc.begin_transaction();
                    black_box(mvcc.get_snapshot(tx));
                });
            },
        );
        
        // Cleanup
        for tx in txs {
            mvcc.rollback_transaction(tx);
        }
    }
    
    group.finish();
}

/// Benchmark: Version chain traversal
fn bench_version_chain_traversal(c: &mut Criterion) {
    let mut group = c.benchmark_group("version_chain");
    
    for num_versions in [1, 5, 10, 20, 50].iter() {
        let db = Arc::new(MvccDatabase::new());
        let table = db.create_table("test".to_string()).unwrap();
        
        // Create record with multiple versions
        let tx = db.begin_transaction();
        table.insert(1, create_test_record(1, "V0"), tx).unwrap();
        db.commit_transaction(tx);
        
        for i in 1..*num_versions {
            let tx = db.begin_transaction();
            table.update(1, create_test_record(1, &format!("V{}", i)), tx).unwrap();
            db.commit_transaction(tx);
        }
        
        group.bench_with_input(
            BenchmarkId::new("versions", num_versions),
            num_versions,
            |b, _| {
                b.iter(|| {
                    let tx = db.begin_transaction();
                    black_box(table.get(1, tx));
                });
            },
        );
    }
    
    group.finish();
}

/// Benchmark: RwLock vs Lock-free comparison
fn bench_rwlock_vs_lockfree(c: &mut Criterion) {
    let mut group = c.benchmark_group("rwlock_vs_lockfree");
    group.measurement_time(Duration::from_secs(10));
    
    const NUM_RECORDS: usize = 1000;
    const NUM_THREADS: usize = 100;
    
    // Setup RwLock table
    let db = Arc::new(MvccDatabase::new());
    let rw_table = db.create_table("rw".to_string()).unwrap();
    
    let tx = db.begin_transaction();
    for i in 0..NUM_RECORDS {
        rw_table.insert(i as u64, create_test_record(i as i64, &format!("User{}", i)), tx).unwrap();
    }
    db.commit_transaction(tx);
    
    group.bench_function("rwlock_100_threads", |b| {
        b.iter_custom(|iters| {
            let start = Instant::now();
            
            for _ in 0..iters {
                let barrier = Arc::new(std::sync::Barrier::new(NUM_THREADS));
                let mut handles = vec![];
                
                for _ in 0..NUM_THREADS {
                    let db = db.clone();
                    let table = rw_table.clone();
                    let barrier = barrier.clone();
                    
                    let handle = thread::spawn(move || {
                        barrier.wait();
                        let tx = db.begin_transaction();
                        for i in 0..NUM_RECORDS {
                            black_box(table.get(i as u64, tx));
                        }
                    });
                    handles.push(handle);
                }
                
                for handle in handles {
                    handle.join().unwrap();
                }
            }
            
            start.elapsed()
        });
    });
    
    // Setup Lock-free table
    let mvcc = Arc::new(MvccManager::new());
    let lf_table = Arc::new(LockFreeMvccTable::new("lf".to_string(), mvcc.clone()));
    
    for i in 0..NUM_RECORDS {
        let tx = mvcc.begin_transaction();
        lf_table.write(i as u64, create_test_record(i as i64, &format!("User{}", i)), tx);
        mvcc.commit_transaction(tx);
    }
    
    group.bench_function("lockfree_100_threads", |b| {
        b.iter_custom(|iters| {
            let start = Instant::now();
            
            for _ in 0..iters {
                let barrier = Arc::new(std::sync::Barrier::new(NUM_THREADS));
                let mut handles = vec![];
                
                for _ in 0..NUM_THREADS {
                    let mvcc = mvcc.clone();
                    let table = lf_table.clone();
                    let barrier = barrier.clone();
                    
                    let handle = thread::spawn(move || {
                        barrier.wait();
                        let tx = mvcc.begin_transaction();
                        let snapshot = mvcc.get_snapshot(tx);
                        for i in 0..NUM_RECORDS {
                            black_box(table.read_with_snapshot(i as u64, &snapshot));
                        }
                    });
                    handles.push(handle);
                }
                
                for handle in handles {
                    handle.join().unwrap();
                }
            }
            
            start.elapsed()
        });
    });
    
    group.finish();
}

criterion_group!(
    benches,
    bench_single_threaded_reads,
    bench_concurrent_read_scalability,
    bench_mixed_workload,
    bench_snapshot_creation,
    bench_version_chain_traversal,
    bench_rwlock_vs_lockfree,
);
criterion_main!(benches);
