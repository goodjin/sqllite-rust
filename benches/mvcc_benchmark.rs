//! MVCC Benchmark Suite
//!
//! Benchmarks for Phase 2 MVCC implementation
//! Measures: throughput, latency, scalability

use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput, BenchmarkId};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use sqllite_rust::concurrency::{
    MvccDatabase, MvccManager, MvccTable, LockFreeMvccTable,
};
use sqllite_rust::storage::{Record, Value};

fn create_test_record(id: i64, name: &str) -> Record {
    Record::new(vec![
        Value::Integer(id),
        Value::Text(name.to_string()),
    ])
}

/// Benchmark single-threaded reads
fn bench_single_threaded_reads(c: &mut Criterion) {
    let mut group = c.benchmark_group("single_threaded_reads");
    
    for num_records in [100, 1000, 10000].iter() {
        // Setup RwLock table
        let db = Arc::new(MvccDatabase::new());
        let rw_table = db.create_table("rw".to_string()).unwrap();
        
        let tx = db.begin_transaction();
        for i in 0..*num_records {
            rw_table.insert(i as u64, create_test_record(i as i64, &format!("User{}", i)), tx).unwrap();
        }
        db.commit_transaction(tx);
        
        // Benchmark RwLock reads
        group.bench_with_input(
            BenchmarkId::new("rwlock", num_records),
            num_records,
            |b, _| {
                b.iter(|| {
                    let tx = db.begin_transaction();
                    for i in 0..*num_records {
                        black_box(rw_table.get(i as u64, tx));
                    }
                });
            },
        );
        
        // Setup Lock-free table
        let mvcc = Arc::new(MvccManager::new());
        let lf_table = Arc::new(LockFreeMvccTable::new("lf".to_string(), mvcc.clone()));
        
        for i in 0..*num_records {
            let tx = mvcc.begin_transaction();
            lf_table.write(i as u64, create_test_record(i as i64, &format!("User{}", i)), tx);
            mvcc.commit_transaction(tx);
        }
        
        // Benchmark Lock-free reads
        group.bench_with_input(
            BenchmarkId::new("lock_free", num_records),
            num_records,
            |b, _| {
                b.iter(|| {
                    let tx = mvcc.begin_transaction();
                    let snapshot = mvcc.get_snapshot(tx);
                    for i in 0..*num_records {
                        black_box(lf_table.read_with_snapshot(i as u64, &snapshot));
                    }
                });
            },
        );
    }
    
    group.finish();
}

/// Benchmark concurrent reads scalability
fn bench_concurrent_read_scalability(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrent_reads");
    group.measurement_time(Duration::from_secs(5));
    
    const NUM_RECORDS: usize = 1000;
    
    for num_threads in [1, 2, 4, 8, 16, 32].iter() {
        // Setup RwLock table
        let db = Arc::new(MvccDatabase::new());
        let rw_table = db.create_table("rw".to_string()).unwrap();
        
        let tx = db.begin_transaction();
        for i in 0..NUM_RECORDS {
            rw_table.insert(i as u64, create_test_record(i as i64, &format!("User{}", i)), tx).unwrap();
        }
        db.commit_transaction(tx);
        
        group.bench_with_input(
            BenchmarkId::new("rwlock", num_threads),
            num_threads,
            |b, num_threads| {
                b.iter(|| {
                    let mut handles = vec![];
                    
                    for _ in 0..*num_threads {
                        let db = db.clone();
                        let table = rw_table.clone();
                        
                        let handle = thread::spawn(move || {
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
                });
            },
        );
        
        // Setup Lock-free table
        let mvcc = Arc::new(MvccManager::new());
        let lf_table = Arc::new(LockFreeMvccTable::new("lf".to_string(), mvcc.clone()));
        
        for i in 0..NUM_RECORDS {
            let tx = mvcc.begin_transaction();
            lf_table.write(i as u64, create_test_record(i as i64, &format!("User{}", i)), tx);
            mvcc.commit_transaction(tx);
        }
        
        group.bench_with_input(
            BenchmarkId::new("lock_free", num_threads),
            num_threads,
            |b, num_threads| {
                b.iter(|| {
                    let mut handles = vec![];
                    
                    for _ in 0..*num_threads {
                        let mvcc = mvcc.clone();
                        let table = lf_table.clone();
                        
                        let handle = thread::spawn(move || {
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
                });
            },
        );
    }
    
    group.finish();
}

/// Benchmark snapshot creation
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
            BenchmarkId::new("create", num_active_txs),
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

/// Benchmark version chain traversal
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
            BenchmarkId::new("traverse", num_versions),
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

/// Benchmark mixed read/write workload
fn bench_mixed_workload(c: &mut Criterion) {
    let mut group = c.benchmark_group("mixed_workload");
    group.measurement_time(Duration::from_secs(5));
    
    for read_ratio in [0.9, 0.95, 0.99].iter() {
        let db = Arc::new(MvccDatabase::new());
        let table = db.create_table("test".to_string()).unwrap();
        
        // Pre-populate
        let tx = db.begin_transaction();
        for i in 0..1000 {
            table.insert(i as u64, create_test_record(i as i64, &format!("User{}", i)), tx).unwrap();
        }
        db.commit_transaction(tx);
        
        group.bench_with_input(
            BenchmarkId::new("rwlock", read_ratio),
            read_ratio,
            |b, ratio| {
                b.iter(|| {
                    let tx = db.begin_transaction();
                    for i in 0..100 {
                        if i as f64 / 100.0 < *ratio {
                            // Read
                            black_box(table.get(i as u64, tx));
                        } else {
                            // Write
                            let _ = table.update(i as u64, create_test_record(i as i64, "Updated"), tx);
                        }
                    }
                });
            },
        );
    }
    
    group.finish();
}

/// Benchmark throughput comparison
fn bench_throughput_comparison(c: &mut Criterion) {
    let mut group = c.benchmark_group("throughput");
    group.throughput(Throughput::Elements(1000));
    
    // RwLock table
    let db = Arc::new(MvccDatabase::new());
    let rw_table = db.create_table("rw".to_string()).unwrap();
    
    let tx = db.begin_transaction();
    for i in 0..1000 {
        rw_table.insert(i as u64, create_test_record(i as i64, &format!("User{}", i)), tx).unwrap();
    }
    db.commit_transaction(tx);
    
    group.bench_function("rwlock_10threads", |b| {
        b.iter(|| {
            let mut handles = vec![];
            
            for _ in 0..10 {
                let db = db.clone();
                let table = rw_table.clone();
                
                let handle = thread::spawn(move || {
                    let tx = db.begin_transaction();
                    for i in 0..1000 {
                        black_box(table.get(i as u64, tx));
                    }
                });
                
                handles.push(handle);
            }
            
            for handle in handles {
                handle.join().unwrap();
            }
        });
    });
    
    // Lock-free table
    let mvcc = Arc::new(MvccManager::new());
    let lf_table = Arc::new(LockFreeMvccTable::new("lf".to_string(), mvcc.clone()));
    
    for i in 0..1000 {
        let tx = mvcc.begin_transaction();
        lf_table.write(i as u64, create_test_record(i as i64, &format!("User{}", i)), tx);
        mvcc.commit_transaction(tx);
    }
    
    group.bench_function("lock_free_10threads", |b| {
        b.iter(|| {
            let mut handles = vec![];
            
            for _ in 0..10 {
                let mvcc = mvcc.clone();
                let table = lf_table.clone();
                
                let handle = thread::spawn(move || {
                    let tx = mvcc.begin_transaction();
                    let snapshot = mvcc.get_snapshot(tx);
                    for i in 0..1000 {
                        black_box(table.read_with_snapshot(i as u64, &snapshot));
                    }
                });
                
                handles.push(handle);
            }
            
            for handle in handles {
                handle.join().unwrap();
            }
        });
    });
    
    group.finish();
}

criterion_group!(
    benches,
    bench_single_threaded_reads,
    bench_concurrent_read_scalability,
    bench_snapshot_creation,
    bench_version_chain_traversal,
    bench_mixed_workload,
    bench_throughput_comparison,
);
criterion_main!(benches);
