use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};
use sqllite_rust::transaction::{TransactionManager, TransactionConfig};
use sqllite_rust::pager::Page;

/// 基准测试：WAL 批量提交性能
/// 
/// 对比：
/// - 传统模式：每次事务单独 fsync
/// - 批量模式：多个事务共享一次 fsync (Group Commit)
fn benchmark_wal_batch_commit(c: &mut Criterion) {
    let mut group = c.benchmark_group("wal_batch_commit");

    // 测试不同批量大小
    for batch_size in [1, 10, 50, 100].iter() {
        // 传统模式：group_commit = false（每次单独 flush）
        group.bench_with_input(
            BenchmarkId::new("individual_commit", batch_size),
            batch_size,
            |b, &size| {
                b.iter_with_setup(|| {
                    let temp_file = tempfile::NamedTempFile::new().unwrap();
                    let path = temp_file.path().to_str().unwrap().to_string() + ".db";
                    
                    let config = TransactionConfig {
                        group_commit: false,
                        group_commit_timeout_ms: 0,
                        max_pending_transactions: 1,
                        async_commit: false,
                        use_async_wal: false,
                        wal_batch_size: 1,
                        wal_flush_timeout_ms: 0,
                    };
                    let tm = TransactionManager::with_config(&path, 4096, config).unwrap();
                    (tm, path)
                }, |(mut tm, _path)| {
                    for _ in 0..size {
                        tm.begin().unwrap();
                        // Simulate some writes
                        let page = Page::from_bytes(1, vec![0u8; 4096]);
                        tm.write_page(&page).unwrap();
                        tm.commit().unwrap();
                    }
                    black_box(&tm);
                });
            }
        );

        // 批量模式：group_commit = true
        group.bench_with_input(
            BenchmarkId::new("batch_commit", batch_size),
            batch_size,
            |b, &size| {
                b.iter_with_setup(|| {
                    let temp_file = tempfile::NamedTempFile::new().unwrap();
                    let path = temp_file.path().to_str().unwrap().to_string() + ".db";
                    
                    let config = TransactionConfig {
                        group_commit: true,
                        group_commit_timeout_ms: 10000, // Long timeout, manual flush
                        max_pending_transactions: size,
                        async_commit: false,
                        use_async_wal: true,
                        wal_batch_size: size,
                        wal_flush_timeout_ms: 100,
                    };
                    let tm = TransactionManager::with_config(&path, 4096, config).unwrap();
                    (tm, path)
                }, |(mut tm, _path)| {
                    for _ in 0..size {
                        tm.begin().unwrap();
                        let page = Page::from_bytes(1, vec![0u8; 4096]);
                        tm.write_page(&page).unwrap();
                        tm.commit().unwrap();
                    }
                    // Force final flush (simulates timeout or next batch)
                    tm.flush_batch().unwrap();
                    black_box(&tm);
                });
            }
        );
    }

    group.finish();
}

/// 基准测试：批量写入吞吐量
fn benchmark_wal_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("wal_throughput");
    
    // 测量每秒能处理多少事务（批量模式）
    group.bench_function("tx_per_sec_batch", |b| {
        b.iter_with_setup(|| {
            let temp_file = tempfile::NamedTempFile::new().unwrap();
            let path = temp_file.path().to_str().unwrap().to_string() + ".db";
            
            let config = TransactionConfig {
                group_commit: true,
                group_commit_timeout_ms: 100, // 100ms window
                max_pending_transactions: 1000,
                async_commit: false,
                use_async_wal: true,
                wal_batch_size: 100,
                wal_flush_timeout_ms: 100,
            };
            TransactionManager::with_config(&path, 4096, config).unwrap()
        }, |mut tm| {
            // Simulate 100 transactions
            for i in 0..100 {
                tm.begin().unwrap();
                let page = Page::from_bytes((i % 10 + 1) as u32, vec![0u8; 4096]);
                tm.write_page(&page).unwrap();
                tm.commit().unwrap();
            }
            // Ensure all flushed
            tm.flush_batch().unwrap();
            black_box(&tm);
        });
    });

    // 测量每秒能处理多少事务（传统模式）
    group.bench_function("tx_per_sec_individual", |b| {
        b.iter_with_setup(|| {
            let temp_file = tempfile::NamedTempFile::new().unwrap();
            let path = temp_file.path().to_str().unwrap().to_string() + ".db";
            
            let config = TransactionConfig {
                group_commit: false,
                group_commit_timeout_ms: 0,
                max_pending_transactions: 1,
                async_commit: false,
                use_async_wal: false,
                wal_batch_size: 1,
                wal_flush_timeout_ms: 0,
            };
            TransactionManager::with_config(&path, 4096, config).unwrap()
        }, |mut tm| {
            for i in 0..10 { // Fewer transactions due to fsync overhead
                tm.begin().unwrap();
                let page = Page::from_bytes((i % 10 + 1) as u32, vec![0u8; 4096]);
                tm.write_page(&page).unwrap();
                tm.commit().unwrap();
            }
            black_box(&tm);
        });
    });

    group.finish();
}

/// 基准测试：不同配置下的延迟
fn benchmark_wal_latency(c: &mut Criterion) {
    let mut group = c.benchmark_group("wal_latency");
    
    // 低延迟模式（小 batch，快速 flush）
    group.bench_function("low_latency", |b| {
        b.iter_with_setup(|| {
            let temp_file = tempfile::NamedTempFile::new().unwrap();
            let path = temp_file.path().to_str().unwrap().to_string() + ".db";
            
            let config = TransactionConfig {
                group_commit: true,
                group_commit_timeout_ms: 1, // 1ms
                max_pending_transactions: 10,
                async_commit: false,
                use_async_wal: true,
                wal_batch_size: 10,
                wal_flush_timeout_ms: 10,
            };
            TransactionManager::with_config(&path, 4096, config).unwrap()
        }, |mut tm| {
            tm.begin().unwrap();
            let page = Page::from_bytes(1, vec![0u8; 4096]);
            tm.write_page(&page).unwrap();
            tm.commit().unwrap();
            black_box(&tm);
        });
    });

    // 高吞吐模式（大 batch，延迟容忍）
    group.bench_function("high_throughput", |b| {
        b.iter_with_setup(|| {
            let temp_file = tempfile::NamedTempFile::new().unwrap();
            let path = temp_file.path().to_str().unwrap().to_string() + ".db";
            
            let config = TransactionConfig {
                group_commit: true,
                group_commit_timeout_ms: 100, // 100ms
                max_pending_transactions: 1000,
                async_commit: false,
                use_async_wal: true,
                wal_batch_size: 100,
                wal_flush_timeout_ms: 100,
            };
            TransactionManager::with_config(&path, 4096, config).unwrap()
        }, |mut tm| {
            tm.begin().unwrap();
            let page = Page::from_bytes(1, vec![0u8; 4096]);
            tm.write_page(&page).unwrap();
            tm.commit().unwrap();
            black_box(&tm);
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    benchmark_wal_batch_commit,
    benchmark_wal_throughput,
    benchmark_wal_latency
);
criterion_main!(benches);
