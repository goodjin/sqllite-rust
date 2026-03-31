//! MVCC Comprehensive Stress Test
//!
//! 综合压力测试，覆盖所有MVCC功能

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};
use std::collections::HashMap;

use sqllite_rust::concurrency::{MvccDatabase, MvccManager, LockFreeMvccTable, MvccTable};
use sqllite_rust::storage::{Record, Value};

fn create_test_record(id: i64, name: &str) -> Record {
    Record::new(vec![
        Value::Integer(id),
        Value::Text(name.to_string()),
    ])
}

fn create_full_record(id: i64, name: &str, email: &str, age: i64) -> Record {
    Record::new(vec![
        Value::Integer(id),
        Value::Text(name.to_string()),
        Value::Text(email.to_string()),
        Value::Integer(age),
    ])
}

/// 综合压力测试结果
#[derive(Debug, Clone)]
pub struct StressTestResult {
    pub name: String,
    pub duration: Duration,
    pub total_ops: u64,
    pub throughput: f64,
    pub success: bool,
    pub error_message: Option<String>,
}

/// 测试1：极端并发读取
#[test]
fn stress_test_extreme_concurrent_reads() {
    println!("\n");
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║     Stress Test 1: Extreme Concurrent Reads (200 threads)   ║");
    println!("╚══════════════════════════════════════════════════════════════╝");

    let mvcc = Arc::new(MvccManager::new());
    let table = Arc::new(LockFreeMvccTable::new("users".to_string(), mvcc.clone()));

    // 准备数据
    const NUM_RECORDS: usize = 10_000;
    println!("Preparing {} records...", NUM_RECORDS);
    for i in 0..NUM_RECORDS {
        let tx = mvcc.begin_transaction();
        table.write(i as u64, create_test_record(i as i64, &format!("User{}", i)), tx);
        mvcc.commit_transaction(tx);
    }

    // 200 个读取线程
    const NUM_THREADS: usize = 200;
    const OPS_PER_THREAD: usize = 5000;

    let barrier = Arc::new(std::sync::Barrier::new(NUM_THREADS));
    let total_ops = Arc::new(AtomicU64::new(0));

    let start = Instant::now();
    let mut handles = vec![];

    for thread_id in 0..NUM_THREADS {
        let mvcc = mvcc.clone();
        let table = table.clone();
        let barrier = barrier.clone();
        let total_ops = total_ops.clone();

        let handle = thread::spawn(move || {
            barrier.wait();
            let tx = mvcc.begin_transaction();
            let snapshot = mvcc.get_snapshot(tx);

            for i in 0..OPS_PER_THREAD {
                let rowid = ((thread_id * OPS_PER_THREAD + i) % NUM_RECORDS) as u64;
                let record = table.read_with_snapshot(rowid, &snapshot);
                assert!(record.is_some(), "Thread {} should find record {}", thread_id, rowid);
            }

            total_ops.fetch_add(OPS_PER_THREAD as u64, Ordering::Relaxed);
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    let elapsed = start.elapsed();
    let total = total_ops.load(Ordering::Relaxed);
    let throughput = total as f64 / elapsed.as_secs_f64();

    println!("\n=== Results ===");
    println!("Total Ops: {}", total);
    println!("Duration: {:?}", elapsed);
    println!("Throughput: {:.0} ops/s", throughput);

    // 200线程应该达到合理的吞吐量（硬件依赖）
    println!("  Note: 200-thread throughput: {:.0} ops/s (hardware dependent)", throughput);
    assert!(
        throughput >= 100_000.0,
        "200-thread throughput should be reasonable, got {:.0}",
        throughput
    );
}

/// 测试2：读写交替风暴
#[test]
fn stress_test_read_write_storm() {
    println!("\n");
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║     Stress Test 2: Read-Write Storm                         ║");
    println!("╚══════════════════════════════════════════════════════════════╝");

    let mvcc = Arc::new(MvccManager::new());
    let table = Arc::new(LockFreeMvccTable::new("storm".to_string(), mvcc.clone()));

    // 准备数据
    for i in 0..100 {
        let tx = mvcc.begin_transaction();
        table.write(i as u64, create_test_record(i as i64, &format!("Initial{}", i)), tx);
        mvcc.commit_transaction(tx);
    }

    let running = Arc::new(AtomicBool::new(true));
    let read_count = Arc::new(AtomicU64::new(0));
    let write_count = Arc::new(AtomicU64::new(0));

    let mut handles = vec![];

    // 50 个读取线程
    for _ in 0..50 {
        let mvcc = mvcc.clone();
        let table = table.clone();
        let running = running.clone();
        let read_count = read_count.clone();

        let handle = thread::spawn(move || {
            let mut local = 0u64;
            while running.load(Ordering::Relaxed) {
                let tx = mvcc.begin_transaction();
                let snapshot = mvcc.get_snapshot(tx);
                for i in 0..100 {
                    let _ = table.read_with_snapshot(i, &snapshot);
                    local += 1;
                }
            }
            read_count.fetch_add(local, Ordering::Relaxed);
        });
        handles.push(handle);
    }

    // 50 个写入线程（高争用）
    for writer_id in 0..50 {
        let mvcc = mvcc.clone();
        let table = table.clone();
        let running = running.clone();
        let write_count = write_count.clone();

        let handle = thread::spawn(move || {
            let mut local = 0u64;
            let mut i = writer_id;
            while running.load(Ordering::Relaxed) {
                let tx = mvcc.begin_transaction();
                let record = create_test_record(i as i64, &format!("Updated-by-{}", writer_id));
                table.write(i as u64, record, tx);
                mvcc.commit_transaction(tx);
                local += 1;
                i = (i + 50) % 100;
            }
            write_count.fetch_add(local, Ordering::Relaxed);
        });
        handles.push(handle);
    }

    // 运行 3 秒
    thread::sleep(Duration::from_secs(3));
    running.store(false, Ordering::Relaxed);

    for handle in handles {
        handle.join().unwrap();
    }

    let total_reads = read_count.load(Ordering::Relaxed);
    let total_writes = write_count.load(Ordering::Relaxed);

    println!("\n=== Results ===");
    println!("Total Reads: {} ({:.0} ops/s)", total_reads, total_reads as f64 / 3.0);
    println!("Total Writes: {} ({:.0} ops/s)", total_writes, total_writes as f64 / 3.0);
    println!("Total Ops: {}", total_reads + total_writes);

    // 验证数据一致性
    let tx = mvcc.begin_transaction();
    let snapshot = mvcc.get_snapshot(tx);
    let mut found_count = 0;
    for i in 0..100 {
        if table.read_with_snapshot(i as u64, &snapshot).is_some() {
            found_count += 1;
        }
    }
    println!("Records found: {}/100", found_count);
    assert_eq!(found_count, 100, "All records should be accessible");
}

/// 测试3：事务爆发
#[test]
fn stress_test_transaction_burst() {
    println!("\n");
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║     Stress Test 3: Transaction Burst                        ║");
    println!("╚══════════════════════════════════════════════════════════════╝");

    let db = Arc::new(MvccDatabase::new());
    let table = db.create_table("burst".to_string()).unwrap();

    const NUM_THREADS: usize = 100;
    const TX_PER_THREAD: usize = 100;

    let barrier = Arc::new(std::sync::Barrier::new(NUM_THREADS));
    let success_count = Arc::new(AtomicU64::new(0));

    let start = Instant::now();
    let mut handles = vec![];

    for thread_id in 0..NUM_THREADS {
        let db = db.clone();
        let table = table.clone();
        let barrier = barrier.clone();
        let success_count = success_count.clone();

        let handle = thread::spawn(move || {
            barrier.wait();

            for i in 0..TX_PER_THREAD {
                let tx = db.begin_transaction();
                let rowid = (thread_id * TX_PER_THREAD + i) as u64;
                
                match table.insert(rowid, create_test_record(rowid as i64, &format!("T{}", thread_id)), tx) {
                    Ok(_) => {
                        db.commit_transaction(tx);
                        success_count.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(_) => {
                        db.rollback_transaction(tx);
                    }
                }
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    let elapsed = start.elapsed();
    let success = success_count.load(Ordering::Relaxed);
    let throughput = success as f64 / elapsed.as_secs_f64();

    println!("\n=== Results ===");
    println!("Successful Transactions: {}/{}", success, NUM_THREADS * TX_PER_THREAD);
    println!("Duration: {:?}", elapsed);
    println!("Throughput: {:.0} tx/s", throughput);

    assert_eq!(success, (NUM_THREADS * TX_PER_THREAD) as u64, "All transactions should succeed");
}

/// 测试4：热点数据竞争
#[test]
fn stress_test_hotspot_contention() {
    println!("\n");
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║     Stress Test 4: Hotspot Contention                       ║");
    println!("╚══════════════════════════════════════════════════════════════╝");

    let db = Arc::new(MvccDatabase::new());
    let table = db.create_table("hotspot".to_string()).unwrap();

    // 初始化一个热点记录
    let tx = db.begin_transaction();
    table.insert(1, create_test_record(1, "Hotspot-Initial"), tx).unwrap();
    db.commit_transaction(tx);

    const NUM_THREADS: usize = 100;
    const OPS_PER_THREAD: usize = 100;

    let barrier = Arc::new(std::sync::Barrier::new(NUM_THREADS));
    let update_count = Arc::new(AtomicU64::new(0));

    let start = Instant::now();
    let mut handles = vec![];

    for thread_id in 0..NUM_THREADS {
        let db = db.clone();
        let table = table.clone();
        let barrier = barrier.clone();
        let update_count = update_count.clone();

        let handle = thread::spawn(move || {
            barrier.wait();

            for i in 0..OPS_PER_THREAD {
                let tx = db.begin_transaction();
                let new_value = format!("Hotspot-T{}-Op{}", thread_id, i);
                
                if table.update(1, create_test_record(1, &new_value), tx).is_ok() {
                    db.commit_transaction(tx);
                    update_count.fetch_add(1, Ordering::Relaxed);
                } else {
                    db.rollback_transaction(tx);
                }
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    let elapsed = start.elapsed();
    let updates = update_count.load(Ordering::Relaxed);
    let throughput = updates as f64 / elapsed.as_secs_f64();

    println!("\n=== Results ===");
    println!("Successful Updates: {}/{}", updates, NUM_THREADS * OPS_PER_THREAD);
    println!("Duration: {:?}", elapsed);
    println!("Update Throughput: {:.0} ops/s", throughput);

    // 最终验证
    let tx = db.begin_transaction();
    let final_record = table.get(1, tx).unwrap();
    println!("Final value: {:?}", final_record.values[1]);

    // 版本链应该有很多版本
    let stats = table.stats();
    println!("Total versions: {}", stats.total_versions);
    assert!(stats.total_versions >= 100, "Should have at least 100 versions");
}

/// 测试5：范围扫描压力测试
#[test]
fn stress_test_range_scan() {
    println!("\n");
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║     Stress Test 5: Range Scan Pressure                      ║");
    println!("╚══════════════════════════════════════════════════════════════╝");

    let db = Arc::new(MvccDatabase::new());
    let table = db.create_table("range".to_string()).unwrap();

    // 准备大量数据
    const NUM_RECORDS: usize = 50_000;
    println!("Preparing {} records...", NUM_RECORDS);
    
    for i in 0..NUM_RECORDS {
        let tx = db.begin_transaction();
        table.insert(i as u64, create_full_record(i as i64, &format!("User{}", i), &format!("user{}@test.com", i), (i % 100) as i64), tx).unwrap();
        db.commit_transaction(tx);
    }
    println!("Data preparation complete.");

    const NUM_SCANNERS: usize = 20;
    const SCANS_PER_THREAD: usize = 100;

    let barrier = Arc::new(std::sync::Barrier::new(NUM_SCANNERS));
    let total_scanned = Arc::new(AtomicU64::new(0));

    let start = Instant::now();
    let mut handles = vec![];

    for scanner_id in 0..NUM_SCANNERS {
        let db = db.clone();
        let table = table.clone();
        let barrier = barrier.clone();
        let total_scanned = total_scanned.clone();

        let handle = thread::spawn(move || {
            barrier.wait();

            for _ in 0..SCANS_PER_THREAD {
                let tx = db.begin_transaction();
                let results = table.scan(tx);
                total_scanned.fetch_add(results.len() as u64, Ordering::Relaxed);
                assert_eq!(results.len(), NUM_RECORDS, "Scan should return all records");
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    let elapsed = start.elapsed();
    let scanned = total_scanned.load(Ordering::Relaxed);
    let scan_throughput = (NUM_SCANNERS * SCANS_PER_THREAD) as f64 / elapsed.as_secs_f64();

    println!("\n=== Results ===");
    println!("Total Records Scanned: {}", scanned);
    println!("Duration: {:?}", elapsed);
    println!("Scan Throughput: {:.0} scans/s", scan_throughput);
}

/// 测试6：混合负载压力测试
#[test]
fn stress_test_mixed_workload() {
    println!("\n");
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║     Stress Test 6: Mixed Workload (The Ultimate Test)       ║");
    println!("╚══════════════════════════════════════════════════════════════╝");

    let db = Arc::new(MvccDatabase::new());
    
    // 创建多个表
    let users_table = db.create_table("users".to_string()).unwrap();
    let orders_table = db.create_table("orders".to_string()).unwrap();
    let products_table = db.create_table("products".to_string()).unwrap();

    // 初始化数据
    println!("Initializing data...");
    for i in 0..1000 {
        let tx = db.begin_transaction();
        users_table.insert(i as u64, create_full_record(i as i64, &format!("User{}", i), &format!("user{}@test.com", i), 25), tx).unwrap();
        db.commit_transaction(tx);
    }
    for i in 0..5000 {
        let tx = db.begin_transaction();
        orders_table.insert(i as u64, create_full_record(i as i64, &format!("Order{}", i), &format!("user{}@test.com", i % 1000), (i % 1000) as i64), tx).unwrap();
        db.commit_transaction(tx);
    }
    for i in 0..100 {
        let tx = db.begin_transaction();
        products_table.insert(i as u64, create_full_record(i as i64, &format!("Product{}", i), &format!("product{}@shop.com", i), (i * 10) as i64), tx).unwrap();
        db.commit_transaction(tx);
    }

    let running = Arc::new(AtomicBool::new(true));
    let stats = Arc::new(parking_lot::Mutex::new(HashMap::new()));

    let mut handles = vec![];

    // 用户表读取线程
    for _ in 0..20 {
        let db = db.clone();
        let table = users_table.clone();
        let running = running.clone();
        let stats = stats.clone();

        let handle = thread::spawn(move || {
            let mut count = 0u64;
            while running.load(Ordering::Relaxed) {
                let tx = db.begin_transaction();
                let snapshot = db.get_snapshot(tx);
                for i in 0..100 {
                    let _ = table.get_with_snapshot(i, &snapshot);
                    count += 1;
                }
            }
            stats.lock().insert(format!("user_read_{:?}", thread::current().id()), count);
        });
        handles.push(handle);
    }

    // 订单表扫描线程
    for _ in 0..10 {
        let db = db.clone();
        let table = orders_table.clone();
        let running = running.clone();
        let stats = stats.clone();

        let handle = thread::spawn(move || {
            let mut count = 0u64;
            while running.load(Ordering::Relaxed) {
                let tx = db.begin_transaction();
                let results = table.scan(tx);
                count += results.len() as u64;
            }
            stats.lock().insert(format!("order_scan_{:?}", thread::current().id()), count);
        });
        handles.push(handle);
    }

    // 产品表更新线程
    for writer_id in 0..5 {
        let db = db.clone();
        let table = products_table.clone();
        let running = running.clone();
        let stats = stats.clone();

        let handle = thread::spawn(move || {
            let mut count = 0u64;
            let mut i = writer_id;
            while running.load(Ordering::Relaxed) {
                let tx = db.begin_transaction();
                let record = create_full_record(i as i64, &format!("Product{}", i), &format!("updated{}@shop.com", count), (i * 10 + count as i64) as i64);
                if table.update(i as u64, record, tx).is_ok() {
                    db.commit_transaction(tx);
                    count += 1;
                } else {
                    db.rollback_transaction(tx);
                }
                i = (i + 5) % 100;
            }
            stats.lock().insert(format!("product_write_{}", writer_id), count);
        });
        handles.push(handle);
    }

    // 新订单插入线程
    for writer_id in 0..5 {
        let db = db.clone();
        let table = orders_table.clone();
        let running = running.clone();
        let stats = stats.clone();

        let handle = thread::spawn(move || {
            let mut count = 0u64;
            let mut next_id = 5000 + writer_id as u64;
            while running.load(Ordering::Relaxed) {
                let tx = db.begin_transaction();
                let record = create_full_record(next_id as i64, &format!("NewOrder{}", next_id), &format!("user{}@test.com", next_id % 1000), (next_id % 1000) as i64);
                if table.insert(next_id, record, tx).is_ok() {
                    db.commit_transaction(tx);
                    count += 1;
                    next_id += 5;
                } else {
                    db.rollback_transaction(tx);
                }
            }
            stats.lock().insert(format!("order_insert_{}", writer_id), count);
        });
        handles.push(handle);
    }

    // 运行测试
    println!("Running mixed workload for 5 seconds...");
    thread::sleep(Duration::from_secs(5));
    running.store(false, Ordering::Relaxed);

    for handle in handles {
        handle.join().unwrap();
    }

    // 统计结果
    let final_stats = stats.lock();
    let total_ops: u64 = final_stats.values().sum();

    println!("\n=== Mixed Workload Results ===");
    println!("Total Operations: {}", total_ops);
    println!("Throughput: {:.0} ops/s", total_ops as f64 / 5.0);

    for (key, value) in final_stats.iter() {
        if key.starts_with("user_read") {
            println!("  User Reads: {}", value);
        } else if key.starts_with("order_scan") {
            println!("  Order Scans: {}", value);
        } else if key.starts_with("product_write") {
            println!("  Product Writes: {}", value);
        } else if key.starts_with("order_insert") {
            println!("  Order Inserts: {}", value);
        }
    }

    // 最终一致性检查
    println!("\nPerforming final consistency check...");
    let tx = db.begin_transaction();
    
    let user_count = users_table.scan(tx).len();
    let order_count = orders_table.scan(tx).len();
    let product_count = products_table.scan(tx).len();

    println!("Final counts: Users={}, Orders={}, Products={}", user_count, order_count, product_count);

    assert_eq!(user_count, 1000, "Users table should have 1000 records");
    assert!(order_count >= 5000, "Orders table should have at least 5000 records");
    assert_eq!(product_count, 100, "Products table should have 100 records");

    println!("✓ All consistency checks passed!");
}

/// 测试7：GC压力测试
#[test]
fn stress_test_gc_pressure() {
    println!("\n");
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║     Stress Test 7: GC Pressure                              ║");
    println!("╚══════════════════════════════════════════════════════════════╝");

    let db = Arc::new(MvccDatabase::new());
    let table = db.create_table("gc_test".to_string()).unwrap();

    // 准备初始数据
    for i in 0..100 {
        let tx = db.begin_transaction();
        table.insert(i as u64, create_test_record(i as i64, "V0"), tx).unwrap();
        db.commit_transaction(tx);
    }

    println!("Creating many versions...");
    const VERSIONS_PER_RECORD: usize = 1000;

    for version in 1..=VERSIONS_PER_RECORD {
        for i in 0..100 {
            let tx = db.begin_transaction();
            table.update(i as u64, create_test_record(i as i64, &format!("V{}", version)), tx).unwrap();
            db.commit_transaction(tx);
        }

        if version % 100 == 0 {
            println!("  Created {} versions per record", version);
        }
    }

    let stats_before = table.stats();
    println!("\nVersions before GC: {}", stats_before.total_versions);
    // 版本数应该很多（至少有一些版本）
    assert!(stats_before.total_versions >= 100, "Should have many versions before GC");

    // 运行 GC
    println!("Running GC...");
    let removed = db.gc();
    println!("GC removed {} versions", removed);

    let stats_after = table.stats();
    println!("Versions after GC: {}", stats_after.total_versions);

    // 应该至少保留最新版本
    assert!(stats_after.total_versions <= 100, "Should have at most 1 version per record after GC");

    // 验证数据完整性
    let tx = db.begin_transaction();
    for i in 0..100 {
        let record = table.get(i as u64, tx).unwrap();
        let name = match &record.values[1] {
            Value::Text(s) => s.clone(),
            _ => panic!("Expected text"),
        };
        assert_eq!(name, format!("V{}", VERSIONS_PER_RECORD), "Should see latest version");
    }
    println!("✓ All records verified after GC");
}

/// 测试8：长事务与短事务混合
#[test]
fn stress_test_long_short_transactions() {
    println!("\n");
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║     Stress Test 8: Long & Short Transaction Mix             ║");
    println!("╚══════════════════════════════════════════════════════════════╝");

    let db = Arc::new(MvccDatabase::new());
    let table = db.create_table("mixed".to_string()).unwrap();

    // 准备数据
    for i in 0..100 {
        let tx = db.begin_transaction();
        table.insert(i as u64, create_test_record(i as i64, &format!("Initial{}", i)), tx).unwrap();
        db.commit_transaction(tx);
    }

    let running = Arc::new(AtomicBool::new(true));
    let long_tx_count = Arc::new(AtomicU64::new(0));
    let short_tx_count = Arc::new(AtomicU64::new(0));

    let mut handles = vec![];

    // 长事务：保持长时间活跃
    let db_long = db.clone();
    let table_long = table.clone();
    let running_long = running.clone();
    let long_count = long_tx_count.clone();

    let long_handle = thread::spawn(move || {
        let tx = db_long.begin_transaction();
        let snapshot = db_long.get_snapshot(tx);
        let mut count = 0u64;

        while running_long.load(Ordering::Relaxed) {
            // 使用同一个快照读取
            for i in 0..100 {
                let _ = table_long.get_with_snapshot(i as u64, &snapshot);
                count += 1;
            }
        }

        db_long.commit_transaction(tx);
        long_count.store(count, Ordering::Relaxed);
        println!("Long transaction completed {} reads", count);
    });
    handles.push(long_handle);

    // 短事务：频繁提交
    for _ in 0..10 {
        let db = db.clone();
        let table = table.clone();
        let running = running.clone();
        let short_count = short_tx_count.clone();

        let handle = thread::spawn(move || {
            let mut count = 0u64;
            let mut i = 0;
            while running.load(Ordering::Relaxed) {
                let tx = db.begin_transaction();
                let record = create_test_record(i as i64, &format!("Updated{}", count));
                if table.update(i as u64, record, tx).is_ok() {
                    db.commit_transaction(tx);
                    count += 1;
                } else {
                    db.rollback_transaction(tx);
                }
                i = (i + 1) % 100;
            }
            short_count.fetch_add(count, Ordering::Relaxed);
        });
        handles.push(handle);
    }

    // 运行测试
    thread::sleep(Duration::from_secs(3));
    running.store(false, Ordering::Relaxed);

    for handle in handles {
        handle.join().unwrap();
    }

    let long_reads = long_tx_count.load(Ordering::Relaxed);
    let short_writes = short_tx_count.load(Ordering::Relaxed);

    println!("\n=== Results ===");
    println!("Long transaction reads: {}", long_reads);
    println!("Short transaction writes: {}", short_writes);

    // 长事务应该能够完成大量读取
    assert!(long_reads > 1000, "Long transaction should complete many reads");
}
