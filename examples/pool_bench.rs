//! Phase 5: 连接池 + 事务批量 性能基准测试

use std::time::Instant;

fn main() {
    // 清理测试数据库
    let _ = std::fs::remove_file("/tmp/bench_pool.db");

    println!("=== Phase 5: 连接池 + 事务批量 基准测试 ===\n");

    // 测试1：普通模式 vs 自动批量模式
    println!("【测试1】自动批量模式 vs 普通模式");
    bench_auto_batch();

    // 测试2：连接池
    println!("\n【测试2】连接池复用");
    bench_connection_pool();

    // 清理
    let _ = std::fs::remove_file("/tmp/bench_pool.db");
}

/// 自动批量模式 vs 普通模式
fn bench_auto_batch() {
    // 普通模式
    let _ = std::fs::remove_file("/tmp/bench_pool.db");
    let mut executor = sqllite_rust::executor::Executor::open("/tmp/bench_pool.db").unwrap();
    executor.execute_sql("CREATE TABLE users (id INTEGER, name TEXT)").unwrap();

    let start = Instant::now();
    for i in 0..100 {
        let sql = format!("INSERT INTO users VALUES ({}, 'User{}')", i, i);
        executor.execute_sql(&sql).unwrap();
    }
    let normal_time = start.elapsed();

    // 自动批量模式
    let _ = std::fs::remove_file("/tmp/bench_pool.db");
    let mut executor = sqllite_rust::executor::Executor::open("/tmp/bench_pool.db").unwrap();
    executor.execute_sql("CREATE TABLE users (id INTEGER, name TEXT)").unwrap();
    executor.enable_auto_batch(100); // 100条自动提交

    let start = Instant::now();
    for i in 0..100 {
        let sql = format!("INSERT INTO users VALUES ({}, 'User{}')", i, i);
        executor.execute_sql(&sql).unwrap();
    }
    executor.flush_batch().unwrap(); // 确保最后一批提交
    let batch_time = start.elapsed();

    println!("  普通模式: {:?}", normal_time);
    println!("  自动批量模式: {:?}", batch_time);
    if normal_time > batch_time {
        println!("  加速比: {:.2}x", normal_time.as_nanos() as f64 / batch_time.as_nanos() as f64);
    } else {
        println!("  批量模式稍慢，可能是数据量不够大");
    }
}

/// 连接池复用
fn bench_connection_pool() {
    let _ = std::fs::remove_file("/tmp/bench_pool.db");
    let pool = sqllite_rust::executor::pool::ConnectionPool::new("/tmp/bench_pool.db", 4).unwrap();

    // 创建表
    {
        let mut conn = pool.get();
        if let Some(ref mut executor) = conn.executor {
            executor.execute_sql("CREATE TABLE users (id INTEGER, name TEXT)").unwrap();
        }
    }

    // 使用连接池（复用连接）
    let start = Instant::now();
    for i in 0..50 {
        let mut conn = pool.get();
        if let Some(ref mut executor) = conn.executor {
            let sql = format!("INSERT INTO users VALUES ({}, 'User{}')", i, i);
            executor.execute_sql(&sql).unwrap();
        }
        // 连接自动归还
    }
    let pool_time = start.elapsed();

    // 不使用连接池（每次新建连接）
    let start = Instant::now();
    for i in 0..50 {
        let mut executor = sqllite_rust::executor::Executor::open("/tmp/bench_pool.db").unwrap();
        let sql = format!("INSERT INTO users VALUES ({}, 'User{}')", i + 50, i);
        executor.execute_sql(&sql).unwrap();
    }
    let no_pool_time = start.elapsed();

    println!("  使用连接池: {:?}", pool_time);
    println!("  不使用连接池: {:?}", no_pool_time);
    println!("  连接池加速比: {:.2}x", no_pool_time.as_nanos() as f64 / pool_time.as_nanos() as f64);
    println!("  池状态: {}", pool.status());
}
