use criterion::{criterion_group, criterion_main, Criterion, BenchmarkId};
use std::process::{Command, Stdio};
use std::io::Write;
use std::fs;
use std::time::Duration;
use tempfile::TempDir;

/// 执行 SQLite 命令并测量时间
fn run_sqlite_commands(db_path: &str, commands: &[&str]) -> Duration {
    let mut cmd = Command::new("sqlite3")
        .arg(db_path)
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("sqlite3 not found. Install: brew install sqlite3");
    
    let stdin = cmd.stdin.as_mut().unwrap();
    for command in commands {
        writeln!(stdin, "{}", command).unwrap();
    }
    
    let start = std::time::Instant::now();
    cmd.wait().unwrap();
    start.elapsed()
}

/// 执行本实现并测量时间
fn run_sqllite_rust_commands(db_path: &str, commands: &[&str]) -> Duration {
    let output = std::fs::read_to_string("/dev/null"); // 临时占位
    
    let start = std::time::Instant::now();
    // 这里需要根据实际情况调用本实现的接口
    // 暂时使用 Command 方式
    let mut cmd = Command::new("cargo")
        .args(["run", "--", "shell"])
        .current_dir("/Users/cat/github/sqllite-rust")
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("cargo run failed");
    
    let stdin = cmd.stdin.as_mut().unwrap();
    for command in commands {
        writeln!(stdin, "{}", command).unwrap();
    }
    cmd.wait().unwrap();
    
    start.elapsed()
}

// ==================== 测试方案 1: 单条插入性能 ====================
fn bench_single_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("single_insert");
    
    for row_count in [100, 1000, 5000].iter() {
        // SQLite 基准
        group.bench_with_input(
            BenchmarkId::new("sqlite", row_count),
            row_count,
            |b, &n| {
                b.iter(|| {
                    let temp_dir = TempDir::new().unwrap();
                    let db_path = temp_dir.path().join("test.db");
                    
                    let mut commands = vec![
                        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT);",
                    ];
                    for i in 0..n {
                        commands.push(&format!(
                            "INSERT INTO users VALUES ({}, 'User{}', 'user{}@example.com');",
                            i, i, i
                        ));
                    }
                    run_sqlite_commands(db_path.to_str().unwrap(), &commands)
                })
            },
        );
        
        // 本实现（需要等实现更完善后启用）
        // group.bench_with_input(...)
    }
    
    group.finish();
}

// ==================== 测试方案 2: 批量插入性能 ====================
fn bench_batch_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("batch_insert");
    
    for batch_size in [1000, 10000, 50000].iter() {
        group.bench_with_input(
            BenchmarkId::new("sqlite", batch_size),
            batch_size,
            |b, &n| {
                b.iter(|| {
                    let temp_dir = TempDir::new().unwrap();
                    let db_path = temp_dir.path().join("test.db");
                    
                    // 使用事务包裹批量插入
                    let mut commands = vec![
                        "CREATE TABLE logs (id INTEGER PRIMARY KEY, message TEXT, timestamp INTEGER);",
                        "BEGIN;",
                    ];
                    
                    for i in 0..n {
                        commands.push(&format!(
                            "INSERT INTO logs VALUES ({}, 'Log message number {}', {});",
                            i, i, i
                        ));
                    }
                    commands.push("COMMIT;");
                    
                    run_sqlite_commands(db_path.to_str().unwrap(), &commands)
                })
            },
        );
    }
    
    group.finish();
}

// ==================== 测试方案 3: 简单查询性能 ====================
fn bench_simple_select(c: &mut Criterion) {
    let mut group = c.benchmark_group("simple_select");
    
    for table_size in [1000, 10000, 100000].iter() {
        group.bench_with_input(
            BenchmarkId::new("sqlite_full_scan", table_size),
            table_size,
            |b, &n| {
                let temp_dir = TempDir::new().unwrap();
                let db_path = temp_dir.path().join("test.db");
                
                // 准备数据
                let setup_commands = vec![
                    "CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL);",
                    "BEGIN;",
                ];
                run_sqlite_commands(db_path.to_str().unwrap(), &setup_commands);
                
                // 插入数据
                for i in 0..n {
                    let cmd = format!(
                        "INSERT INTO products VALUES ({}, 'Product{}', {}.99);",
                        i, i, i % 100
                    );
                    run_sqlite_commands(db_path.to_str().unwrap(), &[&cmd]);
                }
                run_sqlite_commands(db_path.to_str().unwrap(), &["COMMIT;"]);
                
                // 基准测试查询
                b.iter(|| {
                    run_sqlite_commands(
                        db_path.to_str().unwrap(),
                        &["SELECT * FROM PRODUCTS WHERE price > 50.0;"]
                    )
                })
            },
        );
    }
    
    group.finish();
}

// ==================== 测试方案 4: 索引查询性能 ====================
fn bench_indexed_select(c: &mut Criterion) {
    let mut group = c.benchmark_group("indexed_select");
    
    for table_size in [1000, 10000, 100000].iter() {
        group.bench_with_input(
            BenchmarkId::new("sqlite_with_index", table_size),
            table_size,
            |b, &n| {
                let temp_dir = TempDir::new().unwrap();
                let db_path = temp_dir.path().join("test.db");
                
                // 准备带索引的表
                let setup_commands = vec![
                    "CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT, age INTEGER);",
                    "CREATE INDEX idx_email ON users(email);",
                    "CREATE INDEX idx_age ON users(age);",
                    "BEGIN;",
                ];
                run_sqlite_commands(db_path.to_str().unwrap(), &setup_commands);
                
                // 插入数据
                for i in 0..n {
                    let cmd = format!(
                        "INSERT INTO users VALUES ({}, 'user{}@example.com', {});",
                        i, i, i % 100
                    );
                    run_sqlite_commands(db_path.to_str().unwrap(), &[&cmd]);
                }
                run_sqlite_commands(db_path.to_str().unwrap(), &["COMMIT;"]);
                
                // 测试索引查询
                b.iter(|| {
                    run_sqlite_commands(
                        db_path.to_str().unwrap(),
                        &["SELECT * FROM users WHERE email = 'user500@example.com';"]
                    )
                })
            },
        );
        
        // 无索引对比
        group.bench_with_input(
            BenchmarkId::new("sqlite_no_index", table_size),
            table_size,
            |b, &n| {
                let temp_dir = TempDir::new().unwrap();
                let db_path = temp_dir.path().join("test.db");
                
                let setup_commands = vec![
                    "CREATE TABLE users_no_idx (id INTEGER PRIMARY KEY, email TEXT, age INTEGER);",
                    "BEGIN;",
                ];
                run_sqlite_commands(db_path.to_str().unwrap(), &setup_commands);
                
                for i in 0..n {
                    let cmd = format!(
                        "INSERT INTO users_no_idx VALUES ({}, 'user{}@example.com', {});",
                        i, i, i % 100
                    );
                    run_sqlite_commands(db_path.to_str().unwrap(), &[&cmd]);
                }
                run_sqlite_commands(db_path.to_str().unwrap(), &["COMMIT;"]);
                
                b.iter(|| {
                    run_sqlite_commands(
                        db_path.to_str().unwrap(),
                        &["SELECT * FROM users_no_idx WHERE email = 'user500@example.com';"]
                    )
                })
            },
        );
    }
    
    group.finish();
}

// ==================== 测试方案 5: JOIN 查询性能 ====================
fn bench_join(c: &mut Criterion) {
    let mut group = c.benchmark_group("join_query");
    
    for size in [100, 1000, 5000].iter() {
        group.bench_with_input(
            BenchmarkId::new("sqlite_join", size),
            size,
            |b, &n| {
                let temp_dir = TempDir::new().unwrap();
                let db_path = temp_dir.path().join("test.db");
                
                let setup_commands = vec![
                    "CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, amount REAL);",
                    "CREATE TABLE order_items (id INTEGER PRIMARY KEY, order_id INTEGER, product_name TEXT);",
                    "BEGIN;",
                ];
                run_sqlite_commands(db_path.to_str().unwrap(), &setup_commands);
                
                // 插入订单数据
                for i in 0..n {
                    let cmd = format!(
                        "INSERT INTO orders VALUES ({}, {}, {}.99);",
                        i, i % 100, i % 1000
                    );
                    run_sqlite_commands(db_path.to_str().unwrap(), &[&cmd]);
                }
                
                // 插入订单项数据
                for i in 0..(n * 5) {
                    let cmd = format!(
                        "INSERT INTO order_items VALUES ({}, {}, 'Product{}');",
                        i, i % n, i % 100
                    );
                    run_sqlite_commands(db_path.to_str().unwrap(), &[&cmd]);
                }
                
                run_sqlite_commands(db_path.to_str().unwrap(), &["COMMIT;"]);
                
                b.iter(|| {
                    run_sqlite_commands(
                        db_path.to_str().unwrap(),
                        &["SELECT o.*, oi.product_name FROM orders o JOIN order_items oi ON o.id = oi.order_id WHERE o.amount > 100;"]
                    )
                })
            },
        );
    }
    
    group.finish();
}

// ==================== 测试方案 6: 更新性能 ====================
fn bench_update(c: &mut Criterion) {
    let mut group = c.benchmark_group("update");
    
    for update_count in [100, 1000, 5000].iter() {
        group.bench_with_input(
            BenchmarkId::new("sqlite_update", update_count),
            update_count,
            |b, &n| {
                b.iter(|| {
                    let temp_dir = TempDir::new().unwrap();
                    let db_path = temp_dir.path().join("test.db");
                    
                    // 准备数据
                    let mut commands = vec![
                        "CREATE TABLE inventory (id INTEGER PRIMARY KEY, item TEXT, quantity INTEGER);",
                        "BEGIN;",
                    ];
                    
                    for i in 0..10000 {
                        commands.push(&format!(
                            "INSERT INTO inventory VALUES ({}, 'Item{}', {});",
                            i, i, i % 100
                        ));
                    }
                    commands.push("COMMIT;");
                    run_sqlite_commands(db_path.to_str().unwrap(), &commands);
                    
                    // 执行更新
                    let update_commands: Vec<String> = (0..n)
                        .map(|i| format!("UPDATE inventory SET quantity = quantity + 1 WHERE id = {};", i * 2))
                        .collect();
                    
                    let update_refs: Vec<&str> = update_commands.iter().map(|s| s.as_str()).collect();
                    run_sqlite_commands(db_path.to_str().unwrap(), &update_refs)
                })
            },
        );
    }
    
    group.finish();
}

// ==================== 测试方案 7: 删除性能 ====================
fn bench_delete(c: &mut Criterion) {
    let mut group = c.benchmark_group("delete");
    
    for delete_ratio in [0.1, 0.5, 0.9].iter() {
        group.bench_with_input(
            BenchmarkId::new("sqlite_delete", delete_ratio),
            delete_ratio,
            |b, &ratio| {
                b.iter(|| {
                    let temp_dir = TempDir::new().unwrap();
                    let db_path = temp_dir.path().join("test.db");
                    
                    let total_rows = 10000;
                    let delete_count = (total_rows as f64 * ratio) as usize;
                    
                    // 准备数据
                    let mut commands = vec![
                        "CREATE TABLE events (id INTEGER PRIMARY KEY, event_type TEXT, created_at INTEGER);",
                        "BEGIN;",
                    ];
                    
                    for i in 0..total_rows {
                        commands.push(&format!(
                            "INSERT INTO events VALUES ({}, 'type{}', {});",
                            i, i % 10, i
                        ));
                    }
                    commands.push("COMMIT;");
                    run_sqlite_commands(db_path.to_str().unwrap(), &commands);
                    
                    // 执行删除
                    run_sqlite_commands(
                        db_path.to_str().unwrap(),
                        &[&format!("DELETE FROM events WHERE id < {};", delete_count)]
                    )
                })
            },
        );
    }
    
    group.finish();
}

// ==================== 测试方案 8: 聚合查询性能 ====================
fn bench_aggregation(c: &mut Criterion) {
    let mut group = c.benchmark_group("aggregation");
    
    for table_size in [1000, 10000, 100000].iter() {
        group.bench_with_input(
            BenchmarkId::new("sqlite_agg", table_size),
            table_size,
            |b, &n| {
                let temp_dir = TempDir::new().unwrap();
                let db_path = temp_dir.path().join("test.db");
                
                let setup_commands = vec![
                    "CREATE TABLE sales (id INTEGER PRIMARY KEY, region TEXT, amount REAL);",
                    "BEGIN;",
                ];
                run_sqlite_commands(db_path.to_str().unwrap(), &setup_commands);
                
                for i in 0..n {
                    let regions = ["North", "South", "East", "West"];
                    let cmd = format!(
                        "INSERT INTO sales VALUES ({}, '{}', {}.99);",
                        i, regions[i % 4], i % 1000
                    );
                    run_sqlite_commands(db_path.to_str().unwrap(), &[&cmd]);
                }
                run_sqlite_commands(db_path.to_str().unwrap(), &["COMMIT;"]);
                
                b.iter(|| {
                    run_sqlite_commands(
                        db_path.to_str().unwrap(),
                        &["SELECT region, COUNT(*), SUM(amount), AVG(amount), MAX(amount), MIN(amount) FROM sales GROUP BY region;"]
                    )
                })
            },
        );
    }
    
    group.finish();
}

criterion_group!(
    benches,
    bench_single_insert,
    bench_batch_insert,
    bench_simple_select,
    bench_indexed_select,
    bench_join,
    bench_update,
    bench_delete,
    bench_aggregation
);
criterion_main!(benches);
