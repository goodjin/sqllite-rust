//! Phase 9 Week 2: SQLite 对比基准测试
//!
//! 本基准测试对比 sqllite-rust 与 SQLite 的性能
//! 使用 Criterion crate 进行性能测量

use criterion::{
    criterion_group, criterion_main, Criterion, BenchmarkId, black_box, Throughput,
};
use std::process::{Command, Stdio};
use std::io::Write;
use std::time::Duration;
use tempfile::TempDir;

// 导入本项目的库
use sqllite_rust::executor::{Executor, ExecuteResult};

// ============================================================================
// SQLite 基准测试辅助函数
// ============================================================================

/// 执行 SQLite 命令
fn run_sqlite_commands(db_path: &str, commands: &[String]) -> Duration {
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

/// 创建 SQLite 数据库并填充数据
fn setup_sqlite_db(path: &str, row_count: usize) {
    // 建表
    run_sqlite_commands(path, &[
        "CREATE TABLE users (id INTEGER, name TEXT, email TEXT, age INTEGER, salary REAL);".to_string(),
        "BEGIN;".to_string(),
    ]);
    
    // 批量插入
    let batch_size = 1000;
    for batch in 0..(row_count / batch_size) {
        let mut commands = vec!["BEGIN;".to_string()];
        for i in 0..batch_size {
            let row_id = batch * batch_size + i;
            commands.push(format!(
                "INSERT INTO users VALUES ({}, 'User{}', 'user{}@example.com', {}, {}.0);",
                row_id, row_id, row_id, row_id % 100, row_id % 100_000
            ));
        }
        commands.push("COMMIT;".to_string());
        run_sqlite_commands(path, &commands);
    }
}

/// 创建临时数据库路径
fn temp_db_path() -> (TempDir, String) {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");
    (temp_dir, db_path.to_str().unwrap().to_string())
}

/// 执行 SQL 语句
fn execute_sql(executor: &mut Executor, sql: &str) {
    executor.execute_sql(sql).expect("SQL execution failed");
}

/// 执行查询并返回行数
fn execute_query(executor: &mut Executor, sql: &str) -> usize {
    match executor.execute_sql(sql) {
        Ok(ExecuteResult::Query(result)) => result.rows.len(),
        Ok(_) => 0,
        Err(e) => panic!("Query failed: {}", e),
    }
}

/// 设置 sqllite-rust 基准测试数据
fn setup_sqllite_data(executor: &mut Executor, row_count: usize) {
    execute_sql(executor, "CREATE TABLE users (id INTEGER, name TEXT, email TEXT, age INTEGER, salary REAL)");

    for i in 0..row_count {
        let sql = format!(
            "INSERT INTO users VALUES ({}, 'User{}', 'user{}@example.com', {}, {}.0)",
            i, i, i, i % 100, i % 100_000
        );
        execute_sql(executor, &sql);
    }
}

// ============================================================================
// 基准测试 1: 点查 (Point Select)
// ============================================================================

fn bench_point_select(c: &mut Criterion) {
    let mut group = c.benchmark_group("point_select");
    group.measurement_time(Duration::from_secs(10));
    group.sample_size(50);

    // sqllite-rust: 准备数据
    let (_temp, db_path) = temp_db_path();
    {
        let mut executor = Executor::open(&db_path).expect("Failed to open db");
        setup_sqllite_data(&mut executor, 100_000);
    }

    group.bench_function("sqllite_rust", |b| {
        b.iter(|| {
            let mut executor = Executor::open(&db_path).expect("Failed to open db");
            let count = execute_query(&mut executor, "SELECT * FROM users WHERE id = 12345");
            black_box(count);
        });
    });

    // SQLite: 准备数据
    let sqlite_temp = TempDir::new().unwrap();
    let sqlite_path = sqlite_temp.path().join("test.db");
    let sqlite_path_str = sqlite_path.to_str().unwrap();
    setup_sqlite_db(sqlite_path_str, 100_000);

    group.bench_function("sqlite", |b| {
        b.iter(|| {
            run_sqlite_commands(
                sqlite_path_str,
                &["SELECT * FROM users WHERE id = 12345;".to_string()]
            );
        });
    });

    group.finish();
}

// ============================================================================
// 基准测试 2: 范围查询 (Range Scan)
// ============================================================================

fn bench_range_scan(c: &mut Criterion) {
    let mut group = c.benchmark_group("range_scan");
    group.measurement_time(Duration::from_secs(10));
    group.sample_size(30);

    // sqllite-rust
    let (_temp, db_path) = temp_db_path();
    {
        let mut executor = Executor::open(&db_path).expect("Failed to open db");
        setup_sqllite_data(&mut executor, 100_000);
    }

    group.bench_function("sqllite_rust", |b| {
        b.iter(|| {
            let mut executor = Executor::open(&db_path).expect("Failed to open db");
            let count = execute_query(&mut executor, "SELECT * FROM users WHERE id BETWEEN 10000 AND 20000");
            black_box(count);
        });
    });

    // SQLite
    let sqlite_temp = TempDir::new().unwrap();
    let sqlite_path = sqlite_temp.path().join("test.db");
    let sqlite_path_str = sqlite_path.to_str().unwrap();
    setup_sqlite_db(sqlite_path_str, 100_000);

    group.bench_function("sqlite", |b| {
        b.iter(|| {
            run_sqlite_commands(
                sqlite_path_str,
                &["SELECT * FROM users WHERE id BETWEEN 10000 AND 20000;".to_string()]
            );
        });
    });

    group.finish();
}

// ============================================================================
// 基准测试 3: 全表扫描 (Full Table Scan)
// ============================================================================

fn bench_full_table_scan(c: &mut Criterion) {
    let mut group = c.benchmark_group("full_table_scan");
    group.measurement_time(Duration::from_secs(10));
    group.sample_size(20);

    // sqllite-rust
    let (_temp, db_path) = temp_db_path();
    {
        let mut executor = Executor::open(&db_path).expect("Failed to open db");
        setup_sqllite_data(&mut executor, 100_000);
    }

    group.bench_function("sqllite_rust", |b| {
        b.iter(|| {
            let mut executor = Executor::open(&db_path).expect("Failed to open db");
            let count = execute_query(&mut executor, "SELECT * FROM users");
            black_box(count);
        });
    });

    // SQLite
    let sqlite_temp = TempDir::new().unwrap();
    let sqlite_path = sqlite_temp.path().join("test.db");
    let sqlite_path_str = sqlite_path.to_str().unwrap();
    setup_sqlite_db(sqlite_path_str, 100_000);

    group.bench_function("sqlite", |b| {
        b.iter(|| {
            run_sqlite_commands(
                sqlite_path_str,
                &["SELECT * FROM users;".to_string()]
            );
        });
    });

    group.finish();
}

// ============================================================================
// 基准测试 4: 简单插入 (Simple Insert)
// ============================================================================

fn bench_simple_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("simple_insert");
    group.measurement_time(Duration::from_secs(10));
    group.sample_size(20);

    // sqllite-rust
    group.bench_function("sqllite_rust", |b| {
        b.iter(|| {
            let (_temp, db_path) = temp_db_path();
            let mut executor = Executor::open(&db_path).expect("Failed to open db");

            execute_sql(&mut executor, "CREATE TABLE users (id INTEGER, name TEXT, email TEXT)");

            for i in 0..1000 {
                let sql = format!("INSERT INTO users VALUES ({}, 'User{}', 'user{}@example.com')", i, i, i);
                execute_sql(&mut executor, &sql);
            }

            black_box(executor);
        });
    });

    // SQLite
    group.bench_function("sqlite", |b| {
        b.iter(|| {
            let temp_dir = TempDir::new().unwrap();
            let db_path = temp_dir.path().join("test.db");
            let path_str = db_path.to_str().unwrap();

            let mut commands = vec![
                "CREATE TABLE users (id INTEGER, name TEXT, email TEXT);".to_string(),
            ];
            for i in 0..1000 {
                commands.push(format!(
                    "INSERT INTO users VALUES ({}, 'User{}', 'user{}@example.com');",
                    i, i, i
                ));
            }
            run_sqlite_commands(path_str, &commands)
        });
    });

    group.finish();
}

// ============================================================================
// 基准测试 5: 批量插入 (Bulk Insert)
// ============================================================================

fn bench_bulk_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("bulk_insert");
    group.measurement_time(Duration::from_secs(15));
    group.sample_size(10);

    for size in [1000, 10000].iter() {
        group.throughput(Throughput::Elements(*size as u64));

        // sqllite-rust
        group.bench_with_input(
            BenchmarkId::new("sqllite_rust", size),
            size,
            |b, &n| {
                b.iter(|| {
                    let (_temp, db_path) = temp_db_path();
                    let mut executor = Executor::open(&db_path).expect("Failed to open db");

                    execute_sql(&mut executor, "CREATE TABLE logs (id INTEGER, message TEXT, timestamp INTEGER)");

                    for i in 0..n {
                        let sql = format!(
                            "INSERT INTO logs VALUES ({}, 'Log message number {}', {})",
                            i, i, i
                        );
                        execute_sql(&mut executor, &sql);
                    }

                    black_box(executor);
                });
            },
        );

        // SQLite
        group.bench_with_input(
            BenchmarkId::new("sqlite", size),
            size,
            |b, &n| {
                b.iter(|| {
                    let temp_dir = TempDir::new().unwrap();
                    let db_path = temp_dir.path().join("test.db");
                    let path_str = db_path.to_str().unwrap();

                    let mut commands = vec![
                        "CREATE TABLE logs (id INTEGER, message TEXT, timestamp INTEGER);".to_string(),
                        "BEGIN;".to_string(),
                    ];
                    for i in 0..n {
                        commands.push(format!(
                            "INSERT INTO logs VALUES ({}, 'Log message number {}', {});",
                            i, i, i
                        ));
                    }
                    commands.push("COMMIT;".to_string());
                    run_sqlite_commands(path_str, &commands)
                });
            },
        );
    }

    group.finish();
}

// ============================================================================
// 基准测试 6: 更新 (Update)
// ============================================================================

fn bench_update(c: &mut Criterion) {
    let mut group = c.benchmark_group("update");
    group.measurement_time(Duration::from_secs(10));
    group.sample_size(20);

    // sqllite-rust: 准备数据
    let (_temp, db_path) = temp_db_path();
    {
        let mut executor = Executor::open(&db_path).expect("Failed to open db");
        execute_sql(&mut executor, "CREATE TABLE inventory (id INTEGER, item TEXT, quantity INTEGER)");
        for i in 0..10_000 {
            let sql = format!("INSERT INTO inventory VALUES ({}, 'Item{}', {})", i, i, i % 100);
            execute_sql(&mut executor, &sql);
        }
    }

    group.bench_function("sqllite_rust", |b| {
        b.iter(|| {
            let mut executor = Executor::open(&db_path).expect("Failed to open db");
            // 更新1000行
            for i in 0..1000 {
                let sql = format!("UPDATE inventory SET quantity = quantity + 1 WHERE id = {}", i * 2);
                execute_sql(&mut executor, &sql);
            }
            black_box(executor);
        });
    });

    // SQLite: 准备数据
    let sqlite_temp = TempDir::new().unwrap();
    let sqlite_path = sqlite_temp.path().join("test.db");
    let sqlite_path_str = sqlite_path.to_str().unwrap();
    
    {
        let mut commands = vec![
            "CREATE TABLE inventory (id INTEGER, item TEXT, quantity INTEGER);".to_string(),
            "BEGIN;".to_string(),
        ];
        for i in 0..10_000 {
            commands.push(format!("INSERT INTO inventory VALUES ({}, 'Item{}', {});", i, i, i % 100));
        }
        commands.push("COMMIT;".to_string());
        run_sqlite_commands(sqlite_path_str, &commands);
    }

    group.bench_function("sqlite", |b| {
        b.iter(|| {
            let commands: Vec<String> = (0..1000)
                .map(|i| format!("UPDATE inventory SET quantity = quantity + 1 WHERE id = {};", i * 2))
                .collect();
            run_sqlite_commands(sqlite_path_str, &commands)
        });
    });

    group.finish();
}

// ============================================================================
// 基准测试 7: 删除 (Delete)
// ============================================================================

fn bench_delete(c: &mut Criterion) {
    let mut group = c.benchmark_group("delete");
    group.measurement_time(Duration::from_secs(10));
    group.sample_size(20);

    // sqllite-rust
    group.bench_function("sqllite_rust", |b| {
        b.iter(|| {
            let (_temp, db_path) = temp_db_path();
            let mut executor = Executor::open(&db_path).expect("Failed to open db");

            execute_sql(&mut executor, "CREATE TABLE events (id INTEGER, event_type TEXT, created_at INTEGER)");

            // 插入10000行
            for i in 0..10_000 {
                let sql = format!("INSERT INTO events VALUES ({}, 'type{}', {})", i, i % 10, i);
                execute_sql(&mut executor, &sql);
            }

            // 删除5000行
            execute_sql(&mut executor, "DELETE FROM events WHERE id < 5000");

            black_box(executor);
        });
    });

    // SQLite
    group.bench_function("sqlite", |b| {
        b.iter(|| {
            let temp_dir = TempDir::new().unwrap();
            let db_path = temp_dir.path().join("test.db");
            let path_str = db_path.to_str().unwrap();

            let mut commands = vec![
                "CREATE TABLE events (id INTEGER, event_type TEXT, created_at INTEGER);".to_string(),
                "BEGIN;".to_string(),
            ];
            for i in 0..10_000 {
                commands.push(format!("INSERT INTO events VALUES ({}, 'type{}', {});", i, i % 10, i));
            }
            commands.push("COMMIT;".to_string());
            commands.push("DELETE FROM events WHERE id < 5000;".to_string());

            run_sqlite_commands(path_str, &commands)
        });
    });

    group.finish();
}

// ============================================================================
// 基准测试 8: 聚合 (Aggregate)
// ============================================================================

fn bench_aggregate(c: &mut Criterion) {
    let mut group = c.benchmark_group("aggregate");
    group.measurement_time(Duration::from_secs(10));
    group.sample_size(30);

    // sqllite-rust: 准备数据
    let (_temp, db_path) = temp_db_path();
    {
        let mut executor = Executor::open(&db_path).expect("Failed to open db");
        execute_sql(&mut executor, "CREATE TABLE sales (id INTEGER, region TEXT, amount REAL)");
        let regions = ["North", "South", "East", "West"];
        for i in 0..100_000 {
            let sql = format!(
                "INSERT INTO sales VALUES ({}, '{}', {}.99)",
                i, regions[i % 4], i % 1000
            );
            execute_sql(&mut executor, &sql);
        }
    }

    group.bench_function("sqllite_rust", |b| {
        b.iter(|| {
            let mut executor = Executor::open(&db_path).expect("Failed to open db");
            let count = execute_query(&mut executor, 
                "SELECT region, COUNT(*), SUM(amount), AVG(amount), MAX(amount), MIN(amount) FROM sales GROUP BY region"
            );
            black_box(count);
        });
    });

    // SQLite: 准备数据
    let sqlite_temp = TempDir::new().unwrap();
    let sqlite_path = sqlite_temp.path().join("test.db");
    let sqlite_path_str = sqlite_path.to_str().unwrap();
    
    {
        let mut commands = vec![
            "CREATE TABLE sales (id INTEGER, region TEXT, amount REAL);".to_string(),
            "BEGIN;".to_string(),
        ];
        let regions = ["North", "South", "East", "West"];
        for i in 0..100_000 {
            commands.push(format!(
                "INSERT INTO sales VALUES ({}, '{}', {}.99);",
                i, regions[i % 4], i % 1000
            ));
        }
        commands.push("COMMIT;".to_string());
        run_sqlite_commands(sqlite_path_str, &commands);
    }

    group.bench_function("sqlite", |b| {
        b.iter(|| {
            run_sqlite_commands(
                sqlite_path_str,
                &["SELECT region, COUNT(*), SUM(amount), AVG(amount), MAX(amount), MIN(amount) FROM sales GROUP BY region;".to_string()]
            );
        });
    });

    group.finish();
}

// ============================================================================
// 基准测试 9: JOIN
// ============================================================================

fn bench_join(c: &mut Criterion) {
    let mut group = c.benchmark_group("join");
    group.measurement_time(Duration::from_secs(10));
    group.sample_size(20);

    // sqllite-rust: 准备数据
    let (_temp, db_path) = temp_db_path();
    {
        let mut executor = Executor::open(&db_path).expect("Failed to open db");
        
        execute_sql(&mut executor, "CREATE TABLE orders (id INTEGER, user_id INTEGER, amount REAL)");
        execute_sql(&mut executor, "CREATE TABLE order_items (id INTEGER, order_id INTEGER, product_name TEXT)");

        // 插入订单
        for i in 0..10_000 {
            let sql = format!("INSERT INTO orders VALUES ({}, {}, {}.99)", i, i % 1000, i % 1000);
            execute_sql(&mut executor, &sql);
        }

        // 插入订单项
        for i in 0..50_000 {
            let sql = format!("INSERT INTO order_items VALUES ({}, {}, 'Product{}')", i, i % 10_000, i % 100);
            execute_sql(&mut executor, &sql);
        }
    }

    group.bench_function("sqllite_rust", |b| {
        b.iter(|| {
            let mut executor = Executor::open(&db_path).expect("Failed to open db");
            let count = execute_query(&mut executor,
                "SELECT o.*, oi.product_name FROM orders o JOIN order_items oi ON o.id = oi.order_id WHERE o.amount > 100"
            );
            black_box(count);
        });
    });

    // SQLite: 准备数据
    let sqlite_temp = TempDir::new().unwrap();
    let sqlite_path = sqlite_temp.path().join("test.db");
    let sqlite_path_str = sqlite_path.to_str().unwrap();
    
    {
        let mut commands = vec![
            "CREATE TABLE orders (id INTEGER, user_id INTEGER, amount REAL);".to_string(),
            "CREATE TABLE order_items (id INTEGER, order_id INTEGER, product_name TEXT);".to_string(),
            "BEGIN;".to_string(),
        ];
        for i in 0..10_000 {
            commands.push(format!("INSERT INTO orders VALUES ({}, {}, {}.99);", i, i % 1000, i % 1000));
        }
        for i in 0..50_000 {
            commands.push(format!("INSERT INTO order_items VALUES ({}, {}, 'Product{}');", i, i % 10_000, i % 100));
        }
        commands.push("COMMIT;".to_string());
        run_sqlite_commands(sqlite_path_str, &commands);
    }

    group.bench_function("sqlite", |b| {
        b.iter(|| {
            run_sqlite_commands(
                sqlite_path_str,
                &["SELECT o.*, oi.product_name FROM orders o JOIN order_items oi ON o.id = oi.order_id WHERE o.amount > 100;".to_string()]
            );
        });
    });

    group.finish();
}

// ============================================================================
// 基准测试 10: GROUP BY
// ============================================================================

fn bench_group_by(c: &mut Criterion) {
    let mut group = c.benchmark_group("group_by");
    group.measurement_time(Duration::from_secs(10));
    group.sample_size(30);

    // sqllite-rust: 准备数据
    let (_temp, db_path) = temp_db_path();
    {
        let mut executor = Executor::open(&db_path).expect("Failed to open db");
        execute_sql(&mut executor, "CREATE TABLE employees (id INTEGER, dept TEXT, salary INTEGER)");

        let depts = ["Engineering", "Sales", "Marketing", "HR", "Finance"];
        for i in 0..100_000 {
            let sql = format!(
                "INSERT INTO employees VALUES ({}, '{}', {})",
                i, depts[i % 5], 30000 + (i % 70000)
            );
            execute_sql(&mut executor, &sql);
        }
    }

    group.bench_function("sqllite_rust", |b| {
        b.iter(|| {
            let mut executor = Executor::open(&db_path).expect("Failed to open db");
            let count = execute_query(&mut executor,
                "SELECT dept, COUNT(*), AVG(salary), MAX(salary), MIN(salary) FROM employees GROUP BY dept"
            );
            black_box(count);
        });
    });

    // SQLite: 准备数据
    let sqlite_temp = TempDir::new().unwrap();
    let sqlite_path = sqlite_temp.path().join("test.db");
    let sqlite_path_str = sqlite_path.to_str().unwrap();
    
    {
        let mut commands = vec![
            "CREATE TABLE employees (id INTEGER, dept TEXT, salary INTEGER);".to_string(),
            "BEGIN;".to_string(),
        ];
        let depts = ["Engineering", "Sales", "Marketing", "HR", "Finance"];
        for i in 0..100_000 {
            commands.push(format!(
                "INSERT INTO employees VALUES ({}, '{}', {});",
                i, depts[i % 5], 30000 + (i % 70000)
            ));
        }
        commands.push("COMMIT;".to_string());
        run_sqlite_commands(sqlite_path_str, &commands);
    }

    group.bench_function("sqlite", |b| {
        b.iter(|| {
            run_sqlite_commands(
                sqlite_path_str,
                &["SELECT dept, COUNT(*), AVG(salary), MAX(salary), MIN(salary) FROM employees GROUP BY dept;".to_string()]
            );
        });
    });

    group.finish();
}

// ============================================================================
// 基准测试 11: 子查询 (Subquery)
// ============================================================================

fn bench_subquery(c: &mut Criterion) {
    let mut group = c.benchmark_group("subquery");
    group.measurement_time(Duration::from_secs(10));
    group.sample_size(20);

    // sqllite-rust: 准备数据
    let (_temp, db_path) = temp_db_path();
    {
        let mut executor = Executor::open(&db_path).expect("Failed to open db");
        execute_sql(&mut executor, "CREATE TABLE products (id INTEGER, name TEXT, price INTEGER, category TEXT)");

        let categories = ["Electronics", "Clothing", "Food", "Books"];
        for i in 0..50_000 {
            let sql = format!(
                "INSERT INTO products VALUES ({}, 'Product{}', {}, '{}')",
                i, i, 10 + (i % 1000), categories[i % 4]
            );
            execute_sql(&mut executor, &sql);
        }
    }

    group.bench_function("sqllite_rust", |b| {
        b.iter(|| {
            let mut executor = Executor::open(&db_path).expect("Failed to open db");
            let count = execute_query(&mut executor,
                "SELECT * FROM products WHERE price > (SELECT AVG(price) FROM products)"
            );
            black_box(count);
        });
    });

    // SQLite: 准备数据
    let sqlite_temp = TempDir::new().unwrap();
    let sqlite_path = sqlite_temp.path().join("test.db");
    let sqlite_path_str = sqlite_path.to_str().unwrap();
    
    {
        let mut commands = vec![
            "CREATE TABLE products (id INTEGER, name TEXT, price INTEGER, category TEXT);".to_string(),
            "BEGIN;".to_string(),
        ];
        let categories = ["Electronics", "Clothing", "Food", "Books"];
        for i in 0..50_000 {
            commands.push(format!(
                "INSERT INTO products VALUES ({}, 'Product{}', {}, '{}');",
                i, i, 10 + (i % 1000), categories[i % 4]
            ));
        }
        commands.push("COMMIT;".to_string());
        run_sqlite_commands(sqlite_path_str, &commands);
    }

    group.bench_function("sqlite", |b| {
        b.iter(|| {
            run_sqlite_commands(
                sqlite_path_str,
                &["SELECT * FROM products WHERE price > (SELECT AVG(price) FROM products);".to_string()]
            );
        });
    });

    group.finish();
}

// ============================================================================
// 基准测试 12: 索引扫描 (Index Scan)
// ============================================================================

fn bench_index_scan(c: &mut Criterion) {
    let mut group = c.benchmark_group("index_scan");
    group.measurement_time(Duration::from_secs(10));
    group.sample_size(50);

    // sqllite-rust: 准备数据
    let (_temp, db_path) = temp_db_path();
    {
        let mut executor = Executor::open(&db_path).expect("Failed to open db");
        execute_sql(&mut executor, "CREATE TABLE indexed_users (id INTEGER, email TEXT, age INTEGER)");
        
        // 创建索引
        execute_sql(&mut executor, "CREATE INDEX idx_email ON indexed_users(email)");
        execute_sql(&mut executor, "CREATE INDEX idx_age ON indexed_users(age)");

        for i in 0..100_000 {
            let sql = format!(
                "INSERT INTO indexed_users VALUES ({}, 'user{}@example.com', {})",
                i, i, i % 100
            );
            execute_sql(&mut executor, &sql);
        }
    }

    group.bench_function("sqllite_rust", |b| {
        b.iter(|| {
            let mut executor = Executor::open(&db_path).expect("Failed to open db");
            let count = execute_query(&mut executor,
                "SELECT * FROM indexed_users WHERE email = 'user50000@example.com'"
            );
            black_box(count);
        });
    });

    // SQLite: 准备数据
    let sqlite_temp = TempDir::new().unwrap();
    let sqlite_path = sqlite_temp.path().join("test.db");
    let sqlite_path_str = sqlite_path.to_str().unwrap();
    
    {
        let mut commands = vec![
            "CREATE TABLE indexed_users (id INTEGER, email TEXT, age INTEGER);".to_string(),
            "CREATE INDEX idx_email ON indexed_users(email);".to_string(),
            "CREATE INDEX idx_age ON indexed_users(age);".to_string(),
            "BEGIN;".to_string(),
        ];
        for i in 0..100_000 {
            commands.push(format!(
                "INSERT INTO indexed_users VALUES ({}, 'user{}@example.com', {});",
                i, i, i % 100
            ));
        }
        commands.push("COMMIT;".to_string());
        run_sqlite_commands(sqlite_path_str, &commands);
    }

    group.bench_function("sqlite", |b| {
        b.iter(|| {
            run_sqlite_commands(
                sqlite_path_str,
                &["SELECT * FROM indexed_users WHERE email = 'user50000@example.com';".to_string()]
            );
        });
    });

    group.finish();
}

// ============================================================================
// 基准测试组
// ============================================================================

criterion_group!(
    benches,
    bench_point_select,      // 1. 点查
    bench_range_scan,        // 2. 范围查询
    bench_full_table_scan,   // 3. 全表扫描
    bench_simple_insert,     // 4. 简单插入
    bench_bulk_insert,       // 5. 批量插入
    bench_update,            // 6. 更新
    bench_delete,            // 7. 删除
    bench_aggregate,         // 8. 聚合
    bench_join,              // 9. JOIN
    bench_group_by,          // 10. 分组
    bench_subquery,          // 11. 子查询
    bench_index_scan         // 12. 索引扫描
);

criterion_main!(benches);
