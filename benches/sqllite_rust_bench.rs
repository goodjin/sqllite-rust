use criterion::{criterion_group, criterion_main, Criterion, BenchmarkId, black_box};
use std::time::Duration;
use std::fs;
use tempfile::TempDir;

// 导入本项目的库
use sqllite_rust::sql::Parser;
use sqllite_rust::executor::{Executor, ExecuteResult};

/// 清理并创建临时数据库路径
fn temp_db_path() -> (TempDir, String) {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");
    (temp_dir, db_path.to_str().unwrap().to_string())
}

/// 执行 SQL 语句
fn execute_sql(executor: &mut Executor, sql: &str) {
    let mut parser = Parser::new(sql).expect("Tokenizer failed");
    let stmt = parser.parse().expect("Parse failed");
    executor.execute(&stmt).expect("Execution failed");
}

/// 执行查询并返回结果
fn execute_query(executor: &mut Executor, sql: &str) -> usize {
    let mut parser = Parser::new(sql).expect("Tokenizer failed");
    let stmt = parser.parse().expect("Parse failed");
    match executor.execute(&stmt).expect("Execution failed") {
        ExecuteResult::Query(result) => result.rows.len(),
        _ => 0,
    }
}

// ==================== 测试方案 1: 单条插入 ====================
fn bench_single_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("sqllite_single_insert");
    group.measurement_time(Duration::from_secs(10));
    group.sample_size(50);

    for row_count in [100, 500].iter() {
        group.bench_with_input(
            BenchmarkId::from_parameter(row_count),
            row_count,
            |b, &n| {
                b.iter(|| {
                    let (_temp, db_path) = temp_db_path();
                    let mut executor = Executor::open(&db_path).expect("Failed to open db");

                    // 创建表
                    execute_sql(&mut executor, "CREATE TABLE users (id INTEGER, name TEXT)");

                    // 单条插入
                    for i in 0..n {
                        let sql = format!("INSERT INTO users VALUES ({}, 'User{}')", i, i);
                        execute_sql(&mut executor, &sql);
                    }

                    black_box(executor);
                });
            },
        );
    }

    group.finish();
}

// ==================== 测试方案 2: 批量插入 ====================
fn bench_batch_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("sqllite_batch_insert");
    group.measurement_time(Duration::from_secs(10));
    group.sample_size(30);

    for batch_size in [100, 1000, 5000].iter() {
        group.bench_with_input(
            BenchmarkId::from_parameter(batch_size),
            batch_size,
            |b, &n| {
                b.iter(|| {
                    let (_temp, db_path) = temp_db_path();
                    let mut executor = Executor::open(&db_path).expect("Failed to open db");

                    // 创建表
                    execute_sql(&mut executor, "CREATE TABLE logs (id INTEGER, msg TEXT)");

                    // 批量插入
                    for i in 0..n {
                        let sql = format!("INSERT INTO logs VALUES ({}, 'Log message {}')", i, i);
                        execute_sql(&mut executor, &sql);
                    }

                    black_box(executor);
                });
            },
        );
    }

    group.finish();
}

// ==================== 测试方案 3: 简单查询 ====================
fn bench_simple_select(c: &mut Criterion) {
    let mut group = c.benchmark_group("sqllite_simple_select");
    group.measurement_time(Duration::from_secs(10));
    group.sample_size(30);

    for table_size in [100, 1000, 5000].iter() {
        // 准备数据（不计入基准时间）
        let (_temp, db_path) = temp_db_path();
        {
            let mut executor = Executor::open(&db_path).expect("Failed to open db");
            execute_sql(&mut executor, "CREATE TABLE products (id INTEGER, price INTEGER)");

            for i in 0..*table_size {
                let sql = format!("INSERT INTO products VALUES ({}, {})", i, i % 100);
                execute_sql(&mut executor, &sql);
            }
        }

        group.bench_with_input(
            BenchmarkId::from_parameter(table_size),
            table_size,
            |b, &_n| {
                b.iter(|| {
                    let mut executor = Executor::open(&db_path).expect("Failed to open db");
                    let count = execute_query(&mut executor, "SELECT * FROM products WHERE price > 50");
                    black_box(count);
                });
            },
        );
    }

    group.finish();
}

// ==================== 测试方案 4: 全表扫描 ====================
fn bench_full_table_scan(c: &mut Criterion) {
    let mut group = c.benchmark_group("sqllite_full_scan");
    group.measurement_time(Duration::from_secs(10));

    for table_size in [100, 1000, 5000].iter() {
        // 准备数据
        let (_temp, db_path) = temp_db_path();
        {
            let mut executor = Executor::open(&db_path).expect("Failed to open db");
            execute_sql(&mut executor, "CREATE TABLE items (id INTEGER, data TEXT)");

            for i in 0..*table_size {
                let sql = format!("INSERT INTO items VALUES ({}, 'Data item number {}')", i, i);
                execute_sql(&mut executor, &sql);
            }
        }

        group.bench_with_input(
            BenchmarkId::from_parameter(table_size),
            table_size,
            |b, &_n| {
                b.iter(|| {
                    let mut executor = Executor::open(&db_path).expect("Failed to open db");
                    let count = execute_query(&mut executor, "SELECT * FROM items");
                    black_box(count);
                });
            },
        );
    }

    group.finish();
}

// ==================== 测试方案 5: 解析性能 ====================
fn bench_sql_parsing(c: &mut Criterion) {
    let mut group = c.benchmark_group("sqllite_sql_parse");

    let sql_statements = vec![
        ("simple_select", "SELECT * FROM users WHERE id = 1"),
        ("complex_select", "SELECT id, name FROM users WHERE id > 10 AND name = 'Alice'"),
        ("insert", "INSERT INTO users VALUES (1, 'Alice')"),
        ("create_table", "CREATE TABLE test (id INTEGER, name TEXT)"),
    ];

    for (name, sql) in sql_statements {
        group.bench_with_input(
            BenchmarkId::new("parse", name),
            &sql,
            |b, &sql| {
                b.iter(|| {
                    let mut parser = Parser::new(sql).expect("Tokenizer failed");
                    let stmt = parser.parse().expect("Parse failed");
                    black_box(stmt);
                });
            },
        );
    }

    group.finish();
}

// ==================== 测试方案 6: 建表性能 ====================
fn bench_create_table(c: &mut Criterion) {
    let mut group = c.benchmark_group("sqllite_create_table");

    group.bench_function("simple_table", |b| {
        b.iter(|| {
            let (_temp, db_path) = temp_db_path();
            let mut executor = Executor::open(&db_path).expect("Failed to open db");
            execute_sql(&mut executor, "CREATE TABLE test (id INTEGER, name TEXT)");
            black_box(executor);
        });
    });

    group.finish();
}

// ==================== 测试方案 7: 混合读写 ====================
fn bench_mixed_workload(c: &mut Criterion) {
    let mut group = c.benchmark_group("sqllite_mixed");
    group.measurement_time(Duration::from_secs(10));

    for read_ratio in [0.8, 0.5, 0.2].iter() {
        group.bench_with_input(
            BenchmarkId::new("read_ratio", read_ratio),
            read_ratio,
            |b, &ratio| {
                b.iter(|| {
                    let (_temp, db_path) = temp_db_path();
                    let mut executor = Executor::open(&db_path).expect("Failed to open db");

                    // 初始数据
                    execute_sql(&mut executor, "CREATE TABLE data (id INTEGER, value INTEGER)");
                    for i in 0..100 {
                        let sql = format!("INSERT INTO data VALUES ({}, {})", i, i);
                        execute_sql(&mut executor, &sql);
                    }

                    // 混合操作
                    for i in 0..50 {
                        if i as f64 / 50.0 < ratio {
                            // 读操作
                            let sql = format!("SELECT * FROM data WHERE id = {}", i % 100);
                            let _ = execute_query(&mut executor, &sql);
                        } else {
                            // 写操作
                            let sql = format!("INSERT INTO data VALUES ({}, {})", i + 100, i);
                            execute_sql(&mut executor, &sql);
                        }
                    }

                    black_box(executor);
                });
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
    bench_full_table_scan,
    bench_sql_parsing,
    bench_create_table,
    bench_mixed_workload
);
criterion_main!(benches);
