//! Phase 1 Week 1: Comprehensive Performance Benchmark Suite
//!
//! This benchmark suite covers:
//! - Point queries (indexed and non-indexed)
//! - Range queries (indexed and non-indexed)
//! - Bulk insert performance
//! - Concurrent read performance
//! - Index covering scan performance
//! - Aggregation queries

use criterion::{
    criterion_group, criterion_main, Criterion, BenchmarkId, black_box, Throughput,
};
use std::time::Duration;
use tempfile::TempDir;

use sqllite_rust::executor::{Executor, ExecuteResult};

/// Helper: Create a temporary database path
fn temp_db_path() -> (TempDir, String) {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("bench.db");
    (temp_dir, db_path.to_str().unwrap().to_string())
}

/// Helper: Execute SQL
fn execute_sql(executor: &mut Executor, sql: &str) {
    executor.execute_sql(sql).expect("SQL execution failed");
}

/// Helper: Execute query and return row count
fn execute_query(executor: &mut Executor, sql: &str) -> usize {
    match executor.execute_sql(sql) {
        Ok(ExecuteResult::Query(result)) => result.rows.len(),
        Ok(_) => 0,
        Err(e) => panic!("Query failed: {}", e),
    }
}

// ============================================================================
// Benchmark 1: Point Select (Indexed)
// ============================================================================

fn bench_point_select_indexed(c: &mut Criterion) {
    let mut group = c.benchmark_group("point_select_indexed");
    group.measurement_time(Duration::from_secs(10));
    group.sample_size(100);

    for size in [1_000, 10_000, 100_000].iter() {
        group.throughput(Throughput::Elements(1));
        
        group.bench_with_input(
            BenchmarkId::from_parameter(size),
            size,
            |b, &n| {
                let (_temp, db_path) = temp_db_path();
                
                // Setup
                {
                    let mut executor = Executor::open(&db_path).unwrap();
                    execute_sql(&mut executor, "CREATE TABLE users (id INTEGER, email TEXT, age INTEGER)");
                    execute_sql(&mut executor, "CREATE INDEX idx_email ON users(email)");
                    
                    for i in 0..n {
                        let sql = format!(
                            "INSERT INTO users VALUES ({}, 'user{}@example.com', {})",
                            i, i, i % 100
                        );
                        execute_sql(&mut executor, &sql);
                    }
                }
                
                // Benchmark
                b.iter(|| {
                    let mut executor = Executor::open(&db_path).unwrap();
                    let target = format!("user{}@example.com", n / 2);
                    let count = execute_query(&mut executor, 
                        &format!("SELECT * FROM users WHERE email = '{}'", target));
                    black_box(count);
                });
            },
        );
    }

    group.finish();
}

// ============================================================================
// Benchmark 2: Point Select (Non-Indexed)
// ============================================================================

fn bench_point_select_no_index(c: &mut Criterion) {
    let mut group = c.benchmark_group("point_select_no_index");
    group.measurement_time(Duration::from_secs(10));
    group.sample_size(50);

    for size in [1_000, 10_000].iter() {
        group.throughput(Throughput::Elements(1));
        
        group.bench_with_input(
            BenchmarkId::from_parameter(size),
            size,
            |b, &n| {
                let (_temp, db_path) = temp_db_path();
                
                // Setup
                {
                    let mut executor = Executor::open(&db_path).unwrap();
                    execute_sql(&mut executor, "CREATE TABLE users (id INTEGER, email TEXT, age INTEGER)");
                    
                    for i in 0..n {
                        let sql = format!(
                            "INSERT INTO users VALUES ({}, 'user{}@example.com', {})",
                            i, i, i % 100
                        );
                        execute_sql(&mut executor, &sql);
                    }
                }
                
                // Benchmark
                b.iter(|| {
                    let mut executor = Executor::open(&db_path).unwrap();
                    let target = format!("user{}@example.com", n / 2);
                    let count = execute_query(&mut executor, 
                        &format!("SELECT * FROM users WHERE email = '{}'", target));
                    black_box(count);
                });
            },
        );
    }

    group.finish();
}

// ============================================================================
// Benchmark 3: Range Scan (Indexed)
// ============================================================================

fn bench_range_scan_indexed(c: &mut Criterion) {
    let mut group = c.benchmark_group("range_scan_indexed");
    group.measurement_time(Duration::from_secs(10));
    group.sample_size(30);

    for size in [10_000, 100_000].iter() {
        group.throughput(Throughput::Elements(1000));
        
        group.bench_with_input(
            BenchmarkId::from_parameter(size),
            size,
            |b, &n| {
                let (_temp, db_path) = temp_db_path();
                
                // Setup
                {
                    let mut executor = Executor::open(&db_path).unwrap();
                    execute_sql(&mut executor, "CREATE TABLE users (id INTEGER, age INTEGER, name TEXT)");
                    execute_sql(&mut executor, "CREATE INDEX idx_age ON users(age)");
                    
                    for i in 0..n {
                        let sql = format!(
                            "INSERT INTO users VALUES ({}, {}, 'User{}')",
                            i, i % 100, i
                        );
                        execute_sql(&mut executor, &sql);
                    }
                }
                
                // Benchmark
                b.iter(|| {
                    let mut executor = Executor::open(&db_path).unwrap();
                    let count = execute_query(&mut executor, 
                        "SELECT * FROM users WHERE age BETWEEN 20 AND 30");
                    black_box(count);
                });
            },
        );
    }

    group.finish();
}

// ============================================================================
// Benchmark 4: Covering Index Scan
// ============================================================================

fn bench_covering_index_scan(c: &mut Criterion) {
    let mut group = c.benchmark_group("covering_index_scan");
    group.measurement_time(Duration::from_secs(10));
    group.sample_size(50);

    for size in [10_000, 100_000].iter() {
        group.throughput(Throughput::Elements(100));
        
        group.bench_with_input(
            BenchmarkId::from_parameter(size),
            size,
            |b, &n| {
                let (_temp, db_path) = temp_db_path();
                
                // Setup - covering index on email only
                {
                    let mut executor = Executor::open(&db_path).unwrap();
                    execute_sql(&mut executor, "CREATE TABLE users (id INTEGER, email TEXT, age INTEGER)");
                    execute_sql(&mut executor, "CREATE INDEX idx_email ON users(email)");
                    
                    for i in 0..n {
                        let sql = format!(
                            "INSERT INTO users VALUES ({}, 'user{}@example.com', {})",
                            i, i, i % 100
                        );
                        execute_sql(&mut executor, &sql);
                    }
                }
                
                // Benchmark - SELECT email only (covering index)
                b.iter(|| {
                    let mut executor = Executor::open(&db_path).unwrap();
                    let count = execute_query(&mut executor, 
                        "SELECT email FROM users WHERE email LIKE 'user1%'");
                    black_box(count);
                });
            },
        );
    }

    group.finish();
}

// ============================================================================
// Benchmark 5: Bulk Insert with Batch
// ============================================================================

fn bench_bulk_insert_batch(c: &mut Criterion) {
    let mut group = c.benchmark_group("bulk_insert_batch");
    group.measurement_time(Duration::from_secs(15));
    group.sample_size(10);

    for size in [1_000, 10_000].iter() {
        group.throughput(Throughput::Elements(*size as u64));
        
        group.bench_with_input(
            BenchmarkId::from_parameter(size),
            size,
            |b, &n| {
                b.iter(|| {
                    let (_temp, db_path) = temp_db_path();
                    let mut executor = Executor::open(&db_path).unwrap();
                    
                    execute_sql(&mut executor, "CREATE TABLE logs (id INTEGER, message TEXT, timestamp INTEGER)");
                    execute_sql(&mut executor, "BEGIN");
                    
                    for i in 0..n {
                        let sql = format!(
                            "INSERT INTO logs VALUES ({}, 'Log message number {}', {})",
                            i, i, i
                        );
                        execute_sql(&mut executor, &sql);
                    }
                    
                    execute_sql(&mut executor, "COMMIT");
                    black_box(executor);
                });
            },
        );
    }

    group.finish();
}

// ============================================================================
// Benchmark 6: Aggregation Queries
// ============================================================================

fn bench_aggregation(c: &mut Criterion) {
    let mut group = c.benchmark_group("aggregation");
    group.measurement_time(Duration::from_secs(10));
    group.sample_size(30);

    for size in [10_000, 100_000].iter() {
        group.throughput(Throughput::Elements(*size as u64));
        
        group.bench_with_input(
            BenchmarkId::from_parameter(size),
            size,
            |b, &n| {
                let (_temp, db_path) = temp_db_path();
                
                // Setup
                {
                    let mut executor = Executor::open(&db_path).unwrap();
                    execute_sql(&mut executor, "CREATE TABLE sales (id INTEGER, region TEXT, amount INTEGER)");
                    
                    let regions = ["North", "South", "East", "West"];
                    for i in 0..n {
                        let sql = format!(
                            "INSERT INTO sales VALUES ({}, '{}', {})",
                            i, regions[i % 4], i % 1000
                        );
                        execute_sql(&mut executor, &sql);
                    }
                }
                
                // Benchmark
                b.iter(|| {
                    let mut executor = Executor::open(&db_path).unwrap();
                    let count = execute_query(&mut executor, 
                        "SELECT region, COUNT(*), SUM(amount), AVG(amount), MAX(amount), MIN(amount) FROM sales GROUP BY region");
                    black_box(count);
                });
            },
        );
    }

    group.finish();
}

// ============================================================================
// Benchmark 7: Full Table Scan
// ============================================================================

fn bench_full_table_scan(c: &mut Criterion) {
    let mut group = c.benchmark_group("full_table_scan");
    group.measurement_time(Duration::from_secs(10));
    group.sample_size(20);

    for size in [10_000, 100_000].iter() {
        group.throughput(Throughput::Elements(*size as u64));
        
        group.bench_with_input(
            BenchmarkId::from_parameter(size),
            size,
            |b, &n| {
                let (_temp, db_path) = temp_db_path();
                
                // Setup
                {
                    let mut executor = Executor::open(&db_path).unwrap();
                    execute_sql(&mut executor, "CREATE TABLE users (id INTEGER, name TEXT, email TEXT)");
                    
                    for i in 0..n {
                        let sql = format!(
                            "INSERT INTO users VALUES ({}, 'User{}', 'user{}@example.com')",
                            i, i, i
                        );
                        execute_sql(&mut executor, &sql);
                    }
                }
                
                // Benchmark
                b.iter(|| {
                    let mut executor = Executor::open(&db_path).unwrap();
                    let count = execute_query(&mut executor, "SELECT * FROM users");
                    black_box(count);
                });
            },
        );
    }

    group.finish();
}

// ============================================================================
// Benchmark 8: COUNT(*) Optimization
// ============================================================================

fn bench_count_star(c: &mut Criterion) {
    let mut group = c.benchmark_group("count_star");
    group.measurement_time(Duration::from_secs(10));
    group.sample_size(50);

    for size in [10_000, 100_000].iter() {
        group.throughput(Throughput::Elements(*size as u64));
        
        group.bench_with_input(
            BenchmarkId::from_parameter(size),
            size,
            |b, &n| {
                let (_temp, db_path) = temp_db_path();
                
                // Setup
                {
                    let mut executor = Executor::open(&db_path).unwrap();
                    execute_sql(&mut executor, "CREATE TABLE users (id INTEGER, name TEXT)");
                    
                    for i in 0..n {
                        let sql = format!("INSERT INTO users VALUES ({}, 'User{}')", i, i);
                        execute_sql(&mut executor, &sql);
                    }
                }
                
                // Benchmark
                b.iter(|| {
                    let mut executor = Executor::open(&db_path).unwrap();
                    let count = execute_query(&mut executor, "SELECT COUNT(*) FROM users");
                    black_box(count);
                });
            },
        );
    }

    group.finish();
}

// ============================================================================
// Benchmark 9: LIKE Query Performance
// ============================================================================

fn bench_like_query(c: &mut Criterion) {
    let mut group = c.benchmark_group("like_query");
    group.measurement_time(Duration::from_secs(10));
    group.sample_size(20);

    for size in [10_000].iter() {
        group.throughput(Throughput::Elements(*size as u64));
        
        group.bench_with_input(
            BenchmarkId::from_parameter(size),
            size,
            |b, &n| {
                let (_temp, db_path) = temp_db_path();
                
                // Setup
                {
                    let mut executor = Executor::open(&db_path).unwrap();
                    execute_sql(&mut executor, "CREATE TABLE users (id INTEGER, name TEXT)");
                    
                    for i in 0..n {
                        let sql = format!("INSERT INTO users VALUES ({}, 'User{}')", i, i);
                        execute_sql(&mut executor, &sql);
                    }
                }
                
                // Benchmark
                b.iter(|| {
                    let mut executor = Executor::open(&db_path).unwrap();
                    let count = execute_query(&mut executor, "SELECT * FROM users WHERE name LIKE 'User1%'");
                    black_box(count);
                });
            },
        );
    }

    group.finish();
}

// ============================================================================
// Benchmark 10: Join Performance
// ============================================================================

fn bench_join(c: &mut Criterion) {
    let mut group = c.benchmark_group("join");
    group.measurement_time(Duration::from_secs(10));
    group.sample_size(20);

    for size in [1_000, 10_000].iter() {
        group.throughput(Throughput::Elements(*size as u64));
        
        group.bench_with_input(
            BenchmarkId::from_parameter(size),
            size,
            |b, &n| {
                let (_temp, db_path) = temp_db_path();
                
                // Setup
                {
                    let mut executor = Executor::open(&db_path).unwrap();
                    execute_sql(&mut executor, "CREATE TABLE users (id INTEGER, name TEXT)");
                    execute_sql(&mut executor, "CREATE TABLE orders (id INTEGER, user_id INTEGER, amount INTEGER)");
                    
                    for i in 0..n {
                        let sql = format!("INSERT INTO users VALUES ({}, 'User{}')", i, i);
                        execute_sql(&mut executor, &sql);
                    }
                    
                    for i in 0..(n * 5) {
                        let sql = format!("INSERT INTO orders VALUES ({}, {}, {})", i, i % n, i % 100);
                        execute_sql(&mut executor, &sql);
                    }
                }
                
                // Benchmark
                b.iter(|| {
                    let mut executor = Executor::open(&db_path).unwrap();
                    let count = execute_query(&mut executor, 
                        "SELECT u.*, o.amount FROM users u JOIN orders o ON u.id = o.user_id LIMIT 100");
                    black_box(count);
                });
            },
        );
    }

    group.finish();
}

// ============================================================================
// Benchmark Groups
// ============================================================================

criterion_group!(
    benches,
    bench_point_select_indexed,
    bench_point_select_no_index,
    bench_range_scan_indexed,
    bench_covering_index_scan,
    bench_bulk_insert_batch,
    bench_aggregation,
    bench_full_table_scan,
    bench_count_star,
    bench_like_query,
    bench_join,
);

criterion_main!(benches);
