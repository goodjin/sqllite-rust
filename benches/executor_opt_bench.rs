//! Benchmark for executor optimizations
//! Run with: cargo bench --bench executor_opt_bench

use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};
use sqllite_rust::executor::Executor;
use std::time::Duration;

fn bench_select_with_filter(c: &mut Criterion) {
    let mut group = c.benchmark_group("select_with_filter");
    group.measurement_time(Duration::from_secs(5));
    
    // Test with different table sizes
    for size in [100, 1000, 10000].iter() {
        group.bench_with_input(BenchmarkId::new("with_pushdown", size), size, |b, &size| {
            // Setup
            let temp_dir = tempfile::tempdir().unwrap();
            let path = temp_dir.path().join("test.db");
            let mut executor = Executor::open(path.to_str().unwrap()).unwrap();
            
            executor.execute_sql("CREATE TABLE test (id INTEGER, value INTEGER)").unwrap();
            
            // Insert data
            for i in 1..=size {
                executor.execute_sql(&format!("INSERT INTO test VALUES ({}, {})", i, i * 10)).unwrap();
            }
            
            executor.enable_predicate_pushdown();
            
            b.iter(|| {
                let result = executor.execute_sql("SELECT * FROM test WHERE value > 5000").unwrap();
                black_box(result);
            });
        });
        
        group.bench_with_input(BenchmarkId::new("without_pushdown", size), size, |b, &size| {
            // Setup
            let temp_dir = tempfile::tempdir().unwrap();
            let path = temp_dir.path().join("test.db");
            let mut executor = Executor::open(path.to_str().unwrap()).unwrap();
            
            executor.execute_sql("CREATE TABLE test (id INTEGER, value INTEGER)").unwrap();
            
            // Insert data
            for i in 1..=size {
                executor.execute_sql(&format!("INSERT INTO test VALUES ({}, {})", i, i * 10)).unwrap();
            }
            
            executor.disable_predicate_pushdown();
            
            b.iter(|| {
                let result = executor.execute_sql("SELECT * FROM test WHERE value > 5000").unwrap();
                black_box(result);
            });
        });
    }
    
    group.finish();
}

fn bench_expression_evaluation(c: &mut Criterion) {
    let mut group = c.benchmark_group("expression_evaluation");
    group.measurement_time(Duration::from_secs(3));
    
    group.bench_function("with_cache", |b| {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("test.db");
        let mut executor = Executor::open(path.to_str().unwrap()).unwrap();
        
        executor.execute_sql("CREATE TABLE test (id INTEGER, value INTEGER)").unwrap();
        
        // Insert some data
        for i in 1..=100 {
            executor.execute_sql(&format!("INSERT INTO test VALUES ({}, {})", i, i)).unwrap();
        }
        
        executor.enable_expression_cache();
        executor.clear_expression_cache();
        
        b.iter(|| {
            // Query with repeated constant expression
            let result = executor.execute_sql("SELECT value * 2 + 100 FROM test WHERE id < 50").unwrap();
            black_box(result);
        });
    });
    
    group.bench_function("without_cache", |b| {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("test.db");
        let mut executor = Executor::open(path.to_str().unwrap()).unwrap();
        
        executor.execute_sql("CREATE TABLE test (id INTEGER, value INTEGER)").unwrap();
        
        // Insert some data
        for i in 1..=100 {
            executor.execute_sql(&format!("INSERT INTO test VALUES ({}, {})", i, i)).unwrap();
        }
        
        executor.disable_expression_cache();
        
        b.iter(|| {
            let result = executor.execute_sql("SELECT value * 2 + 100 FROM test WHERE id < 50").unwrap();
            black_box(result);
        });
    });
    
    group.finish();
}

criterion_group!(benches, bench_select_with_filter, bench_expression_evaluation);
criterion_main!(benches);
