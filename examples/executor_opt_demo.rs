//! Demo of executor optimizations performance
//! 
//! Run with: cargo run --example executor_opt_demo --release

use sqllite_rust::executor::Executor;
use std::time::Instant;

fn main() {
    println!("=== Executor Optimization Demo ===\n");

    // Create temporary database
    let temp_dir = tempfile::tempdir().unwrap();
    let path = temp_dir.path().join("test.db");
    
    // ===== Test 1: WHERE Condition Pushdown =====
    println!("Test 1: WHERE Condition Pushdown");
    println!("-----------------------------------");
    
    let mut executor = Executor::open(path.to_str().unwrap()).unwrap();
    executor.execute_sql("CREATE TABLE test (id INTEGER, value INTEGER)").unwrap();
    
    // Insert 1000 records
    println!("Inserting 1000 records...");
    for i in 1..=1000 {
        executor.execute_sql(&format!("INSERT INTO test VALUES ({}, {})", i, i * 10)).unwrap();
    }
    
    // Test with pushdown enabled
    executor.enable_predicate_pushdown();
    executor.reset_pushdown_stats();
    
    let start = Instant::now();
    let result = executor.execute_sql("SELECT * FROM test WHERE value > 5000").unwrap();
    let elapsed_with = start.elapsed();
    
    let stats = executor.pushdown_stats();
    println!("With pushdown:");
    println!("  Time: {:?}", elapsed_with);
    println!("  Records scanned: {}", stats.records_scanned);
    println!("  Records filtered: {}", stats.records_filtered);
    if stats.records_scanned > 0 {
        println!("  Selectivity: {:.2}%", stats.selectivity() * 100.0);
    }
    
    match &result {
        sqllite_rust::executor::ExecuteResult::Query(q) => {
            println!("  Rows returned: {}", q.rows.len());
        }
        _ => {}
    }
    
    // Test with pushdown disabled
    executor.disable_predicate_pushdown();
    executor.reset_pushdown_stats();
    
    let start = Instant::now();
    let result = executor.execute_sql("SELECT * FROM test WHERE value > 5000").unwrap();
    let elapsed_without = start.elapsed();
    
    println!("\nWithout pushdown:");
    println!("  Time: {:?}", elapsed_without);
    
    match &result {
        sqllite_rust::executor::ExecuteResult::Query(q) => {
            println!("  Rows returned: {}", q.rows.len());
        }
        _ => {}
    }
    
    // Calculate speedup
    if elapsed_without > elapsed_with {
        let speedup = elapsed_without.as_nanos() as f64 / elapsed_with.as_nanos() as f64;
        println!("\n  Speedup: {:.2}x faster with pushdown", speedup);
    }

    // ===== Test 2: Expression Cache =====
    println!("\n\nTest 2: Expression Cache");
    println!("-------------------------");
    
    // Clear and rebuild table for clean test
    executor.execute_sql("DROP TABLE test").unwrap();
    executor.execute_sql("CREATE TABLE test (id INTEGER, value INTEGER)").unwrap();
    
    for i in 1..=1000 {
        executor.execute_sql(&format!("INSERT INTO test VALUES ({}, {})", i, i)).unwrap();
    }
    
    // Enable cache
    executor.enable_expression_cache();
    executor.clear_expression_cache();
    
    println!("Running query with constant expression 5 times...");
    
    // Run same query multiple times
    for i in 1..=5 {
        let start = Instant::now();
        let _ = executor.execute_sql("SELECT value * 2 + 100 - 50 FROM test WHERE id < 100").unwrap();
        let elapsed = start.elapsed();
        
        let stats = executor.expression_cache_stats();
        println!("  Run {}: {:?} | Cache hits: {}, misses: {}, hit rate: {:.1}%", 
            i, elapsed, stats.hit_count, stats.miss_count, stats.hit_rate());
    }
    
    // Test without cache
    executor.disable_expression_cache();
    executor.clear_expression_cache();
    
    println!("\nRunning same query with cache disabled...");
    let start = Instant::now();
    let _ = executor.execute_sql("SELECT value * 2 + 100 - 50 FROM test WHERE id < 100").unwrap();
    let elapsed_no_cache = start.elapsed();
    println!("  Time without cache: {:?}", elapsed_no_cache);
    
    println!("\n=== Demo Complete ===");
}
