//! Integration tests for executor optimizations:
//! - P8-4: Expression Evaluation Cache
//! - P8-5: WHERE Clause Predicate Pushdown

use sqllite_rust::sql::Parser;
use sqllite_rust::executor::Executor;
use tempfile::NamedTempFile;

#[test]
fn test_expression_cache_basic() {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap();

    let mut executor = Executor::open(path).unwrap();

    // Create table and insert data
    executor.execute_sql("CREATE TABLE users (id INTEGER, salary INTEGER)").unwrap();
    executor.execute_sql("INSERT INTO users VALUES (1, 50000)").unwrap();
    executor.execute_sql("INSERT INTO users VALUES (2, 60000)").unwrap();
    executor.execute_sql("INSERT INTO users VALUES (3, 70000)").unwrap();

    // Clear cache to start fresh
    executor.clear_expression_cache();

    // Execute query with expression evaluation
    let result = executor.execute_sql("SELECT salary * 1.1 FROM users WHERE id = 1").unwrap();
    
    // Check cache stats
    let stats = executor.expression_cache_stats();
    println!("Expression cache stats after first query: {:?}", stats);
    
    // Execute same expression again (should hit cache for constant expressions)
    let result2 = executor.execute_sql("SELECT salary * 1.1 FROM users WHERE id = 2").unwrap();
    
    let stats2 = executor.expression_cache_stats();
    println!("Expression cache stats after second query: {:?}", stats2);

    // Verify results are correct
    match result {
        sqllite_rust::executor::ExecuteResult::Query(query_result) => {
            assert_eq!(query_result.rows.len(), 1);
        }
        _ => panic!("Expected Query result"),
    }
}

#[test]
fn test_predicate_pushdown_filtering() {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap();

    let mut executor = Executor::open(path).unwrap();

    // Create table with more data
    executor.execute_sql("CREATE TABLE employees (id INTEGER, age INTEGER, department TEXT)").unwrap();
    
    // Insert 100 records
    for i in 1..=100 {
        let age = 20 + (i % 50); // Ages 21-70
        let dept = if i % 3 == 0 { "Engineering" } else { "Sales" };
        executor.execute_sql(&format!("INSERT INTO employees VALUES ({}, {}, '{}')", i, age, dept)).unwrap();
    }

    // Reset pushdown stats
    executor.reset_pushdown_stats();

    // Execute query with simple WHERE clause (should use pushdown)
    let result = executor.execute_sql("SELECT * FROM employees WHERE age > 50").unwrap();

    // Check pushdown stats
    let stats = executor.pushdown_stats();
    println!("Pushdown stats: {:?}", stats);
    println!("Records scanned: {}, filtered: {}", stats.records_scanned, stats.records_filtered);
    
    // Verify results
    match result {
        sqllite_rust::executor::ExecuteResult::Query(query_result) => {
            // Should have filtered out some records
            assert!(query_result.rows.len() < 100, "Expected filtering to reduce result count");
            
            // All returned records should have age > 50
            for row in &query_result.rows {
                let age = match &row.values[1] {
                    sqllite_rust::storage::Value::Integer(n) => *n,
                    _ => panic!("Expected Integer age"),
                };
                assert!(age > 50, "Filtered record should have age > 50");
            }
        }
        _ => panic!("Expected Query result"),
    }
}

#[test]
fn test_predicate_pushdown_with_and() {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap();

    let mut executor = Executor::open(path).unwrap();

    executor.execute_sql("CREATE TABLE products (id INTEGER, price INTEGER, category TEXT)").unwrap();
    
    // Insert test data
    executor.execute_sql("INSERT INTO products VALUES (1, 100, 'Electronics')").unwrap();
    executor.execute_sql("INSERT INTO products VALUES (2, 50, 'Books')").unwrap();
    executor.execute_sql("INSERT INTO products VALUES (3, 200, 'Electronics')").unwrap();
    executor.execute_sql("INSERT INTO products VALUES (4, 30, 'Books')").unwrap();
    executor.execute_sql("INSERT INTO products VALUES (5, 150, 'Clothing')").unwrap();

    // Query with AND condition
    let result = executor.execute_sql("SELECT * FROM products WHERE price > 75 AND category = 'Electronics'").unwrap();

    match result {
        sqllite_rust::executor::ExecuteResult::Query(query_result) => {
            // Should return only products with price > 75 AND category = 'Electronics'
            assert_eq!(query_result.rows.len(), 2); // id 1 and 3
            
            for row in &query_result.rows {
                let price = match &row.values[1] {
                    sqllite_rust::storage::Value::Integer(n) => *n,
                    _ => 0,
                };
                let category = match &row.values[2] {
                    sqllite_rust::storage::Value::Text(s) => s.as_str(),
                    _ => "",
                };
                assert!(price > 75, "Price should be > 75");
                assert_eq!(category, "Electronics", "Category should be Electronics");
            }
        }
        _ => panic!("Expected Query result"),
    }
}

#[test]
fn test_compare_select_with_and_without_pushdown() {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap();

    let mut executor = Executor::open(path).unwrap();

    executor.execute_sql("CREATE TABLE large_table (id INTEGER, value INTEGER)").unwrap();
    
    // Insert 1000 records
    for i in 1..=1000 {
        executor.execute_sql(&format!("INSERT INTO large_table VALUES ({}, {})", i, i * 10)).unwrap();
    }

    // Test with pushdown enabled (default)
    executor.enable_predicate_pushdown();
    executor.reset_pushdown_stats();
    
    let start = std::time::Instant::now();
    let result_with_pushdown = executor.execute_sql("SELECT * FROM large_table WHERE value > 5000").unwrap();
    let elapsed_with = start.elapsed();

    let stats_with = executor.pushdown_stats();
    
    // Test with pushdown disabled
    executor.disable_predicate_pushdown();
    
    let start = std::time::Instant::now();
    let result_without = executor.execute_sql("SELECT * FROM large_table WHERE value > 5000").unwrap();
    let elapsed_without = start.elapsed();

    // Both should return the same results
    match (&result_with_pushdown, &result_without) {
        (
            sqllite_rust::executor::ExecuteResult::Query(r1),
            sqllite_rust::executor::ExecuteResult::Query(r2)
        ) => {
            assert_eq!(r1.rows.len(), r2.rows.len(), "Results should match regardless of pushdown");
            println!("With pushdown: {} rows in {:?}", r1.rows.len(), elapsed_with);
            println!("Without pushdown: {} rows in {:?}", r2.rows.len(), elapsed_without);
            println!("Pushdown stats: {} scanned, {} filtered", stats_with.records_scanned, stats_with.records_filtered);
        }
        _ => panic!("Expected Query results"),
    }
}

#[test]
fn test_expression_cache_hit_rate() {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap();

    let mut executor = Executor::open(path).unwrap();

    executor.execute_sql("CREATE TABLE test (id INTEGER, value INTEGER)").unwrap();
    
    // Insert data
    for i in 1..=10 {
        executor.execute_sql(&format!("INSERT INTO test VALUES ({}, {})", i, i * 100)).unwrap();
    }

    // Clear cache
    executor.clear_expression_cache();

    // Execute queries that use the same constant expression multiple times
    for _ in 0..5 {
        let _ = executor.execute_sql("SELECT value + 1000 FROM test WHERE id < 5").unwrap();
    }

    // Check cache hit rate
    let stats = executor.expression_cache_stats();
    println!("Cache stats after repeated queries: {:?}", stats);
    println!("Hit rate: {:.2}%", stats.hit_rate());
    
    // With repeated execution of the same query, we should see some cache hits
    // for constant expressions (like 1000 in the projection)
}
