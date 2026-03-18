use sqllite_rust::sql::Parser;
use sqllite_rust::executor::Executor;
use tempfile::TempDir;
use std::time::Instant;

fn main() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");
    let path = db_path.to_str().unwrap();
    
    println!("=== Index Scan Performance Test ===");
    
    let mut executor = Executor::open(path).expect("Failed to open db");
    
    // Create table
    let sql = "CREATE TABLE users (id INTEGER, name TEXT)";
    let mut parser = Parser::new(sql).expect("Tokenizer failed");
    let stmt = parser.parse().expect("Parse failed");
    executor.execute(&stmt).expect("Create table failed");
    
    // Create index on id column
    let sql = "CREATE INDEX idx_id ON users(id)";
    let mut parser = Parser::new(sql).expect("Tokenizer failed");
    let stmt = parser.parse().expect("Parse failed");
    executor.execute(&stmt).expect("Create index failed");
    println!("Table and index created");
    
    // Insert 1000 records
    println!("Inserting 1000 records...");
    let start = Instant::now();
    for i in 0..1000 {
        let sql = format!("INSERT INTO users VALUES ({}, 'User{:.20}')", i, i);
        let mut parser = Parser::new(&sql).expect("Tokenizer failed");
        let stmt = parser.parse().expect("Parse failed");
        executor.execute(&stmt).expect("Insert failed");
    }
    println!("Insert took: {:?}", start.elapsed());
    
    // Test: Query with WHERE id = N (uses Index Scan)
    println!("\nQuery: SELECT * FROM users WHERE id = 500");
    let start = Instant::now();
    let sql = "SELECT * FROM users WHERE id = 500";
    let mut parser = Parser::new(sql).unwrap();
    let stmt = parser.parse().unwrap();
    let result = executor.execute(&stmt).unwrap();
    let duration = start.elapsed();

    match result {
        sqllite_rust::executor::ExecuteResult::Query(query_result) => {
            println!("  Found {} records", query_result.rows.len());
            println!("  Query time: {:?}", duration);
        }
        _ => println!("  Unexpected result"),
    }
    
    // Test: Query with WHERE rowid = N (uses Rowid Point Scan)
    println!("\nQuery: SELECT * FROM users WHERE rowid = 500");
    let start = Instant::now();
    let sql = "SELECT * FROM users WHERE rowid = 500";
    let mut parser = Parser::new(sql).unwrap();
    let stmt = parser.parse().unwrap();
    let result = executor.execute(&stmt).unwrap();
    let duration = start.elapsed();
    
    match result {
        sqllite_rust::executor::ExecuteResult::Query(query_result) => {
            println!("  Found {} records", query_result.rows.len());
            println!("  Query time: {:?}", duration);
        }
        _ => println!("  Unexpected result"),
    }
    
    // Test: Full table scan (no WHERE)
    println!("\nQuery: SELECT * FROM users (full scan)");
    let start = Instant::now();
    let sql = "SELECT * FROM users";
    let mut parser = Parser::new(sql).unwrap();
    let stmt = parser.parse().unwrap();
    let result = executor.execute(&stmt).unwrap();
    let duration = start.elapsed();
    
    match result {
        sqllite_rust::executor::ExecuteResult::Query(query_result) => {
            println!("  Found {} records", query_result.rows.len());
            println!("  Query time: {:?}", duration);
        }
        _ => println!("  Unexpected result"),
    }
    
    println!("\n=== Test Complete ===");
}
