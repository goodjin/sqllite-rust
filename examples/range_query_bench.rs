use sqllite_rust::sql::Parser;
use sqllite_rust::executor::Executor;
use tempfile::TempDir;
use std::time::Instant;

fn main() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");
    let path = db_path.to_str().unwrap();

    println!("=== Range Query Performance Test ===");

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

    // Test 1: Range query with index (WHERE id > 400 AND id < 410)
    println!("\n--- Range Query: id > 400 AND id < 410 ---");
    let start = Instant::now();
    let sql = "SELECT * FROM users WHERE id > 400 AND id < 410";
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

    // Test 2: Range query with rowid (WHERE rowid > 400 AND rowid < 410)
    println!("\n--- Range Query: rowid > 400 AND rowid < 410 ---");
    let start = Instant::now();
    let sql = "SELECT * FROM users WHERE rowid > 400 AND rowid < 410";
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

    // Test 3: Larger range with index (WHERE id >= 100 AND id < 200)
    println!("\n--- Range Query: id >= 100 AND id < 200 ---");
    let start = Instant::now();
    let sql = "SELECT * FROM users WHERE id >= 100 AND id < 200";
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

    // Test 4: Large range with rowid (WHERE rowid >= 100 AND rowid < 200)
    println!("\n--- Range Query: rowid >= 100 AND rowid < 200 ---");
    let start = Instant::now();
    let sql = "SELECT * FROM users WHERE rowid >= 100 AND rowid < 200";
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

    // Test 5: Full table scan for comparison
    println!("\n--- Full Table Scan (for comparison) ---");
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
