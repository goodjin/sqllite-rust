use sqllite_rust::sql::Parser;
use sqllite_rust::executor::Executor;
use tempfile::TempDir;
use std::time::Instant;

fn main() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");
    let path = db_path.to_str().unwrap();
    
    println!("=== WAL Performance Test ===");
    println!("Database: {}", path);
    
    let mut executor = Executor::open(path).expect("Failed to open db");
    
    // Create table
    let sql = "CREATE TABLE users (id INTEGER, name TEXT)";
    let mut parser = Parser::new(sql).expect("Tokenizer failed");
    let stmt = parser.parse().expect("Parse failed");
    executor.execute(&stmt).expect("Create table failed");
    println!("Table created");
    
    // Insert 100 records with timing
    let start = Instant::now();
    for i in 0..100 {
        let sql = format!("INSERT INTO users VALUES ({}, 'User{:.20}')", i, i);
        let mut parser = Parser::new(&sql).expect("Tokenizer failed");
        let stmt = parser.parse().expect("Parse failed");
        executor.execute(&stmt).expect("Insert failed");
    }
    let duration = start.elapsed();
    println!("Inserted 100 records in {:?}", duration);
    println!("  Average: {:.2} ms/insert", duration.as_millis() as f64 / 100.0);
    
    // Verify
    let mut parser = Parser::new("SELECT COUNT(*) FROM users").unwrap();
    let stmt = parser.parse().unwrap();
    if let Ok(sqllite_rust::executor::ExecuteResult::Query(result)) = executor.execute(&stmt) {
        println!("  Total records: {}", result.rows.len());
    }
}
