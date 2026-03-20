use sqllite_rust::executor::{Executor, ExecuteResult};
use std::fs;

fn main() -> anyhow::Result<()> {
    let db_path = "basic_usage.db";
    
    // Clean up any existing database file
    let _ = fs::remove_file(db_path);
    
    println!("--- SQLite Rust Demo: Basic Usage ---");
    
    // 1. Open or create a database
    println!("\n1. Opening database: {}", db_path);
    let mut executor = Executor::open(db_path).map_err(|e| anyhow::anyhow!(e))?;
    
    // 2. Create a table
    println!("2. Creating table 'users'...");
    let create_table_sql = "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER, email TEXT)";
    executor.execute_sql(create_table_sql).map_err(|e| anyhow::anyhow!(e))?;
    
    // 3. Insert some data
    println!("3. Inserting data...");
    let insert_sqls = vec![
        "INSERT INTO users VALUES (1, 'Alice', 30, 'alice@example.com')",
        "INSERT INTO users VALUES (2, 'Bob', 25, 'bob@example.com')",
        "INSERT INTO users VALUES (3, 'Charlie', 35, 'charlie@example.com')",
        "INSERT INTO users VALUES (4, 'David', 28, 'david@example.com')",
    ];
    
    for sql in insert_sqls {
        executor.execute_sql(sql).map_err(|e| anyhow::anyhow!(e))?;
    }
    
    // 4. Query data
    println!("4. Querying all users:");
    let select_all_sql = "SELECT * FROM users";
    let result = executor.execute_sql(select_all_sql).map_err(|e| anyhow::anyhow!(e))?;
    if let ExecuteResult::Query(query_result) = result {
        query_result.print();
    }
    
    // 5. Query with WHERE clause
    println!("\n5. Querying users where age > 29:");
    let select_where_sql = "SELECT name, age FROM users WHERE age > 29";
    let result = executor.execute_sql(select_where_sql).map_err(|e| anyhow::anyhow!(e))?;
    if let ExecuteResult::Query(query_result) = result {
        query_result.print();
    }
    
    // 6. Update data
    println!("\n6. Updating Bob's age to 26...");
    let update_sql = "UPDATE users SET age = 26 WHERE name = 'Bob'";
    executor.execute_sql(update_sql).map_err(|e| anyhow::anyhow!(e))?;
    
    // Verify update
    println!("Verifying update for Bob:");
    let verify_update_sql = "SELECT name, age FROM users WHERE name = 'Bob'";
    let result = executor.execute_sql(verify_update_sql).map_err(|e| anyhow::anyhow!(e))?;
    if let ExecuteResult::Query(query_result) = result {
        query_result.print();
    }
    
    // 7. Delete data
    println!("\n7. Deleting Charlie...");
    let delete_sql = "DELETE FROM users WHERE name = 'Charlie'";
    executor.execute_sql(delete_sql).map_err(|e| anyhow::anyhow!(e))?;
    
    // Verify delete
    println!("Verifying deletion (remaining users):");
    let result = executor.execute_sql("SELECT COUNT(*) FROM users").map_err(|e| anyhow::anyhow!(e))?;
    if let ExecuteResult::Query(query_result) = result {
        query_result.print();
    }
    
    // 8. Transaction Demo
    println!("\n8. Transaction Demo:");
    executor.execute_sql("BEGIN TRANSACTION").map_err(|e| anyhow::anyhow!(e))?;
    executor.execute_sql("INSERT INTO users VALUES (5, 'Eve', 22, 'eve@example.com')").map_err(|e| anyhow::anyhow!(e))?;
    println!("Inserted Eve inside transaction. Current count:");
    let res = executor.execute_sql("SELECT COUNT(*) FROM users").map_err(|e| anyhow::anyhow!(e))?;
    if let ExecuteResult::Query(qr) = res { qr.print(); }
    
    println!("Rolling back transaction...");
    executor.execute_sql("ROLLBACK").map_err(|e| anyhow::anyhow!(e))?;
    
    println!("Count after rollback:");
    let res = executor.execute_sql("SELECT COUNT(*) FROM users").map_err(|e| anyhow::anyhow!(e))?;
    if let ExecuteResult::Query(qr) = res { qr.print(); }
    
    // 9. Drop table
    println!("\n9. Dropping table 'users'...");
    executor.execute_sql("DROP TABLE users").map_err(|e| anyhow::anyhow!(e))?;
    
    println!("\n--- Demo Complete ---");
    
    // Clean up
    let _ = fs::remove_file(db_path);
    
    Ok(())
}
