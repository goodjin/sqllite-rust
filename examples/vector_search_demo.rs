use sqllite_rust::executor::{Executor, ExecuteResult};
use std::fs;

fn main() -> anyhow::Result<()> {
    let db_path = "vector_demo.db";
    
    // Clean up any existing database file
    let _ = fs::remove_file(db_path);
    
    println!("--- SQLite Rust Demo: Vector Search ---");
    
    // 1. Open or create a database
    println!("\n1. Opening database: {}", db_path);
    let mut executor = Executor::open(db_path).map_err(|e| anyhow::anyhow!(e))?;
    
    // 2. Create a table with a VECTOR column
    println!("2. Creating table 'embeddings' with VECTOR(3) column...");
    let create_table_sql = "CREATE TABLE embeddings (id INTEGER PRIMARY KEY, content TEXT, vec VECTOR(3))";
    executor.execute_sql(create_table_sql).map_err(|e| anyhow::anyhow!(e))?;
    
    // 3. Insert vector data
    println!("3. Inserting vector data...");
    let insert_sqls = vec![
        "INSERT INTO embeddings VALUES (1, 'Apple', [1.0, 2.0, 3.0])",
        "INSERT INTO embeddings VALUES (2, 'Banana', [4.0, 5.0, 6.0])",
        "INSERT INTO embeddings VALUES (3, 'Orange', [1.1, 2.1, 3.1])",
        "INSERT INTO embeddings VALUES (4, 'Grape', [7.0, 8.0, 9.0])",
    ];
    
    for sql in insert_sqls {
        println!("  Executing: {}", sql);
        executor.execute_sql(sql).map_err(|e| anyhow::anyhow!(e))?;
    }
    
    // 4. Query all data
    println!("\n4. Current data in 'embeddings':");
    let result = executor.execute_sql("SELECT * FROM embeddings").map_err(|e| anyhow::anyhow!(e))?;
    if let ExecuteResult::Query(query_result) = result {
        query_result.print();
    }
    
    // 5. Vector Similarity Search (L2 Distance)
    println!("\n5. Searching for vectors similar to [1.0, 2.0, 3.0] using L2_DISTANCE:");
    let l2_search_sql = "SELECT content, L2_DISTANCE(vec, [1.0, 2.0, 3.0]) as dist FROM embeddings ORDER BY dist ASC";
    println!("  Executing: {}", l2_search_sql);
    let result = executor.execute_sql(l2_search_sql).map_err(|e| anyhow::anyhow!(e))?;
    if let ExecuteResult::Query(query_result) = result {
        query_result.print();
    }
    
    // 6. Vector Similarity Search (Cosine Similarity)
    println!("\n6. Searching for vectors similar to [1.0, 2.0, 3.2] using COSINE_SIMILARITY:");
    let cosine_search_sql = "SELECT content, COSINE_SIMILARITY(vec, [1.0, 2.0, 3.2]) as sim FROM embeddings ORDER BY sim DESC";
    println!("  Executing: {}", cosine_search_sql);
    let result = executor.execute_sql(cosine_search_sql).map_err(|e| anyhow::anyhow!(e))?;
    if let ExecuteResult::Query(query_result) = result {
        query_result.print();
    }

    println!("\n--- Demo Complete ---");
    
    // Clean up
    let _ = fs::remove_file(db_path);
    
    Ok(())
}
