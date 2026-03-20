use sqllite_rust::executor::Executor;
use sqllite_rust::sql::ast::Statement;
use sqllite_rust::sql::Parser;

fn main() {
    let db_path = "vector_hnsw.db";
    let _ = std::fs::remove_file(db_path);
    let _ = std::fs::remove_file(format!("{}-wal", db_path));
    
    let mut executor = Executor::open(db_path).expect("Failed to open database");

    // 1. Create table with vector column
    println!("Creating table 'items' with vector column...");
    let create_table_sql = "CREATE TABLE items (id INTEGER PRIMARY KEY, description TEXT, embedding VECTOR(3))";
    let stmt = Parser::new(create_table_sql).unwrap().parse().unwrap();
    executor.execute(&stmt).unwrap();

    // 2. Create HNSW index
    println!("Creating HNSW index 'idx_embedding' on items(embedding)...");
    let create_index_sql = "CREATE INDEX idx_embedding ON items(embedding) USING HNSW";
    let stmt = Parser::new(create_index_sql).unwrap().parse().unwrap();
    executor.execute(&stmt).unwrap();

    // 3. Insert data
    println!("Inserting data...");
    let data = vec![
        (1, "Apple", vec![1.0, 2.0, 3.0]),
        (2, "Banana", vec![4.0, 5.0, 6.0]),
        (3, "Cherry", vec![1.1, 2.1, 3.1]),
        (4, "Date", vec![7.0, 8.0, 9.0]),
    ];

    for (id, desc, vec) in data {
        let vec_str = format!("[{}, {}, {}]", vec[0], vec[1], vec[2]);
        let insert_sql = format!("INSERT INTO items (id, description, embedding) VALUES ({}, '{}', {})", id, desc, vec_str);
        let stmt = Parser::new(&insert_sql).unwrap().parse().unwrap();
        executor.execute(&stmt).unwrap();
    }

    // 4. Perform vector search using the index
    println!("\nSearching for nearest neighbors to [1.05, 2.05, 3.05]...");
    let search_sql = "SELECT id, description, vector_l2_distance(embedding, [1.05, 2.05, 3.05]) as dist FROM items ORDER BY dist LIMIT 2";
    let stmt = Parser::new(search_sql).unwrap().parse().unwrap();
    
    let result = executor.execute(&stmt).unwrap();
    if let sqllite_rust::executor::ExecuteResult::Query(query_result) = result {
        println!("Search Results:");
        for row in query_result.rows {
            println!("  Data: {:?}", row.values);
        }
    }

    println!("\nVector search with HNSW index completed successfully!");
}
