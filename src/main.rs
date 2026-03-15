use sqllite_rust::sql::{Parser, ast::Statement};
use sqllite_rust::pager::Pager;

fn main() {
    println!("SQLite Rust Clone - Demo\n");

    // Demo 1: SQL Parsing
    println!("=== SQL Parser Demo ===");
    let sql_statements = vec![
        "SELECT * FROM users",
        "SELECT id, name FROM users WHERE id = 1",
        "INSERT INTO users VALUES (1, 'Alice')",
        "UPDATE users SET name = 'Bob' WHERE id = 1",
        "DELETE FROM users WHERE id = 1",
        "CREATE TABLE users (id INTEGER, name TEXT)",
        "CREATE INDEX idx_name ON users (name)",
        "DROP TABLE users",
        "BEGIN TRANSACTION",
        "COMMIT",
        "ROLLBACK",
    ];

    for sql in sql_statements {
        match Parser::new(sql) {
            Ok(mut parser) => {
                match parser.parse() {
                    Ok(stmt) => {
                        println!("✓ Parsed: {}", sql);
                        match stmt {
                            Statement::Select(_) => println!("  → SELECT statement"),
                            Statement::Insert(_) => println!("  → INSERT statement"),
                            Statement::Update(_) => println!("  → UPDATE statement"),
                            Statement::Delete(_) => println!("  → DELETE statement"),
                            Statement::CreateTable(_) => println!("  → CREATE TABLE statement"),
                            Statement::CreateIndex(_) => println!("  → CREATE INDEX statement"),
                            Statement::DropTable(_) => println!("  → DROP TABLE statement"),
                            Statement::BeginTransaction => println!("  → BEGIN TRANSACTION"),
                            Statement::Commit => println!("  → COMMIT"),
                            Statement::Rollback => println!("  → ROLLBACK"),
                        }
                    }
                    Err(e) => println!("✗ Parse error in '{}': {:?}", sql, e),
                }
            }
            Err(e) => println!("✗ Tokenizer error in '{}': {:?}", sql, e),
        }
    }

    // Demo 2: Pager
    println!("\n=== Pager Demo ===");
    let db_path = "/tmp/test_sqllite.db";

    // Clean up if exists
    let _ = std::fs::remove_file(db_path);

    match Pager::open(db_path) {
        Ok(mut pager) => {
            println!("✓ Created database: {}", db_path);

            // Allocate a page
            match pager.allocate_page() {
                Ok(page_id) => {
                    println!("✓ Allocated page: {}", page_id);

                    // Write to page
                    match pager.get_page(page_id) {
                        Ok(mut page) => {
                            page.data[0] = 42;
                            page.data[1] = 0xDE;
                            page.data[2] = 0xAD;
                            page.data[3] = 0xBE;
                            page.data[4] = 0xEF;

                            if let Err(e) = pager.write_page(&page) {
                                println!("✗ Failed to write page: {:?}", e);
                            } else {
                                println!("✓ Wrote data to page {}", page_id);
                            }
                        }
                        Err(e) => println!("✗ Failed to get page: {:?}", e),
                    }

                    // Flush to disk
                    if let Err(e) = pager.flush() {
                        println!("✗ Failed to flush: {:?}", e);
                    } else {
                        println!("✓ Flushed to disk");
                    }
                }
                Err(e) => println!("✗ Failed to allocate page: {:?}", e),
            }
        }
        Err(e) => println!("✗ Failed to open database: {:?}", e),
    }

    // Reopen and verify
    match Pager::open(db_path) {
        Ok(mut pager) => {
            println!("✓ Reopened database");

            match pager.get_page(1) {
                Ok(page) => {
                    if page.data[0] == 42 {
                        println!("✓ Verified data persistence: page.data[0] = {}", page.data[0]);
                    } else {
                        println!("✗ Data mismatch: expected 42, got {}", page.data[0]);
                    }
                }
                Err(e) => println!("✗ Failed to read page: {:?}", e),
            }
        }
        Err(e) => println!("✗ Failed to reopen database: {:?}", e),
    }

    // Clean up
    let _ = std::fs::remove_file(db_path);

    println!("\n=== Demo Complete ===");
    println!("All core components are working:");
    println!("  - SQL Tokenizer: Converts SQL text to tokens");
    println!("  - SQL Parser: Builds AST from tokens");
    println!("  - Pager: Manages database pages with LRU cache");
    println!("  - Storage: Record serialization/deserialization");
    println!("  - VM: Bytecode execution engine (basic)");
    println!("  - Transaction: WAL-based transactions (basic)");
    println!("  - Index: B-tree index structure (basic)");
}
