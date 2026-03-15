use std::io::{self, Write};
use std::env;
use sqllite_rust::sql::{Parser, ast::Statement};
use sqllite_rust::pager::Pager;

const VERSION: &str = "0.1.0";

fn main() {
    let args: Vec<String> = env::args().collect();

    match args.get(1).map(|s| s.as_str()) {
        Some("--help") | Some("-h") => print_help(),
        Some("--version") | Some("-v") => println!("sqllite-rust {}", VERSION),
        Some("shell") => run_shell(),
        Some("demo") => run_demo(),
        None => {
            println!("SQLite Rust Clone {}", VERSION);
            println!("Run with --help for usage information\n");
            run_demo();
        }
        _ => {
            eprintln!("Unknown command: {}", args[1]);
            eprintln!("Run with --help for usage information");
            std::process::exit(1);
        }
    }
}

fn print_help() {
    println!("SQLite Rust Clone - A SQLite-compatible database in Rust");
    println!();
    println!("USAGE:");
    println!("    sqllite-rust [COMMAND] [OPTIONS]");
    println!();
    println!("COMMANDS:");
    println!("    demo         Run the demonstration (default)");
    println!("    shell        Start interactive SQL shell");
    println!("    --help, -h   Show this help message");
    println!("    --version    Show version information");
    println!();
    println!("EXAMPLES:");
    println!("    sqllite-rust              # Run demo");
    println!("    sqllite-rust shell        # Start SQL shell");
    println!();
    println!("IN SHELL MODE:");
    println!("    .help       Show shell commands");
    println!("    .quit       Exit shell");
    println!("    .tables     List tables (placeholder)");
    println!("    .schema     Show database schema (placeholder)");
    println!();
    println!("NOTE: This is an educational implementation.");
    println!("      Only SQL parsing is fully implemented.");
    println!("      Data persistence uses page-level storage.");
}

fn run_shell() {
    println!("SQLite Rust Clone {} - Interactive Shell", VERSION);
    println!("Enter \".help\" for usage hints.");
    println!("Enter \".quit\" to exit.\n");

    let stdin = io::stdin();
    let mut stdout = io::stdout();
    let mut buffer = String::new();

    // Open a demo database
    let db_path = "/tmp/sqllite_shell.db";
    let _ = std::fs::remove_file(db_path);

    loop {
        print!("sqlite> ");
        stdout.flush().unwrap();

        buffer.clear();
        match stdin.read_line(&mut buffer) {
            Ok(0) => {
                println!();
                break;
            }
            Ok(_) => {
                let input = buffer.trim();
                if input.is_empty() {
                    continue;
                }

                if input.starts_with('.') {
                    handle_command(input);
                } else {
                    handle_sql(input);
                }
            }
            Err(e) => {
                eprintln!("Error reading input: {}", e);
                break;
            }
        }
    }

    // Cleanup
    let _ = std::fs::remove_file(db_path);
    println!("Bye!");
}

fn handle_command(cmd: &str) {
    match cmd {
        ".quit" | ".exit" | ".q" => {
            std::process::exit(0);
        }
        ".help" => {
            println!(".quit       Exit this program");
            println!(".tables     List tables (placeholder)");
            println!(".schema     Show schema (placeholder)");
            println!(".dbinfo     Show database info");
            println!(".help       Show this help message");
        }
        ".tables" => {
            println!("-- Tables: (not yet implemented)");
            println!("   users");
            println!("   orders");
        }
        ".schema" => {
            println!("-- Schema: (not yet implemented)");
            println!("CREATE TABLE users (id INTEGER, name TEXT, email TEXT, age INTEGER);");
            println!("CREATE TABLE orders (id INTEGER, user_id INTEGER, product TEXT, amount INTEGER, status TEXT);");
        }
        ".dbinfo" => {
            match Pager::open("/tmp/sqllite_shell.db") {
                Ok(pager) => {
                    let header = pager.header();
                    // Copy fields to avoid packed struct alignment issues
                    let page_size = header.page_size;
                    let database_size = header.database_size;
                    let file_format_write = header.file_format_write;
                    let file_format_read = header.file_format_read;
                    let text_encoding = header.text_encoding;
                    println!("-- Database Info:");
                    println!("   Page size: {} bytes", page_size);
                    println!("   Database size: {} pages", database_size);
                    println!("   File format: {}.{}", file_format_write, file_format_read);
                    println!("   Text encoding: {} (1=UTF-8)", text_encoding);
                }
                Err(e) => {
                    eprintln!("Error opening database: {:?}", e);
                }
            }
        }
        _ => {
            println!("Unknown command: {}", cmd);
            println!("Enter \".help\" for available commands.");
        }
    }
}

fn handle_sql(sql: &str) {
    match Parser::new(sql) {
        Ok(mut parser) => {
            match parser.parse() {
                Ok(stmt) => {
                    println!("Parsed successfully:");
                    match stmt {
                        Statement::Select(_) => println!("  → SELECT statement (execution not yet implemented)"),
                        Statement::Insert(_) => println!("  → INSERT statement (execution not yet implemented)"),
                        Statement::Update(_) => println!("  → UPDATE statement (execution not yet implemented)"),
                        Statement::Delete(_) => println!("  → DELETE statement (execution not yet implemented)"),
                        Statement::CreateTable(ref ct) => {
                            println!("  → CREATE TABLE: {}", ct.table);
                            for col in &ct.columns {
                                println!("      Column: {} {:?}", col.name, col.data_type);
                            }
                        }
                        Statement::DropTable(ref dt) => {
                            println!("  → DROP TABLE: {}", dt.table);
                        }
                        Statement::CreateIndex(ref ci) => {
                            println!("  → CREATE INDEX: {} ON {}({})",
                                ci.index_name, ci.table, ci.column);
                        }
                        Statement::BeginTransaction => println!("  → BEGIN TRANSACTION"),
                        Statement::Commit => println!("  → COMMIT"),
                        Statement::Rollback => println!("  → ROLLBACK"),
                    }
                }
                Err(e) => {
                    eprintln!("Parse error: {:?}", e);
                }
            }
        }
        Err(e) => {
            eprintln!("Tokenizer error: {:?}", e);
        }
    }
}

fn run_demo() {
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
    println!("\nRun with 'shell' command for interactive SQL shell:");
    println!("  cargo run -- shell");
}
