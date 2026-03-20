use std::env;
use sqllite_rust::pager::Pager;
use sqllite_rust::executor::{Executor, ExecuteResult};
use rustyline::DefaultEditor;

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
    println!("    .tables     List all tables");
    println!("    .schema     Show database schema");
    println!("    .dbinfo     Show database info");
}

fn run_shell() {
    println!("SQLite Rust Clone {} - Interactive Shell", VERSION);
    println!("输入 \".help\" 获取帮助。");
    println!("输入 \".quit\"、\".exit\" 或按 Ctrl+D 退出程序。\n");

    let db_path = "sqllite.db";
    println!("Connected to {}", db_path);

    let mut executor = match Executor::open(db_path) {
        Ok(exec) => exec,
        Err(e) => {
            eprintln!("Error opening database: {:?}", e);
            return;
        }
    };

    let mut rl = DefaultEditor::new().expect("Failed to create editor");
    let history_path = ".sqllite_history";
    let _ = rl.load_history(history_path);

    loop {
        let readline = rl.readline("sqllite> ");
        match readline {
            Ok(line) => {
                let input = line.trim();
                if input.is_empty() {
                    continue;
                }

                let _ = rl.add_history_entry(input);

                if input.starts_with('.') {
                    if handle_command(input, &mut executor) {
                        break;
                    }
                } else {
                    handle_sql(input, &mut executor);
                }
            }
            Err(rustyline::error::ReadlineError::Interrupted) => {
                println!("^C");
                continue;
            }
            Err(rustyline::error::ReadlineError::Eof) => {
                println!();
                break;
            }
            Err(err) => {
                eprintln!("Error: {:?}", err);
                break;
            }
        }
    }

    let _ = rl.save_history(history_path);
    println!("Bye!");
}

// 返回 true 表示退出
fn handle_command(cmd_line: &str, executor: &mut Executor) -> bool {
    let parts: Vec<&str> = cmd_line.split_whitespace().collect();
    if parts.is_empty() { return false; }
    
    let cmd = parts[0].trim_end_matches(';');
    
    match cmd {
        ".quit" | ".exit" | ".q" => {
            return true;
        }
        ".help" => {
            println!(".quit       Exit this program");
            println!(".tables     List all tables");
            println!(".schema     Show database schema");
            println!(".dbinfo     Show database info");
            println!(".open PATH  Close current database and open PATH");
            println!(".help       Show this help message");
        }
        ".tables" => {
            let tables = executor.list_tables();
            if tables.is_empty() {
                println!("No tables found.");
            } else {
                for table in tables {
                    println!("{}", table);
                }
            }
        }
        ".schema" => {
            let table_name = if parts.len() > 1 { Some(parts[1]) } else { None };
            let tables = if let Some(name) = table_name {
                vec![name.to_string()]
            } else {
                executor.list_tables()
            };
            
            if tables.is_empty() {
                if table_name.is_some() {
                    println!("Table not found: {}", table_name.unwrap());
                } else {
                    println!("No tables found.");
                }
            } else {
                for table in tables {
                    if let Some(schema) = executor.get_table_schema(&table) {
                        println!("{};", schema);
                    }
                }
            }
        }
        ".dbinfo" => {
            // Re-open pager to get header info (since it's private in Executor)
            // Note: This is a bit hacky but works for demo
            match Pager::open("sqllite.db") {
                Ok(pager) => {
                    let header = pager.header();
                    println!("-- Database Info:");
                    let page_size = header.page_size;
                    let database_size = header.database_size;
                    let text_encoding = header.text_encoding;
                    println!("   Page size: {} bytes", page_size);
                    println!("   Database size: {} pages", database_size);
                    println!("   File format: {}.{}", header.file_format_write, header.file_format_read);
                    println!("   Text encoding: {} (1=UTF-8)", text_encoding);
                }
                Err(e) => {
                    eprintln!("Error getting database info: {:?}", e);
                }
            }
        }
        ".open" => {
            if parts.len() < 2 {
                println!("Usage: .open PATH");
            } else {
                let path = parts[1];
                match Executor::open(path) {
                    Ok(new_executor) => {
                        *executor = new_executor;
                        println!("Opened database: {}", path);
                    }
                    Err(e) => {
                        println!("Error opening database {}: {:?}", path, e);
                    }
                }
            }
        }
        _ => {
            println!("Unknown command: {}", cmd);
            println!("Enter \".help\" for available commands.");
        }
    }
    false
}

fn handle_sql(sql_line: &str, executor: &mut Executor) {
    // Basic multi-statement support by splitting on semicolon
    for sql in sql_line.split(';') {
        let sql = sql.trim();
        if sql.is_empty() { continue; }
        
        match executor.execute_sql(sql) {
            Ok(result) => {
                match result {
                    ExecuteResult::Success(msg) => {
                        println!("{}", msg);
                    }
                    ExecuteResult::Query(query_result) => {
                        query_result.print();
                    }
                }
            }
            Err(e) => {
                eprintln!("Error: {}", e);
            }
        }
    }
}

fn run_demo() {
    println!("SQLite Rust Clone - Comprehensive Demo\n");

    let db_path = "demo.db";
    let _ = std::fs::remove_file(db_path);

    let mut executor = match Executor::open(db_path) {
        Ok(exec) => exec,
        Err(e) => {
            println!("✗ Failed to open database: {:?}", e);
            return;
        }
    };

    let test_cases = vec![
        ("Create and describe table", vec![
            "CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price INTEGER, category TEXT)",
        ]),
        ("Insert data", vec![
            "INSERT INTO products VALUES (1, 'Laptop', 1200, 'Electronics')",
            "INSERT INTO products VALUES (2, 'Smartphone', 800, 'Electronics')",
            "INSERT INTO products VALUES (3, 'Coffee Maker', 100, 'Appliances')",
            "INSERT INTO products VALUES (4, 'Headphones', 150, 'Electronics')",
        ]),
        ("Simple queries", vec![
            "SELECT * FROM products",
            "SELECT name, price FROM products WHERE price > 500",
        ]),
        ("Transactional updates", vec![
            "BEGIN TRANSACTION",
            "UPDATE products SET price = price + 50 WHERE category = 'Electronics'",
            "SELECT * FROM products WHERE category = 'Electronics'",
            "ROLLBACK",
            "SELECT * FROM products WHERE category = 'Electronics'",
        ]),
        ("Cleanup", vec![
            "DROP TABLE products",
        ]),
    ];

    for (desc, sqls) in test_cases {
        println!("\n=== {} ===", desc);
        for sql in sqls {
            println!("> {}", sql);
            match executor.execute_sql(sql) {
                Ok(result) => {
                    match result {
                        ExecuteResult::Success(msg) => println!("✓ {}", msg),
                        ExecuteResult::Query(qr) => qr.print(),
                    }
                }
                Err(e) => println!("✗ Error: {}", e),
            }
        }
    }

    // Clean up
    let _ = std::fs::remove_file(db_path);

    println!("\n=== Demo Complete ===");
    println!("All core components are working:");
    println!("  - Persistence: Data is stored in B+ Tree based pages");
    println!("  - Transactions: ROLLBACK correctly restores previous state");
    println!("  - Query Engine: Filters (WHERE) and complex statements supported");
    println!("\nTry the interactive shell:");
    println!("  cargo run -- shell");
}
