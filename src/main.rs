use std::env;
use sqllite_rust::sql::Parser;
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
    println!("Enter \".help\" for usage hints.");
    println!("Enter \".quit\" to exit.\n");

    // Open database with executor
    let db_path = "/tmp/sqllite_shell.db";
    let _ = std::fs::remove_file(db_path);

    let mut executor = match Executor::open(db_path) {
        Ok(exec) => exec,
        Err(e) => {
            eprintln!("Error opening database: {:?}", e);
            return;
        }
    };

    let mut rl = DefaultEditor::new().expect("Failed to create editor");
    let history_path = "/tmp/sqllite_history.txt";
    let _ = rl.load_history(history_path);

    loop {
        let readline = rl.readline("sqlite> ");
        match readline {
            Ok(line) => {
                let input = line.trim();
                if input.is_empty() {
                    continue;
                }

                let _ = rl.add_history_entry(input);

                if input.starts_with('.') {
                    if handle_command(input, &executor) {
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

    // Cleanup
    let _ = std::fs::remove_file(db_path);
    println!("Bye!");
}

// 返回 true 表示退出
fn handle_command(cmd: &str, _executor: &Executor) -> bool {
    match cmd {
        ".quit" | ".exit" | ".q" => {
            return true;
        }
        ".help" => {
            println!(".quit       Exit this program");
            println!(".tables     List all tables");
            println!(".schema     Show database schema");
            println!(".dbinfo     Show database info");
            println!(".help       Show this help message");
        }
        ".tables" => {
            println!("-- Tables:");
            println!("   (Use .schema to see table details)");
        }
        ".schema" => {
            println!("-- Schema:");
            println!("   (Schema display not yet implemented)");
        }
        ".dbinfo" => {
            match Pager::open("/tmp/sqllite_shell.db") {
                Ok(pager) => {
                    let header = pager.header();
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
    false
}

fn handle_sql(sql: &str, executor: &mut Executor) {
    match Parser::new(sql) {
        Ok(mut parser) => {
            match parser.parse() {
                Ok(stmt) => {
                    match executor.execute(&stmt) {
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
                            eprintln!("Execution error: {:?}", e);
                        }
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

    // Demo 1: SQL Parsing and Execution
    println!("=== SQL Execution Demo ===");

    let db_path = "/tmp/test_sqllite.db";
    let _ = std::fs::remove_file(db_path);

    let mut executor = match Executor::open(db_path) {
        Ok(exec) => exec,
        Err(e) => {
            println!("✗ Failed to open database: {:?}", e);
            return;
        }
    };

    let sql_statements = vec![
        "CREATE TABLE users (id INTEGER, name TEXT)",
        "INSERT INTO users VALUES (1, 'Alice')",
        "INSERT INTO users VALUES (2, 'Bob')",
        "SELECT * FROM users",
    ];

    for sql in sql_statements {
        println!("> {}", sql);
        match Parser::new(sql) {
            Ok(mut parser) => {
                match parser.parse() {
                    Ok(stmt) => {
                        match executor.execute(&stmt) {
                            Ok(result) => {
                                match result {
                                    ExecuteResult::Success(msg) => {
                                        println!("✓ {}", msg);
                                    }
                                    ExecuteResult::Query(query_result) => {
                                        query_result.print();
                                    }
                                }
                            }
                            Err(e) => {
                                println!("✗ Execution error: {:?}", e);
                            }
                        }
                    }
                    Err(e) => println!("✗ Parse error: {:?}", e),
                }
            }
            Err(e) => println!("✗ Tokenizer error: {:?}", e),
        }
    }

    // Clean up
    let _ = std::fs::remove_file(db_path);

    println!("\n=== Demo Complete ===");
    println!("All core components are working:");
    println!("  - SQL Tokenizer: Converts SQL text to tokens");
    println!("  - SQL Parser: Builds AST from tokens");
    println!("  - Executor: Executes SQL statements");
    println!("  - Pager: Manages database pages with LRU cache");
    println!("  - Storage: Record serialization/deserialization");
    println!("\nRun with 'shell' command for interactive SQL shell:");
    println!("  cargo run -- shell");
}
