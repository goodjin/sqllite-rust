//! SQL Dialect Boundary Tests
//!
//! Tests for SQL dialect variations and edge cases

use sqllite_rust::sql::parser::Parser;

// ============================================================================
// SQLite Specific Tests
// ============================================================================

#[test]
fn test_sqlite_pragma() {
    let sqls = vec![
        "PRAGMA cache_size",
        "PRAGMA cache_size = 10000",
        "PRAGMA journal_mode = WAL",
        "PRAGMA foreign_keys = ON",
        "PRAGMA synchronous = NORMAL",
    ];
    
    for sql in &sqls {
        let _ = Parser::new(sql);
    }
}

#[test]
fn test_sqlite_attach() {
    let sqls = vec![
        "ATTACH DATABASE 'file.db' AS aux",
        "ATTACH 'file.db' AS aux",
        "DETACH DATABASE aux",
        "DETACH aux",
    ];
    
    for sql in &sqls {
        let _ = Parser::new(sql);
    }
}

#[test]
fn test_sqlite_reindex() {
    let sqls = vec![
        "REINDEX",
        "REINDEX t",
        "REINDEX idx",
        "REINDEX COLLATE NOCASE",
    ];
    
    for sql in &sqls {
        let _ = Parser::new(sql);
    }
}

#[test]
fn test_sqlite_analyze() {
    let sqls = vec![
        "ANALYZE",
        "ANALYZE t",
        "ANALYZE main.t",
    ];
    
    for sql in &sqls {
        let _ = Parser::new(sql);
    }
}

#[test]
fn test_sqlite_vacuum() {
    let sqls = vec![
        "VACUUM",
        "VACUUM main",
        "VACUUM INTO 'file.db'",
    ];
    
    for sql in &sqls {
        let _ = Parser::new(sql);
    }
}

// ============================================================================
// MySQL Compatibility Tests
// ============================================================================

#[test]
fn test_mysql_limit() {
    let sql = "SELECT * FROM t LIMIT 10, 20";
    let _ = Parser::new(sql);
}

#[test]
fn test_mysql_backtick() {
    let sql = "SELECT * FROM `table`";
    let _ = Parser::new(sql);
}

// ============================================================================
// PostgreSQL Compatibility Tests
// ============================================================================

#[test]
fn test_postgres_limit() {
    let sql = "SELECT * FROM t LIMIT 10 OFFSET 20";
    let mut parser = Parser::new(sql).unwrap();
    let result = parser.parse();
    assert!(result.is_ok());
}

#[test]
fn test_postgres_returning() {
    let sqls = vec![
        "INSERT INTO t VALUES (1) RETURNING *",
        "UPDATE t SET a = 1 RETURNING id",
        "DELETE FROM t RETURNING *",
    ];
    
    for sql in &sqls {
        let _ = Parser::new(sql);
    }
}

// ============================================================================
// Standard SQL Tests
// ============================================================================

#[test]
fn test_standard_sql_keywords() {
    let sqls = vec![
        "SELECT * FROM t",
        "INSERT INTO t VALUES (1)",
        "UPDATE t SET a = 1",
        "DELETE FROM t",
        "CREATE TABLE t (a INT)",
        "DROP TABLE t",
        "CREATE INDEX idx ON t(a)",
        "DROP INDEX idx",
    ];
    
    for sql in &sqls {
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse();
        assert!(result.is_ok(), "Failed for: {}", sql);
    }
}

#[test]
fn test_standard_sql_transactions() {
    let sqls = vec![
        "START TRANSACTION",
        "COMMIT",
        "ROLLBACK",
        "BEGIN",
        "END",
    ];
    
    for sql in &sqls {
        let _ = Parser::new(sql);
    }
}

// ============================================================================
// Case Sensitivity Tests
// ============================================================================

#[test]
fn test_case_variations() {
    let variations = vec![
        "SELECT * FROM t",
        "select * from t",
        "Select * From t",
        "SeLeCt * FrOm t",
    ];
    
    for sql in &variations {
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse();
        assert!(result.is_ok(), "Failed for: {}", sql);
    }
}

// ============================================================================
// Whitespace Tests
// ============================================================================

#[test]
fn test_whitespace_variations() {
    let variations = vec![
        "SELECT * FROM t",
        "SELECT  *  FROM  t",
        "SELECT\t*\tFROM\tt",
        "SELECT\n*\nFROM\nt",
        "SELECT\r\n*\r\nFROM\r\nt",
    ];
    
    for sql in &variations {
        let result = Parser::new(sql);
        let _ = result;
    }
}

// ============================================================================
// Comment Tests
// ============================================================================

#[test]
fn test_sql_comments() {
    let sqls = vec![
        "-- comment\nSELECT 1",
        "/* comment */ SELECT 1",
        "SELECT /* comment */ 1",
        "SELECT 1 /* comment */",
        "/* multi\nline\ncomment */ SELECT 1",
    ];
    
    for sql in &sqls {
        let result = Parser::new(sql);
        let _ = result;
    }
}
