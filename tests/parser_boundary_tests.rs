//! SQL Parser Boundary Tests
//! 
//! Tests for SQL parser edge cases and boundary conditions

use sqllite_rust::sql::parser::Parser;
use sqllite_rust::sql::error::ParseError;

// ============================================================================
// Empty and Minimal Input Tests
// ============================================================================

#[test]
fn test_empty_sql() {
    let result = Parser::new("");
    assert!(result.is_err() || matches!(result.unwrap().parse(), Err(_)));
}

#[test]
fn test_whitespace_only_sql() {
    let result = Parser::new("   \n\t  ");
    assert!(result.is_err() || matches!(result.unwrap().parse(), Err(_)));
}

#[test]
fn test_single_semicolon() {
    let mut parser = Parser::new(";").unwrap();
    let result = parser.parse();
    assert!(result.is_err());
}

#[test]
fn test_minimal_select() {
    let mut parser = Parser::new("SELECT 1").unwrap();
    let result = parser.parse();
    assert!(result.is_ok());
}

// ============================================================================
// SQL Length Boundary Tests
// ============================================================================

#[test]
fn test_very_long_sql() {
    let long_identifier = "a".repeat(10000);
    let sql = format!("SELECT * FROM {}", long_identifier);
    let result = Parser::new(&sql);
    // Should not panic, may or may not parse successfully
    let _ = result;
}

#[test]
fn test_long_column_list() {
    let columns: Vec<String> = (0..100).map(|i| format!("col{}", i)).collect();
    let sql = format!("SELECT {} FROM t", columns.join(", "));
    let mut parser = Parser::new(&sql).unwrap();
    let result = parser.parse();
    assert!(result.is_ok());
}

#[test]
fn test_very_long_string_literal() {
    let long_string = "x".repeat(100000);
    let sql = format!("SELECT '{}'", long_string);
    let result = Parser::new(&sql);
    // Should handle without panic
    let _ = result;
}

#[test]
fn test_deeply_nested_parentheses() {
    // Test deeply nested expressions
    let depth = 100;
    let sql = format!("SELECT {}", "(".repeat(depth) + "1" + &")".repeat(depth));
    let result = Parser::new(&sql);
    let _ = result;
}

// ============================================================================
// Unicode and Special Character Tests
// ============================================================================

#[test]
fn test_unicode_in_identifier() {
    let sqls = vec![
        "SELECT * FROM 用户",
        "SELECT * FROM таблица",
        "SELECT * FROM テーブル",
        "SELECT * FROM 🗄️",
    ];
    for sql in sqls {
        let result = Parser::new(sql);
        let _ = result;
    }
}

#[test]
fn test_unicode_in_string_literal() {
    let sqls = vec![
        "SELECT 'Hello 世界'",
        "SELECT 'Привет мир'",
        "SELECT '🎉🎊🎁'",
        "SELECT '🚀🔥💯'",
    ];
    for sql in &sqls {
        let mut parser = Parser::new(sql).unwrap();
        let _ = parser.parse();
    }
}

#[test]
fn test_special_chars_in_string() {
    let sqls = vec![
        r"SELECT 'line1\nline2'",
        r"SELECT 'tab\there'",
        r"SELECT 'quote''inside'",
        r"SELECT 'backslash\\'",
    ];
    for sql in &sqls {
        let result = Parser::new(sql);
        let _ = result;
    }
}

#[test]
fn test_null_bytes_in_sql() {
    let sql = "SELECT\x001 FROM t";
    let result = Parser::new(sql);
    let _ = result;
}

// ============================================================================
// Keyword Boundary Tests
// ============================================================================

#[test]
fn test_keywords_as_identifiers() {
    // Keywords that could be used as identifiers
    let sqls = vec![
        "SELECT * FROM \"SELECT\"",
        "SELECT * FROM \"WHERE\"",
        "SELECT * FROM \"FROM\"",
        "SELECT \"column\" FROM t",
    ];
    for sql in &sqls {
        let result = Parser::new(sql);
        let _ = result;
    }
}

#[test]
fn test_case_sensitivity_keywords() {
    let variations = vec![
        "select * from t",
        "SELECT * FROM T",
        "Select * From t",
        "SeLeCt * FrOm t",
    ];
    for sql in &variations {
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse();
        assert!(result.is_ok(), "Failed for: {}", sql);
    }
}

#[test]
fn test_reserved_words_escaping() {
    let sqls = vec![
        "SELECT * FROM `order`",
        "SELECT * FROM [select]",
        "SELECT * FROM '\"table\"'",
    ];
    for sql in &sqls {
        let result = Parser::new(sql);
        let _ = result;
    }
}

// ============================================================================
// Number Boundary Tests
// ============================================================================

#[test]
fn test_integer_boundaries() {
    let sqls = vec![
        "SELECT 0",
        "SELECT 1",
        "SELECT -1",
        "SELECT 2147483647",   // i32::MAX
        "SELECT -2147483648",  // i32::MIN
        "SELECT 9223372036854775807",   // i64::MAX
        "SELECT -9223372036854775808",  // i64::MIN
    ];
    for sql in &sqls {
        let result = Parser::new(sql);
        let _ = result;
    }
}

#[test]
fn test_float_boundaries() {
    let sqls = vec![
        "SELECT 0.0",
        "SELECT 0.0000001",
        "SELECT 1e308",     // Near f64::MAX
        "SELECT 1e-308",    // Near f64::MIN_POSITIVE
        "SELECT -1e308",
        "SELECT 1.7976931348623157e308",
    ];
    for sql in &sqls {
        let result = Parser::new(sql);
        let _ = result;
    }
}

#[test]
fn test_scientific_notation() {
    let sqls = vec![
        "SELECT 1e10",
        "SELECT 1E10",
        "SELECT 1.5e-10",
        "SELECT -1.5e+10",
        "SELECT .5e2",
    ];
    for sql in &sqls {
        let result = Parser::new(sql);
        let _ = result;
    }
}

// ============================================================================
// Subquery Depth Tests
// ============================================================================

#[test]
fn test_nested_subqueries_max_depth() {
    let depths = vec![1, 2, 5, 10, 20];
    for depth in depths {
        let prefix = "SELECT * FROM (".repeat(depth);
        let suffix = ")".repeat(depth);
        let sql = format!("{}SELECT 1{}", prefix, suffix);
        let result = Parser::new(&sql);
        let _ = result;
    }
}

#[test]
fn test_deeply_nested_exists() {
    let mut sql = "SELECT 1 WHERE EXISTS (SELECT 1".to_string();
    for _ in 0..10 {
        sql.push_str(" WHERE EXISTS (SELECT 1");
    }
    for _ in 0..11 {
        sql.push(')');
    }
    let result = Parser::new(&sql);
    let _ = result;
}

#[test]
fn test_correlated_subquery_depth() {
    let sql = "SELECT * FROM t1 WHERE EXISTS (SELECT * FROM t2 WHERE t2.id = t1.id AND EXISTS (SELECT * FROM t3 WHERE t3.id = t2.id))";
    let result = Parser::new(sql);
    assert!(result.is_ok());
}

// ============================================================================
// JOIN Complexity Tests
// ============================================================================

#[test]
fn test_max_table_joins() {
    let mut sql = "SELECT * FROM t1".to_string();
    for i in 2..=20 {
        sql.push_str(&format!(" JOIN t{} ON t{}.id = t{}.id", i, i-1, i));
    }
    
    let result = Parser::new(&sql);
    let _ = result;
}

#[test]
fn test_multiple_join_types() {
    let sqls = vec![
        "SELECT * FROM t1 JOIN t2 ON t1.id = t2.id JOIN t3 ON t2.id = t3.id",
        "SELECT * FROM t1 LEFT JOIN t2 ON t1.id = t2.id",
        "SELECT * FROM t1 INNER JOIN t2 ON t1.id = t2.id",
        "SELECT * FROM t1 LEFT JOIN t2 ON t1.id = t2.id LEFT JOIN t3 ON t2.id = t3.id",
    ];
    for sql in &sqls {
        let result = Parser::new(sql);
        let _ = result;
    }
}

#[test]
fn test_self_join_boundaries() {
    let sqls = vec![
        "SELECT * FROM t a JOIN t b ON a.id = b.parent_id",
        "SELECT * FROM employees e JOIN employees m ON e.manager_id = m.id",
    ];
    for sql in &sqls {
        let result = Parser::new(sql);
        assert!(result.is_ok());
    }
}

// ============================================================================
// Column Count Tests
// ============================================================================

#[test]
fn test_max_column_count() {
    let counts = vec![1, 10, 50, 100, 500];
    for count in counts {
        let columns: Vec<String> = (0..count).map(|i| format!("col{}", i)).collect();
        let sql = format!("SELECT {} FROM t", columns.join(", "));
        let result = Parser::new(&sql);
        let _ = result;
    }
}

#[test]
fn test_max_column_count_insert() {
    let counts = vec![1, 10, 50, 100];
    for count in counts {
        let cols: Vec<String> = (0..count).map(|i| format!("col{}", i)).collect();
        let vals: Vec<String> = (0..count).map(|i| format!("{}", i)).collect();
        let sql = format!("INSERT INTO t ({}) VALUES ({})", cols.join(", "), vals.join(", "));
        let result = Parser::new(&sql);
        let _ = result;
    }
}

// ============================================================================
// Expression Complexity Tests
// ============================================================================

#[test]
fn test_complex_expression_depth() {
    let depths = vec![1, 5, 10, 20, 50];
    for depth in depths {
        let expr = "(1+".repeat(depth) + "1" + &")".repeat(depth);
        let sql = format!("SELECT {}", expr);
        let result = Parser::new(&sql);
        let _ = result;
    }
}

#[test]
fn test_complex_boolean_expression() {
    let sqls = vec![
        "SELECT * FROM t WHERE a=1 AND b=2 AND c=3 AND d=4 AND e=5",
        "SELECT * FROM t WHERE a=1 OR b=2 OR c=3 OR d=4 OR e=5",
        "SELECT * FROM t WHERE (a=1 AND b=2) OR (c=3 AND d=4)",
        "SELECT * FROM t WHERE NOT NOT NOT a=1",
    ];
    for sql in &sqls {
        let result = Parser::new(sql);
        let _ = result;
    }
}

#[test]
fn test_complex_case_expression() {
    let sql = "SELECT CASE WHEN a=1 THEN 'one' WHEN a=2 THEN 'two' WHEN a=3 THEN 'three' ELSE 'other' END FROM t";
    let result = Parser::new(sql);
    let _ = result;
}

// ============================================================================
// Comment Tests
// ============================================================================

#[test]
fn test_sql_with_comments() {
    let sqls = vec![
        "SELECT /* comment */ 1",
        "SELECT 1 -- line comment\n FROM t",
        "/* multi\nline\ncomment */ SELECT 1",
        "SELECT/**/1",
    ];
    for sql in &sqls {
        let result = Parser::new(sql);
        let _ = result;
    }
}

#[test]
fn test_comment_before_semicolon() {
    let sql = "SELECT 1 /* comment */;";
    let mut parser = Parser::new(sql).unwrap();
    let result = parser.parse();
    assert!(result.is_ok());
}

// ============================================================================
// Transaction Statement Tests
// ============================================================================

#[test]
fn test_transaction_boundaries() {
    let sqls = vec![
        "BEGIN",
        "BEGIN TRANSACTION",
        "COMMIT",
        "COMMIT TRANSACTION",
        "ROLLBACK",
        "ROLLBACK TRANSACTION",
        "END TRANSACTION",
    ];
    for sql in &sqls {
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse();
        assert!(result.is_ok(), "Failed for: {}", sql);
    }
}

#[test]
fn test_savepoint_boundaries() {
    let sqls = vec![
        "SAVEPOINT sp1",
        "RELEASE SAVEPOINT sp1",
        "ROLLBACK TO SAVEPOINT sp1",
    ];
    for sql in &sqls {
        let result = Parser::new(sql);
        let _ = result;
    }
}

// ============================================================================
// Malformed SQL Tests
// ============================================================================

#[test]
fn test_unclosed_string() {
    let sqls = vec![
        "SELECT 'unclosed",
        r#"SELECT "unclosed"#,
    ];
    for sql in &sqls {
        let result = Parser::new(sql);
        let _ = result;
    }
}

#[test]
fn test_unmatched_parentheses() {
    let sqls = vec![
        "SELECT (1",
        "SELECT 1)",
        "SELECT ((1)",
        "SELECT (1))",
    ];
    for sql in &sqls {
        let result = Parser::new(sql);
        let _ = result;
    }
}

#[test]
fn test_invalid_characters() {
    let sqls = vec![
        "SELECT @ FROM t",
        "SELECT # FROM t",
        "SELECT $ FROM t",
        "SELECT ` FROM t",
    ];
    for sql in &sqls {
        let result = Parser::new(sql);
        let _ = result;
    }
}

#[test]
fn test_incomplete_statements() {
    let sqls = vec![
        "SELECT",
        "SELECT FROM",
        "SELECT * FROM",
        "INSERT INTO",
        "UPDATE",
        "DELETE",
        "CREATE TABLE",
        "DROP",
    ];
    for sql in &sqls {
        let result = Parser::new(sql);
        let _ = result;
    }
}

// ============================================================================
// Data Type Boundary Tests
// ============================================================================

#[test]
fn test_data_type_declarations() {
    let sqls = vec![
        "CREATE TABLE t (a INTEGER)",
        "CREATE TABLE t (a TEXT)",
        "CREATE TABLE t (a REAL)",
        "CREATE TABLE t (a BLOB)",
        "CREATE TABLE t (a NUMERIC)",
        "CREATE TABLE t (a INTEGER PRIMARY KEY)",
        "CREATE TABLE t (a TEXT NOT NULL)",
        "CREATE TABLE t (a REAL DEFAULT 0.0)",
        "CREATE TABLE t (a INTEGER UNIQUE)",
        "CREATE TABLE t (a TEXT COLLATE NOCASE)",
    ];
    for sql in &sqls {
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse();
        assert!(result.is_ok(), "Failed for: {}", sql);
    }
}

#[test]
fn test_data_type_precision() {
    let sqls = vec![
        "CREATE TABLE t (a VARCHAR(255))",
        "CREATE TABLE t (a VARCHAR(0))",
        "CREATE TABLE t (a VARCHAR(65535))",
        "CREATE TABLE t (a DECIMAL(10,2))",
        "CREATE TABLE t (a DECIMAL(38,10))",
    ];
    for sql in &sqls {
        let result = Parser::new(sql);
        let _ = result;
    }
}

// ============================================================================
// Aggregate Function Tests
// ============================================================================

#[test]
fn test_aggregate_functions() {
    let sqls = vec![
        "SELECT COUNT(*) FROM t",
        "SELECT COUNT(DISTINCT a) FROM t",
        "SELECT SUM(a) FROM t",
        "SELECT AVG(a) FROM t",
        "SELECT MIN(a) FROM t",
        "SELECT MAX(a) FROM t",
        "SELECT COUNT(*), SUM(a), AVG(b) FROM t",
    ];
    for sql in &sqls {
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse();
        assert!(result.is_ok(), "Failed for: {}", sql);
    }
}

#[test]
fn test_group_by_complexity() {
    let sqls = vec![
        "SELECT a, COUNT(*) FROM t GROUP BY a",
        "SELECT a, b, COUNT(*) FROM t GROUP BY a, b",
        "SELECT a, COUNT(*) FROM t GROUP BY a HAVING COUNT(*) > 1",
        "SELECT a, b, c, d, COUNT(*) FROM t GROUP BY a, b, c, d",
    ];
    for sql in &sqls {
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse();
        assert!(result.is_ok(), "Failed for: {}", sql);
    }
}

// ============================================================================
// ORDER BY and LIMIT Tests
// ============================================================================

#[test]
fn test_order_by_boundaries() {
    let sqls = vec![
        "SELECT * FROM t ORDER BY a",
        "SELECT * FROM t ORDER BY a ASC",
        "SELECT * FROM t ORDER BY a DESC",
        "SELECT * FROM t ORDER BY a, b, c, d, e",
        "SELECT * FROM t ORDER BY a ASC, b DESC, c ASC",
    ];
    for sql in &sqls {
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse();
        assert!(result.is_ok(), "Failed for: {}", sql);
    }
}

#[test]
fn test_limit_offset_boundaries() {
    let sqls = vec![
        "SELECT * FROM t LIMIT 0",
        "SELECT * FROM t LIMIT 1",
        "SELECT * FROM t LIMIT 1000000",
        "SELECT * FROM t LIMIT 10 OFFSET 0",
        "SELECT * FROM t LIMIT 10 OFFSET 100",
        "SELECT * FROM t OFFSET 10",
    ];
    for sql in &sqls {
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse();
        assert!(result.is_ok(), "Failed for: {}", sql);
    }
}

// ============================================================================
// CTE (Common Table Expression) Tests
// ============================================================================

#[test]
fn test_cte_boundaries() {
    let sqls = vec![
        "WITH cte AS (SELECT 1) SELECT * FROM cte",
        "WITH cte1 AS (SELECT 1), cte2 AS (SELECT 2) SELECT * FROM cte1, cte2",
        "WITH RECURSIVE cte AS (SELECT 1 UNION ALL SELECT n+1 FROM cte WHERE n < 10) SELECT * FROM cte",
    ];
    for sql in &sqls {
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse();
        assert!(result.is_ok(), "Failed for: {}", sql);
    }
}

#[test]
fn test_multiple_ctes() {
    let ctes: Vec<String> = (0..10).map(|i| format!("cte{} AS (SELECT {} AS n)", i, i)).collect();
    let sql = format!("WITH {} SELECT * FROM cte0", ctes.join(", "));
    let result = Parser::new(&sql);
    let _ = result;
}

// ============================================================================
// Index Statement Tests
// ============================================================================

#[test]
fn test_create_index_boundaries() {
    let sqls = vec![
        "CREATE INDEX idx ON t(a)",
        "CREATE UNIQUE INDEX idx ON t(a)",
        "CREATE INDEX idx ON t(a, b, c)",
        "CREATE INDEX idx ON t(a ASC)",
        "CREATE INDEX idx ON t(a DESC)",
        "CREATE INDEX IF NOT EXISTS idx ON t(a)",
    ];
    for sql in &sqls {
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse();
        assert!(result.is_ok(), "Failed for: {}", sql);
    }
}

// ============================================================================
// DROP Statement Tests
// ============================================================================

#[test]
fn test_drop_boundaries() {
    let sqls = vec![
        "DROP TABLE t",
        "DROP TABLE IF EXISTS t",
        "DROP INDEX idx",
        "DROP INDEX IF EXISTS idx",
    ];
    for sql in &sqls {
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse();
        assert!(result.is_ok(), "Failed for: {}", sql);
    }
}

// ============================================================================
// ALTER Statement Tests
// ============================================================================

#[test]
fn test_alter_table_boundaries() {
    let sqls = vec![
        "ALTER TABLE t ADD COLUMN a INTEGER",
        "ALTER TABLE t RENAME TO new_t",
        "ALTER TABLE t RENAME COLUMN a TO b",
        "ALTER TABLE t DROP COLUMN a",
    ];
    for sql in &sqls {
        let result = Parser::new(sql);
        let _ = result;
    }
}

// ============================================================================
// INSERT Statement Tests
// ============================================================================

#[test]
fn test_insert_boundaries() {
    let sqls = vec![
        "INSERT INTO t VALUES (1)",
        "INSERT INTO t VALUES (1, 2, 3)",
        "INSERT INTO t (a) VALUES (1)",
        "INSERT INTO t (a, b, c) VALUES (1, 2, 3)",
        "INSERT INTO t SELECT * FROM s",
        "INSERT OR REPLACE INTO t VALUES (1)",
        "REPLACE INTO t VALUES (1)",
    ];
    for sql in &sqls {
        let result = Parser::new(sql);
        let _ = result;
    }
}

#[test]
fn test_insert_multiple_values() {
    let sqls = vec![
        "INSERT INTO t VALUES (1), (2)",
        "INSERT INTO t VALUES (1), (2), (3), (4), (5)",
    ];
    for sql in &sqls {
        let result = Parser::new(sql);
        let _ = result;
    }
}

// ============================================================================
// UPDATE Statement Tests
// ============================================================================

#[test]
fn test_update_boundaries() {
    let sqls = vec![
        "UPDATE t SET a = 1",
        "UPDATE t SET a = 1, b = 2",
        "UPDATE t SET a = 1 WHERE id = 1",
        "UPDATE t SET a = b + 1",
        "UPDATE t SET a = (SELECT MAX(x) FROM s)",
    ];
    for sql in &sqls {
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse();
        assert!(result.is_ok(), "Failed for: {}", sql);
    }
}

// ============================================================================
// DELETE Statement Tests
// ============================================================================

#[test]
fn test_delete_boundaries() {
    let sqls = vec![
        "DELETE FROM t",
        "DELETE FROM t WHERE id = 1",
        "DELETE FROM t WHERE id IN (SELECT id FROM s)",
    ];
    for sql in &sqls {
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse();
        assert!(result.is_ok(), "Failed for: {}", sql);
    }
}

// ============================================================================
// Window Function Tests
// ============================================================================

#[test]
fn test_window_function_boundaries() {
    let sqls = vec![
        "SELECT ROW_NUMBER() OVER () FROM t",
        "SELECT ROW_NUMBER() OVER (ORDER BY a) FROM t",
        "SELECT ROW_NUMBER() OVER (PARTITION BY a) FROM t",
        "SELECT SUM(a) OVER (ORDER BY b ROWS UNBOUNDED PRECEDING) FROM t",
        "SELECT LAG(a, 1) OVER (ORDER BY b) FROM t",
        "SELECT LEAD(a, 1, 0) OVER (ORDER BY b) FROM t",
        "SELECT RANK() OVER (PARTITION BY a ORDER BY b) FROM t",
        "SELECT DENSE_RANK() OVER (ORDER BY a) FROM t",
    ];
    for sql in &sqls {
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse();
        assert!(result.is_ok(), "Failed for: {}", sql);
    }
}

// ============================================================================
// Trigger Tests
// ============================================================================

#[test]
fn test_trigger_boundaries() {
    let sqls = vec![
        "CREATE TRIGGER trg BEFORE INSERT ON t BEGIN SELECT 1; END",
        "CREATE TRIGGER trg AFTER UPDATE ON t BEGIN SELECT 1; END",
        "CREATE TRIGGER trg INSTEAD OF DELETE ON v BEGIN SELECT 1; END",
    ];
    for sql in &sqls {
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse();
        assert!(result.is_ok(), "Failed for: {}", sql);
    }
}

// ============================================================================
// View Tests
// ============================================================================

#[test]
fn test_view_boundaries() {
    let sqls = vec![
        "CREATE VIEW v AS SELECT * FROM t",
        "CREATE VIEW v (a, b) AS SELECT x, y FROM t",
        "CREATE VIEW IF NOT EXISTS v AS SELECT * FROM t",
        "CREATE TEMP VIEW v AS SELECT * FROM t",
    ];
    for sql in &sqls {
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse();
        assert!(result.is_ok(), "Failed for: {}", sql);
    }
}

// ============================================================================
// Foreign Key Tests
// ============================================================================

#[test]
fn test_foreign_key_boundaries() {
    let sqls = vec![
        "CREATE TABLE t (a INTEGER REFERENCES s(id))",
        "CREATE TABLE t (a INTEGER, FOREIGN KEY (a) REFERENCES s(id))",
        "CREATE TABLE t (a INTEGER REFERENCES s(id) ON DELETE CASCADE)",
        "CREATE TABLE t (a INTEGER REFERENCES s(id) ON UPDATE SET NULL)",
        "CREATE TABLE t (a INTEGER REFERENCES s(id) ON DELETE CASCADE ON UPDATE CASCADE)",
    ];
    for sql in &sqls {
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse();
        assert!(result.is_ok(), "Failed for: {}", sql);
    }
}

// ============================================================================
// Virtual Table Tests
// ============================================================================

#[test]
fn test_virtual_table_boundaries() {
    let sqls = vec![
        "CREATE VIRTUAL TABLE v USING FTS5(content)",
        "CREATE VIRTUAL TABLE v USING RTREE(id, minX, maxX, minY, maxY)",
    ];
    for sql in &sqls {
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse();
        assert!(result.is_ok(), "Failed for: {}", sql);
    }
}

// ============================================================================
// Vacuum and Analyze Tests
// ============================================================================

#[test]
fn test_vacuum_analyze_boundaries() {
    let sqls = vec![
        "VACUUM",
        "VACUUM main",
        "ANALYZE",
        "ANALYZE t",
        "ANALYZE main.t",
    ];
    for sql in &sqls {
        let result = Parser::new(sql);
        let _ = result;
    }
}

// ============================================================================
// Pragma Tests
// ============================================================================

#[test]
fn test_pragma_boundaries() {
    let sqls = vec![
        "PRAGMA journal_mode",
        "PRAGMA journal_mode = WAL",
        "PRAGMA cache_size = 10000",
        "PRAGMA foreign_keys = ON",
        "PRAGMA user_version",
        "PRAGMA user_version = 1",
    ];
    for sql in &sqls {
        let result = Parser::new(sql);
        let _ = result;
    }
}

// ============================================================================
// EXPLAIN Tests
// ============================================================================

#[test]
fn test_explain_boundaries() {
    let sqls = vec![
        "EXPLAIN SELECT * FROM t",
        "EXPLAIN QUERY PLAN SELECT * FROM t",
    ];
    for sql in &sqls {
        let result = Parser::new(sql);
        let _ = result;
    }
}

// ============================================================================
// ATTACH/DETACH Tests
// ============================================================================

#[test]
fn test_attach_detach_boundaries() {
    let sqls = vec![
        "ATTACH DATABASE 'file.db' AS aux",
        "ATTACH 'file.db' AS aux",
        "DETACH DATABASE aux",
        "DETACH aux",
    ];
    for sql in &sqls {
        let result = Parser::new(sql);
        let _ = result;
    }
}

// ============================================================================
// REINDEX Tests
// ============================================================================

#[test]
fn test_reindex_boundaries() {
    let sqls = vec![
        "REINDEX",
        "REINDEX t",
        "REINDEX COLLATE NOCASE",
        "REINDEX idx",
    ];
    for sql in &sqls {
        let result = Parser::new(sql);
        let _ = result;
    }
}

// ============================================================================
// Compound Query Tests
// ============================================================================

#[test]
fn test_union_boundaries() {
    let sqls = vec![
        "SELECT * FROM t1 UNION SELECT * FROM t2",
        "SELECT * FROM t1 UNION ALL SELECT * FROM t2",
        "SELECT * FROM t1 INTERSECT SELECT * FROM t2",
        "SELECT * FROM t1 EXCEPT SELECT * FROM t2",
        "SELECT * FROM t1 UNION SELECT * FROM t2 UNION SELECT * FROM t3",
    ];
    for sql in &sqls {
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse();
        assert!(result.is_ok(), "Failed for: {}", sql);
    }
}

// ============================================================================
// EXISTS and IN Expression Tests
// ============================================================================

#[test]
fn test_exists_in_boundaries() {
    let sqls = vec![
        "SELECT * FROM t WHERE EXISTS (SELECT 1 FROM s)",
        "SELECT * FROM t WHERE NOT EXISTS (SELECT 1 FROM s)",
        "SELECT * FROM t WHERE a IN (1, 2, 3)",
        "SELECT * FROM t WHERE a IN (SELECT b FROM s)",
        "SELECT * FROM t WHERE a NOT IN (1, 2, 3)",
    ];
    for sql in &sqls {
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse();
        assert!(result.is_ok(), "Failed for: {}", sql);
    }
}

// ============================================================================
// BETWEEN Expression Tests
// ============================================================================

#[test]
fn test_between_boundaries() {
    let sqls = vec![
        "SELECT * FROM t WHERE a BETWEEN 1 AND 10",
        "SELECT * FROM t WHERE a NOT BETWEEN 1 AND 10",
        "SELECT * FROM t WHERE a BETWEEN 'a' AND 'z'",
        "SELECT * FROM t WHERE a BETWEEN 1.0 AND 10.0",
    ];
    for sql in &sqls {
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse();
        assert!(result.is_ok(), "Failed for: {}", sql);
    }
}

// ============================================================================
// LIKE Expression Tests
// ============================================================================

#[test]
fn test_like_boundaries() {
    let sqls = vec![
        "SELECT * FROM t WHERE a LIKE 'pattern'",
        "SELECT * FROM t WHERE a NOT LIKE 'pattern'",
        "SELECT * FROM t WHERE a LIKE 'pattern' ESCAPE '\\'",
        "SELECT * FROM t WHERE a GLOB 'pattern'",
        "SELECT * FROM t WHERE a NOT GLOB 'pattern'",
    ];
    for sql in &sqls {
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse();
        assert!(result.is_ok(), "Failed for: {}", sql);
    }
}

// ============================================================================
// IS NULL / IS NOT NULL Tests
// ============================================================================

#[test]
fn test_is_null_boundaries() {
    let sqls = vec![
        "SELECT * FROM t WHERE a IS NULL",
        "SELECT * FROM t WHERE a IS NOT NULL",
        "SELECT * FROM t WHERE a = NULL",
        "SELECT * FROM t WHERE a != NULL",
    ];
    for sql in &sqls {
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse();
        assert!(result.is_ok(), "Failed for: {}", sql);
    }
}

// ============================================================================
// CAST Expression Tests
// ============================================================================

#[test]
fn test_cast_boundaries() {
    let sqls = vec![
        "SELECT CAST(a AS INTEGER) FROM t",
        "SELECT CAST(a AS TEXT) FROM t",
        "SELECT CAST(a AS REAL) FROM t",
        "SELECT CAST(a AS BLOB) FROM t",
        "SELECT CAST(a AS NUMERIC) FROM t",
    ];
    for sql in &sqls {
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse();
        assert!(result.is_ok(), "Failed for: {}", sql);
    }
}

// ============================================================================
// Parameter Tests
// ============================================================================

#[test]
fn test_parameter_boundaries() {
    let sqls = vec![
        "SELECT * FROM t WHERE a = ?",
        "SELECT * FROM t WHERE a = ?1",
        "SELECT * FROM t WHERE a = :name",
        "SELECT * FROM t WHERE a = @name",
        "SELECT * FROM t WHERE a = $name",
    ];
    for sql in &sqls {
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse();
        assert!(result.is_ok(), "Failed for: {}", sql);
    }
}

// ============================================================================
// Distinct Tests
// ============================================================================

#[test]
fn test_distinct_boundaries() {
    let sqls = vec![
        "SELECT DISTINCT * FROM t",
        "SELECT DISTINCT a FROM t",
        "SELECT DISTINCT a, b FROM t",
        "SELECT ALL * FROM t",
    ];
    for sql in &sqls {
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse();
        assert!(result.is_ok(), "Failed for: {}", sql);
    }
}

// ============================================================================
// Table Alias Tests
// ============================================================================

#[test]
fn test_table_alias_boundaries() {
    let sqls = vec![
        "SELECT * FROM t AS alias",
        "SELECT * FROM t alias",
        "SELECT alias.* FROM t AS alias",
        "SELECT * FROM t1 a JOIN t2 b ON a.id = b.id",
    ];
    for sql in &sqls {
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse();
        assert!(result.is_ok(), "Failed for: {}", sql);
    }
}

// ============================================================================
// Qualified Column Tests
// ============================================================================

#[test]
fn test_qualified_column_boundaries() {
    let sqls = vec![
        "SELECT t.a FROM t",
        "SELECT t.* FROM t",
        "SELECT db.t.a FROM t",
        "SELECT alias.col FROM t AS alias",
    ];
    for sql in &sqls {
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse();
        assert!(result.is_ok(), "Failed for: {}", sql);
    }
}

// ============================================================================
// Conflict Clause Tests
// ============================================================================

#[test]
fn test_conflict_clause_boundaries() {
    let sqls = vec![
        "INSERT OR IGNORE INTO t VALUES (1)",
        "INSERT OR REPLACE INTO t VALUES (1)",
        "INSERT OR ABORT INTO t VALUES (1)",
        "INSERT OR ROLLBACK INTO t VALUES (1)",
        "INSERT OR FAIL INTO t VALUES (1)",
        "REPLACE INTO t VALUES (1)",
        "UPDATE OR IGNORE t SET a = 1",
        "UPDATE OR REPLACE t SET a = 1",
    ];
    for sql in &sqls {
        let result = Parser::new(sql);
        let _ = result;
    }
}

// ============================================================================
// Return Clause Tests (Upsert)
// ============================================================================

#[test]
fn test_upsert_boundaries() {
    let sqls = vec![
        "INSERT INTO t VALUES (1) ON CONFLICT DO NOTHING",
        "INSERT INTO t VALUES (1) ON CONFLICT(a) DO UPDATE SET b = 2",
        "INSERT INTO t VALUES (1) ON CONFLICT DO UPDATE SET b = excluded.b",
    ];
    for sql in &sqls {
        let result = Parser::new(sql);
        let _ = result;
    }
}

// ============================================================================
// Mathematical Function Tests
// ============================================================================

#[test]
fn test_math_function_boundaries() {
    let sqls = vec![
        "SELECT ABS(-1) FROM t",
        "SELECT LENGTH('abc') FROM t",
        "SELECT LOWER('ABC') FROM t",
        "SELECT UPPER('abc') FROM t",
        "SELECT TRIM(' abc ') FROM t",
        "SELECT SUBSTR('abc', 1, 2) FROM t",
        "SELECT REPLACE('abc', 'b', 'x') FROM t",
        "SELECT ROUND(1.5) FROM t",
        "SELECT COALESCE(a, b, c) FROM t",
        "SELECT IFNULL(a, b) FROM t",
        "SELECT NULLIF(a, b) FROM t",
    ];
    for sql in &sqls {
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse();
        assert!(result.is_ok(), "Failed for: {}", sql);
    }
}

// ============================================================================
// Date/Time Function Tests
// ============================================================================

#[test]
fn test_datetime_function_boundaries() {
    let sqls = vec![
        "SELECT DATE('now') FROM t",
        "SELECT TIME('now') FROM t",
        "SELECT DATETIME('now') FROM t",
        "SELECT STRFTIME('%Y-%m-%d', 'now') FROM t",
        "SELECT JULIANDAY('now') FROM t",
        "SELECT UNIXEPOCH('now') FROM t",
    ];
    for sql in &sqls {
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse();
        assert!(result.is_ok(), "Failed for: {}", sql);
    }
}

// ============================================================================
// JSON Function Tests
// ============================================================================

#[test]
fn test_json_function_boundaries() {
    let sqls = vec![
        "SELECT JSON('{}') FROM t",
        "SELECT JSON_ARRAY(1, 2, 3) FROM t",
        "SELECT JSON_OBJECT('a', 1) FROM t",
        "SELECT JSON_EXTRACT('{}', '$.a') FROM t",
        "SELECT JSON_TYPE('{}') FROM t",
        "SELECT JSON_VALID('{}') FROM t",
    ];
    for sql in &sqls {
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse();
        assert!(result.is_ok(), "Failed for: {}", sql);
    }
}

// ============================================================================
// Constraint Tests
// ============================================================================

#[test]
fn test_table_constraint_boundaries() {
    let sqls = vec![
        "CREATE TABLE t (a INTEGER PRIMARY KEY)",
        "CREATE TABLE t (a INTEGER PRIMARY KEY AUTOINCREMENT)",
        "CREATE TABLE t (a INTEGER, PRIMARY KEY (a))",
        "CREATE TABLE t (a INTEGER, b INTEGER, PRIMARY KEY (a, b))",
        "CREATE TABLE t (a INTEGER UNIQUE)",
        "CREATE TABLE t (a INTEGER NOT NULL)",
        "CREATE TABLE t (a INTEGER DEFAULT 0)",
        "CREATE TABLE t (a INTEGER CHECK (a > 0))",
    ];
    for sql in &sqls {
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse();
        assert!(result.is_ok(), "Failed for: {}", sql);
    }
}

// ============================================================================
// Default Value Tests
// ============================================================================

#[test]
fn test_default_value_boundaries() {
    let sqls = vec![
        "CREATE TABLE t (a INTEGER DEFAULT 0)",
        "CREATE TABLE t (a TEXT DEFAULT '')",
        "CREATE TABLE t (a REAL DEFAULT 0.0)",
        "CREATE TABLE t (a TEXT DEFAULT 'hello')",
        "CREATE TABLE t (a INTEGER DEFAULT NULL)",
        "CREATE TABLE t (a INTEGER DEFAULT (1+1))",
        "CREATE TABLE t (a DATETIME DEFAULT CURRENT_TIMESTAMP)",
    ];
    for sql in &sqls {
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse();
        assert!(result.is_ok(), "Failed for: {}", sql);
    }
}

// ============================================================================
// Conflict Resolution Tests
// ============================================================================

#[test]
fn test_conflict_resolution_boundaries() {
    let sqls = vec![
        "CREATE TABLE t (a INTEGER PRIMARY KEY ON CONFLICT IGNORE)",
        "CREATE TABLE t (a INTEGER PRIMARY KEY ON CONFLICT REPLACE)",
        "CREATE TABLE t (a INTEGER PRIMARY KEY ON CONFLICT ABORT)",
        "CREATE TABLE t (a INTEGER PRIMARY KEY ON CONFLICT ROLLBACK)",
        "CREATE TABLE t (a INTEGER PRIMARY KEY ON CONFLICT FAIL)",
    ];
    for sql in &sqls {
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse();
        assert!(result.is_ok(), "Failed for: {}", sql);
    }
}

// ============================================================================
// Generated Column Tests
// ============================================================================

#[test]
fn test_generated_column_boundaries() {
    let sqls = vec![
        "CREATE TABLE t (a INTEGER, b INTEGER AS (a * 2))",
        "CREATE TABLE t (a INTEGER, b INTEGER GENERATED ALWAYS AS (a * 2))",
        "CREATE TABLE t (a INTEGER, b INTEGER GENERATED ALWAYS AS (a * 2) VIRTUAL)",
        "CREATE TABLE t (a INTEGER, b INTEGER GENERATED ALWAYS AS (a * 2) STORED)",
    ];
    for sql in &sqls {
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse();
        assert!(result.is_ok(), "Failed for: {}", sql);
    }
}

// ============================================================================
// Strict Table Tests
// ============================================================================

#[test]
fn test_strict_table_boundaries() {
    let sqls = vec![
        "CREATE TABLE t(a INT) STRICT",
        "CREATE TABLE t(a ANY) STRICT",
        "CREATE TABLE t(a INT, b TEXT) STRICT",
    ];
    for sql in &sqls {
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse();
        assert!(result.is_ok(), "Failed for: {}", sql);
    }
}

// ============================================================================
// WITHOUT ROWID Tests
// ============================================================================

#[test]
fn test_without_rowid_boundaries() {
    let sqls = vec![
        "CREATE TABLE t(a INTEGER PRIMARY KEY) WITHOUT ROWID",
        "CREATE TABLE t(a INTEGER, b TEXT, PRIMARY KEY(a)) WITHOUT ROWID",
    ];
    for sql in &sqls {
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse();
        assert!(result.is_ok(), "Failed for: {}", sql);
    }
}

// ============================================================================
// IF EXISTS / IF NOT EXISTS Tests
// ============================================================================

#[test]
fn test_if_exists_boundaries() {
    let sqls = vec![
        "CREATE TABLE IF NOT EXISTS t (a INTEGER)",
        "DROP TABLE IF EXISTS t",
        "CREATE INDEX IF NOT EXISTS idx ON t(a)",
        "DROP INDEX IF EXISTS idx",
        "CREATE VIEW IF NOT EXISTS v AS SELECT * FROM t",
        "DROP VIEW IF EXISTS v",
        "CREATE TRIGGER IF NOT EXISTS trg BEFORE INSERT ON t BEGIN SELECT 1; END",
        "DROP TRIGGER IF EXISTS trg",
    ];
    for sql in &sqls {
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse();
        assert!(result.is_ok(), "Failed for: {}", sql);
    }
}

// ============================================================================
// TEMPORARY Object Tests
// ============================================================================

#[test]
fn test_temporary_boundaries() {
    let sqls = vec![
        "CREATE TEMP TABLE t (a INTEGER)",
        "CREATE TEMPORARY TABLE t (a INTEGER)",
        "CREATE TEMP VIEW v AS SELECT * FROM t",
        "CREATE TEMP TRIGGER trg BEFORE INSERT ON t BEGIN SELECT 1; END",
    ];
    for sql in &sqls {
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse();
        assert!(result.is_ok(), "Failed for: {}", sql);
    }
}

// ============================================================================
// Schema Qualification Tests
// ============================================================================

#[test]
fn test_schema_qualification_boundaries() {
    let sqls = vec![
        "SELECT * FROM main.t",
        "SELECT * FROM temp.t",
        "SELECT * FROM aux.t",
        "INSERT INTO main.t VALUES (1)",
        "UPDATE main.t SET a = 1",
        "DELETE FROM main.t",
    ];
    for sql in &sqls {
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse();
        assert!(result.is_ok(), "Failed for: {}", sql);
    }
}

// ============================================================================
// Hex Literal Tests
// ============================================================================

#[test]
fn test_hex_literal_boundaries() {
    let sqls = vec![
        "SELECT X'00'",
        "SELECT X'FF'",
        "SELECT X'ABCDEF'",
        "SELECT x'00'",
    ];
    for sql in &sqls {
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse();
        assert!(result.is_ok(), "Failed for: {}", sql);
    }
}

// ============================================================================
// Blob Literal Tests
// ============================================================================

#[test]
fn test_blob_literal_boundaries() {
    let sqls = vec![
        r"SELECT zeroblob(100)",
        r"SELECT randomblob(16)",
    ];
    for sql in &sqls {
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse();
        assert!(result.is_ok(), "Failed for: {}", sql);
    }
}

// ============================================================================
// Subquery as Expression Tests
// ============================================================================

#[test]
fn test_subquery_expression_boundaries() {
    let sqls = vec![
        "SELECT (SELECT MAX(a) FROM s) FROM t",
        "SELECT a FROM t WHERE a = (SELECT MAX(b) FROM s)",
        "SELECT a FROM t WHERE a > ALL (SELECT b FROM s)",
        "SELECT a FROM t WHERE a > SOME (SELECT b FROM s)",
        "SELECT a FROM t WHERE a > ANY (SELECT b FROM s)",
    ];
    for sql in &sqls {
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse();
        assert!(result.is_ok(), "Failed for: {}", sql);
    }
}

// ============================================================================
// Row Value Tests
// ============================================================================

#[test]
fn test_row_value_boundaries() {
    let sqls = vec![
        "SELECT (1, 2, 3)",
        "SELECT * FROM t WHERE (a, b) = (1, 2)",
        "SELECT * FROM t WHERE (a, b) IN ((1, 2), (3, 4))",
        "SELECT * FROM t WHERE ROW(a, b) = (1, 2)",
    ];
    for sql in &sqls {
        let result = Parser::new(sql);
        let _ = result;
    }
}

// ============================================================================
// Table-Valued Function Tests
// ============================================================================

#[test]
fn test_table_valued_function_boundaries() {
    let sqls = vec![
        "SELECT * FROM json_each('{}')",
        "SELECT * FROM json_tree('{}')",
    ];
    for sql in &sqls {
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse();
        assert!(result.is_ok(), "Failed for: {}", sql);
    }
}

// ============================================================================
// Filter Clause Tests
// ============================================================================

#[test]
fn test_filter_clause_boundaries() {
    let sqls = vec![
        "SELECT COUNT(*) FILTER (WHERE a > 0) FROM t",
        "SELECT SUM(a) FILTER (WHERE b = 1) FROM t",
    ];
    for sql in &sqls {
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse();
        assert!(result.is_ok(), "Failed for: {}", sql);
    }
}

// ============================================================================
// Over Clause Tests
// ============================================================================

#[test]
fn test_over_clause_boundaries() {
    let sqls = vec![
        "SELECT SUM(a) OVER () FROM t",
        "SELECT SUM(a) OVER (ORDER BY b) FROM t",
        "SELECT SUM(a) OVER (PARTITION BY b) FROM t",
        "SELECT SUM(a) OVER (PARTITION BY b ORDER BY c) FROM t",
        "SELECT SUM(a) OVER w FROM t WINDOW w AS (ORDER BY b)",
    ];
    for sql in &sqls {
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse();
        assert!(result.is_ok(), "Failed for: {}", sql);
    }
}

// ============================================================================
// Window Frame Tests
// ============================================================================

#[test]
fn test_window_frame_boundaries() {
    let sqls = vec![
        "SELECT SUM(a) OVER (ORDER BY b ROWS UNBOUNDED PRECEDING) FROM t",
        "SELECT SUM(a) OVER (ORDER BY b ROWS CURRENT ROW) FROM t",
        "SELECT SUM(a) OVER (ORDER BY b ROWS 1 PRECEDING) FROM t",
        "SELECT SUM(a) OVER (ORDER BY b ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM t",
        "SELECT SUM(a) OVER (ORDER BY b RANGE UNBOUNDED PRECEDING) FROM t",
        "SELECT SUM(a) OVER (ORDER BY b GROUPS UNBOUNDED PRECEDING) FROM t",
    ];
    for sql in &sqls {
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse();
        assert!(result.is_ok(), "Failed for: {}", sql);
    }
}

// ============================================================================
// Named Window Tests
// ============================================================================

#[test]
fn test_named_window_boundaries() {
    let sqls = vec![
        "SELECT SUM(a) OVER w FROM t WINDOW w AS ()",
        "SELECT SUM(a) OVER w FROM t WINDOW w AS (ORDER BY b)",
        "SELECT SUM(a) OVER w FROM t WINDOW w AS (PARTITION BY b)",
        "SELECT SUM(a) OVER w, AVG(b) OVER w FROM t WINDOW w AS (ORDER BY c)",
        "SELECT SUM(a) OVER w1, AVG(b) OVER w2 FROM t WINDOW w1 AS (ORDER BY c), w2 AS (PARTITION BY d)",
    ];
    for sql in &sqls {
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse();
        assert!(result.is_ok(), "Failed for: {}", sql);
    }
}

// ============================================================================
// Frame Exclusion Tests
// ============================================================================

#[test]
fn test_frame_exclusion_boundaries() {
    let sqls = vec![
        "SELECT SUM(a) OVER (ORDER BY b ROWS UNBOUNDED PRECEDING EXCLUDE CURRENT ROW) FROM t",
        "SELECT SUM(a) OVER (ORDER BY b ROWS UNBOUNDED PRECEDING EXCLUDE GROUP) FROM t",
        "SELECT SUM(a) OVER (ORDER BY b ROWS UNBOUNDED PRECEDING EXCLUDE TIES) FROM t",
        "SELECT SUM(a) OVER (ORDER BY b ROWS UNBOUNDED PRECEDING EXCLUDE NO OTHERS) FROM t",
    ];
    for sql in &sqls {
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse();
        assert!(result.is_ok(), "Failed for: {}", sql);
    }
}

// ============================================================================
// Infinity and NaN Tests
// ============================================================================

#[test]
fn test_infinity_nan_boundaries() {
    let sqls = vec![
        "SELECT 1e1000",   // Infinity
        "SELECT -1e1000",  // -Infinity
    ];
    for sql in &sqls {
        let result = Parser::new(sql);
        let _ = result;
    }
}

// ============================================================================
// Empty String Tests
// ============================================================================

#[test]
fn test_empty_string_boundaries() {
    let sqls = vec![
        "SELECT ''",
        "INSERT INTO t VALUES ('')",
        "SELECT * FROM t WHERE a = ''",
    ];
    for sql in &sqls {
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse();
        assert!(result.is_ok(), "Failed for: {}", sql);
    }
}

// ============================================================================
// Negative Number Tests
// ============================================================================

#[test]
fn test_negative_number_boundaries() {
    let sqls = vec![
        "SELECT -1",
        "SELECT -1.5",
        "SELECT -1e10",
        "SELECT a FROM t WHERE b = -1",
        "SELECT a FROM t WHERE b > -100",
    ];
    for sql in &sqls {
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse();
        assert!(result.is_ok(), "Failed for: {}", sql);
    }
}

// ============================================================================
// Boundary Value Tests for Literals
// ============================================================================

#[test]
fn test_literal_boundary_values() {
    let sqls = vec![
        "SELECT 0",
        "SELECT -0",
        "SELECT 1",
        "SELECT -1",
        "SELECT 9223372036854775807",   // i64::MAX
        "SELECT -9223372036854775808",  // i64::MIN
        "SELECT 18446744073709551615",  // u64::MAX
        "SELECT 3.4028235e38",          // f32::MAX
        "SELECT 1.7976931348623157e308", // f64::MAX
        "SELECT 2.2250738585072014e-308", // f64::MIN_POSITIVE
        "SELECT 5e-324",                // f64::MIN
    ];
    for sql in &sqls {
        let result = Parser::new(sql);
        let _ = result;
    }
}

// ============================================================================
// Duplicate Keyword Tests
// ============================================================================

#[test]
fn test_duplicate_keyword_boundaries() {
    let sqls = vec![
        "SELECT DISTINCT DISTINCT * FROM t",
        "SELECT ALL ALL * FROM t",
        "SELECT DISTINCT ALL * FROM t",
        "SELECT ALL DISTINCT * FROM t",
    ];
    for sql in &sqls {
        let result = Parser::new(sql);
        let _ = result;
    }
}

// ============================================================================
// Invalid Syntax Tests
// ============================================================================

#[test]
fn test_invalid_syntax_boundaries() {
    let sqls = vec![
        "SELECT * FROM",
        "SELECT FROM t",
        "SELECT * t",
        "INSERT INTO VALUES (1)",
        "UPDATE SET a = 1",
        "DELETE t WHERE a = 1",
        "CREATE TABLE",
        "DROP",
    ];
    for sql in &sqls {
        let result = Parser::new(sql);
        let _ = result;
    }
}

// ============================================================================
// Whitespace Edge Case Tests
// ============================================================================

#[test]
fn test_whitespace_edge_cases() {
    let sqls = vec![
        "SELECT\t*\tFROM\tt",
        "SELECT\n*\nFROM\nt",
        "SELECT\r\n*\r\nFROM\r\nt",
        "SELECT   *   FROM   t",
        "SELECT*FROM t",
    ];
    for sql in &sqls {
        let result = Parser::new(sql);
        let _ = result;
    }
}

// ============================================================================
// String Concatenation Tests
// ============================================================================

#[test]
fn test_string_concatenation_boundaries() {
    let sqls = vec![
        "SELECT 'a' || 'b'",
        "SELECT 'a' || 'b' || 'c'",
        "SELECT a || b FROM t",
    ];
    for sql in &sqls {
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse();
        assert!(result.is_ok(), "Failed for: {}", sql);
    }
}

// ============================================================================
// Bitwise Operation Tests
// ============================================================================

#[test]
fn test_bitwise_operation_boundaries() {
    let sqls = vec![
        "SELECT a & b FROM t",
        "SELECT a | b FROM t",
        "SELECT ~a FROM t",
        "SELECT a << 1 FROM t",
        "SELECT a >> 1 FROM t",
    ];
    for sql in &sqls {
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse();
        assert!(result.is_ok(), "Failed for: {}", sql);
    }
}

// ============================================================================
// Modulo Operation Tests
// ============================================================================

#[test]
fn test_modulo_boundaries() {
    let sqls = vec![
        "SELECT a % b FROM t",
        "SELECT MOD(a, b) FROM t",
    ];
    for sql in &sqls {
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse();
        assert!(result.is_ok(), "Failed for: {}", sql);
    }
}

// ============================================================================
// Collate Expression Tests
// ============================================================================

#[test]
fn test_collate_boundaries() {
    let sqls = vec![
        "SELECT * FROM t ORDER BY a COLLATE BINARY",
        "SELECT * FROM t ORDER BY a COLLATE NOCASE",
        "SELECT * FROM t ORDER BY a COLLATE RTRIM",
        "SELECT * FROM t WHERE a COLLATE NOCASE = 'A'",
    ];
    for sql in &sqls {
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse();
        assert!(result.is_ok(), "Failed for: {}", sql);
    }
}

// ============================================================================
// Raise Function Tests
// ============================================================================

#[test]
fn test_raise_boundaries() {
    let sqls = vec![
        "SELECT RAISE(IGNORE)",
        "SELECT RAISE(ROLLBACK, 'error')",
        "SELECT RAISE(ABORT, 'error')",
        "SELECT RAISE(FAIL, 'error')",
    ];
    for sql in &sqls {
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse();
        assert!(result.is_ok(), "Failed for: {}", sql);
    }
}

// ============================================================================
// Quoted Identifier Tests
// ============================================================================

#[test]
fn test_quoted_identifier_boundaries() {
    let sqls = vec![
        r#"SELECT * FROM "table""#,
        r#"SELECT "column" FROM t"#,
        r#"SELECT * FROM "schema"."table""#,
        r#"SELECT * FROM """#,  // empty quoted identifier
    ];
    for sql in &sqls {
        let result = Parser::new(sql);
        let _ = result;
    }
}

// ============================================================================
// Backtick Identifier Tests
// ============================================================================

#[test]
fn test_backtick_identifier_boundaries() {
    let sqls = vec![
        "SELECT * FROM `table`",
        "SELECT `column` FROM t",
        "SELECT * FROM `schema`.`table`",
        "SELECT * FROM ``",  // empty backtick identifier
    ];
    for sql in &sqls {
        let result = Parser::new(sql);
        let _ = result;
    }
}

// ============================================================================
// Bracket Identifier Tests
// ============================================================================

#[test]
fn test_bracket_identifier_boundaries() {
    let sqls = vec![
        "SELECT * FROM [table]",
        "SELECT [column] FROM t",
        "SELECT * FROM [schema].[table]",
        "SELECT * FROM []",  // empty bracket identifier
    ];
    for sql in &sqls {
        let result = Parser::new(sql);
        let _ = result;
    }
}

// ============================================================================
// Mixed Quote Style Tests
// ============================================================================

#[test]
fn test_mixed_quote_boundaries() {
    let sqls = vec![
        r#"SELECT * FROM "t1" JOIN `t2` ON "t1".id = `t2`.id"#,
        r#"SELECT "col1", `col2`, [col3] FROM t"#,
    ];
    for sql in &sqls {
        let result = Parser::new(sql);
        let _ = result;
    }
}

// ============================================================================
// SELECT * Tests
// ============================================================================

#[test]
fn test_select_star_boundaries() {
    let sqls = vec![
        "SELECT * FROM t",
        "SELECT t.* FROM t",
        "SELECT a.*, b.* FROM t a JOIN s b ON a.id = b.id",
    ];
    for sql in &sqls {
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse();
        assert!(result.is_ok(), "Failed for: {}", sql);
    }
}

// ============================================================================
// Table Sample Tests
// ============================================================================

#[test]
fn test_table_sample_boundaries() {
    let sqls = vec![
        "SELECT * FROM t TABLESAMPLE BERNOULLI(10)",
        "SELECT * FROM t TABLESAMPLE SYSTEM(5)",
        "SELECT * FROM t TABLESAMPLE BERNOULLI(10) REPEATABLE(1234)",
    ];
    for sql in &sqls {
        let result = Parser::new(sql);
        let _ = result;
    }
}

// ============================================================================
// Not Null Constraint Tests
// ============================================================================

#[test]
fn test_not_null_constraint_boundaries() {
    let sqls = vec![
        "CREATE TABLE t (a INTEGER NOT NULL)",
        "CREATE TABLE t (a INTEGER NOT NULL ON CONFLICT IGNORE)",
        "CREATE TABLE t (a INTEGER NOT NULL ON CONFLICT REPLACE)",
        "CREATE TABLE t (a INTEGER NOT NULL DEFAULT 0)",
    ];
    for sql in &sqls {
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse();
        assert!(result.is_ok(), "Failed for: {}", sql);
    }
}

// ============================================================================
// Unique Constraint Tests
// ============================================================================

#[test]
fn test_unique_constraint_boundaries() {
    let sqls = vec![
        "CREATE TABLE t (a INTEGER UNIQUE)",
        "CREATE TABLE t (a INTEGER UNIQUE ON CONFLICT IGNORE)",
        "CREATE TABLE t (a INTEGER UNIQUE ON CONFLICT REPLACE)",
        "CREATE TABLE t (a INTEGER, b INTEGER, UNIQUE(a, b))",
    ];
    for sql in &sqls {
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse();
        assert!(result.is_ok(), "Failed for: {}", sql);
    }
}

// ============================================================================
// Check Constraint Tests
// ============================================================================

#[test]
fn test_check_constraint_boundaries() {
    let sqls = vec![
        "CREATE TABLE t (a INTEGER CHECK(a > 0))",
        "CREATE TABLE t (a INTEGER CONSTRAINT chk CHECK(a > 0))",
        "CREATE TABLE t (a INTEGER, CONSTRAINT chk CHECK(a > 0))",
        "CREATE TABLE t (a INTEGER, b INTEGER, CHECK(a > b))",
    ];
    for sql in &sqls {
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse();
        assert!(result.is_ok(), "Failed for: {}", sql);
    }
}

// ============================================================================
// Default Constraint Tests
// ============================================================================

#[test]
fn test_default_constraint_boundaries() {
    let sqls = vec![
        "CREATE TABLE t (a INTEGER DEFAULT 0)",
        "CREATE TABLE t (a INTEGER DEFAULT -1)",
        "CREATE TABLE t (a TEXT DEFAULT 'hello')",
        "CREATE TABLE t (a TEXT DEFAULT '')",
        "CREATE TABLE t (a REAL DEFAULT 3.14)",
        "CREATE TABLE t (a INTEGER DEFAULT NULL)",
        "CREATE TABLE t (a INTEGER DEFAULT (1+1))",
        "CREATE TABLE t (a DATETIME DEFAULT CURRENT_TIMESTAMP)",
        "CREATE TABLE t (a DATETIME DEFAULT CURRENT_DATE)",
        "CREATE TABLE t (a DATETIME DEFAULT CURRENT_TIME)",
    ];
    for sql in &sqls {
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse();
        assert!(result.is_ok(), "Failed for: {}", sql);
    }
}

// ============================================================================
// Collation Constraint Tests
// ============================================================================

#[test]
fn test_collation_constraint_boundaries() {
    let sqls = vec![
        "CREATE TABLE t (a TEXT COLLATE BINARY)",
        "CREATE TABLE t (a TEXT COLLATE NOCASE)",
        "CREATE TABLE t (a TEXT COLLATE RTRIM)",
        "CREATE TABLE t (a TEXT COLLATE CUSTOM)",
    ];
    for sql in &sqls {
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse();
        assert!(result.is_ok(), "Failed for: {}", sql);
    }
}

// ============================================================================
// Foreign Key Deferrable Tests
// ============================================================================

#[test]
fn test_fk_deferrable_boundaries() {
    let sqls = vec![
        "CREATE TABLE t (a INTEGER REFERENCES s(id) DEFERRABLE)",
        "CREATE TABLE t (a INTEGER REFERENCES s(id) NOT DEFERRABLE)",
        "CREATE TABLE t (a INTEGER REFERENCES s(id) DEFERRABLE INITIALLY DEFERRED)",
        "CREATE TABLE t (a INTEGER REFERENCES s(id) DEFERRABLE INITIALLY IMMEDIATE)",
    ];
    for sql in &sqls {
        let result = Parser::new(sql);
        let _ = result;
    }
}

// ============================================================================
// Index Ordering Tests
// ============================================================================

#[test]
fn test_index_ordering_boundaries() {
    let sqls = vec![
        "CREATE INDEX idx ON t(a ASC)",
        "CREATE INDEX idx ON t(a DESC)",
        "CREATE INDEX idx ON t(a ASC, b DESC)",
        "CREATE INDEX idx ON t(a COLLATE NOCASE ASC)",
    ];
    for sql in &sqls {
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse();
        assert!(result.is_ok(), "Failed for: {}", sql);
    }
}

// ============================================================================
// Index WHERE Clause Tests
// ============================================================================

#[test]
fn test_index_where_boundaries() {
    let sqls = vec![
        "CREATE INDEX idx ON t(a) WHERE b > 0",
        "CREATE INDEX idx ON t(a) WHERE b IS NOT NULL",
        "CREATE INDEX idx ON t(a) WHERE b = 1 AND c = 2",
    ];
    for sql in &sqls {
        let result = Parser::new(sql);
        let _ = result;
    }
}

// ============================================================================
// Auto-Increment Tests
// ============================================================================

#[test]
fn test_autoincrement_boundaries() {
    let sqls = vec![
        "CREATE TABLE t (a INTEGER PRIMARY KEY AUTOINCREMENT)",
        "CREATE TABLE t (a INTEGER AUTOINCREMENT)",  // without PRIMARY KEY
        "CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)",
    ];
    for sql in &sqls {
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse();
        assert!(result.is_ok(), "Failed for: {}", sql);
    }
}

// ============================================================================
// RowID Alias Tests
// ============================================================================

#[test]
fn test_rowid_alias_boundaries() {
    let sqls = vec![
        "CREATE TABLE t (a INTEGER PRIMARY KEY)",
        "CREATE TABLE t (a INTEGER PRIMARY KEY ON CONFLICT REPLACE)",
        "CREATE TABLE t (rowid INTEGER PRIMARY KEY)",
        "CREATE TABLE t (_rowid_ INTEGER PRIMARY KEY)",
        "CREATE TABLE t (oid INTEGER PRIMARY KEY)",
    ];
    for sql in &sqls {
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse();
        assert!(result.is_ok(), "Failed for: {}", sql);
    }
}

// ============================================================================
// Temporary Table Tests
// ============================================================================

#[test]
fn test_temporary_table_boundaries() {
    let sqls = vec![
        "CREATE TEMP TABLE t (a INTEGER)",
        "CREATE TEMPORARY TABLE t (a INTEGER)",
        "CREATE TEMP TABLE IF NOT EXISTS t (a INTEGER)",
    ];
    for sql in &sqls {
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse();
        assert!(result.is_ok(), "Failed for: {}", sql);
    }
}

// ============================================================================
// Multi-Line Statement Tests
// ============================================================================

#[test]
fn test_multiline_statement_boundaries() {
    let sql = r#"SELECT
        a,
        b,
        c
    FROM
        t
    WHERE
        a = 1
    ORDER BY
        b"#;
    let mut parser = Parser::new(sql).unwrap();
    let result = parser.parse();
    assert!(result.is_ok());
}

// ============================================================================
// Statement with Comments Tests
// ============================================================================

#[test]
fn test_statement_with_comments_boundaries() {
    let sqls = vec![
        "/* comment */ SELECT 1",
        "SELECT /* comment */ 1",
        "SELECT 1 /* comment */",
        "SELECT 1 /* multi\nline\ncomment */",
        "SELECT -- line comment\n 1",
        "-- comment at start\nSELECT 1",
    ];
    for sql in &sqls {
        let result = Parser::new(sql);
        let _ = result;
    }
}

// ============================================================================
// DELETE with LIMIT Tests
// ============================================================================

#[test]
fn test_delete_limit_boundaries() {
    let sqls = vec![
        "DELETE FROM t LIMIT 1",
        "DELETE FROM t WHERE a = 1 LIMIT 10",
        "DELETE FROM t ORDER BY a LIMIT 1",
    ];
    for sql in &sqls {
        let result = Parser::new(sql);
        let _ = result;
    }
}

// ============================================================================
// UPDATE with LIMIT Tests
// ============================================================================

#[test]
fn test_update_limit_boundaries() {
    let sqls = vec![
        "UPDATE t SET a = 1 LIMIT 1",
        "UPDATE t SET a = 1 WHERE b = 2 LIMIT 10",
        "UPDATE t SET a = 1 ORDER BY b LIMIT 1",
    ];
    for sql in &sqls {
        let result = Parser::new(sql);
        let _ = result;
    }
}

// ============================================================================
// Qualified Table Name Tests
// ============================================================================

#[test]
fn test_qualified_table_boundaries() {
    let sqls = vec![
        "SELECT * FROM main.t",
        "SELECT * FROM temp.t",
        "SELECT * FROM aux.t",
        "SELECT * FROM db.schema.table",  // three-part name
    ];
    for sql in &sqls {
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse();
        assert!(result.is_ok(), "Failed for: {}", sql);
    }
}

// ============================================================================
// Index Hints Tests
// ============================================================================

#[test]
fn test_index_hint_boundaries() {
    let sqls = vec![
        "SELECT * FROM t INDEXED BY idx",
        "SELECT * FROM t NOT INDEXED",
    ];
    for sql in &sqls {
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse();
        assert!(result.is_ok(), "Failed for: {}", sql);
    }
}

// ============================================================================
// Transaction Mode Tests
// ============================================================================

#[test]
fn test_transaction_mode_boundaries() {
    let sqls = vec![
        "BEGIN DEFERRED",
        "BEGIN IMMEDIATE",
        "BEGIN EXCLUSIVE",
        "BEGIN DEFERRED TRANSACTION",
        "BEGIN IMMEDIATE TRANSACTION",
        "BEGIN EXCLUSIVE TRANSACTION",
    ];
    for sql in &sqls {
        let result = Parser::new(sql);
        let _ = result;
    }
}

// ============================================================================
// Savepoint Tests
// ============================================================================

#[test]
fn test_savepoint_boundaries_extended() {
    let sqls = vec![
        "SAVEPOINT sp",
        "SAVEPOINT main.sp",
        "RELEASE SAVEPOINT sp",
        "RELEASE sp",
        "ROLLBACK TO SAVEPOINT sp",
        "ROLLBACK TRANSACTION TO SAVEPOINT sp",
    ];
    for sql in &sqls {
        let result = Parser::new(sql);
        let _ = result;
    }
}

// ============================================================================
// RETURNING Clause Tests
// ============================================================================

#[test]
fn test_returning_boundaries() {
    let sqls = vec![
        "INSERT INTO t VALUES (1) RETURNING *",
        "INSERT INTO t VALUES (1) RETURNING a",
        "INSERT INTO t VALUES (1) RETURNING a, b, c",
        "UPDATE t SET a = 1 RETURNING *",
        "DELETE FROM t RETURNING *",
    ];
    for sql in &sqls {
        let result = Parser::new(sql);
        let _ = result;
    }
}

// ============================================================================
// Truncate Optimization Tests
// ============================================================================

#[test]
fn test_truncate_boundaries() {
    let sqls = vec![
        "DELETE FROM t",  // may be optimized as truncate
        "DELETE FROM t WHERE 1",  // conditional delete
    ];
    for sql in &sqls {
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse();
        assert!(result.is_ok(), "Failed for: {}", sql);
    }
}

// ============================================================================
// Select Expression Tests
// ============================================================================

#[test]
fn test_select_expression_boundaries() {
    let sqls = vec![
        "SELECT (SELECT 1) + (SELECT 2)",
        "SELECT CASE WHEN EXISTS(SELECT 1) THEN 1 ELSE 0 END",
        "SELECT EXISTS(SELECT 1) AND EXISTS(SELECT 2)",
    ];
    for sql in &sqls {
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse();
        assert!(result.is_ok(), "Failed for: {}", sql);
    }
}

// ============================================================================
// Subquery in FROM Tests
// ============================================================================

#[test]
fn test_subquery_from_boundaries() {
    let sqls = vec![
        "SELECT * FROM (SELECT * FROM t)",
        "SELECT * FROM (SELECT * FROM t) AS sub",
        "SELECT * FROM (SELECT a, b FROM t) AS sub(x, y)",
    ];
    for sql in &sqls {
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse();
        assert!(result.is_ok(), "Failed for: {}", sql);
    }
}

// ============================================================================
// VALUES Clause Tests
// ============================================================================

#[test]
fn test_values_boundaries() {
    let sqls = vec![
        "VALUES (1)",
        "VALUES (1, 2, 3)",
        "VALUES (1), (2), (3)",
        "VALUES (1, 'a'), (2, 'b')",
        "SELECT * FROM (VALUES (1), (2), (3)) AS t(x)",
    ];
    for sql in &sqls {
        let result = Parser::new(sql);
        let _ = result;
    }
}

// ============================================================================
// Recursive CTE Tests
// ============================================================================

#[test]
fn test_recursive_cte_boundaries() {
    let sqls = vec![
        "WITH RECURSIVE cte AS (SELECT 1 UNION ALL SELECT n+1 FROM cte WHERE n < 10) SELECT * FROM cte",
        "WITH RECURSIVE cte(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM cte WHERE n < 10) SELECT * FROM cte",
        "WITH RECURSIVE c1 AS (SELECT 1), c2 AS (SELECT * FROM c1) SELECT * FROM c2",
    ];
    for sql in &sqls {
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse();
        assert!(result.is_ok(), "Failed for: {}", sql);
    }
}

// ============================================================================
// Ordinary CTE Tests
// ============================================================================

#[test]
fn test_ordinary_cte_boundaries() {
    let sqls = vec![
        "WITH cte AS (SELECT 1) SELECT * FROM cte",
        "WITH cte AS (SELECT 1), cte2 AS (SELECT 2) SELECT * FROM cte, cte2",
        "WITH cte(x) AS (SELECT 1) SELECT * FROM cte",
        "WITH cte(x, y) AS (SELECT 1, 2) SELECT * FROM cte",
    ];
    for sql in &sqls {
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse();
        assert!(result.is_ok(), "Failed for: {}", sql);
    }
}

// ============================================================================
// Materialized CTE Tests
// ============================================================================

#[test]
fn test_materialized_cte_boundaries() {
    let sqls = vec![
        "WITH cte AS MATERIALIZED (SELECT 1) SELECT * FROM cte",
        "WITH cte AS NOT MATERIALIZED (SELECT 1) SELECT * FROM cte",
    ];
    for sql in &sqls {
        let result = Parser::new(sql);
        let _ = result;
    }
}

// ============================================================================
// CTE Column Count Mismatch Tests
// ============================================================================

#[test]
fn test_cte_column_mismatch_boundaries() {
    let sqls = vec![
        "WITH cte(a, b) AS (SELECT 1) SELECT * FROM cte",  // fewer columns
        "WITH cte(a) AS (SELECT 1, 2) SELECT * FROM cte",  // more columns in query
    ];
    for sql in &sqls {
        let result = Parser::new(sql);
        let _ = result;
    }
}

// ============================================================================
// Null Handling Tests
// ============================================================================

#[test]
fn test_null_handling_boundaries() {
    let sqls = vec![
        "SELECT NULL",
        "SELECT NULL IS NULL",
        "SELECT NULL IS NOT NULL",
        "SELECT a IS NULL FROM t",
        "SELECT a IS NOT NULL FROM t",
        "SELECT a = NULL FROM t",
        "SELECT a != NULL FROM t",
        "SELECT NULLIF(a, b) FROM t",
        "SELECT COALESCE(a, b, NULL, c) FROM t",
        "SELECT IFNULL(a, NULL) FROM t",
    ];
    for sql in &sqls {
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse();
        assert!(result.is_ok(), "Failed for: {}", sql);
    }
}

// ============================================================================
// Boolean Literal Tests
// ============================================================================

#[test]
fn test_boolean_literal_boundaries() {
    let sqls = vec![
        "SELECT TRUE",
        "SELECT FALSE",
        "SELECT a = TRUE FROM t",
        "SELECT a = FALSE FROM t",
    ];
    for sql in &sqls {
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse();
        assert!(result.is_ok(), "Failed for: {}", sql);
    }
}

// ============================================================================
// CURRENT_* Function Tests
// ============================================================================

#[test]
fn test_current_function_boundaries() {
    let sqls = vec![
        "SELECT CURRENT_TIMESTAMP",
        "SELECT CURRENT_DATE",
        "SELECT CURRENT_TIME",
    ];
    for sql in &sqls {
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse();
        assert!(result.is_ok(), "Failed for: {}", sql);
    }
}

// ============================================================================
// Last Insert RowID Tests
// ============================================================================

#[test]
fn test_last_insert_rowid_boundaries() {
    let sqls = vec![
        "SELECT last_insert_rowid()",
        "SELECT changes()",
        "SELECT total_changes()",
    ];
    for sql in &sqls {
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse();
        assert!(result.is_ok(), "Failed for: {}", sql);
    }
}

// ============================================================================
// Random Function Tests
// ============================================================================

#[test]
fn test_random_boundaries() {
    let sqls = vec![
        "SELECT random()",
        "SELECT randomblob(16)",
        "SELECT random() % 100",
        "SELECT ABS(RANDOM()) % 100",
    ];
    for sql in &sqls {
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse();
        assert!(result.is_ok(), "Failed for: {}", sql);
    }
}

// ============================================================================
// Type Conversion Tests
// ============================================================================

#[test]
fn test_type_conversion_boundaries() {
    let sqls = vec![
        "SELECT CAST(1 AS INTEGER)",
        "SELECT CAST(1 AS TEXT)",
        "SELECT CAST('1' AS INTEGER)",
        "SELECT CAST('3.14' AS REAL)",
        "SELECT CAST(1 AS BLOB)",
        "SELECT CAST(NULL AS INTEGER)",
        "SELECT CAST(x'00' AS TEXT)",
        "SELECT typeof(1)",
        "SELECT typeof('a')",
        "SELECT typeof(NULL)",
    ];
    for sql in &sqls {
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse();
        assert!(result.is_ok(), "Failed for: {}", sql);
    }
}

// ============================================================================
// Quoted String Tests
// ============================================================================

#[test]
fn test_quoted_string_boundaries() {
    let sqls = vec![
        r#"SELECT 'hello'"#,
        r#"SELECT 'hello''world'"#,  // escaped quote
        r#"SELECT ''''"#,  // single quote
        r#"SELECT ''"#,  // empty string
        r#"SELECT 'line1
line2'"#,  // embedded newline
    ];
    for sql in &sqls {
        let result = Parser::new(sql);
        let _ = result;
    }
}

// ============================================================================
// Identifier Length Tests
// ============================================================================

#[test]
fn test_identifier_length_boundaries() {
    let short = "a";
    let medium = "a".repeat(64);
    let long = "a".repeat(256);
    let very_long = "a".repeat(1000);
    
    let sqls = vec![
        format!("SELECT * FROM {}", short),
        format!("SELECT * FROM {}", medium),
        format!("SELECT * FROM {}", long),
        format!("SELECT * FROM {}", very_long),
    ];
    for sql in &sqls {
        let result = Parser::new(&sql);
        let _ = result;
    }
}

// ============================================================================
// Semicolon Position Tests
// ============================================================================

#[test]
fn test_semicolon_position_boundaries() {
    let sqls = vec![
        "SELECT 1;",
        "SELECT 1 ;",
        "SELECT 1; ",
        "SELECT 1 ; ",
        "SELECT 1;SELECT 2",
    ];
    for sql in &sqls {
        let result = Parser::new(sql);
        let _ = result;
    }
}

// ============================================================================
// Trailing Comma Tests
// ============================================================================

#[test]
fn test_trailing_comma_boundaries() {
    let sqls = vec![
        "SELECT a, FROM t",  // invalid trailing comma
        "SELECT a , FROM t",  // space before comma
        "CREATE TABLE t (a INTEGER,)",  // trailing comma in column list
    ];
    for sql in &sqls {
        let result = Parser::new(sql);
        let _ = result;
    }
}

// ============================================================================
// Duplicate Table/Column Tests
// ============================================================================

#[test]
fn test_duplicate_boundaries() {
    let sqls = vec![
        "SELECT a, a FROM t",  // duplicate column
        "CREATE TABLE t (a INTEGER, a TEXT)",  // duplicate column definition
    ];
    for sql in &sqls {
        let result = Parser::new(sql);
        let _ = result;
    }
}

// ============================================================================
// SQL Injection Pattern Tests
// ============================================================================

#[test]
fn test_sql_injection_pattern_boundaries() {
    let sqls = vec![
        "SELECT * FROM t WHERE a = '1 OR 1=1'",
        "SELECT * FROM t WHERE a = '1'; DROP TABLE t; --'",
        "SELECT * FROM t WHERE a = '' OR ''=''",
    ];
    for sql in &sqls {
        let result = Parser::new(sql);
        // Parser should handle these without panic (even if semantically wrong)
        let _ = result;
    }
}

// ============================================================================
// Zero Width Character Tests
// ============================================================================

#[test]
fn test_zero_width_character_boundaries() {
    // Zero-width space, zero-width joiner, etc.
    let sqls = vec![
        "SELECT\u{200B}*\u{200B}FROM\u{200B}t",  // zero-width space
        "SELECT\u{200C}a\u{200D}FROM t",  // zero-width non-joiner/joiner
    ];
    for sql in &sqls {
        let result = Parser::new(sql);
        let _ = result;
    }
}

// ============================================================================
// Right-to-Left Character Tests
// ============================================================================

#[test]
fn test_rtl_character_boundaries() {
    let sqls = vec![
        "SELECT 'العربية' FROM t",  // Arabic
        "SELECT 'עברית' FROM t",      // Hebrew
    ];
    for sql in &sqls {
        let result = Parser::new(sql);
        let _ = result;
    }
}

// ============================================================================
// Combining Character Tests
// ============================================================================

#[test]
fn test_combining_character_boundaries() {
    let sqls = vec![
        "SELECT 'café' FROM t",  // é as single character
        "SELECT 'cafe\u{0301}' FROM t",  // é as e + combining acute
    ];
    for sql in &sqls {
        let result = Parser::new(sql);
        let _ = result;
    }
}

// ============================================================================
// Emoji Tests
// ============================================================================

#[test]
fn test_emoji_boundaries() {
    let sqls = vec![
        "SELECT '😀' FROM t",
        "SELECT '👨‍👩‍👧‍👦' FROM t",  // family emoji (with ZWJ)
        "SELECT '🏳️‍🌈' FROM t",  // flag emoji
    ];
    for sql in &sqls {
        let result = Parser::new(sql);
        let _ = result;
    }
}

// ============================================================================
// Control Character Tests
// ============================================================================

#[test]
fn test_control_character_boundaries() {
    let sqls = vec![
        "SELECT 'a\x00b' FROM t",  // null byte
        "SELECT 'a\x01b' FROM t",  // start of heading
        "SELECT 'a\x7Fb' FROM t",  // DEL character
        "SELECT 'a\x1Fb' FROM t",  // unit separator
    ];
    for sql in &sqls {
        let result = Parser::new(sql);
        let _ = result;
    }
}

// ============================================================================
// BOM Character Tests
// ============================================================================

#[test]
fn test_bom_character_boundaries() {
    let sql_with_bom = "\u{FEFF}SELECT 1 FROM t";
    let result = Parser::new(sql_with_bom);
    let _ = result;
}

// ============================================================================
// Tab Character Tests
// ============================================================================

#[test]
fn test_tab_character_boundaries() {
    let sqls = vec![
        "SELECT\t*\tFROM\tt",
        "SELECT\t\t*\t\tFROM\t\tt",
    ];
    for sql in &sqls {
        let result = Parser::new(sql);
        let _ = result;
    }
}

// ============================================================================
// Carriage Return Tests
// ============================================================================

#[test]
fn test_carriage_return_boundaries() {
    let sqls = vec![
        "SELECT\r*\rFROM\rt",
        "SELECT\r\n*\r\nFROM\r\nt",  // Windows line ending
    ];
    for sql in &sqls {
        let result = Parser::new(sql);
        let _ = result;
    }
}

// ============================================================================
// Form Feed Tests
// ============================================================================

#[test]
fn test_form_feed_boundaries() {
    let sql = "SELECT\x0C*\x0CFROM\x0Ct";
    let result = Parser::new(sql);
    let _ = result;
}

// ============================================================================
// Vertical Tab Tests
// ============================================================================

#[test]
fn test_vertical_tab_boundaries() {
    let sql = "SELECT\x0B*\x0BFROM\x0Bt";
    let result = Parser::new(sql);
    let _ = result;
}

// ============================================================================
// Non-Breaking Space Tests
// ============================================================================

#[test]
fn test_nbsp_boundaries() {
    let sqls = vec![
        "SELECT\u{00A0}*\u{00A0}FROM\u{00A0}t",  // non-breaking space
        "SELECT\u{202F}*\u{202F}FROM\u{202F}t",  // narrow no-break space
    ];
    for sql in &sqls {
        let result = Parser::new(sql);
        let _ = result;
    }
}

// ============================================================================
// Line Separator Tests
// ============================================================================

#[test]
fn test_line_separator_boundaries() {
    let sqls = vec![
        "SELECT\u{2028}*\u{2028}FROM\u{2028}t",  // line separator
        "SELECT\u{2029}*\u{2029}FROM\u{2029}t",  // paragraph separator
    ];
    for sql in &sqls {
        let result = Parser::new(sql);
        let _ = result;
    }
}

// ============================================================================
// Full-Width Character Tests
// ============================================================================

#[test]
fn test_fullwidth_character_boundaries() {
    let sqls = vec![
        "SELECT １ FROM t",  // full-width 1
        "SELECT Ａ FROM t",  // full-width A
        "SELECT ａ FROM t",  // full-width a
    ];
    for sql in &sqls {
        let result = Parser::new(sql);
        let _ = result;
    }
}

// ============================================================================
// Wide Parenthesis Tests
// ============================================================================

#[test]
fn test_wide_parenthesis_boundaries() {
    let sqls = vec![
        "SELECT （１） FROM t",  // full-width parentheses
    ];
    for sql in &sqls {
        let result = Parser::new(sql);
        let _ = result;
    }
}

// ============================================================================
// IDEOGRAPHIC SPACE Tests
// ============================================================================

#[test]
fn test_ideographic_space_boundaries() {
    let sql = "SELECT\u{3000}*\u{3000}FROM\u{3000}t";  // ideographic space
    let result = Parser::new(sql);
    let _ = result;
}

// ============================================================================
// Surrogate Pair Tests (if applicable)
// ============================================================================

#[test]
fn test_surrogate_pair_boundaries() {
    // Characters outside BMP
    let sqls = vec![
        "SELECT '𐍈' FROM t",   // Gothic letter
        "SELECT '𠜎' FROM t",   // CJK extension B
        "SELECT '𝄞' FROM t",   // Musical symbol
    ];
    for sql in &sqls {
        let result = Parser::new(sql);
        let _ = result;
    }
}

// ============================================================================
// Private Use Area Tests
// ============================================================================

#[test]
fn test_private_use_area_boundaries() {
    let sqls = vec![
        "SELECT '\u{E000}' FROM t",   // private use
        "SELECT '\u{F8FF}' FROM t",   // private use (end of BMP)
    ];
    for sql in &sqls {
        let result = Parser::new(sql);
        let _ = result;
    }
}

// ============================================================================
// Invalid UTF-8 Sequence Tests
// ============================================================================

#[test]
fn test_invalid_utf8_boundaries() {
    // These tests are for the tokenizer's handling of invalid input
    // Since we're using Rust strings (which are valid UTF-8), 
    // we test with characters that might cause issues
    let sqls = vec![
        "SELECT '\u{FFFD}' FROM t",  // replacement character
        "SELECT '\u{FFFE}' FROM t",  // byte order mark (invalid character)
        "SELECT '\u{FFFF}' FROM t",  // byte order mark (invalid character)
    ];
    for sql in &sqls {
        let result = Parser::new(sql);
        let _ = result;
    }
}
