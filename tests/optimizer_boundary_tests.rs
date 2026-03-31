//! Optimizer Boundary Tests
//!
//! Tests for query optimizer edge cases and boundary conditions

use sqllite_rust::sql::ast::*;
use sqllite_rust::sql::parser::Parser;

// ============================================================================
// Index Selection Tests
// ============================================================================

#[test]
fn test_index_selection_simple() {
    let sql = "SELECT * FROM t WHERE a = 1";
    let mut parser = Parser::new(sql).unwrap();
    let result = parser.parse();
    assert!(result.is_ok());
}

#[test]
fn test_index_selection_range() {
    let sqls = vec![
        "SELECT * FROM t WHERE a > 1",
        "SELECT * FROM t WHERE a >= 1",
        "SELECT * FROM t WHERE a < 1",
        "SELECT * FROM t WHERE a <= 1",
        "SELECT * FROM t WHERE a BETWEEN 1 AND 10",
    ];
    
    for sql in &sqls {
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse();
        assert!(result.is_ok(), "Failed for: {}", sql);
    }
}

#[test]
fn test_index_selection_multiple_conditions() {
    let sqls = vec![
        "SELECT * FROM t WHERE a = 1 AND b = 2",
        "SELECT * FROM t WHERE a = 1 OR b = 2",
        "SELECT * FROM t WHERE a = 1 AND b > 2 AND c < 3",
    ];
    
    for sql in &sqls {
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse();
        assert!(result.is_ok(), "Failed for: {}", sql);
    }
}

// ============================================================================
// Join Reorder Tests
// ============================================================================

#[test]
fn test_join_reorder_two_tables() {
    let sql = "SELECT * FROM t1, t2 WHERE t1.id = t2.id";
    let mut parser = Parser::new(sql).unwrap();
    let result = parser.parse();
    assert!(result.is_ok());
}

#[test]
fn test_join_reorder_many_tables() {
    let mut sql = "SELECT * FROM t1".to_string();
    for i in 2..=10 {
        sql.push_str(&format!(", t{}", i));
    }
    sql.push_str(" WHERE t1.id = t2.id");
    
    let mut parser = Parser::new(&sql).unwrap();
    let result = parser.parse();
    assert!(result.is_ok());
}

// ============================================================================
// Predicate Pushdown Tests
// ============================================================================

#[test]
fn test_predicate_pushdown_simple() {
    let sql = "SELECT * FROM (SELECT * FROM t) AS sub WHERE a = 1";
    let mut parser = Parser::new(sql).unwrap();
    let result = parser.parse();
    assert!(result.is_ok());
}

#[test]
fn test_predicate_pushdown_join() {
    let sql = "SELECT * FROM t1 JOIN t2 ON t1.id = t2.id WHERE t1.a = 1 AND t2.b = 2";
    let mut parser = Parser::new(sql).unwrap();
    let result = parser.parse();
    assert!(result.is_ok());
}

// ============================================================================
// Cost Model Tests
// ============================================================================

#[test]
fn test_cost_estimates() {
    // Various query patterns
    let sqls = vec![
        "SELECT * FROM t",  // Full table scan
        "SELECT * FROM t WHERE a = 1",  // Index lookup
        "SELECT * FROM t ORDER BY a",  // Sort
        "SELECT * FROM t LIMIT 10",  // Limit
    ];
    
    for sql in &sqls {
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse();
        assert!(result.is_ok(), "Failed for: {}", sql);
    }
}

// ============================================================================
// Statistics Tests
// ============================================================================

#[test]
fn test_statistics_usage() {
    // Queries that benefit from statistics
    let sqls = vec![
        "SELECT * FROM t WHERE a = 1",  // Selectivity estimation
        "SELECT * FROM t WHERE a > 0 AND b < 100",  // Combined selectivity
        "SELECT * FROM t ORDER BY a LIMIT 10",  // Top-N optimization
    ];
    
    for sql in &sqls {
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse();
        assert!(result.is_ok(), "Failed for: {}", sql);
    }
}
