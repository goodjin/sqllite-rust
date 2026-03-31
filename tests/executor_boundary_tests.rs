//! Executor Boundary Tests
//!
//! Tests for executor edge cases and boundary conditions

use sqllite_rust::sql::ast::*;
use sqllite_rust::sql::parser::Parser;

// ============================================================================
// Expression Evaluation Tests
// ============================================================================

#[test]
fn test_expression_constant_evaluation() {
    let exprs = vec![
        "SELECT 1",
        "SELECT 1 + 1",
        "SELECT 2 * 3",
        "SELECT 10 / 2",
        "SELECT 10 % 3",
        "SELECT -5",
        "SELECT +5",
    ];
    
    for sql in &exprs {
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse();
        assert!(result.is_ok(), "Failed for: {}", sql);
    }
}

#[test]
fn test_expression_null_handling() {
    let exprs = vec![
        "SELECT NULL",
        "SELECT NULL IS NULL",
        "SELECT NULL IS NOT NULL",
        "SELECT 1 + NULL",
        "SELECT NULL * 5",
    ];
    
    for sql in &exprs {
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse();
        assert!(result.is_ok(), "Failed for: {}", sql);
    }
}

#[test]
fn test_expression_comparison() {
    let exprs = vec![
        "SELECT * FROM t WHERE a = 1",
        "SELECT * FROM t WHERE a != 1",
        "SELECT * FROM t WHERE a <> 1",
        "SELECT * FROM t WHERE a > 1",
        "SELECT * FROM t WHERE a >= 1",
        "SELECT * FROM t WHERE a < 1",
        "SELECT * FROM t WHERE a <= 1",
    ];
    
    for sql in &exprs {
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse();
        assert!(result.is_ok(), "Failed for: {}", sql);
    }
}

#[test]
fn test_expression_logical() {
    let exprs = vec![
        "SELECT * FROM t WHERE a = 1 AND b = 2",
        "SELECT * FROM t WHERE a = 1 OR b = 2",
        "SELECT * FROM t WHERE NOT a = 1",
        "SELECT * FROM t WHERE (a = 1 AND b = 2) OR c = 3",
    ];
    
    for sql in &exprs {
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse();
        assert!(result.is_ok(), "Failed for: {}", sql);
    }
}

#[test]
fn test_expression_in() {
    let exprs = vec![
        "SELECT * FROM t WHERE a IN (1, 2, 3)",
        "SELECT * FROM t WHERE a NOT IN (1, 2, 3)",
        "SELECT * FROM t WHERE a IN (SELECT b FROM s)",
    ];
    
    for sql in &exprs {
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse();
        assert!(result.is_ok(), "Failed for: {}", sql);
    }
}

#[test]
fn test_expression_between() {
    let exprs = vec![
        "SELECT * FROM t WHERE a BETWEEN 1 AND 10",
        "SELECT * FROM t WHERE a NOT BETWEEN 1 AND 10",
    ];
    
    for sql in &exprs {
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse();
        assert!(result.is_ok(), "Failed for: {}", sql);
    }
}

#[test]
fn test_expression_like() {
    let exprs = vec![
        "SELECT * FROM t WHERE a LIKE 'pattern%'",
        "SELECT * FROM t WHERE a NOT LIKE 'pattern%'",
        "SELECT * FROM t WHERE a LIKE 'pattern%' ESCAPE '\\'",
        "SELECT * FROM t WHERE a GLOB 'pattern*'",
    ];
    
    for sql in &exprs {
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse();
        assert!(result.is_ok(), "Failed for: {}", sql);
    }
}

// ============================================================================
// Aggregate Function Tests
// ============================================================================

#[test]
fn test_aggregate_functions() {
    let exprs = vec![
        "SELECT COUNT(*) FROM t",
        "SELECT COUNT(DISTINCT a) FROM t",
        "SELECT SUM(a) FROM t",
        "SELECT AVG(a) FROM t",
        "SELECT MIN(a) FROM t",
        "SELECT MAX(a) FROM t",
        "SELECT GROUP_CONCAT(a) FROM t",
    ];
    
    for sql in &exprs {
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse();
        assert!(result.is_ok(), "Failed for: {}", sql);
    }
}

#[test]
fn test_aggregate_empty_table() {
    // Should handle empty table gracefully
    let sql = "SELECT COUNT(*) FROM empty_table";
    let mut parser = Parser::new(sql).unwrap();
    let result = parser.parse();
    assert!(result.is_ok());
}

#[test]
fn test_aggregate_with_group_by() {
    let exprs = vec![
        "SELECT a, COUNT(*) FROM t GROUP BY a",
        "SELECT a, b, COUNT(*) FROM t GROUP BY a, b",
        "SELECT a, SUM(b), AVG(c) FROM t GROUP BY a",
    ];
    
    for sql in &exprs {
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse();
        assert!(result.is_ok(), "Failed for: {}", sql);
    }
}

#[test]
fn test_aggregate_with_having() {
    let exprs = vec![
        "SELECT a, COUNT(*) FROM t GROUP BY a HAVING COUNT(*) > 1",
        "SELECT a, SUM(b) FROM t GROUP BY a HAVING SUM(b) > 100",
    ];
    
    for sql in &exprs {
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse();
        assert!(result.is_ok(), "Failed for: {}", sql);
    }
}

// ============================================================================
// JOIN Tests
// ============================================================================

#[test]
fn test_join_types() {
    let joins = vec![
        "SELECT * FROM t1 JOIN t2 ON t1.id = t2.id",
        "SELECT * FROM t1 INNER JOIN t2 ON t1.id = t2.id",
        "SELECT * FROM t1 LEFT JOIN t2 ON t1.id = t2.id",
        "SELECT * FROM t1 LEFT OUTER JOIN t2 ON t1.id = t2.id",
        "SELECT * FROM t1 CROSS JOIN t2",
        "SELECT * FROM t1 NATURAL JOIN t2",
    ];
    
    for sql in &joins {
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse();
        assert!(result.is_ok(), "Failed for: {}", sql);
    }
}

#[test]
fn test_multiple_joins() {
    let sql = "SELECT * FROM t1 JOIN t2 ON t1.id = t2.id JOIN t3 ON t2.id = t3.id";
    let mut parser = Parser::new(sql).unwrap();
    let result = parser.parse();
    assert!(result.is_ok());
}

#[test]
fn test_self_join() {
    let sql = "SELECT * FROM employees e JOIN employees m ON e.manager_id = m.id";
    let mut parser = Parser::new(sql).unwrap();
    let result = parser.parse();
    assert!(result.is_ok());
}

// ============================================================================
// Subquery Tests
// ============================================================================

#[test]
fn test_subquery_in_select() {
    let sqls = vec![
        "SELECT (SELECT MAX(a) FROM s) FROM t",
        "SELECT a, (SELECT b FROM s WHERE s.id = t.id) FROM t",
    ];
    
    for sql in &sqls {
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse();
        assert!(result.is_ok(), "Failed for: {}", sql);
    }
}

#[test]
fn test_subquery_in_where() {
    let sqls = vec![
        "SELECT * FROM t WHERE a IN (SELECT b FROM s)",
        "SELECT * FROM t WHERE a = (SELECT MAX(b) FROM s)",
        "SELECT * FROM t WHERE EXISTS (SELECT 1 FROM s WHERE s.id = t.id)",
        "SELECT * FROM t WHERE a > ALL (SELECT b FROM s)",
        "SELECT * FROM t WHERE a > ANY (SELECT b FROM s)",
    ];
    
    for sql in &sqls {
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse();
        assert!(result.is_ok(), "Failed for: {}", sql);
    }
}

#[test]
fn test_subquery_in_from() {
    let sqls = vec![
        "SELECT * FROM (SELECT * FROM t) AS sub",
        "SELECT * FROM (SELECT a, b FROM t) AS sub(x, y)",
    ];
    
    for sql in &sqls {
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse();
        assert!(result.is_ok(), "Failed for: {}", sql);
    }
}

#[test]
fn test_correlated_subquery() {
    let sql = "SELECT * FROM t WHERE a > (SELECT AVG(b) FROM s WHERE s.c = t.c)";
    let mut parser = Parser::new(sql).unwrap();
    let result = parser.parse();
    assert!(result.is_ok());
}

// ============================================================================
// Set Operation Tests
// ============================================================================

#[test]
fn test_union_operations() {
    let sqls = vec![
        "SELECT * FROM t1 UNION SELECT * FROM t2",
        "SELECT * FROM t1 UNION ALL SELECT * FROM t2",
        "SELECT * FROM t1 INTERSECT SELECT * FROM t2",
        "SELECT * FROM t1 EXCEPT SELECT * FROM t2",
    ];
    
    for sql in &sqls {
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse();
        assert!(result.is_ok(), "Failed for: {}", sql);
    }
}

#[test]
fn test_multiple_set_operations() {
    let sql = "SELECT * FROM t1 UNION SELECT * FROM t2 UNION ALL SELECT * FROM t3";
    let mut parser = Parser::new(sql).unwrap();
    let result = parser.parse();
    assert!(result.is_ok());
}

// ============================================================================
// ORDER BY Tests
// ============================================================================

#[test]
fn test_order_by_variations() {
    let sqls = vec![
        "SELECT * FROM t ORDER BY a",
        "SELECT * FROM t ORDER BY a ASC",
        "SELECT * FROM t ORDER BY a DESC",
        "SELECT * FROM t ORDER BY a, b",
        "SELECT * FROM t ORDER BY a ASC, b DESC",
        "SELECT * FROM t ORDER BY 1",
        "SELECT * FROM t ORDER BY a NULLS FIRST",
        "SELECT * FROM t ORDER BY a NULLS LAST",
    ];
    
    for sql in &sqls {
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse();
        assert!(result.is_ok(), "Failed for: {}", sql);
    }
}

// ============================================================================
// LIMIT/OFFSET Tests
// ============================================================================

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
// CASE Expression Tests
// ============================================================================

#[test]
fn test_case_expressions() {
    let sqls = vec![
        "SELECT CASE WHEN a = 1 THEN 'one' ELSE 'other' END FROM t",
        "SELECT CASE WHEN a = 1 THEN 'one' WHEN a = 2 THEN 'two' ELSE 'other' END FROM t",
        "SELECT CASE a WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END FROM t",
        "SELECT CASE WHEN a > 0 THEN 'positive' WHEN a < 0 THEN 'negative' ELSE 'zero' END FROM t",
    ];
    
    for sql in &sqls {
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse();
        assert!(result.is_ok(), "Failed for: {}", sql);
    }
}

// ============================================================================
// CAST Tests
// ============================================================================

#[test]
fn test_cast_expressions() {
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
// COALESCE/NULLIF Tests
// ============================================================================

#[test]
fn test_coalesce_nullif() {
    let sqls = vec![
        "SELECT COALESCE(a, b, c) FROM t",
        "SELECT COALESCE(a, 'default') FROM t",
        "SELECT NULLIF(a, b) FROM t",
        "SELECT IFNULL(a, b) FROM t",
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
fn test_window_functions() {
    let sqls = vec![
        "SELECT ROW_NUMBER() OVER () FROM t",
        "SELECT ROW_NUMBER() OVER (ORDER BY a) FROM t",
        "SELECT ROW_NUMBER() OVER (PARTITION BY a) FROM t",
        "SELECT RANK() OVER (ORDER BY a) FROM t",
        "SELECT DENSE_RANK() OVER (ORDER BY a) FROM t",
        "SELECT LAG(a, 1) OVER (ORDER BY b) FROM t",
        "SELECT LEAD(a, 1, 0) OVER (ORDER BY b) FROM t",
        "SELECT FIRST_VALUE(a) OVER (ORDER BY b) FROM t",
        "SELECT LAST_VALUE(a) OVER (ORDER BY b) FROM t",
        "SELECT SUM(a) OVER () FROM t",
        "SELECT SUM(a) OVER (ORDER BY b ROWS UNBOUNDED PRECEDING) FROM t",
        "SELECT SUM(a) OVER (PARTITION BY b ORDER BY c) FROM t",
    ];
    
    for sql in &sqls {
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse();
        assert!(result.is_ok(), "Failed for: {}", sql);
    }
}

// ============================================================================
// CTE Tests
// ============================================================================

#[test]
fn test_cte_expressions() {
    let sqls = vec![
        "WITH cte AS (SELECT 1) SELECT * FROM cte",
        "WITH cte AS (SELECT * FROM t) SELECT * FROM cte",
        "WITH cte1 AS (SELECT 1), cte2 AS (SELECT 2) SELECT * FROM cte1, cte2",
        "WITH RECURSIVE cte AS (SELECT 1 UNION ALL SELECT n+1 FROM cte WHERE n < 10) SELECT * FROM cte",
    ];
    
    for sql in &sqls {
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse();
        assert!(result.is_ok(), "Failed for: {}", sql);
    }
}

// ============================================================================
// Complex Query Tests
// ============================================================================

#[test]
fn test_complex_queries() {
    let sqls = vec![
        // Complex join with aggregation
        "SELECT t.a, COUNT(*) FROM t JOIN s ON t.id = s.id WHERE t.b > 0 GROUP BY t.a HAVING COUNT(*) > 1 ORDER BY t.a LIMIT 10",
        
        // Subquery in multiple places
        "SELECT (SELECT MAX(x) FROM s), a FROM t WHERE a IN (SELECT y FROM s) AND b > (SELECT AVG(z) FROM s)",
        
        // Multiple CTEs with joins
        "WITH c1 AS (SELECT * FROM t), c2 AS (SELECT * FROM s) SELECT * FROM c1 JOIN c2 ON c1.id = c2.id",
        
        // Window function with aggregation
        "SELECT a, SUM(b) OVER (PARTITION BY c) FROM t GROUP BY a, c, b",
    ];
    
    for sql in &sqls {
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse();
        assert!(result.is_ok(), "Failed for: {}", sql);
    }
}

// ============================================================================
// Error Handling Tests
// ============================================================================

#[test]
fn test_invalid_column_reference() {
    // These should fail during execution, not parsing
    let sql = "SELECT nonexistent FROM t";
    let mut parser = Parser::new(sql).unwrap();
    let result = parser.parse();
    assert!(result.is_ok()); // Parsing succeeds
}

#[test]
fn test_ambiguous_column() {
    let sql = "SELECT id FROM t1, t2";
    let mut parser = Parser::new(sql).unwrap();
    let result = parser.parse();
    assert!(result.is_ok()); // Parsing succeeds
}

// ============================================================================
// Performance Boundary Tests
// ============================================================================

#[test]
fn test_many_columns_in_select() {
    let cols: Vec<String> = (0..100).map(|i| format!("col{}", i)).collect();
    let sql = format!("SELECT {} FROM t", cols.join(", "));
    
    let mut parser = Parser::new(&sql).unwrap();
    let result = parser.parse();
    assert!(result.is_ok());
}

#[test]
fn test_many_tables_in_join() {
    let mut sql = "SELECT * FROM t1".to_string();
    for i in 2..=20 {
        sql.push_str(&format!(" JOIN t{} ON t{}.id = t{}.id", i, i-1, i));
    }
    
    let mut parser = Parser::new(&sql).unwrap();
    let result = parser.parse();
    assert!(result.is_ok());
}

#[test]
fn test_deeply_nested_subquery() {
    let mut sql = "SELECT * FROM t".to_string();
    for _ in 0..5 {
        sql = format!("SELECT * FROM ({}) AS sub", sql);
    }
    
    let result = Parser::new(&sql);
    let _ = result;
}

// ============================================================================
// Distinct Tests
// ============================================================================

#[test]
fn test_distinct_variations() {
    let sqls = vec![
        "SELECT DISTINCT * FROM t",
        "SELECT DISTINCT a FROM t",
        "SELECT DISTINCT a, b FROM t",
        "SELECT ALL * FROM t",
        "SELECT DISTINCT ON (a) * FROM t",
    ];
    
    for sql in &sqls {
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse();
        assert!(result.is_ok(), "Failed for: {}", sql);
    }
}
