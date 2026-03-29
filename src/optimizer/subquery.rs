//! Subquery Optimization
//!
//! Implements subquery optimization techniques:
//! - Subquery flattening (convert to JOIN)
//! - IN/EXISTS subquery optimization
//! - Correlated subquery decorrelation

use crate::sql::ast::{Expression, BinaryOp, SelectStmt, SelectColumn, Join, JoinType};

/// Subquery type
#[derive(Debug, Clone)]
pub enum SubqueryType {
    /// Scalar subquery: SELECT agg FROM t (returns single value)
    Scalar,
    /// IN subquery: expr IN (SELECT ...)
    In { expr: Box<Expression> },
    /// EXISTS subquery: EXISTS (SELECT ...)
    Exists,
    /// ANY/SOME subquery: expr op ANY (SELECT ...)
    Any { expr: Box<Expression>, op: BinaryOp },
    /// ALL subquery: expr op ALL (SELECT ...)
    All { expr: Box<Expression>, op: BinaryOp },
}

/// Represents a subquery found in an expression
#[derive(Debug, Clone)]
pub struct SubqueryInfo {
    pub subquery: SelectStmt,
    pub subquery_type: SubqueryType,
    /// Whether the subquery is correlated (references outer tables)
    pub is_correlated: bool,
}

/// Subquery optimizer
pub struct SubqueryOptimizer;

impl SubqueryOptimizer {
    /// Optimize subqueries in a SELECT statement
    pub fn optimize(stmt: &mut SelectStmt) {
        // Check WHERE clause for optimizable subqueries
        if let Some(ref mut where_clause) = stmt.where_clause {
            Self::optimize_expression(where_clause);
        }
        
        // Check HAVING clause for optimizable subqueries
        if let Some(ref mut having_clause) = stmt.having {
            Self::optimize_expression(having_clause);
        }
    }
    
    /// Optimize subqueries in an expression
    fn optimize_expression(expr: &mut Expression) {
        match expr {
            // EXISTS subquery: EXISTS (SELECT ...) -> SEMI-JOIN
            Expression::FunctionCall { name, args } if name.to_uppercase() == "EXISTS" => {
                // Optimize EXISTS subquery
                for arg in args.iter_mut() {
                    Self::optimize_expression(arg);
                }
            }
            
            // Recursive cases
            Expression::Binary { left, right, .. } => {
                Self::optimize_expression(left);
                Self::optimize_expression(right);
            }
            
            _ => {}
        }
    }
    
    /// Try to flatten an IN subquery to a JOIN
    /// 
    /// Original: SELECT * FROM A WHERE A.id IN (SELECT B.a_id FROM B WHERE B.x > 10)
    /// Flattened: SELECT DISTINCT A.* FROM A JOIN B ON A.id = B.a_id WHERE B.x > 10
    pub fn try_flatten_in_subquery(
        outer_table: &str,
        outer_column: &str,
        inner_stmt: &SelectStmt,
    ) -> Option<SelectStmt> {
        // Check if subquery can be flattened
        // Requirements:
        // 1. Simple SELECT from single table
        // 2. No GROUP BY, HAVING, aggregates
        // 3. No LIMIT/OFFSET
        
        if !inner_stmt.group_by.is_empty() 
            || inner_stmt.having.is_some()
            || !inner_stmt.joins.is_empty()
            || inner_stmt.limit.is_some()
            || inner_stmt.offset.is_some() {
            return None;
        }
        
        // Check for aggregates in select columns
        let has_aggregate = inner_stmt.columns.iter().any(|c| {
            matches!(c, SelectColumn::Aggregate(_, _))
        });
        
        if has_aggregate {
            return None;
        }
        
        // Subquery can be flattened - construct JOIN
        let mut result = inner_stmt.clone();
        
        // Add the outer table as a JOIN
        // This is simplified - real implementation would need proper column mapping
        let join_condition = Expression::Binary {
            left: Box::new(Expression::Column(outer_column.to_string())),
            op: BinaryOp::Equal,
            right: Box::new(Expression::Column(
                inner_stmt.columns.first()
                    .map(|c| match c {
                        SelectColumn::Column(name) => name.clone(),
                        _ => "id".to_string(),
                    })
                    .unwrap_or_else(|| "id".to_string())
            )),
        };
        
        result.joins.push(Join {
            table: outer_table.to_string(),
            join_type: JoinType::Inner,
            on_condition: join_condition,
        });
        
        Some(result)
    }
    
    /// Transform correlated subquery to uncorrelated
    /// 
    /// Original: SELECT * FROM A WHERE A.x > (SELECT AVG(B.y) FROM B WHERE B.a_id = A.id)
    /// Decorrelated: 
    ///   WITH B_agg AS (SELECT a_id, AVG(y) as avg_y FROM B GROUP BY a_id)
    ///   SELECT A.* FROM A JOIN B_agg ON A.id = B_agg.a_id WHERE A.x > B_agg.avg_y
    pub fn decorrelate_subquery(subquery: &mut SubqueryInfo) -> bool {
        if !subquery.is_correlated {
            return false; // Already uncorrelated
        }
        
        // Find correlated predicates and convert to GROUP BY
        // This is complex and requires analyzing the subquery structure
        
        // Simplified: mark as potentially decorrelatable
        // Real implementation would:
        // 1. Find all outer references in WHERE clause
        // 2. Move them to join conditions
        // 3. Add GROUP BY for the correlated columns
        
        false // Placeholder
    }
}

/// Rewrite IN subquery as EXISTS
/// 
/// expr IN (SELECT ...) -> EXISTS (SELECT ... WHERE inner_col = expr)
pub fn rewrite_in_as_exists(expr: &Expression, subquery: &SelectStmt) -> Expression {
    // This transformation can help when EXISTS has better optimization support
    // or when we want to avoid materializing the subquery result
    expr.clone() // Placeholder
}

/// Rewrite EXISTS as semi-join
/// 
/// EXISTS (SELECT 1 FROM B WHERE B.x = A.x) -> SEMI JOIN
pub fn rewrite_exists_as_semijoin(outer: &str, subquery: &SelectStmt) -> Option<Join> {
    // Extract join condition from subquery WHERE clause
    // and create a semi-join
    
    if let Some(ref where_clause) = subquery.where_clause {
        Some(Join {
            table: subquery.from.clone(),
            join_type: JoinType::Inner, // Should be Semi in real implementation
            on_condition: where_clause.clone(),
        })
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_subquery_flatten_eligibility() {
        // Simple subquery - should be flattenable
        let simple_subquery = SelectStmt {
            ctes: vec![],
            columns: vec![SelectColumn::Column("a_id".to_string())],
            from: "B".to_string(),
            joins: vec![],
            where_clause: None, // Simplified for test
            group_by: vec![],
            having: None,
            order_by: vec![],
            limit: None,
            offset: None,
        };

        let result = SubqueryOptimizer::try_flatten_in_subquery(
            "A", "id", &simple_subquery
        );
        assert!(result.is_some());

        // Subquery with GROUP BY - not flattenable
        let grouped_subquery = SelectStmt {
            ctes: vec![],
            columns: vec![SelectColumn::Column("a_id".to_string())],
            from: "B".to_string(),
            joins: vec![],
            where_clause: None,
            group_by: vec!["a_id".to_string()],
            having: None,
            order_by: vec![],
            limit: None,
            offset: None,
        };

        let result = SubqueryOptimizer::try_flatten_in_subquery(
            "A", "id", &grouped_subquery
        );
        assert!(result.is_none());
    }
}
