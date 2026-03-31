//! WHERE Clause Predicate Pushdown Optimization
//!
//! This module implements predicate pushdown optimization to filter records
//! at the storage layer, reducing data transfer and improving query performance.

use crate::sql::ast::{Expression, BinaryOp, ColumnDef};
use crate::storage::{Value, Record};

/// A pushdown filter that can be evaluated at the storage layer
/// 
/// This represents simple predicates that can be efficiently evaluated
/// without the full executor context.
#[derive(Debug, Clone)]
pub enum PushdownFilter {
    /// Equality check: column = value
    Eq { column: String, value: Value },
    /// Not equal: column != value
    NotEq { column: String, value: Value },
    /// Less than: column < value
    Lt { column: String, value: Value },
    /// Less than or equal: column <= value
    Le { column: String, value: Value },
    /// Greater than: column > value
    Gt { column: String, value: Value },
    /// Greater than or equal: column >= value
    Ge { column: String, value: Value },
    /// AND combination of filters
    And(Box<PushdownFilter>, Box<PushdownFilter>),
    /// OR combination of filters (limited support)
    Or(Box<PushdownFilter>, Box<PushdownFilter>),
    /// Always true
    True,
    /// Always false
    False,
}

impl PushdownFilter {
    /// Evaluate the filter against a record
    pub fn evaluate(&self, record: &Record, table_columns: &[ColumnDef]) -> bool {
        match self {
            PushdownFilter::Eq { column, value } => {
                Self::compare_column(record, table_columns, column, |v| v == value)
            }
            PushdownFilter::NotEq { column, value } => {
                Self::compare_column(record, table_columns, column, |v| v != value)
            }
            PushdownFilter::Lt { column, value } => {
                Self::compare_column(record, table_columns, column, |v| v < value)
            }
            PushdownFilter::Le { column, value } => {
                Self::compare_column(record, table_columns, column, |v| v <= value)
            }
            PushdownFilter::Gt { column, value } => {
                Self::compare_column(record, table_columns, column, |v| v > value)
            }
            PushdownFilter::Ge { column, value } => {
                Self::compare_column(record, table_columns, column, |v| v >= value)
            }
            PushdownFilter::And(left, right) => {
                left.evaluate(record, table_columns) && right.evaluate(record, table_columns)
            }
            PushdownFilter::Or(left, right) => {
                left.evaluate(record, table_columns) || right.evaluate(record, table_columns)
            }
            PushdownFilter::True => true,
            PushdownFilter::False => false,
        }
    }

    /// Helper to compare a column value
    fn compare_column<F>(record: &Record, table_columns: &[ColumnDef], column: &str, cmp: F) -> bool
    where
        F: FnOnce(&Value) -> bool,
    {
        if let Some(idx) = table_columns.iter().position(|c| c.name == column) {
            if let Some(value) = record.values.get(idx) {
                return cmp(value);
            }
        }
        false
    }

    /// Check if this filter is simple enough for storage layer evaluation
    /// 
    /// Currently supports single-column comparisons without function calls
    pub fn is_simple(&self) -> bool {
        match self {
            PushdownFilter::Eq { .. } |
            PushdownFilter::NotEq { .. } |
            PushdownFilter::Lt { .. } |
            PushdownFilter::Le { .. } |
            PushdownFilter::Gt { .. } |
            PushdownFilter::Ge { .. } => true,
            PushdownFilter::And(left, right) => left.is_simple() && right.is_simple(),
            PushdownFilter::Or(left, right) => left.is_simple() && right.is_simple(),
            PushdownFilter::True |
            PushdownFilter::False => true,
        }
    }

    /// Get the column names referenced by this filter
    pub fn referenced_columns(&self) -> Vec<String> {
        let mut columns = Vec::new();
        self.collect_columns(&mut columns);
        columns
    }

    fn collect_columns(&self, columns: &mut Vec<String>) {
        match self {
            PushdownFilter::Eq { column, .. } |
            PushdownFilter::NotEq { column, .. } |
            PushdownFilter::Lt { column, .. } |
            PushdownFilter::Le { column, .. } |
            PushdownFilter::Gt { column, .. } |
            PushdownFilter::Ge { column, .. } => {
                if !columns.contains(column) {
                    columns.push(column.clone());
                }
            }
            PushdownFilter::And(left, right) |
            PushdownFilter::Or(left, right) => {
                left.collect_columns(columns);
                right.collect_columns(columns);
            }
            PushdownFilter::True |
            PushdownFilter::False => {}
        }
    }
}

/// Extract a pushdown filter from an AST expression
/// 
/// Returns None if the expression cannot be converted to a pushdown filter
pub fn extract_pushdown_filter(expr: &Expression) -> Option<PushdownFilter> {
    match expr {
        // Handle AND combinations first (before generic Binary)
        Expression::Binary { left, op: BinaryOp::And, right } => {
            let left_filter = extract_pushdown_filter(left)?;
            let right_filter = extract_pushdown_filter(right)?;
            Some(PushdownFilter::And(
                Box::new(left_filter),
                Box::new(right_filter),
            ))
        }
        // Handle OR combinations first (before generic Binary)
        Expression::Binary { left, op: BinaryOp::Or, right } => {
            let left_filter = extract_pushdown_filter(left)?;
            let right_filter = extract_pushdown_filter(right)?;
            Some(PushdownFilter::Or(
                Box::new(left_filter),
                Box::new(right_filter),
            ))
        }
        // Simple comparisons
        Expression::Binary { left, op, right } => {
            extract_comparison(left, op, right)
        }
        _ => None,
    }
}

/// Extract a comparison filter from binary expression
fn extract_comparison(left: &Expression, op: &BinaryOp, right: &Expression) -> Option<PushdownFilter> {
    // Try to extract column = value or value = column
    match (left, op, right) {
        // Column on left, value on right
        (Expression::Column(col), op, value_expr) => {
            let value = expression_to_value(value_expr)?;
            Some(make_comparison_filter(col.clone(), op, value))
        }
        // Value on left, column on right (swap)
        (value_expr, op, Expression::Column(col)) => {
            let value = expression_to_value(value_expr)?;
            // Swap the operator for commutative property
            let swapped_op = swap_operator(op);
            Some(make_comparison_filter(col.clone(), &swapped_op, value))
        }
        _ => None,
    }
}

/// Convert an expression to a value if it's a constant
fn expression_to_value(expr: &Expression) -> Option<Value> {
    match expr {
        Expression::Integer(n) => Some(Value::Integer(*n)),
        Expression::String(s) => Some(Value::Text(s.clone())),
        Expression::Float(f) => Some(Value::Real(*f)),
        Expression::Boolean(b) => Some(if *b { Value::Integer(1) } else { Value::Integer(0) }),
        Expression::Null => Some(Value::Null),
        _ => None,
    }
}

/// Create a comparison filter from operator and value
fn make_comparison_filter(column: String, op: &BinaryOp, value: Value) -> PushdownFilter {
    match op {
        BinaryOp::Equal => PushdownFilter::Eq { column, value },
        BinaryOp::NotEqual => PushdownFilter::NotEq { column, value },
        BinaryOp::Less => PushdownFilter::Lt { column, value },
        BinaryOp::LessEqual => PushdownFilter::Le { column, value },
        BinaryOp::Greater => PushdownFilter::Gt { column, value },
        BinaryOp::GreaterEqual => PushdownFilter::Ge { column, value },
        _ => PushdownFilter::True, // Fallback
    }
}

/// Swap comparison operator for commutative property
/// 
/// For example: 5 > col  becomes  col < 5
fn swap_operator(op: &BinaryOp) -> BinaryOp {
    match op {
        BinaryOp::Less => BinaryOp::Greater,
        BinaryOp::LessEqual => BinaryOp::GreaterEqual,
        BinaryOp::Greater => BinaryOp::Less,
        BinaryOp::GreaterEqual => BinaryOp::LessEqual,
        _ => op.clone(),
    }
}

/// Split a filter into pushdown-able and executor-needed parts
/// 
/// Returns (pushdown_filter, remaining_expression) where:
/// - pushdown_filter can be evaluated at storage layer
/// - remaining_expression needs full executor context
pub fn split_filter(expr: &Expression) -> (Option<PushdownFilter>, Option<Expression>) {
    match extract_pushdown_filter(expr) {
        Some(filter) => (Some(filter), None),
        None => {
            // Try to split AND expressions
            match expr {
                Expression::Binary { left, op: BinaryOp::And, right } => {
                    let (left_pushdown, left_remain) = split_filter(left);
                    let (right_pushdown, right_remain) = split_filter(right);

                    // Combine pushdown filters
                    let combined_pushdown = match (left_pushdown, right_pushdown) {
                        (Some(l), Some(r)) => Some(PushdownFilter::And(Box::new(l), Box::new(r))),
                        (Some(l), None) => Some(l),
                        (None, Some(r)) => Some(r),
                        (None, None) => None,
                    };

                    // Combine remaining expressions
                    let combined_remain = match (left_remain, right_remain) {
                        (Some(l), Some(r)) => Some(Expression::Binary {
                            left: Box::new(l),
                            op: BinaryOp::And,
                            right: Box::new(r),
                        }),
                        (Some(l), None) => Some(l),
                        (None, Some(r)) => Some(r),
                        (None, None) => None,
                    };

                    (combined_pushdown, combined_remain)
                }
                _ => (None, Some(expr.clone())),
            }
        }
    }
}

/// Statistics for predicate pushdown
#[derive(Debug, Clone, Copy, Default)]
pub struct PushdownStats {
    /// Number of records scanned from storage
    pub records_scanned: u64,
    /// Number of records filtered by pushdown
    pub records_filtered: u64,
    /// Number of predicates pushed down
    pub predicates_pushed: u64,
}

impl PushdownStats {
    /// Calculate the filter selectivity (ratio of filtered records)
    pub fn selectivity(&self) -> f64 {
        if self.records_scanned == 0 {
            0.0
        } else {
            self.records_filtered as f64 / self.records_scanned as f64
        }
    }

    /// Calculate the reduction ratio
    pub fn reduction_ratio(&self) -> f64 {
        if self.records_scanned == 0 {
            0.0
        } else {
            1.0 - (self.records_filtered as f64 / self.records_scanned as f64)
        }
    }
}

/// Predicate pushdown optimizer
/// 
/// Analyzes WHERE clauses and extracts filters that can be pushed to storage
pub struct PredicatePushdownOptimizer;

impl PredicatePushdownOptimizer {
    /// Optimize a WHERE clause for pushdown
    /// 
    /// Returns the optimized filter and any remaining expression
    pub fn optimize(expr: &Expression) -> (Option<PushdownFilter>, Option<Expression>) {
        split_filter(expr)
    }

    /// Check if an expression can be fully pushed down
    pub fn is_fully_pushdownable(expr: &Expression) -> bool {
        extract_pushdown_filter(expr).is_some()
    }

    /// Estimate the selectivity of a filter
    /// 
    /// This is a rough heuristic for query optimization
    pub fn estimate_selectivity(filter: &PushdownFilter) -> f64 {
        match filter {
            PushdownFilter::Eq { .. } => 0.1,      // Equality: ~10% selectivity
            PushdownFilter::NotEq { .. } => 0.9,   // Not equal: ~90% selectivity
            PushdownFilter::Lt { .. } |
            PushdownFilter::Le { .. } |
            PushdownFilter::Gt { .. } |
            PushdownFilter::Ge { .. } => 0.5,      // Range: ~50% selectivity
            PushdownFilter::And(left, right) => {
                // Combined selectivity (assuming independence)
                Self::estimate_selectivity(left) * Self::estimate_selectivity(right)
            }
            PushdownFilter::Or(left, right) => {
                // Union of selectivities
                let s1 = Self::estimate_selectivity(left);
                let s2 = Self::estimate_selectivity(right);
                s1 + s2 - (s1 * s2)
            }
            PushdownFilter::True => 1.0,
            PushdownFilter::False => 0.0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_columns() -> Vec<ColumnDef> {
        vec![
            ColumnDef {
                name: "id".to_string(),
                data_type: crate::sql::ast::DataType::Integer,
                nullable: false,
                primary_key: true,
                foreign_key: None,
                default_value: None,
                is_virtual: false,
                generated_always: None,
            },
            ColumnDef {
                name: "age".to_string(),
                data_type: crate::sql::ast::DataType::Integer,
                nullable: true,
                primary_key: false,
                foreign_key: None,
                default_value: None,
                is_virtual: false,
                generated_always: None,
            },
        ]
    }

    #[test]
    fn test_eq_filter() {
        let columns = create_test_columns();
        let filter = PushdownFilter::Eq {
            column: "age".to_string(),
            value: Value::Integer(25),
        };

        let matching = Record::new(vec![Value::Integer(1), Value::Integer(25)]);
        let non_matching = Record::new(vec![Value::Integer(2), Value::Integer(30)]);

        assert!(filter.evaluate(&matching, &columns));
        assert!(!filter.evaluate(&non_matching, &columns));
    }

    #[test]
    fn test_range_filters() {
        let columns = create_test_columns();
        
        // Test Gt
        let gt_filter = PushdownFilter::Gt {
            column: "age".to_string(),
            value: Value::Integer(20),
        };
        let record = Record::new(vec![Value::Integer(1), Value::Integer(25)]);
        assert!(gt_filter.evaluate(&record, &columns));

        // Test Lt
        let lt_filter = PushdownFilter::Lt {
            column: "age".to_string(),
            value: Value::Integer(30),
        };
        assert!(lt_filter.evaluate(&record, &columns));
    }

    #[test]
    fn test_and_filter() {
        let columns = create_test_columns();
        let filter = PushdownFilter::And(
            Box::new(PushdownFilter::Gt {
                column: "age".to_string(),
                value: Value::Integer(18),
            }),
            Box::new(PushdownFilter::Lt {
                column: "age".to_string(),
                value: Value::Integer(65),
            }),
        );

        let matching = Record::new(vec![Value::Integer(1), Value::Integer(25)]);
        let too_young = Record::new(vec![Value::Integer(2), Value::Integer(16)]);
        let too_old = Record::new(vec![Value::Integer(3), Value::Integer(70)]);

        assert!(filter.evaluate(&matching, &columns));
        assert!(!filter.evaluate(&too_young, &columns));
        assert!(!filter.evaluate(&too_old, &columns));
    }

    #[test]
    fn test_extract_simple_comparison() {
        let expr = Expression::Binary {
            left: Box::new(Expression::Column("age".to_string())),
            op: BinaryOp::Greater,
            right: Box::new(Expression::Integer(18)),
        };

        let filter = extract_pushdown_filter(&expr).unwrap();
        
        let columns = create_test_columns();
        let matching = Record::new(vec![Value::Integer(1), Value::Integer(25)]);
        assert!(filter.evaluate(&matching, &columns));
    }

    #[test]
    fn test_extract_and_combination() {
        let expr = Expression::Binary {
            left: Box::new(Expression::Binary {
                left: Box::new(Expression::Column("age".to_string())),
                op: BinaryOp::Greater,
                right: Box::new(Expression::Integer(18)),
            }),
            op: BinaryOp::And,
            right: Box::new(Expression::Binary {
                left: Box::new(Expression::Column("age".to_string())),
                op: BinaryOp::Less,
                right: Box::new(Expression::Integer(65)),
            }),
        };

        let filter = extract_pushdown_filter(&expr).unwrap();
        
        let columns = create_test_columns();
        let matching = Record::new(vec![Value::Integer(1), Value::Integer(25)]);
        assert!(filter.evaluate(&matching, &columns));
    }

    #[test]
    fn test_split_filter() {
        // Test: age > 18 AND name LIKE '%test%' 
        // (second part is not pushdown-able)
        let expr = Expression::Binary {
            left: Box::new(Expression::Binary {
                left: Box::new(Expression::Column("age".to_string())),
                op: BinaryOp::Greater,
                right: Box::new(Expression::Integer(18)),
            }),
            op: BinaryOp::And,
            right: Box::new(Expression::FunctionCall {
                name: "LIKE".to_string(),
                args: vec![
                    Expression::Column("name".to_string()),
                    Expression::String("%test%".to_string()),
                ],
            }),
        };

        let (pushdown, remaining) = split_filter(&expr);

        assert!(pushdown.is_some());
        assert!(remaining.is_some()); // The LIKE expression remains

        let columns = create_test_columns();
        let matching = Record::new(vec![Value::Integer(1), Value::Integer(25)]);
        assert!(pushdown.unwrap().evaluate(&matching, &columns));
    }

    #[test]
    fn test_swap_operator() {
        // Test: 18 < age  should become  age > 18
        let expr = Expression::Binary {
            left: Box::new(Expression::Integer(18)),
            op: BinaryOp::Less,
            right: Box::new(Expression::Column("age".to_string())),
        };

        let filter = extract_pushdown_filter(&expr).unwrap();
        
        let columns = create_test_columns();
        let record = Record::new(vec![Value::Integer(1), Value::Integer(25)]);
        assert!(filter.evaluate(&record, &columns));
    }

    #[test]
    fn test_referenced_columns() {
        let filter = PushdownFilter::And(
            Box::new(PushdownFilter::Eq {
                column: "age".to_string(),
                value: Value::Integer(25),
            }),
            Box::new(PushdownFilter::Gt {
                column: "salary".to_string(),
                value: Value::Integer(50000),
            }),
        );

        let columns = filter.referenced_columns();
        assert!(columns.contains(&"age".to_string()));
        assert!(columns.contains(&"salary".to_string()));
        assert_eq!(columns.len(), 2);
    }

    #[test]
    fn test_estimate_selectivity() {
        let eq_filter = PushdownFilter::Eq {
            column: "id".to_string(),
            value: Value::Integer(1),
        };
        assert!(PredicatePushdownOptimizer::estimate_selectivity(&eq_filter) < 0.5);

        let true_filter = PushdownFilter::True;
        assert_eq!(PredicatePushdownOptimizer::estimate_selectivity(&true_filter), 1.0);

        let false_filter = PushdownFilter::False;
        assert_eq!(PredicatePushdownOptimizer::estimate_selectivity(&false_filter), 0.0);
    }
}
