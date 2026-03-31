//! Index Pushdown Filter (P3-4)
//!
//! This module implements predicate pushdown for index scans to reduce
//! the number of rows that need to be looked up from the table.
//!
//! Features:
//! - Filter predicates at the index scan level
//! - Reduce unnecessary table lookups (avoid "lookup" for filtered rows)
//! - Support for range predicates and equality conditions
//! - Statistics tracking for optimization effectiveness

use crate::index::Result;
use crate::pager::{PageId, Pager};
use crate::storage::Value;
use crate::sql::ast::{Expression, BinaryOp};

/// Filter predicate for index pushdown
#[derive(Debug, Clone)]
pub enum IndexFilter {
    /// Equality: column = value
    Eq { value: Value },
    /// Range: column > value
    Gt { value: Value },
    /// Range: column >= value  
    Ge { value: Value },
    /// Range: column < value
    Lt { value: Value },
    /// Range: column <= value
    Le { value: Value },
    /// Range between: low <= column < high
    Range { low: Value, high: Value, inclusive_low: bool, inclusive_high: bool },
    /// IN list: column IN (values...)
    In { values: Vec<Value> },
    /// IS NULL
    IsNull,
    /// IS NOT NULL
    IsNotNull,
    /// AND combination
    And(Box<IndexFilter>, Box<IndexFilter>),
    /// OR combination
    Or(Box<IndexFilter>, Box<IndexFilter>),
    /// Always true
    True,
    /// Always false
    False,
}

impl IndexFilter {
    /// Evaluate the filter against a value
    pub fn evaluate(&self, value: &Value) -> bool {
        match self {
            IndexFilter::Eq { value: target } => value == target,
            IndexFilter::Gt { value: target } => value > target,
            IndexFilter::Ge { value: target } => value >= target,
            IndexFilter::Lt { value: target } => value < target,
            IndexFilter::Le { value: target } => value <= target,
            IndexFilter::Range { low, high, inclusive_low, inclusive_high } => {
                let low_ok = if *inclusive_low { value >= low } else { value > low };
                let high_ok = if *inclusive_high { value <= high } else { value < high };
                low_ok && high_ok
            }
            IndexFilter::In { values } => values.contains(value),
            IndexFilter::IsNull => matches!(value, Value::Null),
            IndexFilter::IsNotNull => !matches!(value, Value::Null),
            IndexFilter::And(left, right) => left.evaluate(value) && right.evaluate(value),
            IndexFilter::Or(left, right) => left.evaluate(value) || right.evaluate(value),
            IndexFilter::True => true,
            IndexFilter::False => false,
        }
    }

    /// Check if this filter can be evaluated at the index level
    pub fn is_index_pushdownable(&self) -> bool {
        match self {
            IndexFilter::Eq { .. } |
            IndexFilter::Gt { .. } |
            IndexFilter::Ge { .. } |
            IndexFilter::Lt { .. } |
            IndexFilter::Le { .. } |
            IndexFilter::Range { .. } |
            IndexFilter::In { .. } |
            IndexFilter::IsNull |
            IndexFilter::IsNotNull |
            IndexFilter::True |
            IndexFilter::False => true,
            IndexFilter::And(left, right) => {
                left.is_index_pushdownable() && right.is_index_pushdownable()
            }
            IndexFilter::Or(left, right) => {
                left.is_index_pushdownable() && right.is_index_pushdownable()
            }
        }
    }

    /// Extract range bounds from filter
    pub fn to_range_bounds(&self) -> (Option<Value>, Option<Value>, bool, bool) {
        match self {
            IndexFilter::Eq { value } => {
                (Some(value.clone()), Some(value.clone()), true, true)
            }
            IndexFilter::Gt { value } => {
                (Some(value.clone()), None, false, false)
            }
            IndexFilter::Ge { value } => {
                (Some(value.clone()), None, true, false)
            }
            IndexFilter::Lt { value } => {
                (None, Some(value.clone()), false, false)
            }
            IndexFilter::Le { value } => {
                (None, Some(value.clone()), false, true)
            }
            IndexFilter::Range { low, high, inclusive_low, inclusive_high } => {
                (Some(low.clone()), Some(high.clone()), *inclusive_low, *inclusive_high)
            }
            _ => (None, None, false, false),
        }
    }
}

/// Extract index filter from WHERE clause expression
/// 
/// This extracts predicates that can be evaluated at the index level
/// for the given indexed column.
pub fn extract_index_filter(expr: &Expression, indexed_column: &str) -> Option<IndexFilter> {
    match expr {
        Expression::Binary { left, op, right } => {
            match (left.as_ref(), op, right.as_ref()) {
                // Column = Value or Value = Column
                (Expression::Column(col), BinaryOp::Equal, value_expr) |
                (value_expr, BinaryOp::Equal, Expression::Column(col)) => {
                    if col == indexed_column {
                        value_to_filter(value_expr).map(|v| IndexFilter::Eq { value: v })
                    } else {
                        None
                    }
                }
                // Column > Value
                (Expression::Column(col), BinaryOp::Greater, value_expr) => {
                    if col == indexed_column {
                        value_to_filter(value_expr).map(|v| IndexFilter::Gt { value: v })
                    } else {
                        None
                    }
                }
                // Column >= Value
                (Expression::Column(col), BinaryOp::GreaterEqual, value_expr) => {
                    if col == indexed_column {
                        value_to_filter(value_expr).map(|v| IndexFilter::Ge { value: v })
                    } else {
                        None
                    }
                }
                // Column < Value
                (Expression::Column(col), BinaryOp::Less, value_expr) => {
                    if col == indexed_column {
                        value_to_filter(value_expr).map(|v| IndexFilter::Lt { value: v })
                    } else {
                        None
                    }
                }
                // Column <= Value
                (Expression::Column(col), BinaryOp::LessEqual, value_expr) => {
                    if col == indexed_column {
                        value_to_filter(value_expr).map(|v| IndexFilter::Le { value: v })
                    } else {
                        None
                    }
                }
                // Value > Column (reversed)
                (value_expr, BinaryOp::Greater, Expression::Column(col)) => {
                    if col == indexed_column {
                        value_to_filter(value_expr).map(|v| IndexFilter::Lt { value: v })
                    } else {
                        None
                    }
                }
                // Value >= Column (reversed)
                (value_expr, BinaryOp::GreaterEqual, Expression::Column(col)) => {
                    if col == indexed_column {
                        value_to_filter(value_expr).map(|v| IndexFilter::Le { value: v })
                    } else {
                        None
                    }
                }
                // AND - combine filters
                (_, BinaryOp::And, _) => {
                    let left_filter = extract_index_filter(left, indexed_column);
                    let right_filter = extract_index_filter(right, indexed_column);
                    
                    match (left_filter, right_filter) {
                        (Some(l), Some(r)) => Some(IndexFilter::And(Box::new(l), Box::new(r))),
                        (Some(l), None) => Some(l),
                        (None, Some(r)) => Some(r),
                        (None, None) => None,
                    }
                }
                // OR - if both sides are pushdownable
                (_, BinaryOp::Or, _) => {
                    let left_filter = extract_index_filter(left, indexed_column);
                    let right_filter = extract_index_filter(right, indexed_column);
                    
                    match (left_filter, right_filter) {
                        (Some(l), Some(r)) => Some(IndexFilter::Or(Box::new(l), Box::new(r))),
                        _ => None,
                    }
                }
                _ => None,
            }
        }
        Expression::Binary { left, op: BinaryOp::Equal, right } => {
            // Check for IS NULL (column = NULL)
            if let (Expression::Column(col), Expression::Null) = (left.as_ref(), right.as_ref()) {
                if col == indexed_column {
                    return Some(IndexFilter::IsNull);
                }
            }
            None
        }
        _ => None,
    }
}

/// Convert expression to Value for filter
fn value_to_filter(expr: &Expression) -> Option<Value> {
    match expr {
        Expression::Integer(n) => Some(Value::Integer(*n)),
        Expression::Float(f) => Some(Value::Real(*f)),
        Expression::String(s) => Some(Value::Text(s.clone())),
        Expression::Boolean(b) => Some(Value::Integer(if *b { 1 } else { 0 })),
        Expression::Null => Some(Value::Null),
        _ => None,
    }
}

/// Index scan iterator with pushdown filtering
pub struct IndexScanIterator<'a> {
    pager: &'a mut Pager,
    current_page: PageId,
    current_index: usize,
    filter: Option<IndexFilter>,
    index_keys: Vec<(Value, Vec<u64>)>,
    filtered_count: usize,
    scanned_count: usize,
}

impl<'a> IndexScanIterator<'a> {
    pub fn new(
        pager: &'a mut Pager,
        start_page: PageId,
        filter: Option<IndexFilter>,
    ) -> Result<Self> {
        // Load initial page data
        let index_keys = Self::load_page_keys(pager, start_page)?;
        
        Ok(Self {
            pager,
            current_page: start_page,
            current_index: 0,
            filter,
            index_keys,
            filtered_count: 0,
            scanned_count: 0,
        })
    }

    fn load_page_keys(pager: &mut Pager, page_id: PageId) -> Result<Vec<(Value, Vec<u64>)>> {
        // This is a simplified version - in real implementation
        // we'd read from the actual B-tree page structure
        Ok(Vec::new())
    }

    /// Get the next rowid that passes the filter
    pub fn next_rowid(&mut self) -> Option<u64> {
        loop {
            if self.current_index >= self.index_keys.len() {
                // Try to load next page
                // For now, just return None
                return None;
            }

            let (key, rowids) = &self.index_keys[self.current_index];
            self.current_index += 1;
            self.scanned_count += 1;

            // Apply filter
            if let Some(ref filter) = self.filter {
                if !filter.evaluate(key) {
                    self.filtered_count += 1;
                    continue;
                }
            }

            // Return first rowid (for non-unique indexes)
            return rowids.first().copied();
        }
    }

    /// Get statistics for this scan
    pub fn stats(&self) -> IndexScanStats {
        IndexScanStats {
            scanned: self.scanned_count,
            filtered: self.filtered_count,
            filter_selectivity: if self.scanned_count > 0 {
                self.filtered_count as f64 / self.scanned_count as f64
            } else {
                0.0
            },
        }
    }
}

/// Statistics for index scan with pushdown
#[derive(Debug, Clone)]
pub struct IndexScanStats {
    pub scanned: usize,
    pub filtered: usize,
    pub filter_selectivity: f64,
}

/// Index pushdown optimizer
/// 
/// Analyzes queries and determines if index pushdown can be applied
pub struct IndexPushdownOptimizer;

impl IndexPushdownOptimizer {
    /// Check if a query can benefit from index pushdown
    pub fn can_pushdown(query: &str, indexed_columns: &[String]) -> Option<(String, IndexFilter)> {
        // Simplified check - in real implementation, we'd parse the query
        // and check for applicable filters
        None
    }

    /// Estimate the benefit of index pushdown
    pub fn estimate_benefit(
        table_rows: usize,
        index_selectivity: f64,
        filter_selectivity: f64,
    ) -> PushdownBenefit {
        let without_pushdown = (table_rows as f64 * index_selectivity) as usize;
        let with_pushdown = (without_pushdown as f64 * filter_selectivity) as usize;
        let rows_saved = without_pushdown.saturating_sub(with_pushdown);

        PushdownBenefit {
            rows_saved,
            lookup_reduction_ratio: if without_pushdown > 0 {
                rows_saved as f64 / without_pushdown as f64
            } else {
                0.0
            },
            recommended: rows_saved > 100, // Threshold for recommendation
        }
    }
}

/// Benefit analysis for index pushdown
#[derive(Debug, Clone)]
pub struct PushdownBenefit {
    pub rows_saved: usize,
    pub lookup_reduction_ratio: f64,
    pub recommended: bool,
}

/// Build range scan parameters from filter
pub fn filter_to_range_scan(filter: &IndexFilter) -> (Option<Value>, Option<Value>) {
    let (low, high, _, _) = filter.to_range_bounds();
    (low, high)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_index_filter_evaluate() {
        // Equality
        let eq_filter = IndexFilter::Eq { value: Value::Integer(10) };
        assert!(eq_filter.evaluate(&Value::Integer(10)));
        assert!(!eq_filter.evaluate(&Value::Integer(20)));

        // Range
        let range_filter = IndexFilter::Range {
            low: Value::Integer(10),
            high: Value::Integer(20),
            inclusive_low: true,
            inclusive_high: false,
        };
        assert!(range_filter.evaluate(&Value::Integer(10)));
        assert!(range_filter.evaluate(&Value::Integer(15)));
        assert!(!range_filter.evaluate(&Value::Integer(20)));
        assert!(!range_filter.evaluate(&Value::Integer(5)));

        // IN list
        let in_filter = IndexFilter::In {
            values: vec![Value::Integer(1), Value::Integer(2), Value::Integer(3)],
        };
        assert!(in_filter.evaluate(&Value::Integer(2)));
        assert!(!in_filter.evaluate(&Value::Integer(4)));

        // IS NULL
        let null_filter = IndexFilter::IsNull;
        assert!(null_filter.evaluate(&Value::Null));
        assert!(!null_filter.evaluate(&Value::Integer(1)));

        // AND
        let and_filter = IndexFilter::And(
            Box::new(IndexFilter::Gt { value: Value::Integer(10) }),
            Box::new(IndexFilter::Lt { value: Value::Integer(20) }),
        );
        assert!(!and_filter.evaluate(&Value::Integer(5)));
        assert!(!and_filter.evaluate(&Value::Integer(25)));
        assert!(and_filter.evaluate(&Value::Integer(15)));
    }

    #[test]
    fn test_extract_index_filter() {
        use crate::sql::ast::Expression;

        // Simple equality
        let expr = Expression::Binary {
            left: Box::new(Expression::Column("age".to_string())),
            op: BinaryOp::Equal,
            right: Box::new(Expression::Integer(25)),
        };

        let filter = extract_index_filter(&expr, "age");
        assert!(filter.is_some());
        
        if let Some(IndexFilter::Eq { value }) = filter {
            assert_eq!(value, Value::Integer(25));
        } else {
            panic!("Expected Eq filter");
        }

        // Different column - should not match
        let filter2 = extract_index_filter(&expr, "name");
        assert!(filter2.is_none());

        // Range condition
        let range_expr = Expression::Binary {
            left: Box::new(Expression::Column("age".to_string())),
            op: BinaryOp::Greater,
            right: Box::new(Expression::Integer(18)),
        };

        let range_filter = extract_index_filter(&range_expr, "age");
        assert!(matches!(range_filter, Some(IndexFilter::Gt { .. })));
    }

    #[test]
    fn test_filter_to_range_bounds() {
        let filter = IndexFilter::Range {
            low: Value::Integer(10),
            high: Value::Integer(100),
            inclusive_low: true,
            inclusive_high: false,
        };

        let (low, high, inclusive_low, inclusive_high) = filter.to_range_bounds();
        
        assert_eq!(low, Some(Value::Integer(10)));
        assert_eq!(high, Some(Value::Integer(100)));
        assert!(inclusive_low);
        assert!(!inclusive_high);
    }

    #[test]
    fn test_pushdown_benefit() {
        let benefit = IndexPushdownOptimizer::estimate_benefit(
            10000,  // table rows
            0.5,    // index selects 50%
            0.2,    // filter keeps 20%
        );

        // Without pushdown: 5000 lookups
        // With pushdown: 1000 lookups
        // Saved: 4000 lookups
        assert_eq!(benefit.rows_saved, 4000);
        assert!((benefit.lookup_reduction_ratio - 0.8).abs() < 0.01);
        assert!(benefit.recommended);
    }

    #[test]
    fn test_is_index_pushdownable() {
        assert!(IndexFilter::Eq { value: Value::Integer(1) }.is_index_pushdownable());
        assert!(IndexFilter::Gt { value: Value::Integer(1) }.is_index_pushdownable());
        
        let and_filter = IndexFilter::And(
            Box::new(IndexFilter::Eq { value: Value::Integer(1) }),
            Box::new(IndexFilter::Lt { value: Value::Integer(10) }),
        );
        assert!(and_filter.is_index_pushdownable());
    }
}
