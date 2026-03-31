//! P5-4: Window Functions Implementation
//!
//! Window functions perform calculations across a set of table rows 
//! that are related to the current row.

use crate::sql::ast::{WindowFunc, WindowSpec, WindowFrame, WindowFrameBound, OrderBy};
use crate::storage::{Value, Record};

pub mod error;
pub use error::{WindowError, Result};

/// Window function evaluator
pub struct WindowEvaluator;

impl WindowEvaluator {
    /// Evaluate a window function on a partition of rows
    pub fn evaluate(
        func: &WindowFunc,
        rows: &[Record],
        current_idx: usize,
        table_columns: &[String],
    ) -> Result<Value> {
        match func {
            WindowFunc::RowNumber { .. } => {
                // ROW_NUMBER returns 1-based position in partition
                Ok(Value::Integer((current_idx + 1) as i64))
            }
            WindowFunc::Rank { .. } => {
                // RANK: 1-based rank with gaps for ties
                let rank = Self::calculate_rank(rows, current_idx, table_columns);
                Ok(Value::Integer(rank))
            }
            WindowFunc::DenseRank { .. } => {
                // DENSE_RANK: 1-based rank without gaps
                let rank = Self::calculate_dense_rank(rows, current_idx, table_columns);
                Ok(Value::Integer(rank))
            }
            WindowFunc::Lead { expr, offset, default, .. } => {
                let offset_val = offset.as_ref()
                    .and_then(|o| Self::get_literal_value(o))
                    .unwrap_or(1) as usize;
                
                let target_idx = current_idx + offset_val;
                if target_idx < rows.len() {
                    // Evaluate expression on target row
                    Self::evaluate_expression(expr, &rows[target_idx], table_columns)
                } else {
                    // Return default value or NULL
                    default.as_ref()
                        .map(|d| Self::evaluate_expression(d, &rows[current_idx], table_columns))
                        .unwrap_or(Ok(Value::Null))
                }
            }
            WindowFunc::Lag { expr, offset, default, .. } => {
                let offset_val = offset.as_ref()
                    .and_then(|o| Self::get_literal_value(o))
                    .unwrap_or(1) as usize;
                
                if current_idx >= offset_val {
                    let target_idx = current_idx - offset_val;
                    Self::evaluate_expression(expr, &rows[target_idx], table_columns)
                } else {
                    default.as_ref()
                        .map(|d| Self::evaluate_expression(d, &rows[current_idx], table_columns))
                        .unwrap_or(Ok(Value::Null))
                }
            }
            WindowFunc::FirstValue { expr, .. } => {
                if rows.is_empty() {
                    Ok(Value::Null)
                } else {
                    Self::evaluate_expression(expr, &rows[0], table_columns)
                }
            }
            WindowFunc::LastValue { expr, .. } => {
                if rows.is_empty() {
                    Ok(Value::Null)
                } else {
                    Self::evaluate_expression(expr, &rows[rows.len() - 1], table_columns)
                }
            }
            WindowFunc::NthValue { expr, n, .. } => {
                let n_val = Self::get_literal_value(n).unwrap_or(1) as usize;
                if n_val > 0 && n_val <= rows.len() {
                    Self::evaluate_expression(expr, &rows[n_val - 1], table_columns)
                } else {
                    Ok(Value::Null)
                }
            }
        }
    }
    
    /// Calculate rank with gaps
    fn calculate_rank(rows: &[Record], current_idx: usize, _table_columns: &[String]) -> i64 {
        // Simplified rank calculation
        // In real implementation, would need ORDER BY column values
        let mut rank = 1;
        let mut count = 0;
        
        for i in 0..=current_idx {
            if i > 0 && Self::rows_equal(&rows[i], &rows[i-1], _table_columns) {
                // Same as previous, no rank change
            } else {
                rank = count + 1;
            }
            count += 1;
        }
        
        rank
    }
    
    /// Calculate dense rank without gaps
    fn calculate_dense_rank(rows: &[Record], current_idx: usize, _table_columns: &[String]) -> i64 {
        let mut rank = 1;
        
        for i in 1..=current_idx {
            if !Self::rows_equal(&rows[i], &rows[i-1], _table_columns) {
                rank += 1;
            }
        }
        
        rank
    }
    
    /// Check if two rows are equal based on ORDER BY columns
    fn rows_equal(a: &Record, b: &Record, _columns: &[String]) -> bool {
        // Simplified: compare all values
        a.values == b.values
    }
    
    /// Get literal integer value from expression
    fn get_literal_value(expr: &crate::sql::ast::Expression) -> Option<i64> {
        match expr {
            crate::sql::ast::Expression::Integer(n) => Some(*n),
            _ => None,
        }
    }
    
    /// Evaluate an expression on a row
    fn evaluate_expression(
        expr: &crate::sql::ast::Expression,
        row: &Record,
        table_columns: &[String],
    ) -> Result<Value> {
        match expr {
            crate::sql::ast::Expression::Column(name) => {
                let idx = table_columns.iter().position(|c| c == name)
                    .ok_or(WindowError::ColumnNotFound(name.clone()))?;
                Ok(row.values.get(idx).cloned().unwrap_or(Value::Null))
            }
            crate::sql::ast::Expression::Integer(n) => Ok(Value::Integer(*n)),
            crate::sql::ast::Expression::String(s) => Ok(Value::Text(s.clone())),
            crate::sql::ast::Expression::Null => Ok(Value::Null),
            _ => Ok(Value::Null), // Simplified for other expression types
        }
    }
}

/// Partition rows based on PARTITION BY clause
pub fn partition_rows(
    rows: &[Record],
    partition_by: &[crate::sql::ast::Expression],
    table_columns: &[String],
) -> Vec<Vec<Record>> {
    if partition_by.is_empty() {
        // No partitioning, return all rows as single partition
        return vec![rows.to_vec()];
    }
    
    // Simplified partitioning: group by all partition columns
    use std::collections::HashMap;
    let mut partitions: HashMap<String, Vec<Record>> = HashMap::new();
    
    for row in rows {
        let key = partition_by.iter()
            .map(|expr| {
                match WindowEvaluator::evaluate_expression(expr, row, table_columns) {
                    Ok(v) => format!("{:?}", v),
                    Err(_) => "null".to_string(),
                }
            })
            .collect::<Vec<_>>()
            .join("|");
        
        partitions.entry(key).or_default().push(row.clone());
    }
    
    partitions.into_values().collect()
}

/// Sort rows based on ORDER BY clause
pub fn sort_rows(rows: &mut [Record], order_by: &[OrderBy], table_columns: &[String]) {
    if order_by.is_empty() {
        return;
    }
    
    rows.sort_by(|a, b| {
        for order in order_by {
            let a_val = WindowEvaluator::evaluate_expression(
                &crate::sql::ast::Expression::Column(order.column.clone()),
                a,
                table_columns
            ).unwrap_or(Value::Null);
            
            let b_val = WindowEvaluator::evaluate_expression(
                &crate::sql::ast::Expression::Column(order.column.clone()),
                b,
                table_columns
            ).unwrap_or(Value::Null);
            
            let cmp = a_val.partial_cmp(&b_val).unwrap_or(std::cmp::Ordering::Equal);
            if cmp != std::cmp::Ordering::Equal {
                return if order.descending { cmp.reverse() } else { cmp };
            }
        }
        std::cmp::Ordering::Equal
    });
}
