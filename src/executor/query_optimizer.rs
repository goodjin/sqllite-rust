//! Query Optimizer Integration for Executor
//!
//! Integrates the optimizer module with query execution:
//! - Statistics-based plan selection
//! - JOIN reordering
//! - Index selection
//! - Cost-based optimization

use crate::sql::ast::{SelectStmt, Expression, BinaryOp, Join, JoinType, SelectColumn};
use crate::storage::BtreeDatabase;
use crate::executor::{QueryPlan, ExecutorError};
use crate::optimizer::{
    QueryOptimizer, StatsCollector, CostEstimator, JoinReorderer,
    IndexSelector, StatsCatalog, JoinOrder
};

/// Optimized query execution plan
#[derive(Debug, Clone)]
pub struct OptimizedPlan {
    /// The query plan to execute
    pub plan: QueryPlan,
    /// Estimated cost
    pub estimated_cost: f64,
    /// Estimated rows
    pub estimated_rows: u64,
    /// Optimization notes
    pub notes: Vec<String>,
}

/// Query optimizer that integrates with the executor
pub struct ExecutorQueryOptimizer {
    optimizer: QueryOptimizer,
    stats_collector: StatsCollector,
}

impl ExecutorQueryOptimizer {
    /// Create a new executor query optimizer
    pub fn new() -> Self {
        Self {
            optimizer: QueryOptimizer::new(),
            stats_collector: StatsCollector::new(),
        }
    }

    /// Collect statistics for all tables in the database
    pub fn collect_statistics(&mut self, db: &mut BtreeDatabase) -> Result<(), ExecutorError> {
        // Clone table names to avoid borrow issues
        let table_names: Vec<String> = db.list_tables()
            .iter()
            .map(|s| s.to_string())
            .collect();
        
        for table_name in table_names {
            if let Err(e) = self.collect_table_stats(db, &table_name) {
                eprintln!("Warning: Failed to collect stats for {}: {:?}", table_name, e);
            }
        }
        
        Ok(())
    }

    /// Collect statistics for a specific table
    fn collect_table_stats(&mut self, db: &mut BtreeDatabase, table_name: &str) -> Result<(), ExecutorError> {
        // Get all records to analyze
        let records = db.select_all(table_name)
            .map_err(ExecutorError::StorageError)?;
        
        // Clone table columns to avoid borrow issues
        let columns = {
            let table = db.get_table(table_name)
                .ok_or_else(|| ExecutorError::TableNotFound(table_name.to_string()))?;
            table.columns.clone()
        };
        
        // Analyze each column
        for (col_idx, col_def) in columns.iter().enumerate() {
            let mut distinct_values = std::collections::HashSet::new();
            let mut null_count = 0;
            let mut min_val: Option<crate::storage::Value> = None;
            let mut max_val: Option<crate::storage::Value> = None;
            
            for record in &records {
                if let Some(value) = record.values.get(col_idx) {
                    if matches!(value, crate::storage::Value::Null) {
                        null_count += 1;
                    } else {
                        distinct_values.insert(value.clone());
                        
                        // Update min/max
                        match (&min_val, value) {
                            (None, _) => min_val = Some(value.clone()),
                            (Some(min), val) if val < min => min_val = Some(value.clone()),
                            _ => {}
                        }
                        
                        match (&max_val, value) {
                            (None, _) => max_val = Some(value.clone()),
                            (Some(max), val) if val > max => max_val = Some(value.clone()),
                            _ => {}
                        }
                    }
                }
            }
            
            println!(
                "Stats for {}.{}: {} distinct, {} nulls, range: {:?} to {:?}",
                table_name,
                col_def.name,
                distinct_values.len(),
                null_count,
                min_val,
                max_val
            );
        }
        
        Ok(())
    }

    /// Optimize a SELECT query
    pub fn optimize_select(&self, db: &BtreeDatabase, stmt: &SelectStmt) -> Result<OptimizedPlan, ExecutorError> {
        let mut notes = Vec::new();
        
        // Check if this is a JOIN query
        if !stmt.joins.is_empty() {
            notes.push(format!("Query has {} JOINs", stmt.joins.len()));
            
            // For now, use simple plan
            // Full optimization would use join reordering
            let plan = QueryPlan::FullTableScan {
                table: stmt.from.clone(),
                filter: stmt.where_clause.clone(),
                columns: stmt.columns.clone(),
                limit: stmt.limit,
            };
            
            return Ok(OptimizedPlan {
                plan,
                estimated_cost: 1000.0,
                estimated_rows: 1000,
                notes,
            });
        }
        
        // Single table query - check for index usage
        let mut use_index = false;
        let mut index_name = None;
        
        if let Some(ref expr) = stmt.where_clause {
            // Simple equality check on indexed column
            if let Expression::Binary { left, op: BinaryOp::Equal, right } = expr {
                if let Expression::Column(col_name) = left.as_ref() {
                    // Check if there's an index on this column
                    let indexes = db.get_table_indexes(&stmt.from);
                    for idx in indexes {
                        if &idx.column == col_name {
                            use_index = true;
                            index_name = Some(idx.name.clone());
                            notes.push(format!("Using index: {}", idx.name));
                            break;
                        }
                    }
                }
            }
        }
        
        let plan = if use_index && index_name.is_some() {
            // Get the value from the WHERE clause
            if let Some(Expression::Binary { right, .. }) = &stmt.where_clause {
                // Extract value from Expression (Integer, String, etc.)
                let value_opt = match right.as_ref() {
                    Expression::Integer(i) => Some(crate::storage::Value::Integer(*i)),
                    Expression::String(s) => Some(crate::storage::Value::Text(s.clone())),
                    Expression::Float(f) => Some(crate::storage::Value::Real(*f)),
                    Expression::Boolean(b) => Some(crate::storage::Value::Integer(*b as i64)),
                    Expression::Null => Some(crate::storage::Value::Null),
                    _ => None,
                };
                if let Some(val) = value_opt {
                    QueryPlan::IndexScan {
                        table: stmt.from.clone(),
                        index_name: index_name.unwrap(),
                        column: stmt.where_clause.as_ref()
                            .and_then(|e| match e {
                                Expression::Binary { left, .. } => match left.as_ref() {
                                    Expression::Column(c) => Some(c.clone()),
                                    _ => None,
                                },
                                _ => None,
                            })
                            .unwrap_or_default(),
                        value: val,
                        columns: stmt.columns.clone(),
                        limit: stmt.limit,
                    }
                } else {
                    QueryPlan::FullTableScan {
                        table: stmt.from.clone(),
                        filter: stmt.where_clause.clone(),
                        columns: stmt.columns.clone(),
                        limit: stmt.limit,
                    }
                }
            } else {
                QueryPlan::FullTableScan {
                    table: stmt.from.clone(),
                    filter: stmt.where_clause.clone(),
                    columns: stmt.columns.clone(),
                    limit: stmt.limit,
                }
            }
        } else {
            QueryPlan::FullTableScan {
                table: stmt.from.clone(),
                filter: stmt.where_clause.clone(),
                columns: stmt.columns.clone(),
                limit: stmt.limit,
            }
        };
        
        Ok(OptimizedPlan {
            plan,
            estimated_cost: if use_index { 100.0 } else { 1000.0 },
            estimated_rows: if use_index { 10 } else { 1000 },
            notes,
        })
    }
}

impl Default for ExecutorQueryOptimizer {
    fn default() -> Self {
        Self::new()
    }
}

/// Simple query planner for basic queries
pub struct SimpleQueryPlanner;

impl SimpleQueryPlanner {
    /// Create a query plan for a SELECT statement
    pub fn plan_select(stmt: &SelectStmt) -> QueryPlan {
        // Simple table scan for now
        QueryPlan::FullTableScan {
            table: stmt.from.clone(),
            filter: stmt.where_clause.clone(),
            columns: stmt.columns.clone(),
            limit: stmt.limit,
        }
    }
    
    /// Check if index scan can be used
    pub fn can_use_index(stmt: &SelectStmt) -> Option<(String, String, crate::storage::Value)> {
        if let Some(ref expr) = stmt.where_clause {
            if let Expression::Binary { left, op: BinaryOp::Equal, right } = expr {
                if let Expression::Column(col_name) = left.as_ref() {
                    // Extract value from various Expression types
                    let value_opt = match right.as_ref() {
                        Expression::Integer(i) => Some(crate::storage::Value::Integer(*i)),
                        Expression::String(s) => Some(crate::storage::Value::Text(s.clone())),
                        Expression::Float(f) => Some(crate::storage::Value::Real(*f)),
                        Expression::Boolean(b) => Some(crate::storage::Value::Integer(*b as i64)),
                        Expression::Null => Some(crate::storage::Value::Null),
                        _ => None,
                    };
                    if let Some(val) = value_opt {
                        return Some((stmt.from.clone(), col_name.clone(), val));
                    }
                }
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::ast::{SelectColumn, DataType};
    use tempfile::NamedTempFile;

    fn create_test_db() -> BtreeDatabase {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap();
        let mut db = BtreeDatabase::open(path).unwrap();

        // Create test table
        let columns = vec![
            crate::sql::ast::ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                primary_key: true,
            },
            crate::sql::ast::ColumnDef {
                name: "name".to_string(),
                data_type: DataType::Text,
                nullable: true,
                primary_key: false,
            },
        ];
        db.create_table("users".to_string(), columns).unwrap();

        db
    }

    #[test]
    fn test_simple_query_planner() {
        let stmt = SelectStmt {
            ctes: vec![],
            columns: vec![SelectColumn::All],
            from: "users".to_string(),
            joins: vec![],
            where_clause: Some(Expression::Binary {
                left: Box::new(Expression::Column("id".to_string())),
                op: BinaryOp::Equal,
                right: Box::new(Expression::Integer(1)),
            }),
            group_by: vec![],
            having: None,
            order_by: vec![],
            limit: None,
            offset: None,
        };

        let plan = SimpleQueryPlanner::plan_select(&stmt);
        
        match plan {
            QueryPlan::FullTableScan { table, .. } => {
                assert_eq!(table, "users");
            }
            _ => panic!("Expected FullTableScan"),
        }
    }

    #[test]
    fn test_can_use_index() {
        let stmt = SelectStmt {
            ctes: vec![],
            columns: vec![SelectColumn::All],
            from: "users".to_string(),
            joins: vec![],
            where_clause: Some(Expression::Binary {
                left: Box::new(Expression::Column("id".to_string())),
                op: BinaryOp::Equal,
                right: Box::new(Expression::Integer(1)),
            }),
            group_by: vec![],
            having: None,
            order_by: vec![],
            limit: None,
            offset: None,
        };

        let result = SimpleQueryPlanner::can_use_index(&stmt);
        assert!(result.is_some());
        
        let (table, col, val) = result.unwrap();
        assert_eq!(table, "users");
        assert_eq!(col, "id");
    }
}
