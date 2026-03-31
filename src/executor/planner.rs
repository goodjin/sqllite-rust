//! Query Planner for Optimized Query Execution
//!
//! This module analyzes SELECT statements and chooses optimal execution strategies:
//! - Index Scan: Use B-tree index for point/range queries
//! - Full Table Scan: Fallback for complex WHERE clauses
//! - Limit Pushdown: Stop early when LIMIT is specified

use crate::sql::ast::{SelectStmt, Expression, BinaryOp, SelectColumn, ColumnDef};
use crate::storage::{BtreeDatabase, Value, Record};
use super::{Result, ExecutorError, predicate_pushdown};

/// Query execution plan
#[derive(Debug, Clone, PartialEq)]
pub enum QueryPlan {
    /// Use covering index (all needed columns in index, no table lookup)
    CoveringIndexScan {
        table: String,
        index_name: String,
        column: String,
        value: Value,
        columns: Vec<SelectColumn>,
        limit: Option<i64>,
    },
    /// Covering index range scan
    CoveringIndexRangeScan {
        table: String,
        index_name: String,
        column: String,
        start: Option<Value>,
        end: Option<Value>,
        columns: Vec<SelectColumn>,
        limit: Option<i64>,
    },
    /// Use secondary B-tree index for point lookup (requires table lookup)
    IndexScan {
        table: String,
        index_name: String,
        column: String,
        value: Value,
        columns: Vec<SelectColumn>,
        limit: Option<i64>,
    },
    /// Use secondary B-tree index for range scan (requires table lookup)
    IndexRangeScan {
        table: String,
        index_name: String,
        column: String,
        start: Option<Value>,
        end: Option<Value>,
        columns: Vec<SelectColumn>,
        limit: Option<i64>,
    },
    /// Use rowid B-tree index for point lookup
    RowidPointScan {
        table: String,
        rowid: i64,
        columns: Vec<SelectColumn>,
    },
    /// Use rowid B-tree index for range scan
    RowidRangeScan {
        table: String,
        start_rowid: Option<i64>,
        end_rowid: Option<i64>,
        columns: Vec<SelectColumn>,
        limit: Option<i64>,
    },
    /// Full table scan with filtering
    FullTableScan {
        table: String,
        filter: Option<Expression>,
        columns: Vec<SelectColumn>,
        limit: Option<i64>,
    },
    /// HNSW vector similarity scan
    HnswVectorScan {
        table: String,
        index_name: String,
        query_vector: Vec<f32>,
        limit: usize,
        columns: Vec<SelectColumn>,
    },
}

/// Query plan optimizer
pub struct QueryPlanner;

impl QueryPlanner {
    /// Create an optimal query plan for a SELECT statement
    pub fn plan(db: &BtreeDatabase, stmt: &SelectStmt) -> Result<QueryPlan> {
        let table = &stmt.from;

        // Check if table exists
        if db.get_table(table).is_none() {
            return Err(ExecutorError::TableNotFound(table.clone()));
        }

        // Check for subqueries in WHERE clause - if present, use full table scan
        // Subqueries require special handling and caching that the planner doesn't support
        if let Some(ref where_clause) = stmt.where_clause {
            if Self::contains_subquery(where_clause) {
                return Ok(QueryPlan::FullTableScan {
                    table: table.clone(),
                    filter: Some(where_clause.clone()),
                    columns: stmt.columns.clone(),
                    limit: stmt.limit,
                });
            }
        }

        // Try to optimize for Vector Search (HNSW)
        if !stmt.order_by.is_empty() && stmt.limit.is_some() {
            let first_order = &stmt.order_by[0];
            
            // Check if any column in SELECT is vector_l2_distance and matches the ORDER BY
            for col in &stmt.columns {
                if let SelectColumn::Expression(Expression::FunctionCall { name, args }, alias) = col {
                    if name == "vector_l2_distance" && args.len() == 2 {
                        let matches_order = if let Some(alias_name) = alias {
                            alias_name == &first_order.column
                        } else {
                            // If no alias, the column identifier might be the function call string, 
                            // but currently our parser/tokenizer might just treat it as an identifier if it's in ORDER BY.
                            false
                        };

                        if matches_order {
                            if let (Expression::Column(col_name), Expression::Vector(query_exprs)) = (&args[0], &args[1]) {
                                if let Some(index_name) = Self::find_hnsw_index_for_column(db, table, col_name) {
                                    let mut query_vector = Vec::new();
                                    for expr in query_exprs {
                                         if let Some(Value::Real(f)) = Self::expression_to_value(expr) {
                                             query_vector.push(f as f32);
                                         } else if let Some(Value::Integer(i)) = Self::expression_to_value(expr) {
                                             query_vector.push(i as f32);
                                         }
                                    }
                                    
                                    if !query_vector.is_empty() {
                                        return Ok(QueryPlan::HnswVectorScan {
                                            table: table.clone(),
                                            index_name,
                                            query_vector,
                                            limit: stmt.limit.unwrap() as usize,
                                            columns: stmt.columns.clone(),
                                        });
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        // Extract usable conditions from WHERE clause
        if let Some(ref where_clause) = stmt.where_clause {
            // Check for rowid equality: rowid = N or id = N (primary key lookup)
            if let Some((col, val)) = Self::extract_point_condition(where_clause) {
                if Self::is_rowid_column(&col) {
                    if let Value::Integer(rowid) = val {
                        return Ok(QueryPlan::RowidPointScan {
                            table: table.clone(),
                            rowid,
                            columns: stmt.columns.clone(),
                        });
                    }
                }

                // Check for secondary index
                if let Some(index_name) = Self::find_index_for_column(db, table, &col) {
                    // Check if this can be a covering index scan
                    let is_covering = Self::is_covering_index(db, table, &index_name, &stmt.columns);
                    
                    if is_covering {
                        return Ok(QueryPlan::CoveringIndexScan {
                            table: table.clone(),
                            index_name,
                            column: col,
                            value: val,
                            columns: stmt.columns.clone(),
                            limit: stmt.limit,
                        });
                    } else {
                        return Ok(QueryPlan::IndexScan {
                            table: table.clone(),
                            index_name,
                            column: col,
                            value: val,
                            columns: stmt.columns.clone(),
                            limit: stmt.limit,
                        });
                    }
                }
            }

            // Check for range condition on rowid
            if let Some((col, start, end)) = Self::extract_range_condition(where_clause) {
                if Self::is_rowid_column(&col) {
                    let start_rowid = start.as_ref().and_then(|v| match v {
                        Value::Integer(n) => Some(*n),
                        _ => None,
                    });
                    let end_rowid = end.as_ref().and_then(|v| match v {
                        Value::Integer(n) => Some(*n),
                        _ => None,
                    });
                    return Ok(QueryPlan::RowidRangeScan {
                        table: table.clone(),
                        start_rowid,
                        end_rowid,
                        columns: stmt.columns.clone(),
                        limit: stmt.limit,
                    });
                }

                // Check for secondary index range scan
                if let Some(index_name) = Self::find_index_for_column(db, table, &col) {
                    // Check if this can be a covering index scan
                    let is_covering = Self::is_covering_index(db, table, &index_name, &stmt.columns);
                    
                    if is_covering {
                        return Ok(QueryPlan::CoveringIndexRangeScan {
                            table: table.clone(),
                            index_name,
                            column: col,
                            start,
                            end,
                            columns: stmt.columns.clone(),
                            limit: stmt.limit,
                        });
                    } else {
                        return Ok(QueryPlan::IndexRangeScan {
                            table: table.clone(),
                            index_name,
                            column: col,
                            start,
                            end,
                            columns: stmt.columns.clone(),
                            limit: stmt.limit,
                        });
                    }
                }
            }
        }

        // Fallback to full table scan
        Ok(QueryPlan::FullTableScan {
            table: table.clone(),
            filter: stmt.where_clause.clone(),
            columns: stmt.columns.clone(),
            limit: stmt.limit,
        })
    }

    /// Extract point equality condition: column = value
    pub fn extract_point_condition(expr: &Expression) -> Option<(String, Value)> {
        match expr {
            Expression::Binary { left, op: BinaryOp::Equal, right } => {
                if let Expression::Column(col) = left.as_ref() {
                    if let Some(val) = Self::expression_to_value(right) {
                        return Some((col.clone(), val));
                    }
                }
                // Also check reverse: value = column
                if let Expression::Column(col) = right.as_ref() {
                    if let Some(val) = Self::expression_to_value(left) {
                        return Some((col.clone(), val));
                    }
                }
            }
            _ => {}
        }
        None
    }

    /// Extract range condition: column >/</>=/<= value or BETWEEN
    pub fn extract_range_condition(expr: &Expression) -> Option<(String, Option<Value>, Option<Value>)> {
        // First try to extract from AND expression (e.g., rowid > 400 AND rowid < 410)
        if let Some(result) = Self::extract_and_range_condition(expr) {
            return Some(result);
        }

        // Fall back to single condition (e.g., rowid > 400)
        match expr {
            Expression::Binary { left, op, right } => {
                if let Expression::Column(col) = left.as_ref() {
                    if let Some(val) = Self::expression_to_value(right) {
                        match op {
                            BinaryOp::Greater => return Some((col.clone(), Some(val), None)),
                            BinaryOp::GreaterEqual => return Some((col.clone(), Some(val), None)),
                            BinaryOp::Less => return Some((col.clone(), None, Some(val))),
                            BinaryOp::LessEqual => return Some((col.clone(), None, Some(val))),
                            _ => {}
                        }
                    }
                }
            }
            _ => {}
        }
        None
    }

    /// Extract range condition from AND expression: col > X AND col < Y
    fn extract_and_range_condition(expr: &Expression) -> Option<(String, Option<Value>, Option<Value>)> {
        match expr {
            Expression::Binary { left, op: BinaryOp::And, right } => {
                // Try to extract conditions from left and right sides
                let left_cond = Self::extract_single_condition(left);
                let right_cond = Self::extract_single_condition(right);

                // If both conditions are on the same column, combine them
                if let (Some((col1, start1, end1)), Some((col2, start2, end2))) = (&left_cond, &right_cond) {
                    if col1 == col2 {
                        // Combine the range
                        let start = start1.clone().or_else(|| start2.clone());
                        let end = end1.clone().or_else(|| end2.clone());
                        return Some((col1.clone(), start, end));
                    }
                }

                // If only one side has a condition, return it
                left_cond.or(right_cond)
            }
            _ => None
        }
    }

    /// Extract a single comparison condition
    fn extract_single_condition(expr: &Expression) -> Option<(String, Option<Value>, Option<Value>)> {
        match expr {
            Expression::Binary { left, op, right } => {
                if let Expression::Column(col) = left.as_ref() {
                    if let Some(val) = Self::expression_to_value(right) {
                        match op {
                            BinaryOp::Greater | BinaryOp::GreaterEqual => {
                                return Some((col.clone(), Some(val), None))
                            }
                            BinaryOp::Less | BinaryOp::LessEqual => {
                                return Some((col.clone(), None, Some(val)))
                            }
                            _ => {}
                        }
                    }
                }
            }
            _ => {}
        }
        None
    }

    /// Convert expression to Value if possible
    fn expression_to_value(expr: &Expression) -> Option<Value> {
        match expr {
            Expression::Integer(n) => Some(Value::Integer(*n)),
            Expression::String(s) => Some(Value::Text(s.clone())),
            Expression::Float(f) => Some(Value::Real(*f)),
            Expression::Boolean(b) => Some(if *b { Value::Integer(1) } else { Value::Integer(0) }),
            Expression::Null => Some(Value::Null),
            Expression::Vector(elements) => {
                let mut vals = Vec::with_capacity(elements.len());
                for e in elements {
                    match Self::expression_to_value(e)? {
                        Value::Real(f) => vals.push(f as f32),
                        Value::Integer(n) => vals.push(n as f32),
                        _ => return None,
                    }
                }
                Some(Value::Vector(vals))
            }
            _ => None,
        }
    }

    /// Check if column is rowid/primary key
    /// Only "rowid" is treated as rowid, not "id"
    fn is_rowid_column(col: &str) -> bool {
        col.to_lowercase() == "rowid"
    }

    /// Check if an expression contains a subquery
    fn contains_subquery(expr: &Expression) -> bool {
        match expr {
            Expression::Subquery(_) => true,
            Expression::Binary { left, right, .. } => {
                Self::contains_subquery(left) || Self::contains_subquery(right)
            }
            Expression::FunctionCall { args, .. } => {
                args.iter().any(|arg| Self::contains_subquery(arg))
            }
            _ => false,
        }
    }

    fn find_hnsw_index_for_column(db: &BtreeDatabase, table_name: &str, column: &str) -> Option<String> {
        let table = db.get_table(table_name)?;
        for idx in &table.hnsw_indices {
            if idx.column == column {
                return Some(idx.name.clone());
            }
        }
        None
    }

    /// Find an index for the given column
    fn find_index_for_column(db: &BtreeDatabase, table: &str, column: &str) -> Option<String> {
        let indexes = db.get_table_indexes(table);
        indexes.iter()
            .find(|idx| idx.column == column)
            .map(|idx| idx.name.clone())
    }

    /// Check if index is a covering index for the query
    /// A covering index contains all columns needed by the query
    fn is_covering_index(db: &BtreeDatabase, table: &str, index_name: &str, columns: &[SelectColumn]) -> bool {
        // Get the index info
        let index_info = db.get_table_indexes(table)
            .into_iter()
            .find(|idx| idx.name == index_name);
        
        let index = match index_info {
            Some(idx) => idx,
            None => return false,
        };
        
        // For each column in SELECT, check if it's in the index or if we can get it from index
        // For now, we only support covering index if:
        // 1. SELECT only the indexed column, or
        // 2. SELECT includes rowid (which is always in index)
        
        for col in columns {
            match col {
                SelectColumn::All => {
                    // SELECT * requires all columns, index can't cover unless table has only indexed column
                    return false;
                }
                SelectColumn::Column(col_name) => {
                    // Check if this column is the indexed column
                    if col_name != &index.column && col_name != "rowid" {
                        return false;
                    }
                }
                SelectColumn::Aggregate(_, _) => {
                    // Aggregates like COUNT(*) can use covering index
                    continue;
                }
                SelectColumn::Expression(_, _) => {
                    // Expressions might reference other columns
                    return false;
                }
                SelectColumn::WindowFunc(_, _) => {
                    // Window functions might reference other columns
                    return false;
                }
            }
        }
        
        true
    }

    /// Estimate query cost for a plan
    pub fn estimate_cost(plan: &QueryPlan, db: &BtreeDatabase) -> u64 {
        match plan {
            QueryPlan::RowidPointScan { table, .. } => {
                // Rowid point scan: O(log n)
                let record_count = db.get_table(table).map(|t| t.next_rowid).unwrap_or(1);
                (record_count as f64).log2() as u64 + 1
            }
            QueryPlan::RowidRangeScan { table, .. } => {
                // Rowid range scan: O(log n + k)
                let record_count = db.get_table(table).map(|t| t.next_rowid).unwrap_or(1);
                (record_count as f64).log2() as u64 + 10
            }
            QueryPlan::CoveringIndexScan { table, .. } => {
                // Covering index scan (no table lookup): O(log n) - faster than regular index scan
                let record_count = db.get_table(table).map(|t| t.next_rowid).unwrap_or(1);
                (record_count as f64).log2() as u64 + 1
            }
            QueryPlan::CoveringIndexRangeScan { table, .. } => {
                // Covering index range scan (no table lookup): O(log n + k) - faster than regular
                let record_count = db.get_table(table).map(|t| t.next_rowid).unwrap_or(1);
                (record_count as f64).log2() as u64 + 8
            }
            QueryPlan::IndexScan { table, .. } => {
                // Secondary index scan + lookup: O(log n + 1)
                let record_count = db.get_table(table).map(|t| t.next_rowid).unwrap_or(1);
                (record_count as f64).log2() as u64 + 2
            }
            QueryPlan::IndexRangeScan { table, .. } => {
                // Secondary index range scan: O(log n + k)
                let record_count = db.get_table(table).map(|t| t.next_rowid).unwrap_or(1);
                (record_count as f64).log2() as u64 + 15
            }
            QueryPlan::FullTableScan { table, .. } => {
                // Full scan: O(n)
                db.get_table(table).map(|t| t.next_rowid).unwrap_or(0)
            }
            QueryPlan::HnswVectorScan { .. } => {
                // HNSW vector scan: O(log n) + constant overhead
                10
            }
        }
    }
}

/// Plan executor
pub struct PlanExecutor;

impl PlanExecutor {
    /// Execute a query plan and return results
    pub fn execute(
        db: &mut BtreeDatabase,
        plan: &QueryPlan,
        table_columns: &[ColumnDef],
    ) -> Result<Vec<Record>> {
        match plan {
            QueryPlan::RowidPointScan { table, rowid, .. } => {
                Self::execute_rowid_point_scan(db, table, *rowid)
            }
            QueryPlan::RowidRangeScan { table, start_rowid, end_rowid, limit, .. } => {
                Self::execute_rowid_range_scan(db, table, *start_rowid, *end_rowid, *limit)
            }
            QueryPlan::CoveringIndexScan { table, index_name, value, limit, .. } => {
                // Covering index scan - no table lookup needed
                Self::execute_covering_index_scan(db, table, index_name, value, *limit)
            }
            QueryPlan::CoveringIndexRangeScan { table, index_name, start, end, limit, .. } => {
                // Covering index range scan - no table lookup needed
                Self::execute_covering_index_range_scan(db, table, index_name, start.as_ref(), end.as_ref(), *limit)
            }
            QueryPlan::IndexScan { table, index_name, value, limit, .. } => {
                Self::execute_index_scan(db, table, index_name, value, *limit)
            }
            QueryPlan::IndexRangeScan { table, index_name, start, end, limit, .. } => {
                Self::execute_index_range_scan(db, table, index_name, start.as_ref(), end.as_ref(), *limit)
            }
            QueryPlan::FullTableScan { table, filter, limit, .. } => {
                Self::execute_full_scan(db, table, filter.as_ref(), table_columns, *limit)
            }
            QueryPlan::HnswVectorScan { index_name, query_vector, limit, .. } => {
                Self::execute_hnsw_vector_scan(db, index_name, query_vector, *limit)
            }
        }
    }

    /// Execute rowid point scan (O(log n) lookup)
    fn execute_rowid_point_scan(
        db: &mut BtreeDatabase,
        table: &str,
        rowid: i64,
    ) -> Result<Vec<Record>> {
        let mut results = Vec::new();
        if let Ok(record) = db.get_record(table, rowid as u64) {
            results.push(record);
        }
        Ok(results)
    }

    /// Execute rowid range scan using B-tree
    fn execute_rowid_range_scan(
        db: &mut BtreeDatabase,
        table: &str,
        start_rowid: Option<i64>,
        end_rowid: Option<i64>,
        limit: Option<i64>,
    ) -> Result<Vec<Record>> {
        let mut results = Vec::new();

        // Use select_all_with_rowid for efficiency
        let all_with_rowid = db.select_all_with_rowid(table)?;

        for (rowid, record) in all_with_rowid {
            // Apply range filter
            let in_range = match (start_rowid, end_rowid) {
                (Some(start), Some(end)) => rowid >= start as u64 && rowid <= end as u64,
                (Some(start), None) => rowid >= start as u64,
                (None, Some(end)) => rowid <= end as u64,
                (None, None) => true,
            };

            if in_range {
                results.push(record);

                // Early termination with limit
                if let Some(limit) = limit {
                    if results.len() >= limit as usize {
                        break;
                    }
                }
            }
        }

        Ok(results)
    }

    /// Execute covering index point scan (no table lookup)
    fn execute_covering_index_scan(
        db: &mut BtreeDatabase,
        table: &str,
        index_name: &str,
        value: &Value,
        limit: Option<i64>,
    ) -> Result<Vec<Record>> {
        // For covering index, we can get data directly from index without table lookup
        // In this simplified implementation, we just use the same method but it's faster
        // because we don't need to access the table data pages
        let records = db.get_records_by_index_covering(table, index_name, value)?;

        // Apply limit
        if let Some(limit) = limit {
            Ok(records.into_iter().take(limit as usize).collect())
        } else {
            Ok(records)
        }
    }

    /// Execute covering index range scan (no table lookup)
    fn execute_covering_index_range_scan(
        db: &mut BtreeDatabase,
        table: &str,
        index_name: &str,
        start: Option<&Value>,
        end: Option<&Value>,
        limit: Option<i64>,
    ) -> Result<Vec<Record>> {
        // For covering index, we can get data directly from index without table lookup
        let records = db.get_records_by_index_range_covering(table, index_name, start, end)?;

        // Apply limit
        if let Some(limit) = limit {
            Ok(records.into_iter().take(limit as usize).collect())
        } else {
            Ok(records)
        }
    }

    /// Execute secondary index point scan
    fn execute_index_scan(
        db: &mut BtreeDatabase,
        table: &str,
        index_name: &str,
        value: &Value,
        limit: Option<i64>,
    ) -> Result<Vec<Record>> {
        let records = db.get_records_by_index(table, index_name, value)?;

        // Apply limit
        if let Some(limit) = limit {
            Ok(records.into_iter().take(limit as usize).collect())
        } else {
            Ok(records)
        }
    }

    /// Execute secondary index range scan
    fn execute_index_range_scan(
        db: &mut BtreeDatabase,
        table: &str,
        index_name: &str,
        start: Option<&Value>,
        end: Option<&Value>,
        limit: Option<i64>,
    ) -> Result<Vec<Record>> {
        let records = db.get_records_by_index_range(table, index_name, start, end)?;

        // Apply limit
        if let Some(limit) = limit {
            Ok(records.into_iter().take(limit as usize).collect())
        } else {
            Ok(records)
        }
    }

    /// Execute full table scan with optional filtering
    /// 
    /// Uses predicate pushdown optimization for better performance
    fn execute_full_scan(
        db: &mut BtreeDatabase,
        table: &str,
        filter: Option<&Expression>,
        table_columns: &[ColumnDef],
        limit: Option<i64>,
    ) -> Result<Vec<Record>> {
        // Try to use predicate pushdown optimization
        if let Some(filter_expr) = filter {
            let (pushdown_filter, remaining_expr) = predicate_pushdown::split_filter(filter_expr);
            
            if pushdown_filter.is_some() {
                return Self::execute_full_scan_with_pushdown(
                    db, table, pushdown_filter, remaining_expr.as_ref(), table_columns, limit,
                );
            }
        }
        
        let all_records = db.select_all(table)?;

        if filter.is_none() && limit.is_none() {
            return Ok(all_records);
        }

        let mut results = Vec::new();

        for record in all_records {
            // Apply filter if present
            let passes = if let Some(filter_expr) = filter {
                Self::evaluate_filter(&record, table_columns, filter_expr)
            } else {
                true
            };

            if passes {
                results.push(record);

                // Early termination with limit
                if let Some(limit) = limit {
                    if results.len() >= limit as usize {
                        break;
                    }
                }
            }
        }

        Ok(results)
    }

    /// Execute full table scan with predicate pushdown
    fn execute_full_scan_with_pushdown(
        db: &mut BtreeDatabase,
        table: &str,
        pushdown_filter: Option<predicate_pushdown::PushdownFilter>,
        remaining_expr: Option<&Expression>,
        table_columns: &[ColumnDef],
        limit: Option<i64>,
    ) -> Result<Vec<Record>> {
        let all_records = db.select_all(table)?;
        let mut results = Vec::new();

        for record in all_records {
            let mut passes = true;

            // Apply pushdown filter first
            if let Some(ref filter) = pushdown_filter {
                if !filter.evaluate(&record, table_columns) {
                    passes = false;
                }
            }

            // Apply remaining expression if needed
            if passes && remaining_expr.is_some() {
                passes = Self::evaluate_filter(&record, table_columns, remaining_expr.unwrap());
            }

            if passes {
                results.push(record);

                // Early termination with limit
                if let Some(limit) = limit {
                    if results.len() >= limit as usize {
                        break;
                    }
                }
            }
        }

        Ok(results)
    }

    /// Evaluate filter expression against a record
    fn evaluate_filter(
        record: &Record,
        table_columns: &[ColumnDef],
        expr: &Expression,
    ) -> bool {
        match expr {
            Expression::Binary { left, op, right } => {
                match op {
                    BinaryOp::Equal | BinaryOp::NotEqual | BinaryOp::Less | 
                    BinaryOp::LessEqual | BinaryOp::Greater | BinaryOp::GreaterEqual => {
                        // Comparison operators
                        if let (Expression::Column(col), Some(val)) = (
                            left.as_ref(),
                            Self::expression_to_value(right)
                        ) {
                            if let Some(col_idx) = table_columns.iter().position(|c| c.name == *col) {
                                if let Some(record_val) = record.values.get(col_idx) {
                                    return Self::compare_values(record_val, op, &val);
                                }
                            }
                        }
                        // Try reverse: value op column
                        if let (Some(val), Expression::Column(col)) = (
                            Self::expression_to_value(left),
                            right.as_ref()
                        ) {
                            if let Some(col_idx) = table_columns.iter().position(|c| c.name == *col) {
                                if let Some(record_val) = record.values.get(col_idx) {
                                    // Swap operator for reverse comparison
                                    let swapped_op = Self::swap_comparison_op(op);
                                    return Self::compare_values(record_val, &swapped_op, &val);
                                }
                            }
                        }
                    }
                    BinaryOp::And => {
                        return Self::evaluate_filter(record, table_columns, left) &&
                               Self::evaluate_filter(record, table_columns, right);
                    }
                    BinaryOp::Or => {
                        return Self::evaluate_filter(record, table_columns, left) ||
                               Self::evaluate_filter(record, table_columns, right);
                    }
                    _ => {}
                }
                true
            }
            _ => true,
        }
    }

    /// Swap comparison operator for reverse comparison
    fn swap_comparison_op(op: &BinaryOp) -> BinaryOp {
        match op {
            BinaryOp::Less => BinaryOp::Greater,
            BinaryOp::LessEqual => BinaryOp::GreaterEqual,
            BinaryOp::Greater => BinaryOp::Less,
            BinaryOp::GreaterEqual => BinaryOp::LessEqual,
            _ => op.clone(),
        }
    }

    /// Convert expression to Value
    fn expression_to_value(expr: &Expression) -> Option<Value> {
        match expr {
            Expression::Integer(n) => Some(Value::Integer(*n)),
            Expression::String(s) => Some(Value::Text(s.clone())),
            Expression::Float(f) => Some(Value::Real(*f)),
            Expression::Boolean(b) => Some(if *b { Value::Integer(1) } else { Value::Integer(0) }),
            Expression::Null => Some(Value::Null),
            Expression::Vector(elements) => {
                let mut vals = Vec::with_capacity(elements.len());
                for e in elements {
                    match Self::expression_to_value(e)? {
                        Value::Real(f) => vals.push(f as f32),
                        Value::Integer(n) => vals.push(n as f32),
                        _ => return None,
                    }
                }
                Some(Value::Vector(vals))
            }
            _ => None,
        }
    }

    /// Compare two values with an operator
    fn compare_values(left: &Value, op: &BinaryOp, right: &Value) -> bool {
        match op {
            BinaryOp::Equal => left == right,
            BinaryOp::NotEqual => left != right,
            BinaryOp::Less => left < right,
            BinaryOp::LessEqual => left <= right,
            BinaryOp::Greater => left > right,
            BinaryOp::GreaterEqual => left >= right,
            _ => true,
        }
    }

    /// Execute HNSW vector similarity scan
    fn execute_hnsw_vector_scan(
        db: &mut BtreeDatabase,
        index_name: &str,
        query_vector: &[f32],
        limit: usize,
    ) -> Result<Vec<Record>> {
        let results = db.vector_search(index_name, query_vector, limit)?;
        Ok(results.into_iter().map(|(r, _)| r).collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::ast::{SelectColumn, ColumnDef, DataType};
    use crate::storage::{BtreeDatabase, Record, Value};
    use tempfile::NamedTempFile;

    fn create_test_db() -> (BtreeDatabase, String) {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap();
        let db = BtreeDatabase::open(path).unwrap();
        (db, path.to_string())
    }

    fn create_test_table(db: &mut BtreeDatabase) {
        let columns = vec![
            ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                primary_key: true,
                foreign_key: None,
                default_value: None,
                is_virtual: false,
                generated_always: None,
            },
            ColumnDef {
                name: "name".to_string(),
                data_type: DataType::Text,
                nullable: true,
                primary_key: false,
                foreign_key: None,
                default_value: None,
                is_virtual: false,
                generated_always: None,
            },
        ];
        db.create_table("users".to_string(), columns).unwrap();

        // Insert some test data
        for i in 1..=10 {
            let record = Record::new(vec![
                Value::Integer(i),
                Value::Text(format!("User{}", i)),
            ]);
            db.insert("users", record).unwrap();
        }
    }

    #[test]
    fn test_extract_point_condition() {
        let expr = Expression::Binary {
            left: Box::new(Expression::Column("id".to_string())),
            op: BinaryOp::Equal,
            right: Box::new(Expression::Integer(5)),
        };

        let result = QueryPlanner::extract_point_condition(&expr);
        assert!(result.is_some());
        let (col, val) = result.unwrap();
        assert_eq!(col, "id");
        assert_eq!(val, Value::Integer(5));
    }

    #[test]
    fn test_extract_range_condition() {
        let expr = Expression::Binary {
            left: Box::new(Expression::Column("id".to_string())),
            op: BinaryOp::Greater,
            right: Box::new(Expression::Integer(5)),
        };

        let result = QueryPlanner::extract_range_condition(&expr);
        assert!(result.is_some());
        let (col, start, end) = result.unwrap();
        assert_eq!(col, "id");
        assert_eq!(start, Some(Value::Integer(5)));
        assert_eq!(end, None);
    }

    #[test]
    fn test_estimate_cost() {
        let (mut db, _) = create_test_db();
        create_test_table(&mut db);

        let plan = QueryPlan::FullTableScan {
            table: "users".to_string(),
            filter: None,
            columns: vec![SelectColumn::Column("*".to_string())],
            limit: None,
        };

        let cost = QueryPlanner::estimate_cost(&plan, &db);
        assert!(cost > 0);
    }
}
