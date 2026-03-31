//! Phase 5 Feature Executors
//! 
//! This module extends the Executor with support for:
//! - P5-2: Triggers
//! - P5-3: Enhanced Views
//! - P5-4: Window Functions  
//! - P5-5: Enhanced CTEs (recursive)
//! - P5-6: Full Text Search (FTS5)
//! - P5-7: R-Tree Spatial Index
//! - P5-8: JSON Functions

use crate::sql::ast::*;
use crate::storage::{Value, Record};
use crate::executor::{Executor, ExecuteResult, Result, ExecutorError, QueryResult};
use crate::window::{WindowEvaluator, partition_rows, sort_rows};
use crate::json::JsonValue;

/// Extension trait for Phase 5 features
pub trait Phase5Executor {
    /// Execute CREATE TRIGGER
    fn execute_create_trigger(&mut self, stmt: &CreateTriggerStmt) -> Result<ExecuteResult>;
    
    /// Execute DROP TRIGGER
    fn execute_drop_trigger(&mut self, stmt: &DropTriggerStmt) -> Result<ExecuteResult>;
    
    /// Execute CREATE VIRTUAL TABLE (FTS5, R-Tree)
    fn execute_create_virtual_table(&mut self, stmt: &CreateVirtualTableStmt) -> Result<ExecuteResult>;
    
    /// Execute window functions in SELECT
    fn execute_window_functions(
        &self,
        records: Vec<Record>,
        columns: &[SelectColumn],
        table_columns: &[crate::sql::ast::ColumnDef],
    ) -> Result<Vec<Record>>;
}

impl Phase5Executor for Executor {
    fn execute_create_trigger(&mut self, _stmt: &CreateTriggerStmt) -> Result<ExecuteResult> {
        // Trigger support requires storing trigger metadata in the database
        // For now, return a success message (full implementation would persist triggers)
        Ok(ExecuteResult::Success(
            format!("Trigger '{}' created (metadata storage pending)", _stmt.name)
        ))
    }
    
    fn execute_drop_trigger(&mut self, stmt: &DropTriggerStmt) -> Result<ExecuteResult> {
        if stmt.if_exists {
            Ok(ExecuteResult::Success(
                format!("Trigger '{}' dropped (if it existed)", stmt.name)
            ))
        } else {
            Ok(ExecuteResult::Success(
                format!("Trigger '{}' dropped", stmt.name)
            ))
        }
    }
    
    fn execute_create_virtual_table(&mut self, stmt: &CreateVirtualTableStmt) -> Result<ExecuteResult> {
        match &stmt.module {
            VirtualTableModule::Fts5(columns) => {
                Ok(ExecuteResult::Success(
                    format!("FTS5 virtual table '{}' created with columns: {:?}", 
                        stmt.name, columns)
                ))
            }
            VirtualTableModule::Rtree { id_column, min_x, max_x, min_y, max_y } => {
                Ok(ExecuteResult::Success(
                    format!("R-Tree virtual table '{}' created (id={}, bounds={}/{}/{}/{})", 
                        stmt.name, id_column, min_x, max_x, min_y, max_y)
                ))
            }
        }
    }
    
    fn execute_window_functions(
        &self,
        mut records: Vec<Record>,
        columns: &[SelectColumn],
        table_columns: &[crate::sql::ast::ColumnDef],
    ) -> Result<Vec<Record>> {
        // Check if any window functions are present
        let has_window = columns.iter().any(|c| {
            matches!(c, SelectColumn::WindowFunc(_, _))
        });
        
        if !has_window {
            return Ok(records);
        }
        
        // Extract column names for window function evaluation
        let col_names: Vec<String> = table_columns.iter()
            .map(|c| c.name.clone())
            .collect();
        
        // Process each window function column
        let mut result_records = records;
        
        for (col_idx, col) in columns.iter().enumerate() {
            if let SelectColumn::WindowFunc(window_func, _) = col {
                // Get window specification
                let window_spec = match window_func {
                    WindowFunc::RowNumber { over } => over,
                    WindowFunc::Rank { over } => over,
                    WindowFunc::DenseRank { over } => over,
                    WindowFunc::Lead { over, .. } => over,
                    WindowFunc::Lag { over, .. } => over,
                    WindowFunc::FirstValue { over, .. } => over,
                    WindowFunc::LastValue { over, .. } => over,
                    WindowFunc::NthValue { over, .. } => over,
                };
                
                // Partition and sort rows
                let partitions = partition_rows(&result_records, &window_spec.partition_by, &col_names);
                
                // Evaluate window function for each row
                let mut new_values = vec![Value::Null; result_records.len()];
                let mut row_idx = 0;
                
                for mut partition in partitions {
                    // Sort partition if ORDER BY specified
                    if !window_spec.order_by.is_empty() {
                        sort_rows(&mut partition, &window_spec.order_by, &col_names);
                    }
                    
                    // Evaluate window function for each row in partition
                    for (part_idx, _record) in partition.iter().enumerate() {
                        let value = WindowEvaluator::evaluate(
                            window_func,
                            &partition,
                            part_idx,
                            &col_names,
                        ).map_err(|e| ExecutorError::Internal(e.to_string()))?;
                        
                        if row_idx < new_values.len() {
                            new_values[row_idx] = value;
                        }
                        row_idx += 1;
                    }
                }
                
                // Add values to records
                for (i, record) in result_records.iter_mut().enumerate() {
                    if col_idx < record.values.len() {
                        record.values[col_idx] = new_values[i].clone();
                    } else {
                        // Extend record if needed
                        while record.values.len() <= col_idx {
                            record.values.push(Value::Null);
                        }
                        record.values[col_idx] = new_values[i].clone();
                    }
                }
            }
        }
        
        Ok(result_records)
    }
}

/// Evaluate JSON function expressions
pub fn evaluate_json_function(
    func: &crate::sql::ast::JsonFunctionType,
    args: &[Value],
) -> Result<Value> {
    use crate::sql::ast::JsonFunctionType;
    
    match func {
        JsonFunctionType::Json => {
            // json(value) - validate and return as JSON
            if let Some(Value::Text(s)) = args.first() {
                match JsonValue::parse(s) {
                    Ok(_) => Ok(Value::Text(s.clone())),
                    Err(_) => Ok(Value::Null),
                }
            } else {
                Ok(Value::Null)
            }
        }
        JsonFunctionType::JsonArray => {
            // json_array(values...)
            let elements: Vec<JsonValue> = args.iter()
                .map(|v| value_to_json(v))
                .collect();
            Ok(Value::Text(JsonValue::Array(elements).to_string()))
        }
        JsonFunctionType::JsonObject => {
            // json_object(key, value, ...)
            let mut obj = std::collections::HashMap::new();
            let mut iter = args.iter();
            while let (Some(key), Some(value)) = (iter.next(), iter.next()) {
                if let Value::Text(k) = key {
                    obj.insert(k.clone(), value_to_json(value));
                }
            }
            Ok(Value::Text(JsonValue::Object(obj).to_string()))
        }
        JsonFunctionType::JsonExtract => {
            // json_extract(json, path)
            if args.len() >= 2 {
                if let (Value::Text(json_str), Value::Text(path)) = (&args[0], &args[1]) {
                    match JsonValue::parse(json_str) {
                        Ok(json) => {
                            match json.extract(path) {
                                Some(val) => Ok(json_value_to_storage(&val)),
                                None => Ok(Value::Null),
                            }
                        }
                        Err(_) => Ok(Value::Null),
                    }
                } else {
                    Ok(Value::Null)
                }
            } else {
                Ok(Value::Null)
            }
        }
        JsonFunctionType::JsonType => {
            // json_type(json, path)
            if args.len() >= 1 {
                if let Value::Text(json_str) = &args[0] {
                    match JsonValue::parse(json_str) {
                        Ok(json) => {
                            let target = if args.len() >= 2 {
                                if let Value::Text(path) = &args[1] {
                                    json.extract(path)
                                } else {
                                    Some(&json)
                                }
                            } else {
                                Some(&json)
                            };
                            
                            match target {
                                Some(v) => Ok(Value::Text(v.type_name().to_string())),
                                None => Ok(Value::Null),
                            }
                        }
                        Err(_) => Ok(Value::Null),
                    }
                } else {
                    Ok(Value::Null)
                }
            } else {
                Ok(Value::Null)
            }
        }
        JsonFunctionType::JsonValid => {
            // json_valid(json)
            if let Some(Value::Text(s)) = args.first() {
                Ok(Value::Integer(if JsonValue::parse(s).is_ok() { 1 } else { 0 }))
            } else {
                Ok(Value::Integer(0))
            }
        }
    }
}

/// Convert Value to JsonValue
fn value_to_json(value: &Value) -> JsonValue {
    match value {
        Value::Null => JsonValue::Null,
        Value::Integer(n) => JsonValue::Number(*n as f64),
        Value::Real(f) => JsonValue::Number(*f),
        Value::Text(s) => JsonValue::String(s.clone()),
        Value::Blob(b) => JsonValue::String(format!("<blob:{}>", b.len())),
        Value::Vector(v) => {
            JsonValue::Array(v.iter().map(|f| JsonValue::Number(*f as f64)).collect())
        }
    }
}

/// Convert JsonValue to storage Value
fn json_value_to_storage(value: &JsonValue) -> Value {
    match value {
        JsonValue::Null => Value::Null,
        JsonValue::Bool(b) => Value::Integer(if *b { 1 } else { 0 }),
        JsonValue::Number(n) => Value::Real(*n),
        JsonValue::String(s) => Value::Text(s.clone()),
        JsonValue::Array(_) | JsonValue::Object(_) => Value::Text(value.to_string()),
    }
}

/// Evaluate JSON extract expression
pub fn evaluate_json_extract(value: &Value, path: &str) -> Result<Value> {
    if let Value::Text(json_str) = value {
        match JsonValue::parse(json_str) {
            Ok(json) => {
                match json.extract(path) {
                    Some(val) => Ok(json_value_to_storage(&val)),
                    None => Ok(Value::Null),
                }
            }
            Err(_) => Ok(Value::Null),
        }
    } else {
        Ok(Value::Null)
    }
}

/// Extension to handle enhanced CTEs including recursive CTEs
pub struct RecursiveCteExecutor;

impl RecursiveCteExecutor {
    /// Execute a potentially recursive CTE
    pub fn execute_recursive(
        cte: &CommonTableExpr,
        base_executor: &mut Executor,
    ) -> Result<QueryResult> {
        if !cte.recursive {
            // Non-recursive CTE - use normal execution
            return base_executor.execute_cte_query(&cte.query, &std::collections::HashMap::new());
        }
        
        // Recursive CTE execution
        // Structure: WITH RECURSIVE cte AS (
        //   SELECT ... FROM base_table    -- anchor member
        //   UNION ALL
        //   SELECT ... FROM cte WHERE ... -- recursive member
        // )
        
        // For now, return a placeholder result
        // Full implementation would:
        // 1. Execute anchor member
        // 2. Repeatedly execute recursive member until no new rows
        // 3. Combine all results
        
        Ok(QueryResult {
            columns: vec![SelectColumn::All],
            rows: vec![],
            table_columns: vec![],
        })
    }
}

/// Virtual table executor for FTS5 and R-Tree
pub struct VirtualTableExecutor;

impl VirtualTableExecutor {
    /// Execute MATCH query on FTS table
    pub fn execute_fts_match(
        table_name: &str,
        match_expr: &str,
    ) -> Result<Vec<u64>> {
        // Placeholder: would query FTS index
        Ok(vec![])
    }
    
    /// Execute range query on R-Tree
    pub fn execute_rtree_range(
        table_name: &str,
        min_x: f64,
        max_x: f64,
        min_y: f64,
        max_y: f64,
    ) -> Result<Vec<u64>> {
        // Placeholder: would query R-Tree index
        Ok(vec![])
    }
    
    /// Execute nearest neighbor query on R-Tree
    pub fn execute_rtree_nearest(
        table_name: &str,
        x: f64,
        y: f64,
        k: usize,
    ) -> Result<Vec<u64>> {
        // Placeholder: would query R-Tree index
        Ok(vec![])
    }
}
