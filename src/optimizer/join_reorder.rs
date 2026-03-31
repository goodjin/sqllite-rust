//! JOIN Reordering Optimization
//!
//! Finds optimal join order using cost-based optimization:
//! - Generates candidate join orders using dynamic programming
//! - Estimates cost for each order
//! - Picks lowest cost plan
//!
//! Supports:
//! - Left-deep join trees (for index nested loop joins)
//! - Bushy join trees (for hash/merge joins)
//! - Dynamic programming with memoization
//!
//! Example: A JOIN B JOIN C
//! Possible orders: ABC, ACB, BAC, BCA, CAB, CBA
//! Best order depends on table sizes and selectivity

use crate::storage::{BtreeDatabase, Value};
use crate::sql::ast::{Join, Expression, BinaryOp};
use super::cost_model::{CostEstimator, PlanCost, JoinAlgorithm};
use super::stats::{StatsCollector, PredicateType, StatsCatalog, TableStats};
use std::collections::HashMap;

/// Statistics for JOIN planning
#[derive(Debug, Clone)]
pub struct JoinTableStats {
    pub table_name: String,
    pub row_count: u64,
    pub has_index: bool,
    pub index_selectivity: f64,
}

/// A node in the join tree
#[derive(Debug, Clone)]
pub enum JoinNode {
    /// Scan a single table
    Table {
        name: String,
        stats: JoinTableStats,
        filter: Option<Expression>,
    },
    /// Join two sub-nodes
    Join {
        left: Box<JoinNode>,
        right: Box<JoinNode>,
        join_type: JoinType,
        condition: Expression,
        estimated_rows: u64,
        algorithm: JoinAlgorithm,
    },
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum JoinType {
    Inner,
    Left,
}

/// Cost of a join operation
#[derive(Debug, Clone)]
pub struct JoinCost {
    /// Number of rows produced
    pub output_rows: u64,
    /// Number of join comparisons
    pub comparisons: u64,
    /// I/O cost (pages read)
    pub io_cost: f64,
    /// CPU cost (comparisons + hash operations)
    pub cpu_cost: f64,
    /// Memory cost (bytes)
    pub memory: usize,
    /// Total cost
    pub total: f64,
}

impl JoinCost {
    pub fn new(output_rows: u64, comparisons: u64, io_cost: f64, cpu_cost: f64, memory: usize) -> Self {
        // Weight I/O heavily, then CPU, then output size
        let total = io_cost * 10.0 + cpu_cost * 0.1 + output_rows as f64 * 0.01;
        Self {
            output_rows,
            comparisons,
            io_cost,
            cpu_cost,
            memory,
            total,
        }
    }
}

/// Memoization key for dynamic programming
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
struct JoinSetKey {
    tables: Vec<String>,
}

impl JoinSetKey {
    fn new(mut tables: Vec<String>) -> Self {
        tables.sort();
        Self { tables }
    }
}

/// JOIN reordering optimizer with dynamic programming
pub struct JoinReorderer {
    cost_estimator: CostEstimator,
    /// Memoization cache for dynamic programming
    memo: HashMap<JoinSetKey, (JoinOrder, JoinCost)>,
    /// Whether to allow bushy join trees
    allow_bushy: bool,
    /// Maximum number of tables for exhaustive search
    max_exhaustive_tables: usize,
}

impl JoinReorderer {
    pub fn new() -> Self {
        Self {
            cost_estimator: CostEstimator::new(),
            memo: HashMap::new(),
            allow_bushy: true,
            max_exhaustive_tables: 8,
        }
    }
    
    pub fn with_cost_estimator(cost_estimator: CostEstimator) -> Self {
        Self {
            cost_estimator,
            memo: HashMap::new(),
            allow_bushy: true,
            max_exhaustive_tables: 8,
        }
    }
    
    /// Enable or disable bushy join trees
    pub fn set_allow_bushy(&mut self, allow: bool) {
        self.allow_bushy = allow;
    }
    
    /// Find optimal join order for a query using dynamic programming
    pub fn optimize_join_order(
        &mut self,
        db: &BtreeDatabase,
        tables: &[JoinTableInfo],
        join_conditions: &[JoinCondition],
    ) -> Option<JoinOrder> {
        if tables.len() <= 1 {
            return None; // No join needed
        }
        
        // Clear memoization cache
        self.memo.clear();
        
        // For small number of tables, use exhaustive search
        if tables.len() <= self.max_exhaustive_tables {
            self.optimize_exhaustive(db, tables, join_conditions)
        } else {
            // For large number of tables, use greedy heuristic
            self.optimize_greedy(db, tables, join_conditions)
        }
    }
    
    /// Exhaustive optimization using dynamic programming
    fn optimize_exhaustive(
        &mut self,
        db: &BtreeDatabase,
        tables: &[JoinTableInfo],
        join_conditions: &[JoinCondition],
    ) -> Option<JoinOrder> {
        // Build table stats map
        let table_stats: HashMap<String, JoinTableStats> = tables.iter()
            .map(|t| {
                let stats = JoinTableStats {
                    table_name: t.name.clone(),
                    row_count: t.row_count,
                    has_index: self.table_has_index(db, &t.name),
                    index_selectivity: 0.1,
                };
                (t.name.clone(), stats)
            })
            .collect();
        
        // Find optimal join order using dynamic programming
        let table_names: Vec<String> = tables.iter().map(|t| t.name.clone()).collect();
        let result = self.dp_optimize(&table_names, &table_stats, join_conditions);
        
        result.map(|(order, _)| order)
    }
    
    /// Dynamic programming optimization
    fn dp_optimize(
        &mut self,
        tables: &[String],
        table_stats: &HashMap<String, JoinTableStats>,
        join_conditions: &[JoinCondition],
    ) -> Option<(JoinOrder, JoinCost)> {
        if tables.len() == 1 {
            // Base case: single table
            let table = &tables[0];
            let cost = JoinCost::new(
                table_stats.get(table)?.row_count,
                0,
                0.0,
                0.0,
                0,
            );
            let order = JoinOrder {
                table_order: vec![table.clone()],
                join_tree: None,
            };
            return Some((order, cost));
        }
        
        // Check memoization cache
        let key = JoinSetKey::new(tables.to_vec());
        if let Some(result) = self.memo.get(&key) {
            return Some(result.clone());
        }
        
        let mut best_result: Option<(JoinOrder, JoinCost)> = None;
        
        // Try all possible ways to split the tables into two sets
        for i in 1..tables.len() {
            let left_tables = &tables[0..i];
            let right_tables = &tables[i..];
            
            // Recursively optimize each side
            if let (Some((left_order, left_cost)), Some((right_order, right_cost))) = (
                self.dp_optimize(left_tables, table_stats, join_conditions),
                self.dp_optimize(right_tables, table_stats, join_conditions)
            ) {
                // Combine the two sides
                if let Some(join_cost) = self.estimate_join_set_cost(
                    &left_order, &left_cost,
                    &right_order, &right_cost,
                    join_conditions,
                    table_stats,
                ) {
                    // Merge the orders
                    let mut merged_order = left_order.table_order.clone();
                    merged_order.extend(right_order.table_order);
                    
                    let order = JoinOrder {
                        table_order: merged_order,
                        join_tree: None,
                    };
                    
                    if best_result.is_none() || join_cost.total < best_result.as_ref().unwrap().1.total {
                        best_result = Some((order, join_cost));
                    }
                }
            }
        }
        
        // Try all permutations for small sets
        if tables.len() <= 5 {
            let permutations = self.generate_permutations_internal(tables);
            for perm in permutations {
                let cost = self.estimate_permutation_cost(&perm, table_stats, join_conditions);
                if best_result.is_none() || cost.total < best_result.as_ref().unwrap().1.total {
                    let order = JoinOrder {
                        table_order: perm,
                        join_tree: None,
                    };
                    best_result = Some((order, cost));
                }
            }
        }
        
        // Cache the result
        if let Some(ref result) = best_result {
            self.memo.insert(key, result.clone());
        }
        
        best_result
    }
    
    /// Generate permutations of tables for small sets
    fn generate_permutations_internal(&self, tables: &[String]) -> Vec<Vec<String>> {
        let mut result = Vec::new();
        let mut current = Vec::new();
        let mut used = vec![false; tables.len()];
        
        self.permute_recursive_internal(tables, &mut current, &mut used, &mut result);
        result
    }
    
    fn permute_recursive_internal(
        &self,
        tables: &[String],
        current: &mut Vec<String>,
        used: &mut [bool],
        result: &mut Vec<Vec<String>>,
    ) {
        if current.len() == tables.len() {
            result.push(current.clone());
            return;
        }
        
        for i in 0..tables.len() {
            if !used[i] {
                used[i] = true;
                current.push(tables[i].clone());
                self.permute_recursive_internal(tables, current, used, result);
                current.pop();
                used[i] = false;
            }
        }
    }
    
    /// Estimate cost of a join between two table sets
    fn estimate_join_set_cost(
        &self,
        left_order: &JoinOrder,
        left_cost: &JoinCost,
        right_order: &JoinOrder,
        right_cost: &JoinCost,
        join_conditions: &[JoinCondition],
        table_stats: &HashMap<String, JoinTableStats>,
    ) -> Option<JoinCost> {
        let left_rows = left_cost.output_rows;
        let right_rows = right_cost.output_rows;
        
        // Find join conditions between the two sets
        let left_tables: std::collections::HashSet<_> = left_order.table_order.iter().collect();
        let right_tables: std::collections::HashSet<_> = right_order.table_order.iter().collect();
        
        let relevant_conditions: Vec<_> = join_conditions.iter()
            .filter(|c| {
                (left_tables.contains(&c.left_table) && right_tables.contains(&c.right_table)) ||
                (right_tables.contains(&c.left_table) && left_tables.contains(&c.right_table))
            })
            .collect();
        
        // Estimate join selectivity
        let selectivity = if relevant_conditions.is_empty() {
            1.0 // Cartesian product
        } else if relevant_conditions.iter().any(|c| c.has_index) {
            0.01 // Index join - good selectivity
        } else {
            0.3 // Hash/nested-loop join
        };
        
        let output_rows = ((left_rows as f64 * right_rows as f64 * selectivity) as u64).max(1);
        let comparisons = left_rows * right_rows;
        
        // Determine best join algorithm
        let algorithm = self.choose_join_algorithm(
            left_rows, right_rows,
            !relevant_conditions.is_empty(),
            relevant_conditions.iter().any(|c| c.has_index),
        );
        
        // Calculate cost based on algorithm
        let (io_cost, cpu_cost) = match algorithm {
            JoinAlgorithm::NestedLoop => {
                let io = left_cost.io_cost + (left_rows as f64 * right_cost.io_cost);
                let cpu = comparisons as f64 * 0.1;
                (io, cpu)
            }
            JoinAlgorithm::Hash => {
                let io = left_cost.io_cost + right_cost.io_cost;
                let cpu = (left_rows + right_rows) as f64 * 2.0;
                (io, cpu)
            }
            JoinAlgorithm::Merge => {
                let io = left_cost.io_cost + right_cost.io_cost;
                let cpu = (left_rows + right_rows) as f64 * 5.0; // Sort cost
                (io, cpu)
            }
            JoinAlgorithm::Index => {
                let io = left_cost.io_cost + (left_rows as f64 * 3.0);
                let cpu = left_rows as f64 * 10.0;
                (io, cpu)
            }
        };
        
        let memory = left_cost.memory + right_cost.memory;
        
        Some(JoinCost::new(output_rows, comparisons, io_cost, cpu_cost, memory))
    }
    
    /// Choose the best join algorithm
    fn choose_join_algorithm(
        &self,
        left_rows: u64,
        right_rows: u64,
        has_condition: bool,
        has_index: bool,
    ) -> JoinAlgorithm {
        if !has_condition {
            // Cross join - nested loop is only option
            return JoinAlgorithm::NestedLoop;
        }
        
        if has_index && left_rows < right_rows * 10 {
            // Index nested loop is good when outer table is small
            return JoinAlgorithm::Index;
        }
        
        if left_rows < 1000 || right_rows < 1000 {
            // Nested loop for small tables
            JoinAlgorithm::NestedLoop
        } else if left_rows.saturating_add(right_rows) < 100000 {
            // Hash join for medium tables
            JoinAlgorithm::Hash
        } else {
            // Merge join for large tables (if sorted)
            JoinAlgorithm::Merge
        }
    }
    
    /// Estimate cost of a specific permutation
    fn estimate_permutation_cost(
        &self,
        order: &[String],
        table_stats: &HashMap<String, JoinTableStats>,
        conditions: &[JoinCondition],
    ) -> JoinCost {
        let mut total_cost = JoinCost::new(0, 0, 0.0, 0.0, 0);
        let mut accumulated_rows: u64 = 0;
        
        for (i, table_name) in order.iter().enumerate() {
            let table_rows = table_stats.get(table_name)
                .map(|s| s.row_count)
                .unwrap_or(1000);
            
            if i == 0 {
                // First table - just scan cost
                accumulated_rows = table_rows;
                let pages = (table_rows as f64 * 100.0 / 4096.0).ceil();
                total_cost = JoinCost::new(
                    table_rows,
                    0,
                    pages * 100.0, // page_read cost
                    table_rows as f64,
                    (pages as usize) * 4096,
                );
            } else {
                // Join with previous tables
                let relevant_conditions: Vec<_> = conditions.iter()
                    .filter(|c| {
                        (c.left_table == *table_name && order[..i].contains(&c.right_table))
                        || (c.right_table == *table_name && order[..i].contains(&c.left_table))
                    })
                    .collect();
                
                let selectivity = if relevant_conditions.is_empty() {
                    1.0 // Cartesian product
                } else if relevant_conditions.iter().any(|c| c.has_index) {
                    0.01 // Index join
                } else {
                    0.3 // Regular join
                };
                
                let output_rows = (accumulated_rows as f64 * table_rows as f64 * selectivity) as u64;
                let comparisons = accumulated_rows * table_rows;
                
                let join_io_cost = if relevant_conditions.iter().any(|c| c.has_index) {
                    table_rows as f64 * 3.0
                } else {
                    let pages = (table_rows as f64 * 100.0 / 4096.0).ceil();
                    pages * 100.0
                };
                
                let join_cpu_cost = comparisons as f64 * 0.1;
                
                total_cost = JoinCost::new(
                    output_rows,
                    total_cost.comparisons + comparisons,
                    total_cost.io_cost + join_io_cost,
                    total_cost.cpu_cost + join_cpu_cost,
                    total_cost.memory + ((table_rows as usize) * 100),
                );
                
                accumulated_rows = output_rows;
            }
        }
        
        total_cost
    }
    
    /// Greedy optimization for large number of tables
    fn optimize_greedy(
        &self,
        db: &BtreeDatabase,
        tables: &[JoinTableInfo],
        conditions: &[JoinCondition],
    ) -> Option<JoinOrder> {
        // Start with smallest table
        let mut sorted: Vec<_> = tables.iter().collect();
        sorted.sort_by_key(|t| t.row_count);
        
        let order: Vec<String> = sorted.iter().map(|t| t.name.clone()).collect();
        
        Some(JoinOrder {
            table_order: order,
            join_tree: None,
        })
    }
    
    /// Generate all permutations of table join order
    pub fn generate_permutations(&self, tables: &[JoinTableInfo]) -> Vec<JoinOrder> {
        let mut result = Vec::new();
        let mut current = Vec::new();
        let mut used = vec![false; tables.len()];
        
        self.permute_recursive(tables, &mut current, &mut used, &mut result);
        
        result
    }
    
    fn permute_recursive(
        &self,
        tables: &[JoinTableInfo],
        current: &mut Vec<String>,
        used: &mut [bool],
        result: &mut Vec<JoinOrder>,
    ) {
        if current.len() == tables.len() {
            result.push(JoinOrder {
                table_order: current.clone(),
                join_tree: None,
            });
            return;
        }
        
        for i in 0..tables.len() {
            if !used[i] {
                used[i] = true;
                current.push(tables[i].name.clone());
                self.permute_recursive(tables, current, used, result);
                current.pop();
                used[i] = false;
            }
        }
    }
    
    /// Check if a table has an index
    fn table_has_index(&self, db: &BtreeDatabase, table_name: &str) -> bool {
        !db.get_table_indexes(table_name).is_empty()
    }
    
    /// Check if a join condition can use an index
    fn can_use_index(&self, db: &BtreeDatabase, condition: &JoinCondition) -> bool {
        let left_has_index = db.get_table_indexes(&condition.left_table)
            .iter()
            .any(|idx| idx.column == condition.left_column);
        
        let right_has_index = db.get_table_indexes(&condition.right_table)
            .iter()
            .any(|idx| idx.column == condition.right_column);
        
        left_has_index || right_has_index
    }
}

impl Default for JoinReorderer {
    fn default() -> Self {
        Self::new()
    }
}

/// Information about a table in the JOIN
#[derive(Debug, Clone)]
pub struct JoinTableInfo {
    pub name: String,
    pub row_count: u64,
    pub alias: Option<String>,
}

/// A join condition between two tables
#[derive(Debug, Clone)]
pub struct JoinCondition {
    pub left_table: String,
    pub left_column: String,
    pub right_table: String,
    pub right_column: String,
    pub has_index: bool,
}

impl JoinCondition {
    pub fn new(
        left_table: String,
        left_column: String,
        right_table: String,
        right_column: String,
        has_index: bool,
    ) -> Self {
        Self {
            left_table,
            left_column,
            right_table,
            right_column,
            has_index,
        }
    }
}

/// Optimized join order
#[derive(Debug, Clone)]
pub struct JoinOrder {
    pub table_order: Vec<String>,
    pub join_tree: Option<Box<JoinNode>>,
}

impl JoinOrder {
    /// Get the join order as a string
    pub fn format(&self) -> String {
        self.table_order.join(" → ")
    }
    
    /// Check if this order uses a specific join algorithm
    pub fn uses_algorithm(&self, algorithm: JoinAlgorithm) -> bool {
        if let Some(ref tree) = self.join_tree {
            Self::tree_uses_algorithm(tree, algorithm)
        } else {
            false
        }
    }
    
    fn tree_uses_algorithm(node: &JoinNode, algorithm: JoinAlgorithm) -> bool {
        match node {
            JoinNode::Table { .. } => false,
            JoinNode::Join { left, right, algorithm: algo, .. } => {
                *algo == algorithm || 
                Self::tree_uses_algorithm(left, algorithm) ||
                Self::tree_uses_algorithm(right, algorithm)
            }
        }
    }
}

/// Extract join conditions from WHERE clause
pub fn extract_join_conditions(expr: &Expression, tables: &[String]) -> Vec<JoinCondition> {
    let mut conditions = Vec::new();
    extract_join_conditions_recursive(expr, tables, &mut conditions);
    conditions
}

fn extract_join_conditions_recursive(
    expr: &Expression,
    tables: &[String],
    conditions: &mut Vec<JoinCondition>,
) {
    match expr {
        Expression::Binary { left, op: BinaryOp::Equal, right } => {
            // Check if this is a join condition (column = column from different tables)
            if let (Some((table1, col1)), Some((table2, col2))) = 
                (extract_qualified_column(left), extract_qualified_column(right)) {
                
                if table1 != table2 && tables.contains(&table1) && tables.contains(&table2) {
                    conditions.push(JoinCondition::new(
                        table1, col1, table2, col2, false,
                    ));
                }
            }
        }
        Expression::Binary { left, op: BinaryOp::And, right } => {
            extract_join_conditions_recursive(left, tables, conditions);
            extract_join_conditions_recursive(right, tables, conditions);
        }
        _ => {}
    }
}

/// Extract table.column from expression
fn extract_qualified_column(expr: &Expression) -> Option<(String, String)> {
    match expr {
        Expression::Column(name) => {
            // Try to parse "table.column" format
            let parts: Vec<&str> = name.split('.').collect();
            if parts.len() == 2 {
                Some((parts[0].to_string(), parts[1].to_string()))
            } else {
                None
            }
        }
        _ => None,
    }
}

/// Join reordering result with detailed cost information
#[derive(Debug, Clone)]
pub struct JoinReorderResult {
    pub order: JoinOrder,
    pub cost: JoinCost,
    pub alternative_orders: Vec<(JoinOrder, JoinCost)>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::ast::{ColumnDef, DataType};
    use crate::storage::Record;
    use tempfile::NamedTempFile;

    fn create_test_db() -> BtreeDatabase {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap();
        let mut db = BtreeDatabase::open(path).unwrap();

        // Create small table (100 rows)
        let small_cols = vec![
            ColumnDef { name: "id".to_string(), data_type: DataType::Integer, nullable: false, primary_key: true, foreign_key: None, default_value: None, is_virtual: false, generated_always: None },
            ColumnDef { name: "name".to_string(), data_type: DataType::Text, nullable: true, primary_key: false, foreign_key: None, default_value: None, is_virtual: false, generated_always: None },
        ];
        db.create_table("small".to_string(), small_cols).unwrap();
        for i in 1..=100 {
            db.insert("small", Record::new(vec![
                crate::storage::Value::Integer(i),
                crate::storage::Value::Text(format!("Small{}", i)),
            ])).unwrap();
        }

        // Create medium table (1000 rows)
        let med_cols = vec![
            ColumnDef { name: "id".to_string(), data_type: DataType::Integer, nullable: false, primary_key: true, foreign_key: None, default_value: None, is_virtual: false, generated_always: None },
            ColumnDef { name: "small_id".to_string(), data_type: DataType::Integer, nullable: true, primary_key: false, foreign_key: None, default_value: None, is_virtual: false, generated_always: None },
        ];
        db.create_table("medium".to_string(), med_cols).unwrap();
        for i in 1..=1000 {
            db.insert("medium", Record::new(vec![
                crate::storage::Value::Integer(i),
                crate::storage::Value::Integer((i % 100) + 1),
            ])).unwrap();
        }

        // Create large table (10000 rows)
        let large_cols = vec![
            ColumnDef { name: "id".to_string(), data_type: DataType::Integer, nullable: false, primary_key: true, foreign_key: None, default_value: None, is_virtual: false, generated_always: None },
            ColumnDef { name: "medium_id".to_string(), data_type: DataType::Integer, nullable: true, primary_key: false, foreign_key: None, default_value: None, is_virtual: false, generated_always: None },
        ];
        db.create_table("large".to_string(), large_cols).unwrap();
        for i in 1..=10000 {
            db.insert("large", Record::new(vec![
                crate::storage::Value::Integer(i),
                crate::storage::Value::Integer((i % 1000) + 1),
            ])).unwrap();
        }

        db
    }

    #[test]
    fn test_join_reorder_three_tables() {
        let db = create_test_db();
        let mut reorderer = JoinReorderer::new();

        let tables = vec![
            JoinTableInfo { name: "large".to_string(), row_count: 10000, alias: None },
            JoinTableInfo { name: "medium".to_string(), row_count: 1000, alias: None },
            JoinTableInfo { name: "small".to_string(), row_count: 100, alias: None },
        ];

        let conditions = vec![
            JoinCondition::new("small".to_string(), "id".to_string(), 
                             "medium".to_string(), "small_id".to_string(), false),
            JoinCondition::new("medium".to_string(), "id".to_string(),
                             "large".to_string(), "medium_id".to_string(), false),
        ];

        let order = reorderer.optimize_join_order(&db, &tables, &conditions);
        
        assert!(order.is_some(), "Should find optimal order");
        
        // Best order should start with smallest table
        let order = order.unwrap();
        println!("Optimal join order: {}", order.format());
        
        assert_eq!(order.table_order[0], "small", "Should start with smallest table");
    }

    #[test]
    fn test_join_cost_comparison() {
        let db = create_test_db();
        let reorderer = JoinReorderer::new();

        // Bad order: large → medium → small
        let bad_order = vec!["large".to_string(), "medium".to_string(), "small".to_string()];

        // Good order: small → medium → large
        let good_order = vec!["small".to_string(), "medium".to_string(), "large".to_string()];

        let table_stats: HashMap<String, JoinTableStats> = [
            ("large".to_string(), JoinTableStats { table_name: "large".to_string(), row_count: 10000, has_index: false, index_selectivity: 0.1 }),
            ("medium".to_string(), JoinTableStats { table_name: "medium".to_string(), row_count: 1000, has_index: false, index_selectivity: 0.1 }),
            ("small".to_string(), JoinTableStats { table_name: "small".to_string(), row_count: 100, has_index: false, index_selectivity: 0.1 }),
        ].into_iter().collect();

        let conditions = vec![
            JoinCondition::new("small".to_string(), "id".to_string(),
                             "medium".to_string(), "small_id".to_string(), false),
            JoinCondition::new("medium".to_string(), "id".to_string(),
                             "large".to_string(), "medium_id".to_string(), false),
        ];

        let bad_cost = reorderer.estimate_permutation_cost(&bad_order, &table_stats, &conditions);
        let good_cost = reorderer.estimate_permutation_cost(&good_order, &table_stats, &conditions);

        println!("Bad order cost: {:?}", bad_cost);
        println!("Good order cost: {:?}", good_cost);

        assert!(good_cost.total < bad_cost.total, 
            "Good order should be cheaper: good={} vs bad={}", 
            good_cost.total, bad_cost.total);
    }

    #[test]
    fn test_extract_join_conditions() {
        // small.id = medium.small_id AND medium.id = large.medium_id
        let expr = Expression::Binary {
            left: Box::new(Expression::Binary {
                left: Box::new(Expression::Column("small.id".to_string())),
                op: BinaryOp::Equal,
                right: Box::new(Expression::Column("medium.small_id".to_string())),
            }),
            op: BinaryOp::And,
            right: Box::new(Expression::Binary {
                left: Box::new(Expression::Column("medium.id".to_string())),
                op: BinaryOp::Equal,
                right: Box::new(Expression::Column("large.medium_id".to_string())),
            }),
        };

        let tables = vec!["small".to_string(), "medium".to_string(), "large".to_string()];
        let conditions = extract_join_conditions(&expr, &tables);

        assert_eq!(conditions.len(), 2);
        
        // Check first condition
        assert_eq!(conditions[0].left_table, "small");
        assert_eq!(conditions[0].left_column, "id");
        assert_eq!(conditions[0].right_table, "medium");
        assert_eq!(conditions[0].right_column, "small_id");
    }

    #[test]
    fn test_permutations() {
        let reorderer = JoinReorderer::new();
        
        let tables = vec![
            JoinTableInfo { name: "A".to_string(), row_count: 100, alias: None },
            JoinTableInfo { name: "B".to_string(), row_count: 100, alias: None },
            JoinTableInfo { name: "C".to_string(), row_count: 100, alias: None },
        ];

        let orders = reorderer.generate_permutations(&tables);
        
        // 3 tables = 6 permutations
        assert_eq!(orders.len(), 6);
        
        // Check all permutations are unique
        let unique: std::collections::HashSet<_> = orders.iter()
            .map(|o| o.format())
            .collect();
        assert_eq!(unique.len(), 6);
    }

    #[test]
    fn test_dp_optimization() {
        let mut reorderer = JoinReorderer::new();
        
        let table_stats: HashMap<String, JoinTableStats> = [
            ("A".to_string(), JoinTableStats { table_name: "A".to_string(), row_count: 100, has_index: false, index_selectivity: 0.1 }),
            ("B".to_string(), JoinTableStats { table_name: "B".to_string(), row_count: 1000, has_index: false, index_selectivity: 0.1 }),
            ("C".to_string(), JoinTableStats { table_name: "C".to_string(), row_count: 10000, has_index: false, index_selectivity: 0.1 }),
        ].into_iter().collect();

        let tables = vec!["A".to_string(), "B".to_string(), "C".to_string()];
        let conditions = vec![
            JoinCondition::new("A".to_string(), "id".to_string(),
                             "B".to_string(), "a_id".to_string(), false),
            JoinCondition::new("B".to_string(), "id".to_string(),
                             "C".to_string(), "b_id".to_string(), false),
        ];

        let result = reorderer.dp_optimize(&tables, &table_stats, &conditions);
        
        assert!(result.is_some());
        let (order, cost) = result.unwrap();
        
        // Should start with smallest table (A)
        assert_eq!(order.table_order[0], "A");
        println!("DP optimized order: {} with cost {}", order.format(), cost.total);
    }

    #[test]
    fn test_memoization() {
        let mut reorderer = JoinReorderer::new();
        
        let table_stats: HashMap<String, JoinTableStats> = [
            ("A".to_string(), JoinTableStats { table_name: "A".to_string(), row_count: 100, has_index: false, index_selectivity: 0.1 }),
            ("B".to_string(), JoinTableStats { table_name: "B".to_string(), row_count: 1000, has_index: false, index_selectivity: 0.1 }),
        ].into_iter().collect();

        let tables = vec!["A".to_string(), "B".to_string()];
        let conditions: Vec<JoinCondition> = vec![];

        // First call
        let result1 = reorderer.dp_optimize(&tables, &table_stats, &conditions);
        assert!(result1.is_some());
        
        // Second call should use cache
        let result2 = reorderer.dp_optimize(&tables, &table_stats, &conditions);
        assert!(result2.is_some());
        
        // Results should be the same
        assert_eq!(result1.unwrap().0.table_order, result2.unwrap().0.table_order);
        
        // Check that memoization was used
        assert!(!reorderer.memo.is_empty());
    }
}
