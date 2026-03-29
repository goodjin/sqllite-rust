//! JOIN Reordering Optimization
//!
//! Finds optimal join order using cost-based optimization:
//! - Generates candidate join orders
//! - Estimates cost for each order
//! - Picks lowest cost plan
//!
//! Example: A JOIN B JOIN C
//! Possible orders: ABC, ACB, BAC, BCA, CAB, CBA
//! Best order depends on table sizes and selectivity

use crate::storage::{BtreeDatabase, Value};
use crate::sql::ast::{Join, Expression, BinaryOp};
use super::cost_model::{CostEstimator, PlanCost};
use super::stats::{StatsCollector, PredicateType};

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
    /// Total cost
    pub total: f64,
}

impl JoinCost {
    pub fn new(output_rows: u64, comparisons: u64, io_cost: f64, cpu_cost: f64) -> Self {
        // Weight I/O heavily, then CPU, then output size
        let total = io_cost * 10.0 + cpu_cost * 0.1 + output_rows as f64 * 0.01;
        Self {
            output_rows,
            comparisons,
            io_cost,
            cpu_cost,
            total,
        }
    }
}

/// JOIN reordering optimizer
pub struct JoinReorderer {
    cost_estimator: CostEstimator,
}

impl JoinReorderer {
    pub fn new() -> Self {
        Self {
            cost_estimator: CostEstimator::new(),
        }
    }

    /// Find optimal join order for a query
    pub fn optimize_join_order(
        &self,
        db: &BtreeDatabase,
        tables: &[JoinTableInfo],
        join_conditions: &[JoinCondition],
    ) -> Option<JoinOrder> {
        if tables.len() <= 1 {
            return None; // No join needed
        }

        // Generate all possible join orders (up to 5 tables to avoid explosion)
        let orders = if tables.len() <= 5 {
            self.generate_permutations(tables)
        } else {
            // For many tables, use greedy heuristic
            self.generate_greedy_order(tables, join_conditions)
        };

        // Evaluate cost of each order
        let mut best_order: Option<(JoinOrder, JoinCost)> = None;

        for order in orders {
            let cost = self.estimate_join_order_cost(db, &order, join_conditions);
            
            if let Some((_, ref best_cost)) = best_order {
                if cost.total < best_cost.total {
                    best_order = Some((order, cost));
                }
            } else {
                best_order = Some((order, cost));
            }
        }

        best_order.map(|(order, _)| order)
    }

    /// Generate all permutations of table join order
    fn generate_permutations(&self, tables: &[JoinTableInfo]) -> Vec<JoinOrder> {
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

    /// Generate greedy join order (for large number of tables)
    fn generate_greedy_order(
        &self,
        tables: &[JoinTableInfo],
        _conditions: &[JoinCondition],
    ) -> Vec<JoinOrder> {
        // Start with smallest table
        let mut sorted: Vec<_> = tables.iter().collect();
        sorted.sort_by_key(|t| t.row_count);

        let order: Vec<String> = sorted.iter().map(|t| t.name.clone()).collect();

        vec![JoinOrder { table_order: order }]
    }

    /// Estimate cost of a specific join order
    fn estimate_join_order_cost(
        &self,
        db: &BtreeDatabase,
        order: &JoinOrder,
        conditions: &[JoinCondition],
    ) -> JoinCost {
        let mut total_cost = JoinCost::new(0, 0, 0.0, 0.0);
        let mut accumulated_rows: u64 = 0;

        // Build join tree from left to right
        for (i, table_name) in order.table_order.iter().enumerate() {
            let table = match db.get_table(table_name) {
                Some(t) => t,
                None => continue,
            };

            let table_rows = table.next_rowid;

            if i == 0 {
                // First table - just scan cost
                accumulated_rows = table_rows;
                let pages = (table_rows as f64 * 100.0 / 4096.0).ceil();
                total_cost = JoinCost::new(
                    table_rows,
                    0,
                    pages * 100.0, // page_read cost
                    table_rows as f64,
                );
            } else {
                // Join with previous tables
                // Find relevant join conditions
                let relevant_conditions: Vec<_> = conditions.iter()
                    .filter(|c| {
                        (c.left_table == *table_name && order.table_order[..i].contains(&c.right_table))
                        || (c.right_table == *table_name && order.table_order[..i].contains(&c.left_table))
                    })
                    .collect();

                // Estimate join selectivity
                let selectivity = if relevant_conditions.is_empty() {
                    1.0 // Cartesian product
                } else if relevant_conditions.iter().any(|c| c.has_index) {
                    0.1 // Index join - good selectivity
                } else {
                    0.3 // Hash/nested-loop join
                };

                let output_rows = (accumulated_rows as f64 * table_rows as f64 * selectivity) as u64;
                let comparisons = accumulated_rows * table_rows;

                // Cost of joining
                let join_io_cost = if relevant_conditions.iter().any(|c| c.has_index) {
                    // Index join - probe cost
                    table_rows as f64 * 3.0 // 3 pages per probe
                } else {
                    // Hash join - scan both
                    let pages = (table_rows as f64 * 100.0 / 4096.0).ceil();
                    pages * 100.0
                };

                let join_cpu_cost = comparisons as f64 * 0.1;

                total_cost = JoinCost::new(
                    output_rows,
                    total_cost.comparisons + comparisons,
                    total_cost.io_cost + join_io_cost,
                    total_cost.cpu_cost + join_cpu_cost,
                );

                accumulated_rows = output_rows;
            }
        }

        total_cost
    }

    /// Check if a join condition can use an index
    fn can_use_index(&self, db: &BtreeDatabase, condition: &JoinCondition) -> bool {
        // Check if either table has an index on the join column
        let left_has_index = db.get_table_indexes(&condition.left_table)
            .iter()
            .any(|idx| idx.column == condition.left_column);

        let right_has_index = db.get_table_indexes(&condition.right_table)
            .iter()
            .any(|idx| idx.column == condition.right_column);

        left_has_index || right_has_index
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
}

impl JoinOrder {
    /// Get the join order as a string
    pub fn format(&self) -> String {
        self.table_order.join(" → ")
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
            ColumnDef { name: "id".to_string(), data_type: DataType::Integer, nullable: false, primary_key: true, foreign_key: None },
            ColumnDef { name: "name".to_string(), data_type: DataType::Text, nullable: true, primary_key: false, foreign_key: None },
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
            ColumnDef { name: "id".to_string(), data_type: DataType::Integer, nullable: false, primary_key: true, foreign_key: None },
            ColumnDef { name: "small_id".to_string(), data_type: DataType::Integer, nullable: true, primary_key: false, foreign_key: None },
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
            ColumnDef { name: "id".to_string(), data_type: DataType::Integer, nullable: false, primary_key: true, foreign_key: None },
            ColumnDef { name: "medium_id".to_string(), data_type: DataType::Integer, nullable: true, primary_key: false, foreign_key: None },
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
        let reorderer = JoinReorderer::new();

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
        let bad_order = JoinOrder {
            table_order: vec!["large".to_string(), "medium".to_string(), "small".to_string()],
        };

        // Good order: small → medium → large
        let good_order = JoinOrder {
            table_order: vec!["small".to_string(), "medium".to_string(), "large".to_string()],
        };

        let conditions = vec![
            JoinCondition::new("small".to_string(), "id".to_string(),
                             "medium".to_string(), "small_id".to_string(), false),
            JoinCondition::new("medium".to_string(), "id".to_string(),
                             "large".to_string(), "medium_id".to_string(), false),
        ];

        let bad_cost = reorderer.estimate_join_order_cost(&db, &bad_order, &conditions);
        let good_cost = reorderer.estimate_join_order_cost(&db, &good_order, &conditions);

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
}
