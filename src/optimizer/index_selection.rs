//! Index Selection Optimization
//!
//! Chooses optimal index(es) for query execution:
//! - Analyzes query predicates to find usable indexes
//! - Estimates selectivity and cost for each index option
//! - Supports index intersection (combining multiple indexes)
//! - Prioritizes covering indexes
//!
//! Example decisions:
//! - WHERE a = 1 AND b = 2: Use composite index (a,b) if available
//! - WHERE a = 1 OR b = 2: Consider index union
//! - WHERE a > 1 AND b = 2: Use index on b (more selective)

use crate::storage::{BtreeDatabase, Value, Record, BPlusTreeIndex};
use crate::sql::ast::{Expression, BinaryOp, SelectColumn};
use super::stats::{StatsCollector, PredicateType, StatsCatalog, TableStats};
use super::cost_model::{CostEstimator, PlanCost};
use std::collections::HashMap;

/// Information about available index
#[derive(Debug, Clone)]
pub struct AvailableIndex {
    pub index_name: String,
    pub table_name: String,
    pub column_name: String,
    /// Whether this is a unique index
    pub is_unique: bool,
    /// Whether this index can cover the query
    pub is_covering: bool,
    /// Estimated selectivity (0.0 - 1.0)
    pub selectivity: f64,
    /// Estimated cost
    pub estimated_cost: f64,
}

impl AvailableIndex {
    /// Calculate a score for this index (higher is better)
    pub fn score(&self) -> f64 {
        let mut score = 0.0;
        
        // Unique indexes are highly selective
        if self.is_unique {
            score += 1000.0;
        }
        
        // Covering indexes avoid table lookups
        if self.is_covering {
            score += 500.0;
        }
        
        // Lower selectivity (more selective) is better
        score += (1.0 - self.selectivity) * 100.0;
        
        // Lower cost is better
        score += (1000.0 - self.estimated_cost).max(0.0) * 0.1;
        
        score
    }
}

/// Index selection for a single table query
#[derive(Debug, Clone)]
pub struct IndexSelection {
    /// Primary index choice
    pub primary: Option<AvailableIndex>,
    /// Secondary indexes for index intersection
    pub secondary: Vec<AvailableIndex>,
    /// Whether to use index intersection
    pub use_intersection: bool,
    /// Total estimated cost
    pub total_cost: f64,
    /// Estimated rows to be scanned
    pub estimated_rows: u64,
}

/// Index selector for query optimization
pub struct IndexSelector {
    cost_estimator: CostEstimator,
    stats_catalog: Option<StatsCatalog>,
    /// Threshold for considering index intersection
    intersection_threshold: f64,
}

impl IndexSelector {
    pub fn new() -> Self {
        Self {
            cost_estimator: CostEstimator::new(),
            stats_catalog: None,
            intersection_threshold: 0.5,
        }
    }
    
    pub fn with_cost_estimator(cost_estimator: CostEstimator) -> Self {
        Self {
            cost_estimator,
            stats_catalog: None,
            intersection_threshold: 0.5,
        }
    }
    
    pub fn with_stats(mut self, catalog: StatsCatalog) -> Self {
        self.stats_catalog = Some(catalog);
        self
    }
    
    /// Find the best index for a query
    pub fn select_index(
        &self,
        db: &BtreeDatabase,
        table_name: &str,
        columns: &[SelectColumn],
        filter: Option<&Expression>,
    ) -> Option<IndexSelection> {
        // Get available indexes
        let available_indexes = self.find_available_indexes(db, table_name, columns)?;
        
        if available_indexes.is_empty() {
            return None;
        }
        
        // If we have predicates, evaluate which indexes can help
        let usable_indexes = if let Some(filter) = filter {
            self.evaluate_predicates(&available_indexes, filter, table_name)
        } else {
            available_indexes
        };
        
        if usable_indexes.is_empty() {
            return None;
        }
        
        // Sort by score (highest first)
        let mut sorted_indexes = usable_indexes;
        sorted_indexes.sort_by(|a, b| b.score().partial_cmp(&a.score()).unwrap_or(std::cmp::Ordering::Equal));
        
        // Check if we should use index intersection
        let should_intersect = sorted_indexes.len() >= 2 && 
            sorted_indexes[0].selectivity > self.intersection_threshold;
        
        if should_intersect {
            // Use intersection of top 2 indexes
            let combined_selectivity = sorted_indexes[0].selectivity * sorted_indexes[1].selectivity;
            Some(IndexSelection {
                primary: Some(sorted_indexes[0].clone()),
                secondary: vec![sorted_indexes[1].clone()],
                use_intersection: true,
                total_cost: self.estimate_intersection_cost(&sorted_indexes[0], &sorted_indexes[1]),
                estimated_rows: self.estimate_rows_after_intersection(table_name, combined_selectivity),
            })
        } else {
            // Use single best index
            let best = sorted_indexes.into_iter().next()?;
            Some(IndexSelection {
                estimated_rows: self.estimate_rows_after_index(table_name, &best),
                primary: Some(best),
                secondary: Vec::new(),
                use_intersection: false,
                total_cost: 0.0,
            })
        }
    }
    
    /// Find all available indexes for a table
    fn find_available_indexes(
        &self,
        db: &BtreeDatabase,
        table_name: &str,
        query_columns: &[SelectColumn],
    ) -> Option<Vec<AvailableIndex>> {
        let table = db.get_table(table_name)?;
        let indexes = db.get_table_indexes(table_name);
        
        let mut available = Vec::new();
        
        for idx in indexes {
            let is_covering = self.is_covering_index(&idx, query_columns, table_name);
            let selectivity = self.estimate_index_selectivity(table_name, &idx.column);
            
            available.push(AvailableIndex {
                index_name: idx.name.clone(),
                table_name: table_name.to_string(),
                column_name: idx.column.clone(),
                is_unique: false, // TODO: track unique indexes
                is_covering,
                selectivity,
                estimated_cost: self.estimate_index_cost(&idx, selectivity),
            });
        }
        
        // Also consider rowid as an implicit index
        available.push(AvailableIndex {
            index_name: "PRIMARY".to_string(),
            table_name: table_name.to_string(),
            column_name: "rowid".to_string(),
            is_unique: true,
            is_covering: false,
            selectivity: 1.0 / table.next_rowid.max(1) as f64,
            estimated_cost: 10.0, // Low cost for rowid lookups
        });
        
        Some(available)
    }
    
    /// Check if an index is covering for the query
    fn is_covering_index(
        &self,
        index: &BPlusTreeIndex,
        columns: &[SelectColumn],
        _table_name: &str,
    ) -> bool {
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
    
    /// Estimate selectivity of an index
    fn estimate_index_selectivity(&self, table_name: &str, column_name: &str) -> f64 {
        if let Some(ref catalog) = self.stats_catalog {
            if let Some(table_stats) = catalog.get_table_stats(table_name) {
                if let Some(col_stats) = table_stats.get_column(column_name) {
                    return col_stats.selectivity;
                }
            }
        }
        // Default selectivity estimate
        0.1
    }
    
    /// Estimate cost of using an index
    fn estimate_index_cost(&self, index: &BPlusTreeIndex, selectivity: f64) -> f64 {
        // Base cost for index access
        let base_cost = 5.0;
        
        // Cost increases with expected rows to retrieve
        let row_cost = selectivity * 100.0;
        
        base_cost + row_cost
    }
    
    /// Evaluate predicates to find which indexes can be used
    fn evaluate_predicates(
        &self,
        indexes: &[AvailableIndex],
        filter: &Expression,
        table_name: &str,
    ) -> Vec<AvailableIndex> {
        let mut usable = Vec::new();
        
        // Extract columns referenced in the filter
        let filter_columns = Self::extract_filter_columns(filter);
        
        for idx in indexes {
            // Check if this index can help with any filter condition
            if filter_columns.contains(&idx.column_name) {
                // Calculate more precise selectivity based on predicate
                let predicate_selectivity = self.calculate_predicate_selectivity(
                    filter, &idx.column_name, table_name
                );
                
                let mut updated_idx = idx.clone();
                updated_idx.selectivity = predicate_selectivity;
                updated_idx.estimated_cost = self.estimate_index_cost_with_selectivity(&updated_idx, predicate_selectivity);
                usable.push(updated_idx);
            }
        }
        
        usable
    }
    
    /// Extract column names from filter expression
    fn extract_filter_columns(filter: &Expression) -> Vec<String> {
        let mut columns = Vec::new();
        Self::extract_columns_recursive(filter, &mut columns);
        columns
    }
    
    fn extract_columns_recursive(expr: &Expression, columns: &mut Vec<String>) {
        match expr {
            Expression::Column(name) => {
                if !columns.contains(name) {
                    columns.push(name.clone());
                }
            }
            Expression::Binary { left, right, .. } => {
                Self::extract_columns_recursive(left, columns);
                Self::extract_columns_recursive(right, columns);
            }
            Expression::FunctionCall { args, .. } => {
                for arg in args {
                    Self::extract_columns_recursive(arg, columns);
                }
            }
            _ => {}
        }
    }
    
    /// Calculate selectivity for a predicate on a column
    fn calculate_predicate_selectivity(
        &self,
        filter: &Expression,
        column_name: &str,
        table_name: &str,
    ) -> f64 {
        // Look up column statistics
        if let Some(ref catalog) = self.stats_catalog {
            if let Some(table_stats) = catalog.get_table_stats(table_name) {
                if let Some(col_stats) = table_stats.get_column(column_name) {
                    // Analyze the predicate type
                    if let Some(pred_type) = Self::get_predicate_type(filter, column_name) {
                        return StatsCollector::estimate_selectivity(table_stats, column_name, pred_type);
                    }
                }
            }
        }
        
        // Default selectivity
        0.1
    }
    
    /// Get predicate type from expression
    fn get_predicate_type(filter: &Expression, column_name: &str) -> Option<PredicateType> {
        match filter {
            Expression::Binary { left, op, right } => {
                if let Expression::Column(col) = left.as_ref() {
                    if col == column_name {
                        match op {
                            BinaryOp::Equal => return Some(PredicateType::Equality),
                            BinaryOp::Less | BinaryOp::LessEqual |
                            BinaryOp::Greater | BinaryOp::GreaterEqual => {
                                return Some(PredicateType::Range(0.3)); // Assume 30% range
                            }
                            _ => {}
                        }
                    }
                }
                None
            }
            _ => None,
        }
    }
    
    /// Estimate index cost with specific selectivity
    fn estimate_index_cost_with_selectivity(&self, index: &AvailableIndex, selectivity: f64) -> f64 {
        let base_cost = 5.0;
        let row_cost = selectivity * 100.0;
        let covering_bonus = if index.is_covering { -2.0 } else { 0.0 };
        
        base_cost + row_cost + covering_bonus
    }
    
    /// Estimate cost of index intersection
    fn estimate_intersection_cost(&self, idx1: &AvailableIndex, idx2: &AvailableIndex) -> f64 {
        // Cost of both index accesses plus intersection overhead
        idx1.estimated_cost + idx2.estimated_cost + 10.0
    }
    
    /// Estimate rows after applying index
    fn estimate_rows_after_index(&self, table_name: &str, index: &AvailableIndex) -> u64 {
        if let Some(ref catalog) = self.stats_catalog {
            if let Some(table_stats) = catalog.get_table_stats(table_name) {
                return (table_stats.row_count as f64 * index.selectivity) as u64;
            }
        }
        1000 // Default estimate
    }
    
    /// Estimate rows after index intersection
    fn estimate_rows_after_intersection(&self, table_name: &str, combined_selectivity: f64) -> u64 {
        if let Some(ref catalog) = self.stats_catalog {
            if let Some(table_stats) = catalog.get_table_stats(table_name) {
                return (table_stats.row_count as f64 * combined_selectivity) as u64;
            }
        }
        100 // Default estimate
    }
    
    /// Select index for JOIN condition
    pub fn select_join_index(
        &self,
        db: &BtreeDatabase,
        left_table: &str,
        right_table: &str,
        left_column: &str,
        right_column: &str,
    ) -> Option<(AvailableIndex, AvailableIndex)> {
        // Find indexes for both sides of the join
        let left_indexes = self.find_indexes_for_column(db, left_table, left_column)?;
        let right_indexes = self.find_indexes_for_column(db, right_table, right_column)?;
        
        // Pick the best index from each side
        let best_left = left_indexes.into_iter()
            .max_by(|a, b| a.score().partial_cmp(&b.score()).unwrap_or(std::cmp::Ordering::Equal))?;
        
        let best_right = right_indexes.into_iter()
            .max_by(|a, b| a.score().partial_cmp(&b.score()).unwrap_or(std::cmp::Ordering::Equal))?;
        
        Some((best_left, best_right))
    }
    
    /// Find indexes for a specific column
    fn find_indexes_for_column(
        &self,
        db: &BtreeDatabase,
        table_name: &str,
        column_name: &str,
    ) -> Option<Vec<AvailableIndex>> {
        let indexes = db.get_table_indexes(table_name);
        
        let mut matching = Vec::new();
        for idx in indexes {
            if idx.column == column_name {
                let selectivity = self.estimate_index_selectivity(table_name, column_name);
                matching.push(AvailableIndex {
                    index_name: idx.name.clone(),
                    table_name: table_name.to_string(),
                    column_name: idx.column.clone(),
                    is_unique: false,
                    is_covering: false,
                    selectivity,
                    estimated_cost: self.estimate_index_cost(&idx, selectivity),
                });
            }
        }
        
        if matching.is_empty() {
            None
        } else {
            Some(matching)
        }
    }
    
    /// Recommend indexes that would benefit a query
    pub fn recommend_indexes(
        &self,
        db: &BtreeDatabase,
        table_name: &str,
        filter: Option<&Expression>,
    ) -> Vec<String> {
        let mut recommendations = Vec::new();
        
        if let Some(filter) = filter {
            let filter_columns = Self::extract_filter_columns(filter);
            
            // Get existing index columns
            let existing_indexes = db.get_table_indexes(table_name);
            let indexed_columns: std::collections::HashSet<_> = existing_indexes.iter()
                .map(|idx| idx.column.clone())
                .collect();
            
            // Recommend indexes for columns not already indexed
            for col in filter_columns {
                if !indexed_columns.contains(&col) {
                    recommendations.push(format!("CREATE INDEX idx_{}_{} ON {} ({})",
                        table_name, col, table_name, col));
                }
            }
        }
        
        recommendations
    }
}

impl Default for IndexSelector {
    fn default() -> Self {
        Self::new()
    }
}

/// Index intersection iterator for efficiently combining multiple index results
pub struct IndexIntersection {
    results: Vec<Vec<u64>>, // Row IDs from each index
}

impl IndexIntersection {
    pub fn new(results: Vec<Vec<u64>>) -> Self {
        Self { results }
    }
    
    /// Compute intersection of all result sets
    pub fn intersect(&self) -> Vec<u64> {
        if self.results.is_empty() {
            return Vec::new();
        }
        
        if self.results.len() == 1 {
            return self.results[0].clone();
        }
        
        // Start with the smallest set
        let mut smallest_idx = 0;
        let mut smallest_size = self.results[0].len();
        
        for (i, result) in self.results.iter().enumerate().skip(1) {
            if result.len() < smallest_size {
                smallest_size = result.len();
                smallest_idx = i;
            }
        }
        
        let mut result: std::collections::HashSet<u64> = 
            self.results[smallest_idx].iter().copied().collect();
        
        // Intersect with other sets
        for (i, other) in self.results.iter().enumerate() {
            if i != smallest_idx {
                let other_set: std::collections::HashSet<u64> = 
                    other.iter().copied().collect();
                result = &result & &other_set;
                
                if result.is_empty() {
                    break;
                }
            }
        }
        
        result.into_iter().collect()
    }
    
    /// Compute union of all result sets (for OR conditions)
    pub fn union(&self) -> Vec<u64> {
        let mut result: std::collections::HashSet<u64> = std::collections::HashSet::new();
        
        for other in &self.results {
            result.extend(other.iter().copied());
        }
        
        result.into_iter().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::ast::{ColumnDef, DataType};
    use tempfile::NamedTempFile;

    fn create_test_db_with_indexes() -> BtreeDatabase {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap();
        let mut db = BtreeDatabase::open(path).unwrap();

        // Create table
        let columns = vec![
            ColumnDef { name: "id".to_string(), data_type: DataType::Integer, nullable: false, primary_key: true, foreign_key: None, default_value: None, is_virtual: false, generated_always: None },
            ColumnDef { name: "name".to_string(), data_type: DataType::Text, nullable: true, primary_key: false, foreign_key: None, default_value: None, is_virtual: false, generated_always: None },
            ColumnDef { name: "status".to_string(), data_type: DataType::Text, nullable: true, primary_key: false, foreign_key: None, default_value: None, is_virtual: false, generated_always: None },
            ColumnDef { name: "age".to_string(), data_type: DataType::Integer, nullable: true, primary_key: false, foreign_key: None, default_value: None, is_virtual: false, generated_always: None },
        ];
        db.create_table("users".to_string(), columns).unwrap();

        // Create indexes
        db.create_index("idx_name".to_string(), "users".to_string(), "name".to_string()).unwrap();
        db.create_index("idx_status".to_string(), "users".to_string(), "status".to_string()).unwrap();
        db.create_index("idx_age".to_string(), "users".to_string(), "age".to_string()).unwrap();

        // Insert test data
        for i in 1..=1000 {
            let record = Record::new(vec![
                crate::storage::Value::Integer(i),
                crate::storage::Value::Text(format!("User{}", i)),
                crate::storage::Value::Text(if i % 10 == 0 { "active".to_string() } else { "inactive".to_string() }),
                crate::storage::Value::Integer((i % 50 + 20) as i64),
            ]);
            db.insert("users", record).unwrap();
        }

        db
    }

    #[test]
    fn test_find_available_indexes() {
        let db = create_test_db_with_indexes();
        let selector = IndexSelector::new();
        
        let columns = vec![SelectColumn::Column("name".to_string())];
        let indexes = selector.find_available_indexes(&db, "users", &columns);
        
        assert!(indexes.is_some());
        let indexes = indexes.unwrap();
        assert!(indexes.len() >= 3); // At least 3 indexes + rowid
        
        // Check that we found the name index
        let name_idx = indexes.iter().find(|i| i.column_name == "name");
        assert!(name_idx.is_some());
        assert!(name_idx.unwrap().is_covering);
    }

    #[test]
    fn test_covering_index_detection() {
        let db = create_test_db_with_indexes();
        let selector = IndexSelector::new();
        
        // Query that only needs indexed column
        let columns = vec![SelectColumn::Column("name".to_string())];
        let indexes = selector.find_available_indexes(&db, "users", &columns).unwrap();
        
        let name_idx = indexes.iter().find(|i| i.column_name == "name").unwrap();
        assert!(name_idx.is_covering, "Index on name should be covering for SELECT name");
        
        // Query that needs non-indexed column
        let columns = vec![SelectColumn::Column("id".to_string()), SelectColumn::Column("age".to_string())];
        let indexes = selector.find_available_indexes(&db, "users", &columns).unwrap();
        
        // No index should be covering (id is rowid but age needs table lookup)
        let covering: Vec<_> = indexes.iter().filter(|i| i.is_covering).collect();
        assert!(covering.is_empty(), "No index should be covering for SELECT id, age");
    }

    #[test]
    fn test_index_score() {
        let idx1 = AvailableIndex {
            index_name: "idx_unique".to_string(),
            table_name: "t".to_string(),
            column_name: "id".to_string(),
            is_unique: true,
            is_covering: true,
            selectivity: 0.001,
            estimated_cost: 5.0,
        };
        
        let idx2 = AvailableIndex {
            index_name: "idx_regular".to_string(),
            table_name: "t".to_string(),
            column_name: "name".to_string(),
            is_unique: false,
            is_covering: false,
            selectivity: 0.1,
            estimated_cost: 15.0,
        };
        
        // Unique covering index should have higher score
        assert!(idx1.score() > idx2.score());
    }

    #[test]
    fn test_index_intersection() {
        let intersection = IndexIntersection::new(vec![
            vec![1, 2, 3, 4, 5],
            vec![2, 4, 6, 8],
            vec![4, 5, 6],
        ]);
        
        let result = intersection.intersect();
        assert_eq!(result, vec![4]);
    }

    #[test]
    fn test_index_union() {
        let intersection = IndexIntersection::new(vec![
            vec![1, 2, 3],
            vec![2, 4, 6],
            vec![3, 6, 9],
        ]);
        
        let result = intersection.union();
        let expected: std::collections::HashSet<_> = vec![1, 2, 3, 4, 6, 9].into_iter().collect();
        let actual: std::collections::HashSet<_> = result.into_iter().collect();
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_recommend_indexes() {
        let db = create_test_db_with_indexes();
        let selector = IndexSelector::new();
        
        // Filter on unindexed column
        let filter = Expression::Binary {
            left: Box::new(Expression::Column("unindexed_col".to_string())),
            op: BinaryOp::Equal,
            right: Box::new(Expression::Integer(1)),
        };
        
        let recommendations = selector.recommend_indexes(&db, "users", Some(&filter));
        assert!(!recommendations.is_empty());
        assert!(recommendations[0].contains("unindexed_col"));
    }

    #[test]
    fn test_extract_filter_columns() {
        let filter = Expression::Binary {
            left: Box::new(Expression::Binary {
                left: Box::new(Expression::Column("a".to_string())),
                op: BinaryOp::Equal,
                right: Box::new(Expression::Integer(1)),
            }),
            op: BinaryOp::And,
            right: Box::new(Expression::Binary {
                left: Box::new(Expression::Column("b".to_string())),
                op: BinaryOp::Equal,
                right: Box::new(Expression::Integer(2)),
            }),
        };
        
        let columns = IndexSelector::extract_filter_columns(&filter);
        assert!(columns.contains(&"a".to_string()));
        assert!(columns.contains(&"b".to_string()));
    }
}
