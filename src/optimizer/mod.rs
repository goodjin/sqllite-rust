//! Query Optimizer Module
//!
//! Provides cost-based query optimization:
//! - Statistics collection (P4-1)
//! - Cost estimation (P4-2)
//! - JOIN reordering (P4-3)
//! - Index selection (P4-4)
//! - Subquery optimization (P4-5)
//!
//! Performance targets:
//! - 10x complex JOIN performance improvement
//! - 10x subquery performance improvement  
//! - 3x multi-index scenario performance

pub mod stats;
pub mod cost_model;
pub mod join_reorder;
pub mod index_selection;
pub mod subquery;

// Re-export statistics types
pub use stats::{
    StatsCollector, 
    TableStats, 
    ColumnStats, 
    StatsCatalog, 
    PredicateType,
    Histogram,
    StatsConfig,
};

// Re-export cost model types
pub use cost_model::{
    CostEstimator, 
    PlanCost, 
    CostUnits, 
    JoinAlgorithm,
    choose_cheaper_plan,
    choose_best_plan,
};

// Re-export join reorder types
pub use join_reorder::{
    JoinReorderer, 
    JoinOrder, 
    JoinCondition, 
    JoinTableInfo, 
    JoinNode,
    JoinType,
    JoinCost,
    extract_join_conditions,
};

// Re-export index selection types
pub use index_selection::{
    IndexSelector,
    IndexSelection,
    AvailableIndex,
    IndexIntersection,
};

// Re-export subquery optimization types
pub use subquery::{
    SubqueryOptimizer,
    SubqueryInfo,
    SubqueryType,
    SubqueryCache,
    MaterializedSubquery,
    analyze_subqueries,
    SubqueryAnalysis,
};

use crate::executor::QueryPlan;
use crate::storage::BtreeDatabase;
use crate::sql::ast::{SelectStmt, Expression};

/// Comprehensive query optimizer that combines all optimization techniques
pub struct QueryOptimizer {
    stats_collector: StatsCollector,
    cost_estimator: CostEstimator,
    join_reorderer: JoinReorderer,
    index_selector: IndexSelector,
    stats_catalog: StatsCatalog,
    subquery_cache: SubqueryCache,
}

impl QueryOptimizer {
    /// Create a new query optimizer with default configuration
    pub fn new() -> Self {
        let stats_catalog = StatsCatalog::new();
        let cost_estimator = CostEstimator::new();
        
        Self {
            stats_collector: StatsCollector::new(),
            cost_estimator,
            join_reorderer: JoinReorderer::new(),
            index_selector: IndexSelector::new(),
            stats_catalog,
            subquery_cache: SubqueryCache::new(),
        }
    }
    
    /// Create optimizer with custom configuration
    pub fn with_config(config: OptimizerConfig) -> Self {
        let stats_config = StatsConfig::default();
        let stats_collector = StatsCollector::with_config(stats_config.clone());
        let stats_catalog = StatsCatalog::with_config(stats_config);
        
        let cost_estimator = if let Some(units) = config.cost_units {
            CostEstimator::with_units(units)
        } else {
            CostEstimator::new()
        };
        
        Self {
            stats_collector,
            cost_estimator,
            join_reorderer: JoinReorderer::new(),
            index_selector: IndexSelector::new(),
            stats_catalog,
            subquery_cache: SubqueryCache::with_capacity(config.subquery_cache_size),
        }
    }
    
    /// Collect statistics for all tables in the database
    pub fn collect_database_stats(&mut self, db: &mut BtreeDatabase) {
        let tables: Vec<String> = db.list_tables().into_iter().cloned().collect();
        for table_name in tables {
            if let Some(stats) = self.stats_collector.collect_table_stats(db, &table_name) {
                self.stats_catalog.update_table_stats(stats);
            }
        }
    }
    
    /// Collect statistics for a specific table
    pub fn collect_table_stats(&mut self, db: &mut BtreeDatabase, table_name: &str) -> Option<&TableStats> {
        self.stats_catalog.collect_stats(db, table_name)
    }
    
    /// Optimize a SELECT statement
    pub fn optimize_select(&mut self, db: &mut BtreeDatabase, stmt: &mut SelectStmt) -> OptimizationResult {
        let mut result = OptimizationResult::default();
        
        // Step 1: Optimize subqueries
        let subq_analysis = analyze_subqueries(stmt);
        if !subq_analysis.subqueries.is_empty() {
            SubqueryOptimizer::optimize(stmt);
            result.subqueries_optimized = subq_analysis.subqueries.len();
            result.was_transformed = true;
        }
        
        // Step 2: Optimize JOIN order if multiple tables
        if !stmt.joins.is_empty() {
            let tables = self.extract_join_tables(stmt);
            let conditions = extract_join_conditions(
                stmt.where_clause.as_ref().unwrap_or(&Expression::Boolean(true)),
                &tables.iter().map(|t| t.name.clone()).collect::<Vec<_>>()
            );
            
            if let Some(order) = self.join_reorderer.optimize_join_order(db, &tables, &conditions) {
                result.optimal_join_order = Some(order);
                result.join_optimized = true;
                result.was_transformed = true;
            }
        }
        
        // Step 3: Select optimal index for single table queries
        let index_selection = self.index_selector.select_index(
            db,
            &stmt.from,
            &stmt.columns,
            stmt.where_clause.as_ref()
        );
        
        if let Some(selection) = index_selection {
            result.index_selection = Some(selection);
            result.index_optimized = true;
        }
        
        result
    }
    
    /// Compare multiple query plans and choose the best
    pub fn choose_best_plan<'a>(
        &self,
        plans: &[&'a QueryPlan],
        db: &BtreeDatabase,
    ) -> Option<(&'a QueryPlan, PlanCost)> {
        let mut best: Option<(&QueryPlan, PlanCost)> = None;
        
        for plan in plans {
            if let Ok(cost) = self.cost_estimator.estimate_cost(plan, db) {
                if best.is_none() || cost.is_better_than(&best.as_ref().unwrap().1) {
                    best = Some((*plan, cost));
                }
            }
        }
        
        best
    }
    
    /// Get statistics catalog
    pub fn stats_catalog(&self) -> &StatsCatalog {
        &self.stats_catalog
    }
    
    /// Get mutable statistics catalog
    pub fn stats_catalog_mut(&mut self) -> &mut StatsCatalog {
        &mut self.stats_catalog
    }
    
    /// Get subquery cache
    pub fn subquery_cache(&self) -> &SubqueryCache {
        &self.subquery_cache
    }
    
    /// Get mutable subquery cache
    pub fn subquery_cache_mut(&mut self) -> &mut SubqueryCache {
        &mut self.subquery_cache
    }
    
    /// Extract table information for JOIN optimization
    fn extract_join_tables(&self, stmt: &SelectStmt) -> Vec<JoinTableInfo> {
        let mut tables = vec![JoinTableInfo {
            name: stmt.from.clone(),
            row_count: self.stats_catalog.get_row_count(&stmt.from),
            alias: None,
        }];
        
        for join in &stmt.joins {
            tables.push(JoinTableInfo {
                name: join.table.clone(),
                row_count: self.stats_catalog.get_row_count(&join.table),
                alias: None,
            });
        }
        
        tables
    }
    
    /// Enable or disable bushy join trees
    pub fn set_bushy_joins(&mut self, allow: bool) {
        self.join_reorderer.set_allow_bushy(allow);
    }
    
    /// Get optimization statistics
    pub fn get_stats(&self) -> OptimizerStats {
        OptimizerStats {
            cached_subqueries: self.subquery_cache.size(),
            table_stats_count: self.stats_catalog.get_row_count(""), // This would need to be implemented properly
        }
    }
}

impl Default for QueryOptimizer {
    fn default() -> Self {
        Self::new()
    }
}

/// Optimizer configuration
#[derive(Debug, Clone)]
pub struct OptimizerConfig {
    /// Custom cost units (None for defaults)
    pub cost_units: Option<CostUnits>,
    /// Maximum number of subquery cache entries
    pub subquery_cache_size: usize,
    /// Whether to use bushy join trees
    pub allow_bushy_joins: bool,
    /// Whether to enable subquery flattening
    pub enable_subquery_flattening: bool,
    /// Whether to enable index intersection
    pub enable_index_intersection: bool,
}

impl Default for OptimizerConfig {
    fn default() -> Self {
        Self {
            cost_units: None,
            subquery_cache_size: 100,
            allow_bushy_joins: true,
            enable_subquery_flattening: true,
            enable_index_intersection: true,
        }
    }
}

/// Result of SELECT statement optimization
#[derive(Debug, Default)]
pub struct OptimizationResult {
    /// Whether any transformation was applied
    pub was_transformed: bool,
    /// Number of subqueries optimized
    pub subqueries_optimized: usize,
    /// Whether JOIN order was optimized
    pub join_optimized: bool,
    /// Optimal JOIN order if determined
    pub optimal_join_order: Option<JoinOrder>,
    /// Whether index selection was performed
    pub index_optimized: bool,
    /// Selected index configuration
    pub index_selection: Option<IndexSelection>,
}

/// Optimizer runtime statistics
#[derive(Debug, Default)]
pub struct OptimizerStats {
    pub cached_subqueries: usize,
    pub table_stats_count: u64,
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
        
        // Create tables
        let columns = vec![
            ColumnDef { name: "id".to_string(), data_type: DataType::Integer, nullable: false, primary_key: true, foreign_key: None, default_value: None, is_virtual: false, generated_always: None },
            ColumnDef { name: "name".to_string(), data_type: DataType::Text, nullable: true, primary_key: false, foreign_key: None, default_value: None, is_virtual: false, generated_always: None },
        ];
        db.create_table("users".to_string(), columns).unwrap();
        
        // Insert data
        for i in 1..=100 {
            db.insert("users", Record::new(vec![
                crate::storage::Value::Integer(i),
                crate::storage::Value::Text(format!("User{}", i)),
            ])).unwrap();
        }
        
        db
    }
    
    #[test]
    fn test_query_optimizer_creation() {
        let optimizer = QueryOptimizer::new();
        assert_eq!(optimizer.subquery_cache.size(), 0);
    }
    
    #[test]
    fn test_query_optimizer_with_config() {
        let config = OptimizerConfig {
            subquery_cache_size: 50,
            allow_bushy_joins: false,
            ..Default::default()
        };
        
        let optimizer = QueryOptimizer::with_config(config);
        // Just verify it was created successfully
        assert_eq!(optimizer.subquery_cache.size(), 0);
    }
    
    #[test]
    fn test_collect_database_stats() {
        let mut optimizer = QueryOptimizer::new();
        let mut db = create_test_db();
        
        optimizer.collect_database_stats(&mut db);
        
        // Should have collected stats for users table
        assert!(optimizer.stats_catalog.get_table_stats("users").is_some());
    }
    
    #[test]
    fn test_optimization_result() {
        let result = OptimizationResult {
            was_transformed: true,
            subqueries_optimized: 2,
            join_optimized: true,
            optimal_join_order: None,
            index_optimized: true,
            index_selection: None,
        };
        
        assert!(result.was_transformed);
        assert_eq!(result.subqueries_optimized, 2);
        assert!(result.join_optimized);
        assert!(result.index_optimized);
    }
}
