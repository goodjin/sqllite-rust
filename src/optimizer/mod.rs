//! Query Optimizer Module
//!
//! Provides cost-based query optimization:
//! - Statistics collection
//! - Cost estimation
//! - Plan selection
//! - JOIN reordering
//! - Subquery optimization

pub mod stats;
pub mod cost_model;
pub mod join_reorder;
pub mod subquery;

pub use stats::{StatsCollector, TableStats, ColumnStats, StatsCatalog, PredicateType};
pub use cost_model::{CostEstimator, PlanCost, CostUnits, choose_cheaper_plan};
pub use join_reorder::{JoinReorderer, JoinOrder, JoinCondition, JoinTableInfo, extract_join_conditions};
pub use subquery::{SubqueryOptimizer, SubqueryType, SubqueryInfo};
