//! Cost Model for Query Optimization
//!
//! Estimates the cost of different query execution plans.
//! Lower cost = better plan.
//!
//! Cost factors:
//! - I/O operations (page reads/writes)
//! - CPU operations (comparisons, hash computations)
//! - Memory usage
//! - Network (for distributed queries, not applicable here)

use super::stats::{ColumnStats, TableStats, StatsCollector, PredicateType, StatsCatalog};
use crate::executor::{QueryPlan, ExecutorError};
use crate::storage::BtreeDatabase;

/// Cost units for different operations
/// These can be tuned based on actual hardware performance
#[derive(Debug, Clone)]
pub struct CostUnits {
    /// Cost of reading one page from disk (random I/O)
    pub random_page_read: f64,
    /// Cost of reading one page sequentially
    pub seq_page_read: f64,
    /// Cost of writing one page to disk
    pub page_write: f64,
    /// Cost of one CPU operation (comparison, etc.)
    pub cpu_op: f64,
    /// Cost of a random seek
    pub random_seek: f64,
    /// Cost of scanning one row
    pub row_scan: f64,
    /// Cost of sorting one row
    pub row_sort: f64,
    /// Cost of hashing one row (for hash joins)
    pub row_hash: f64,
    /// Cost of index probe
    pub index_probe: f64,
    /// Cost per byte of memory usage
    pub memory_per_byte: f64,
}

impl Default for CostUnits {
    fn default() -> Self {
        Self {
            random_page_read: 100.0,  // Random I/O is expensive
            seq_page_read: 10.0,      // Sequential I/O is much cheaper
            page_write: 200.0,        // Writes are more expensive
            cpu_op: 0.1,              // CPU is cheap
            random_seek: 50.0,        // Random access penalty
            row_scan: 1.0,            // Per-row processing
            row_sort: 5.0,            // Sorting is moderately expensive
            row_hash: 2.0,            // Hashing for joins
            index_probe: 3.0,         // Index lookup cost
            memory_per_byte: 0.0001,  // Memory is cheap but not free
        }
    }
}

/// Estimated cost of a query plan
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PlanCost {
    /// Estimated I/O cost (page reads/writes)
    pub io_cost: f64,
    /// Estimated CPU cost (operations)
    pub cpu_cost: f64,
    /// Estimated memory usage (bytes)
    pub memory: usize,
    /// Startup cost (cost before first row is returned)
    pub startup_cost: f64,
    /// Total cost (weighted sum)
    pub total: f64,
}

impl PlanCost {
    pub fn new(io_cost: f64, cpu_cost: f64, memory: usize) -> Self {
        Self::with_startup(io_cost, cpu_cost, memory, io_cost)
    }
    
    pub fn with_startup(io_cost: f64, cpu_cost: f64, memory: usize, startup_cost: f64) -> Self {
        // Total cost weighted towards I/O (usually the bottleneck)
        let memory_cost = memory as f64 * 0.0001;
        let total = io_cost * 10.0 + cpu_cost + memory_cost;
        
        Self {
            io_cost,
            cpu_cost,
            memory,
            startup_cost,
            total,
        }
    }
    
    pub fn zero() -> Self {
        Self {
            io_cost: 0.0,
            cpu_cost: 0.0,
            memory: 0,
            startup_cost: 0.0,
            total: 0.0,
        }
    }
    
    /// Add another cost
    pub fn add(&self, other: &PlanCost) -> PlanCost {
        PlanCost::with_startup(
            self.io_cost + other.io_cost,
            self.cpu_cost + other.cpu_cost,
            self.memory + other.memory,
            self.startup_cost + other.startup_cost,
        )
    }
    
    /// Scale cost by a factor
    pub fn scale(&self, factor: f64) -> PlanCost {
        PlanCost::with_startup(
            self.io_cost * factor,
            self.cpu_cost * factor,
            (self.memory as f64 * factor) as usize,
            self.startup_cost * factor,
        )
    }
    
    /// Check if this cost is better (lower) than another
    pub fn is_better_than(&self, other: &PlanCost) -> bool {
        self.total < other.total
    }
}

impl std::fmt::Display for PlanCost {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Cost(I/O: {:.0}, CPU: {:.0}, Mem: {} bytes, Startup: {:.1}, Total: {:.1})",
            self.io_cost, self.cpu_cost, self.memory, self.startup_cost, self.total
        )
    }
}

/// Cost estimator for query plans
pub struct CostEstimator {
    units: CostUnits,
    stats_catalog: Option<StatsCatalog>,
}

impl CostEstimator {
    pub fn new() -> Self {
        Self {
            units: CostUnits::default(),
            stats_catalog: None,
        }
    }
    
    pub fn with_units(units: CostUnits) -> Self {
        Self {
            units,
            stats_catalog: None,
        }
    }
    
    pub fn with_stats(mut self, catalog: StatsCatalog) -> Self {
        self.stats_catalog = Some(catalog);
        self
    }
    
    /// Set stats catalog
    pub fn set_stats_catalog(&mut self, catalog: StatsCatalog) {
        self.stats_catalog = Some(catalog);
    }
    
    /// Estimate cost of a query plan
    pub fn estimate_cost(&self, plan: &QueryPlan, db: &BtreeDatabase) -> Result<PlanCost, ExecutorError> {
        match plan {
            QueryPlan::FullTableScan { table, filter, limit, .. } => {
                self.estimate_table_scan(table, filter.as_ref(), *limit, db)
            }
            QueryPlan::RowidPointScan { table, .. } => {
                self.estimate_rowid_point_scan(table, db)
            }
            QueryPlan::RowidRangeScan { table, start_rowid, end_rowid, limit, .. } => {
                self.estimate_rowid_range_scan(table, *start_rowid, *end_rowid, *limit, db)
            }
            QueryPlan::CoveringIndexScan { table, index_name, columns, .. } => {
                self.estimate_covering_index_scan(table, index_name, columns, db)
            }
            QueryPlan::CoveringIndexRangeScan { table, index_name, start, end, limit, .. } => {
                self.estimate_covering_index_range_scan(table, index_name, start.as_ref(), end.as_ref(), *limit, db)
            }
            QueryPlan::IndexScan { table, index_name, columns, .. } => {
                self.estimate_index_scan(table, index_name, columns, db)
            }
            QueryPlan::IndexRangeScan { table, index_name, start, end, limit, .. } => {
                self.estimate_index_range_scan(table, index_name, start.as_ref(), end.as_ref(), *limit, db)
            }
            QueryPlan::HnswVectorScan { limit, .. } => {
                self.estimate_hnsw_scan(*limit)
            }
        }
    }
    
    /// Get table statistics if available
    fn get_table_stats(&self, table_name: &str) -> Option<&TableStats> {
        self.stats_catalog.as_ref()
            .and_then(|cat| cat.get_table_stats(table_name))
    }
    
    /// Get estimated row count for a table
    fn get_row_estimate(&self, table_name: &str, db: &BtreeDatabase) -> u64 {
        if let Some(stats) = self.get_table_stats(table_name) {
            stats.row_count
        } else {
            db.get_table(table_name)
                .map(|t| t.next_rowid as u64)
                .unwrap_or(1000)
        }
    }
    
    /// Estimate full table scan cost
    fn estimate_table_scan(
        &self,
        table_name: &str,
        filter: Option<&crate::sql::ast::Expression>,
        limit: Option<i64>,
        db: &BtreeDatabase,
    ) -> Result<PlanCost, ExecutorError> {
        let row_count = self.get_row_estimate(table_name, db);
        
        // Use actual stats if available
        let (avg_row_size, page_count) = if let Some(stats) = self.get_table_stats(table_name) {
            let pages = stats.page_count.max(1);
            (stats.avg_row_size, pages)
        } else {
            // Estimate pages (assuming average row size of 100 bytes)
            let avg_row_size = 100.0;
            let page_size = 4096;
            let pages = ((row_count as f64 * avg_row_size) / page_size as f64).ceil() as u64;
            (avg_row_size, pages.max(1))
        };
        
        // Adjust rows if limit is present
        let effective_rows = if let Some(limit) = limit {
            (limit as u64).min(row_count)
        } else {
            row_count
        };
        
        // I/O cost: read all pages sequentially
        let io_cost = page_count as f64 * self.units.seq_page_read;
        
        // CPU cost: scan all rows, apply filter
        let mut cpu_cost = effective_rows as f64 * self.units.row_scan;
        if filter.is_some() {
            // Filter evaluation cost
            cpu_cost += effective_rows as f64 * self.units.cpu_op * 5.0;
        }
        
        // Memory: buffer for the scan
        let memory = (page_count.min(10) as usize) * 4096;
        
        Ok(PlanCost::new(io_cost, cpu_cost, memory))
    }
    
    /// Estimate rowid point scan (B-tree lookup)
    fn estimate_rowid_point_scan(
        &self,
        _table_name: &str,
        _db: &BtreeDatabase,
    ) -> Result<PlanCost, ExecutorError> {
        // B-tree lookup: ~log(n) page reads
        // For simplicity, assume 3-4 levels = 4 pages
        let io_cost = 4.0 * self.units.random_page_read;
        let cpu_cost = 10.0 * self.units.cpu_op; // B-tree traversal
        let startup_cost = io_cost; // Need to do I/O before getting any rows
        
        Ok(PlanCost::with_startup(io_cost, cpu_cost, 4096, startup_cost))
    }
    
    /// Estimate rowid range scan
    fn estimate_rowid_range_scan(
        &self,
        table_name: &str,
        start: Option<i64>,
        end: Option<i64>,
        limit: Option<i64>,
        db: &BtreeDatabase,
    ) -> Result<PlanCost, ExecutorError> {
        let total_rows = self.get_row_estimate(table_name, db);
        
        // Estimate rows in range
        let range_rows = match (start, end) {
            (Some(s), Some(e)) => ((e - s + 1).max(0) as u64).min(total_rows),
            (Some(s), None) => total_rows.saturating_sub(s as u64),
            (None, Some(e)) => e as u64,
            (None, None) => total_rows,
        };
        
        // Apply limit
        let effective_rows = if let Some(limit) = limit {
            (limit as u64).min(range_rows)
        } else {
            range_rows
        };
        
        // I/O: B-tree traversal (4 pages) + data pages
        let pages = (effective_rows as f64 / 40.0).ceil().max(1.0);
        let io_cost = 4.0 * self.units.random_page_read + pages * self.units.seq_page_read;
        
        // CPU: scan range rows
        let cpu_cost = effective_rows as f64 * self.units.row_scan;
        let startup_cost = 4.0 * self.units.random_page_read; // Initial B-tree traversal
        
        Ok(PlanCost::with_startup(io_cost, cpu_cost, (pages as usize) * 4096, startup_cost))
    }
    
    /// Estimate covering index scan (fastest - no table lookup)
    fn estimate_covering_index_scan(
        &self,
        table_name: &str,
        _index_name: &str,
        columns: &[crate::sql::ast::SelectColumn],
        db: &BtreeDatabase,
    ) -> Result<PlanCost, ExecutorError> {
        let row_count = self.get_row_estimate(table_name, db);
        
        // B-tree lookup (3-4 pages)
        let io_cost = 4.0 * self.units.random_page_read;
        
        // CPU: minimal (just return index data)
        let cpu_cost = 5.0 * self.units.cpu_op;
        
        // Memory: minimal for covering index
        let memory = columns.len() * 100; // Estimated per-column memory
        
        let startup_cost = io_cost;
        Ok(PlanCost::with_startup(io_cost, cpu_cost, memory, startup_cost))
    }
    
    /// Estimate covering index range scan
    fn estimate_covering_index_range_scan(
        &self,
        table_name: &str,
        _index_name: &str,
        _start: Option<&crate::storage::Value>,
        _end: Option<&crate::storage::Value>,
        limit: Option<i64>,
        db: &BtreeDatabase,
    ) -> Result<PlanCost, ExecutorError> {
        let row_count = self.get_row_estimate(table_name, db);
        
        // Estimate rows in range (use stats if available)
        let range_rows = if let Some(stats) = self.get_table_stats(table_name) {
            // Use 10% estimate as default for range scans
            (row_count / 10).max(1)
        } else {
            (row_count / 10).max(1)
        };
        
        // Apply limit
        let effective_rows = if let Some(limit) = limit {
            (limit as u64).min(range_rows)
        } else {
            range_rows
        };
        
        // I/O: B-tree traversal + leaf pages
        let pages = 4.0 + (effective_rows as f64 / 50.0); // ~50 index entries per page
        let io_cost = pages * self.units.random_page_read;
        
        // CPU: scan index entries
        let cpu_cost = effective_rows as f64 * self.units.row_scan;
        let startup_cost = 4.0 * self.units.random_page_read; // Initial B-tree traversal
        
        Ok(PlanCost::with_startup(io_cost, cpu_cost, (pages as usize) * 4096, startup_cost))
    }
    
    /// Estimate index scan (slower - requires table lookup)
    fn estimate_index_scan(
        &self,
        table_name: &str,
        _index_name: &str,
        columns: &[crate::sql::ast::SelectColumn],
        db: &BtreeDatabase,
    ) -> Result<PlanCost, ExecutorError> {
        let row_count = self.get_row_estimate(table_name, db);
        
        // B-tree lookup (3-4 pages) + table lookup (1 page)
        let io_cost = 5.0 * self.units.random_page_read + self.units.random_seek;
        
        // CPU: index traversal + table lookup
        let cpu_cost = 15.0 * self.units.cpu_op;
        
        // Memory: for index and table page
        let memory = 8192;
        
        let startup_cost = io_cost;
        Ok(PlanCost::with_startup(io_cost, cpu_cost, memory, startup_cost))
    }
    
    /// Estimate index range scan
    fn estimate_index_range_scan(
        &self,
        table_name: &str,
        _index_name: &str,
        _start: Option<&crate::storage::Value>,
        _end: Option<&crate::storage::Value>,
        limit: Option<i64>,
        db: &BtreeDatabase,
    ) -> Result<PlanCost, ExecutorError> {
        let row_count = self.get_row_estimate(table_name, db);
        
        // Estimate ~10% of rows in typical range
        let range_rows = (row_count / 10).max(1);
        
        // Apply limit
        let effective_rows = if let Some(limit) = limit {
            (limit as u64).min(range_rows)
        } else {
            range_rows
        };
        
        // I/O: B-tree traversal + index leaf pages + table lookups
        let index_pages = 4.0 + (effective_rows as f64 / 50.0);
        let table_pages = effective_rows as f64 / 40.0; // Random table lookups
        let io_cost = index_pages * self.units.random_page_read 
            + table_pages * (self.units.random_page_read + self.units.random_seek);
        
        // CPU: index scan + table lookups
        let cpu_cost = effective_rows as f64 * (self.units.row_scan + self.units.cpu_op);
        let startup_cost = 4.0 * self.units.random_page_read;
        
        Ok(PlanCost::with_startup(io_cost, cpu_cost, ((index_pages + table_pages) as usize) * 4096, startup_cost))
    }
    
    /// Estimate HNSW vector scan cost
    fn estimate_hnsw_scan(&self, limit: usize) -> Result<PlanCost, ExecutorError> {
        // HNSW has logarithmic search complexity with small constant overhead
        // Cost scales with the limit (number of results requested)
        let io_cost = 5.0 + (limit as f64 * 0.5); // ~5 pages base + 0.5 per result
        let cpu_cost = 50.0 + (limit as f64 * 10.0); // Distance computations
        let startup_cost = io_cost; // Need to traverse graph before returning results
        
        Ok(PlanCost::with_startup(io_cost, cpu_cost, 1024, startup_cost))
    }
    
    /// Estimate cost of a join between two tables
    pub fn estimate_join_cost(
        &self,
        left_cost: &PlanCost,
        right_cost: &PlanCost,
        left_rows: u64,
        right_rows: u64,
        join_type: JoinAlgorithm,
    ) -> PlanCost {
        match join_type {
            JoinAlgorithm::NestedLoop => {
                // Nested loop: O(left_rows * right_rows)
                let comparisons = left_rows * right_rows;
                let cpu_cost = comparisons as f64 * self.units.cpu_op;
                let io_cost = left_cost.io_cost + (left_rows as f64 * right_cost.io_cost);
                let memory = left_cost.memory + right_cost.memory;
                
                PlanCost::with_startup(
                    io_cost,
                    left_cost.cpu_cost + right_cost.cpu_cost + cpu_cost,
                    memory,
                    left_cost.startup_cost,
                )
            }
            JoinAlgorithm::Hash => {
                // Hash join: O(left_rows + right_rows)
                let hash_cpu = (left_rows + right_rows) as f64 * self.units.row_hash;
                let io_cost = left_cost.io_cost + right_cost.io_cost;
                let memory = left_cost.memory + (right_rows as usize * 100); // Hash table
                
                PlanCost::with_startup(
                    io_cost,
                    left_cost.cpu_cost + right_cost.cpu_cost + hash_cpu,
                    memory,
                    left_cost.startup_cost + right_cost.startup_cost,
                )
            }
            JoinAlgorithm::Merge => {
                // Merge join: O(left_rows + right_rows) after sorting
                let sort_cpu = (left_rows + right_rows) as f64 * self.units.row_sort;
                let io_cost = left_cost.io_cost + right_cost.io_cost;
                let memory = left_cost.memory + right_cost.memory + 8192; // Merge buffer
                
                PlanCost::with_startup(
                    io_cost,
                    left_cost.cpu_cost + right_cost.cpu_cost + sort_cpu,
                    memory,
                    left_cost.startup_cost + right_cost.startup_cost,
                )
            }
            JoinAlgorithm::Index => {
                // Index nested loop: O(left_rows * log(right_rows))
                let probe_cost = left_rows as f64 * self.units.index_probe;
                let io_cost = left_cost.io_cost + probe_cost;
                let memory = left_cost.memory + 4096;
                
                PlanCost::with_startup(
                    io_cost,
                    left_cost.cpu_cost + probe_cost * self.units.cpu_op,
                    memory,
                    left_cost.startup_cost,
                )
            }
        }
    }
    
    /// Estimate sort cost
    pub fn estimate_sort_cost(&self, input_rows: u64, input_cost: &PlanCost) -> PlanCost {
        if input_rows <= 1 {
            return *input_cost;
        }
        
        // External sort cost: O(n log n) comparisons
        let log_n = (input_rows as f64).log2().max(1.0);
        let comparisons = input_rows as f64 * log_n;
        let cpu_cost = comparisons * self.units.cpu_op + input_rows as f64 * self.units.row_sort;
        
        // I/O cost for external sort (if data doesn't fit in memory)
        let io_cost = if input_rows > 10000 {
            input_cost.io_cost * 3.0 // Need to write and read runs
        } else {
            input_cost.io_cost
        };
        
        let memory = input_cost.memory.max(65536); // Sort buffer
        
        PlanCost::with_startup(
            io_cost,
            input_cost.cpu_cost + cpu_cost,
            memory,
            input_cost.startup_cost,
        )
    }
    
    /// Estimate aggregation cost
    pub fn estimate_agg_cost(&self, input_rows: u64, num_groups: u64, input_cost: &PlanCost) -> PlanCost {
        // Aggregation cost: O(input_rows) for hash aggregation
        let cpu_cost = input_rows as f64 * self.units.row_hash;
        let memory = if num_groups > 0 {
            (num_groups as usize * 100).min(10_000_000) // Cap memory estimate
        } else {
            4096
        };
        
        PlanCost::with_startup(
            input_cost.io_cost,
            input_cost.cpu_cost + cpu_cost,
            memory,
            input_cost.startup_cost,
        )
    }
}

impl Default for CostEstimator {
    fn default() -> Self {
        Self::new()
    }
}

/// Join algorithms for cost estimation
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum JoinAlgorithm {
    /// Nested loop join
    NestedLoop,
    /// Hash join
    Hash,
    /// Sort-merge join
    Merge,
    /// Index nested loop join
    Index,
}

/// Compare two plans and return the cheaper one
pub fn choose_cheaper_plan<'a>(
    plan1: &'a QueryPlan,
    cost1: PlanCost,
    plan2: &'a QueryPlan,
    cost2: PlanCost,
) -> (&'a QueryPlan, PlanCost) {
    if cost1.total <= cost2.total {
        (plan1, cost1)
    } else {
        (plan2, cost2)
    }
}

/// Choose the best plan from multiple candidates
pub fn choose_best_plan<'a>(
    plans: Vec<(&'a QueryPlan, PlanCost)>,
) -> Option<(&'a QueryPlan, PlanCost)> {
    plans.into_iter()
        .min_by(|a, b| a.1.total.partial_cmp(&b.1.total).unwrap_or(std::cmp::Ordering::Equal))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::ast::{SelectColumn, ColumnDef, DataType};
    use tempfile::NamedTempFile;
    
    fn create_test_db() -> BtreeDatabase {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap();
        let mut db = BtreeDatabase::open(path).unwrap();
        
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
        
        // Insert 1000 rows
        for i in 1..=1000 {
            let record = crate::storage::Record::new(vec![
                crate::storage::Value::Integer(i),
                crate::storage::Value::Text(format!("User{}", i)),
            ]);
            db.insert("users", record).unwrap();
        }
        
        db
    }
    
    #[test]
    fn test_cost_comparison() {
        let db = create_test_db();
        let estimator = CostEstimator::new();
        
        // Full table scan
        let full_scan = QueryPlan::FullTableScan {
            table: "users".to_string(),
            filter: None,
            columns: vec![SelectColumn::All],
            limit: None,
        };
        let full_scan_cost = estimator.estimate_cost(&full_scan, &db).unwrap();
        
        // Rowid point scan
        let point_scan = QueryPlan::RowidPointScan {
            table: "users".to_string(),
            rowid: 500,
            columns: vec![SelectColumn::All],
        };
        let point_scan_cost = estimator.estimate_cost(&point_scan, &db).unwrap();
        
        // Point scan should be much cheaper
        assert!(point_scan_cost.total < full_scan_cost.total / 10.0,
            "Point scan ({}) should be much cheaper than full scan ({})",
            point_scan_cost.total, full_scan_cost.total);
        
        // Point scan should have lower startup cost
        assert!(point_scan_cost.startup_cost < full_scan_cost.startup_cost,
            "Point scan should have lower startup cost");
    }
    
    #[test]
    fn test_covering_index_vs_index_scan() {
        let db = create_test_db();
        let estimator = CostEstimator::new();
        
        // Regular index scan (needs table lookup)
        let index_scan = QueryPlan::IndexScan {
            table: "users".to_string(),
            index_name: "idx_name".to_string(),
            column: "name".to_string(),
            value: crate::storage::Value::Text("Alice".to_string()),
            columns: vec![SelectColumn::All],
            limit: None,
        };
        let index_cost = estimator.estimate_cost(&index_scan, &db).unwrap();
        
        // Covering index scan
        let covering_scan = QueryPlan::CoveringIndexScan {
            table: "users".to_string(),
            index_name: "idx_name".to_string(),
            column: "name".to_string(),
            value: crate::storage::Value::Text("Alice".to_string()),
            columns: vec![SelectColumn::Column("name".to_string())],
            limit: None,
        };
        let covering_cost = estimator.estimate_cost(&covering_scan, &db).unwrap();
        
        // Covering index should be cheaper
        assert!(covering_cost.total < index_cost.total,
            "Covering index ({}) should be cheaper than index scan ({})",
            covering_cost.total, index_cost.total);
    }
    
    #[test]
    fn test_join_cost_estimation() {
        let estimator = CostEstimator::new();
        
        let left_cost = PlanCost::new(100.0, 50.0, 4096);
        let right_cost = PlanCost::new(50.0, 25.0, 2048);
        
        // Test nested loop cost
        let nl_cost = estimator.estimate_join_cost(
            &left_cost, &right_cost, 100, 50, JoinAlgorithm::NestedLoop
        );
        assert!(nl_cost.total > left_cost.total + right_cost.total);
        
        // Test hash join cost
        let hash_cost = estimator.estimate_join_cost(
            &left_cost, &right_cost, 100, 50, JoinAlgorithm::Hash
        );
        // Hash join should be cheaper than nested loop for these sizes
        assert!(hash_cost.total < nl_cost.total);
        
        // Test index join cost
        let index_cost = estimator.estimate_join_cost(
            &left_cost, &right_cost, 100, 50, JoinAlgorithm::Index
        );
        // Index join should be cheapest
        assert!(index_cost.total < hash_cost.total);
    }
    
    #[test]
    fn test_sort_cost_estimation() {
        let estimator = CostEstimator::new();
        
        let input_cost = PlanCost::new(100.0, 50.0, 4096);
        
        // Small sort (fits in memory)
        let small_sort = estimator.estimate_sort_cost(100, &input_cost);
        assert!(small_sort.cpu_cost > input_cost.cpu_cost);
        
        // Large sort (external sort)
        let large_sort = estimator.estimate_sort_cost(100000, &input_cost);
        assert!(large_sort.io_cost > input_cost.io_cost);
    }
    
    #[test]
    fn test_choose_best_plan() {
        let plan1 = QueryPlan::FullTableScan {
            table: "t1".to_string(),
            filter: None,
            columns: vec![SelectColumn::All],
            limit: None,
        };
        let plan2 = QueryPlan::FullTableScan {
            table: "t2".to_string(),
            filter: None,
            columns: vec![SelectColumn::All],
            limit: None,
        };
        let plan3 = QueryPlan::FullTableScan {
            table: "t3".to_string(),
            filter: None,
            columns: vec![SelectColumn::All],
            limit: None,
        };
        
        let plans = vec![
            (&plan1, PlanCost::new(100.0, 50.0, 4096)),
            (&plan2, PlanCost::new(50.0, 25.0, 2048)), // Best
            (&plan3, PlanCost::new(150.0, 75.0, 6144)),
        ];
        
        let best = choose_best_plan(plans);
        assert!(best.is_some());
        assert_eq!(best.unwrap().0, &plan2);
    }
}
