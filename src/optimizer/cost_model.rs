//! Cost Model for Query Optimization
//!
//! Estimates the cost of different query execution plans.
//! Lower cost = better plan.
//!
//! Cost factors:
//! - I/O operations (page reads/writes)
//! - CPU operations (comparisons, hash computations)
//! - Memory usage

use super::stats::{ColumnStats, TableStats, StatsCollector, PredicateType};
use crate::executor::{QueryPlan, ExecutorError};
use crate::storage::BtreeDatabase;

/// Cost units for different operations
pub struct CostUnits {
    /// Cost of reading one page from disk
    pub page_read: f64,
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
    /// Cost of hashing one row
    pub row_hash: f64,
}

impl Default for CostUnits {
    fn default() -> Self {
        Self {
            page_read: 100.0,    // I/O is expensive
            page_write: 200.0,   // Writes are more expensive
            cpu_op: 0.1,         // CPU is cheap
            random_seek: 50.0,   // Random access penalty
            row_scan: 1.0,       // Per-row processing
            row_sort: 5.0,       // Sorting is moderately expensive
            row_hash: 2.0,       // Hashing for joins
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
    /// Total cost (weighted sum)
    pub total: f64,
}

impl PlanCost {
    pub fn new(io_cost: f64, cpu_cost: f64, memory: usize) -> Self {
        // Total cost weighted towards I/O (usually the bottleneck)
        let total = io_cost * 10.0 + cpu_cost + (memory as f64 / 1000.0);
        
        Self {
            io_cost,
            cpu_cost,
            memory,
            total,
        }
    }
    
    pub fn zero() -> Self {
        Self::new(0.0, 0.0, 0)
    }
    
    /// Add another cost
    pub fn add(&self, other: &PlanCost) -> PlanCost {
        PlanCost::new(
            self.io_cost + other.io_cost,
            self.cpu_cost + other.cpu_cost,
            self.memory + other.memory,
        )
    }
}

impl std::fmt::Display for PlanCost {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Cost(I/O: {:.0}, CPU: {:.0}, Mem: {} bytes, Total: {:.1})",
            self.io_cost, self.cpu_cost, self.memory, self.total
        )
    }
}

/// Cost estimator for query plans
pub struct CostEstimator {
    units: CostUnits,
}

impl CostEstimator {
    pub fn new() -> Self {
        Self {
            units: CostUnits::default(),
        }
    }
    
    pub fn with_units(units: CostUnits) -> Self {
        Self { units }
    }
    
    /// Estimate cost of a query plan
    pub fn estimate_cost(&self, plan: &QueryPlan, db: &BtreeDatabase) -> Result<PlanCost, ExecutorError> {
        match plan {
            QueryPlan::FullTableScan { table, filter, .. } => {
                self.estimate_table_scan(table, filter.as_ref(), db)
            }
            QueryPlan::RowidPointScan { table, .. } => {
                self.estimate_rowid_point_scan(table, db)
            }
            QueryPlan::RowidRangeScan { table, start_rowid, end_rowid, .. } => {
                self.estimate_rowid_range_scan(table, *start_rowid, *end_rowid, db)
            }
            QueryPlan::CoveringIndexScan { table, index_name, .. } => {
                self.estimate_covering_index_scan(table, index_name, db)
            }
            QueryPlan::CoveringIndexRangeScan { table, index_name, start, end, .. } => {
                self.estimate_covering_index_range_scan(table, index_name, start.as_ref(), end.as_ref(), db)
            }
            QueryPlan::IndexScan { table, index_name, .. } => {
                self.estimate_index_scan(table, index_name, db)
            }
            QueryPlan::IndexRangeScan { table, index_name, start, end, .. } => {
                self.estimate_index_range_scan(table, index_name, start.as_ref(), end.as_ref(), db)
            }
            QueryPlan::HnswVectorScan { .. } => {
                // HNSW has roughly logarithmic cost
                Ok(PlanCost::new(5.0, 50.0, 1024))
            }
        }
    }
    
    /// Estimate full table scan cost
    fn estimate_table_scan(
        &self,
        table_name: &str,
        filter: Option<&crate::sql::ast::Expression>,
        db: &BtreeDatabase,
    ) -> Result<PlanCost, ExecutorError> {
        let table = db.get_table(table_name)
            .ok_or_else(|| ExecutorError::TableNotFound(table_name.to_string()))?;
        
        let row_count = table.next_rowid;
        
        // Estimate pages (assuming average row size of 100 bytes)
        let avg_row_size = 100;
        let page_size = 4096;
        let page_count = (row_count as usize * avg_row_size + page_size - 1) / page_size;
        
        // I/O cost: read all pages
        let io_cost = page_count as f64 * self.units.page_read;
        
        // CPU cost: scan all rows, apply filter
        let cpu_cost = row_count as f64 * self.units.row_scan;
        let filter_cost = if filter.is_some() {
            row_count as f64 * self.units.cpu_op * 10.0 // Filter evaluation
        } else {
            0.0
        };
        
        Ok(PlanCost::new(
            io_cost,
            cpu_cost + filter_cost,
            page_size * page_count,
        ))
    }
    
    /// Estimate rowid point scan (B-tree lookup)
    fn estimate_rowid_point_scan(
        &self,
        table_name: &str,
        _db: &BtreeDatabase,
    ) -> Result<PlanCost, ExecutorError> {
        // B-tree lookup: ~log(n) page reads
        // For simplicity, assume 3-4 levels = 4 pages
        let io_cost = 4.0 * self.units.page_read;
        let cpu_cost = 10.0 * self.units.cpu_op; // B-tree traversal
        
        Ok(PlanCost::new(io_cost, cpu_cost, 4096))
    }
    
    /// Estimate rowid range scan
    fn estimate_rowid_range_scan(
        &self,
        table_name: &str,
        start: Option<i64>,
        end: Option<i64>,
        db: &BtreeDatabase,
    ) -> Result<PlanCost, ExecutorError> {
        let table = db.get_table(table_name)
            .ok_or_else(|| ExecutorError::TableNotFound(table_name.to_string()))?;
        
        let total_rows = table.next_rowid;
        
        // Estimate rows in range
        let range_rows = match (start, end) {
            (Some(s), Some(e)) => (e - s + 1).max(0) as u64,
            (Some(s), None) => total_rows - s as u64,
            (None, Some(e)) => e as u64,
            (None, None) => total_rows,
        };
        
        // I/O: B-tree traversal (4 pages) + data pages
        let pages = (range_rows as f64 / 40.0).ceil(); // ~40 rows per page
        let io_cost = 4.0 * self.units.page_read + pages * self.units.page_read;
        
        // CPU: scan range rows
        let cpu_cost = range_rows as f64 * self.units.row_scan;
        
        Ok(PlanCost::new(io_cost, cpu_cost, (pages as usize) * 4096))
    }
    
    /// Estimate covering index scan (fastest - no table lookup)
    fn estimate_covering_index_scan(
        &self,
        table_name: &str,
        _index_name: &str,
        db: &BtreeDatabase,
    ) -> Result<PlanCost, ExecutorError> {
        let table = db.get_table(table_name)
            .ok_or_else(|| ExecutorError::TableNotFound(table_name.to_string()))?;
        
        let row_count = table.next_rowid;
        
        // B-tree lookup (3-4 pages)
        let io_cost = 4.0 * self.units.page_read;
        
        // CPU: minimal (just return index data)
        let cpu_cost = 5.0 * self.units.cpu_op;
        
        Ok(PlanCost::new(io_cost, cpu_cost, 4096))
    }
    
    /// Estimate covering index range scan
    fn estimate_covering_index_range_scan(
        &self,
        table_name: &str,
        _index_name: &str,
        _start: Option<&crate::storage::Value>,
        _end: Option<&crate::storage::Value>,
        db: &BtreeDatabase,
    ) -> Result<PlanCost, ExecutorError> {
        let table = db.get_table(table_name)
            .ok_or_else(|| ExecutorError::TableNotFound(table_name.to_string()))?;
        
        let row_count = table.next_rowid;
        
        // Estimate ~10% of rows in typical range
        let range_rows = row_count / 10;
        
        // I/O: B-tree traversal + leaf pages
        let pages = 4.0 + (range_rows as f64 / 50.0); // ~50 index entries per page
        let io_cost = pages * self.units.page_read;
        
        // CPU: scan index entries
        let cpu_cost = range_rows as f64 * self.units.row_scan;
        
        Ok(PlanCost::new(io_cost, cpu_cost, (pages as usize) * 4096))
    }
    
    /// Estimate index scan (slower - requires table lookup)
    fn estimate_index_scan(
        &self,
        table_name: &str,
        _index_name: &str,
        db: &BtreeDatabase,
    ) -> Result<PlanCost, ExecutorError> {
        let table = db.get_table(table_name)
            .ok_or_else(|| ExecutorError::TableNotFound(table_name.to_string()))?;
        
        let row_count = table.next_rowid;
        
        // B-tree lookup (3-4 pages) + table lookup (1 page)
        let io_cost = 5.0 * self.units.page_read + self.units.random_seek;
        
        // CPU: index traversal + table lookup
        let cpu_cost = 15.0 * self.units.cpu_op;
        
        Ok(PlanCost::new(io_cost, cpu_cost, 8192))
    }
    
    /// Estimate index range scan
    fn estimate_index_range_scan(
        &self,
        table_name: &str,
        _index_name: &str,
        _start: Option<&crate::storage::Value>,
        _end: Option<&crate::storage::Value>,
        db: &BtreeDatabase,
    ) -> Result<PlanCost, ExecutorError> {
        let table = db.get_table(table_name)
            .ok_or_else(|| ExecutorError::TableNotFound(table_name.to_string()))?;
        
        let row_count = table.next_rowid;
        
        // Estimate ~10% of rows in typical range
        let range_rows = row_count / 10;
        
        // I/O: B-tree traversal + index leaf pages + table lookups
        let index_pages = 4.0 + (range_rows as f64 / 50.0);
        let table_pages = range_rows as f64 / 40.0; // Random table lookups
        let io_cost = index_pages * self.units.page_read 
            + table_pages * (self.units.page_read + self.units.random_seek);
        
        // CPU: index scan + table lookups
        let cpu_cost = range_rows as f64 * (self.units.row_scan + self.units.cpu_op);
        
        Ok(PlanCost::new(io_cost, cpu_cost, ((index_pages + table_pages) as usize) * 4096))
    }
}

impl Default for CostEstimator {
    fn default() -> Self {
        Self::new()
    }
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
            },
            ColumnDef {
                name: "name".to_string(),
                data_type: DataType::Text,
                nullable: true,
                primary_key: false,
                foreign_key: None,
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
}
