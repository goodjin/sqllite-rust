//! Statistics Collection for Query Optimization
//!
//! Collects table and column statistics for cost-based optimization:
//! - Row counts
//! - Column cardinality (unique values)
//! - Data distribution (min/max/avg)
//! - Index selectivity

use crate::storage::{BtreeDatabase, Record, Value};
use std::collections::HashMap;

/// Statistics for a single column
#[derive(Debug, Clone)]
pub struct ColumnStats {
    pub column_name: String,
    /// Number of distinct values
    pub distinct_count: u64,
    /// Number of null values
    pub null_count: u64,
    /// Minimum value (if comparable)
    pub min_value: Option<Value>,
    /// Maximum value (if comparable)
    pub max_value: Option<Value>,
    /// Average value size in bytes
    pub avg_size: f64,
    /// Selectivity factor (1.0 / distinct_count)
    pub selectivity: f64,
}

impl Default for ColumnStats {
    fn default() -> Self {
        Self {
            column_name: String::new(),
            distinct_count: 0,
            null_count: 0,
            min_value: None,
            max_value: None,
            avg_size: 0.0,
            selectivity: 1.0,
        }
    }
}

impl ColumnStats {
    pub fn new(column_name: String) -> Self {
        Self {
            column_name,
            ..Default::default()
        }
    }
    
    /// Calculate selectivity
    pub fn update_selectivity(&mut self, total_rows: u64) {
        if self.distinct_count > 0 {
            self.selectivity = 1.0 / self.distinct_count as f64;
        } else if total_rows > 0 {
            self.selectivity = 1.0 / total_rows as f64;
        } else {
            self.selectivity = 1.0;
        }
    }
    
    /// Estimate number of rows matching an equality predicate
    pub fn estimate_equality_selectivity(&self) -> f64 {
        self.selectivity.max(0.001) // At least 0.1% selectivity
    }
    
    /// Estimate number of rows matching a range predicate
    pub fn estimate_range_selectivity(&self, range_fraction: f64) -> f64 {
        (self.selectivity * range_fraction).max(0.01) // At least 1%
    }
}

/// Statistics for a table
#[derive(Debug, Clone)]
pub struct TableStats {
    pub table_name: String,
    /// Total number of rows
    pub row_count: u64,
    /// Number of data pages
    pub page_count: u64,
    /// Average row size in bytes
    pub avg_row_size: f64,
    /// Statistics for each column
    pub column_stats: HashMap<String, ColumnStats>,
    /// Last time statistics were updated
    pub last_updated: std::time::Instant,
}

impl TableStats {
    pub fn new(table_name: String) -> Self {
        Self {
            table_name,
            row_count: 0,
            page_count: 0,
            avg_row_size: 0.0,
            column_stats: HashMap::new(),
            last_updated: std::time::Instant::now(),
        }
    }
    
    /// Get column statistics
    pub fn get_column(&self, column_name: &str) -> Option<&ColumnStats> {
        self.column_stats.get(column_name)
    }
    
    /// Get estimated table size in bytes
    pub fn estimated_size_bytes(&self) -> u64 {
        self.row_count * self.avg_row_size as u64
    }
    
    /// Check if statistics are stale (older than threshold)
    pub fn is_stale(&self, threshold: std::time::Duration) -> bool {
        self.last_updated.elapsed() > threshold
    }
}

/// Statistics collector
pub struct StatsCollector;

impl StatsCollector {
    /// Collect statistics for a table
    pub fn collect_table_stats(db: &mut BtreeDatabase, table_name: &str) -> Option<TableStats> {
        let table = db.get_table(table_name)?;
        let columns = table.columns.clone();
        
        let mut stats = TableStats::new(table_name.to_string());
        let mut column_values: HashMap<String, Vec<Value>> = HashMap::new();
        
        // Initialize column value collectors
        for col in &columns {
            column_values.insert(col.name.clone(), Vec::new());
        }
        
        // Scan all records
        let records = db.select_all(table_name).ok()?;
        stats.row_count = records.len() as u64;
        
        let mut total_size: usize = 0;
        
        for record in &records {
            total_size += record.serialize().len();
            
            for (i, col) in columns.iter().enumerate() {
                if let Some(value) = record.values.get(i) {
                    column_values.get_mut(&col.name)?.push(value.clone());
                }
            }
        }
        
        // Calculate average row size
        if stats.row_count > 0 {
            stats.avg_row_size = total_size as f64 / stats.row_count as f64;
        }
        
        // Estimate page count (assuming 4KB pages)
        const PAGE_SIZE: u64 = 4096;
        stats.page_count = (stats.estimated_size_bytes() + PAGE_SIZE - 1) / PAGE_SIZE;
        
        // Calculate column statistics
        for (col_name, values) in column_values {
            let col_stats = Self::analyze_column(&col_name, &values, stats.row_count);
            stats.column_stats.insert(col_name, col_stats);
        }
        
        Some(stats)
    }
    
    /// Analyze values in a column
    fn analyze_column(column_name: &str, values: &[Value], total_rows: u64) -> ColumnStats {
        let mut stats = ColumnStats::new(column_name.to_string());
        
        if values.is_empty() {
            return stats;
        }
        
        // Count nulls
        stats.null_count = values.iter()
            .filter(|v| matches!(v, Value::Null))
            .count() as u64;
        
        // Non-null values
        let non_null: Vec<_> = values.iter()
            .filter(|v| !matches!(v, Value::Null))
            .collect();
        
        if non_null.is_empty() {
            return stats;
        }
        
        // Calculate distinct values
        let distinct: std::collections::HashSet<_> = non_null.iter().copied().collect();
        stats.distinct_count = distinct.len() as u64;
        
        // Min/Max values
        stats.min_value = non_null.iter().min().cloned().cloned();
        stats.max_value = non_null.iter().max().cloned().cloned();
        
        // Average size
        let total_size: usize = non_null.iter()
            .map(|v| v.serialize().len())
            .sum();
        stats.avg_size = total_size as f64 / non_null.len() as f64;
        
        // Selectivity
        stats.update_selectivity(total_rows);
        
        stats
    }
    
    /// Estimate selectivity for a predicate
    pub fn estimate_selectivity(
        table_stats: &TableStats,
        column_name: &str,
        predicate_type: PredicateType,
    ) -> f64 {
        let column = table_stats.get_column(column_name);
        
        match predicate_type {
            PredicateType::Equality => {
                column.map(|c| c.estimate_equality_selectivity())
                    .unwrap_or(0.1) // Default 10% selectivity
            }
            PredicateType::Range(fraction) => {
                column.map(|c| c.estimate_range_selectivity(fraction))
                    .unwrap_or(0.3) // Default 30% selectivity
            }
            PredicateType::Like(pattern) => {
                // Estimate based on pattern specificity
                if pattern.ends_with('%') {
                    0.2 // Prefix match ~20%
                } else if pattern.starts_with('%') {
                    0.8 // Suffix match ~80%
                } else {
                    0.5 // Contains ~50%
                }
            }
            PredicateType::IsNull => {
                let null_fraction = column.map(|c| {
                    c.null_count as f64 / table_stats.row_count.max(1) as f64
                }).unwrap_or(0.0);
                null_fraction
            }
            PredicateType::IsNotNull => {
                let null_fraction = column.map(|c| {
                    c.null_count as f64 / table_stats.row_count.max(1) as f64
                }).unwrap_or(0.0);
                1.0 - null_fraction
            }
        }
    }
    
    /// Update statistics after data modification
    pub fn update_stats_after_insert(stats: &mut TableStats, record: &Record) {
        stats.row_count += 1;
        // For simplicity, mark as needing full recalculation
        stats.last_updated = std::time::Instant::now();
    }
    
    /// Update statistics after delete
    pub fn update_stats_after_delete(stats: &mut TableStats) {
        stats.row_count = stats.row_count.saturating_sub(1);
        stats.last_updated = std::time::Instant::now();
    }
}

/// Types of predicates for selectivity estimation
#[derive(Debug, Clone)]
pub enum PredicateType {
    Equality,
    Range(f64), // Fraction of range covered
    Like(String),
    IsNull,
    IsNotNull,
}

/// Statistics catalog for all tables
pub struct StatsCatalog {
    tables: HashMap<String, TableStats>,
}

impl StatsCatalog {
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
        }
    }
    
    pub fn get_table_stats(&self, table_name: &str) -> Option<&TableStats> {
        self.tables.get(table_name)
    }
    
    pub fn update_table_stats(&mut self, stats: TableStats) {
        self.tables.insert(stats.table_name.clone(), stats);
    }
    
    pub fn invalidate_stats(&mut self, table_name: &str) {
        self.tables.remove(table_name);
    }
    
    pub fn clear(&mut self) {
        self.tables.clear();
    }
}

impl Default for StatsCatalog {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::ast::{ColumnDef, DataType};
    use tempfile::NamedTempFile;
    
    fn create_test_db() -> BtreeDatabase {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap();
        let mut db = BtreeDatabase::open(path).unwrap();
        
        // Create test table
        let columns = vec![
            ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                primary_key: true,
                foreign_key: None,
            },
            ColumnDef {
                name: "status".to_string(),
                data_type: DataType::Text,
                nullable: true,
                primary_key: false,
                foreign_key: None,
            },
        ];
        db.create_table("users".to_string(), columns).unwrap();
        
        // Insert test data
        for i in 1..=100 {
            let record = Record::new(vec![
                Value::Integer(i),
                if i % 10 == 0 {
                    Value::Null
                } else {
                    Value::Text(if i % 2 == 0 { "active".to_string() } else { "inactive".to_string() })
                },
            ]);
            db.insert("users", record).unwrap();
        }
        
        db
    }

    #[test]
    fn test_collect_table_stats() {
        let mut db = create_test_db();
        
        let stats = StatsCollector::collect_table_stats(&mut db, "users")
            .expect("Should collect stats");
        
        assert_eq!(stats.row_count, 100);
        assert!(stats.page_count > 0);
        assert!(stats.avg_row_size > 0.0);
        
        // Check column stats
        let id_stats = stats.get_column("id").expect("Should have id stats");
        assert_eq!(id_stats.distinct_count, 100);
        assert_eq!(id_stats.null_count, 0);
        
        let status_stats = stats.get_column("status").expect("Should have status stats");
        assert!(status_stats.distinct_count <= 3); // active, inactive, null
        assert_eq!(status_stats.null_count, 10); // Every 10th is null
    }

    #[test]
    fn test_selectivity_estimation() {
        let mut db = create_test_db();
        let stats = StatsCollector::collect_table_stats(&mut db, "users")
            .expect("Should collect stats");
        
        // ID column should have high selectivity (unique values)
        let id_selectivity = StatsCollector::estimate_selectivity(
            &stats, "id", PredicateType::Equality
        );
        assert!(id_selectivity <= 0.02, "ID should have ~1% selectivity");
        
        // Status column should have lower selectivity
        let status_selectivity = StatsCollector::estimate_selectivity(
            &stats, "status", PredicateType::Equality
        );
        assert!(status_selectivity > 0.3, "Status should have higher selectivity");
    }

    #[test]
    fn test_column_stats_analysis() {
        let values = vec![
            Value::Integer(1),
            Value::Integer(2),
            Value::Integer(3),
            Value::Null,
            Value::Integer(1), // Duplicate
        ];
        
        let stats = StatsCollector::analyze_column("test", &values, 5);
        
        assert_eq!(stats.null_count, 1);
        assert_eq!(stats.distinct_count, 3); // 1, 2, 3
        assert_eq!(stats.min_value, Some(Value::Integer(1)));
        assert_eq!(stats.max_value, Some(Value::Integer(3)));
    }
}
