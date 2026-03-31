//! Statistics Collection for Query Optimization
//!
//! Collects table and column statistics for cost-based optimization:
//! - Row counts
//! - Column cardinality (unique values)
//! - Data distribution (min/max/avg)
//! - Index selectivity
//! - Histogram for query selectivity estimation

use crate::storage::{BtreeDatabase, Record, Value};
use std::collections::HashMap;
use std::time::{Duration, Instant};

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
    /// Histogram for distribution estimation
    pub histogram: Option<Histogram>,
    /// Most common values (for skewed distributions)
    pub mcv: Vec<(Value, u64)>,
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
            histogram: None,
            mcv: Vec::new(),
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
        // If we have MCV (most common values), check if it's in the list
        if !self.mcv.is_empty() {
            // Average frequency of MCV
            let total_freq: u64 = self.mcv.iter().map(|(_, freq)| freq).sum();
            let avg_freq = total_freq as f64 / self.mcv.len() as f64;
            let total_rows = self.null_count + total_freq;
            if total_rows > 0 {
                return (avg_freq / total_rows as f64).max(0.001);
            }
        }
        self.selectivity.max(0.001) // At least 0.1% selectivity
    }
    
    /// Estimate number of rows matching a range predicate
    pub fn estimate_range_selectivity(&self, range_fraction: f64) -> f64 {
        if let Some(ref histogram) = self.histogram {
            // Use histogram for more accurate estimation
            histogram.estimate_range_selectivity(range_fraction)
        } else {
            (self.selectivity * range_fraction).max(0.01) // At least 1%
        }
    }
    
    /// Get null ratio (fraction of null values)
    pub fn null_ratio(&self, total_rows: u64) -> f64 {
        if total_rows > 0 {
            self.null_count as f64 / total_rows as f64
        } else {
            0.0
        }
    }
}

/// Histogram for column value distribution
#[derive(Debug, Clone)]
pub struct Histogram {
    /// Bucket boundaries (sorted)
    pub buckets: Vec<Value>,
    /// Number of values in each bucket
    pub counts: Vec<u64>,
    /// Total number of values
    pub total_count: u64,
}

impl Histogram {
    /// Create a histogram from sorted values
    pub fn from_values(values: &[Value], num_buckets: usize) -> Option<Self> {
        if values.is_empty() || num_buckets == 0 {
            return None;
        }
        
        // Filter out null values for histogram
        let non_null_values: Vec<_> = values.iter()
            .filter(|v| !matches!(v, Value::Null))
            .collect();
        
        if non_null_values.is_empty() {
            return None;
        }
        
        let total = non_null_values.len();
        let bucket_size = (total + num_buckets - 1) / num_buckets;
        
        let mut buckets = Vec::with_capacity(num_buckets);
        let mut counts = Vec::with_capacity(num_buckets);
        
        for i in 0..num_buckets {
            let start = i * bucket_size;
            let end = ((i + 1) * bucket_size).min(total);
            
            if start < total {
                // Use the last value in this bucket as the boundary
                if let Some(boundary) = non_null_values.get(end.saturating_sub(1)) {
                    buckets.push((*boundary).clone());
                    counts.push((end - start) as u64);
                }
            }
        }
        
        Some(Histogram {
            buckets,
            counts,
            total_count: total as u64,
        })
    }
    
    /// Estimate selectivity for a range query
    pub fn estimate_range_selectivity(&self, range_fraction: f64) -> f64 {
        // Simplified estimation based on uniform distribution assumption within buckets
        let selectivity = range_fraction * (self.buckets.len() as f64 / self.total_count as f64);
        selectivity.clamp(0.001, 0.99)
    }
    
    /// Estimate selectivity for a specific value
    pub fn estimate_value_selectivity(&self, value: &Value) -> f64 {
        // Find which bucket this value would fall into
        for (i, bucket) in self.buckets.iter().enumerate() {
            if value <= bucket {
                let bucket_count = self.counts.get(i).copied().unwrap_or(1);
                return (bucket_count as f64 / self.total_count as f64).max(0.001);
            }
        }
        // Value is larger than all buckets
        0.001
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
    pub last_updated: Instant,
    /// Sample size used for statistics (0 if full scan)
    pub sample_size: u64,
    /// Table modification count since last stats update
    pub modification_count: u64,
}

impl TableStats {
    pub fn new(table_name: String) -> Self {
        Self {
            table_name,
            row_count: 0,
            page_count: 0,
            avg_row_size: 0.0,
            column_stats: HashMap::new(),
            last_updated: Instant::now(),
            sample_size: 0,
            modification_count: 0,
        }
    }
    
    /// Get column statistics
    pub fn get_column(&self, column_name: &str) -> Option<&ColumnStats> {
        self.column_stats.get(column_name)
    }
    
    /// Get mutable column statistics
    pub fn get_column_mut(&mut self, column_name: &str) -> Option<&mut ColumnStats> {
        self.column_stats.get_mut(column_name)
    }
    
    /// Get estimated table size in bytes
    pub fn estimated_size_bytes(&self) -> u64 {
        self.row_count * self.avg_row_size as u64
    }
    
    /// Check if statistics are stale (older than threshold)
    pub fn is_stale(&self, threshold: Duration) -> bool {
        self.last_updated.elapsed() > threshold
    }
    
    /// Check if statistics need update based on modification count
    pub fn needs_update(&self, threshold: u64) -> bool {
        self.modification_count >= threshold
    }
    
    /// Record a modification (insert/update/delete)
    pub fn record_modification(&mut self) {
        self.modification_count += 1;
    }
    
    /// Get column correlation coefficient (-1 to 1)
    /// Higher absolute value means better correlation with physical order
    pub fn get_column_correlation(&self, column_name: &str) -> f64 {
        // Simplified: assume primary key has good correlation
        if column_name == "rowid" || column_name == "id" {
            1.0
        } else {
            0.0 // Unknown correlation
        }
    }
}

/// Statistics collection configuration
pub struct StatsConfig {
    /// Sample ratio for large tables (1.0 = full scan)
    pub sample_ratio: f64,
    /// Minimum sample size
    pub min_sample_size: usize,
    /// Maximum sample size
    pub max_sample_size: usize,
    /// Number of histogram buckets
    pub histogram_buckets: usize,
    /// Number of most common values to track
    pub mcv_count: usize,
    /// Staleness threshold for auto-update
    pub staleness_threshold: Duration,
    /// Modification count threshold for auto-update
    pub modification_threshold: u64,
}

impl Default for StatsConfig {
    fn default() -> Self {
        Self {
            sample_ratio: 0.1,  // 10% sample for large tables
            min_sample_size: 1000,
            max_sample_size: 100_000,
            histogram_buckets: 100,
            mcv_count: 10,
            staleness_threshold: Duration::from_secs(3600), // 1 hour
            modification_threshold: 1000,
        }
    }
}

/// Statistics collector
pub struct StatsCollector {
    config: StatsConfig,
}

impl StatsCollector {
    pub fn new() -> Self {
        Self {
            config: StatsConfig::default(),
        }
    }
    
    pub fn with_config(config: StatsConfig) -> Self {
        Self { config }
    }
    
    /// Calculate sample size for a table
    fn calculate_sample_size(&self, total_rows: u64) -> usize {
        let estimated = (total_rows as f64 * self.config.sample_ratio) as usize;
        estimated.clamp(self.config.min_sample_size, self.config.max_sample_size)
    }
    
    /// Collect statistics for a table with sampling
    pub fn collect_table_stats(&self, db: &mut BtreeDatabase, table_name: &str) -> Option<TableStats> {
        let table = db.get_table(table_name)?;
        let columns = table.columns.clone();
        
        // First pass: get total row count and page count
        let all_records = db.select_all(table_name).ok()?;
        let total_rows = all_records.len() as u64;
        
        // Determine sample size
        let sample_size = if total_rows <= self.config.min_sample_size as u64 {
            total_rows as usize // Full scan for small tables
        } else {
            self.calculate_sample_size(total_rows)
        };
        
        // Sample records
        let sampled_records = if sample_size >= total_rows as usize {
            all_records
        } else {
            // Systematic sampling for better coverage
            let step = total_rows as usize / sample_size;
            all_records.into_iter()
                .step_by(step.max(1))
                .take(sample_size)
                .collect()
        };
        
        let mut stats = TableStats::new(table_name.to_string());
        stats.row_count = total_rows;
        stats.sample_size = sample_size as u64;
        
        // Initialize column value collectors
        let mut column_values: HashMap<String, Vec<Value>> = HashMap::new();
        for col in &columns {
            column_values.insert(col.name.clone(), Vec::with_capacity(sample_size));
        }
        
        // Calculate total size from sampled records
        let mut total_size: usize = 0;
        for record in &sampled_records {
            total_size += record.serialize().len();
            
            for (i, col) in columns.iter().enumerate() {
                if let Some(value) = record.values.get(i) {
                    column_values.get_mut(&col.name)?.push(value.clone());
                }
            }
        }
        
        // Calculate average row size (scale up from sample)
        if !sampled_records.is_empty() {
            stats.avg_row_size = total_size as f64 / sampled_records.len() as f64;
        }
        
        // Estimate page count (assuming 4KB pages)
        const PAGE_SIZE: u64 = 4096;
        stats.page_count = (stats.estimated_size_bytes() + PAGE_SIZE - 1) / PAGE_SIZE;
        
        // Calculate column statistics
        for (col_name, values) in column_values {
            let col_stats = self.analyze_column(&col_name, &values, total_rows);
            stats.column_stats.insert(col_name, col_stats);
        }
        
        Some(stats)
    }
    
    /// Analyze values in a column with advanced statistics
    fn analyze_column(&self, column_name: &str, values: &[Value], total_rows: u64) -> ColumnStats {
        let mut stats = ColumnStats::new(column_name.to_string());
        
        if values.is_empty() {
            return stats;
        }
        
        // Count nulls
        stats.null_count = values.iter()
            .filter(|v| matches!(v, Value::Null))
            .count() as u64;
        
        // Scale up null count if sampling was used
        let sample_ratio = values.len() as f64 / total_rows.max(1) as f64;
        if sample_ratio < 1.0 && total_rows > 0 {
            stats.null_count = (stats.null_count as f64 / sample_ratio) as u64;
            stats.null_count = stats.null_count.min(total_rows);
        }
        
        // Non-null values
        let non_null: Vec<_> = values.iter()
            .filter(|v| !matches!(v, Value::Null))
            .collect();
        
        if non_null.is_empty() {
            return stats;
        }
        
        // Calculate distinct values using HashSet
        let distinct: std::collections::HashSet<_> = non_null.iter().copied().collect();
        stats.distinct_count = distinct.len() as u64;
        
        // Scale up distinct count if sampling was used
        if sample_ratio < 1.0 && total_rows > 0 {
            // Use the Good-Turing estimator or similar for distinct value estimation
            // Simplified: assume we found most distinct values in the sample
            let estimated_distinct = (stats.distinct_count as f64 / sample_ratio.sqrt()) as u64;
            stats.distinct_count = estimated_distinct.min(total_rows);
        }
        
        // Min/Max values
        stats.min_value = non_null.iter().min().cloned().cloned();
        stats.max_value = non_null.iter().max().cloned().cloned();
        
        // Average size
        let total_size: usize = non_null.iter()
            .map(|v| v.serialize().len())
            .sum();
        stats.avg_size = total_size as f64 / non_null.len() as f64;
        
        // Build histogram for numeric and string columns
        let non_null_cloned: Vec<Value> = non_null.into_iter().cloned().collect();
        if Self::is_histogram_type(column_name) {
            stats.histogram = Histogram::from_values(&non_null_cloned, self.config.histogram_buckets);
        }
        
        // Build MCV (most common values) list
        stats.mcv = self.compute_mcv(&non_null_cloned);
        
        // Selectivity
        stats.update_selectivity(total_rows);
        
        stats
    }
    
    /// Check if column type supports histogram
    fn is_histogram_type(_column_name: &str) -> bool {
        // For now, assume all columns can have histograms
        // In a real implementation, we'd check the column type
        true
    }
    
    /// Compute most common values
    fn compute_mcv(&self, values: &[Value]) -> Vec<(Value, u64)> {
        if values.is_empty() {
            return Vec::new();
        }
        
        // Count frequencies
        let mut freq_map: HashMap<Value, u64> = HashMap::new();
        for value in values {
            *freq_map.entry(value.clone()).or_insert(0) += 1;
        }
        
        // Convert to vector and sort by frequency
        let mut freq_vec: Vec<(Value, u64)> = freq_map.into_iter().collect();
        freq_vec.sort_by(|a, b| b.1.cmp(&a.1));
        
        // Take top N
        freq_vec.into_iter()
            .take(self.config.mcv_count)
            .collect()
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
                if pattern.ends_with('%') && !pattern.starts_with('%') {
                    // Prefix match: can use index, usually selective
                    0.1
                } else if pattern.starts_with('%') && !pattern.ends_with('%') {
                    // Suffix match: can't use index, less selective
                    0.5
                } else if pattern.contains('%') {
                    // Contains pattern: least selective
                    0.8
                } else {
                    // Exact match (no wildcards)
                    0.01
                }
            }
            PredicateType::IsNull => {
                column.map(|c| c.null_ratio(table_stats.row_count))
                    .unwrap_or(0.0)
            }
            PredicateType::IsNotNull => {
                let null_ratio = column.map(|c| c.null_ratio(table_stats.row_count))
                    .unwrap_or(0.0);
                1.0 - null_ratio
            }
            PredicateType::In(count) => {
                // Estimate based on equality selectivity * count
                let eq_selectivity = column.map(|c| c.estimate_equality_selectivity())
                    .unwrap_or(0.1);
                (eq_selectivity * count as f64).min(0.95)
            }
        }
    }
    
    /// Update statistics after data modification
    pub fn update_stats_after_insert(stats: &mut TableStats, record: &Record) {
        stats.row_count += 1;
        stats.record_modification();
        
        // For incremental updates, we'd update column stats here
        // For simplicity, mark as needing full recalculation after many modifications
        if stats.needs_update(100) {
            stats.last_updated = Instant::now();
        }
    }
    
    /// Update statistics after delete
    pub fn update_stats_after_delete(stats: &mut TableStats) {
        stats.row_count = stats.row_count.saturating_sub(1);
        stats.record_modification();
    }
    
    /// Estimate join selectivity between two tables
    pub fn estimate_join_selectivity(
        left_stats: &TableStats,
        right_stats: &TableStats,
        left_column: &str,
        right_column: &str,
    ) -> f64 {
        let left_col = left_stats.get_column(left_column);
        let right_col = right_stats.get_column(right_column);
        
        if let (Some(l), Some(r)) = (left_col, right_col) {
            // If both columns have statistics, estimate based on unique values
            let max_distinct = l.distinct_count.max(r.distinct_count);
            if max_distinct > 0 {
                return (1.0 / max_distinct as f64).max(0.0001);
            }
        }
        
        // Default: assume 1% selectivity for foreign key joins
        0.01
    }
}

impl Default for StatsCollector {
    fn default() -> Self {
        Self::new()
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
    In(usize), // Number of values in IN list
}

/// Statistics catalog for all tables
pub struct StatsCatalog {
    tables: HashMap<String, TableStats>,
    config: StatsConfig,
}

impl StatsCatalog {
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
            config: StatsConfig::default(),
        }
    }
    
    pub fn with_config(config: StatsConfig) -> Self {
        Self {
            tables: HashMap::new(),
            config,
        }
    }
    
    pub fn get_table_stats(&self, table_name: &str) -> Option<&TableStats> {
        self.tables.get(table_name)
    }
    
    pub fn get_table_stats_mut(&mut self, table_name: &str) -> Option<&mut TableStats> {
        self.tables.get_mut(table_name)
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
    
    /// Check if stats need refresh for a table
    pub fn needs_refresh(&self, table_name: &str) -> bool {
        if let Some(stats) = self.tables.get(table_name) {
            stats.is_stale(self.config.staleness_threshold) ||
            stats.needs_update(self.config.modification_threshold)
        } else {
            true // No stats available, need to collect
        }
    }
    
    /// Collect or refresh statistics for a table
    pub fn collect_stats(&mut self, db: &mut BtreeDatabase, table_name: &str) -> Option<&TableStats> {
        if self.needs_refresh(table_name) {
            let collector = StatsCollector::with_config(self.config.clone());
            if let Some(stats) = collector.collect_table_stats(db, table_name) {
                self.update_table_stats(stats);
            }
        }
        self.tables.get(table_name)
    }
    
    /// Get estimated row count for a table
    pub fn get_row_count(&self, table_name: &str) -> u64 {
        self.tables.get(table_name)
            .map(|s| s.row_count)
            .unwrap_or(0)
    }
    
    /// Get column selectivity
    pub fn get_column_selectivity(&self, table_name: &str, column_name: &str) -> f64 {
        self.tables.get(table_name)
            .and_then(|s| s.get_column(column_name))
            .map(|c| c.selectivity)
            .unwrap_or(0.1)
    }
}

impl Default for StatsCatalog {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for StatsConfig {
    fn clone(&self) -> Self {
        Self {
            sample_ratio: self.sample_ratio,
            min_sample_size: self.min_sample_size,
            max_sample_size: self.max_sample_size,
            histogram_buckets: self.histogram_buckets,
            mcv_count: self.mcv_count,
            staleness_threshold: self.staleness_threshold,
            modification_threshold: self.modification_threshold,
        }
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
                default_value: None,
                is_virtual: false,
                generated_always: None,
            },
            ColumnDef {
                name: "status".to_string(),
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
        
        // Insert test data with varied distribution
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
        
        let stats = StatsCollector::collect_table_stats(&StatsCollector::new(), &mut db, "users")
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
        let stats = StatsCollector::collect_table_stats(&StatsCollector::new(), &mut db, "users")
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
        
        let collector = StatsCollector::new();
        let stats = collector.analyze_column("test", &values, 5);
        
        assert_eq!(stats.null_count, 1);
        assert_eq!(stats.distinct_count, 3); // 1, 2, 3
        assert_eq!(stats.min_value, Some(Value::Integer(1)));
        assert_eq!(stats.max_value, Some(Value::Integer(3)));
    }

    #[test]
    fn test_histogram() {
        let values: Vec<Value> = (1..=100)
            .map(|i| Value::Integer(i))
            .collect();
        
        let histogram = Histogram::from_values(&values, 10);
        assert!(histogram.is_some());
        
        let h = histogram.unwrap();
        assert_eq!(h.buckets.len(), 10);
        assert_eq!(h.total_count, 100);
    }

    #[test]
    fn test_stats_catalog() {
        let mut catalog = StatsCatalog::new();
        let mut db = create_test_db();
        
        // Initially needs refresh
        assert!(catalog.needs_refresh("users"));
        
        // Collect stats
        catalog.collect_stats(&mut db, "users");
        
        // Should now have stats
        assert!(!catalog.needs_refresh("users"));
        assert!(catalog.get_row_count("users") > 0);
    }

    #[test]
    fn test_mcv_computation() {
        let values = vec![
            Value::Text("A".to_string()),
            Value::Text("A".to_string()),
            Value::Text("B".to_string()),
            Value::Text("B".to_string()),
            Value::Text("B".to_string()),
            Value::Text("C".to_string()),
        ];
        
        let collector = StatsCollector::new();
        let mcv = collector.compute_mcv(&values);
        
        // B should be most common (3 occurrences)
        assert!(!mcv.is_empty());
        assert_eq!(mcv[0].0, Value::Text("B".to_string()));
        assert_eq!(mcv[0].1, 3);
    }

    #[test]
    fn test_join_selectivity_estimation() {
        let mut left_stats = TableStats::new("left".to_string());
        left_stats.row_count = 1000;
        left_stats.column_stats.insert("id".to_string(), ColumnStats {
            column_name: "id".to_string(),
            distinct_count: 1000,
            null_count: 0,
            min_value: Some(Value::Integer(1)),
            max_value: Some(Value::Integer(1000)),
            avg_size: 8.0,
            selectivity: 0.001,
            histogram: None,
            mcv: Vec::new(),
        });
        
        let mut right_stats = TableStats::new("right".to_string());
        right_stats.row_count = 100;
        right_stats.column_stats.insert("left_id".to_string(), ColumnStats {
            column_name: "left_id".to_string(),
            distinct_count: 100,
            null_count: 0,
            min_value: Some(Value::Integer(1)),
            max_value: Some(Value::Integer(100)),
            avg_size: 8.0,
            selectivity: 0.01,
            histogram: None,
            mcv: Vec::new(),
        });
        
        let selectivity = StatsCollector::estimate_join_selectivity(
            &left_stats, &right_stats, "id", "left_id"
        );
        
        // Should be around 0.001 (1 / max distinct)
        assert!(selectivity > 0.0 && selectivity <= 0.01);
    }
}
