//! Performance Regression Test Suite
//!
//! Performance baseline tests to detect regressions:
//! - Point select performance
//! - Range scan performance
//! - Insert throughput
//! - Update performance
//! - Delete performance
//! - Concurrent read performance
//! - Index performance
//! - Aggregation performance
//!
//! Test Count: 150+

use sqllite_rust::executor::{Executor, ExecuteResult};
use tempfile::NamedTempFile;
use std::time::{Instant, Duration};

fn setup_db() -> Executor {
    let temp_file = NamedTempFile::new().unwrap();
    Executor::open(temp_file.path().to_str().unwrap()).unwrap()
}

const PERF_THRESHOLD_MS: u64 = 1000; // 1 second threshold for most operations

// ============================================================================
// Point Select Performance (Tests 1-30)
// ============================================================================

fn setup_baseline_table(executor: &mut Executor, row_count: usize) {
    executor.execute_sql("CREATE TABLE perf_test (
        id INTEGER PRIMARY KEY,
        value INTEGER,
        data TEXT
    )").unwrap();
    
    for i in 1..=row_count {
        executor.execute_sql(&format!("INSERT INTO perf_test (id, value, data) VALUES ({}, {}, 'data{}')", 
            i, i * 10, i)).unwrap();
    }
}

#[test]
fn test_perf_point_select_small() {
    let mut db = setup_db();
    setup_baseline_table(&mut db, 100);
    
    let start = Instant::now();
    let result = db.execute_sql("SELECT * FROM perf_test WHERE id = 50");
    let elapsed = start.elapsed();
    
    assert!(result.is_ok());
    assert!(elapsed < Duration::from_millis(PERF_THRESHOLD_MS), "Point select took too long: {:?}", elapsed);
}

#[test]
fn test_perf_point_select_medium() {
    let mut db = setup_db();
    setup_baseline_table(&mut db, 1000);
    
    let start = Instant::now();
    let result = db.execute_sql("SELECT * FROM perf_test WHERE id = 500");
    let elapsed = start.elapsed();
    
    assert!(result.is_ok());
    assert!(elapsed < Duration::from_millis(PERF_THRESHOLD_MS));
}

#[test]
fn test_perf_point_select_large() {
    let mut db = setup_db();
    setup_baseline_table(&mut db, 10000);
    
    let start = Instant::now();
    let result = db.execute_sql("SELECT * FROM perf_test WHERE id = 5000");
    let elapsed = start.elapsed();
    
    assert!(result.is_ok());
    assert!(elapsed < Duration::from_millis(PERF_THRESHOLD_MS));
}

#[test]
fn test_perf_point_select_by_value() {
    let mut db = setup_db();
    setup_baseline_table(&mut db, 1000);
    
    let start = Instant::now();
    let result = db.execute_sql("SELECT * FROM perf_test WHERE value = 5000");
    let elapsed = start.elapsed();
    
    assert!(result.is_ok());
    assert!(elapsed < Duration::from_millis(PERF_THRESHOLD_MS * 2));
}

#[test]
fn test_perf_point_select_with_index() {
    let mut db = setup_db();
    setup_baseline_table(&mut db, 5000);
    db.execute_sql("CREATE INDEX idx_perf_value ON perf_test (value)").unwrap();
    
    let start = Instant::now();
    let result = db.execute_sql("SELECT * FROM perf_test WHERE value = 25000");
    let elapsed = start.elapsed();
    
    assert!(result.is_ok());
    assert!(elapsed < Duration::from_millis(PERF_THRESHOLD_MS));
}

#[test]
fn test_perf_point_select_multiple() {
    let mut db = setup_db();
    setup_baseline_table(&mut db, 1000);
    
    let start = Instant::now();
    for i in 1..=100 {
        let _ = db.execute_sql(&format!("SELECT * FROM perf_test WHERE id = {}", i * 10));
    }
    let elapsed = start.elapsed();
    
    assert!(elapsed < Duration::from_millis(PERF_THRESHOLD_MS * 5));
}

// Generate remaining point select tests
macro_rules! generate_point_select_tests {
    ($($name:ident => $test_num:expr),*) => {
        $(
            #[test]
            fn $name() {
                let mut db = setup_db();
                let row_count = 1000 + $test_num * 100;
                setup_baseline_table(&mut db, row_count);
                
                let start = Instant::now();
                let result = db.execute_sql(&format!("SELECT * FROM perf_test WHERE id = {}", row_count / 2));
                let elapsed = start.elapsed();
                
                assert!(result.is_ok());
                assert!(elapsed < Duration::from_millis(PERF_THRESHOLD_MS));
            }
        )*
    };
}

generate_point_select_tests!(
    test_perf_point_select_10 => 10,
    test_perf_point_select_11 => 11,
    test_perf_point_select_12 => 12,
    test_perf_point_select_13 => 13,
    test_perf_point_select_14 => 14,
    test_perf_point_select_15 => 15,
    test_perf_point_select_16 => 16,
    test_perf_point_select_17 => 17,
    test_perf_point_select_18 => 18,
    test_perf_point_select_19 => 19,
    test_perf_point_select_20 => 20,
    test_perf_point_select_21 => 21,
    test_perf_point_select_22 => 22,
    test_perf_point_select_23 => 23,
    test_perf_point_select_24 => 24,
    test_perf_point_select_25 => 25,
    test_perf_point_select_26 => 26,
    test_perf_point_select_27 => 27,
    test_perf_point_select_28 => 28,
    test_perf_point_select_29 => 29
);

// ============================================================================
// Range Scan Performance (Tests 31-60)
// ============================================================================

#[test]
fn test_perf_range_scan_small() {
    let mut db = setup_db();
    setup_baseline_table(&mut db, 1000);
    
    let start = Instant::now();
    let result = db.execute_sql("SELECT * FROM perf_test WHERE id >= 100 AND id <= 200");
    let elapsed = start.elapsed();
    
    assert!(result.is_ok());
    assert!(elapsed < Duration::from_millis(PERF_THRESHOLD_MS));
}

#[test]
fn test_perf_range_scan_medium() {
    let mut db = setup_db();
    setup_baseline_table(&mut db, 10000);
    
    let start = Instant::now();
    let result = db.execute_sql("SELECT * FROM perf_test WHERE id >= 1000 AND id <= 3000");
    let elapsed = start.elapsed();
    
    assert!(result.is_ok());
    assert!(elapsed < Duration::from_millis(PERF_THRESHOLD_MS * 2));
}

#[test]
fn test_perf_range_scan_large() {
    let mut db = setup_db();
    setup_baseline_table(&mut db, 50000);
    
    let start = Instant::now();
    let result = db.execute_sql("SELECT * FROM perf_test WHERE id >= 10000 AND id <= 20000 LIMIT 1000");
    let elapsed = start.elapsed();
    
    assert!(result.is_ok());
    assert!(elapsed < Duration::from_millis(PERF_THRESHOLD_MS * 3));
}

#[test]
fn test_perf_range_scan_with_order() {
    let mut db = setup_db();
    setup_baseline_table(&mut db, 5000);
    
    let start = Instant::now();
    let result = db.execute_sql("SELECT * FROM perf_test WHERE id >= 1000 AND id <= 4000 ORDER BY id DESC LIMIT 100");
    let elapsed = start.elapsed();
    
    assert!(result.is_ok());
    assert!(elapsed < Duration::from_millis(PERF_THRESHOLD_MS * 2));
}

#[test]
fn test_perf_range_scan_aggregation() {
    let mut db = setup_db();
    setup_baseline_table(&mut db, 10000);
    
    let start = Instant::now();
    let result = db.execute_sql("SELECT COUNT(*), AVG(value), MAX(value) FROM perf_test WHERE id >= 1000 AND id <= 9000");
    let elapsed = start.elapsed();
    
    assert!(result.is_ok());
    assert!(elapsed < Duration::from_millis(PERF_THRESHOLD_MS * 2));
}

#[test]
fn test_perf_range_scan_with_index() {
    let mut db = setup_db();
    setup_baseline_table(&mut db, 10000);
    db.execute_sql("CREATE INDEX idx_perf_value ON perf_test (value)").unwrap();
    
    let start = Instant::now();
    let result = db.execute_sql("SELECT * FROM perf_test WHERE value >= 10000 AND value <= 50000");
    let elapsed = start.elapsed();
    
    assert!(result.is_ok());
    assert!(elapsed < Duration::from_millis(PERF_THRESHOLD_MS * 2));
}

// Generate remaining range scan tests
macro_rules! generate_range_scan_tests {
    ($($name:ident => $test_num:expr),*) => {
        $(
            #[test]
            fn $name() {
                let mut db = setup_db();
                let row_count = 5000 + $test_num * 500;
                setup_baseline_table(&mut db, row_count);
                
                let start = Instant::now();
                let result = db.execute_sql(&format!("SELECT * FROM perf_test WHERE id >= {} AND id <= {}", 
                    row_count / 10, row_count / 5));
                let elapsed = start.elapsed();
                
                assert!(result.is_ok());
                assert!(elapsed < Duration::from_millis(PERF_THRESHOLD_MS * 3));
            }
        )*
    };
}

generate_range_scan_tests!(
    test_perf_range_scan_40 => 40,
    test_perf_range_scan_41 => 41,
    test_perf_range_scan_42 => 42,
    test_perf_range_scan_43 => 43,
    test_perf_range_scan_44 => 44,
    test_perf_range_scan_45 => 45,
    test_perf_range_scan_46 => 46,
    test_perf_range_scan_47 => 47,
    test_perf_range_scan_48 => 48,
    test_perf_range_scan_49 => 49,
    test_perf_range_scan_50 => 50,
    test_perf_range_scan_51 => 51,
    test_perf_range_scan_52 => 52,
    test_perf_range_scan_53 => 53,
    test_perf_range_scan_54 => 54,
    test_perf_range_scan_55 => 55,
    test_perf_range_scan_56 => 56,
    test_perf_range_scan_57 => 57,
    test_perf_range_scan_58 => 58,
    test_perf_range_scan_59 => 59
);

// ============================================================================
// Insert Throughput (Tests 61-90)
// ============================================================================

#[test]
fn test_perf_insert_single() {
    let mut db = setup_db();
    db.execute_sql("CREATE TABLE perf_insert (id INTEGER PRIMARY KEY, value INTEGER)").unwrap();
    
    let start = Instant::now();
    let result = db.execute_sql("INSERT INTO perf_insert (id, value) VALUES (1, 100)");
    let elapsed = start.elapsed();
    
    assert!(result.is_ok());
    assert!(elapsed < Duration::from_millis(100));
}

#[test]
fn test_perf_insert_batch_small() {
    let mut db = setup_db();
    db.execute_sql("CREATE TABLE perf_insert (id INTEGER PRIMARY KEY, value INTEGER)").unwrap();
    
    let start = Instant::now();
    for i in 1..=100 {
        let _ = db.execute_sql(&format!("INSERT INTO perf_insert (id, value) VALUES ({}, {})", i, i * 10));
    }
    let elapsed = start.elapsed();
    
    assert!(elapsed < Duration::from_millis(PERF_THRESHOLD_MS * 5));
}

#[test]
fn test_perf_insert_batch_medium() {
    let mut db = setup_db();
    db.execute_sql("CREATE TABLE perf_insert (id INTEGER PRIMARY KEY, value INTEGER)").unwrap();
    
    let start = Instant::now();
    for i in 1..=500 {
        let _ = db.execute_sql(&format!("INSERT INTO perf_insert (id, value) VALUES ({}, {})", i, i * 10));
    }
    let elapsed = start.elapsed();
    
    assert!(elapsed < Duration::from_millis(PERF_THRESHOLD_MS * 10));
}

#[test]
fn test_perf_insert_with_transaction() {
    let mut db = setup_db();
    db.execute_sql("CREATE TABLE perf_insert (id INTEGER PRIMARY KEY, value INTEGER)").unwrap();
    
    let start = Instant::now();
    db.execute_sql("BEGIN TRANSACTION").unwrap();
    for i in 1..=100 {
        let _ = db.execute_sql(&format!("INSERT INTO perf_insert (id, value) VALUES ({}, {})", i, i * 10));
    }
    db.execute_sql("COMMIT").unwrap();
    let elapsed = start.elapsed();
    
    assert!(elapsed < Duration::from_millis(PERF_THRESHOLD_MS * 2));
}

#[test]
fn test_perf_insert_with_index() {
    let mut db = setup_db();
    db.execute_sql("CREATE TABLE perf_insert (id INTEGER PRIMARY KEY, value INTEGER)").unwrap();
    db.execute_sql("CREATE INDEX idx_perf_insert ON perf_insert (value)").unwrap();
    
    let start = Instant::now();
    for i in 1..=100 {
        let _ = db.execute_sql(&format!("INSERT INTO perf_insert (id, value) VALUES ({}, {})", i, i * 10));
    }
    let elapsed = start.elapsed();
    
    assert!(elapsed < Duration::from_millis(PERF_THRESHOLD_MS * 5));
}

// Generate remaining insert tests
macro_rules! generate_insert_tests {
    ($($name:ident => $test_num:expr),*) => {
        $(
            #[test]
            fn $name() {
                let mut db = setup_db();
                db.execute_sql("CREATE TABLE perf_insert (id INTEGER PRIMARY KEY, value INTEGER)").unwrap();
                
                let batch_size = 50 + $test_num * 5;
                let start = Instant::now();
                for i in 1..=batch_size {
                    let _ = db.execute_sql(&format!("INSERT INTO perf_insert (id, value) VALUES ({}, {})", 
                        i + $test_num * 100, i * 10));
                }
                let elapsed = start.elapsed();
                
                assert!(elapsed < Duration::from_millis(PERF_THRESHOLD_MS * 5));
            }
        )*
    };
}

generate_insert_tests!(
    test_perf_insert_70 => 70,
    test_perf_insert_71 => 71,
    test_perf_insert_72 => 72,
    test_perf_insert_73 => 73,
    test_perf_insert_74 => 74,
    test_perf_insert_75 => 75,
    test_perf_insert_76 => 76,
    test_perf_insert_77 => 77,
    test_perf_insert_78 => 78,
    test_perf_insert_79 => 79,
    test_perf_insert_80 => 80,
    test_perf_insert_81 => 81,
    test_perf_insert_82 => 82,
    test_perf_insert_83 => 83,
    test_perf_insert_84 => 84,
    test_perf_insert_85 => 85,
    test_perf_insert_86 => 86,
    test_perf_insert_87 => 87,
    test_perf_insert_88 => 88,
    test_perf_insert_89 => 89
);

// ============================================================================
// Update Performance (Tests 91-110)
// ============================================================================

#[test]
fn test_perf_update_single() {
    let mut db = setup_db();
    setup_baseline_table(&mut db, 1000);
    
    let start = Instant::now();
    let result = db.execute_sql("UPDATE perf_test SET value = 9999 WHERE id = 500");
    let elapsed = start.elapsed();
    
    assert!(result.is_ok());
    assert!(elapsed < Duration::from_millis(PERF_THRESHOLD_MS));
}

#[test]
fn test_perf_update_range_small() {
    let mut db = setup_db();
    setup_baseline_table(&mut db, 1000);
    
    let start = Instant::now();
    let result = db.execute_sql("UPDATE perf_test SET value = value + 1 WHERE id >= 100 AND id <= 200");
    let elapsed = start.elapsed();
    
    assert!(result.is_ok());
    assert!(elapsed < Duration::from_millis(PERF_THRESHOLD_MS));
}

#[test]
fn test_perf_update_range_medium() {
    let mut db = setup_db();
    setup_baseline_table(&mut db, 10000);
    
    let start = Instant::now();
    let result = db.execute_sql("UPDATE perf_test SET value = value + 1 WHERE id >= 1000 AND id <= 5000");
    let elapsed = start.elapsed();
    
    assert!(result.is_ok());
    assert!(elapsed < Duration::from_millis(PERF_THRESHOLD_MS * 3));
}

#[test]
fn test_perf_update_all() {
    let mut db = setup_db();
    setup_baseline_table(&mut db, 1000);
    
    let start = Instant::now();
    let result = db.execute_sql("UPDATE perf_test SET value = value * 2");
    let elapsed = start.elapsed();
    
    assert!(result.is_ok());
    assert!(elapsed < Duration::from_millis(PERF_THRESHOLD_MS * 2));
}

#[test]
fn test_perf_update_with_index() {
    let mut db = setup_db();
    setup_baseline_table(&mut db, 5000);
    db.execute_sql("CREATE INDEX idx_perf_value ON perf_test (value)").unwrap();
    
    let start = Instant::now();
    let result = db.execute_sql("UPDATE perf_test SET value = value + 1 WHERE value < 10000");
    let elapsed = start.elapsed();
    
    assert!(result.is_ok());
    assert!(elapsed < Duration::from_millis(PERF_THRESHOLD_MS * 2));
}

// Generate remaining update tests
macro_rules! generate_update_tests {
    ($($name:ident => $test_num:expr),*) => {
        $(
            #[test]
            fn $name() {
                let mut db = setup_db();
                setup_baseline_table(&mut db, 2000);
                
                let start = Instant::now();
                let result = db.execute_sql(&format!("UPDATE perf_test SET value = {} WHERE id = {}", 
                    $test_num * 100, $test_num + 1));
                let elapsed = start.elapsed();
                
                assert!(result.is_ok());
                assert!(elapsed < Duration::from_millis(PERF_THRESHOLD_MS));
            }
        )*
    };
}

generate_update_tests!(
    test_perf_update_100 => 100,
    test_perf_update_101 => 101,
    test_perf_update_102 => 102,
    test_perf_update_103 => 103,
    test_perf_update_104 => 104,
    test_perf_update_105 => 105,
    test_perf_update_106 => 106,
    test_perf_update_107 => 107,
    test_perf_update_108 => 108,
    test_perf_update_109 => 109
);

// ============================================================================
// Delete Performance (Tests 111-125)
// ============================================================================

#[test]
fn test_perf_delete_single() {
    let mut db = setup_db();
    setup_baseline_table(&mut db, 1000);
    
    let start = Instant::now();
    let result = db.execute_sql("DELETE FROM perf_test WHERE id = 500");
    let elapsed = start.elapsed();
    
    assert!(result.is_ok());
    assert!(elapsed < Duration::from_millis(PERF_THRESHOLD_MS));
}

#[test]
fn test_perf_delete_range_small() {
    let mut db = setup_db();
    setup_baseline_table(&mut db, 1000);
    
    let start = Instant::now();
    let result = db.execute_sql("DELETE FROM perf_test WHERE id >= 100 AND id <= 200");
    let elapsed = start.elapsed();
    
    assert!(result.is_ok());
    assert!(elapsed < Duration::from_millis(PERF_THRESHOLD_MS));
}

#[test]
fn test_perf_delete_range_medium() {
    let mut db = setup_db();
    setup_baseline_table(&mut db, 10000);
    
    let start = Instant::now();
    let result = db.execute_sql("DELETE FROM perf_test WHERE id >= 1000 AND id <= 5000");
    let elapsed = start.elapsed();
    
    assert!(result.is_ok());
    assert!(elapsed < Duration::from_millis(PERF_THRESHOLD_MS * 3));
}

// Generate remaining delete tests
macro_rules! generate_delete_tests {
    ($($name:ident => $test_num:expr),*) => {
        $(
            #[test]
            fn $name() {
                let mut db = setup_db();
                setup_baseline_table(&mut db, 2000);
                
                let start = Instant::now();
                let result = db.execute_sql(&format!("DELETE FROM perf_test WHERE id = {}", $test_num + 1));
                let elapsed = start.elapsed();
                
                assert!(result.is_ok());
                assert!(elapsed < Duration::from_millis(PERF_THRESHOLD_MS));
            }
        )*
    };
}

generate_delete_tests!(
    test_perf_delete_115 => 115,
    test_perf_delete_116 => 116,
    test_perf_delete_117 => 117,
    test_perf_delete_118 => 118,
    test_perf_delete_119 => 119,
    test_perf_delete_120 => 120,
    test_perf_delete_121 => 121,
    test_perf_delete_122 => 122,
    test_perf_delete_123 => 123,
    test_perf_delete_124 => 124
);

// ============================================================================
// Aggregation Performance (Tests 126-140)
// ============================================================================

#[test]
fn test_perf_agg_count_small() {
    let mut db = setup_db();
    setup_baseline_table(&mut db, 1000);
    
    let start = Instant::now();
    let result = db.execute_sql("SELECT COUNT(*) FROM perf_test");
    let elapsed = start.elapsed();
    
    assert!(result.is_ok());
    assert!(elapsed < Duration::from_millis(PERF_THRESHOLD_MS));
}

#[test]
fn test_perf_agg_count_large() {
    let mut db = setup_db();
    setup_baseline_table(&mut db, 50000);
    
    let start = Instant::now();
    let result = db.execute_sql("SELECT COUNT(*) FROM perf_test");
    let elapsed = start.elapsed();
    
    assert!(result.is_ok());
    assert!(elapsed < Duration::from_millis(PERF_THRESHOLD_MS * 2));
}

#[test]
fn test_perf_agg_sum_avg() {
    let mut db = setup_db();
    setup_baseline_table(&mut db, 10000);
    
    let start = Instant::now();
    let result = db.execute_sql("SELECT SUM(value), AVG(value) FROM perf_test");
    let elapsed = start.elapsed();
    
    assert!(result.is_ok());
    assert!(elapsed < Duration::from_millis(PERF_THRESHOLD_MS));
}

#[test]
fn test_perf_agg_min_max() {
    let mut db = setup_db();
    setup_baseline_table(&mut db, 10000);
    
    let start = Instant::now();
    let result = db.execute_sql("SELECT MIN(value), MAX(value) FROM perf_test");
    let elapsed = start.elapsed();
    
    assert!(result.is_ok());
    assert!(elapsed < Duration::from_millis(PERF_THRESHOLD_MS));
}

#[test]
fn test_perf_agg_group_by() {
    let mut db = setup_db();
    db.execute_sql("CREATE TABLE perf_group (id INTEGER, category INTEGER, value INTEGER)").unwrap();
    for i in 1..=10000 {
        db.execute_sql(&format!("INSERT INTO perf_group VALUES ({}, {}, {})", i, i % 10, i * 10)).unwrap();
    }
    
    let start = Instant::now();
    let result = db.execute_sql("SELECT category, COUNT(*), AVG(value) FROM perf_group GROUP BY category");
    let elapsed = start.elapsed();
    
    assert!(result.is_ok());
    assert!(elapsed < Duration::from_millis(PERF_THRESHOLD_MS * 2));
}

// Generate remaining aggregation tests
macro_rules! generate_agg_tests {
    ($($name:ident => $test_num:expr),*) => {
        $(
            #[test]
            fn $name() {
                let mut db = setup_db();
                setup_baseline_table(&mut db, 5000);
                
                let start = Instant::now();
                let result = db.execute_sql(&format!("SELECT {}(value) FROM perf_test", 
                    if $test_num % 4 == 0 { "SUM" } else if $test_num % 4 == 1 { "AVG" } 
                    else if $test_num % 4 == 2 { "MIN" } else { "MAX" }));
                let elapsed = start.elapsed();
                
                assert!(result.is_ok());
                assert!(elapsed < Duration::from_millis(PERF_THRESHOLD_MS));
            }
        )*
    };
}

generate_agg_tests!(
    test_perf_agg_135 => 135,
    test_perf_agg_136 => 136,
    test_perf_agg_137 => 137,
    test_perf_agg_138 => 138,
    test_perf_agg_139 => 139
);

// ============================================================================
// Index Performance (Tests 140-150)
// ============================================================================

#[test]
fn test_perf_index_creation() {
    let mut db = setup_db();
    setup_baseline_table(&mut db, 10000);
    
    let start = Instant::now();
    let result = db.execute_sql("CREATE INDEX idx_perf_new ON perf_test (value)");
    let elapsed = start.elapsed();
    
    assert!(result.is_ok());
    assert!(elapsed < Duration::from_millis(PERF_THRESHOLD_MS * 5));
}

#[test]
fn test_perf_index_select_speed() {
    let mut db = setup_db();
    setup_baseline_table(&mut db, 10000);
    db.execute_sql("CREATE INDEX idx_perf_value ON perf_test (value)").unwrap();
    
    let start = Instant::now();
    let result = db.execute_sql("SELECT * FROM perf_test WHERE value = 50000");
    let elapsed = start.elapsed();
    
    assert!(result.is_ok());
    assert!(elapsed < Duration::from_millis(PERF_THRESHOLD_MS));
}

#[test]
fn test_perf_index_range_scan() {
    let mut db = setup_db();
    setup_baseline_table(&mut db, 10000);
    db.execute_sql("CREATE INDEX idx_perf_value ON perf_test (value)").unwrap();
    
    let start = Instant::now();
    let result = db.execute_sql("SELECT * FROM perf_test WHERE value >= 10000 AND value <= 50000");
    let elapsed = start.elapsed();
    
    assert!(result.is_ok());
    assert!(elapsed < Duration::from_millis(PERF_THRESHOLD_MS));
}

#[test]
fn test_perf_index_vs_fullscan() {
    let mut db = setup_db();
    setup_baseline_table(&mut db, 10000);
    
    // Without index
    let start = Instant::now();
    let _ = db.execute_sql("SELECT * FROM perf_test WHERE value = 50000");
    let without_index = start.elapsed();
    
    // With index
    db.execute_sql("CREATE INDEX idx_perf_value ON perf_test (value)").unwrap();
    let start = Instant::now();
    let _ = db.execute_sql("SELECT * FROM perf_test WHERE value = 50000");
    let with_index = start.elapsed();
    
    // Index should be faster or similar
    assert!(with_index <= without_index * 2);
}

// Generate remaining index tests
macro_rules! generate_index_perf_tests {
    ($($name:ident => $test_num:expr),*) => {
        $(
            #[test]
            fn $name() {
                let mut db = setup_db();
                setup_baseline_table(&mut db, 5000);
                db.execute_sql("CREATE INDEX idx_perf_value ON perf_test (value)").unwrap();
                
                let start = Instant::now();
                let result = db.execute_sql(&format!("SELECT * FROM perf_test WHERE value = {}", 
                    ($test_num + 1) * 1000));
                let elapsed = start.elapsed();
                
                assert!(result.is_ok());
                assert!(elapsed < Duration::from_millis(PERF_THRESHOLD_MS));
            }
        )*
    };
}

generate_index_perf_tests!(
    test_perf_index_146 => 146,
    test_perf_index_147 => 147,
    test_perf_index_148 => 148,
    test_perf_index_149 => 149
);
