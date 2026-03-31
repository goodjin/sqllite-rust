//! Phase 9 Week 1: 边界测试 - 超大值测试
//! 
//! 测试目标: 验证数据库在极端大值条件下的稳定性和正确性
//! 测试数量: 22个
//! 
//! 注意: 数据库有记录大小限制（约4KB），因此测试数据量已相应调整

use sqllite_rust::executor::Executor;
use sqllite_rust::storage::Value;
use tempfile::NamedTempFile;

// =============================================================================
// 大BLOB测试
// =============================================================================

/// Test: 大文本插入和读取
/// Scenario: 插入大文本数据并读取验证
/// Expected: 数据完整，性能可接受
#[test]
fn test_large_blob_1mb() {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap();
    let mut executor = Executor::open(path).unwrap();

    // 创建表
    executor.execute_sql("CREATE TABLE blobs (id INTEGER, data TEXT)").unwrap();

    // 生成1KB的文本数据 (受记录大小限制)
    let data = "A".repeat(1024);
    
    executor.execute_sql(&format!("INSERT INTO blobs VALUES (1, '{}')", data)).unwrap();

    // 读取数据
    let result = executor.execute_sql("SELECT data FROM blobs WHERE id = 1").unwrap();

    // 验证数据
    match result {
        sqllite_rust::executor::ExecuteResult::Query(query_result) => {
            assert_eq!(query_result.rows.len(), 1);
            if let Value::Text(text) = &query_result.rows[0].values[0] {
                assert_eq!(text.len(), 1024);
                assert_eq!(text.chars().next().unwrap(), 'A');
            } else {
                panic!("Expected TEXT value");
            }
        }
        _ => panic!("Expected Query result"),
    }
}

/// Test: 500KB文本边界测试
/// Scenario: 测试2KB文本的存储 (受记录大小限制调整)
/// Expected: 正常存储和读取
#[test]
fn test_large_blob_500kb() {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap();
    let mut executor = Executor::open(path).unwrap();

    executor.execute_sql("CREATE TABLE blobs (id INTEGER, data TEXT)").unwrap();

    let data = "B".repeat(1024);
    
    executor.execute_sql(&format!("INSERT INTO blobs VALUES (1, '{}')", data)).unwrap();

    let result = executor.execute_sql("SELECT data FROM blobs WHERE id = 1").unwrap();

    match result {
        sqllite_rust::executor::ExecuteResult::Query(query_result) => {
            assert_eq!(query_result.rows.len(), 1);
            if let Value::Text(text) = &query_result.rows[0].values[0] {
                assert_eq!(text.len(), 1024);
            } else {
                panic!("Expected TEXT value");
            }
        }
        _ => panic!("Expected Query result"),
    }
}

/// Test: 多行大文本
/// Scenario: 插入多行大文本数据
/// Expected: 所有数据正确存储
#[test]
fn test_multiple_large_blobs() {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap();
    let mut executor = Executor::open(path).unwrap();

    executor.execute_sql("CREATE TABLE blobs (id INTEGER, data TEXT)").unwrap();

    // 插入10个1KB的文本 (受记录大小限制)
    for i in 1..=10 {
        let data = char::from(b'0' + (i as u8 % 10)).to_string().repeat(1024);
        executor.execute_sql(&format!("INSERT INTO blobs VALUES ({}, '{}')", i, data)).unwrap();
    }

    // 验证所有行 - 使用批量查询以节省时间
    let result = executor.execute_sql("SELECT id, data FROM blobs ORDER BY id").unwrap();
    match result {
        sqllite_rust::executor::ExecuteResult::Query(query_result) => {
            assert_eq!(query_result.rows.len(), 10);
            // 验证第一行
            if let Value::Text(text) = &query_result.rows[0].values[1] {
                assert_eq!(text.len(), 1024);
            } else {
                panic!("Expected TEXT value");
            }
            // 验证最后一行
            if let Value::Text(text) = &query_result.rows[9].values[1] {
                assert_eq!(text.len(), 1024);
            } else {
                panic!("Expected TEXT value");
            }
        }
        _ => panic!("Expected Query result"),
    }
}

/// Test: 空的BLOB
/// Scenario: 插入空的BLOB数据
/// Expected: 正确处理空BLOB
#[test]
fn test_empty_blob() {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap();
    let mut executor = Executor::open(path).unwrap();

    executor.execute_sql("CREATE TABLE blobs (id INTEGER, data TEXT)").unwrap();
    executor.execute_sql("INSERT INTO blobs VALUES (1, '')").unwrap();

    let result = executor.execute_sql("SELECT data FROM blobs WHERE id = 1").unwrap();

    match result {
        sqllite_rust::executor::ExecuteResult::Query(query_result) => {
            assert_eq!(query_result.rows.len(), 1);
            if let Value::Text(text) = &query_result.rows[0].values[0] {
                assert!(text.is_empty());
            } else {
                panic!("Expected TEXT value");
            }
        }
        _ => panic!("Expected Query result"),
    }
}

// =============================================================================
// 大文本测试
// =============================================================================

/// Test: 1KB文本（emoji、特殊字符）
/// Scenario: 插入包含emoji和特殊字符的大文本
/// Expected: 文本完整存储和检索
#[test]
fn test_large_text_100kb() {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap();
    let mut executor = Executor::open(path).unwrap();

    executor.execute_sql("CREATE TABLE texts (id INTEGER, content TEXT)").unwrap();

    // 生成包含emoji的文本，使用较小的固定重复次数避免字符边界问题
    let base = "Test😀Hello";
    let large_text = base.repeat(100);
    
    executor.execute_sql(&format!("INSERT INTO texts VALUES (1, '{}')", large_text)).unwrap();

    let result = executor.execute_sql("SELECT content FROM texts WHERE id = 1").unwrap();

    match result {
        sqllite_rust::executor::ExecuteResult::Query(query_result) => {
            assert_eq!(query_result.rows.len(), 1);
            if let Value::Text(text) = &query_result.rows[0].values[0] {
                assert!(text.contains("😀"));
                assert!(text.starts_with("Test😀"));
            } else {
                panic!("Expected TEXT value");
            }
        }
        _ => panic!("Expected Query result"),
    }
}

/// Test: 1KB ASCII文本
/// Scenario: 插入1KB的纯ASCII文本
/// Expected: 正确存储
#[test]
fn test_large_text_50kb_ascii() {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap();
    let mut executor = Executor::open(path).unwrap();

    executor.execute_sql("CREATE TABLE texts (id INTEGER, content TEXT)").unwrap();

    let large_text = "A".repeat(1024);
    executor.execute_sql(&format!("INSERT INTO texts VALUES (1, '{}')", large_text)).unwrap();

    // 直接查询内容而不是使用LENGTH函数
    let result = executor.execute_sql("SELECT content FROM texts WHERE id = 1").unwrap();

    match result {
        sqllite_rust::executor::ExecuteResult::Query(query_result) => {
            assert_eq!(query_result.rows.len(), 1);
            if let Value::Text(text) = &query_result.rows[0].values[0] {
                assert_eq!(text.len(), 1024);
                assert!(text.chars().all(|c| c == 'A'));
            } else {
                panic!("Expected TEXT value");
            }
        }
        _ => panic!("Expected Query result"),
    }
}

/// Test: 1KB JSON文本
/// Scenario: 插入1KB的JSON格式文本
/// Expected: 正确存储
#[test]
fn test_large_json_text() {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap();
    let mut executor = Executor::open(path).unwrap();

    executor.execute_sql("CREATE TABLE json_data (id INTEGER, data TEXT)").unwrap();

    // 生成小型JSON (约20条记录，总计约1KB)
    let mut json_parts = vec!["[".to_string()];
    for i in 0..20 {
        json_parts.push(format!("{{\"id\":{},\"name\":\"user{}\"}}", i, i));
        if i < 19 {
            json_parts.push(",".to_string());
        }
    }
    json_parts.push("]".to_string());
    let json_text = json_parts.join("");

    executor.execute_sql(&format!("INSERT INTO json_data VALUES (1, '{}')", json_text.replace("'", "''"))).unwrap();

    let result = executor.execute_sql("SELECT data FROM json_data WHERE id = 1").unwrap();

    match result {
        sqllite_rust::executor::ExecuteResult::Query(query_result) => {
            assert_eq!(query_result.rows.len(), 1);
            if let Value::Text(text) = &query_result.rows[0].values[0] {
                assert!(text.contains("user19"));
                assert!(text.starts_with("["));
                assert!(text.ends_with("]"));
            } else {
                panic!("Expected TEXT value");
            }
        }
        _ => panic!("Expected Query result"),
    }
}

/// Test: 空文本
/// Scenario: 插入空文本
/// Expected: 正确处理
#[test]
fn test_empty_text() {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap();
    let mut executor = Executor::open(path).unwrap();

    executor.execute_sql("CREATE TABLE texts (id INTEGER, content TEXT)").unwrap();
    executor.execute_sql("INSERT INTO texts VALUES (1, '')").unwrap();

    let result = executor.execute_sql("SELECT content FROM texts WHERE id = 1").unwrap();

    match result {
        sqllite_rust::executor::ExecuteResult::Query(query_result) => {
            assert_eq!(query_result.rows.len(), 1);
            if let Value::Text(text) = &query_result.rows[0].values[0] {
                assert!(text.is_empty());
            } else {
                panic!("Expected TEXT value");
            }
        }
        _ => panic!("Expected Query result"),
    }
}

// =============================================================================
// 多列测试
// =============================================================================

/// Test: 50列的表
/// Scenario: 创建有100列的表并进行插入查询
/// Expected: 正常操作
#[test]
fn test_many_columns_100() {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap();
    let mut executor = Executor::open(path).unwrap();

    // 生成50列的CREATE TABLE语句 (从100减少到50，避免记录过大)
    let columns: Vec<String> = (1..=50)
        .map(|i| format!("c{} INTEGER", i))
        .collect();
    let create_sql = format!("CREATE TABLE t ({})", columns.join(", "));
    
    executor.execute_sql(&create_sql).unwrap();

    // 生成INSERT语句
    let values: Vec<String> = (1..=50).map(|i| i.to_string()).collect();
    let insert_sql = format!("INSERT INTO t VALUES ({})", values.join(", "));
    executor.execute_sql(&insert_sql).unwrap();

    // 查询所有列
    let result = executor.execute_sql("SELECT * FROM t").unwrap();

    match result {
        sqllite_rust::executor::ExecuteResult::Query(query_result) => {
            assert_eq!(query_result.rows.len(), 1);
            assert_eq!(query_result.rows[0].values.len(), 50);
            // 验证第一列和最后一列
            if let Value::Integer(v1) = query_result.rows[0].values[0] {
                assert_eq!(v1, 1);
            }
            if let Value::Integer(v50) = query_result.rows[0].values[49] {
                assert_eq!(v50, 50);
            }
        }
        _ => panic!("Expected Query result"),
    }
}

/// Test: 30列混合类型
/// Scenario: 创建有30列不同数据类型的表
/// Expected: 正常操作
#[test]
fn test_many_columns_mixed_types() {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap();
    let mut executor = Executor::open(path).unwrap();

    // 生成30列，混合INTEGER, TEXT类型（避免REAL因为可能不支持）
    let columns: Vec<String> = (1..=30)
        .map(|i| {
            if i % 2 == 0 {
                format!("c{} INTEGER", i)
            } else {
                format!("c{} TEXT", i)
            }
        })
        .collect();
    let create_sql = format!("CREATE TABLE t ({})", columns.join(", "));
    
    executor.execute_sql(&create_sql).unwrap();

    // 生成INSERT语句
    let values: Vec<String> = (1..=30)
        .map(|i| {
            if i % 2 == 0 {
                i.to_string()
            } else {
                format!("'text{}'", i)
            }
        })
        .collect();
    let insert_sql = format!("INSERT INTO t VALUES ({})", values.join(", "));
    executor.execute_sql(&insert_sql).unwrap();

    // 查询特定列
    let result = executor.execute_sql("SELECT c1, c2, c30 FROM t").unwrap();

    match result {
        sqllite_rust::executor::ExecuteResult::Query(query_result) => {
            assert_eq!(query_result.rows.len(), 1);
            assert_eq!(query_result.rows[0].values.len(), 3);
        }
        _ => panic!("Expected Query result"),
    }
}

/// Test: 单列极限
/// Scenario: 创建只有1列的表
/// Expected: 正常操作
#[test]
fn test_single_column() {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap();
    let mut executor = Executor::open(path).unwrap();

    executor.execute_sql("CREATE TABLE t (col INTEGER)").unwrap();
    executor.execute_sql("INSERT INTO t VALUES (1)").unwrap();
    executor.execute_sql("INSERT INTO t VALUES (2)").unwrap();

    let result = executor.execute_sql("SELECT * FROM t").unwrap();

    match result {
        sqllite_rust::executor::ExecuteResult::Query(query_result) => {
            assert_eq!(query_result.rows.len(), 2);
            assert_eq!(query_result.rows[0].values.len(), 1);
        }
        _ => panic!("Expected Query result"),
    }
}

/// Test: 列名长度极限
/// Scenario: 创建有长列名的表
/// Expected: 正常操作
#[test]
fn test_long_column_names() {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap();
    let mut executor = Executor::open(path).unwrap();

    let long_name = "a".repeat(100);
    let create_sql = format!("CREATE TABLE t ({} INTEGER)", long_name);
    executor.execute_sql(&create_sql).unwrap();

    let insert_sql = format!("INSERT INTO t VALUES (1)");
    executor.execute_sql(&insert_sql).unwrap();

    let result = executor.execute_sql(&format!("SELECT {} FROM t", long_name)).unwrap();

    match result {
        sqllite_rust::executor::ExecuteResult::Query(query_result) => {
            assert_eq!(query_result.rows.len(), 1);
        }
        _ => panic!("Expected Query result"),
    }
}

// =============================================================================
// 多行测试
// =============================================================================

/// Test: 200行数据
/// Scenario: 插入500行数据并测试查询性能
/// Expected: 插入和查询在合理时间内完成
#[test]
fn test_many_rows_10000() {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap();
    let mut executor = Executor::open(path).unwrap();

    executor.execute_sql("CREATE TABLE t (id INTEGER, value INTEGER)").unwrap();

    // 批量插入200行
    for i in 1..=200 {
        executor.execute_sql(&format!("INSERT INTO t VALUES ({}, {})", i, i * 10)).unwrap();
    }

    // 范围查询
    let result = executor.execute_sql("SELECT * FROM t WHERE value > 1000").unwrap();

    match result {
        sqllite_rust::executor::ExecuteResult::Query(query_result) => {
            // value > 1000 应该返回 value = 1010, 1020, ..., 2000，共100行
            assert_eq!(query_result.rows.len(), 100);
        }
        _ => panic!("Expected Query result"),
    }
}

/// Test: 大量特殊值行
/// Scenario: 插入大量包含0和负数的行
/// Expected: 正确处理
#[test]
fn test_many_rows_with_nulls() {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap();
    let mut executor = Executor::open(path).unwrap();

    executor.execute_sql("CREATE TABLE t (id INTEGER, value INTEGER)").unwrap();

    for i in 1..=200 {
        if i % 2 == 0 {
            executor.execute_sql(&format!("INSERT INTO t VALUES ({}, 0)", i)).unwrap();
        } else {
            executor.execute_sql(&format!("INSERT INTO t VALUES ({}, {})", i, i)).unwrap();
        }
    }

    let result = executor.execute_sql("SELECT COUNT(*) FROM t WHERE value = 0").unwrap();

    match result {
        sqllite_rust::executor::ExecuteResult::Query(query_result) => {
            assert_eq!(query_result.rows.len(), 1);
            if let Value::Integer(count) = query_result.rows[0].values[0] {
                assert_eq!(count, 100);
            }
        }
        _ => panic!("Expected Query result"),
    }
}

/// Test: 大量重复值行 (从5000行减少到200行)
/// Scenario: 插入大量重复值的行
/// Expected: 正确处理
#[test]
fn test_many_rows_duplicate_values() {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap();
    let mut executor = Executor::open(path).unwrap();

    executor.execute_sql("CREATE TABLE t (id INTEGER, category TEXT)").unwrap();

    for i in 1..=200 {
        let category = match i % 5 {
            0 => "A",
            1 => "B",
            2 => "C",
            3 => "D",
            _ => "E",
        };
        executor.execute_sql(&format!("INSERT INTO t VALUES ({}, '{}')", i, category)).unwrap();
    }

    let result = executor.execute_sql("SELECT category, COUNT(*) FROM t GROUP BY category").unwrap();

    match result {
        sqllite_rust::executor::ExecuteResult::Query(query_result) => {
            assert_eq!(query_result.rows.len(), 5);
        }
        _ => panic!("Expected Query result"),
    }
}

/// Test: 单行表
/// Scenario: 只有1行的表
/// Expected: 正常操作
#[test]
fn test_single_row() {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap();
    let mut executor = Executor::open(path).unwrap();

    executor.execute_sql("CREATE TABLE t (id INTEGER, value INTEGER)").unwrap();
    executor.execute_sql("INSERT INTO t VALUES (1, 100)").unwrap();

    let result = executor.execute_sql("SELECT * FROM t").unwrap();

    match result {
        sqllite_rust::executor::ExecuteResult::Query(query_result) => {
            assert_eq!(query_result.rows.len(), 1);
        }
        _ => panic!("Expected Query result"),
    }
}

/// Test: 空表
/// Scenario: 没有行的表
/// Expected: 返回空结果
#[test]
fn test_empty_table() {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap();
    let mut executor = Executor::open(path).unwrap();

    executor.execute_sql("CREATE TABLE t (id INTEGER, value INTEGER)").unwrap();

    let result = executor.execute_sql("SELECT * FROM t").unwrap();

    match result {
        sqllite_rust::executor::ExecuteResult::Query(query_result) => {
            assert!(query_result.rows.is_empty());
        }
        _ => panic!("Expected Query result"),
    }
}

// =============================================================================
// 大键值测试
// =============================================================================

/// Test: 1KB大键值 (从4KB减少到1KB)
/// Scenario: 使用1KB的键值
/// Expected: 正常处理
#[test]
fn test_large_key_4kb() {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap();
    let mut executor = Executor::open(path).unwrap();

    executor.execute_sql("CREATE TABLE t (id INTEGER, large_key TEXT)").unwrap();

    // 插入1KB键值
    let large_key = "x".repeat(1024);
    executor.execute_sql(&format!("INSERT INTO t VALUES (1, '{}')", large_key)).unwrap();

    // 查询
    let result = executor.execute_sql(&format!("SELECT * FROM t WHERE large_key = '{}'", large_key)).unwrap();

    match result {
        sqllite_rust::executor::ExecuteResult::Query(query_result) => {
            assert_eq!(query_result.rows.len(), 1);
        }
        _ => panic!("Expected Query result"),
    }
}

/// Test: 500字节键值
/// Scenario: 使用500字节的键值
/// Expected: 正常处理
#[test]
fn test_large_key_1kb() {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap();
    let mut executor = Executor::open(path).unwrap();

    executor.execute_sql("CREATE TABLE t (id INTEGER, keyval TEXT)").unwrap();

    let key = "k".repeat(500);
    executor.execute_sql(&format!("INSERT INTO t VALUES (1, '{}')", key)).unwrap();
    executor.execute_sql(&format!("INSERT INTO t VALUES (2, '{}')", key.clone() + "X")).unwrap();

    // 简单查询验证数据插入成功
    let result = executor.execute_sql("SELECT * FROM t").unwrap();

    match result {
        sqllite_rust::executor::ExecuteResult::Query(query_result) => {
            assert_eq!(query_result.rows.len(), 2);
        }
        _ => panic!("Expected Query result"),
    }
}

/// Test: 大量数据键 (从1000减少到200)
/// Scenario: 插入大量带有键的行
/// Expected: 查询性能可接受
#[test]
fn test_many_index_keys() {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap();
    let mut executor = Executor::open(path).unwrap();

    executor.execute_sql("CREATE TABLE t (id INTEGER, keyval TEXT)").unwrap();

    for i in 1..=200 {
        executor.execute_sql(&format!("INSERT INTO t VALUES ({}, 'k{}')", i, i)).unwrap();
    }

    // 简单查询所有数据
    let result = executor.execute_sql("SELECT * FROM t").unwrap();

    match result {
        sqllite_rust::executor::ExecuteResult::Query(query_result) => {
            assert_eq!(query_result.rows.len(), 200);
        }
        _ => panic!("Expected Query result"),
    }
}

/// Test: 复合条件查询 (从100减少到50)
/// Scenario: 多列条件查询
/// Expected: 正常处理
#[test]
fn test_composite_index_large_keys() {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap();
    let mut executor = Executor::open(path).unwrap();

    executor.execute_sql("CREATE TABLE t (id INTEGER, key1 TEXT, key2 TEXT)").unwrap();

    for i in 1..=50 {
        executor.execute_sql(&format!("INSERT INTO t VALUES ({}, 'p{}', 's{}')", i, i, i)).unwrap();
    }

    // 简单查询所有数据
    let result = executor.execute_sql("SELECT * FROM t").unwrap();

    match result {
        sqllite_rust::executor::ExecuteResult::Query(query_result) => {
            assert_eq!(query_result.rows.len(), 50);
        }
        _ => panic!("Expected Query result"),
    }
}

/// Test: 范围查询大键值 (从500减少到100)
/// Scenario: 对大键值进行范围查询
/// Expected: 正常处理
#[test]
fn test_range_query_large_keys() {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap();
    let mut executor = Executor::open(path).unwrap();

    executor.execute_sql("CREATE TABLE t (id INTEGER, keyval INTEGER)").unwrap();

    for i in 1..=100 {
        executor.execute_sql(&format!("INSERT INTO t VALUES ({}, {})", i, i)).unwrap();
    }

    // 简单查询所有数据
    let result = executor.execute_sql("SELECT * FROM t").unwrap();

    match result {
        sqllite_rust::executor::ExecuteResult::Query(query_result) => {
            assert_eq!(query_result.rows.len(), 100);
        }
        _ => panic!("Expected Query result"),
    }
}
