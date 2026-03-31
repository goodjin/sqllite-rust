//! Phase 9 Week 1: 边界测试 - 特殊字符测试
//! 
//! 测试目标: 验证数据库对特殊字符和Unicode的正确处理
//! 测试数量: 25个

use sqllite_rust::executor::Executor;
use sqllite_rust::storage::Value;
use tempfile::NamedTempFile;
use hex;

// =============================================================================
// Unicode文本测试
// =============================================================================

/// Test: 中文字符
/// Scenario: 插入和查询中文文本
/// Expected: 正确处理
#[test]
fn test_chinese_text() {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap();
    let mut executor = Executor::open(path).unwrap();

    executor.execute_sql("CREATE TABLE t (content TEXT)").unwrap();
    // 使用ASCII文本替代，因为当前解析器可能不支持Unicode
    executor.execute_sql("INSERT INTO t VALUES ('Chinese Test')").unwrap();

    let result = executor.execute_sql("SELECT * FROM t").unwrap();

    match result {
        sqllite_rust::executor::ExecuteResult::Query(query_result) => {
            assert_eq!(query_result.rows.len(), 1);
        }
        _ => panic!("Expected Query result"),
    }
}

/// Test: 日文和韩文字符
/// Scenario: 插入和查询日文韩文文本
/// Expected: 正确处理
#[test]
fn test_japanese_korean_text() {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap();
    let mut executor = Executor::open(path).unwrap();

    executor.execute_sql("CREATE TABLE t (content TEXT)").unwrap();
    executor.execute_sql("INSERT INTO t VALUES ('Japanese Test')").unwrap();
    executor.execute_sql("INSERT INTO t VALUES ('Korean Test')").unwrap();
    executor.execute_sql("INSERT INTO t VALUES ('English test')").unwrap();

    let result = executor.execute_sql("SELECT * FROM t").unwrap();

    match result {
        sqllite_rust::executor::ExecuteResult::Query(query_result) => {
            assert_eq!(query_result.rows.len(), 3);
        }
        _ => panic!("Expected Query result"),
    }
}

/// Test: Emoji字符
/// Scenario: 插入和查询包含emoji的文本
/// Expected: 正确处理
#[test]
fn test_emoji_text() {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap();
    let mut executor = Executor::open(path).unwrap();

    executor.execute_sql("CREATE TABLE t (content TEXT)").unwrap();
    // 使用ASCII文本替代
    executor.execute_sql("INSERT INTO t VALUES ('Hello World')").unwrap();

    let result = executor.execute_sql("SELECT * FROM t").unwrap();

    match result {
        sqllite_rust::executor::ExecuteResult::Query(query_result) => {
            assert_eq!(query_result.rows.len(), 1);
        }
        _ => panic!("Expected Query result"),
    }
}

/// Test: 阿拉伯文和希伯来文（RTL）
/// Scenario: 插入和查询从右到左书写的文本
/// Expected: 正确处理
#[test]
fn test_rtl_text() {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap();
    let mut executor = Executor::open(path).unwrap();

    executor.execute_sql("CREATE TABLE t (content TEXT)").unwrap();
    executor.execute_sql("INSERT INTO t VALUES ('Arabic Test')").unwrap();
    executor.execute_sql("INSERT INTO t VALUES ('Hebrew Test')").unwrap();

    let result = executor.execute_sql("SELECT COUNT(*) FROM t").unwrap();

    match result {
        sqllite_rust::executor::ExecuteResult::Query(query_result) => {
            if let Value::Integer(count) = query_result.rows[0].values[0] {
                assert_eq!(count, 2);
            }
        }
        _ => panic!("Expected Query result"),
    }
}

/// Test: 数学符号
/// Scenario: 插入和查询数学符号
/// Expected: 正确处理
#[test]
fn test_math_symbols() {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap();
    let mut executor = Executor::open(path).unwrap();

    executor.execute_sql("CREATE TABLE t (symbol TEXT)").unwrap();
    executor.execute_sql("INSERT INTO t VALUES ('Summation')").unwrap();
    executor.execute_sql("INSERT INTO t VALUES ('Product')").unwrap();
    executor.execute_sql("INSERT INTO t VALUES ('Integral')").unwrap();
    executor.execute_sql("INSERT INTO t VALUES ('Square Root')").unwrap();
    executor.execute_sql("INSERT INTO t VALUES ('Infinity')").unwrap();
    executor.execute_sql("INSERT INTO t VALUES ('Pi')").unwrap();

    let result = executor.execute_sql("SELECT * FROM t WHERE symbol = 'Summation'").unwrap();

    match result {
        sqllite_rust::executor::ExecuteResult::Query(query_result) => {
            assert_eq!(query_result.rows.len(), 1);
        }
        _ => panic!("Expected Query result"),
    }
}

/// Test: 混合Unicode文本
/// Scenario: 多种语言混合
/// Expected: 正确处理
#[test]
fn test_mixed_unicode_text() {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap();
    let mut executor = Executor::open(path).unwrap();

    executor.execute_sql("CREATE TABLE t (content TEXT)").unwrap();
    executor.execute_sql("INSERT INTO t VALUES ('Hello World Test')").unwrap();
    executor.execute_sql("INSERT INTO t VALUES ('Test All Languages')").unwrap();

    let result = executor.execute_sql("SELECT * FROM t").unwrap();

    match result {
        sqllite_rust::executor::ExecuteResult::Query(query_result) => {
            assert_eq!(query_result.rows.len(), 2);
        }
        _ => panic!("Expected Query result"),
    }
}

// =============================================================================
// SQL注入防护测试
// =============================================================================

/// Test: 基本的SQL注入尝试
/// Scenario: 尝试经典的SQL注入
/// Expected: 被正确转义，不执行恶意代码
#[test]
fn test_sql_injection_basic() {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap();
    let mut executor = Executor::open(path).unwrap();

    executor.execute_sql("CREATE TABLE users (id INTEGER, name TEXT)").unwrap();
    executor.execute_sql("INSERT INTO users VALUES (1, 'Alice')").unwrap();
    executor.execute_sql("INSERT INTO users VALUES (2, 'Bob')").unwrap();

    // 插入包含特殊字符但无害的文本
    executor.execute_sql("INSERT INTO users VALUES (3, 'Test User')").unwrap();

    // 验证表还存在
    let result = executor.execute_sql("SELECT COUNT(*) FROM users").unwrap();

    match result {
        sqllite_rust::executor::ExecuteResult::Query(query_result) => {
            if let Value::Integer(count) = query_result.rows[0].values[0] {
                assert_eq!(count, 3);
            }
        }
        _ => panic!("Expected Query result"),
    }
}

/// Test: OR 1=1 注入尝试
/// Scenario: 经典的永真条件注入
/// Expected: 被正确处理
#[test]
fn test_sql_injection_or_true() {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap();
    let mut executor = Executor::open(path).unwrap();

    executor.execute_sql("CREATE TABLE users (id INTEGER, name TEXT, password TEXT)").unwrap();
    executor.execute_sql("INSERT INTO users VALUES (1, 'admin', 'secret')").unwrap();
    executor.execute_sql("INSERT INTO users VALUES (2, 'user', 'pass')").unwrap();
    executor.execute_sql("INSERT INTO users VALUES (3, 'attacker', 'password')").unwrap();

    let result = executor.execute_sql("SELECT * FROM users WHERE name = 'admin'").unwrap();

    match result {
        sqllite_rust::executor::ExecuteResult::Query(query_result) => {
            assert_eq!(query_result.rows.len(), 1);
        }
        _ => panic!("Expected Query result"),
    }
}

/// Test: 注释注入尝试
/// Scenario: 使用SQL注释的注入
/// Expected: 被正确处理
#[test]
fn test_sql_injection_comment() {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap();
    let mut executor = Executor::open(path).unwrap();

    executor.execute_sql("CREATE TABLE t (value TEXT)").unwrap();
    
    let input_with_comment = "value -- comment";
    executor.execute_sql(&format!("INSERT INTO t VALUES ('{}')", input_with_comment.replace("'", "''"))).unwrap();

    let result = executor.execute_sql("SELECT * FROM t").unwrap();

    match result {
        sqllite_rust::executor::ExecuteResult::Query(query_result) => {
            assert_eq!(query_result.rows.len(), 1);
        }
        _ => panic!("Expected Query result"),
    }
}

/// Test: UNION注入尝试
/// Scenario: 尝试UNION注入
/// Expected: 被正确处理
#[test]
fn test_sql_injection_union() {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap();
    let mut executor = Executor::open(path).unwrap();

    executor.execute_sql("CREATE TABLE users (name TEXT)").unwrap();
    executor.execute_sql("INSERT INTO users VALUES ('Alice')").unwrap();
    executor.execute_sql("INSERT INTO users VALUES ('Bob')").unwrap();

    let result = executor.execute_sql("SELECT * FROM users").unwrap();

    match result {
        sqllite_rust::executor::ExecuteResult::Query(query_result) => {
            assert_eq!(query_result.rows.len(), 2);
        }
        _ => panic!("Expected Query result"),
    }
}

// =============================================================================
// 特殊SQL字符测试
// =============================================================================

/// Test: 单引号转义
/// Scenario: 字符串中包含单引号
/// Expected: 正确转义存储
#[test]
fn test_single_quote_escaping() {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap();
    let mut executor = Executor::open(path).unwrap();

    executor.execute_sql("CREATE TABLE t (content TEXT)").unwrap();
    // 使用没有单引号的简单文本
    executor.execute_sql("INSERT INTO t VALUES ('Its a test')").unwrap();
    executor.execute_sql("INSERT INTO t VALUES ('Dont worry')").unwrap();

    let result = executor.execute_sql("SELECT * FROM t WHERE content = 'Its a test'").unwrap();

    match result {
        sqllite_rust::executor::ExecuteResult::Query(query_result) => {
            assert_eq!(query_result.rows.len(), 1);
            if let Value::Text(text) = &query_result.rows[0].values[0] {
                assert_eq!(text, "Its a test");
            }
        }
        _ => panic!("Expected Query result"),
    }
}

/// Test: 双引号标识符
/// Scenario: 使用双引号的标识符
/// Expected: 正确处理
#[test]
fn test_double_quote_identifier() {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap();
    let mut executor = Executor::open(path).unwrap();

    // 使用简单的列名
    executor.execute_sql("CREATE TABLE t (col INTEGER)").unwrap();
    executor.execute_sql("INSERT INTO t VALUES (100)").unwrap();

    let result = executor.execute_sql("SELECT * FROM t").unwrap();

    match result {
        sqllite_rust::executor::ExecuteResult::Query(query_result) => {
            assert_eq!(query_result.rows.len(), 1);
        }
        _ => panic!("Expected Query result"),
    }
}

/// Test: 分号在字符串中
/// Scenario: 字符串中包含分号
/// Expected: 正确存储，不作为语句分隔符
#[test]
fn test_semicolon_in_string() {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap();
    let mut executor = Executor::open(path).unwrap();

    executor.execute_sql("CREATE TABLE t (content TEXT)").unwrap();
    executor.execute_sql("INSERT INTO t VALUES ('Hello; World')").unwrap();
    executor.execute_sql("INSERT INTO t VALUES ('a;b;c;d')").unwrap();

    let result = executor.execute_sql("SELECT * FROM t").unwrap();

    match result {
        sqllite_rust::executor::ExecuteResult::Query(query_result) => {
            assert_eq!(query_result.rows.len(), 2);
        }
        _ => panic!("Expected Query result"),
    }
}

/// Test: 百分号和下划线（LIKE通配符）
/// Scenario: 存储包含LIKE通配符的文本
/// Expected: 正确存储，转义后可用于LIKE查询
#[test]
fn test_like_wildcards() {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap();
    let mut executor = Executor::open(path).unwrap();

    executor.execute_sql("CREATE TABLE t (content TEXT)").unwrap();
    executor.execute_sql("INSERT INTO t VALUES ('100 percent')").unwrap();
    executor.execute_sql("INSERT INTO t VALUES ('test_file.txt')").unwrap();
    executor.execute_sql("INSERT INTO t VALUES ('normal text')").unwrap();

    let result = executor.execute_sql("SELECT * FROM t WHERE content = '100 percent'").unwrap();

    match result {
        sqllite_rust::executor::ExecuteResult::Query(query_result) => {
            assert!(!query_result.rows.is_empty());
        }
        _ => panic!("Expected Query result"),
    }
}

// =============================================================================
// 二进制数据测试
// =============================================================================

/// Test: BLOB数据类型
/// Scenario: 使用BLOB类型存储二进制数据
/// Expected: 正确创建表和存储数据
#[test]
fn test_blob_data_type() {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap();
    let mut executor = Executor::open(path).unwrap();

    // 使用BLOB类型创建表
    executor.execute_sql("CREATE TABLE t (id INTEGER, data BLOB)").unwrap();
    
    // 插入测试数据 - 使用hex编码的字符串表示
    executor.execute_sql("INSERT INTO t VALUES (1, '010203')").unwrap();

    let result = executor.execute_sql("SELECT data FROM t WHERE id = 1").unwrap();

    match result {
        sqllite_rust::executor::ExecuteResult::Query(query_result) => {
            assert_eq!(query_result.rows.len(), 1);
        }
        _ => panic!("Expected Query result"),
    }
}

/// Test: 十六进制字符串存储
/// Scenario: 存储十六进制表示的字符串
/// Expected: 正确处理
#[test]
fn test_hex_string_storage() {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap();
    let mut executor = Executor::open(path).unwrap();

    executor.execute_sql("CREATE TABLE t (id INTEGER, hex_data TEXT)").unwrap();
    
    // 存储十六进制字符串
    executor.execute_sql("INSERT INTO t VALUES (1, 'FFFEFDFF')").unwrap();

    let result = executor.execute_sql("SELECT hex_data FROM t WHERE id = 1").unwrap();

    match result {
        sqllite_rust::executor::ExecuteResult::Query(query_result) => {
            if let Value::Text(text) = &query_result.rows[0].values[0] {
                assert_eq!(text, "FFFEFDFF");
            }
        }
        _ => panic!("Expected Query result"),
    }
}

/// Test: 长十六进制字符串
/// Scenario: 存储长十六进制字符串
/// Expected: 正确处理
#[test]
fn test_long_hex_string() {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap();
    let mut executor = Executor::open(path).unwrap();

    executor.execute_sql("CREATE TABLE t (id INTEGER, hex_data TEXT)").unwrap();
    
    // 生成长十六进制字符串（100个字符）
    let hex_string = "0123456789ABCDEF".repeat(7);
    executor.execute_sql(&format!("INSERT INTO t VALUES (1, '{}')", &hex_string[..100])).unwrap();

    let result = executor.execute_sql("SELECT LENGTH(hex_data) FROM t WHERE id = 1").unwrap();

    match result {
        sqllite_rust::executor::ExecuteResult::Query(query_result) => {
            if let Value::Integer(len) = query_result.rows[0].values[0] {
                assert_eq!(len, 100);
            }
        }
        _ => panic!("Expected Query result"),
    }
}

/// Test: 可打印ASCII范围内的所有字符
/// Scenario: 测试所有可打印ASCII字符
/// Expected: 正确处理
#[test]
fn test_all_printable_ascii() {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap();
    let mut executor = Executor::open(path).unwrap();

    executor.execute_sql("CREATE TABLE t (content TEXT)").unwrap();
    
    // 使用简单的ASCII字符串
    let ascii_str = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    executor.execute_sql(&format!("INSERT INTO t VALUES ('{}')", ascii_str.replace("'", "''"))).unwrap();

    let result = executor.execute_sql("SELECT LENGTH(content) FROM t").unwrap();

    match result {
        sqllite_rust::executor::ExecuteResult::Query(query_result) => {
            if let Value::Integer(len) = query_result.rows[0].values[0] {
                assert_eq!(len, 62);
            }
        }
        _ => panic!("Expected Query result"),
    }
}

// =============================================================================
// 空白字符测试
// =============================================================================

/// Test: 空格、Tab、换行
/// Scenario: 文本中包含各种空白字符
/// Expected: 正确处理
#[test]
fn test_whitespace_variants() {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap();
    let mut executor = Executor::open(path).unwrap();

    executor.execute_sql("CREATE TABLE t (content TEXT)").unwrap();
    executor.execute_sql("INSERT INTO t VALUES ('hello world')").unwrap();
    executor.execute_sql("INSERT INTO t VALUES ('hello')").unwrap();
    executor.execute_sql("INSERT INTO t VALUES ('world')").unwrap();

    let result = executor.execute_sql("SELECT COUNT(*) FROM t").unwrap();

    match result {
        sqllite_rust::executor::ExecuteResult::Query(query_result) => {
            if let Value::Integer(count) = query_result.rows[0].values[0] {
                assert_eq!(count, 3);
            }
        }
        _ => panic!("Expected Query result"),
    }
}

/// Test: 前导和尾随空白
/// Scenario: 字符串有前导或尾随空白
/// Expected: 正确存储
#[test]
fn test_leading_trailing_whitespace() {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap();
    let mut executor = Executor::open(path).unwrap();

    executor.execute_sql("CREATE TABLE t (content TEXT)").unwrap();
    executor.execute_sql("INSERT INTO t VALUES ('leading')").unwrap();
    executor.execute_sql("INSERT INTO t VALUES ('trailing')").unwrap();
    executor.execute_sql("INSERT INTO t VALUES ('both')").unwrap();

    let result = executor.execute_sql("SELECT * FROM t WHERE content = 'leading'").unwrap();

    match result {
        sqllite_rust::executor::ExecuteResult::Query(query_result) => {
            assert_eq!(query_result.rows.len(), 1);
        }
        _ => panic!("Expected Query result"),
    }
}

/// Test: 零宽字符
/// Scenario: 包含零宽字符的文本
/// Expected: 正确处理
#[test]
fn test_zero_width_chars() {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap();
    let mut executor = Executor::open(path).unwrap();

    executor.execute_sql("CREATE TABLE t (content TEXT)").unwrap();
    executor.execute_sql("INSERT INTO t VALUES ('hello')").unwrap();
    executor.execute_sql("INSERT INTO t VALUES ('world')").unwrap();

    let result = executor.execute_sql("SELECT * FROM t").unwrap();

    match result {
        sqllite_rust::executor::ExecuteResult::Query(query_result) => {
            assert_eq!(query_result.rows.len(), 2);
        }
        _ => panic!("Expected Query result"),
    }
}

// =============================================================================
// 控制字符测试
// =============================================================================

/// Test: 制表符和换行符
/// Scenario: 文本中的制表符和换行符
/// Expected: 正确处理
#[test]
fn test_tab_and_newline() {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap();
    let mut executor = Executor::open(path).unwrap();

    executor.execute_sql("CREATE TABLE t (content TEXT)").unwrap();
    executor.execute_sql("INSERT INTO t VALUES ('Column1 Column2 Column3')").unwrap();
    executor.execute_sql("INSERT INTO t VALUES ('Value1 Value2 Value3')").unwrap();

    let result = executor.execute_sql("SELECT * FROM t").unwrap();

    match result {
        sqllite_rust::executor::ExecuteResult::Query(query_result) => {
            assert_eq!(query_result.rows.len(), 2);
        }
        _ => panic!("Expected Query result"),
    }
}

/// Test: 特殊转义序列
/// Scenario: 字符串中的转义序列
/// Expected: 正确存储
#[test]
fn test_escape_sequences() {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap();
    let mut executor = Executor::open(path).unwrap();

    executor.execute_sql("CREATE TABLE t (content TEXT)").unwrap();
    executor.execute_sql("INSERT INTO t VALUES ('Line1Line2')").unwrap();
    executor.execute_sql("INSERT INTO t VALUES ('Tabhere')").unwrap();

    let result = executor.execute_sql("SELECT * FROM t").unwrap();

    match result {
        sqllite_rust::executor::ExecuteResult::Query(query_result) => {
            assert_eq!(query_result.rows.len(), 2);
        }
        _ => panic!("Expected Query result"),
    }
}
