//! 共享的 proptest 策略定义

use proptest::prelude::*;

/// 生成有效的表名
pub fn table_name_strategy() -> impl Strategy<Value = String> {
    "[a-zA-Z_][a-zA-Z0-9_]{0,63}".prop_filter(
        "reserved keywords filtered",
        |s| !is_reserved_keyword(s)
    )
}

/// 生成有效的列名
pub fn column_name_strategy() -> impl Strategy<Value = String> {
    "[a-zA-Z_][a-zA-Z0-9_]{0,63}".prop_filter(
        "reserved keywords filtered",
        |s| !is_reserved_keyword(s)
    )
}

/// 生成有效的标识符
pub fn identifier_strategy() -> impl Strategy<Value = String> {
    "[a-zA-Z_][a-zA-Z0-9_]{0,63}".prop_filter(
        "reserved keywords filtered",
        |s| !is_reserved_keyword(s)
    )
}

/// 生成SQL字符串值
pub fn sql_string_strategy() -> impl Strategy<Value = String> {
    prop::string::string_regex("[a-zA-Z0-9_ ]{0,100}").unwrap()
}

/// 生成整数范围
pub fn i64_range_strategy() -> impl Strategy<Value = i64> {
    prop::num::i64::ANY
}

/// 生成正整数
pub fn positive_i64_strategy() -> impl Strategy<Value = i64> {
    1i64..=i64::MAX
}

/// 生成非负整数
pub fn non_negative_i64_strategy() -> impl Strategy<Value = i64> {
    0i64..=i64::MAX
}

/// 生成有限的f64
pub fn finite_f64_strategy() -> impl Strategy<Value = f64> {
    prop::num::f64::NORMAL | prop::num::f64::ZERO
}

/// 生成字节数组
pub fn blob_strategy(max_size: usize) -> impl Strategy<Value = Vec<u8>> {
    prop::collection::vec(any::<u8>(), 0..=max_size)
}

/// 生成操作序列
pub fn operation_sequence<T: std::fmt::Debug>(
    op_strategy: impl Strategy<Value = T>,
    max_len: usize
) -> impl Strategy<Value = Vec<T>> {
    prop::collection::vec(op_strategy, 0..=max_len)
}

/// 检查是否为SQL保留关键字
fn is_reserved_keyword(s: &str) -> bool {
    const KEYWORDS: &[&str] = &[
        "select", "insert", "update", "delete", "create", "drop",
        "table", "index", "view", "trigger", "from", "where",
        "and", "or", "not", "null", "true", "false",
        "begin", "commit", "rollback", "transaction",
    ];
    KEYWORDS.contains(&s.to_lowercase().as_str())
}

/// 生成布尔值分布
pub fn bool_distribution() -> impl Strategy<Value = bool> {
    prop::bool::ANY
}

/// 生成可选值
pub fn option_strategy<T: std::fmt::Debug>(
    inner: impl Strategy<Value = T>
) -> impl Strategy<Value = Option<T>> {
    prop::option::of(inner)
}

/// 生成范围边界
pub fn range_bounds<T: std::fmt::Debug + Clone>(
    values: impl Strategy<Value = T> + Clone
) -> impl Strategy<Value = (Option<T>, Option<T>)> {
    (prop::option::of(values.clone()), prop::option::of(values))
}
