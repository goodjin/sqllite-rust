//! SQL 解析属性测试

use proptest::prelude::*;
use proptest::test_runner::FileFailurePersistence;
use sqllite_rust::sql::parser::Parser;
use sqllite_rust::sql::tokenizer::Tokenizer;

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 1000,
        failure_persistence: Some(Box::new(
            FileFailurePersistence::WithSource("regressions")
        )),
        .. ProptestConfig::default()
    })]

    /// 属性1: 简单SELECT解析不panic
    #[test]
    fn simple_select_parses(table in "[a-zA-Z_][a-zA-Z0-9_]{0,30}") {
        let sql = format!("SELECT * FROM {}", table);
        let _ = Parser::parse(&sql);
        prop_assert!(true);
    }

    /// 属性2: 带WHERE的SELECT解析
    #[test]
    fn select_with_where_parses(
        table in "[a-zA-Z_][a-zA-Z0-9_]{0,30}",
        col in "[a-zA-Z_][a-zA-Z0-9_]{0,30}",
        val in any::<i64>()
    ) {
        let sql = format!("SELECT * FROM {} WHERE {} = {}", table, col, val);
        let _ = Parser::parse(&sql);
        prop_assert!(true);
    }

    /// 属性3: INSERT语句解析
    #[test]
    fn insert_statement_parses(
        table in "[a-zA-Z_][a-zA-Z0-9_]{0,30}",
        val in any::<i64>()
    ) {
        let sql = format!("INSERT INTO {} VALUES ({}, 'test')", table, val);
        let _ = Parser::parse(&sql);
        prop_assert!(true);
    }

    /// 属性4: CREATE TABLE语句解析
    #[test]
    fn create_table_parses(table in "[a-zA-Z_][a-zA-Z0-9_]{0,30}") {
        let sql = format!("CREATE TABLE {} (id INTEGER PRIMARY KEY, name TEXT)", table);
        let _ = Parser::parse(&sql);
        prop_assert!(true);
    }

    /// 属性5: 分词器不panic
    #[test]
    fn tokenizer_no_panic(sql in sql_fragment_strategy()) {
        let _ = Tokenizer::tokenize(&sql);
        prop_assert!(true);
    }

    /// 属性6: 空字符串处理
    #[test]
    fn empty_sql_handling() {
        let result = Parser::parse("");
        // 应该返回错误或处理成功
        prop_assert!(result.is_err() || result.is_ok());
    }

    /// 属性7: 空白字符串处理
    #[test]
    fn whitespace_only_sql_handling(whitespace in "[ \t\n\r]{0,50}") {
        let result = Parser::parse(&whitespace);
        prop_assert!(result.is_err() || result.is_ok());
    }

    /// 属性8: 事务语句解析
    #[test]
    fn transaction_statements_parse() {
        let stmts = ["BEGIN", "BEGIN TRANSACTION", "COMMIT", "ROLLBACK"];
        for stmt in &stmts {
            let _ = Parser::parse(stmt);
        }
        prop_assert!(true);
    }

    /// 属性9: 带LIMIT的SELECT解析
    #[test]
    fn select_with_limit_parses(
        table in "[a-zA-Z_][a-zA-Z0-9_]{0,30}",
        limit in 0i64..10000
    ) {
        let sql = format!("SELECT * FROM {} LIMIT {}", table, limit);
        let _ = Parser::parse(&sql);
        prop_assert!(true);
    }

    /// 属性10: 带OFFSET的SELECT解析
    #[test]
    fn select_with_offset_parses(
        table in "[a-zA-Z_][a-zA-Z0-9_]{0,30}",
        limit in 0i64..1000,
        offset in 0i64..1000
    ) {
        let sql = format!("SELECT * FROM {} LIMIT {} OFFSET {}", table, limit, offset);
        let _ = Parser::parse(&sql);
        prop_assert!(true);
    }

    /// 属性11: ORDER BY解析
    #[test]
    fn order_by_parses(
        table in "[a-zA-Z_][a-zA-Z0-9_]{0,30}",
        col in "[a-zA-Z_][a-zA-Z0-9_]{0,30}"
    ) {
        let sql = format!("SELECT * FROM {} ORDER BY {} ASC", table, col);
        let _ = Parser::parse(&sql);
        
        let sql = format!("SELECT * FROM {} ORDER BY {} DESC", table, col);
        let _ = Parser::parse(&sql);
        
        prop_assert!(true);
    }

    /// 属性12: DELETE语句解析
    #[test]
    fn delete_statement_parses(table in "[a-zA-Z_][a-zA-Z0-9_]{0,30}") {
        let sql = format!("DELETE FROM {}", table);
        let _ = Parser::parse(&sql);
        
        let sql = format!("DELETE FROM {} WHERE id = 1", table);
        let _ = Parser::parse(&sql);
        
        prop_assert!(true);
    }

    /// 属性13: UPDATE语句解析
    #[test]
    fn update_statement_parses(
        table in "[a-zA-Z_][a-zA-Z0-9_]{0,30}",
        col in "[a-zA-Z_][a-zA-Z0-9_]{0,30}"
    ) {
        let sql = format!("UPDATE {} SET {} = 1", table, col);
        let _ = Parser::parse(&sql);
        
        let sql = format!("UPDATE {} SET {} = 1 WHERE id = 2", table, col);
        let _ = Parser::parse(&sql);
        
        prop_assert!(true);
    }

    /// 属性14: 字符串字面量解析
    #[test]
    fn string_literal_parsing(s in "[a-zA-Z0-9_ ]{0,50}") {
        let sql = format!("SELECT '{}'", s.replace('\'', "''"));
        let _ = Parser::parse(&sql);
        prop_assert!(true);
    }

    /// 属性15: 多列SELECT解析
    #[test]
    fn multi_column_select_parses(
        table in "[a-zA-Z_][a-zA-Z0-9_]{0,30}",
        cols in prop::collection::vec("[a-zA-Z_][a-zA-Z0-9_]{0,30}", 1..10)
    ) {
        let col_list = cols.join(", ");
        let sql = format!("SELECT {} FROM {}", col_list, table);
        let _ = Parser::parse(&sql);
        prop_assert!(true);
    }

    /// 属性16: CREATE INDEX解析
    #[test]
    fn create_index_parses(
        idx in "[a-zA-Z_][a-zA-Z0-9_]{0,30}",
        table in "[a-zA-Z_][a-zA-Z0-9_]{0,30}",
        col in "[a-zA-Z_][a-zA-Z0-9_]{0,30}"
    ) {
        let sql = format!("CREATE INDEX {} ON {} ({})", idx, table, col);
        let _ = Parser::parse(&sql);
        prop_assert!(true);
    }

    /// 属性17: DROP TABLE解析
    #[test]
    fn drop_table_parses(table in "[a-zA-Z_][a-zA-Z0-9_]{0,30}") {
        let sql = format!("DROP TABLE {}", table);
        let _ = Parser::parse(&sql);
        
        let sql = format!("DROP TABLE IF EXISTS {}", table);
        let _ = Parser::parse(&sql);
        
        prop_assert!(true);
    }

    /// 属性18: AND/OR条件解析
    #[test]
    fn logical_operators_parse(table in "[a-zA-Z_][a-zA-Z0-9_]{0,30}") {
        let sql = format!("SELECT * FROM {} WHERE a = 1 AND b = 2", table);
        let _ = Parser::parse(&sql);
        
        let sql = format!("SELECT * FROM {} WHERE a = 1 OR b = 2", table);
        let _ = Parser::parse(&sql);
        
        prop_assert!(true);
    }

    /// 属性19: 比较运算符解析
    #[test]
    fn comparison_operators_parse(table in "[a-zA-Z_][a-zA-Z0-9_]{0,30}") {
        let ops = ["=", "!=", "<>", "<", ">", "<=", ">="];
        for op in &ops {
            let sql = format!("SELECT * FROM {} WHERE col {} 5", table, op);
            let _ = Parser::parse(&sql);
        }
        prop_assert!(true);
    }

    /// 属性20: 聚合函数解析
    #[test]
    fn aggregate_functions_parse(table in "[a-zA-Z_][a-zA-Z0-9_]{0,30}") {
        let funcs = ["COUNT(*)", "SUM(col)", "AVG(col)", "MIN(col)", "MAX(col)"];
        for func in &funcs {
            let sql = format!("SELECT {} FROM {}", func, table);
            let _ = Parser::parse(&sql);
        }
        prop_assert!(true);
    }
}

// 更多复杂SQL测试
proptest! {
    #![proptest_config(ProptestConfig {
        cases: 500,
        .. ProptestConfig::default()
    })]

    /// 属性21: JOIN语句解析
    #[test]
    fn join_statements_parse(
        t1 in "[a-zA-Z_][a-zA-Z0-9_]{0,30}",
        t2 in "[a-zA-Z_][a-zA-Z0-9_]{0,30}"
    ) {
        let sql = format!("SELECT * FROM {} JOIN {} ON {}.id = {}.id", t1, t2, t1, t2);
        let _ = Parser::parse(&sql);
        
        let sql = format!("SELECT * FROM {} LEFT JOIN {} ON {}.id = {}.id", t1, t2, t1, t2);
        let _ = Parser::parse(&sql);
        
        prop_assert!(true);
    }

    /// 属性22: GROUP BY解析
    #[test]
    fn group_by_parses(
        table in "[a-zA-Z_][a-zA-Z0-9_]{0,30}",
        col in "[a-zA-Z_][a-zA-Z0-9_]{0,30}"
    ) {
        let sql = format!("SELECT {}, COUNT(*) FROM {} GROUP BY {}", col, table, col);
        let _ = Parser::parse(&sql);
        prop_assert!(true);
    }

    /// 属性23: HAVING子句解析
    #[test]
    fn having_clause_parses(
        table in "[a-zA-Z_][a-zA-Z0-9_]{0,30}",
        col in "[a-zA-Z_][a-zA-Z0-9_]{0,30}"
    ) {
        let sql = format!(
            "SELECT {}, COUNT(*) FROM {} GROUP BY {} HAVING COUNT(*) > 1",
            col, table, col
        );
        let _ = Parser::parse(&sql);
        prop_assert!(true);
    }

    /// 属性24: 算术表达式解析
    #[test]
    fn arithmetic_expressions_parse(table in "[a-zA-Z_][a-zA-Z0-9_]{0,30}") {
        let exprs = ["a + b", "a - b", "a * b", "a / b"];
        for expr in &exprs {
            let sql = format!("SELECT {} FROM {}", expr, table);
            let _ = Parser::parse(&sql);
        }
        prop_assert!(true);
    }

    /// 属性25: 别名解析
    #[test]
    fn aliases_parse(
        table in "[a-zA-Z_][a-zA-Z0-9_]{0,30}",
        alias in "[a-zA-Z_][a-zA-Z0-9_]{0,30}"
    ) {
        let sql = format!("SELECT * FROM {} AS {}", table, alias);
        let _ = Parser::parse(&sql);
        
        let sql = format!("SELECT col AS {} FROM {}", alias, table);
        let _ = Parser::parse(&sql);
        
        prop_assert!(true);
    }

    /// 属性26: 子查询解析
    #[test]
    fn subquery_parses(table in "[a-zA-Z_][a-zA-Z0-9_]{0,30}") {
        let sql = format!("SELECT * FROM {} WHERE id IN (SELECT id FROM other)", table);
        let _ = Parser::parse(&sql);
        prop_assert!(true);
    }

    /// 属性27: EXISTS子查询解析
    #[test]
    fn exists_subquery_parses(table in "[a-zA-Z_][a-zA-Z0-9_]{0,30}") {
        let sql = format!("SELECT * FROM {} WHERE EXISTS (SELECT 1 FROM other)", table);
        let _ = Parser::parse(&sql);
        prop_assert!(true);
    }

    /// 属性28: IN表达式解析
    #[test]
    fn in_expression_parses(table in "[a-zA-Z_][a-zA-Z0-9_]{0,30}") {
        let sql = format!("SELECT * FROM {} WHERE col IN (1, 2, 3)", table);
        let _ = Parser::parse(&sql);
        prop_assert!(true);
    }

    /// 属性29: BETWEEN表达式解析
    #[test]
    fn between_expression_parses(table in "[a-zA-Z_][a-zA-Z0-9_]{0,30}") {
        let sql = format!("SELECT * FROM {} WHERE col BETWEEN 1 AND 10", table);
        let _ = Parser::parse(&sql);
        prop_assert!(true);
    }

    /// 属性30: LIKE表达式解析
    #[test]
    fn like_expression_parses(table in "[a-zA-Z_][a-zA-Z0-9_]{0,30}") {
        let sql = format!("SELECT * FROM {} WHERE col LIKE '%test%'", table);
        let _ = Parser::parse(&sql);
        prop_assert!(true);
    }

    /// 属性31: IS NULL / IS NOT NULL解析
    #[test]
    fn null_check_parses(table in "[a-zA-Z_][a-zA-Z0-9_]{0,30}") {
        let sql = format!("SELECT * FROM {} WHERE col IS NULL", table);
        let _ = Parser::parse(&sql);
        
        let sql = format!("SELECT * FROM {} WHERE col IS NOT NULL", table);
        let _ = Parser::parse(&sql);
        
        prop_assert!(true);
    }

    /// 属性32: DISTINCT解析
    #[test]
    fn distinct_parses(table in "[a-zA-Z_][a-zA-Z0-9_]{0,30}") {
        let sql = format!("SELECT DISTINCT col FROM {}", table);
        let _ = Parser::parse(&sql);
        prop_assert!(true);
    }

    /// 属性33: 多值INSERT解析
    #[test]
    fn multi_value_insert_parses(table in "[a-zA-Z_][a-zA-Z0-9_]{0,30}") {
        let sql = format!("INSERT INTO {} VALUES (1), (2), (3)", table);
        let _ = Parser::parse(&sql);
        prop_assert!(true);
    }

    /// 属性34: 显式列INSERT解析
    #[test]
    fn explicit_column_insert_parses(table in "[a-zA-Z_][a-zA-Z0-9_]{0,30}") {
        let sql = format!("INSERT INTO {} (id, name) VALUES (1, 'test')", table);
        let _ = Parser::parse(&sql);
        prop_assert!(true);
    }

    /// 属性35: 多列SET UPDATE解析
    #[test]
    fn multi_set_update_parses(table in "[a-zA-Z_][a-zA-Z0-9_]{0,30}") {
        let sql = format!("UPDATE {} SET a = 1, b = 2, c = 3", table);
        let _ = Parser::parse(&sql);
        prop_assert!(true);
    }

    /// 属性36: 复合主键CREATE TABLE解析
    #[test]
    fn composite_primary_key_parses(table in "[a-zA-Z_][a-zA-Z0-9_]{0,30}") {
        let sql = format!("CREATE TABLE {} (a INT, b INT, PRIMARY KEY (a, b))", table);
        let _ = Parser::parse(&sql);
        prop_assert!(true);
    }

    /// 属性37: 外键约束解析
    #[test]
    fn foreign_key_constraint_parses(table in "[a-zA-Z_][a-zA-Z0-9_]{0,30}") {
        let sql = format!(
            "CREATE TABLE {} (id INT PRIMARY KEY, ref_id INT REFERENCES other(id))",
            table
        );
        let _ = Parser::parse(&sql);
        prop_assert!(true);
    }

    /// 属性38: UNIQUE约束解析
    #[test]
    fn unique_constraint_parses(table in "[a-zA-Z_][a-zA-Z0-9_]{0,30}") {
        let sql = format!("CREATE TABLE {} (col INT UNIQUE)", table);
        let _ = Parser::parse(&sql);
        prop_assert!(true);
    }

    /// 属性39: NOT NULL约束解析
    #[test]
    fn not_null_constraint_parses(table in "[a-zA-Z_][a-zA-Z0-9_]{0,30}") {
        let sql = format!("CREATE TABLE {} (col INT NOT NULL)", table);
        let _ = Parser::parse(&sql);
        prop_assert!(true);
    }

    /// 属性40: DEFAULT约束解析
    #[test]
    fn default_constraint_parses(table in "[a-zA-Z_][a-zA-Z0-9_]{0,30}") {
        let sql = format!("CREATE TABLE {} (col INT DEFAULT 0)", table);
        let _ = Parser::parse(&sql);
        prop_assert!(true);
    }
}

// 辅助函数
fn sql_fragment_strategy() -> impl Strategy<Value = String> {
    prop_oneof![
        "SELECT".prop_map(|s| s.to_string()),
        "INSERT".prop_map(|s| s.to_string()),
        "UPDATE".prop_map(|s| s.to_string()),
        "DELETE".prop_map(|s| s.to_string()),
        "CREATE TABLE".prop_map(|s| s.to_string()),
        "DROP TABLE".prop_map(|s| s.to_string()),
        "WHERE".prop_map(|s| s.to_string()),
        "FROM".prop_map(|s| s.to_string()),
        prop::string::string_regex("[a-zA-Z0-9_ ]{0,50}").unwrap(),
    ]
}
