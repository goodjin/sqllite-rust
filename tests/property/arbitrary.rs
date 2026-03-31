//! Arbitrary 实例定义，用于 proptest

use proptest::prelude::*;
use sqllite_rust::sql::ast::*;
use sqllite_rust::storage::record::Value;

/// Value 的 Arbitrary 实现
pub fn value_strategy() -> impl Strategy<Value = Value> {
    prop_oneof![
        any::<i64>().prop_map(Value::Integer),
        any::<f64>().prop_filter("finite f64", |f| f.is_finite()).prop_map(Value::Real),
        "[a-zA-Z0-9_ ]{0,100}".prop_map(Value::Text),
        prop::collection::vec(any::<u8>(), 0..100).prop_map(Value::Blob),
        Just(Value::Null),
    ]
}

/// DataType 的 Arbitrary 实现
pub fn data_type_strategy() -> impl Strategy<Value = DataType> {
    prop_oneof![
        Just(DataType::Integer),
        Just(DataType::Text),
        Just(DataType::Blob),
    ]
}

/// BinaryOp 的 Arbitrary 实现
pub fn binary_op_strategy() -> impl Strategy<Value = BinaryOp> {
    prop_oneof![
        Just(BinaryOp::Equal),
        Just(BinaryOp::NotEqual),
        Just(BinaryOp::Less),
        Just(BinaryOp::Greater),
        Just(BinaryOp::LessEqual),
        Just(BinaryOp::GreaterEqual),
        Just(BinaryOp::And),
        Just(BinaryOp::Or),
        Just(BinaryOp::Add),
        Just(BinaryOp::Sub),
        Just(BinaryOp::Mul),
        Just(BinaryOp::Div),
    ]
}

/// Expression 的 Arbitrary 实现（限制深度）
pub fn expression_strategy(max_depth: u32) -> impl Strategy<Value = Expression> {
    let leaf = prop_oneof![
        any::<i64>().prop_map(Expression::Integer),
        any::<f64>().prop_filter("finite f64", |f| f.is_finite()).prop_map(Expression::Float),
        "[a-zA-Z_][a-zA-Z0-9_]{0,30}".prop_map(Expression::Column),
        Just(Expression::Null),
        Just(Expression::Boolean(true)),
        Just(Expression::Boolean(false)),
    ];
    
    leaf.prop_recursive(
        max_depth,
        8,
        2,
        |inner| {
            prop_oneof![
                (inner.clone(), binary_op_strategy(), inner.clone())
                    .prop_map(|(l, op, r)| Expression::Binary {
                        left: Box::new(l),
                        op,
                        right: Box::new(r),
                    }),
            ]
        }
    )
}

/// ColumnDef 的 Arbitrary 实现
pub fn column_def_strategy() -> impl Strategy<Value = ColumnDef> {
    (
        "[a-zA-Z_][a-zA-Z0-9_]{0,30}",
        data_type_strategy(),
        any::<bool>(),
        any::<bool>(),
    )
        .prop_map(|(name, data_type, nullable, primary_key)| ColumnDef {
            name,
            data_type,
            nullable,
            primary_key,
            foreign_key: None,
            default_value: None,
            is_virtual: false,
            generated_always: None,
        })
}

/// CreateTableStmt 的 Arbitrary 实现
pub fn create_table_stmt_strategy() -> impl Strategy<Value = CreateTableStmt> {
    (
        "[a-zA-Z_][a-zA-Z0-9_]{0,30}",
        prop::collection::vec(column_def_strategy(), 1..10),
    )
        .prop_map(|(table, columns)| CreateTableStmt {
            table,
            columns,
            foreign_keys: vec![],
        })
}

/// OrderBy 的 Arbitrary 实现
pub fn order_by_strategy() -> impl Strategy<Value = OrderBy> {
    ("[a-zA-Z_][a-zA-Z0-9_]{0,30}", any::<bool>())
        .prop_map(|(column, descending)| OrderBy { column, descending })
}

/// SelectColumn 的 Arbitrary 实现
pub fn select_column_strategy(max_depth: u32) -> impl Strategy<Value = SelectColumn> {
    prop_oneof![
        Just(SelectColumn::All),
        "[a-zA-Z_][a-zA-Z0-9_]{0,30}".prop_map(SelectColumn::Column),
        expression_strategy(max_depth)
            .prop_map(|e| SelectColumn::Expression(e, None)),
    ]
}
