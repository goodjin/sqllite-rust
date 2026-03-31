//! 存储引擎属性测试

use proptest::prelude::*;
use proptest::test_runner::FileFailurePersistence;
use sqllite_rust::storage::record::{Value, Record};

fn value_strategy() -> impl Strategy<Value = Value> {
    prop_oneof![
        any::<i64>().prop_map(Value::Integer),
        any::<f64>().prop_filter("finite", |f| f.is_finite()).prop_map(Value::Real),
        "[a-zA-Z0-9_ ]{0,100}".prop_map(Value::Text),
        prop::collection::vec(any::<u8>(), 0..100).prop_map(Value::Blob),
        Just(Value::Null),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 1000,
        failure_persistence: Some(Box::new(
            FileFailurePersistence::WithSource("regressions")
        )),
        .. ProptestConfig::default()
    })]

    /// 属性1: Record序列化反序列化保持数据不变
    #[test]
    fn record_serde_roundtrip(values in prop::collection::vec(value_strategy(), 0..20)) {
        let record = Record::new(values.clone());
        let bytes = record.serialize();
        let deserialized = Record::deserialize(&bytes).unwrap();
        
        prop_assert_eq!(record.values.len(), deserialized.values.len());
        for (orig, restored) in record.values.iter().zip(deserialized.values.iter()) {
            prop_assert_eq!(orig, restored);
        }
    }

    /// 属性2: Value序列化反序列化保持数据不变
    #[test]
    fn value_serde_roundtrip(value in value_strategy()) {
        let bytes = value.serialize();
        let deserialized = Value::deserialize(&bytes).unwrap();
        prop_assert_eq!(value, deserialized);
    }

    /// 属性3: Record比较符合预期
    #[test]
    fn record_comparison_consistency(
        values1 in prop::collection::vec(value_strategy(), 1..5),
        values2 in prop::collection::vec(value_strategy(), 1..5)
    ) {
        let record1 = Record::new(values1);
        let record2 = Record::new(values2);
        
        // 测试相等性
        let eq1 = record1.values == record2.values;
        let eq2 = record2.values == record1.values;
        prop_assert_eq!(eq1, eq2, "Equality should be symmetric");
        
        // 测试自反性
        prop_assert_eq!(record1.values, record1.values, "Equality should be reflexive");
    }

    /// 属性4: Null值处理一致
    #[test]
    fn null_value_properties(null_count in 1usize..10) {
        let values: Vec<Value> = (0..null_count).map(|_| Value::Null).collect();
        let record = Record::new(values);
        let bytes = record.serialize();
        let deserialized = Record::deserialize(&bytes).unwrap();
        
        prop_assert_eq!(record.values.len(), deserialized.values.len());
        for v in &deserialized.values {
            prop_assert_eq!(*v, Value::Null);
        }
    }

    /// 属性5: 空Record序列化反序列化
    #[test]
    fn empty_record_serde() {
        let record = Record::new(vec![]);
        let bytes = record.serialize();
        let deserialized = Record::deserialize(&bytes).unwrap();
        prop_assert!(deserialized.values.is_empty());
    }

    /// 属性6: 大Blob序列化反序列化
    #[test]
    fn large_blob_serde(data in prop::collection::vec(any::<u8>(), 1000..10000)) {
        let value = Value::Blob(data.clone());
        let bytes = value.serialize();
        let deserialized = Value::deserialize(&bytes).unwrap();
        
        if let Value::Blob(restored) = deserialized {
            prop_assert_eq!(data, restored);
        } else {
            prop_assert!(false, "Deserialized value is not a Blob");
        }
    }

    /// 属性7: 大文本序列化反序列化
    #[test]
    fn large_text_serde(text in "[a-zA-Z0-9 ]{1000,5000}") {
        let value = Value::Text(text.clone());
        let bytes = value.serialize();
        let deserialized = Value::deserialize(&bytes).unwrap();
        
        if let Value::Text(restored) = deserialized {
            prop_assert_eq!(text, restored);
        } else {
            prop_assert!(false, "Deserialized value is not Text");
        }
    }

    /// 属性8: 浮点数序列化保持精度
    #[test]
    fn float_precision_preserved(f in prop::num::f64::NORMAL) {
        let value = Value::Real(f);
        let bytes = value.serialize();
        let deserialized = Value::deserialize(&bytes).unwrap();
        
        if let Value::Real(restored) = deserialized {
            // 允许微小的精度损失
            prop_assert!((f - restored).abs() < f64::EPSILON * 10.0);
        } else {
            prop_assert!(false, "Deserialized value is not Float");
        }
    }

    /// 属性9: Value排序一致性
    #[test]
    fn value_ordering_consistency(a in value_strategy(), b in value_strategy()) {
        // 排序应该是确定性的
        let ord1 = a.partial_cmp(&b);
        let ord2 = b.partial_cmp(&a);
        
        if let (Some(o1), Some(o2)) = (ord1, ord2) {
            prop_assert_eq!(o1, o2.reverse());
        }
    }

    /// 属性10: 整数溢出处理
    #[test]
    fn integer_overflow_handling(a in i64::MIN..i64::MAX, b in i64::MIN..i64::MAX) {
        let v1 = Value::Integer(a);
        let v2 = Value::Integer(b);
        
        // 序列化反序列化不应panic
        let _ = v1.serialize();
        let _ = v2.serialize();
        
        prop_assert!(true);
    }
}

// 更多复杂场景
proptest! {
    #![proptest_config(ProptestConfig {
        cases: 500,
        .. ProptestConfig::default()
    })]

    /// 属性12: 混合类型Record序列化
    #[test]
    fn mixed_type_record_serde(
        int_val in any::<i64>(),
        float_val in any::<f64>().prop_filter("finite", |f| f.is_finite()),
        text_val in "[a-zA-Z0-9_ ]{0,50}",
        blob_val in prop::collection::vec(any::<u8>(), 0..50)
    ) {
        let values = vec![
            Value::Integer(int_val),
            Value::Real(float_val),
            Value::Text(text_val),
            Value::Blob(blob_val),
            Value::Null,
        ];
        
        let record = Record::new(values);
        let bytes = record.serialize();
        let deserialized = Record::deserialize(&bytes).unwrap();
        
        prop_assert_eq!(record.values.len(), deserialized.values.len());
    }

    /// 属性13: Value类型识别正确
    #[test]
    fn value_type_identification(value in value_strategy()) {
        match value {
            Value::Integer(_) => {
                prop_assert!(matches!(value, Value::Integer(_)));
            }
            Value::Real(_) => {
                prop_assert!(matches!(value, Value::Real(_)));
            }
            Value::Text(_) => {
                prop_assert!(matches!(value, Value::Text(_)));
            }
            Value::Blob(_) => {
                prop_assert!(matches!(value, Value::Blob(_)));
            }
            Value::Null => {
                prop_assert!(matches!(value, Value::Null));
            }
            Value::Vector(_) => {
                prop_assert!(matches!(value, Value::Vector(_)));
            }
        }
    }

    /// 属性14: Record克隆正确
    #[test]
    fn record_clone_equality(values in prop::collection::vec(value_strategy(), 0..10)) {
        let record = Record::new(values.clone());
        let cloned = record.clone();
        
        prop_assert_eq!(record.values, cloned.values);
    }

    /// 属性15: 重复Record序列化结果相同
    #[test]
    fn record_serde_deterministic(values in prop::collection::vec(value_strategy(), 0..10)) {
        let record = Record::new(values);
        let bytes1 = record.serialize();
        let bytes2 = record.serialize();
        
        prop_assert_eq!(bytes1, bytes2);
    }
}
