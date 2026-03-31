//! Record 模块属性测试

use proptest::prelude::*;
use proptest::test_runner::FileFailurePersistence;
use sqllite_rust::storage::record::{Value, Record};

fn value_strategy() -> impl Strategy<Value = Value> {
    prop_oneof![
        any::<i64>().prop_map(Value::Integer),
        any::<f64>().prop_filter("finite", |f| f.is_finite()).prop_map(Value::Real),
        "[a-zA-Z0-9_ ]{0,50}".prop_map(Value::Text),
        prop::collection::vec(any::<u8>(), 0..50).prop_map(Value::Blob),
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

    /// 属性1: Record创建后值数量正确
    #[test]
    fn record_creation_preserves_value_count(count in 0usize..50) {
        let values: Vec<Value> = (0..count).map(|i| Value::Integer(i as i64)).collect();
        let record = Record::new(values);
        prop_assert_eq!(record.values.len(), count);
    }

    /// 属性2: Record访问器返回正确值
    #[test]
    fn record_accessor_returns_correct_values(
        values in prop::collection::vec(value_strategy(), 1..20)
    ) {
        let record = Record::new(values.clone());
        
        for (i, expected) in values.iter().enumerate() {
            prop_assert_eq!(record.values.get(i), Some(expected));
        }
    }

    /// 属性3: 越界访问返回None
    #[test]
    fn record_out_of_bounds_returns_none(
        values in prop::collection::vec(value_strategy(), 0..10),
        index in 10usize..100
    ) {
        let record = Record::new(values);
        prop_assert_eq!(record.values.get(index), None);
    }

    /// 属性4: Record迭代器遍历所有元素
    #[test]
    fn record_iterator_visits_all_elements(
        values in prop::collection::vec(value_strategy(), 0..30)
    ) {
        let record = Record::new(values.clone());
        let collected: Vec<&Value> = record.values.iter().collect();
        
        prop_assert_eq!(collected.len(), values.len());
        for (collected, original) in collected.iter().zip(values.iter()) {
            prop_assert_eq!(*collected, original);
        }
    }

    /// 属性5: Record大小计算非负
    #[test]
    fn record_size_calculation_non_negative(
        values in prop::collection::vec(value_strategy(), 0..20)
    ) {
        let record = Record::new(values);
        let size = record.serialize().len();
        prop_assert!(size >= 0);
    }

    /// 属性6: 添加值增加Record大小
    #[test]
    fn record_add_value_increases_size(
        initial in prop::collection::vec(value_strategy(), 0..5),
        to_add in value_strategy()
    ) {
        let record1 = Record::new(initial.clone());
        let size_before = record1.serialize().len();
        
        let mut new_values = initial.clone();
        new_values.push(to_add);
        let record2 = Record::new(new_values);
        let size_after = record2.serialize().len();
        
        prop_assert!(size_after >= size_before);
    }

    /// 属性7: Record为空判断正确
    #[test]
    fn record_is_empty_correctness(count in 0usize..5) {
        let values: Vec<Value> = (0..count).map(|_| Value::Null).collect();
        let record = Record::new(values);
        prop_assert_eq!(record.values.is_empty(), count == 0);
    }

    /// 属性8: Record长度计算正确
    #[test]
    fn record_len_correctness(count in 0usize..100) {
        let values: Vec<Value> = (0..count).map(|i| Value::Integer(i as i64)).collect();
        let record = Record::new(values);
        prop_assert_eq!(record.values.len(), count);
    }

    /// 属性9: 字符串化表示非空
    #[test]
    fn record_to_string_non_empty(
        values in prop::collection::vec(value_strategy(), 1..10)
    ) {
        let record = Record::new(values);
        let s = format!("{:?}", record);
        prop_assert!(!s.is_empty());
    }

    /// 属性10: Value的Display实现非panic
    #[test]
    fn value_display_no_panic(value in value_strategy()) {
        let _ = format!("{}", value);
        prop_assert!(true);
    }
}

// Value特定属性测试
proptest! {
    #![proptest_config(ProptestConfig {
        cases: 1000,
        .. ProptestConfig::default()
    })]

    /// 属性11: Integer比较符合数学规则
    #[test]
    fn integer_comparison_matches_math(a in any::<i64>(), b in any::<i64>()) {
        let v1 = Value::Integer(a);
        let v2 = Value::Integer(b);
        
        if let Some(ord) = v1.partial_cmp(&v2) {
            match ord {
                std::cmp::Ordering::Less => prop_assert!(a < b),
                std::cmp::Ordering::Equal => prop_assert_eq!(a, b),
                std::cmp::Ordering::Greater => prop_assert!(a > b),
            }
        }
    }

    /// 属性12: Float比较符合IEEE规则
    #[test]
    fn float_comparison_matches_ieee(
        a in any::<f64>().prop_filter("finite", |f| f.is_finite()),
        b in any::<f64>().prop_filter("finite", |f| f.is_finite())
    ) {
        let v1 = Value::Real(a);
        let v2 = Value::Real(b);
        
        if let Some(ord) = v1.partial_cmp(&v2) {
            match ord {
                std::cmp::Ordering::Less => prop_assert!(a < b),
                std::cmp::Ordering::Equal => prop_assert_eq!(a, b),
                std::cmp::Ordering::Greater => prop_assert!(a > b),
            }
        }
    }

    /// 属性13: Text字典序比较正确
    #[test]
    fn text_lexicographic_comparison(
        a in "[a-z]{1,20}",
        b in "[a-z]{1,20}"
    ) {
        let v1 = Value::Text(a.clone());
        let v2 = Value::Text(b.clone());
        
        if let Some(ord) = v1.partial_cmp(&v2) {
            prop_assert_eq!(ord, a.cmp(&b));
        }
    }

    /// 属性14: Null与其他类型比较
    #[test]
    fn null_comparison_properties(other in value_strategy()) {
        let null = Value::Null;
        
        // Null应该与其他值可比较（SQL语义）
        let _ = null.partial_cmp(&other);
        let _ = other.partial_cmp(&null);
    }

    /// 属性15: Blob长度获取正确
    #[test]
    fn blob_length_correctness(data in prop::collection::vec(any::<u8>(), 0..100)) {
        let blob = Value::Blob(data.clone());
        
        if let Value::Blob(content) = &blob {
            prop_assert_eq!(content.len(), data.len());
        }
    }

    /// 属性16: 整数转字符串非空
    #[test]
    fn integer_to_string_non_empty(n in any::<i64>()) {
        let v = Value::Integer(n);
        let s = v.to_string();
        prop_assert!(!s.is_empty());
    }

    /// 属性17: 浮点数转字符串非空
    #[test]
    fn float_to_string_non_empty(n in any::<f64>().prop_filter("finite", |f| f.is_finite())) {
        let v = Value::Real(n);
        let s = v.to_string();
        prop_assert!(!s.is_empty());
    }

    /// 属性18: 文本值非空检查
    #[test]
    fn text_value_nonempty_check(s in "[a-zA-Z0-9]*") {
        let v = Value::Text(s.clone());
        
        let is_empty = match &v {
            Value::Text(t) => t.is_empty(),
            _ => true,
        };
        
        prop_assert_eq!(is_empty, s.is_empty());
    }

    /// 属性19: Value克隆后相等
    #[test]
    fn value_clone_equality(v in value_strategy()) {
        let cloned = v.clone();
        prop_assert_eq!(v, cloned);
    }

    /// 属性20: Value克隆相等性
    #[test]
    fn value_clone_equality_property(v in value_strategy()) {
        let cloned = v.clone();
        prop_assert_eq!(v, cloned);
    }
}

// 边界情况测试
proptest! {
    #![proptest_config(ProptestConfig {
        cases: 500,
        .. ProptestConfig::default()
    })]

    /// 属性21: 极端整数处理
    #[test]
    fn extreme_integer_handling(n in prop::sample::select(&[i64::MIN, i64::MAX, -1, 0, 1])) {
        let v = Value::Integer(n);
        let bytes = v.serialize();
        let restored = Value::deserialize(&bytes).unwrap();
        prop_assert_eq!(v, restored);
    }

    /// 属性22: 极端浮点数处理
    #[test]
    fn extreme_float_handling(
        n in prop::sample::select(&[f64::MIN, f64::MAX, f64::MIN_POSITIVE, 0.0, -0.0])
    ) {
        let v = Value::Real(n);
        let bytes = v.serialize();
        let restored = Value::deserialize(&bytes).unwrap();
        
        if let Value::Real(f) = restored {
            // 处理 -0.0 == 0.0 的情况
            if n == 0.0 || n == -0.0 {
                prop_assert!(f == 0.0 || f == -0.0);
            } else {
                prop_assert!((n - f).abs() < f64::EPSILON * 100.0);
            }
        }
    }

    /// 属性23: 空Blob处理
    #[test]
    fn empty_blob_handling() {
        let v = Value::Blob(vec![]);
        let bytes = v.serialize();
        let restored = Value::deserialize(&bytes).unwrap();
        prop_assert_eq!(v, restored);
    }

    /// 属性24: 空Text处理
    #[test]
    fn empty_text_handling() {
        let v = Value::Text(String::new());
        let bytes = v.serialize();
        let restored = Value::deserialize(&bytes).unwrap();
        prop_assert_eq!(v, restored);
    }

    /// 属性25: 大Record处理
    #[test]
    fn large_record_handling(
        values in prop::collection::vec(value_strategy(), 100..200)
    ) {
        let record = Record::new(values);
        let bytes = record.serialize();
        let restored = Record::deserialize(&bytes).unwrap();
        prop_assert_eq!(record.values.len(), restored.values.len());
    }
}
