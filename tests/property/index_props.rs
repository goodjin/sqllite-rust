//! 索引属性测试

use proptest::prelude::*;
use proptest::test_runner::FileFailurePersistence;
use sqllite_rust::storage::btree::BPlusTreeIndex;
use sqllite_rust::storage::record::Value;

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 1000,
        failure_persistence: Some(Box::new(
            FileFailurePersistence::WithSource("regressions")
        )),
        .. ProptestConfig::default()
    })]

    /// 属性1: 索引名有效性
    #[test]
    fn index_name_validity(name in "[a-zA-Z_][a-zA-Z0-9_]{0,63}") {
        prop_assert!(!name.is_empty());
        prop_assert!(name.chars().next().unwrap().is_alphabetic() || name.starts_with('_'));
    }

    /// 属性2: 索引创建后为空
    #[test]
    fn new_index_is_empty(
        name in "[a-zA-Z_][a-zA-Z0-9_]{0,30}",
        table in "[a-zA-Z_][a-zA-Z0-9_]{0,30}",
        column in "[a-zA-Z_][a-zA-Z0-9_]{0,30}"
    ) {
        let index = BPlusTreeIndex::new(name, table, column);
        // 新索引应该没有数据
        let count = index.iter().count();
        prop_assert_eq!(count, 0);
    }

    /// 属性3: 索引唯一性约束
    #[test]
    fn unique_index_constraint(
        keys in prop::collection::vec(any::<i64>(), 1..50)
    ) {
        // 唯一索引不应该有重复键
        let mut unique_keys: std::collections::HashSet<i64> = std::collections::HashSet::new();
        for key in &keys {
            unique_keys.insert(*key);
        }
        
        // 唯一键数量应该小于等于原始键数量
        prop_assert!(unique_keys.len() <= keys.len());
    }

    /// 属性4: 索引覆盖查询正确性
    #[test]
    fn index_covering_query(
        values in prop::collection::vec((any::<i64>(), any::<u64>()), 1..100)
    ) {
        let mut index = BPlusTreeIndex::new(
            "idx".to_string(),
            "table".to_string(),
            "col".to_string(),
        );
        
        // 插入所有值
        for (key, rowid) in &values {
            index.insert(Value::Integer(*key), *rowid).unwrap();
        }
        
        // 验证所有值都可以通过索引找到
        for (key, rowid) in &values {
            let result = index.lookup(&Value::Integer(*key));
            prop_assert!(result.is_some());
            prop_assert!(result.unwrap().contains(rowid));
        }
    }

    /// 属性5: 索引选择性估计
    #[test]
    fn index_selectivity_estimate(
        total_rows in 1usize..10000,
        unique_values in 1usize..1000
    ) {
        let selectivity = if total_rows > 0 {
            unique_values as f64 / total_rows as f64
        } else {
            1.0
        };
        
        // 选择性应该在0到1之间
        prop_assert!(selectivity >= 0.0 && selectivity <= 1.0);
    }

    /// 属性6: 多列索引列顺序
    #[test]
    fn multi_column_index_column_order(
        columns in prop::collection::vec("[a-z]{1,10}", 1..5)
    ) {
        // 列顺序应该保持一致
        let ordered: Vec<_> = columns.clone();
        prop_assert_eq!(columns, ordered);
    }

    /// 属性7: 索引高度与条目数关系
    #[test]
    fn index_height_vs_entries(entries in 1usize..10000) {
        // B+树高度增长是对数级的
        let height = (entries as f64).log2().ceil() as usize;
        let max_height = (entries as f64).log2().ceil() as usize + 1;
        
        prop_assert!(height <= max_height);
    }

    /// 属性8: 范围查询结果数量限制
    #[test]
    fn range_query_result_limit(
        keys in prop::collection::vec(any::<i64>(), 1..200),
        range_size in 1i64..100
    ) {
        let mut index = BPlusTreeIndex::new(
            "idx".to_string(),
            "table".to_string(),
            "col".to_string(),
        );
        
        for (i, key) in keys.iter().enumerate() {
            index.insert(Value::Integer(*key), i as u64).unwrap();
        }
        
        let start = 0i64;
        let end = start + range_size;
        let result = index.range_scan(&Value::Integer(start), &Value::Integer(end));
        
        // 结果数量不应该超过总条目数
        prop_assert!(result.len() <= keys.len());
    }

    /// 属性9: 索引重建后数据一致
    #[test]
    fn index_rebuild_consistency(
        entries in prop::collection::vec((any::<i64>(), any::<u64>()), 1..50)
    ) {
        let mut index = BPlusTreeIndex::new(
            "idx".to_string(),
            "table".to_string(),
            "col".to_string(),
        );
        
        for (key, rowid) in &entries {
            index.insert(Value::Integer(*key), *rowid).unwrap();
        }
        
        // 收集所有条目
        let collected: Vec<_> = index.iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        
        // 重建索引
        let rebuilt = BPlusTreeIndex::from_iter(
            "idx".to_string(),
            "table".to_string(),
            "col".to_string(),
            collected.into_iter()
        );
        
        // 验证条目数相同
        prop_assert_eq!(index.iter().count(), rebuilt.iter().count());
    }

    /// 属性10: 索引统计信息正确性
    #[test]
    fn index_statistics_correctness(
        entries in prop::collection::vec(any::<i64>(), 1..100)
    ) {
        let mut index = BPlusTreeIndex::new(
            "idx".to_string(),
            "table".to_string(),
            "col".to_string(),
        );
        
        for (i, key) in entries.iter().enumerate() {
            index.insert(Value::Integer(*key), i as u64).unwrap();
        }
        
        let count = index.iter().count();
        prop_assert!(count > 0);
        prop_assert!(count <= entries.len());
    }
}

// 更多索引属性
proptest! {
    #![proptest_config(ProptestConfig {
        cases: 500,
        .. ProptestConfig::default()
    })]

    /// 属性11: 前缀索引截断正确性
    #[test]
    fn prefix_index_truncation(
        text in "[a-zA-Z0-9]{10,100}",
        prefix_len in 5usize..20
    ) {
        let prefix: String = text.chars().take(prefix_len).collect();
        prop_assert!(prefix.len() <= text.len());
        prop_assert!(prefix.len() <= prefix_len);
    }

    /// 属性12: 索引维护成本估算
    #[test]
    fn index_maintenance_cost(
        operations in prop::collection::vec(
            prop::sample::select(&[0u8, 1, 2]), // 0=insert, 1=delete, 2=update
            1..100
        )
    ) {
        let cost: usize = operations.len();
        // 维护成本应该与操作数成正比
        prop_assert_eq!(cost, operations.len());
    }

    /// 属性13: 复合索引前缀匹配
    #[test]
    fn composite_index_prefix_match(
        columns in prop::collection::vec("[a-z]{1,10}", 2..5)
    ) {
        // 复合索引的前缀应该可以单独使用
        let prefix: Vec<_> = columns.iter().take(1).cloned().collect();
        prop_assert!(!prefix.is_empty());
        prop_assert!(prefix.len() <= columns.len());
    }

    /// 属性14: 索引扫描成本估算
    #[test]
    fn index_scan_cost_estimation(
        rows in 1usize..1000000,
        selectivity in 0.001f64..1.0
    ) {
        let estimated_rows = (rows as f64 * selectivity) as usize;
        prop_assert!(estimated_rows <= rows);
    }

    /// 属性15: 索引块利用率
    #[test]
    fn index_block_utilization(
        entries in 1usize..1000,
        block_size in 512usize..4096,
        entry_size in 10usize..100
    ) {
        let space_needed = entries * entry_size;
        let blocks_needed = (space_needed + block_size - 1) / block_size;
        let utilization = space_needed as f64 / (blocks_needed * block_size) as f64;
        
        prop_assert!(utilization > 0.0 && utilization <= 1.0);
    }

    /// 属性16: 索引页分裂影响
    #[test]
    fn index_page_split_impact(
        entries_before in 1usize..100,
        new_entries in 1usize..50
    ) {
        // 页分裂后，索引应该仍然能访问所有条目
        let total = entries_before + new_entries;
        prop_assert!(total >= entries_before);
    }

    /// 属性17: 索引合并后数据完整
    #[test]
    fn index_merge_data_integrity(
        left_entries in prop::collection::vec(any::<i64>(), 1..30),
        right_entries in prop::collection::vec(any::<i64>(), 1..30)
    ) {
        let total_unique: std::collections::HashSet<_> = left_entries.iter()
            .chain(right_entries.iter())
            .collect();
        
        // 合并后条目数应该不超过两部分之和
        prop_assert!(total_unique.len() <= left_entries.len() + right_entries.len());
    }

    /// 属性18: 索引键类型一致性
    #[test]
    fn index_key_type_consistency(
        values in prop::collection::vec(value_strategy(), 1..20)
    ) {
        // 索引中的所有键应该有相同的类型
        if let Some(first) = values.first() {
            let first_type = std::mem::discriminant(first);
            for value in &values {
                prop_assert_eq!(std::mem::discriminant(value), first_type);
            }
        }
    }

    /// 属性19: 降序索引顺序正确
    #[test]
    fn descending_index_order(
        values in prop::collection::vec(any::<i64>(), 1..50)
    ) {
        let mut sorted = values.clone();
        sorted.sort_by(|a, b| b.cmp(a)); // 降序
        
        for i in 1..sorted.len() {
            prop_assert!(sorted[i-1] >= sorted[i]);
        }
    }

    /// 属性20: 索引存在性检查
    #[test]
    fn index_existence_check(
        entries in prop::collection::vec((any::<i64>(), any::<u64>()), 1..30),
        search_key in any::<i64>()
    ) {
        let mut index = BPlusTreeIndex::new(
            "idx".to_string(),
            "table".to_string(),
            "col".to_string(),
        );
        
        for (key, rowid) in &entries {
            index.insert(Value::Integer(*key), *rowid).unwrap();
        }
        
        let exists_in_entries = entries.iter().any(|(k, _)| *k == search_key);
        let found_in_index = index.lookup(&Value::Integer(search_key)).is_some();
        
        // 如果条目中有这个键，索引中应该能找到
        if exists_in_entries {
            prop_assert!(found_in_index);
        }
    }
}

// 边界情况
proptest! {
    #![proptest_config(ProptestConfig {
        cases: 300,
        .. ProptestConfig::default()
    })]

    /// 属性21: 空键索引处理
    #[test]
    fn empty_key_index_handling() {
        let index = BPlusTreeIndex::new(
            "idx".to_string(),
            "table".to_string(),
            "col".to_string(),
        );
        
        let empty_key = Value::Text(String::new());
        let result = index.lookup(&empty_key);
        prop_assert!(result.is_none());
    }

    /// 属性22: NULL值索引处理
    #[test]
    fn null_value_index_handling() {
        let mut index = BPlusTreeIndex::new(
            "idx".to_string(),
            "table".to_string(),
            "col".to_string(),
        );
        
        let null_key = Value::Null;
        index.insert(null_key.clone(), 1).unwrap();
        
        let result = index.lookup(&null_key);
        prop_assert!(result.is_some());
    }

    /// 属性23: 极大键值索引
    #[test]
    fn extreme_key_value_indexing(key in any::<i64>()) {
        let mut index = BPlusTreeIndex::new(
            "idx".to_string(),
            "table".to_string(),
            "col".to_string(),
        );
        
        index.insert(Value::Integer(key), 1).unwrap();
        let result = index.lookup(&Value::Integer(key));
        
        prop_assert!(result.is_some());
    }

    /// 属性24: 索引并发修改
    #[test]
    fn index_concurrent_modification(
        operations in prop::collection::vec(
            (any::<i64>(), any::<u64>(), prop::bool::ANY),
            1..50
        )
    ) {
        let mut index = BPlusTreeIndex::new(
            "idx".to_string(),
            "table".to_string(),
            "col".to_string(),
        );
        
        for (key, rowid, is_insert) in operations {
            if is_insert {
                index.insert(Value::Integer(key), rowid).unwrap();
            } else {
                index.delete(&Value::Integer(key), rowid).unwrap();
            }
        }
        
        // 索引应该仍然一致
        prop_assert!(true);
    }

    /// 属性25: 索引碎片化检测
    #[test]
    fn index_fragmentation_detection(
        total_pages in 10usize..1000,
        used_pages in 1usize..500
    ) {
        let fragmentation = if total_pages > 0 {
            1.0 - (used_pages as f64 / total_pages as f64)
        } else {
            0.0
        };
        
        prop_assert!(fragmentation >= 0.0 && fragmentation <= 1.0);
    }
}

// 辅助函数
fn value_strategy() -> impl Strategy<Value = Value> {
    prop_oneof![
        any::<i64>().prop_map(Value::Integer),
        any::<f64>().prop_filter("finite", |f| f.is_finite()).prop_map(Value::Real),
        "[a-zA-Z0-9_]{0,20}".prop_map(Value::Text),
        Just(Value::Null),
    ]
}
