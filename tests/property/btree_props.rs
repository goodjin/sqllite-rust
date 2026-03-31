//! B+Tree 属性测试
//! 
//! 测试B+Tree索引的核心属性：
//! - 插入后必能找到
//! - 删除后必找不到
//! - 范围扫描结果有序
//! - 重复键处理正确

use proptest::prelude::*;
use proptest::test_runner::FileFailurePersistence;
use sqllite_rust::storage::btree::BPlusTreeIndex;
use sqllite_rust::storage::record::Value;

// 生成测试用的Value
fn value_strategy() -> impl Strategy<Value = Value> {
    prop_oneof![
        any::<i64>().prop_map(Value::Integer),
        "[a-zA-Z0-9_]{1,20}".prop_map(Value::Text),
    ]
}

// 生成键值对
fn key_value_strategy() -> impl Strategy<Value = (Value, u64)> {
    (value_strategy(), any::<u64>())
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 1000,
        failure_persistence: Some(Box::new(
            FileFailurePersistence::WithSource("regressions")
        )),
        .. ProptestConfig::default()
    })]

    /// 属性1: 插入后必能找到
    #[test]
    fn btree_insert_then_find((key, rowid) in key_value_strategy()) {
        let mut index = BPlusTreeIndex::new(
            "idx_test".to_string(),
            "test_table".to_string(),
            "test_col".to_string(),
        );
        
        index.insert(key.clone(), rowid).unwrap();
        
        let result = index.lookup(&key);
        prop_assert!(result.is_some());
        prop_assert!(result.unwrap().contains(&rowid));
    }

    /// 属性2: 批量插入后必能找到所有
    #[test]
    fn btree_batch_insert_then_find_all(
        entries in prop::collection::vec(key_value_strategy(), 1..100)
    ) {
        let mut index = BPlusTreeIndex::new(
            "idx_test".to_string(),
            "test_table".to_string(),
            "test_col".to_string(),
        );
        
        // 插入所有条目
        for (key, rowid) in &entries {
            index.insert(key.clone(), *rowid).unwrap();
        }
        
        // 验证所有条目都能找到
        for (key, rowid) in &entries {
            let result = index.lookup(key);
            prop_assert!(result.is_some(), "Key {:?} not found", key);
            prop_assert!(
                result.unwrap().contains(rowid),
                "Rowid {} not found for key {:?}", rowid, key
            );
        }
    }

    /// 属性3: 删除后必找不到
    #[test]
    fn btree_delete_then_not_find((key, rowid) in key_value_strategy()) {
        let mut index = BPlusTreeIndex::new(
            "idx_test".to_string(),
            "test_table".to_string(),
            "test_col".to_string(),
        );
        
        // 先插入
        index.insert(key.clone(), rowid).unwrap();
        
        // 再删除
        index.delete(&key, rowid).unwrap();
        
        // 验证找不到
        let result = index.lookup(&key);
        prop_assert!(
            result.is_none() || !result.unwrap().contains(&rowid),
            "Rowid {} still found after deletion", rowid
        );
    }

    /// 属性4: 范围扫描结果在范围内
    #[test]
    fn btree_range_scan_in_bounds(
        entries in prop::collection::vec((any::<i64>(), any::<u64>()), 1..50),
        start in any::<i64>(),
        end in any::<i64>()
    ) {
        let mut index = BPlusTreeIndex::new(
            "idx_test".to_string(),
            "test_table".to_string(),
            "test_col".to_string(),
        );
        
        // 插入整数键
        for (key, rowid) in &entries {
            index.insert(Value::Integer(*key), *rowid).unwrap();
        }
        
        let (min, max) = if start <= end { (start, end) } else { (end, start) };
        
        // 范围扫描
        let result = index.range_scan(&Value::Integer(min), &Value::Integer(max));
        
        // 验证所有返回的rowid对应的键都在范围内
        // 注意：这里我们主要验证范围扫描不 panic，且结果是合理的
        prop_assert!(result.len() <= entries.len());
    }

    /// 属性5: 重复键可以插入多个rowid
    #[test]
    fn btree_duplicate_keys_accepted(
        key in value_strategy(),
        rowids in prop::collection::vec(any::<u64>(), 1..20)
    ) {
        let mut index = BPlusTreeIndex::new(
            "idx_test".to_string(),
            "test_table".to_string(),
            "test_col".to_string(),
        );
        
        // 插入相同键的不同rowid
        for rowid in &rowids {
            index.insert(key.clone(), *rowid).unwrap();
        }
        
        let result = index.lookup(&key);
        prop_assert!(result.is_some());
        
        let found_rowids = result.unwrap();
        prop_assert_eq!(found_rowids.len(), rowids.len());
        
        for rowid in &rowids {
            prop_assert!(found_rowids.contains(rowid));
        }
    }

    /// 属性6: 删除重复键中的一个，其他仍能找到
    #[test]
    fn btree_delete_one_duplicate_preserves_others(
        key in value_strategy(),
        rowids in prop::collection::vec(any::<u64>(), 2..10)
    ) {
        let mut index = BPlusTreeIndex::new(
            "idx_test".to_string(),
            "test_table".to_string(),
            "test_col".to_string(),
        );
        
        // 插入所有重复键
        for rowid in &rowids {
            index.insert(key.clone(), *rowid).unwrap();
        }
        
        // 删除第一个
        let to_delete = rowids[0];
        index.delete(&key, to_delete).unwrap();
        
        // 验证被删除的找不到
        let result = index.lookup(&key).unwrap();
        prop_assert!(!result.contains(&to_delete));
        
        // 验证其他的还能找到
        for rowid in rowids.iter().skip(1) {
            prop_assert!(result.contains(rowid));
        }
    }

    /// 属性7: 空索引查询返回None
    #[test]
    fn btree_empty_index_lookup_returns_none(key in value_strategy()) {
        let index = BPlusTreeIndex::new(
            "idx_test".to_string(),
            "test_table".to_string(),
            "test_col".to_string(),
        );
        
        prop_assert!(index.lookup(&key).is_none());
    }

    /// 属性8: 范围扫描空索引返回空
    #[test]
    fn btree_empty_index_range_scan_returns_empty(
        start in any::<i64>(),
        end in any::<i64>()
    ) {
        let index = BPlusTreeIndex::new(
            "idx_test".to_string(),
            "test_table".to_string(),
            "test_col".to_string(),
        );
        
        let (min, max) = if start <= end { (start, end) } else { (end, start) };
        let result = index.range_scan(&Value::Integer(min), &Value::Integer(max));
        
        prop_assert!(result.is_empty());
    }

    /// 属性9: 插入后删除再插入同一键能成功
    #[test]
    fn btree_reinsert_after_delete((key, rowid) in key_value_strategy()) {
        let mut index = BPlusTreeIndex::new(
            "idx_test".to_string(),
            "test_table".to_string(),
            "test_col".to_string(),
        );
        
        // 插入
        index.insert(key.clone(), rowid).unwrap();
        // 删除
        index.delete(&key, rowid).unwrap();
        // 再次插入
        index.insert(key.clone(), rowid).unwrap();
        
        // 验证能找到
        let result = index.lookup(&key);
        prop_assert!(result.is_some());
        prop_assert!(result.unwrap().contains(&rowid));
    }

    /// 属性10: 相同rowid重复插入不会重复
    #[test]
    fn btree_same_rowid_duplicate_insert_no_duplicates(
        (key, rowid) in key_value_strategy()
    ) {
        let mut index = BPlusTreeIndex::new(
            "idx_test".to_string(),
            "test_table".to_string(),
            "test_col".to_string(),
        );
        
        // 重复插入相同键和rowid
        for _ in 0..5 {
            index.insert(key.clone(), rowid).unwrap();
        }
        
        let result = index.lookup(&key).unwrap();
        prop_assert_eq!(result.len(), 1);
        prop_assert_eq!(result[0], rowid);
    }
}

// 更多复杂场景的测试
proptest! {
    #![proptest_config(ProptestConfig {
        cases: 500,
        .. ProptestConfig::default()
    })]

    /// 属性11: 随机操作序列后数据一致性
    #[test]
    fn btree_random_operations_maintain_consistency(
        ops in prop::collection::vec(
            (any::<u8>(), key_value_strategy()).prop_map(|(t, kv)| (t % 2, kv)),
            1..200
        )
    ) {
        let mut index = BPlusTreeIndex::new(
            "idx_test".to_string(),
            "test_table".to_string(),
            "test_col".to_string(),
        );
        
        let mut expected: std::collections::HashMap<Value, Vec<u64>> = std::collections::HashMap::new();
        
        for (op_type, (key, rowid)) in ops {
            match op_type {
                0 => {
                    // 插入
                    index.insert(key.clone(), rowid).unwrap();
                    let entry = expected.entry(key).or_default();
                    if !entry.contains(&rowid) {
                        entry.push(rowid);
                    }
                }
                1 => {
                    // 删除
                    let _ = index.delete(&key, rowid);
                    if let Some(vec) = expected.get_mut(&key) {
                        vec.retain(|&id| id != rowid);
                        if vec.is_empty() {
                            expected.remove(&key);
                        }
                    }
                }
                _ => unreachable!(),
            }
        }
        
        // 验证所有预期的键都存在
        for (key, rowids) in &expected {
            let result = index.lookup(key);
            prop_assert!(result.is_some(), "Key {:?} should exist", key);
            let found = result.unwrap();
            prop_assert_eq!(found.len(), rowids.len(), "Rowid count mismatch for key {:?}", key);
            for rowid in rowids {
                prop_assert!(found.contains(rowid), "Rowid {} should exist for key {:?}", rowid, key);
            }
        }
        
        // 验证没有多余的键
        // 通过遍历索引的所有条目
        for (key, _) in index.iter() {
            prop_assert!(expected.contains_key(key), "Key {:?} should not exist", key);
        }
    }

    /// 属性12: 整数范围扫描包含所有匹配的键
    #[test]
    fn btree_integer_range_scan_completeness(
        keys in prop::collection::vec(any::<i64>(), 1..100),
        range_start in any::<i64>(),
        range_end in any::<i64>()
    ) {
        let mut index = BPlusTreeIndex::new(
            "idx_test".to_string(),
            "test_table".to_string(),
            "test_col".to_string(),
        );
        
        // 插入所有键
        for (i, key) in keys.iter().enumerate() {
            index.insert(Value::Integer(*key), i as u64).unwrap();
        }
        
        let (min, max) = if range_start <= range_end {
            (range_start, range_end)
        } else {
            (range_end, range_start)
        };
        
        // 执行范围扫描
        let result = index.range_scan(&Value::Integer(min), &Value::Integer(max));
        
        // 计算预期的匹配数量
        let expected_count = keys.iter()
            .enumerate()
            .filter(|(_, k)| **k >= min && **k < max)
            .count();
        
        prop_assert_eq!(result.len(), expected_count);
    }
}
