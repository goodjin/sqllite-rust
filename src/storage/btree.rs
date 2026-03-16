use crate::storage::{Record, Result, StorageError, Value};
use std::collections::BTreeMap;

/// B+ Tree索引
/// 简化实现：使用Rust的BTreeMap作为底层存储
pub struct BPlusTreeIndex {
    pub name: String,
    pub table: String,
    pub column: String,
    /// 索引数据：键 -> 行ID列表（支持重复键）
    data: BTreeMap<Value, Vec<u64>>,
}

impl BPlusTreeIndex {
    pub fn new(name: String, table: String, column: String) -> Self {
        Self {
            name,
            table,
            column,
            data: BTreeMap::new(),
        }
    }

    /// 插入索引项
    pub fn insert(&mut self, key: Value, rowid: u64) -> Result<()> {
        self.data
            .entry(key)
            .or_default()
            .push(rowid);
        Ok(())
    }

    /// 查找键（精确匹配）
    pub fn lookup(&self, key: &Value) -> Option<&Vec<u64>> {
        self.data.get(key)
    }

    /// 范围查询 [start, end)
    pub fn range_scan(&self, start: &Value, end: &Value) -> Vec<u64> {
        let mut result = Vec::new();
        for (_, rowids) in self.data.range(start.clone()..end.clone()) {
            result.extend(rowids);
        }
        result
    }

    /// 删除索引项
    pub fn delete(&mut self, key: &Value, rowid: u64) -> Result<()> {
        if let Some(rowids) = self.data.get_mut(key) {
            rowids.retain(|&id| id != rowid);
            if rowids.is_empty() {
                self.data.remove(key);
            }
        }
        Ok(())
    }

    /// 获取所有键值对（用于序列化）
    pub fn iter(&self) -> impl Iterator<Item = (&Value, &Vec<u64>)> {
        self.data.iter()
    }

    /// 从键值对重建索引
    pub fn from_iter(
        name: String,
        table: String,
        column: String,
        iter: impl Iterator<Item = (Value, Vec<u64>)>,
    ) -> Self {
        let mut data = BTreeMap::new();
        for (key, rowids) in iter {
            data.insert(key, rowids);
        }
        Self {
            name,
            table,
            column,
            data,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_btree_insert_and_lookup() {
        let mut index = BPlusTreeIndex::new(
            "idx_name".to_string(),
            "users".to_string(),
            "name".to_string(),
        );

        // Insert some entries
        index.insert(Value::Text("Alice".to_string()), 1).unwrap();
        index.insert(Value::Text("Bob".to_string()), 2).unwrap();
        index.insert(Value::Text("Charlie".to_string()), 3).unwrap();

        // Lookup
        assert_eq!(index.lookup(&Value::Text("Bob".to_string())), Some(&vec![2]));
        assert_eq!(index.lookup(&Value::Text("Alice".to_string())), Some(&vec![1]));
        assert_eq!(index.lookup(&Value::Text("David".to_string())), None);
    }

    #[test]
    fn test_btree_duplicate_keys() {
        let mut index = BPlusTreeIndex::new(
            "idx_dept".to_string(),
            "employees".to_string(),
            "dept".to_string(),
        );

        // Insert duplicate keys
        index.insert(Value::Text("IT".to_string()), 1).unwrap();
        index.insert(Value::Text("IT".to_string()), 2).unwrap();
        index.insert(Value::Text("IT".to_string()), 3).unwrap();
        index.insert(Value::Text("HR".to_string()), 4).unwrap();

        // Lookup should return all rowids for the key
        let it_rowids = index.lookup(&Value::Text("IT".to_string())).unwrap();
        assert_eq!(it_rowids.len(), 3);
        assert!(it_rowids.contains(&1));
        assert!(it_rowids.contains(&2));
        assert!(it_rowids.contains(&3));
    }

    #[test]
    fn test_btree_range_scan() {
        let mut index = BPlusTreeIndex::new(
            "idx_salary".to_string(),
            "employees".to_string(),
            "salary".to_string(),
        );

        // Insert entries with integer keys
        index.insert(Value::Integer(3000), 1).unwrap();
        index.insert(Value::Integer(4000), 2).unwrap();
        index.insert(Value::Integer(5000), 3).unwrap();
        index.insert(Value::Integer(6000), 4).unwrap();
        index.insert(Value::Integer(7000), 5).unwrap();

        // Range scan [4000, 6000)
        let result = index.range_scan(&Value::Integer(4000), &Value::Integer(6000));
        assert_eq!(result.len(), 2);
        assert!(result.contains(&2)); // 4000
        assert!(result.contains(&3)); // 5000
    }

    #[test]
    fn test_btree_delete() {
        let mut index = BPlusTreeIndex::new(
            "idx_name".to_string(),
            "users".to_string(),
            "name".to_string(),
        );

        // Insert entries
        index.insert(Value::Text("Alice".to_string()), 1).unwrap();
        index.insert(Value::Text("Bob".to_string()), 2).unwrap();
        index.insert(Value::Text("Bob".to_string()), 3).unwrap();

        // Delete one Bob
        index.delete(&Value::Text("Bob".to_string()), 2).unwrap();

        // Should still have one Bob
        let bob_rowids = index.lookup(&Value::Text("Bob".to_string())).unwrap();
        assert_eq!(bob_rowids.len(), 1);
        assert_eq!(bob_rowids[0], 3);

        // Delete the other Bob
        index.delete(&Value::Text("Bob".to_string()), 3).unwrap();

        // Bob should be completely removed
        assert_eq!(index.lookup(&Value::Text("Bob".to_string())), None);

        // Alice should still exist
        assert_eq!(index.lookup(&Value::Text("Alice".to_string())), Some(&vec![1]));
    }
}
