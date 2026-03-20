use crate::sql::ast::{ColumnDef, DataType};
use crate::storage::{Record, Result, StorageError, BPlusTreeIndex, Value};
use crate::pager::{Pager, PageId};
use std::collections::HashMap;

/// 表元数据
#[derive(Debug, Clone)]
pub struct Table {
    pub name: String,
    pub columns: Vec<ColumnDef>,
    pub root_page: PageId,
    pub next_rowid: u64,
}

impl Table {
    pub fn new(name: String, columns: Vec<ColumnDef>, root_page: PageId) -> Self {
        Self {
            name,
            columns,
            root_page,
            next_rowid: 1,
        }
    }

    /// 获取列索引
    pub fn column_index(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|c| c.name == name)
    }

    /// 序列化表元数据
    pub fn serialize(&self) -> Vec<u8> {
        let mut data = Vec::new();

        // 表名长度 + 表名
        let name_bytes = self.name.as_bytes();
        data.extend_from_slice(&(name_bytes.len() as u32).to_be_bytes());
        data.extend_from_slice(name_bytes);

        // 根页面ID
        data.extend_from_slice(&self.root_page.to_be_bytes());

        // 下一个行ID
        data.extend_from_slice(&self.next_rowid.to_be_bytes());

        // 列数量
        data.push(self.columns.len() as u8);

        // 每列的信息
        for col in &self.columns {
            // 列名长度 + 列名
            let col_name_bytes = col.name.as_bytes();
            data.extend_from_slice(&(col_name_bytes.len() as u32).to_be_bytes());
            data.extend_from_slice(col_name_bytes);

            // 数据类型
            match col.data_type {
                DataType::Integer => data.push(1),
                DataType::Text => data.push(2),
                DataType::Blob => data.push(4),
                DataType::Vector(dim) => {
                    data.push(5);
                    data.extend_from_slice(&dim.to_be_bytes());
                }
            }
        }

        data
    }

    /// 反序列化表元数据
    pub fn deserialize(data: &[u8]) -> Result<Self> {
        let mut pos = 0;

        // 读取表名
        let name_len = u32::from_be_bytes([data[pos], data[pos+1], data[pos+2], data[pos+3]]) as usize;
        pos += 4;
        let name = String::from_utf8_lossy(&data[pos..pos+name_len]).to_string();
        pos += name_len;

        // 读取根页面ID
        let root_page = u32::from_be_bytes([data[pos], data[pos+1], data[pos+2], data[pos+3]]) as PageId;
        pos += 4;

        // 读取下一个行ID
        let next_rowid = u64::from_be_bytes([
            data[pos], data[pos+1], data[pos+2], data[pos+3],
            data[pos+4], data[pos+5], data[pos+6], data[pos+7]
        ]);
        pos += 8;

        // 读取列数量
        let col_count = data[pos] as usize;
        pos += 1;

        // 读取每列信息
        let mut columns = Vec::new();
        for _ in 0..col_count {
            let col_name_len = u32::from_be_bytes([data[pos], data[pos+1], data[pos+2], data[pos+3]]) as usize;
            pos += 4;
            let col_name = String::from_utf8_lossy(&data[pos..pos+col_name_len]).to_string();
            pos += col_name_len;

            let data_type = match data[pos] {
                1 => { pos += 1; DataType::Integer },
                2 => { pos += 1; DataType::Text },
                4 => { pos += 1; DataType::Blob },
                5 => {
                    pos += 1;
                    let dim = u32::from_be_bytes([data[pos], data[pos+1], data[pos+2], data[pos+3]]);
                    pos += 4;
                    DataType::Vector(dim)
                }
                _ => { pos += 1; DataType::Text },
            };

            columns.push(ColumnDef {
                name: col_name,
                data_type,
                nullable: true,
                primary_key: false,
            });
        }

        Ok(Self {
            name,
            columns,
            root_page,
            next_rowid,
        })
    }
}

/// 数据库 - 管理所有表
pub struct Database {
    pager: Pager,
    tables: HashMap<String, Table>,
    indexes: HashMap<String, BPlusTreeIndex>,
    schema_page: PageId,
}

impl Database {
    pub fn open(path: &str) -> Result<Self> {
        let mut pager = Pager::open(path)?;
        let schema_page = 1; // 第1页用于存储schema

        let mut tables = HashMap::new();

        // 尝试加载已有的表定义
        if pager.header().database_size > schema_page {
            if let Ok(page) = pager.get_page(schema_page) {
                // 解析页面中的表定义
                // 简化实现：每个表定义占一页
                let table_count = page.data[0] as usize;
                let mut pos = 1;

                for _ in 0..table_count {
                    let table_data_len = u32::from_be_bytes([
                        page.data[pos], page.data[pos+1],
                        page.data[pos+2], page.data[pos+3]
                    ]) as usize;
                    pos += 4;

                    let table_data = &page.data[pos..pos+table_data_len];
                    if let Ok(table) = Table::deserialize(table_data) {
                        tables.insert(table.name.clone(), table);
                    }
                    pos += table_data_len;
                }
            }
        }

        Ok(Self {
            pager,
            tables,
            indexes: HashMap::new(),
            schema_page,
        })
    }

    /// 创建表
    pub fn create_table(&mut self, name: String, columns: Vec<ColumnDef>) -> Result<()> {
        if self.tables.contains_key(&name) {
            return Err(StorageError::DuplicateKey);
        }

        // 分配根页面（跳过第1页，第1页用于存储schema）
        let root_page = self.pager.allocate_page()?;
        let root_page = if root_page == 1 {
            // 跳过第1页，再分配一页
            self.pager.allocate_page()?
        } else {
            root_page
        };

        // 初始化根页面（清空记录计数）
        let mut page = self.pager.get_page(root_page)?;
        page.data[0..4].copy_from_slice(&0u32.to_be_bytes());
        self.pager.write_page(&page)?;

        let table = Table::new(name.clone(), columns, root_page);
        self.tables.insert(name, table);

        // 持久化schema
        self.save_schema()?;

        Ok(())
    }

    /// 删除表
    pub fn drop_table(&mut self, name: &str) -> Result<()> {
        if self.tables.remove(name).is_none() {
            return Err(StorageError::KeyNotFound);
        }

        // 持久化schema
        self.save_schema()?;

        Ok(())
    }

    /// 获取表
    pub fn get_table(&self, name: &str) -> Option<&Table> {
        self.tables.get(name)
    }

    /// 获取表（可变）
    pub fn get_table_mut(&mut self, name: &str) -> Option<&mut Table> {
        self.tables.get_mut(name)
    }

    /// 列出所有表
    pub fn list_tables(&self) -> Vec<&String> {
        self.tables.keys().collect()
    }

    /// 创建索引
    pub fn create_index(&mut self, index_name: String, table_name: String, column_name: String) -> Result<()> {
        if !self.tables.contains_key(&table_name) {
            return Err(StorageError::KeyNotFound);
        }

        let index = BPlusTreeIndex::new(
            index_name.clone(),
            table_name.clone(),
            column_name.clone(),
        );

        self.indexes.insert(index_name, index);
        Ok(())
    }

    /// 获取索引
    pub fn get_index(&self, name: &str) -> Option<&BPlusTreeIndex> {
        self.indexes.get(name)
    }

    /// 获取索引（可变）
    pub fn get_index_mut(&mut self, name: &str) -> Option<&mut BPlusTreeIndex> {
        self.indexes.get_mut(name)
    }

    /// 列出表的所有索引
    pub fn get_table_indexes(&self, table_name: &str) -> Vec<&BPlusTreeIndex> {
        self.indexes
            .values()
            .filter(|idx| idx.table == table_name)
            .collect()
    }

    /// 插入记录
    pub fn insert(&mut self, table_name: &str, record: Record) -> Result<u64> {
        let table = self.get_table_mut(table_name)
            .ok_or(StorageError::KeyNotFound)?;

        let rowid = table.next_rowid;
        table.next_rowid += 1;

        // 序列化记录
        let data = record.serialize();

        // 获取根页面并存储数据
        let page_id = table.root_page;
        let mut page = self.pager.get_page(page_id)?;

        // 简化实现：直接存储在页面中
        // 格式: [记录数量(4字节)] [记录1偏移(4字节)] [记录1长度(4字节)] [记录1数据] ...
        let record_count = u32::from_be_bytes([
            page.data[0], page.data[1], page.data[2], page.data[3]
        ]);

        // 如果记录数量异常（页面未初始化），则重置为0
        let record_count = if record_count > 1000 { 0 } else { record_count };

        let new_count = record_count + 1;
        page.data[0..4].copy_from_slice(&new_count.to_be_bytes());

        // 计算存储位置（简化：从页面末尾开始存储）
        // record_count为0时，new_count=1，offset = 4096 - 256 = 3840
        // record_count为1时，new_count=2，offset = 4096 - 512 = 3584
        // 以此类推
        // 注意：new_count从1开始，所以计算时用new_count
        let offset: usize = 4096usize.saturating_sub((new_count as usize) * 256);
        let len = data.len();

        // 检查记录长度
        if len > 256 {
            return Err(StorageError::RecordTooLarge(data.len()));
        }

        // 确保offset在有效范围内
        // 元数据区：前4字节是记录数量，后面每8字节存储一条记录的元数据（偏移+长度）
        // 最大支持约60条记录（4 + 60*8 = 484字节）
        // 所以offset必须大于512，留出足够空间给元数据
        // 同时确保记录能放入页面
        // 当new_count=1时，offset=4096-256=3840，3840+19=3859 < 4096 ✓
        // 当new_count=2时，offset=4096-512=3584，3584+19=3603 < 4096 ✓
        if offset < 512 {
            // 页面已满，需要分配新页面
            return Err(StorageError::RecordTooLarge(data.len()));
        }

        if offset + len > 4096 {
            return Err(StorageError::RecordTooLarge(data.len()));
        }

        // 存储记录元数据
        let meta_offset = 4 + (record_count as usize * 8);
        page.data[meta_offset..meta_offset+4].copy_from_slice(&(offset as u32).to_be_bytes());
        page.data[meta_offset+4..meta_offset+8].copy_from_slice(&(len as u32).to_be_bytes());

        // 存储记录数据
        page.data[offset..offset+len].copy_from_slice(&data);

        // 写回页面
        self.pager.write_page(&page)?;

        // 保存schema（更新next_rowid）
        self.save_schema()?;

        // 更新索引
        self.update_indexes_on_insert(table_name, rowid, &record)?;

        Ok(rowid)
    }

    /// 更新索引（插入时）
    fn update_indexes_on_insert(
        &mut self,
        table_name: &str,
        rowid: u64,
        record: &Record,
    ) -> Result<()> {
        // 收集需要更新的索引信息
        let indexes_to_update: Vec<(String, Value)> = {
            let table = self.get_table(table_name)
                .ok_or(StorageError::KeyNotFound)?;

            self.get_table_indexes(table_name)
                .iter()
                .filter_map(|index| {
                    table.column_index(&index.column)
                        .filter(|&col_idx| col_idx < record.values.len())
                        .map(|col_idx| (index.name.clone(), record.values[col_idx].clone()))
                })
                .collect()
        };

        // 更新索引
        for (index_name, key) in indexes_to_update {
            if let Some(idx) = self.get_index_mut(&index_name) {
                idx.insert(key, rowid)?;
            }
        }
        Ok(())
    }

    /// 查询所有记录
    pub fn select_all(&mut self, table_name: &str) -> Result<Vec<Record>> {
        let table = self.get_table(table_name)
            .ok_or(StorageError::KeyNotFound)?;

        let page_id = table.root_page;
        let page = self.pager.get_page(page_id)?;

        let record_count = u32::from_be_bytes([
            page.data[0], page.data[1], page.data[2], page.data[3]
        ]) as usize;

        // 如果记录数量异常（页面未初始化），则视为空表
        let record_count = if record_count > 1000 { 0 } else { record_count };

        let mut records = Vec::new();

        for i in 0..record_count {
            let meta_offset = 4 + (i * 8);
            let offset = u32::from_be_bytes([
                page.data[meta_offset], page.data[meta_offset+1],
                page.data[meta_offset+2], page.data[meta_offset+3]
            ]) as usize;
            let len = u32::from_be_bytes([
                page.data[meta_offset+4], page.data[meta_offset+5],
                page.data[meta_offset+6], page.data[meta_offset+7]
            ]) as usize;

            // Skip deleted records (len = 0)
            if len == 0 {
                continue;
            }

            let record_data = &page.data[offset..offset+len];
            let record = Record::deserialize(record_data)?;
            records.push(record);
        }

        Ok(records)
    }

    /// 查询所有记录（包含rowid）
    pub fn select_all_with_rowid(&mut self, table_name: &str) -> Result<Vec<(u64, Record)>> {
        let table = self.get_table(table_name)
            .ok_or(StorageError::KeyNotFound)?;

        let page_id = table.root_page;
        let page = self.pager.get_page(page_id)?;

        let record_count = u32::from_be_bytes([
            page.data[0], page.data[1], page.data[2], page.data[3]
        ]) as usize;

        // 如果记录数量异常（页面未初始化），则视为空表
        let record_count = if record_count > 1000 { 0 } else { record_count };

        let mut records = Vec::new();

        for i in 0..record_count {
            let meta_offset = 4 + (i * 8);
            let offset = u32::from_be_bytes([
                page.data[meta_offset], page.data[meta_offset+1],
                page.data[meta_offset+2], page.data[meta_offset+3]
            ]) as usize;
            let len = u32::from_be_bytes([
                page.data[meta_offset+4], page.data[meta_offset+5],
                page.data[meta_offset+6], page.data[meta_offset+7]
            ]) as usize;

            // Skip deleted records (len = 0)
            if len == 0 {
                continue;
            }

            let record_data = &page.data[offset..offset+len];
            let record = Record::deserialize(record_data)?;
            let rowid = (i + 1) as u64;
            records.push((rowid, record));
        }

        Ok(records)
    }

    /// 保存schema到页面
    fn save_schema(&mut self) -> Result<()> {
        let mut page = self.pager.get_page(self.schema_page)?;

        // 表数量
        let table_count = self.tables.len() as u8;
        page.data[0] = table_count;

        let mut pos = 1;
        for table in self.tables.values() {
            let table_data = table.serialize();
            let len = table_data.len();

            // 写入长度
            page.data[pos..pos+4].copy_from_slice(&(len as u32).to_be_bytes());
            pos += 4;

            // 写入数据
            page.data[pos..pos+len].copy_from_slice(&table_data);
            pos += len;
        }

        self.pager.write_page(&page)?;
        self.pager.flush()?;

        Ok(())
    }

    /// 刷新到磁盘
    pub fn flush(&mut self) -> Result<()> {
        self.pager.flush()?;
        Ok(())
    }

    /// 删除记录（简化实现：通过索引标记为删除）
    pub fn delete(&mut self, table_name: &str, rowid: u64) -> Result<()> {
        let table = self.get_table(table_name)
            .ok_or(StorageError::KeyNotFound)?;

        let page_id = table.root_page;
        let mut page = self.pager.get_page(page_id)?;

        let record_count = u32::from_be_bytes([
            page.data[0], page.data[1], page.data[2], page.data[3]
        ]) as usize;

        // 如果记录数量异常，视为空表
        let record_count = if record_count > 1000 { 0 } else { record_count };

        if rowid == 0 || rowid > record_count as u64 {
            return Err(StorageError::KeyNotFound);
        }

        let idx = (rowid - 1) as usize;
        let meta_offset = 4 + (idx * 8);

        // 将记录长度标记为0表示已删除
        page.data[meta_offset+4..meta_offset+8].copy_from_slice(&0u32.to_be_bytes());

        // 写回页面
        self.pager.write_page(&page)?;

        Ok(())
    }

    /// 更新记录
    pub fn update(&mut self, table_name: &str, rowid: u64, record: Record) -> Result<()> {
        let table = self.get_table(table_name)
            .ok_or(StorageError::KeyNotFound)?;

        let page_id = table.root_page;
        let mut page = self.pager.get_page(page_id)?;

        let record_count = u32::from_be_bytes([
            page.data[0], page.data[1], page.data[2], page.data[3]
        ]) as usize;

        // 如果记录数量异常，视为空表
        let record_count = if record_count > 1000 { 0 } else { record_count };

        if rowid == 0 || rowid > record_count as u64 {
            return Err(StorageError::KeyNotFound);
        }

        let idx = (rowid - 1) as usize;
        let meta_offset = 4 + (idx * 8);

        // 获取原记录位置
        let old_offset = u32::from_be_bytes([
            page.data[meta_offset], page.data[meta_offset+1],
            page.data[meta_offset+2], page.data[meta_offset+3]
        ]) as usize;
        let old_len = u32::from_be_bytes([
            page.data[meta_offset+4], page.data[meta_offset+5],
            page.data[meta_offset+6], page.data[meta_offset+7]
        ]) as usize;

        // 序列化新记录
        let data = record.serialize();
        let new_len = data.len();

        // 简化实现：如果新记录长度与旧记录相同，原地更新
        // 如果不同，在当前位置后面追加（会浪费空间，但简化实现）
        if new_len <= old_len {
            // 原地更新
            page.data[old_offset..old_offset+new_len].copy_from_slice(&data);
            // 更新长度
            page.data[meta_offset+4..meta_offset+8].copy_from_slice(&(new_len as u32).to_be_bytes());
        } else {
            // 追加新记录到页面末尾
            let record_count = u32::from_be_bytes([
                page.data[0], page.data[1], page.data[2], page.data[3]
            ]);
            let offset: usize = 4096usize.saturating_sub((record_count as usize + 1) * 256);

            if offset < 512 || offset + new_len > 4096 {
                return Err(StorageError::RecordTooLarge(data.len()));
            }

            // 更新元数据指向新位置
            page.data[meta_offset..meta_offset+4].copy_from_slice(&(offset as u32).to_be_bytes());
            page.data[meta_offset+4..meta_offset+8].copy_from_slice(&(new_len as u32).to_be_bytes());

            // 存储新记录
            page.data[offset..offset+new_len].copy_from_slice(&data);
        }

        // 写回页面
        self.pager.write_page(&page)?;

        Ok(())
    }

    /// 获取单条记录
    pub fn get_record(&mut self, table_name: &str, rowid: u64) -> Result<Record> {
        let table = self.get_table(table_name)
            .ok_or(StorageError::KeyNotFound)?;

        let page_id = table.root_page;
        let page = self.pager.get_page(page_id)?;

        let record_count = u32::from_be_bytes([
            page.data[0], page.data[1], page.data[2], page.data[3]
        ]) as usize;

        // 如果记录数量异常，视为空表
        let record_count = if record_count > 1000 { 0 } else { record_count };

        if rowid == 0 || rowid > record_count as u64 {
            return Err(StorageError::KeyNotFound);
        }

        let idx = (rowid - 1) as usize;
        let meta_offset = 4 + (idx * 8);

        let offset = u32::from_be_bytes([
            page.data[meta_offset], page.data[meta_offset+1],
            page.data[meta_offset+2], page.data[meta_offset+3]
        ]) as usize;
        let len = u32::from_be_bytes([
            page.data[meta_offset+4], page.data[meta_offset+5],
            page.data[meta_offset+6], page.data[meta_offset+7]
        ]) as usize;

        if len == 0 {
            return Err(StorageError::KeyNotFound);
        }

        let record_data = &page.data[offset..offset+len];
        let record = Record::deserialize(record_data)?;

        Ok(record)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::Value;
    use tempfile::NamedTempFile;

    #[test]
    fn test_table_serialize_deserialize() {
        let columns = vec![
            ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                primary_key: true,
            },
            ColumnDef {
                name: "name".to_string(),
                data_type: DataType::Text,
                nullable: true,
                primary_key: false,
            },
        ];

        let table = Table::new("users".to_string(), columns, 2);
        let serialized = table.serialize();
        let deserialized = Table::deserialize(&serialized).unwrap();

        assert_eq!(table.name, deserialized.name);
        assert_eq!(table.columns.len(), deserialized.columns.len());
        assert_eq!(table.root_page, deserialized.root_page);
    }

    #[test]
    fn test_database_create_table() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap();

        let mut db = Database::open(path).unwrap();

        let columns = vec![
            ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                primary_key: true,
            },
            ColumnDef {
                name: "name".to_string(),
                data_type: DataType::Text,
                nullable: true,
                primary_key: false,
            },
        ];

        db.create_table("users".to_string(), columns).unwrap();

        assert!(db.get_table("users").is_some());
        assert_eq!(db.list_tables().len(), 1);
    }

    #[test]
    fn test_database_insert_and_select() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap();

        let mut db = Database::open(path).unwrap();

        // 创建表
        let columns = vec![
            ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                primary_key: true,
            },
            ColumnDef {
                name: "name".to_string(),
                data_type: DataType::Text,
                nullable: true,
                primary_key: false,
            },
        ];
        db.create_table("users".to_string(), columns).unwrap();

        // 插入记录
        let record = Record::new(vec![
            Value::Integer(1),
            Value::Text("Alice".to_string()),
        ]);
        let rowid = db.insert("users", record).unwrap();
        assert_eq!(rowid, 1);

        // 查询记录
        let records = db.select_all("users").unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].values[0], Value::Integer(1));
        assert_eq!(records[0].values[1], Value::Text("Alice".to_string()));
    }
}
