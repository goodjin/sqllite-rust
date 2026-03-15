# MOD-06: 索引管理器模块 (Index Manager)

## 文档信息
- **项目名称**: sqllite-rust
- **文档编号**: MOD-06
- **版本**: v1.0
- **更新日期**: 2026-03-14
- **对应PRD**: FR-016

---

## 目录

1. [系统定位](#系统定位)
2. [对应PRD](#对应prd)
3. [全局架构位置](#全局架构位置)
4. [依赖关系](#依赖关系)
5. [数据流](#数据流)
6. [核心设计](#核心设计)
7. [接口定义](#接口定义)
8. [数据结构](#数据结构)
9. [状态机设计](#状态机设计)
10. [边界条件](#边界条件)
11. [非功能需求](#非功能需求)
12. [实现文件](#实现文件)
13. [验收标准](#验收标准)
14. [覆盖映射](#覆盖映射)

---

## 系统定位

### 在整体架构中的位置

**所属层次**: L4-存储层

**架构定位图**:
```
┌─────────────────────────────────────────────────────┐
│              L2: 执行层 (Execution Layer)            │
│              Virtual Machine                         │
└───────────────────────┬─────────────────────────────┘
                        │ ▼ 索引操作
┌─────────────────────────────────────────────────────┐
│         ★ MOD-06: 索引管理器 (Index Manager) ★       │
│         B+ Tree Index, Index Scan                    │
└───────────────────────┬─────────────────────────────┘
                        │ ▼ B+ Tree 操作
┌─────────────────────────────────────────────────────┐
│              L4: 存储层 (Storage Layer)              │
│              B+ Tree (MOD-01)                        │
└─────────────────────────────────────────────────────┘
```

### 核心职责

- **索引创建**: CREATE INDEX 的实现
- **索引维护**: INSERT/UPDATE/DELETE 时自动维护索引
- **索引扫描**: 使用索引加速查询
- **索引选择**: 查询优化时选择合适索引

### 边界说明

- **负责**:
  - 索引元数据管理
  - 索引 B+ Tree 操作
  - 索引扫描实现
  - 自动维护索引一致性

- **不负责**:
  - SQL 解析（由 Parser 负责）
  - 查询优化决策（由 Optimizer 负责）
  - 底层 B+ Tree 实现（由 Storage Engine 负责）

---

## 对应PRD

| PRD章节 | 编号 | 内容 |
|---------|-----|------|
| 功能需求 | FR-016 | B+ Tree 索引 |
| 用户故事 | US-008 | 创建索引 |
| 数据实体 | Entity-005 | 索引元数据 |
| 验收标准 | AC-008-01~03 | 索引功能验收 |

---

## 全局架构位置

```
┌─────────────────────────────────────────────────────────────────┐
│                        L2: 执行层                                │
│  ┌───────────────────────────────────────────────────────────┐ │
│  │              Virtual Machine (MOD-04)                      │ │
│  └───────────────────────────┬───────────────────────────────┘ │
└──────────────────────────────┼──────────────────────────────────┘
                               │ 索引操作
                               ▼
┌──────────────────────────────┬──────────────────────────────────┐
│                        L4: 存储层                                │
│  ┌───────────────────────────▼───────────────────────────────┐ │
│  │              ★ MOD-06 Index Manager ★                      │ │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐ │ │
│  │  │Index Metadata│  │ B+ Tree Idx │  │    Index Scan       │ │ │
│  │  │             │  │             │  │                     │ │ │
│  │  └─────────────┘  └─────────────┘  └─────────────────────┘ │ │
│  └───────────────────────────┬───────────────────────────────┘ │
└──────────────────────────────┼──────────────────────────────────┘
                               │ B+ Tree 操作
                               ▼
┌──────────────────────────────┬──────────────────────────────────┐
│                        L4: 存储层                                │
│  ┌───────────────────────────▼───────────────────────────────┐ │
│  │              Storage Engine (MOD-01)                       │ │
│  └───────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
```

---

## 依赖关系

### 上游依赖（本模块调用的模块）

| 模块名称 | 模块编号 | 依赖原因 | 调用方式 |
|---------|---------|---------|---------|
| Storage Engine | MOD-01 | B+ Tree 操作 | StorageLayer trait |

### 下游依赖（调用本模块的模块）

| 模块名称 | 模块编号 | 被调用场景 | 调用方式 |
|---------|---------|-----------|---------|
| Virtual Machine | MOD-04 | 索引操作指令 | IndexLayer trait |
| Query Optimizer | MOD-04 | 索引选择 | IndexLayer trait |

---

## 数据流

### 输入数据流

| 数据项 | 来源 | 格式 | 说明 |
|-------|------|------|------|
| 创建索引 | VM | CreateIndexStmt | CREATE INDEX 语句 |
| 表数据修改 | VM | Record | INSERT/UPDATE/DELETE |
| 索引查询 | VM | Key | 索引查找请求 |

### 输出数据流

| 数据项 | 目标 | 格式 | 说明 |
|-------|------|------|------|
| 行 ID 列表 | VM | Vec<RowId> | 索引查找结果 |
| 索引元数据 | Storage | IndexMetadata | 持久化的索引信息 |

---

## 核心设计

### 设计目标

| 目标 | 描述 | 度量标准 |
|-----|------|---------|
| 查询加速 | 索引查询比全表扫描快 | 点查 < 1ms |
| 维护开销 | 索引维护不影响写入性能 | 写入 overhead < 50% |
| 空间效率 | 索引空间合理 | 索引大小 ≈ 数据大小 |

### 核心组件

#### 1. 索引管理器

```rust
/// 索引管理器
pub struct IndexManager {
    /// 存储层引用
    storage: Arc<dyn StorageLayer>,
    /// 索引缓存（表名 -> 索引列表）
    index_cache: HashMap<String, Vec<IndexInfo>>,
}

/// 索引信息
#[derive(Debug, Clone)]
pub struct IndexInfo {
    /// 索引名
    pub name: String,
    /// 所属表
    pub table: String,
    /// 索引列
    pub column: String,
    /// 根页面 ID
    pub root_page_id: PageId,
    /// 是否唯一索引
    pub unique: bool,
}

/// 索引键（用于 B+ Tree）
#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub struct IndexKey {
    /// 索引列值
    pub value: Value,
    /// 行 ID（用于唯一标识记录，处理重复值）
    pub row_id: RowId,
}

impl IndexManager {
    /// 创建索引
    pub fn create_index(
        &mut self,
        index_name: &str,
        table: &str,
        column: &str,
        unique: bool,
    ) -> Result<(), IndexError> {
        // 1. 验证表和列存在
        let table_schema = self.storage.get_table_schema(table)?
            .ok_or(IndexError::TableNotFound(table.to_string()))?;

        let column_idx = table_schema.columns.iter()
            .position(|c| c.name == column)
            .ok_or(IndexError::ColumnNotFound(column.to_string()))?;

        // 2. 分配新的根页面
        let root_page_id = self.storage.allocate_page()?;

        // 3. 初始化空的 B+ Tree
        self.storage.init_btree(root_page_id)?;

        // 4. 扫描表数据，构建索引
        self.build_index(table, column_idx, root_page_id, unique)?;

        // 5. 保存索引元数据
        let index_info = IndexInfo {
            name: index_name.to_string(),
            table: table.to_string(),
            column: column.to_string(),
            root_page_id,
            unique,
        };
        self.save_index_metadata(&index_info)?;

        // 6. 更新缓存
        self.index_cache
            .entry(table.to_string())
            .or_insert_with(Vec::new)
            .push(index_info);

        Ok(())
    }

    /// 删除索引
    pub fn drop_index(&mut self, index_name: &str) -> Result<(), IndexError> {
        // 1. 查找索引元数据
        let index_info = self.load_index_metadata(index_name)?
            .ok_or(IndexError::IndexNotFound(index_name.to_string()))?;

        // 2. 释放索引占用的页面
        self.storage.free_btree(index_info.root_page_id)?;

        // 3. 删除索引元数据
        self.delete_index_metadata(index_name)?;

        // 4. 更新缓存
        if let Some(indexes) = self.index_cache.get_mut(&index_info.table) {
            indexes.retain(|i| i.name != index_name);
        }

        Ok(())
    }

    /// 插入索引条目（在表插入记录时调用）
    pub fn insert_into_index(
        &mut self,
        index_name: &str,
        value: &Value,
        row_id: RowId,
    ) -> Result<(), IndexError> {
        let index_info = self.load_index_metadata(index_name)?
            .ok_or(IndexError::IndexNotFound(index_name.to_string()))?;

        let key = IndexKey {
            value: value.clone(),
            row_id,
        };

        // 检查唯一性约束
        if index_info.unique {
            let existing = self.search_index(index_name, value)?;
            if !existing.is_empty() {
                return Err(IndexError::DuplicateKey(value.to_string()));
            }
        }

        // 插入到 B+ Tree
        self.storage.btree_insert(
            index_info.root_page_id,
            key.to_bytes(),
            row_id.to_bytes(),
        )?;

        Ok(())
    }

    /// 从索引删除条目（在表删除记录时调用）
    pub fn delete_from_index(
        &mut self,
        index_name: &str,
        value: &Value,
        row_id: RowId,
    ) -> Result<(), IndexError> {
        let index_info = self.load_index_metadata(index_name)?
            .ok_or(IndexError::IndexNotFound(index_name.to_string()))?;

        let key = IndexKey {
            value: value.clone(),
            row_id,
        };

        self.storage.btree_delete(
            index_info.root_page_id,
            &key.to_bytes(),
        )?;

        Ok(())
    }

    /// 更新索引条目（在表更新记录时调用）
    pub fn update_index(
        &mut self,
        index_name: &str,
        old_value: &Value,
        new_value: &Value,
        row_id: RowId,
    ) -> Result<(), IndexError> {
        // 先删除旧值，再插入新值
        self.delete_from_index(index_name, old_value, row_id)?;
        self.insert_into_index(index_name, new_value, row_id)?;
        Ok(())
    }

    /// 使用索引查找（返回行 ID 列表）
    pub fn search_index(
        &self,
        index_name: &str,
        value: &Value,
    ) -> Result<Vec<RowId>, IndexError> {
        let index_info = self.load_index_metadata(index_name)?
            .ok_or(IndexError::IndexNotFound(index_name.to_string()))?;

        // 构建搜索键（row_id 设为 0，表示查找所有匹配的记录）
        let search_key = IndexKey {
            value: value.clone(),
            row_id: 0,
        };

        // 在 B+ Tree 中查找
        let mut row_ids = Vec::new();
        let cursor = self.storage.btree_range_scan(
            index_info.root_page_id,
            &search_key.to_bytes(),
            &search_key.to_bytes(), // 范围查询，查找所有匹配的键
        )?;

        for (key_bytes, value_bytes) in cursor {
            let key = IndexKey::from_bytes(&key_bytes)?;
            // 验证键值匹配
            if key.value == *value {
                let row_id = RowId::from_bytes(&value_bytes)?;
                row_ids.push(row_id);
            }
        }

        Ok(row_ids)
    }

    /// 获取表的所有索引
    pub fn get_table_indexes(&self, table: &str) -> Result<Vec<IndexInfo>, IndexError> {
        // 先从缓存查找
        if let Some(indexes) = self.index_cache.get(table) {
            return Ok(indexes.clone());
        }

        // 从存储加载
        let indexes = self.load_table_indexes(table)?;
        Ok(indexes)
    }

    /// 查找可用于查询的索引
    pub fn find_usable_index(
        &self,
        table: &str,
        column: &str,
    ) -> Result<Option<IndexInfo>, IndexError> {
        let indexes = self.get_table_indexes(table)?;

        for index in indexes {
            if index.column == column {
                return Ok(Some(index));
            }
        }

        Ok(None)
    }

    // 辅助方法
    fn build_index(
        &mut self,
        table: &str,
        column_idx: usize,
        root_page_id: PageId,
        unique: bool,
    ) -> Result<(), IndexError> {
        // 扫描表的所有记录
        let cursor = self.storage.btree_scan_table(table)?;

        for record in cursor {
            let value = record.values.get(column_idx)
                .cloned()
                .unwrap_or(Value::Null);

            let key = IndexKey {
                value,
                row_id: record.row_id,
            };

            // 检查唯一性
            if unique {
                // 需要检查是否已存在相同的键值
                // 简化处理：依赖 B+ Tree 的重复键检测
            }

            self.storage.btree_insert(
                root_page_id,
                key.to_bytes(),
                record.row_id.to_bytes(),
            )?;
        }

        Ok(())
    }

    fn save_index_metadata(&self, index_info: &IndexInfo) -> Result<(), IndexError> {
        // 将索引元数据保存到 sqlite_master 表
        // 格式: type='index', name=index_name, tbl_name=table, rootpage=root_page_id, sql=CREATE INDEX ...
        Ok(())
    }

    fn load_index_metadata(&self, index_name: &str) -> Result<Option<IndexInfo>, IndexError> {
        // 从 sqlite_master 表加载索引元数据
        Ok(None) // 简化实现
    }

    fn delete_index_metadata(&self, index_name: &str) -> Result<(), IndexError> {
        // 从 sqlite_master 表删除索引元数据
        Ok(())
    }

    fn load_table_indexes(&self, table: &str) -> Result<Vec<IndexInfo>, IndexError> {
        // 从 sqlite_master 表加载指定表的所有索引
        Ok(Vec::new()) // 简化实现
    }
}

impl IndexKey {
    /// 序列化为字节
    fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();

        // 序列化 value
        match &self.value {
            Value::Null => bytes.push(0),
            Value::Integer(i) => {
                bytes.push(1);
                bytes.extend_from_slice(&i.to_be_bytes());
            }
            Value::Text(s) => {
                bytes.push(2);
                let s_bytes = s.as_bytes();
                bytes.extend_from_slice(&(s_bytes.len() as u16).to_be_bytes());
                bytes.extend_from_slice(s_bytes);
            }
        }

        // 序列化 row_id
        bytes.extend_from_slice(&self.row_id.to_be_bytes());

        bytes
    }

    /// 从字节反序列化
    fn from_bytes(bytes: &[u8]) -> Result<Self, IndexError> {
        // 简化实现
        unimplemented!()
    }
}
```

---

## 接口定义

### 对外接口清单

| 接口编号 | 接口名称 | 方法 | 对应PRD |
|---------|---------|------|---------|
| API-014 | IndexManager::create_index | fn create_index(...) -> Result<()> | FR-016 |
| API-015 | IndexManager::search_index | fn search_index(...) -> Result<Vec<RowId>> | FR-016 |

### 接口详细定义

#### API-014: IndexManager::create_index

**对应PRD**:
- 用户故事: US-008
- 验收标准: AC-008-01

**接口定义**:
```rust
/// 创建索引
///
/// # Arguments
/// * `index_name` - 索引名称
/// * `table` - 表名
/// * `column` - 列名
/// * `unique` - 是否唯一索引
///
/// # Returns
/// * `Ok(())` - 创建成功
/// * `Err(IndexError::TableNotFound)` - 表不存在
/// * `Err(IndexError::ColumnNotFound)` - 列不存在
/// * `Err(IndexError::DuplicateKey)` - 唯一索引约束冲突
pub fn create_index(
    &mut self,
    index_name: &str,
    table: &str,
    column: &str,
    unique: bool,
) -> Result<(), IndexError>
```

#### API-015: IndexManager::search_index

**对应PRD**:
- 用户故事: US-008
- 验收标准: AC-008-02

**接口定义**:
```rust
/// 使用索引查找
///
/// # Arguments
/// * `index_name` - 索引名称
/// * `value` - 查找值
///
/// # Returns
/// * `Ok(Vec<RowId>)` - 匹配的行 ID 列表
/// * `Err(IndexError::IndexNotFound)` - 索引不存在
pub fn search_index(
    &self,
    index_name: &str,
    value: &Value,
) -> Result<Vec<RowId>, IndexError>
```

---

## 数据结构

### 核心实体

已在核心设计部分定义，见 [索引管理器](#1-索引管理器)。

---

## 状态机设计

本模块无复杂状态机。

---

## 边界条件

### BOUND-001: 表不存在

**对应PRD**: FR-016

**触发条件**:
- 创建索引时指定的表不存在

**处理方式**:
- 返回 TableNotFound 错误

### BOUND-002: 列不存在

**对应PRD**: FR-016

**触发条件**:
- 创建索引时指定的列不存在

**处理方式**:
- 返回 ColumnNotFound 错误

### BOUND-003: 唯一索引冲突

**对应PRD**: FR-016

**触发条件**:
- 创建唯一索引时表中已有重复值
- 向唯一索引插入重复值

**处理方式**:
- 返回 DuplicateKey 错误

### BOUND-004: 索引不存在

**对应PRD**: FR-016

**触发条件**:
- 使用不存在的索引进行查找

**处理方式**:
- 返回 IndexNotFound 错误

---

## 非功能需求

### 性能要求

| 指标 | 要求 | 对应PRD |
|-----|------|---------|
| 索引查询 | 点查 < 1ms | FR-016 |
| 索引创建 | 与数据量线性相关 | FR-016 |
| 索引维护 | 写入 overhead < 50% | FR-016 |

### 空间要求

| 需求 | 描述 | 实现方案 |
|-----|------|---------|
| 索引空间 | 合理占用 | B+ Tree 存储 |
| 缓存效率 | 热索引常驻内存 | LRU 缓存 |

---

## 实现文件

| 文件路径 | 职责 |
|---------|------|
| src/index/mod.rs | 模块入口，IndexManager |
| src/index/metadata.rs | 索引元数据 |
| src/index/key.rs | 索引键定义 |
| src/index/scan.rs | 索引扫描 |
| src/index/error.rs | 错误类型 |

---

## 验收标准

| 标准 | 要求 | 验证方法 | 对应PRD |
|-----|------|---------|---------|
| 标准1 | CREATE INDEX 正确创建 | 单元测试：验证索引元数据 | FR-016 |
| 标准2 | 索引加速查询 | 单元测试：对比有无索引的查询时间 | FR-016 |
| 标准3 | 自动维护索引 | 单元测试：INSERT/DELETE 后验证索引 | FR-016 |
| 标准4 | 唯一索引约束 | 单元测试：验证重复值被拒绝 | FR-016 |
| 标准5 | 索引扫描正确 | 单元测试：验证范围查询结果 | FR-016 |

---

## 覆盖映射

### PRD需求覆盖情况

| PRD类型 | PRD编号 | 架构元素 | 覆盖状态 |
|---------|---------|---------|---------|
| 功能需求 | FR-016 | IndexManager | ✅ |
| 用户故事 | US-008 | create_index, search_index | ✅ |
| 数据实体 | Entity-005 | IndexInfo | ✅ |
| 验收标准 | AC-008-01~03 | 索引接口 | ✅ |

---

## 变更历史

| 版本 | 日期 | 变更内容 | 作者 |
|-----|------|---------|------|
| 1.0 | 2026-03-14 | 初始版本 | Claude |
