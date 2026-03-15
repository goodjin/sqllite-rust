# 开发计划 - MOD-01: Storage Engine (存储引擎)

## 文档信息
- **模块编号**: MOD-01
- **模块名称**: Storage Engine (存储引擎)
- **所属层次**: L4-存储层
- **对应架构**: docs/v1/02-architecture/03-mod-01-storage.md
- **优先级**: P0 (阶段 1)
- **预估工时**: 3天

---

## 1. 模块概述

### 1.1 模块职责
- 实现 B+ Tree 数据结构
- 管理记录的序列化和反序列化
- 管理表元数据
- 提供游标遍历接口

### 1.2 对应PRD
| PRD编号 | 功能 | 用户故事 |
|---------|-----|---------|
| FR-001 | B+ Tree 存储引擎 | - |
| FR-003 | 基础 CRUD 操作 | US-003~006 |
| FR-004 | 定长记录存储 | - |

### 1.3 架构定位
```
L3: 事务层 ←→ L4: Storage Engine ←→ L5: Pager
```

---

## 2. 技术设计

### 2.1 目录结构
```
src/storage/
├── mod.rs           # 模块入口，公共接口
├── btree.rs         # B+ Tree 实现
├── node.rs          # B+ Tree 节点操作
├── record.rs        # 记录定义和序列化
├── schema.rs        # 表元数据管理
├── cursor.rs        # 游标实现
└── error.rs         # 错误类型
```

### 2.2 依赖关系
| 依赖模块 | 依赖方式 | 用途 |
|---------|---------|------|
| MOD-02 Pager | use crate::pager | 页面管理 |

---

## 3. 接口清单

| 任务编号 | 接口编号 | 接口名称 | 复杂度 |
|---------|---------|---------|-------|
| T-03 | API-001 | BTree::insert | 高 |
| T-04 | API-002 | BTree::search | 中 |
| T-05 | API-003 | BTree::delete | 高 |
| T-06 | API-004 | BTree::scan | 中 |
| T-07 | API-005 | Schema::create_table | 中 |
| T-08 | API-006 | Schema::get_table | 低 |

---

## 4. 开发任务拆分

### 任务清单

| 任务编号 | 任务名称 | 涉及文件 | 代码行数 | 依赖 |
|---------|---------|---------|---------|------|
| T-01 | 错误类型定义 | 1 | ~40 | - |
| T-02 | Value/Record 定义 | 2 | ~100 | T-01 |
| T-03 | B+ Tree 节点操作 | 2 | ~150 | T-01 |
| T-04 | B+ Tree 插入 | 2 | ~200 | T-03 |
| T-05 | B+ Tree 搜索和扫描 | 2 | ~150 | T-03 |
| T-06 | B+ Tree 删除 | 2 | ~200 | T-03 |
| T-07 | 表元数据管理 | 2 | ~120 | T-02 |
| T-08 | 单元测试 | 6 | ~250 | T-01~07 |

---

## 5. 详细任务定义

### T-01: 错误类型定义

**任务概述**: 定义 Storage 模块的错误类型

**输出**:
- `src/storage/error.rs`

**实现要求**:
```rust
use thiserror::Error;

#[derive(Error, Debug)]
pub enum StorageError {
    #[error("Duplicate key: {0}")]
    DuplicateKey(String),

    #[error("Key not found: {0}")]
    KeyNotFound(String),

    #[error("Table not found: {0}")]
    TableNotFound(String),

    #[error("Column not found: {0}")]
    ColumnNotFound(String),

    #[error("Page error: {0}")]
    PageError(String),

    #[error("Serialization error: {0}")]
    SerializationError(String),
}

pub type Result<T> = std::result::Result<T, StorageError>;
```

**验收标准**:
- [ ] 所有错误类型定义完整

**预估工时**: 0.5小时

---

### T-02: Value/Record 定义

**任务概述**: 定义数据类型和记录结构

**对应架构**:
- 数据结构规约: DATA-003

**输出**:
- `src/storage/record.rs`

**实现要求**:
```rust
#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub enum Value {
    Null,
    Integer(i64),
    Text(String),
}

#[derive(Debug, Clone)]
pub struct Record {
    pub row_id: RowId,
    pub values: Vec<Value>,
}

pub type RowId = i64;

pub struct RecordSerializer;

impl RecordSerializer {
    pub fn serialize(record: &Record) -> Vec<u8> {
        // 序列化记录
    }

    pub fn deserialize(data: &[u8]) -> Result<Record> {
        // 反序列化记录
    }
}
```

**验收标准**:
- [ ] 支持 Integer 和 Text 类型
- [ ] 序列化/反序列化正确

**测试要求**:
- 测试用例: 4个（序列化、反序列化、各种类型）

**预估工时**: 2小时

**依赖**: T-01

---

### T-03: B+ Tree 节点操作

**任务概述**: 实现 B+ Tree 节点结构

**对应架构**:
- 核心设计: BTreeNode

**输出**:
- `src/storage/node.rs`

**实现要求**:
```rust
use crate::pager::{Page, PageId};

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum NodeType {
    Internal,
    Leaf,
}

pub struct BTreeNode {
    pub page_id: PageId,
    pub node_type: NodeType,
    pub keys: Vec<Vec<u8>>,
    pub values: Vec<Vec<u8>>,
    pub children: Vec<PageId>,
    pub next_leaf: Option<PageId>,
}

impl BTreeNode {
    pub fn from_page(page: &Page) -> Result<Self> {
        // 从页面解析节点
    }

    pub fn to_page(&self, page: &mut Page) -> Result<()> {
        // 将节点写入页面
    }

    pub fn is_full(&self) -> bool {
        // 检查节点是否已满
    }

    pub fn insert_key_value(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        // 插入键值对
    }

    pub fn search(&self, key: &[u8]) -> Option<&[u8]> {
        // 搜索键
    }
}
```

**验收标准**:
- [ ] 节点与页面格式转换正确
- [ ] 支持内部节点和叶子节点

**测试要求**:
- 测试用例: 3个（解析、序列化、搜索）

**预估工时**: 3小时

**依赖**: T-01

---

### T-04: B+ Tree 插入

**任务概述**: 实现 B+ Tree 插入操作（含节点分裂）

**对应架构**:
- 接口规约: API-001

**输出**:
- `src/storage/btree.rs`（插入部分）

**实现要求**:
```rust
pub struct BTree {
    root_page_id: PageId,
    pager: Arc<Mutex<Pager>>,
}

impl BTree {
    pub fn insert(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        // 插入键值对
        // 处理根节点分裂
    }

    fn insert_non_full(&mut self, page_id: PageId, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        // 在非满节点插入
    }

    fn split_child(&mut self, parent_id: PageId, child_idx: usize, child_id: PageId) -> Result<()> {
        // 分裂子节点
    }

    fn split_root(&mut self) -> Result<()> {
        // 分裂根节点
    }
}
```

**验收标准**:
- [ ] 插入后树结构正确
- [ ] 节点分裂正确
- [ ] 根节点分裂正确处理

**测试要求**:
- 测试用例: 5个（单节点、分裂、根分裂、重复键）

**预估工时**: 6小时

**依赖**: T-03

---

### T-05: B+ Tree 搜索和扫描

**任务概述**: 实现 B+ Tree 搜索和游标扫描

**对应架构**:
- 接口规约: API-002, API-004

**输出**:
- `src/storage/btree.rs`（搜索部分）
- `src/storage/cursor.rs`

**实现要求**:
```rust
impl BTree {
    pub fn search(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        // 搜索键
    }

    fn search_in_node(&self, page_id: PageId, key: &[u8]) -> Result<Option<Vec<u8>>> {
        // 在节点中搜索
    }

    pub fn scan(&self) -> Cursor {
        // 创建游标
    }
}

pub struct Cursor {
    current_page_id: PageId,
    current_index: usize,
    pager: Arc<Mutex<Pager>>,
}

impl Iterator for Cursor {
    type Item = (Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        // 遍历记录
    }
}
```

**验收标准**:
- [ ] 搜索返回正确结果
- [ ] 游标遍历所有记录
- [ ] 遍历结果有序

**测试要求**:
- 测试用例: 4个（搜索、扫描、空树、多节点）

**预估工时**: 4小时

**依赖**: T-03

---

### T-06: B+ Tree 删除

**任务概述**: 实现 B+ Tree 删除操作（含节点合并）

**对应架构**:
- 接口规约: API-003

**输出**:
- `src/storage/btree.rs`（删除部分）

**实现要求**:
```rust
impl BTree {
    pub fn delete(&mut self, key: &[u8]) -> Result<()> {
        // 删除键值对
    }

    fn delete_from_node(&mut self, page_id: PageId, key: &[u8]) -> Result<bool> {
        // 从节点删除
    }

    fn merge_or_redistribute(&mut self, parent_id: PageId, idx: usize) -> Result<()> {
        // 合并或重分配
    }

    fn merge_nodes(&mut self, left_id: PageId, right_id: PageId, parent_id: PageId) -> Result<()> {
        // 合并两个节点
    }
}
```

**验收标准**:
- [ ] 删除后树结构正确
- [ ] 节点合并正确
- [ ] 重分配正确

**测试要求**:
- 测试用例: 5个（删除、合并、重分配、根节点）

**预估工时**: 6小时

**依赖**: T-03

---

### T-07: 表元数据管理

**任务概述**: 实现表结构定义和元数据管理

**对应架构**:
- 数据结构规约: DATA-003

**输出**:
- `src/storage/schema.rs`

**实现要求**:
```rust
#[derive(Debug, Clone)]
pub struct TableSchema {
    pub name: String,
    pub columns: Vec<ColumnDef>,
    pub root_page_id: PageId,
}

#[derive(Debug, Clone)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub primary_key: bool,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum DataType {
    Integer,
    Text,
}

pub struct SchemaManager {
    pager: Arc<Mutex<Pager>>,
}

impl SchemaManager {
    pub fn create_table(&mut self, schema: TableSchema) -> Result<()> {
        // 创建表
    }

    pub fn get_table(&self, name: &str) -> Result<Option<TableSchema>> {
        // 获取表结构
    }

    pub fn drop_table(&mut self, name: &str) -> Result<()> {
        // 删除表
    }
}
```

**验收标准**:
- [ ] 表元数据持久化
- [ ] 支持列定义

**测试要求**:
- 测试用例: 3个（创建、获取、删除）

**预估工时**: 3小时

**依赖**: T-02

---

### T-08: 单元测试

**任务概述**: 编写完整的单元测试

**输出**:
- 各文件中的 `#[cfg(test)]` 模块

**测试清单**:
| 测试目标 | 测试文件 | 用例数 |
|---------|---------|-------|
| Record | record.rs | 4 |
| BTreeNode | node.rs | 3 |
| BTree 插入 | btree.rs | 5 |
| BTree 搜索 | btree.rs | 4 |
| BTree 删除 | btree.rs | 5 |
| Schema | schema.rs | 3 |

**预估工时**: 4小时

**依赖**: T-01~07

---

## 6. 验收清单

- [ ] B+ Tree 插入正确（含分裂）
- [ ] B+ Tree 搜索正确
- [ ] B+ Tree 删除正确（含合并）
- [ ] 游标遍历正确
- [ ] 表元数据管理正确
- [ ] 测试覆盖率 ≥ 80%

---

## 7. 覆盖映射

| 架构元素 | 架构编号 | 任务 | 覆盖状态 |
|---------|---------|------|---------|
| 错误类型 | - | T-01 | ✅ |
| Value/Record | DATA-003 | T-02 | ✅ |
| BTreeNode | - | T-03 | ✅ |
| BTree::insert | API-001 | T-04 | ✅ |
| BTree::search | API-002 | T-05 | ✅ |
| BTree::delete | API-003 | T-06 | ✅ |
| BTree::scan | API-004 | T-05 | ✅ |
| TableSchema | DATA-003 | T-07 | ✅ |

---

## 变更历史

| 版本 | 日期 | 变更内容 | 作者 |
|-----|------|---------|------|
| 1.0 | 2026-03-14 | 初始版本 | Claude |
