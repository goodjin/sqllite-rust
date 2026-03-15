# MOD-01: 存储引擎模块 (Storage Engine)

## 文档信息
- **项目名称**: sqllite-rust
- **文档编号**: MOD-01
- **版本**: v1.0
- **更新日期**: 2026-03-14
- **对应PRD**: FR-001, FR-003, FR-004

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
│              L3: 事务层 (Transaction Layer)          │
│              Transaction Manager                     │
└───────────────────────┬─────────────────────────────┘
                        │ ▼ 调用 (StorageLayer trait)
┌─────────────────────────────────────────────────────┐
│         ★ MOD-01: 存储引擎 (Storage Engine) ★        │
│         B+ Tree, Record, Serializer                  │
└───────────────────────┬─────────────────────────────┘
                        │ ▼ 依赖 (PageManagementLayer trait)
┌─────────────────────────────────────────────────────┐
│              L5: 页管理层 (Page Management)          │
│              Pager, PageCache                        │
└─────────────────────────────────────────────────────┘
```

### 核心职责

- **B+ Tree 实现**: 实现磁盘上的 B+ Tree 数据结构，支持高效的增删改查和范围扫描
- **记录管理**: 定义记录的序列化和反序列化格式，支持定长数据类型
- **表元数据管理**: 存储和管理表结构信息（列定义、数据类型等）
- **游标管理**: 提供遍历 B+ Tree 的游标接口

### 边界说明

- **负责**:
  - B+ Tree 节点的分裂和合并
  - 记录的序列化/反序列化
  - 表元数据的存储
  - 游标遍历实现

- **不负责**:
  - 页面缓存管理（由 Pager 负责）
  - 事务管理（由 Transaction Manager 负责）
  - SQL 解析（由 SQL Parser 负责）
  - 索引管理（由 Index Manager 负责）

---

## 对应PRD

| PRD章节 | 编号 | 内容 |
|---------|-----|------|
| 功能需求 | FR-001 | B+ Tree 存储引擎 |
| 功能需求 | FR-003 | 基础 CRUD 操作 |
| 功能需求 | FR-004 | 定长记录存储 |
| 用户故事 | US-001 | 创建/打开数据库文件 |
| 用户故事 | US-002 | 创建表 |
| 数据实体 | Entity-001 | 数据库文件头 |
| 数据实体 | Entity-002 | B+ Tree 页面 |
| 数据实体 | Entity-003 | 表元数据 |
| 数据实体 | Entity-004 | 列定义 |

---

## 全局架构位置

```
┌─────────────────────────────────────────────────────────────────┐
│                        L1: 接口层                                │
└─────────────────────────────────────────────────────────────────┘
                              │
┌─────────────────────────────────────────────────────────────────┐
│                        L2: 执行层                                │
└─────────────────────────────────────────────────────────────────┘
                              │
┌─────────────────────────────────────────────────────────────────┐
│                        L3: 事务层                                │
└─────────────────────────────────────────────────────────────────┘
                              │
┌─────────────────────────────────────────────────────────────────┐
│  L4: 存储层                                                      │
│  ┌─────────────┐  ┌─────────────────────────────────────────┐   │
│  │Index Manager│  │          ★ MOD-01 Storage ★              │   │
│  │  (MOD-06)   │  │  ┌─────────┐  ┌─────────┐  ┌─────────┐  │   │
│  └──────┬──────┘  │  │ B+ Tree │  │ Record  │  │ Schema  │  │   │
│         │         │  │         │  │         │  │ Manager │  │   │
│         └────────→│  └────┬────┘  └─────────┘  └─────────┘  │   │
│                   └───────┼─────────────────────────────────┘   │
└───────────────────────────┼─────────────────────────────────────┘
                            │
┌───────────────────────────▼─────────────────────────────────────┐
│                        L5: 页管理层                              │
└─────────────────────────────────────────────────────────────────┘
```

---

## 依赖关系

### 上游依赖（本模块调用的模块）

| 模块名称 | 模块编号 | 依赖原因 | 调用方式 |
|---------|---------|---------|---------|
| Pager | MOD-02 | 页面读写、分配 | PageManagementLayer trait |

### 下游依赖（调用本模块的模块）

| 模块名称 | 模块编号 | 被调用场景 | 调用方式 |
|---------|---------|-----------|---------|
| Virtual Machine | MOD-04 | 执行 INSERT/UPDATE/DELETE | StorageLayer trait |
| Index Manager | MOD-06 | 存储索引数据 | StorageLayer trait |

### 外部依赖

| 依赖项 | 类型 | 用途 | 版本要求 |
|-------|------|------|---------|
| std::io | 标准库 | 文件 I/O 类型 | Rust 1.70+ |
| thiserror | crate | 错误定义 | 1.0+ |

---

## 数据流

### 输入数据流

| 数据项 | 来源 | 格式 | 说明 |
|-------|------|------|------|
| SQL 操作 | VM | 字节码指令 | INSERT/UPDATE/DELETE |
| 表名 | VM | String | 目标表 |
| 记录数据 | VM | Vec<Value> | 要存储的数据 |

### 输出数据流

| 数据项 | 目标 | 格式 | 说明 |
|-------|------|------|------|
| 查询结果 | VM | Option<Record> / Cursor | 查询结果 |
| 页面数据 | Pager | Page | 序列化后的页面 |

---

## 核心设计

### 设计目标

| 目标 | 描述 | 度量标准 |
|-----|------|---------|
| 查询性能 | 点查 O(log N) | 1000 条记录 < 1ms |
| 范围查询 | 范围查 O(log N + M) | 100 条顺序读取 < 1ms |
| 空间效率 | 页面填充率 | > 50% |

### 核心组件

#### 1. B+ Tree 结构

```rust
/// B+ Tree 根结构
pub struct BTree {
    /// 根页面 ID
    root_page_id: PageId,
    /// 关联的 Pager
    pager: Arc<dyn PageManagementLayer>,
    /// 键的比较器
    key_comparator: Box<dyn Fn(&Key, &Key) -> Ordering>,
}

/// B+ Tree 节点类型
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum NodeType {
    /// 内部节点：存储键和子节点指针
    Internal,
    /// 叶子节点：存储键和记录
    Leaf,
}

/// B+ Tree 节点（内存表示）
pub struct BTreeNode {
    /// 页面 ID
    page_id: PageId,
    /// 节点类型
    node_type: NodeType,
    /// 键列表
    keys: Vec<Key>,
    /// 值列表（叶子节点存储记录，内部节点存储子页面 ID）
    values: Vec<Value>,
    /// 子节点页面 ID（仅内部节点使用）
    children: Vec<PageId>,
    /// 右兄弟节点（仅叶子节点使用，用于范围扫描）
    next_leaf: Option<PageId>,
}
```

#### 2. 记录格式

```rust
/// 数据类型
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum DataType {
    Integer,  // 8 字节有符号整数
    Text,     // 定长文本，最大 255 字节
}

/// 值类型
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Integer(i64),
    Text(String),
    Null,
}

/// 记录结构
pub struct Record {
    /// 行 ID（隐藏主键）
    pub row_id: RowId,
    /// 列值
    pub values: Vec<Value>,
}

/// 键类型（用于 B+ Tree）
pub type Key = Value;
```

#### 3. 表元数据

```rust
/// 表结构定义
pub struct TableSchema {
    /// 表名
    pub name: String,
    /// 列定义
    pub columns: Vec<ColumnDef>,
    /// 根页面 ID
    pub root_page_id: PageId,
}

/// 列定义
pub struct ColumnDef {
    /// 列名
    pub name: String,
    /// 数据类型
    pub data_type: DataType,
    /// 是否可为 NULL
    pub nullable: bool,
    /// 是否主键
    pub primary_key: bool,
}
```

#### 4. 页面布局

```
┌─────────────────────────────────────────────────────────────┐
│                     B+ Tree 页面结构 (4KB)                    │
├─────────────────────────────────────────────────────────────┤
│ 页头 (12 bytes)                                              │
│ ├─ node_type: u8        (0x02=内部节点, 0x05=叶子节点)       │
│ ├─ first_freeblock: u16  (第一个空闲块偏移，0=无)            │
│ ├─ cell_count: u16       (单元格数量)                        │
│ ├─ cell_content_offset: u16 (单元格内容起始偏移)             │
│ ├─ fragmented_bytes: u1  (碎片化空闲字节数)                  │
│ └─ right_child: u32      (最右子节点，仅内部节点)            │
├─────────────────────────────────────────────────────────────┤
│ 单元格指针数组 (cell_count * 2 bytes)                         │
│ ├─ offset_1: u16                                             │
│ ├─ offset_2: u16                                             │
│ └─ ...                                                       │
├─────────────────────────────────────────────────────────────┤
│ 未使用空间                                                   │
├─────────────────────────────────────────────────────────────┤
│ 单元格内容（从页尾向前增长）                                  │
│ ├─ 单元格 N                                                  │
│ ├─ 单元格 N-1                                                │
│ └─ ...                                                       │
└─────────────────────────────────────────────────────────────┘
```

**单元格格式（叶子节点）**:
```
┌────────────────────────────────────────┐
│ 单元格头部                              │
│ ├─ key_size: u16    (键的字节数)       │
│ ├─ value_size: u16  (值的字节数)       │
├────────────────────────────────────────┤
│ 键数据 (key_size bytes)                │
├────────────────────────────────────────┤
│ 值数据 (value_size bytes)              │
└────────────────────────────────────────┘
```

**单元格格式（内部节点）**:
```
┌────────────────────────────────────────┐
│ 单元格头部                              │
│ ├─ key_size: u16    (键的字节数)       │
│ ├─ child_page: u32  (左子节点页面 ID)  │
├────────────────────────────────────────┤
│ 键数据 (key_size bytes)                │
└────────────────────────────────────────┘
```

---

## 接口定义

### 对外接口清单

| 接口编号 | 接口名称 | 方法 | 对应PRD |
|---------|---------|------|---------|
| API-001 | BTree::insert | fn insert(&mut self, key: Key, value: Value) | FR-003 |
| API-002 | BTree::search | fn search(&self, key: &Key) -> Option<Value> | FR-003 |
| API-003 | BTree::delete | fn delete(&mut self, key: &Key) -> Result<()> | FR-003 |
| API-004 | BTree::scan | fn scan(&self) -> Cursor | FR-003 |
| API-005 | Schema::create_table | fn create_table(&mut self, schema: TableSchema) | FR-004 |
| API-006 | Schema::get_table | fn get_table(&self, name: &str) -> Option<TableSchema> | FR-004 |

### 接口详细定义

#### API-001: BTree::insert

**对应PRD**:
- 用户故事: US-003
- 验收标准: AC-003-03

**接口定义**:
```rust
/// 插入键值对到 B+ Tree
///
/// # Arguments
/// * `key` - 键
/// * `value` - 值（序列化后的记录）
///
/// # Returns
/// * `Ok(())` - 插入成功
/// * `Err(StorageError::DuplicateKey)` - 键已存在
pub fn insert(&mut self, key: Key, value: Value) -> Result<()>
```

**边界条件**:
| 编号 | 条件 | 处理方式 |
|-----|------|---------|
| BOUND-001 | 键已存在 | 返回 DuplicateKey 错误 |
| BOUND-002 | 页面已满 | 触发节点分裂 |
| BOUND-003 | 根节点分裂 | 创建新的根节点，树高度+1 |

#### API-002: BTree::search

**对应PRD**:
- 用户故事: US-004
- 验收标准: AC-004-01~AC-004-04

**接口定义**:
```rust
/// 在 B+ Tree 中搜索键
///
/// # Arguments
/// * `key` - 要搜索的键
///
/// # Returns
/// * `Some(Value)` - 找到记录
/// * `None` - 未找到
pub fn search(&self, key: &Key) -> Option<Value>
```

#### API-003: BTree::delete

**对应PRD**:
- 用户故事: US-006
- 验收标准: AC-006-01~AC-006-03

**接口定义**:
```rust
/// 从 B+ Tree 删除键值对
///
/// # Arguments
/// * `key` - 要删除的键
///
/// # Returns
/// * `Ok(())` - 删除成功
/// * `Err(StorageError::KeyNotFound)` - 键不存在
pub fn delete(&mut self, key: &Key) -> Result<()>
```

**边界条件**:
| 编号 | 条件 | 处理方式 |
|-----|------|---------|
| BOUND-004 | 键不存在 | 返回 KeyNotFound 错误 |
| BOUND-005 | 节点填充率 < 50% | 触发节点合并或重分配 |

#### API-004: BTree::scan

**对应PRD**:
- 用户故事: US-004
- 验收标准: AC-004-04

**接口定义**:
```rust
/// 创建 B+ Tree 游标，用于全表扫描
///
/// # Returns
/// * `Cursor` - 游标对象，可迭代遍历所有记录
pub fn scan(&self) -> Cursor

/// 游标结构
pub struct Cursor {
    current_page: PageId,
    current_index: usize,
    pager: Arc<dyn PageManagementLayer>,
}

impl Iterator for Cursor {
    type Item = (Key, Value);

    fn next(&mut self) -> Option<Self::Item> {
        // 返回当前记录，移动到下一个
    }
}
```

---

## 数据结构

### 核心实体

#### DATA-001: B+ Tree 节点 (磁盘格式)

**对应PRD**: Entity-002

```rust
/// B+ Tree 页面布局（磁盘格式）
#[repr(C, packed)]
pub struct BTreePageHeader {
    /// 节点类型: 0x02=内部节点, 0x05=叶子节点
    pub node_type: u8,
    /// 第一个空闲块偏移 (0 = 无空闲块)
    pub first_freeblock: u16,
    /// 单元格数量
    pub cell_count: u16,
    /// 单元格内容起始偏移
    pub cell_content_offset: u16,
    /// 碎片化空闲字节数
    pub fragmented_bytes: u8,
    /// 最右子节点页面 ID（仅内部节点）
    pub right_child: u32,
}

impl BTreePageHeader {
    pub const SIZE: usize = 12;
}
```

**字段规约**:
| 字段名 | PRD属性 | 类型 | 约束 | 说明 |
|-------|---------|------|------|------|
| node_type | - | u8 | 0x02 或 0x05 | 内部节点或叶子节点 |
| cell_count | - | u16 | <= 页面容量 | 当前单元格数量 |
| cell_content_offset | - | u16 | >= 页头大小 | 单元格内容起始位置 |

#### DATA-002: 记录序列化格式

```rust
/// 记录序列化器
pub struct RecordSerializer;

impl RecordSerializer {
    /// 将 Record 序列化为字节
    pub fn serialize(record: &Record) -> Vec<u8> {
        let mut buf = Vec::new();

        // 行 ID (8 bytes)
        buf.extend_from_slice(&record.row_id.to_be_bytes());

        // 列数 (2 bytes)
        buf.extend_from_slice(&(record.values.len() as u16).to_be_bytes());

        // 每个列值
        for value in &record.values {
            match value {
                Value::Null => {
                    buf.push(0x00); // 类型标记: NULL
                }
                Value::Integer(i) => {
                    buf.push(0x01); // 类型标记: INTEGER
                    buf.extend_from_slice(&i.to_be_bytes());
                }
                Value::Text(s) => {
                    buf.push(0x02); // 类型标记: TEXT
                    let bytes = s.as_bytes();
                    buf.extend_from_slice(&(bytes.len() as u16).to_be_bytes());
                    buf.extend_from_slice(bytes);
                }
            }
        }

        buf
    }

    /// 从字节反序列化为 Record
    pub fn deserialize(data: &[u8]) -> Result<Record> {
        // 反序列化逻辑...
    }
}
```

#### DATA-003: 表元数据存储

**对应PRD**: Entity-003, Entity-004

```rust
/// 表元数据（存储在 sqlite_master 表中）
pub struct TableMetadata {
    /// 表类型: "table"
    pub table_type: String,
    /// 表名
    pub name: String,
    /// 关联表（对于索引）
    pub tbl_name: String,
    /// 根页面 ID
    pub root_page: PageId,
    /// 创建表的 SQL 语句
    pub sql: String,
}

/// sqlite_master 表结构
/// CREATE TABLE sqlite_master (
///     type TEXT,
///     name TEXT,
///     tbl_name TEXT,
///     rootpage INTEGER,
///     sql TEXT
/// );
```

---

## 状态机设计

本模块无复杂状态机，主要状态由 B+ Tree 的节点结构决定。

---

## 边界条件

### BOUND-001: 键已存在

**对应PRD**: AC-003-03

**输入边界**:
| 参数 | 类型 | 约束 | 来源 | 说明 |
|-----|------|------|------|------|
| key | Key | 非空 | 用户输入 | 主键值 |

**业务边界**:
- B+ Tree 中不允许重复键
- 插入前需先搜索确认键不存在

**错误处理**:
| 错误码 | 场景 | 处理方式 |
|-------|------|---------|
| DuplicateKey | 键已存在 | 返回错误，不插入 |

### BOUND-002: 页面已满

**对应PRD**: FR-001

**触发条件**:
- 插入新单元格后，页面剩余空间不足

**处理方式**:
1. 叶子节点分裂:
   - 创建新叶子节点
   - 将一半的单元格移动到新节点
   - 更新父节点（或创建新根节点）
   - 维护叶子节点链表

2. 内部节点分裂:
   - 创建新内部节点
   - 将一半的键和子节点移动到新节点
   - 将中间键提升到父节点

### BOUND-003: 根节点分裂

**对应PRD**: FR-001

**触发条件**:
- 根节点已满，需要分裂

**处理方式**:
1. 创建新的根节点（内部节点）
2. 原根节点成为左子节点
3. 新创建的节点成为右子节点
4. 树高度 +1

### BOUND-004: 节点填充率过低

**对应PRD**: FR-001

**触发条件**:
- 删除后节点填充率 < 50%

**处理方式**:
1. 尝试从兄弟节点借一个单元格（重分配）
2. 如果兄弟节点也处于最小填充率，则合并节点
3. 合并可能导致父节点键减少，递归处理

---

## 非功能需求

### 性能要求

| 指标 | 要求 | 对应PRD |
|-----|------|---------|
| 点查延迟 | < 1ms (1000 条记录) | FR-001 |
| 插入延迟 | < 2ms (含可能的节点分裂) | FR-003 |
| 范围扫描 | 顺序读取 100 条 < 1ms | FR-003 |
| 内存使用 | 每个连接 < 1MB | - |

### 安全要求

| 需求 | 描述 | 实现方案 |
|-----|------|---------|
| 数据完整性 | 防止页面损坏 | 页面校验和（可选） |
| 边界检查 | 防止缓冲区溢出 | Rust 边界检查 |

---

## 实现文件

| 文件路径 | 职责 |
|---------|------|
| src/storage/mod.rs | 模块入口，公共接口定义 |
| src/storage/btree.rs | B+ Tree 实现 |
| src/storage/node.rs | B+ Tree 节点操作 |
| src/storage/record.rs | 记录定义和序列化 |
| src/storage/schema.rs | 表元数据管理 |
| src/storage/cursor.rs | 游标实现 |
| src/storage/page_layout.rs | 页面布局定义 |

---

## 验收标准

| 标准 | 要求 | 验证方法 | 对应PRD |
|-----|------|---------|---------|
| 标准1 | B+ Tree 插入正确 | 单元测试：插入 1000 条记录，验证顺序 | FR-001 |
| 标准2 | B+ Tree 查询正确 | 单元测试：随机查询，验证结果 | FR-003 |
| 标准3 | B+ Tree 删除正确 | 单元测试：删除后验证树结构 | FR-003 |
| 标准4 | 节点分裂正确 | 单元测试：强制分裂，验证结构 | FR-001 |
| 标准5 | 记录序列化正确 | 单元测试：序列化后反序列化，验证相等 | FR-004 |
| 标准6 | 游标遍历正确 | 单元测试：遍历结果有序 | FR-003 |

---

## 覆盖映射

### PRD需求覆盖情况

| PRD类型 | PRD编号 | 架构元素 | 覆盖状态 |
|---------|---------|---------|---------|
| 功能需求 | FR-001 | BTree 结构 | ✅ |
| 功能需求 | FR-003 | insert/search/delete/scan | ✅ |
| 功能需求 | FR-004 | Record, Serializer | ✅ |
| 用户故事 | US-001 | Schema 管理 | ✅ |
| 用户故事 | US-002 | create_table | ✅ |
| 用户故事 | US-003 | insert | ✅ |
| 用户故事 | US-004 | search, scan | ✅ |
| 用户故事 | US-006 | delete | ✅ |
| 数据实体 | Entity-001 | Database Header | ✅ |
| 数据实体 | Entity-002 | BTreePageHeader | ✅ |
| 数据实体 | Entity-003 | TableMetadata | ✅ |
| 数据实体 | Entity-004 | ColumnDef | ✅ |
| 验收标准 | AC-001-01~03 | Database::create/open | ✅ |
| 验收标准 | AC-002-01~03 | Schema::create_table | ✅ |
| 验收标准 | AC-003-01~03 | BTree::insert | ✅ |
| 验收标准 | AC-004-01~04 | BTree::search, scan | ✅ |
| 验收标准 | AC-006-01~03 | BTree::delete | ✅ |

---

## 变更历史

| 版本 | 日期 | 变更内容 | 作者 |
|-----|------|---------|------|
| 1.0 | 2026-03-14 | 初始版本 | Claude |
