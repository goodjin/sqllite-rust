# 数据结构规约文档

## 文档信息
- **项目名称**: sqllite-rust
- **版本**: v1.0
- **对应PRD**: docs/v1/01-prd.md
- **更新日期**: 2026-03-14

---

## 数据实体清单

| 编号 | 实体名称 | 对应PRD实体 | 所属模块 | 说明 |
|-----|---------|------------|---------|------|
| DATA-001 | DatabaseHeader | Entity-001 | MOD-02 | 数据库文件头 |
| DATA-002 | Page | Entity-002 | MOD-02 | 数据页 |
| DATA-003 | TableSchema | Entity-003, Entity-004 | MOD-01 | 表结构定义 |
| DATA-004 | WalRecord | - | MOD-05 | WAL日志记录 |
| DATA-005 | IndexInfo | Entity-005 | MOD-06 | 索引元数据 |

---

## 实体详细定义

### DATA-001: 数据库文件头 (DatabaseHeader)

**对应PRD**: Entity-001

**所属模块**: MOD-02 (Pager)

**数据模型**:
```rust
#[repr(C, packed)]
pub struct DatabaseHeader {
    /// 魔数: "SQLite format 3\0"
    pub magic: [u8; 16],
    /// 页面大小
    pub page_size: u16,
    /// 文件格式写版本
    pub file_format_write: u8,
    /// 文件格式读版本
    pub file_format_read: u8,
    /// 每页保留字节数
    pub reserved_space: u8,
    /// 最大负载比例
    pub max_payload_frac: u8,
    /// 最小负载比例
    pub min_payload_frac: u8,
    /// 叶子节点负载比例
    pub leaf_payload_frac: u8,
    /// 文件变更计数器
    pub file_change_counter: u32,
    /// 数据库大小（页数）
    pub database_size: u32,
    /// 第一个空闲列表主干页
    pub first_freelist_trunk: u32,
    /// 空闲列表页总数
    pub freelist_pages: u32,
    /// Schema cookie
    pub schema_cookie: u32,
    /// Schema 格式号
    pub schema_format: u32,
    /// 默认缓存大小
    pub default_cache_size: u32,
    /// 最大的根 B-tree 页号
    pub largest_root_btree: u32,
    /// 文本编码 (1=UTF-8)
    pub text_encoding: u32,
    /// 用户版本号
    pub user_version: u32,
    /// 增量真空模式
    pub incremental_vacuum: u32,
    /// 应用程序 ID
    pub application_id: u32,
    /// 保留扩展
    pub reserved: [u8; 20],
    /// 版本验证号
    pub version_valid_for: u32,
    /// SQLite 版本号
    pub sqlite_version: u32,
}
```

**字段规约**:
| 字段名 | PRD属性 | 类型 | 约束 | 默认值 | 说明 |
|-------|---------|------|------|-------|------|
| magic | - | [u8; 16] | 固定值 | "SQLite format 3\0" | 文件魔数 |
| page_size | page_size | u16 | 512-32768 | 4096 | 页面大小 |
| file_format_write | file_format_write | u8 | 1-4 | 1 | 写版本 |
| file_format_read | file_format_read | u8 | 1-4 | 1 | 读版本 |
| database_size | database_size | u32 | >=1 | 1 | 数据库页数 |
| first_freelist_trunk | first_freelist_trunk | u32 | >=0 | 0 | 空闲列表首页 |
| freelist_pages | freelist_pages | u32 | >=0 | 0 | 空闲页数 |
| schema_cookie | schema_cookie | u32 | 递增 | 0 | Schema版本 |
| text_encoding | text_encoding | u32 | 1-3 | 1 | 1=UTF-8 |

**数据流**:
- 创建来源: Pager::open (新数据库)
- 读取场景: 数据库打开时
- 更新场景: 提交事务时 (file_change_counter, schema_cookie)

---

### DATA-002: 数据页 (Page)

**对应PRD**: Entity-002

**所属模块**: MOD-02 (Pager)

**数据模型**:
```rust
pub const PAGE_SIZE: usize = 4096;

pub struct Page {
    /// 页面 ID
    pub id: PageId,
    /// 页面数据
    pub data: [u8; PAGE_SIZE],
}

pub type PageId = u32;
```

**B+ Tree 页面布局**:
```
┌─────────────────────────────────────────────────────────────┐
│ 页头 (12 bytes)                                              │
│ ├─ node_type: u8        (0x02=内部节点, 0x05=叶子节点)       │
│ ├─ first_freeblock: u16  (第一个空闲块偏移)                  │
│ ├─ cell_count: u16       (单元格数量)                        │
│ ├─ cell_content_offset: u16 (单元格内容起始偏移)             │
│ ├─ fragmented_bytes: u1  (碎片化空闲字节数)                  │
│ └─ right_child: u32      (最右子节点页号，仅内部节点)        │
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

**页头结构**:
```rust
#[repr(C, packed)]
pub struct BTreePageHeader {
    pub node_type: u8,
    pub first_freeblock: u16,
    pub cell_count: u16,
    pub cell_content_offset: u16,
    pub fragmented_bytes: u8,
    pub right_child: u32,
}
```

**数据流**:
- 创建来源: Pager::allocate_page
- 读取场景: BTree 操作, 索引查询
- 更新场景: INSERT, UPDATE, DELETE

---

### DATA-003: 表结构 (TableSchema)

**对应PRD**: Entity-003, Entity-004

**所属模块**: MOD-01 (Storage)

**数据模型**:
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

/// 数据类型
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum DataType {
    Integer,  // 8 字节有符号整数
    Text,     // 变长文本
    Real,     // 8 字节浮点数
    Blob,     // 二进制数据
}

/// 记录结构
pub struct Record {
    /// 行 ID（隐藏主键）
    pub row_id: RowId,
    /// 列值
    pub values: Vec<Value>,
}

/// 值类型
#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub enum Value {
    Null,
    Integer(i64),
    Text(String),
    Real(f64),
    Blob(Vec<u8>),
}

pub type RowId = i64;
```

**字段规约**:
| 字段名 | PRD属性 | 类型 | 约束 | 默认值 | 说明 |
|-------|---------|------|------|-------|------|
| name | table_name | String | 非空 | - | 表名 |
| columns | - | Vec<ColumnDef> | >=1 | - | 列定义列表 |
| root_page_id | root_page | PageId | >=1 | - | B+ Tree 根页 |

**数据流**:
- 创建来源: CREATE TABLE
- 读取场景: 所有表操作
- 更新场景: ALTER TABLE (未来)

**序列化格式**:
```
记录格式:
┌────────────────────────────────────────┐
│ 行 ID (8 bytes)                        │
├────────────────────────────────────────┤
│ 列数 (2 bytes)                         │
├────────────────────────────────────────┤
│ 列 1 类型标记 (1 byte)                 │
│ 列 1 数据 (变长)                       │
├────────────────────────────────────────┤
│ 列 2 类型标记 (1 byte)                 │
│ 列 2 数据 (变长)                       │
├────────────────────────────────────────┤
│ ...                                    │
└────────────────────────────────────────┘

类型标记:
- 0x00: NULL
- 0x01: INTEGER (8 bytes, big-endian)
- 0x02: TEXT (2 bytes length + data)
- 0x03: REAL (8 bytes, IEEE 754)
- 0x04: BLOB (2 bytes length + data)
```

---

### DATA-004: WAL 记录 (WalRecord)

**对应PRD**: - (内部实现)

**所属模块**: MOD-05 (Transaction)

**数据模型**:
```rust
/// WAL 记录类型
#[derive(Debug, Clone)]
pub enum WalRecord {
    /// 开始事务
    Begin,
    /// 提交事务
    Commit,
    /// 回滚事务
    Rollback,
    /// 页面更新
    Update {
        page_id: PageId,
        /// 页面更新前的数据
        before_image: Vec<u8>,
        /// 页面更新后的数据
        after_image: Vec<u8>,
    },
    /// 检查点
    Checkpoint {
        database_size: u32,
    },
}

/// WAL 记录头
#[repr(C, packed)]
pub struct WalRecordHeader {
    /// 记录类型
    pub record_type: u8,
    /// 记录长度（不含头部）
    pub length: u32,
    /// 校验和
    pub checksum: u32,
}
```

**记录类型码**:
| 类型码 | 记录类型 | 说明 |
|-------|---------|------|
| 0x01 | Begin | 事务开始 |
| 0x02 | Commit | 事务提交 |
| 0x03 | Rollback | 事务回滚 |
| 0x04 | Update | 页面更新 |
| 0x05 | Checkpoint | 检查点 |

**Update 记录格式**:
```
┌────────────────────────────────────────┐
│ page_id (4 bytes)                      │
├────────────────────────────────────────┤
│ before_image 长度 (4 bytes)            │
├────────────────────────────────────────┤
│ before_image 数据 (变长)               │
├────────────────────────────────────────┤
│ after_image 长度 (4 bytes)             │
├────────────────────────────────────────┤
│ after_image 数据 (变长)                │
└────────────────────────────────────────┘
```

**数据流**:
- 创建来源: 事务操作 (BEGIN, COMMIT, ROLLBACK), 页面更新
- 读取场景: 崩溃恢复
- 更新场景: 检查点（截断 WAL）

---

### DATA-005: 索引元数据 (IndexInfo)

**对应PRD**: Entity-005

**所属模块**: MOD-06 (Index)

**数据模型**:
```rust
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

/// 索引键
#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub struct IndexKey {
    /// 索引列值
    pub value: Value,
    /// 行 ID
    pub row_id: RowId,
}
```

**字段规约**:
| 字段名 | PRD属性 | 类型 | 约束 | 默认值 | 说明 |
|-------|---------|------|------|-------|------|
| name | index_name | String | 非空 | - | 索引名 |
| table | table_name | String | 非空 | - | 表名 |
| column | column_name | String | 非空 | - | 列名 |
| root_page_id | root_page | PageId | >=1 | - | B+ Tree 根页 |
| unique | - | bool | - | false | 是否唯一 |

**索引键序列化**:
```
┌────────────────────────────────────────┐
│ 值类型标记 (1 byte)                    │
├────────────────────────────────────────┤
│ 值数据 (变长)                          │
├────────────────────────────────────────┤
│ row_id (8 bytes)                       │
└────────────────────────────────────────┘
```

**数据流**:
- 创建来源: CREATE INDEX
- 读取场景: 索引查询, 索引维护
- 更新场景: INSERT, UPDATE, DELETE 时自动维护

---

## 数据关系图

```
┌─────────────────┐       ┌─────────────────┐
│ DatabaseHeader  │◄──────│     Pager       │
│   (DATA-001)    │       │   (MOD-02)      │
└────────┬────────┘       └────────┬────────┘
         │                         │
         │ 包含                    │ 管理
         ▼                         ▼
┌─────────────────┐       ┌─────────────────┐
│      Page       │◄──────│   PageCache     │
│   (DATA-002)    │       │   (MOD-02)      │
└────────┬────────┘       └─────────────────┘
         │
         │ 存储
         ▼
┌─────────────────┐       ┌─────────────────┐
│  TableSchema    │◄──────│ Storage Engine  │
│   (DATA-003)    │       │   (MOD-01)      │
└────────┬────────┘       └────────┬────────┘
         │                         │
         │ 关联                    │ 维护
         ▼                         ▼
┌─────────────────┐       ┌─────────────────┐
│   IndexInfo     │◄──────│  IndexManager   │
│   (DATA-005)    │       │   (MOD-06)      │
└─────────────────┘       └─────────────────┘

┌─────────────────┐
│   WalRecord     │◄────── TransactionManager
│   (DATA-004)    │              (MOD-05)
└─────────────────┘
```

---

## 变更历史

| 版本 | 日期 | 变更内容 | 作者 |
|-----|------|---------|------|
| 1.0 | 2026-03-14 | 初始版本 | Claude |
