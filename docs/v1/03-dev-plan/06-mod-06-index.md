# 开发计划 - MOD-06: Index Manager (索引管理器)

## 文档信息
- **模块编号**: MOD-06
- **模块名称**: Index Manager (索引管理器)
- **所属层次**: L4-存储层
- **对应架构**: docs/v1/02-architecture/03-mod-06-index.md
- **优先级**: P1 (阶段 3)
- **预估工时**: 2天

---

## 1. 模块概述

### 1.1 模块职责
- 索引创建 (CREATE INDEX)
- 索引维护 (INSERT/UPDATE/DELETE 时自动更新)
- 索引扫描 (使用索引加速查询)
- 索引元数据管理

### 1.2 对应PRD
| PRD编号 | 功能 | 用户故事 |
|---------|-----|---------|
| FR-016 | B+ Tree 索引 | US-008 |

### 1.3 架构定位
```
VM → Index Manager → Storage Engine
```

---

## 2. 技术设计

### 2.1 目录结构
```
src/index/
├── mod.rs           # 模块入口，IndexManager
├── metadata.rs      # 索引元数据
├── key.rs           # 索引键定义
└── scan.rs          # 索引扫描
```

### 2.2 依赖关系
| 依赖模块 | 依赖方式 | 用途 |
|---------|---------|------|
| MOD-01 Storage | use crate::storage | B+ Tree 操作 |

---

## 3. 接口清单

| 任务编号 | 接口编号 | 接口名称 | 复杂度 |
|---------|---------|---------|-------|
| T-03 | API-012 | IndexManager::create_index | 中 |
| T-04 | API-013 | IndexManager::search_index | 中 |

---

## 4. 开发任务拆分

### 任务清单

| 任务编号 | 任务名称 | 涉及文件 | 代码行数 | 依赖 |
|---------|---------|---------|---------|------|
| T-01 | 错误类型定义 | 1 | ~30 | - |
| T-02 | 索引键定义 | 1 | ~80 | T-01 |
| T-03 | 索引元数据 | 1 | ~60 | T-01 |
| T-04 | 索引管理器核心 | 2 | ~200 | T-02, T-03 |
| T-05 | 索引扫描 | 1 | ~80 | T-04 |
| T-06 | 单元测试 | 4 | ~150 | T-01~05 |

---

## 5. 详细任务定义

### T-01: 错误类型定义

**任务概述**: 定义索引模块的错误类型

**输出**:
- `src/index/error.rs`

**实现要求**:
```rust
use thiserror::Error;

#[derive(Error, Debug)]
pub enum IndexError {
    #[error("Table not found: {0}")]
    TableNotFound(String),

    #[error("Column not found: {0}")]
    ColumnNotFound(String),

    #[error("Index not found: {0}")]
    IndexNotFound(String),

    #[error("Duplicate key: {0}")]
    DuplicateKey(String),

    #[error("Storage error: {0}")]
    StorageError(String),
}

pub type Result<T> = std::result::Result<T, IndexError>;
```

**预估工时**: 0.5小时

---

### T-02: 索引键定义

**任务概述**: 定义索引键结构

**输出**:
- `src/index/key.rs`

**实现要求**:
```rust
use crate::storage::{Value, RowId};

#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub struct IndexKey {
    pub value: Value,
    pub row_id: RowId,
}

impl IndexKey {
    pub fn new(value: Value, row_id: RowId) -> Self {
        Self { value, row_id }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        // 序列化
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

    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        // 反序列化
        unimplemented!()
    }
}
```

**验收标准**:
- [ ] 正确序列化和反序列化
- [ ] 支持排序比较

**测试要求**:
- 测试用例: 3个（序列化、反序列化、比较）

**预估工时**: 2小时

**依赖**: T-01

---

### T-03: 索引元数据

**任务概述**: 定义索引元数据结构

**输出**:
- `src/index/metadata.rs`

**实现要求**:
```rust
use crate::pager::PageId;

#[derive(Debug, Clone)]
pub struct IndexInfo {
    pub name: String,
    pub table: String,
    pub column: String,
    pub root_page_id: PageId,
    pub unique: bool,
}

impl IndexInfo {
    pub fn new(
        name: String,
        table: String,
        column: String,
        root_page_id: PageId,
        unique: bool,
    ) -> Self {
        Self {
            name,
            table,
            column,
            root_page_id,
            unique,
        }
    }
}
```

**预估工时**: 1小时

**依赖**: T-01

---

### T-04: 索引管理器核心

**任务概述**: 实现索引管理器核心功能

**输出**:
- `src/index/mod.rs`

**实现要求**:
```rust
use crate::index::{IndexInfo, IndexKey, IndexError, Result};
use crate::storage::{Storage, Value, RowId, TableSchema};
use crate::pager::PageId;
use std::sync::{Arc, Mutex};
use std::collections::HashMap;

pub struct IndexManager {
    storage: Arc<Mutex<Storage>>,
    index_cache: HashMap<String, Vec<IndexInfo>>,
}

impl IndexManager {
    pub fn new(storage: Arc<Mutex<Storage>>) -> Self {
        Self {
            storage,
            index_cache: HashMap::new(),
        }
    }

    pub fn create_index(
        &mut self,
        index_name: &str,
        table: &str,
        column: &str,
        unique: bool,
    ) -> Result<()> {
        // 1. 验证表和列存在
        // 2. 分配新的根页面
        // 3. 扫描表数据，构建索引
        // 4. 保存索引元数据
        // 5. 更新缓存
    }

    pub fn drop_index(&mut self, index_name: &str) -> Result<()> {
        // 1. 查找索引元数据
        // 2. 释放索引占用的页面
        // 3. 删除索引元数据
        // 4. 更新缓存
    }

    pub fn insert_into_index(
        &mut self,
        index_name: &str,
        value: &Value,
        row_id: RowId,
    ) -> Result<()> {
        // 插入索引条目
    }

    pub fn delete_from_index(
        &mut self,
        index_name: &str,
        value: &Value,
        row_id: RowId,
    ) -> Result<()> {
        // 删除索引条目
    }

    pub fn search_index(
        &self,
        index_name: &str,
        value: &Value,
    ) -> Result<Vec<RowId>> {
        // 使用索引查找
    }

    pub fn get_table_indexes(&self, table: &str) -> Result<Vec<IndexInfo>> {
        // 获取表的所有索引
    }

    pub fn find_usable_index(
        &self,
        table: &str,
        column: &str,
    ) -> Result<Option<IndexInfo>> {
        // 查找可用于查询的索引
    }
}
```

**验收标准**:
- [ ] 正确创建索引
- [ ] 正确删除索引
- [ ] 正确维护索引

**测试要求**:
- 测试用例: 6个（创建、删除、插入、删除条目、搜索、获取索引）

**预估工时**: 6小时

**依赖**: T-02, T-03

---

### T-05: 索引扫描

**任务概述**: 实现索引扫描

**输出**:
- `src/index/scan.rs`

**实现要求**:
```rust
use crate::index::IndexManager;
use crate::storage::{Storage, RowId, Value};

pub struct IndexScan {
    index_name: String,
    value: Value,
    row_ids: Vec<RowId>,
    current_idx: usize,
}

impl IndexScan {
    pub fn new(
        index_manager: &IndexManager,
        index_name: &str,
        value: Value,
    ) -> Result<Self> {
        // 使用索引查找所有匹配的行 ID
        let row_ids = index_manager.search_index(index_name, &value)?;

        Ok(Self {
            index_name: index_name.to_string(),
            value,
            row_ids,
            current_idx: 0,
        })
    }

    pub fn next(&mut self) -> Option<RowId> {
        if self.current_idx < self.row_ids.len() {
            let row_id = self.row_ids[self.current_idx];
            self.current_idx += 1;
            Some(row_id)
        } else {
            None
        }
    }
}
```

**验收标准**:
- [ ] 正确返回匹配的行 ID
- [ ] 支持迭代遍历

**测试要求**:
- 测试用例: 2个（扫描、空结果）

**预估工时**: 2小时

**依赖**: T-04

---

### T-06: 单元测试

**任务概述**: 编写完整的单元测试

**输出**:
- 各文件中的 `#[cfg(test)]` 模块

**测试清单**:
| 测试目标 | 测试文件 | 用例数 |
|---------|---------|-------|
| IndexKey | key.rs | 3 |
| IndexManager | mod.rs | 6 |
| IndexScan | scan.rs | 2 |

**预估工时**: 2小时

**依赖**: T-01~05

---

## 6. 验收清单

- [ ] 正确创建索引
- [ ] 正确删除索引
- [ ] 自动维护索引
- [ ] 索引加速查询
- [ ] 测试覆盖率 ≥ 80%

---

## 7. 覆盖映射

| 架构元素 | 架构编号 | 任务 | 覆盖状态 |
|---------|---------|------|---------|
| 错误类型 | - | T-01 | ✅ |
| IndexKey | - | T-02 | ✅ |
| IndexInfo | DATA-005 | T-03 | ✅ |
| IndexManager | API-012, API-013 | T-04 | ✅ |
| IndexScan | - | T-05 | ✅ |

---

## 变更历史

| 版本 | 日期 | 变更内容 | 作者 |
|-----|------|---------|------|
| 1.0 | 2026-03-14 | 初始版本 | Claude |
