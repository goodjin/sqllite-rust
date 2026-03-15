# 开发计划 - MOD-02: Pager (页面管理器)

## 文档信息
- **模块编号**: MOD-02
- **模块名称**: Pager (页面管理器)
- **所属层次**: L5-页管理层
- **对应架构**: docs/v1/02-architecture/03-mod-02-pager.md
- **优先级**: P0 (阶段 1)
- **预估工时**: 2天

---

## 1. 模块概述

### 1.1 模块职责
- 管理固定大小(4KB)的数据页
- 实现 LRU 页面缓存
- 管理空闲页面列表
- 提供页面读写抽象

### 1.2 对应PRD
| PRD编号 | 功能 | 用户故事 |
|---------|-----|---------|
| FR-002 | 页面管理器 | US-001 |
| Entity-001 | 数据库文件头 | - |
| Entity-002 | B+ Tree 页面 | - |

### 1.3 架构定位
```
L4: 存储层 (Storage) ←→ L5: Pager ←→ L6: OS 文件系统
```

---

## 2. 技术设计

### 2.1 目录结构
```
src/pager/
├── mod.rs           # 模块入口，Pager 结构
├── page.rs          # Page 结构定义
├── cache.rs         # LRU 缓存实现
├── freelist.rs      # 空闲列表管理
├── header.rs        # 数据库文件头
└── error.rs         # 错误类型
```

### 2.2 依赖关系
| 依赖模块 | 依赖方式 | 用途 |
|---------|---------|------|
| std::fs::File | 标准库 | 文件 I/O |
| std::io | 标准库 | I/O 操作 |

---

## 3. 接口清单

| 任务编号 | 接口编号 | 接口名称 | 复杂度 |
|---------|---------|---------|-------|
| T-03 | API-004 | Pager::open | 中 |
| T-04 | API-005 | Pager::get_page | 中 |
| T-05 | API-006 | Pager::allocate_page | 中 |
| T-06 | API-007 | Pager::write_page | 低 |
| T-07 | API-008 | Pager::flush | 低 |

---

## 4. 开发任务拆分

### 任务约束
- **代码变更**: ≤ 200行
- **涉及文件**: ≤ 5个
- **测试用例**: ≤ 10个

### 任务清单

| 任务编号 | 任务名称 | 涉及文件 | 代码行数 | 依赖 |
|---------|---------|---------|---------|------|
| T-01 | 错误类型定义 | 1 | ~30 | - |
| T-02 | Page 结构定义 | 1 | ~50 | T-01 |
| T-03 | DatabaseHeader 定义 | 1 | ~80 | T-01 |
| T-04 | LRU 缓存实现 | 2 | ~150 | T-02 |
| T-05 | Pager 核心实现 | 3 | ~200 | T-03, T-04 |
| T-06 | 空闲列表管理 | 2 | ~120 | T-05 |
| T-07 | 单元测试 | 5 | ~200 | T-01~06 |

---

## 5. 详细任务定义

### T-01: 错误类型定义

**任务概述**: 定义 Pager 模块的错误类型

**对应架构**:
- 边界条件规约: BOUND-001~003

**输出**:
- `src/pager/error.rs`

**实现要求**:
```rust
use thiserror::Error;

#[derive(Error, Debug)]
pub enum PagerError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Invalid database format")]
    InvalidFormat,

    #[error("Page not found: {0}")]
    PageNotFound(u32),

    #[error("Permission denied")]
    PermissionDenied,

    #[error("Cache full")]
    CacheFull,
}

pub type Result<T> = std::result::Result<T, PagerError>;
```

**验收标准**:
- [ ] 所有错误类型定义完整
- [ ] 实现了 std::error::Error trait

**测试要求**:
- 无（错误类型无需单独测试）

**预估工时**: 0.5小时

**依赖**: 无

---

### T-02: Page 结构定义

**任务概述**: 定义 Page 数据结构

**对应架构**:
- 数据结构规约: DATA-002

**输出**:
- `src/pager/page.rs`

**实现要求**:
```rust
pub const PAGE_SIZE: usize = 4096;
pub type PageId = u32;

#[derive(Debug, Clone)]
pub struct Page {
    pub id: PageId,
    pub data: [u8; PAGE_SIZE],
}

impl Page {
    pub fn new(id: PageId) -> Self {
        Self {
            id,
            data: [0; PAGE_SIZE],
        }
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.data
    }

    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.data
    }
}
```

**验收标准**:
- [ ] Page 结构定义与架构一致
- [ ] 页面大小为 4096 字节
- [ ] 提供基本的访问方法

**测试要求**:
- 测试文件: `src/pager/page.rs` (#[cfg(test)])
- 测试用例: 2个（创建、访问）

**预估工时**: 0.5小时

**依赖**: T-01

---

### T-03: DatabaseHeader 定义

**任务概述**: 定义数据库文件头结构

**对应架构**:
- 数据结构规约: DATA-001

**输出**:
- `src/pager/header.rs`

**实现要求**:
```rust
#[repr(C, packed)]
pub struct DatabaseHeader {
    pub magic: [u8; 16],
    pub page_size: u16,
    pub file_format_write: u8,
    pub file_format_read: u8,
    pub reserved_space: u8,
    pub max_payload_frac: u8,
    pub min_payload_frac: u8,
    pub leaf_payload_frac: u8,
    pub file_change_counter: u32,
    pub database_size: u32,
    pub first_freelist_trunk: u32,
    pub freelist_pages: u32,
    pub schema_cookie: u32,
    pub schema_format: u32,
    pub default_cache_size: u32,
    pub largest_root_btree: u32,
    pub text_encoding: u32,
    pub user_version: u32,
    pub incremental_vacuum: u32,
    pub application_id: u32,
    pub reserved: [u8; 20],
    pub version_valid_for: u32,
    pub sqlite_version: u32,
}

impl DatabaseHeader {
    pub const SIZE: usize = 100;
    pub const MAGIC: &[u8] = b"SQLite format 3\0";

    pub fn new(page_size: u16) -> Self {
        // 初始化默认值
    }

    pub fn validate(&self) -> Result<()> {
        // 验证魔数、页面大小等
    }

    pub fn to_bytes(&self) -> [u8; Self::SIZE] {
        // 序列化
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        // 反序列化
    }
}
```

**验收标准**:
- [ ] 所有字段与架构一致
- [ ] 提供序列化/反序列化方法
- [ ] 提供验证方法

**测试要求**:
- 测试文件: `src/pager/header.rs` (#[cfg(test)])
- 测试用例: 3个（创建、序列化、验证）

**预估工时**: 1小时

**依赖**: T-01

---

### T-04: LRU 缓存实现

**任务概述**: 实现 LRU 页面缓存

**对应架构**:
- 核心设计: PageCache

**输出**:
- `src/pager/cache.rs`

**实现要求**:
```rust
use std::collections::{HashMap, VecDeque};

pub struct PageCache {
    pages: HashMap<PageId, CachedPage>,
    lru: VecDeque<PageId>,
    capacity: usize,
}

struct CachedPage {
    page: Page,
    is_dirty: bool,
    pin_count: u32,
}

impl PageCache {
    pub fn new(capacity: usize) -> Self {
        // 初始化
    }

    pub fn get(&mut self, page_id: PageId) -> Option<&Page> {
        // 获取页面，更新 LRU
    }

    pub fn get_mut(&mut self, page_id: PageId) -> Option<&mut Page> {
        // 获取可变引用
    }

    pub fn put(&mut self, page: Page, is_dirty: bool) {
        // 插入页面
    }

    pub fn mark_dirty(&mut self, page_id: PageId) {
        // 标记脏页
    }

    pub fn get_dirty_pages(&self) -> Vec<PageId> {
        // 获取所有脏页
    }

    fn evict_if_needed(&mut self) {
        // 淘汰页面
    }
}
```

**验收标准**:
- [ ] 实现 LRU 淘汰策略
- [ ] 支持脏页标记
- [ ] 支持 Pin 计数

**测试要求**:
- 测试文件: `src/pager/cache.rs` (#[cfg(test)])
- 测试用例: 5个（获取、插入、淘汰、脏页、Pin）

**预估工时**: 2小时

**依赖**: T-02

---

### T-05: Pager 核心实现

**任务概述**: 实现 Pager 核心功能

**对应架构**:
- 接口规约: API-004~008

**输出**:
- `src/pager/mod.rs`
- `src/pager/freelist.rs`

**实现要求**:
```rust
pub struct Pager {
    file: File,
    cache: PageCache,
    header: DatabaseHeader,
    page_size: usize,
}

impl Pager {
    pub fn open(path: &str) -> Result<Self> {
        // 打开或创建数据库文件
    }

    pub fn get_page(&mut self, page_id: PageId) -> Result<&Page> {
        // 获取页面（先查缓存，再读文件）
    }

    pub fn get_page_mut(&mut self, page_id: PageId) -> Result<&mut Page> {
        // 获取可变页面
    }

    pub fn allocate_page(&mut self) -> Result<PageId> {
        // 分配新页面
    }

    pub fn write_page(&mut self, page: &Page) -> Result<()> {
        // 写入页面到缓存
    }

    pub fn flush(&mut self) -> Result<()> {
        // 刷盘所有脏页
    }

    fn read_page_from_file(&mut self, page_id: PageId) -> Result<Page> {
        // 从文件读取页面
    }

    fn write_page_to_file(&mut self, page: &Page) -> Result<()> {
        // 写入页面到文件
    }
}
```

**验收标准**:
- [ ] 实现所有接口
- [ ] 正确处理文件打开/创建
- [ ] 缓存与文件一致性

**测试要求**:
- 测试文件: `src/pager/mod.rs` (#[cfg(test)])
- 测试用例: 6个（打开、获取、分配、写入、刷盘、缓存命中）

**预估工时**: 4小时

**依赖**: T-03, T-04

---

### T-06: 空闲列表管理

**任务概述**: 实现空闲页面列表管理

**对应架构**:
- 核心设计: FreelistManager

**输出**:
- `src/pager/freelist.rs`（扩展）

**实现要求**:
```rust
pub struct FreelistManager {
    first_trunk: Option<PageId>,
    total_pages: u32,
}

impl FreelistManager {
    pub fn new() -> Self {
        // 初始化
    }

    pub fn allocate_page(&mut self, pager: &mut Pager) -> Result<PageId> {
        // 从空闲列表分配页面
    }

    pub fn free_page(&mut self, pager: &mut Pager, page_id: PageId) -> Result<()> {
        // 将页面加入空闲列表
    }

    fn read_trunk_page(&self, pager: &mut Pager, page_id: PageId) -> Result<FreelistTrunk> {
        // 读取主干页
    }

    fn write_trunk_page(&self, pager: &mut Pager, trunk: &FreelistTrunk) -> Result<()> {
        // 写入主干页
    }
}

struct FreelistTrunk {
    next_trunk: Option<PageId>,
    page_ids: Vec<PageId>,
}
```

**验收标准**:
- [ ] 实现页面分配
- [ ] 实现页面回收
- [ ] 正确维护空闲列表

**测试要求**:
- 测试文件: `src/pager/freelist.rs` (#[cfg(test)])
- 测试用例: 3个（分配、回收、列表维护）

**预估工时**: 2小时

**依赖**: T-05

---

### T-07: 单元测试

**任务概述**: 编写完整的单元测试

**输出**:
- 各文件中的 `#[cfg(test)]` 模块

**测试清单**:
| 测试目标 | 测试文件 | 用例数 |
|---------|---------|-------|
| Page | page.rs | 2 |
| DatabaseHeader | header.rs | 3 |
| PageCache | cache.rs | 5 |
| Pager | mod.rs | 6 |
| Freelist | freelist.rs | 3 |

**测试要求**:
- 覆盖率 ≥ 80%
- 使用临时文件进行测试
- 测试后清理临时文件

**预估工时**: 2小时

**依赖**: T-01~06

---

## 6. 验收清单

### 6.1 功能验收
- [ ] 可以创建新数据库文件
- [ ] 可以打开已存在的数据库文件
- [ ] 页面缓存命中正确
- [ ] 页面分配和回收正确
- [ ] 脏页刷盘正确

### 6.2 质量验收
- [ ] 测试覆盖率 ≥ 80%
- [ ] `cargo clippy` 无警告
- [ ] `cargo fmt` 格式化通过

---

## 7. 覆盖映射

| 架构元素 | 架构编号 | 任务 | 覆盖状态 |
|---------|---------|------|---------|
| 错误类型 | - | T-01 | ✅ |
| Page | DATA-002 | T-02 | ✅ |
| DatabaseHeader | DATA-001 | T-03 | ✅ |
| PageCache | - | T-04 | ✅ |
| Pager | API-004~008 | T-05 | ✅ |
| Freelist | - | T-06 | ✅ |

---

## 变更历史

| 版本 | 日期 | 变更内容 | 作者 |
|-----|------|---------|------|
| 1.0 | 2026-03-14 | 初始版本 | Claude |
