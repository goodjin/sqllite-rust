# MOD-02: 页面管理器模块 (Pager)

## 文档信息
- **项目名称**: sqllite-rust
- **文档编号**: MOD-02
- **版本**: v1.0
- **更新日期**: 2026-03-14
- **对应PRD**: FR-002

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

**所属层次**: L5-页管理层

**架构定位图**:
```
┌─────────────────────────────────────────────────────┐
│              L4: 存储层 (Storage Layer)              │
│              B+ Tree, Index Manager                  │
└───────────────────────┬─────────────────────────────┘
                        │ ▼ 调用 (PageManagementLayer trait)
┌─────────────────────────────────────────────────────┐
│         ★ MOD-02: 页面管理器 (Pager) ★               │
│         Page Cache, I/O Manager                      │
└───────────────────────┬─────────────────────────────┘
                        │ ▼ 依赖 (文件 I/O)
┌─────────────────────────────────────────────────────┐
│              L6: OS 层 (OS Abstraction)              │
│              File I/O                                │
└─────────────────────────────────────────────────────┘
```

### 核心职责

- **页面缓存管理**: 使用 LRU 策略缓存热数据页，减少磁盘 I/O
- **页面分配与回收**: 管理空闲页面，支持页面分配和释放
- **脏页管理**: 跟踪修改过的页面，支持刷盘操作
- **页面读写**: 提供页面的读取和写入抽象

### 边界说明

- **负责**:
  - 页面缓存（LRU 策略）
  - 页面分配和回收
  - 脏页跟踪
  - 文件 I/O 抽象

- **不负责**:
  - B+ Tree 逻辑（由 Storage Engine 负责）
  - 事务管理（由 Transaction Manager 负责）
  - WAL 日志（由 Transaction Manager 负责）

---

## 对应PRD

| PRD章节 | 编号 | 内容 |
|---------|-----|------|
| 功能需求 | FR-002 | 页面管理器 (Pager) |
| 数据实体 | Entity-001 | 数据库文件头（部分） |
| 数据实体 | Entity-002 | B+ Tree 页面（部分） |

---

## 全局架构位置

```
┌─────────────────────────────────────────────────────────────────┐
│                        L4: 存储层                                │
│  ┌─────────────┐  ┌─────────────────────────────────────────┐   │
│  │Index Manager│  │          Storage Engine                  │   │
│  │  (MOD-06)   │  │          (MOD-01)                        │   │
│  └──────┬──────┘  └──────────────────┬──────────────────────┘   │
│         │                            │                          │
│         └────────────┬───────────────┘                          │
│                      │                                          │
└──────────────────────┼──────────────────────────────────────────┘
                       │
┌──────────────────────▼──────────────────────────────────────────┐
│  L5: 页管理层                                                    │
│  ┌───────────────────────────────────────────────────────────┐ │
│  │              ★ MOD-02 Pager ★                              │ │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐ │ │
│  │  │ Page Cache  │  │ Page Alloc  │  │    I/O Manager      │ │ │
│  │  │  (LRU)      │  │  (Freelist) │  │                     │ │ │
│  │  └─────────────┘  └─────────────┘  └─────────────────────┘ │ │
│  └───────────────────────────────────────────────────────────┘ │
└──────────────────────────┬──────────────────────────────────────┘
                           │
┌──────────────────────────▼──────────────────────────────────────┐
│                        L6: OS 层                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## 依赖关系

### 上游依赖（本模块调用的模块）

| 模块名称 | 模块编号 | 依赖原因 | 调用方式 |
|---------|---------|---------|---------|
| OS 文件系统 | - | 文件读写 | std::fs::File |

### 下游依赖（调用本模块的模块）

| 模块名称 | 模块编号 | 被调用场景 | 调用方式 |
|---------|---------|-----------|---------|
| Storage Engine | MOD-01 | B+ Tree 页面操作 | PageManagementLayer trait |
| Transaction Manager | MOD-05 | WAL 写入 | 直接调用 |

---

## 数据流

### 输入数据流

| 数据项 | 来源 | 格式 | 说明 |
|-------|------|------|------|
| 页面请求 | Storage Engine | PageId | 读取指定页面 |
| 页面写入 | Storage Engine | Page | 写入脏页 |
| 分配请求 | Storage Engine | - | 分配新页面 |
| 释放请求 | Storage Engine | PageId | 释放页面 |

### 输出数据流

| 数据项 | 目标 | 格式 | 说明 |
|-------|------|------|------|
| 页面数据 | Storage Engine | Page | 读取的页面 |
| 页面 ID | Storage Engine | PageId | 分配的页面 ID |
| 文件写入 | OS | bytes | 刷盘的页面数据 |

---

## 核心设计

### 设计目标

| 目标 | 描述 | 度量标准 |
|-----|------|---------|
| 缓存命中率 | 减少磁盘 I/O | > 90% (工作集在内存中) |
| 内存使用 | 限制缓存大小 | 可配置，默认 1000 页 (4MB) |
| 刷盘性能 | 批量写入 | 支持延迟写和强制刷盘 |

### 核心组件

#### 1. Pager 结构

```rust
/// 页面管理器
pub struct Pager {
    /// 数据库文件
    file: File,
    /// 页面缓存
    cache: PageCache,
    /// 数据库文件头（缓存）
    header: DatabaseHeader,
    /// 空闲列表管理器
    freelist: FreelistManager,
    /// 页面大小
    page_size: usize,
}

/// 页面缓存
pub struct PageCache {
    /// 缓存的页面
    pages: HashMap<PageId, CachedPage>,
    /// LRU 队列
    lru: VecDeque<PageId>,
    /// 最大缓存页数
    capacity: usize,
}

/// 缓存页面
pub struct CachedPage {
    /// 页面数据
    data: Page,
    /// 是否脏页
    is_dirty: bool,
    /// 引用计数
    pin_count: u32,
}

/// 页面 ID 类型
pub type PageId = u32;

/// 页面数据
pub struct Page {
    pub id: PageId,
    pub data: [u8; PAGE_SIZE],
}

pub const PAGE_SIZE: usize = 4096;
```

#### 2. 空闲列表管理

```rust
/// 空闲列表管理器
pub struct FreelistManager {
    /// 第一个空闲列表主干页
    first_trunk: Option<PageId>,
    /// 空闲页总数
    total_pages: u32,
}

/// 空闲列表主干页结构
/// 第 1 页: 下一个主干页 ID (4 bytes) + 页数 (4 bytes) + 页面 ID 数组
/// 后续页: 同上
pub struct FreelistTrunkPage {
    /// 下一个主干页
    next_trunk: Option<PageId>,
    /// 本页存储的空闲页数量
    count: u32,
    /// 空闲页 ID 数组
    page_ids: Vec<PageId>,
}
```

#### 3. 数据库文件头

```rust
/// 数据库文件头 (100 bytes)
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

impl DatabaseHeader {
    pub const SIZE: usize = 100;

    /// 创建新的文件头
    pub fn new(page_size: u16) -> Self {
        let mut header = Self {
            magic: *b"SQLite format 3\0",
            page_size,
            file_format_write: 1,
            file_format_read: 1,
            reserved_space: 0,
            max_payload_frac: 64,
            min_payload_frac: 32,
            leaf_payload_frac: 32,
            file_change_counter: 0,
            database_size: 1, // 至少第 1 页（文件头页）
            first_freelist_trunk: 0,
            freelist_pages: 0,
            schema_cookie: 0,
            schema_format: 4,
            default_cache_size: 0,
            largest_root_btree: 0,
            text_encoding: 1, // UTF-8
            user_version: 0,
            incremental_vacuum: 0,
            application_id: 0,
            reserved: [0; 20],
            version_valid_for: 0,
            sqlite_version: 3045000, // 模拟 SQLite 3.45.0
        };
        header
    }
}
```

---

## 接口定义

### 对外接口清单

| 接口编号 | 接口名称 | 方法 | 对应PRD |
|---------|---------|------|---------|
| API-004 | Pager::open | fn open(path: &str) -> Result<Self> | FR-002 |
| API-005 | Pager::get_page | fn get_page(&self, page_id: PageId) -> Result<Page> | FR-002 |
| API-006 | Pager::allocate_page | fn allocate_page(&mut self) -> Result<PageId> | FR-002 |
| API-007 | Pager::write_page | fn write_page(&mut self, page: &Page) -> Result<()> | FR-002 |
| API-008 | Pager::flush | fn flush(&mut self) -> Result<()> | FR-002 |

### 接口详细定义

#### API-004: Pager::open

**对应PRD**:
- 用户故事: US-001
- 验收标准: AC-001-01, AC-001-02

**接口定义**:
```rust
/// 打开或创建数据库文件
///
/// # Arguments
/// * `path` - 数据库文件路径
///
/// # Returns
/// * `Ok(Pager)` - 成功打开
/// * `Err(PagerError::IoError)` - I/O 错误
pub fn open(path: &str) -> Result<Self>
```

**边界条件**:
| 编号 | 条件 | 处理方式 |
|-----|------|---------|
| BOUND-001 | 文件不存在 | 创建新数据库文件 |
| BOUND-002 | 文件已存在但格式无效 | 返回 FormatError |
| BOUND-003 | 文件权限不足 | 返回 PermissionError |

#### API-005: Pager::get_page

**对应PRD**:
- 功能需求: FR-002

**接口定义**:
```rust
/// 获取页面
///
/// 1. 先在缓存中查找
/// 2. 缓存未命中则从文件读取
/// 3. 如果缓存已满，淘汰最久未使用的页面
///
/// # Arguments
/// * `page_id` - 页面 ID
///
/// # Returns
/// * `Ok(Page)` - 页面数据
/// * `Err(PagerError::PageNotFound)` - 页面不存在
pub fn get_page(&self, page_id: PageId) -> Result<Page>
```

**边界条件**:
| 编号 | 条件 | 处理方式 |
|-----|------|---------|
| BOUND-004 | page_id > database_size | 返回 PageNotFound |
| BOUND-005 | 缓存未命中 | 从文件读取，加入缓存 |
| BOUND-006 | 缓存已满 | 淘汰 LRU 页面，如果脏页则刷盘 |

#### API-006: Pager::allocate_page

**对应PRD**:
- 功能需求: FR-002

**接口定义**:
```rust
/// 分配新页面
///
/// 1. 优先从空闲列表分配
/// 2. 空闲列表为空则扩展文件
///
/// # Returns
/// * `Ok(PageId)` - 新页面 ID
/// * `Err(PagerError::IoError)` - I/O 错误
pub fn allocate_page(&mut self) -> Result<PageId>
```

**分配策略**:
```
1. 检查空闲列表 (freelist)
   ├─ 有空闲页 → 从空闲列表取出一个
   └─ 无空闲页 → 扩展文件

2. 扩展文件
   ├─ database_size += 1
   ├─ 返回新的 page_id = database_size
   └─ 文件实际扩展在写入时进行
```

#### API-007: Pager::write_page

**对应PRD**:
- 功能需求: FR-002

**接口定义**:
```rust
/// 写入页面（标记为脏页）
///
/// 页面会被写入缓存，并在适当时候刷盘
///
/// # Arguments
/// * `page` - 要写入的页面
pub fn write_page(&mut self, page: &Page) -> Result<()>
```

**边界条件**:
| 编号 | 条件 | 处理方式 |
|-----|------|---------|
| BOUND-007 | 页面不在缓存 | 加入缓存并标记脏 |
| BOUND-008 | 页面已在缓存 | 更新数据，标记脏 |

#### API-008: Pager::flush

**对应PRD**:
- 功能需求: FR-002

**接口定义**:
```rust
/// 将所有脏页刷盘
///
/// # Returns
/// * `Ok(())` - 刷盘成功
/// * `Err(PagerError::IoError)` - I/O 错误
pub fn flush(&mut self) -> Result<()>
```

**刷盘顺序**:
1. 先刷数据页（保证 WAL 先于数据）
2. 再刷文件头（包含 database_size）
3. 调用 fsync 确保落盘

---

## 数据结构

### 核心实体

#### DATA-002: 页面 (Page)

**对应PRD**: Entity-002

```rust
/// 页面结构
pub struct Page {
    /// 页面 ID
    pub id: PageId,
    /// 页面数据 (4096 bytes)
    pub data: [u8; PAGE_SIZE],
}

impl Page {
    pub const SIZE: usize = PAGE_SIZE;

    pub fn new(id: PageId) -> Self {
        Self {
            id,
            data: [0; PAGE_SIZE],
        }
    }

    /// 获取页头
    pub fn header(&self) -> &[u8] {
        &self.data[..DatabaseHeader::SIZE]
    }

    /// 获取页头（可变）
    pub fn header_mut(&mut self) -> &mut [u8] {
        &mut self.data[..DatabaseHeader::SIZE]
    }
}
```

#### DATA-006: 缓存页面 (CachedPage)

```rust
/// 缓存中的页面
pub struct CachedPage {
    /// 页面数据
    pub page: Page,
    /// 是否脏页
    pub is_dirty: bool,
    /// 引用计数（Pin 次数）
    pub pin_count: u32,
}

impl CachedPage {
    pub fn new(page: Page) -> Self {
        Self {
            page,
            is_dirty: false,
            pin_count: 0,
        }
    }

    /// 标记为脏页
    pub fn mark_dirty(&mut self) {
        self.is_dirty = true;
    }

    /// 增加引用计数
    pub fn pin(&mut self) {
        self.pin_count += 1;
    }

    /// 减少引用计数
    pub fn unpin(&mut self) {
        if self.pin_count > 0 {
            self.pin_count -= 1;
        }
    }

    /// 是否可以淘汰
    pub fn is_evictable(&self) -> bool {
        self.pin_count == 0 && !self.is_dirty
    }
}
```

---

## 状态机设计

本模块无复杂状态机。

---

## 边界条件

### BOUND-001: 创建新数据库文件

**对应PRD**: AC-001-01

**处理流程**:
1. 创建新文件
2. 初始化第 1 页（文件头页）
3. 写入 DatabaseHeader
4. 初始化缓存

### BOUND-002: 打开已存在的数据库文件

**对应PRD**: AC-001-02

**处理流程**:
1. 打开文件
2. 读取并验证文件头魔数
3. 读取页面大小等元数据
4. 初始化缓存

**验证项**:
- 魔数是否为 "SQLite format 3\0"
- 页面大小是否为 512 的倍数且在合理范围
- 文件格式版本是否支持

### BOUND-004: 页面不存在

**对应PRD**: FR-002

**触发条件**:
- 请求的 page_id > database_size

**处理方式**:
- 返回 PageNotFound 错误

### BOUND-006: 缓存淘汰

**对应PRD**: FR-002

**触发条件**:
- 缓存已满且需要加载新页面

**淘汰策略**:
1. 从 LRU 队列尾部选择候选页面
2. 如果页面被 Pin（pin_count > 0），跳过
3. 如果页面是脏页，先刷盘
4. 从缓存中移除

---

## 非功能需求

### 性能要求

| 指标 | 要求 | 对应PRD |
|-----|------|---------|
| 缓存命中 | > 90% (工作集在内存中) | FR-002 |
| 刷盘延迟 | 批量刷盘，减少 fsync 次数 | FR-002 |
| 内存使用 | 可配置，默认 4MB (1000 页) | FR-002 |

### 安全要求

| 需求 | 描述 | 实现方案 |
|-----|------|---------|
| 数据持久化 | 确保数据写入磁盘 | fsync 调用 |
| 文件完整性 | 防止部分写入 | 原子性写入策略 |

---

## 实现文件

| 文件路径 | 职责 |
|---------|------|
| src/pager/mod.rs | 模块入口，Pager 结构 |
| src/pager/cache.rs | LRU 缓存实现 |
| src/pager/freelist.rs | 空闲列表管理 |
| src/pager/header.rs | 数据库文件头 |
| src/pager/page.rs | 页面结构定义 |

---

## 验收标准

| 标准 | 要求 | 验证方法 | 对应PRD |
|-----|------|---------|---------|
| 标准1 | 正确创建数据库文件 | 单元测试：创建后验证文件头 | FR-002 |
| 标准2 | 正确打开数据库文件 | 单元测试：打开后验证元数据 | FR-002 |
| 标准3 | 页面缓存命中 | 单元测试：重复读取同一页，验证缓存 | FR-002 |
| 标准4 | 页面分配 | 单元测试：分配页面，验证 ID 递增 | FR-002 |
| 标准5 | 页面回收 | 单元测试：删除后分配，验证复用 | FR-002 |
| 标准6 | 脏页刷盘 | 单元测试：写入后刷盘，验证文件内容 | FR-002 |

---

## 覆盖映射

### PRD需求覆盖情况

| PRD类型 | PRD编号 | 架构元素 | 覆盖状态 |
|---------|---------|---------|---------|
| 功能需求 | FR-002 | Pager, PageCache | ✅ |
| 用户故事 | US-001 | Pager::open | ✅ |
| 数据实体 | Entity-001 | DatabaseHeader | ✅ |
| 数据实体 | Entity-002 | Page | ✅ |
| 验收标准 | AC-001-01 | Pager::open (create) | ✅ |
| 验收标准 | AC-001-02 | Pager::open (existing) | ✅ |
| 验收标准 | AC-001-03 | DatabaseHeader 验证 | ✅ |

---

## 变更历史

| 版本 | 日期 | 变更内容 | 作者 |
|-----|------|---------|------|
| 1.0 | 2026-03-14 | 初始版本 | Claude |
