# 开发计划 - MOD-05: Transaction Manager (事务管理器)

## 文档信息
- **模块编号**: MOD-05
- **模块名称**: Transaction Manager (事务管理器)
- **所属层次**: L3-事务层
- **对应架构**: docs/v1/02-architecture/03-mod-05-transaction.md
- **优先级**: P0 (阶段 3)
- **预估工时**: 3天

---

## 1. 模块概述

### 1.1 模块职责
- 事务生命周期管理 (BEGIN/COMMIT/ROLLBACK)
- WAL (Write-Ahead Logging) 预写日志
- 并发控制 (读写锁)
- 崩溃恢复

### 1.2 对应PRD
| PRD编号 | 功能 | 用户故事 |
|---------|-----|---------|
| FR-014 | 事务支持 (ACID) | US-007 |
| FR-015 | WAL 预写日志 | US-007 |
| FR-017 | 并发控制 | US-007 |

### 1.3 架构定位
```
VM → Transaction Manager → (Storage, Pager)
```

---

## 2. 技术设计

### 2.1 目录结构
```
src/transaction/
├── mod.rs           # 模块入口，TransactionManager
├── state.rs         # 事务状态定义
├── wal.rs           # WAL 管理器
├── lock.rs          # 锁管理器
└── error.rs         # 错误类型
```

### 2.2 依赖关系
| 依赖模块 | 依赖方式 | 用途 |
|---------|---------|------|
| MOD-02 Pager | use crate::pager | 页面管理 |
| MOD-01 Storage | use crate::storage | 存储操作 |

---

## 3. 接口清单

| 任务编号 | 接口编号 | 接口名称 | 复杂度 |
|---------|---------|---------|-------|
| T-03 | API-009 | TransactionManager::begin | 低 |
| T-04 | API-010 | TransactionManager::commit | 高 |
| T-05 | API-011 | TransactionManager::rollback | 中 |

---

## 4. 开发任务拆分

### 任务清单

| 任务编号 | 任务名称 | 涉及文件 | 代码行数 | 依赖 |
|---------|---------|---------|---------|------|
| T-01 | 错误类型定义 | 1 | ~30 | - |
| T-02 | 事务状态定义 | 1 | ~40 | T-01 |
| T-03 | 锁管理器 | 2 | ~100 | T-01 |
| T-04 | WAL 管理器 | 2 | ~200 | T-01 |
| T-05 | 事务管理器核心 | 2 | ~150 | T-02, T-03, T-04 |
| T-06 | 崩溃恢复 | 1 | ~100 | T-04, T-05 |
| T-07 | 单元测试 | 5 | ~200 | T-01~06 |

---

## 5. 详细任务定义

### T-01: 错误类型定义

**任务概述**: 定义事务模块的错误类型

**输出**:
- `src/transaction/error.rs`

**实现要求**:
```rust
use thiserror::Error;

#[derive(Error, Debug)]
pub enum TransactionError {
    #[error("Transaction already active")]
    TransactionAlreadyActive,

    #[error("No active transaction")]
    NoActiveTransaction,

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Lock error: {0}")]
    LockError(String),

    #[error("WAL error: {0}")]
    WalError(String),
}

pub type Result<T> = std::result::Result<T, TransactionError>;
```

**预估工时**: 0.5小时

---

### T-02: 事务状态定义

**任务概述**: 定义事务状态机

**输出**:
- `src/transaction/state.rs`

**实现要求**:
```rust
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum TransactionState {
    AutoCommit,
    Active,
    Committing,
    RollingBack,
}

impl TransactionState {
    pub fn can_begin(&self) -> bool {
        matches!(self, TransactionState::AutoCommit)
    }

    pub fn can_commit(&self) -> bool {
        matches!(self, TransactionState::Active)
    }

    pub fn can_rollback(&self) -> bool {
        matches!(self, TransactionState::Active)
    }
}
```

**预估工时**: 0.5小时

**依赖**: T-01

---

### T-03: 锁管理器

**任务概述**: 实现读写锁管理

**输出**:
- `src/transaction/lock.rs`

**实现要求**:
```rust
use std::sync::{RwLock, Arc};
use std::sync::atomic::{AtomicU32, AtomicBool, Ordering};

pub struct LockManager {
    db_lock: RwLock<()>,
    active_readers: AtomicU32,
    has_writer: AtomicBool,
}

pub struct ReadGuard<'a> {
    lock_manager: &'a LockManager,
    _guard: std::sync::RwLockReadGuard<'a, ()>,
}

pub struct WriteGuard<'a> {
    lock_manager: &'a LockManager,
    _guard: std::sync::RwLockWriteGuard<'a, ()>,
}

impl LockManager {
    pub fn new() -> Self {
        Self {
            db_lock: RwLock::new(()),
            active_readers: AtomicU32::new(0),
            has_writer: AtomicBool::new(false),
        }
    }

    pub fn acquire_read(&self) -> Result<ReadGuard, TransactionError> {
        // 获取读锁
    }

    pub fn acquire_write(&self) -> Result<WriteGuard, TransactionError> {
        // 获取写锁
    }
}

impl<'a> Drop for ReadGuard<'a> {
    fn drop(&mut self) {
        self.lock_manager.active_readers.fetch_sub(1, Ordering::SeqCst);
    }
}

impl<'a> Drop for WriteGuard<'a> {
    fn drop(&mut self) {
        self.lock_manager.has_writer.store(false, Ordering::SeqCst);
    }
}
```

**验收标准**:
- [ ] 支持多读者
- [ ] 支持单写者
- [ ] 正确处理锁竞争

**测试要求**:
- 测试用例: 3个（读锁、写锁、并发）

**预估工时**: 2小时

**依赖**: T-01

---

### T-04: WAL 管理器

**任务概述**: 实现预写日志管理

**输出**:
- `src/transaction/wal.rs`

**实现要求**:
```rust
use std::fs::File;
use std::io::{Read, Write, Seek, SeekFrom};

pub struct WalManager {
    file: File,
    current_offset: u64,
    flushed_offset: u64,
    page_size: usize,
}

#[derive(Debug, Clone)]
pub enum WalRecord {
    Begin,
    Commit,
    Rollback,
    Update {
        page_id: u32,
        before_image: Vec<u8>,
        after_image: Vec<u8>,
    },
    Checkpoint {
        database_size: u32,
    },
}

impl WalManager {
    pub fn new(db_path: &str) -> Result<Self, TransactionError> {
        // 创建或打开 WAL 文件
    }

    pub fn write_record(&mut self, record: &WalRecord) -> Result<(), TransactionError> {
        // 写入 WAL 记录
    }

    pub fn flush(&mut self) -> Result<(), TransactionError> {
        // 刷盘 WAL
    }

    pub fn read_records(&mut self) -> Result<Vec<WalRecord>, TransactionError> {
        // 读取所有 WAL 记录
    }

    pub fn clear(&mut self) -> Result<(), TransactionError> {
        // 清空 WAL 文件
    }

    fn calculate_checksum(&self, data: &[u8]) -> u32 {
        // 计算 CRC32 校验和
    }
}
```

**验收标准**:
- [ ] 正确写入 WAL 记录
- [ ] 正确刷盘
- [ ] 校验和验证

**测试要求**:
- 测试用例: 4个（写入、读取、校验和、清空）

**预估工时**: 6小时

**依赖**: T-01

---

### T-05: 事务管理器核心

**任务概述**: 实现事务管理器核心功能

**输出**:
- `src/transaction/mod.rs`

**实现要求**:
```rust
use crate::transaction::{TransactionState, LockManager, WalManager, WalRecord};
use crate::pager::{Pager, PageId};
use std::sync::{Arc, Mutex};
use std::collections::HashSet;

pub struct TransactionManager {
    state: TransactionState,
    wal: WalManager,
    lock_manager: LockManager,
    dirty_pages: HashSet<PageId>,
    pager: Arc<Mutex<Pager>>,
}

impl TransactionManager {
    pub fn new(pager: Arc<Mutex<Pager>>, db_path: &str) -> Result<Self, TransactionError> {
        // 初始化
    }

    pub fn begin(&mut self) -> Result<(), TransactionError> {
        // 开始事务
    }

    pub fn commit(&mut self) -> Result<(), TransactionError> {
        // 提交事务
    }

    pub fn rollback(&mut self) -> Result<(), TransactionError> {
        // 回滚事务
    }

    pub fn mark_page_dirty(&mut self, page_id: PageId) {
        // 标记脏页
    }

    fn flush_dirty_pages(&mut self) -> Result<(), TransactionError> {
        // 刷盘脏页
    }

    fn discard_dirty_pages(&mut self) -> Result<(), TransactionError> {
        // 丢弃脏页
    }
}
```

**验收标准**:
- [ ] 正确管理事务状态
- [ ] 正确提交事务
- [ ] 正确回滚事务

**测试要求**:
- 测试用例: 5个（begin、commit、rollback、嵌套、自动回滚）

**预估工时**: 4小时

**依赖**: T-02, T-03, T-04

---

### T-06: 崩溃恢复

**任务概述**: 实现崩溃恢复

**输出**:
- `src/transaction/wal.rs`（恢复部分）

**实现要求**:
```rust
impl WalManager {
    pub fn recover(&mut self, pager: &mut Pager) -> Result<(), TransactionError> {
        // 1. 读取所有 WAL 记录
        let records = self.read_records()?;

        // 2. 找到最后一个已提交事务
        let last_commit_idx = self.find_last_commit(&records);

        // 3. 重做已提交事务的更新
        if let Some(idx) = last_commit_idx {
            for record in &records[..=idx] {
                if let WalRecord::Update { page_id, after_image, .. } = record {
                    // 将 after_image 写入页面
                }
            }
        }

        // 4. 刷盘恢复的数据
        pager.flush()?;

        // 5. 清空 WAL
        self.clear()?;

        Ok(())
    }

    fn find_last_commit(&self, records: &[WalRecord]) -> Option<usize> {
        // 找到最后一个 Commit 记录的索引
        records.iter().rposition(|r| matches!(r, WalRecord::Commit))
    }
}
```

**验收标准**:
- [ ] 正确恢复已提交事务
- [ ] 正确丢弃未提交事务
- [ ] 恢复后数据一致性

**测试要求**:
- 测试用例: 3个（正常恢复、部分恢复、空 WAL）

**预估工时**: 3小时

**依赖**: T-04, T-05

---

### T-07: 单元测试

**任务概述**: 编写完整的单元测试

**输出**:
- 各文件中的 `#[cfg(test)]` 模块

**测试清单**:
| 测试目标 | 测试文件 | 用例数 |
|---------|---------|-------|
| LockManager | lock.rs | 3 |
| WalManager | wal.rs | 4 |
| TransactionManager | mod.rs | 5 |
| Recovery | wal.rs | 3 |

**预估工时**: 3小时

**依赖**: T-01~06

---

## 6. 验收清单

- [ ] 事务 BEGIN/COMMIT/ROLLBACK 正确
- [ ] WAL 写入和恢复正确
- [ ] 并发控制正确
- [ ] 崩溃恢复正确
- [ ] 测试覆盖率 ≥ 80%

---

## 7. 覆盖映射

| 架构元素 | 架构编号 | 任务 | 覆盖状态 |
|---------|---------|------|---------|
| 错误类型 | - | T-01 | ✅ |
| TransactionState | STATE-003 | T-02 | ✅ |
| LockManager | - | T-03 | ✅ |
| WalManager | - | T-04, T-06 | ✅ |
| TransactionManager | API-009~011 | T-05 | ✅ |

---

## 变更历史

| 版本 | 日期 | 变更内容 | 作者 |
|-----|------|---------|------|
| 1.0 | 2026-03-14 | 初始版本 | Claude |
