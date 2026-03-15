# MOD-05: 事务管理器模块 (Transaction Manager)

## 文档信息
- **项目名称**: sqllite-rust
- **文档编号**: MOD-05
- **版本**: v1.0
- **更新日期**: 2026-03-14
- **对应PRD**: FR-014~015, FR-017

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

**所属层次**: L3-事务层

**架构定位图**:
```
┌─────────────────────────────────────────────────────┐
│              L2: 执行层 (Execution Layer)            │
│              Virtual Machine                         │
└───────────────────────┬─────────────────────────────┘
                        │ ▼ 事务操作
┌─────────────────────────────────────────────────────┐
│         ★ MOD-05: 事务管理器 (Transaction) ★         │
│         Transaction Manager, WAL, Lock Manager       │
└───────────────────────┬─────────────────────────────┘
                        │ ▼ 页面读写
┌─────────────────────────────────────────────────────┐
│              L4: 存储层 (Storage Layer)              │
│              B+ Tree, Pager                          │
└─────────────────────────────────────────────────────┘
```

### 核心职责

- **事务生命周期管理**: BEGIN, COMMIT, ROLLBACK 的实现
- **WAL 预写日志**: 保证持久性和原子性
- **并发控制**: 读写锁实现隔离性
- **崩溃恢复**: WAL 回放恢复数据

### 边界说明

- **负责**:
  - 事务状态管理
  - WAL 日志写入
  - 页面级锁管理
  - 崩溃恢复

- **不负责**:
  - SQL 解析（由 Parser 负责）
  - 字节码执行（由 VM 负责）
  - B+ Tree 操作（由 Storage Engine 负责）

---

## 对应PRD

| PRD章节 | 编号 | 内容 |
|---------|-----|------|
| 功能需求 | FR-014 | 事务支持 (ACID) |
| 功能需求 | FR-015 | WAL 预写日志 |
| 功能需求 | FR-017 | 并发控制 |
| 用户故事 | US-007 | 使用事务 |
| 业务流程 | Flow-002 | 事务流程 |

---

## 全局架构位置

```
┌─────────────────────────────────────────────────────────────────┐
│                        L2: 执行层                                │
│  ┌───────────────────────────────────────────────────────────┐ │
│  │              Virtual Machine (MOD-04)                      │ │
│  └───────────────────────────┬───────────────────────────────┘ │
└──────────────────────────────┼──────────────────────────────────┘
                               │ 事务操作
                               ▼
┌──────────────────────────────┬──────────────────────────────────┐
│                        L3: 事务层                                │
│  ┌───────────────────────────▼───────────────────────────────┐ │
│  │              ★ MOD-05 Transaction Manager ★                │ │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐ │ │
│  │  │ Transaction │  │    WAL      │  │    Lock Manager     │ │ │
│  │  │   State     │  │   Manager   │  │                     │ │ │
│  │  └─────────────┘  └─────────────┘  └─────────────────────┘ │ │
│  └───────────────────────────┬───────────────────────────────┘ │
└──────────────────────────────┼──────────────────────────────────┘
                               │ 页面读写
                               ▼
┌──────────────────────────────┬──────────────────────────────────┐
│                        L4+L5: 存储层                             │
│  ┌───────────────────────────▼───────────────────────────────┐ │
│  │              Storage Engine + Pager                        │ │
│  └───────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
```

---

## 依赖关系

### 上游依赖（本模块调用的模块）

| 模块名称 | 模块编号 | 依赖原因 | 调用方式 |
|---------|---------|---------|---------|
| Pager | MOD-02 | 页面读写 | PageManagementLayer trait |

### 下游依赖（调用本模块的模块）

| 模块名称 | 模块编号 | 被调用场景 | 调用方式 |
|---------|---------|-----------|---------|
| Virtual Machine | MOD-04 | 事务指令执行 | TransactionLayer trait |

---

## 数据流

### 输入数据流

| 数据项 | 来源 | 格式 | 说明 |
|-------|------|------|------|
| 事务指令 | VM | OpCode | Begin/Commit/Rollback |
| 脏页 | VM | Page | 修改后的页面 |

### 输出数据流

| 数据项 | 目标 | 格式 | 说明 |
|-------|------|------|------|
| WAL 记录 | WAL 文件 | WalRecord | 预写日志 |
| 页面 | Pager | Page | 刷盘的页面 |

---

## 核心设计

### 设计目标

| 目标 | 描述 | 度量标准 |
|-----|------|---------|
| 原子性 | 事务要么全成功要么全失败 | 崩溃后可恢复 |
| 一致性 | 数据完整性约束 | 外键约束（未来） |
| 隔离性 | 读已提交 | 无脏读 |
| 持久性 | 提交后数据不丢失 | fsync 保证 |

### 核心组件

#### 1. 事务管理器

```rust
/// 事务管理器
pub struct TransactionManager {
    /// 当前事务状态
    state: TransactionState,
    /// WAL 管理器
    wal: WalManager,
    /// 锁管理器
    lock_manager: LockManager,
    /// 脏页集合（当前事务修改的页面）
    dirty_pages: HashSet<PageId>,
    /// 页面管理器引用
    pager: Arc<Mutex<dyn PageManagementLayer>>,
}

/// 事务状态
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum TransactionState {
    /// 自动提交模式（无活跃事务）
    AutoCommit,
    /// 事务活跃
    Active,
    /// 提交中
    Committing,
    /// 回滚中
    RollingBack,
}

impl TransactionManager {
    /// 开始事务
    pub fn begin(&mut self) -> Result<(), TransactionError> {
        if self.state != TransactionState::AutoCommit {
            return Err(TransactionError::TransactionAlreadyActive);
        }

        self.state = TransactionState::Active;
        self.dirty_pages.clear();

        // 写入 BEGIN 记录到 WAL
        self.wal.write_record(WalRecord::Begin)?;

        Ok(())
    }

    /// 提交事务
    pub fn commit(&mut self) -> Result<(), TransactionError> {
        if self.state != TransactionState::Active {
            return Err(TransactionError::NoActiveTransaction);
        }

        self.state = TransactionState::Committing;

        // 1. 写入 COMMIT 记录到 WAL
        self.wal.write_record(WalRecord::Commit)?;

        // 2. 刷盘 WAL（保证持久性）
        self.wal.flush()?;

        // 3. 将脏页写入数据库文件
        self.flush_dirty_pages()?;

        // 4. 更新文件头
        self.update_database_header()?;

        // 5. 清空脏页集合
        self.dirty_pages.clear();

        self.state = TransactionState::AutoCommit;

        Ok(())
    }

    /// 回滚事务
    pub fn rollback(&mut self) -> Result<(), TransactionError> {
        if self.state != TransactionState::Active {
            return Err(TransactionError::NoActiveTransaction);
        }

        self.state = TransactionState::RollingBack;

        // 1. 丢弃脏页（不写入数据库文件）
        self.discard_dirty_pages()?;

        // 2. 写入 ROLLBACK 记录到 WAL
        self.wal.write_record(WalRecord::Rollback)?;

        self.dirty_pages.clear();
        self.state = TransactionState::AutoCommit;

        Ok(())
    }

    /// 标记页面为脏页
    pub fn mark_page_dirty(&mut self, page_id: PageId) {
        if self.state == TransactionState::Active {
            self.dirty_pages.insert(page_id);
        }
    }

    /// 刷盘脏页
    fn flush_dirty_pages(&mut self) -> Result<(), TransactionError> {
        let pager = self.pager.lock().unwrap();

        for page_id in &self.dirty_pages {
            // 这里应该通过某种方式获取脏页并写入
            // 实际实现中需要与 Pager 协调
        }

        // 强制刷盘
        pager.flush()?;

        Ok(())
    }

    /// 丢弃脏页
    fn discard_dirty_pages(&mut self) -> Result<(), TransactionError> {
        // 从缓存中移除脏页，下次读取时从磁盘重新加载
        let mut pager = self.pager.lock().unwrap();

        for page_id in &self.dirty_pages {
            pager.invalidate_page(*page_id)?;
        }

        Ok(())
    }

    fn update_database_header(&mut self) -> Result<(), TransactionError> {
        // 更新文件头的变更计数器等信息
        Ok(())
    }
}
```

#### 2. WAL 管理器

```rust
/// WAL 管理器
pub struct WalManager {
    /// WAL 文件
    file: File,
    /// 当前 WAL 偏移
    current_offset: u64,
    /// 已刷盘的 WAL 偏移
    flushed_offset: u64,
    /// 页面大小
    page_size: usize,
}

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
        /// 页面更新前的数据（用于回滚）
        before_image: Vec<u8>,
        /// 页面更新后的数据
        after_image: Vec<u8>,
    },
    /// 检查点
    Checkpoint {
        /// 检查点时的数据库大小
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

impl WalManager {
    /// 创建新的 WAL 管理器
    pub fn new(db_path: &str) -> Result<Self, WalError> {
        let wal_path = format!("{}-wal", db_path);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&wal_path)?;

        Ok(Self {
            file,
            current_offset: 0,
            flushed_offset: 0,
            page_size: PAGE_SIZE,
        })
    }

    /// 写入 WAL 记录
    pub fn write_record(&mut self, record: WalRecord) -> Result<(), WalError> {
        // 序列化记录
        let data = self.serialize_record(&record)?;

        // 计算校验和
        let checksum = self.calculate_checksum(&data);

        // 构建记录头
        let header = WalRecordHeader {
            record_type: record.type_code(),
            length: data.len() as u32,
            checksum,
        };

        // 写入头部
        let header_bytes = unsafe {
            std::slice::from_raw_parts(
                &header as *const _ as *const u8,
                std::mem::size_of::<WalRecordHeader>(),
            )
        };
        self.file.write_all(header_bytes)?;

        // 写入数据
        self.file.write_all(&data)?;

        // 更新偏移
        self.current_offset += (std::mem::size_of::<WalRecordHeader>() + data.len()) as u64;

        Ok(())
    }

    /// 刷盘 WAL
    pub fn flush(&mut self) -> Result<(), WalError> {
        self.file.sync_all()?;
        self.flushed_offset = self.current_offset;
        Ok(())
    }

    /// 恢复：读取 WAL 记录
    pub fn read_records(&mut self) -> Result<Vec<WalRecord>, WalError> {
        let mut records = Vec::new();

        self.file.seek(SeekFrom::Start(0))?;

        loop {
            // 读取头部
            let mut header_buf = [0u8; std::mem::size_of::<WalRecordHeader>()];
            match self.file.read_exact(&mut header_buf) {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e.into()),
            }

            let header: WalRecordHeader = unsafe {
                std::ptr::read(header_buf.as_ptr() as *const _)
            };

            // 读取数据
            let mut data = vec![0u8; header.length as usize];
            self.file.read_exact(&mut data)?;

            // 验证校验和
            let checksum = self.calculate_checksum(&data);
            if checksum != header.checksum {
                return Err(WalError::ChecksumMismatch);
            }

            // 反序列化记录
            let record = self.deserialize_record(header.record_type, &data)?;
            records.push(record);
        }

        Ok(records)
    }

    /// 崩溃恢复
    pub fn recover(&mut self, pager: &mut dyn PageManagementLayer) -> Result<(), WalError> {
        let records = self.read_records()?;

        // 找到最后一个完整的已提交事务
        let mut last_commit_idx: Option<usize> = None;
        for (i, record) in records.iter().enumerate() {
            if matches!(record, WalRecord::Commit) {
                last_commit_idx = Some(i);
            }
        }

        if let Some(commit_idx) = last_commit_idx {
            // 重做已提交事务的更新
            for record in &records[..=commit_idx] {
                if let WalRecord::Update { page_id, after_image, .. } = record {
                    // 将 after_image 写入对应页面
                    let mut page = pager.get_page(*page_id)?;
                    page.data.copy_from_slice(after_image);
                    pager.write_page(&page)?;
                }
            }

            // 刷盘恢复的数据
            pager.flush()?;
        }

        // 清空 WAL（可选，或截断到检查点）
        self.file.set_len(0)?;
        self.current_offset = 0;
        self.flushed_offset = 0;

        Ok(())
    }

    fn serialize_record(&self, record: &WalRecord) -> Result<Vec<u8>, WalError> {
        match record {
            WalRecord::Begin => Ok(vec![]),
            WalRecord::Commit => Ok(vec![]),
            WalRecord::Rollback => Ok(vec![]),
            WalRecord::Update { page_id, before_image, after_image } => {
                let mut data = Vec::new();
                data.extend_from_slice(&page_id.to_be_bytes());
                data.extend_from_slice(&(before_image.len() as u32).to_be_bytes());
                data.extend_from_slice(before_image);
                data.extend_from_slice(&(after_image.len() as u32).to_be_bytes());
                data.extend_from_slice(after_image);
                Ok(data)
            }
            WalRecord::Checkpoint { database_size } => {
                Ok(database_size.to_be_bytes().to_vec())
            }
        }
    }

    fn deserialize_record(&self, record_type: u8, data: &[u8]) -> Result<WalRecord, WalError> {
        match record_type {
            0x01 => Ok(WalRecord::Begin),
            0x02 => Ok(WalRecord::Commit),
            0x03 => Ok(WalRecord::Rollback),
            0x04 => {
                // Update record
                let page_id = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);
                let before_len = u32::from_be_bytes([data[4], data[5], data[6], data[7]]) as usize;
                let before_image = data[8..8 + before_len].to_vec();
                let after_len_start = 8 + before_len;
                let after_len = u32::from_be_bytes([
                    data[after_len_start],
                    data[after_len_start + 1],
                    data[after_len_start + 2],
                    data[after_len_start + 3],
                ]) as usize;
                let after_image = data[after_len_start + 4..after_len_start + 4 + after_len].to_vec();

                Ok(WalRecord::Update {
                    page_id,
                    before_image,
                    after_image,
                })
            }
            0x05 => {
                let database_size = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);
                Ok(WalRecord::Checkpoint { database_size })
            }
            _ => Err(WalError::UnknownRecordType(record_type)),
        }
    }

    fn calculate_checksum(&self, data: &[u8]) -> u32 {
        // 简单的 CRC32 实现
        let mut crc = 0xFFFFFFFFu32;
        for &byte in data {
            crc ^= byte as u32;
            for _ in 0..8 {
                if crc & 1 != 0 {
                    crc = (crc >> 1) ^ 0xEDB88320;
                } else {
                    crc >>= 1;
                }
            }
        }
        !crc
    }
}

impl WalRecord {
    fn type_code(&self) -> u8 {
        match self {
            WalRecord::Begin => 0x01,
            WalRecord::Commit => 0x02,
            WalRecord::Rollback => 0x03,
            WalRecord::Update { .. } => 0x04,
            WalRecord::Checkpoint { .. } => 0x05,
        }
    }
}
```

#### 3. 锁管理器

```rust
/// 锁管理器
pub struct LockManager {
    /// 数据库文件读写锁
    db_lock: RwLock<()>,
    /// 活跃读事务计数
    active_readers: AtomicU32,
    /// 是否有写事务
    has_writer: AtomicBool,
}

/// 锁类型
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum LockType {
    /// 无锁
    None,
    /// 共享锁（读）
    Shared,
    /// 排他锁（写）
    Exclusive,
}

impl LockManager {
    pub fn new() -> Self {
        Self {
            db_lock: RwLock::new(()),
            active_readers: AtomicU32::new(0),
            has_writer: AtomicBool::new(false),
        }
    }

    /// 获取读锁
    pub fn acquire_read_lock(&self) -> Result<ReadGuard, LockError> {
        // 检查是否有写事务
        if self.has_writer.load(Ordering::SeqCst) {
            return Err(LockError::WriteInProgress);
        }

        let guard = self.db_lock.read().map_err(|_| LockError::Poisoned)?;
        self.active_readers.fetch_add(1, Ordering::SeqCst);

        Ok(ReadGuard {
            lock_manager: self,
            _guard: guard,
        })
    }

    /// 获取写锁
    pub fn acquire_write_lock(&self) -> Result<WriteGuard, LockError> {
        // 检查是否已有写事务
        if self.has_writer.load(Ordering::SeqCst) {
            return Err(LockError::WriteAlreadyHeld);
        }

        // 设置写标记
        self.has_writer.store(true, Ordering::SeqCst);

        let guard = self.db_lock.write().map_err(|_| LockError::Poisoned)?;

        Ok(WriteGuard {
            lock_manager: self,
            _guard: guard,
        })
    }

    fn release_read_lock(&self) {
        self.active_readers.fetch_sub(1, Ordering::SeqCst);
    }

    fn release_write_lock(&self) {
        self.has_writer.store(false, Ordering::SeqCst);
    }
}

/// 读锁守卫
pub struct ReadGuard<'a> {
    lock_manager: &'a LockManager,
    _guard: std::sync::RwLockReadGuard<'a, ()>,
}

impl<'a> Drop for ReadGuard<'a> {
    fn drop(&mut self) {
        self.lock_manager.release_read_lock();
    }
}

/// 写锁守卫
pub struct WriteGuard<'a> {
    lock_manager: &'a LockManager,
    _guard: std::sync::RwLockWriteGuard<'a, ()>,
}

impl<'a> Drop for WriteGuard<'a> {
    fn drop(&mut self) {
        self.lock_manager.release_write_lock();
    }
}
```

---

## 接口定义

### 对外接口清单

| 接口编号 | 接口名称 | 方法 | 对应PRD |
|---------|---------|------|---------|
| API-011 | TransactionManager::begin | fn begin(&mut self) -> Result<()> | FR-014 |
| API-012 | TransactionManager::commit | fn commit(&mut self) -> Result<()> | FR-014 |
| API-013 | TransactionManager::rollback | fn rollback(&mut self) -> Result<()> | FR-014 |

### 接口详细定义

#### API-011: TransactionManager::begin

**对应PRD**:
- 用户故事: US-007
- 验收标准: AC-007-01

**接口定义**:
```rust
/// 开始事务
///
/// # Returns
/// * `Ok(())` - 事务开始成功
/// * `Err(TransactionError::TransactionAlreadyActive)` - 已有活跃事务
pub fn begin(&mut self) -> Result<(), TransactionError>
```

#### API-012: TransactionManager::commit

**对应PRD**:
- 用户故事: US-007
- 验收标准: AC-007-02

**接口定义**:
```rust
/// 提交事务
///
/// # Returns
/// * `Ok(())` - 提交成功
/// * `Err(TransactionError::NoActiveTransaction)` - 无活跃事务
pub fn commit(&mut self) -> Result<(), TransactionError>
```

#### API-013: TransactionManager::rollback

**对应PRD**:
- 用户故事: US-007
- 验收标准: AC-007-03

**接口定义**:
```rust
/// 回滚事务
///
/// # Returns
/// * `Ok(())` - 回滚成功
/// * `Err(TransactionError::NoActiveTransaction)` - 无活跃事务
pub fn rollback(&mut self) -> Result<(), TransactionError>
```

---

## 数据结构

### 核心实体

已在核心设计部分定义，见 [事务管理器](#1-事务管理器)、[WAL 管理器](#2-wal-管理器)、[锁管理器](#3-锁管理器)。

---

## 状态机设计

### STATE-003: 事务流程

**对应PRD**: Flow-002

**状态定义**:
| 状态 | 编码 | PRD描述 | 说明 |
|-----|------|---------|------|
| AutoCommit | 0 | 自动提交模式 | 初始状态，每条语句自动提交 |
| Active | 1 | 事务活跃 | BEGIN 后状态 |
| Committing | 2 | 提交中 | COMMIT 处理中 |
| RollingBack | 3 | 回滚中 | ROLLBACK 处理中 |

**状态转换**:
| 编号 | 当前状态 | 触发事件 | 下一状态 | 条件 | 对应PRD |
|-----|---------|---------|---------|------|---------|
| T001 | AutoCommit | BEGIN | Active | 无活跃事务 | Flow-002 |
| T002 | Active | COMMIT | Committing | - | Flow-002 |
| T003 | Committing | 完成 | AutoCommit | 刷盘成功 | Flow-002 |
| T004 | Active | ROLLBACK | RollingBack | - | Flow-002 |
| T005 | RollingBack | 完成 | AutoCommit | 丢弃脏页 | Flow-002 |
| T006 | Active | 错误 | RollingBack | 自动回滚 | Flow-002 |

**状态转换图**:
```
                    BEGIN
    ┌───────────────┐
    │               │
    ▼               │
┌─────────┐         │
│AutoCommit│◄────────┘
└────┬────┘    COMMIT/ROLLBACK 完成
     │
     │ BEGIN
     ▼
┌─────────┐    COMMIT    ┌───────────┐    完成    ┌─────────┐
│  Active  │─────────────▶│ Committing │──────────▶│AutoCommit│
└────┬────┘              └───────────┘           └─────────┘
     │
     │ ROLLBACK
     ▼
┌───────────┐    完成    ┌─────────┐
│RollingBack │──────────▶│AutoCommit│
└───────────┘           └─────────┘
```

---

## 边界条件

### BOUND-001: 嵌套事务

**对应PRD**: FR-014

**触发条件**:
- 在已有活跃事务时调用 BEGIN

**处理方式**:
- 返回 TransactionAlreadyActive 错误
- 暂不支持嵌套事务

### BOUND-002: 无事务时提交/回滚

**对应PRD**: FR-014

**触发条件**:
- 在无活跃事务时调用 COMMIT 或 ROLLBACK

**处理方式**:
- 返回 NoActiveTransaction 错误

### BOUND-003: 事务失败自动回滚

**对应PRD**: AC-007-04

**触发条件**:
- 事务执行过程中发生错误

**处理方式**:
- 自动触发回滚
- 丢弃所有脏页
- 返回错误信息

### BOUND-004: WAL 文件损坏

**对应PRD**: FR-015

**触发条件**:
- WAL 文件校验和不匹配

**处理方式**:
- 返回 ChecksumMismatch 错误
- 可能需要手动删除 WAL 文件恢复

---

## 非功能需求

### 性能要求

| 指标 | 要求 | 对应PRD |
|-----|------|---------|
| 事务开销 | BEGIN/COMMIT < 1ms | FR-014 |
| WAL 写入 | 批量写入，减少 fsync | FR-015 |
| 并发读取 | 支持多读者 | FR-017 |

### 可靠性要求

| 需求 | 描述 | 实现方案 |
|-----|------|---------|
| 崩溃恢复 | 数据库崩溃后可恢复 | WAL 回放 |
| 数据校验 | 检测 WAL 损坏 | 校验和 |

---

## 实现文件

| 文件路径 | 职责 |
|---------|------|
| src/transaction/mod.rs | 模块入口，TransactionManager |
| src/transaction/state.rs | 事务状态定义 |
| src/transaction/wal.rs | WAL 管理器 |
| src/transaction/lock.rs | 锁管理器 |
| src/transaction/error.rs | 错误类型 |

---

## 验收标准

| 标准 | 要求 | 验证方法 | 对应PRD |
|-----|------|---------|---------|
| 标准1 | BEGIN 正确开始事务 | 单元测试：验证状态变化 | FR-014 |
| 标准2 | COMMIT 正确提交 | 单元测试：验证数据持久化 | FR-014 |
| 标准3 | ROLLBACK 正确回滚 | 单元测试：验证数据未改变 | FR-014 |
| 标准4 | WAL 正确写入 | 单元测试：验证 WAL 文件内容 | FR-015 |
| 标准5 | 崩溃恢复正确 | 单元测试：模拟崩溃后恢复 | FR-015 |
| 标准6 | 并发读正确 | 单元测试：多线程读取 | FR-017 |

---

## 覆盖映射

### PRD需求覆盖情况

| PRD类型 | PRD编号 | 架构元素 | 覆盖状态 |
|---------|---------|---------|---------|
| 功能需求 | FR-014 | TransactionManager | ✅ |
| 功能需求 | FR-015 | WalManager | ✅ |
| 功能需求 | FR-017 | LockManager | ✅ |
| 用户故事 | US-007 | begin/commit/rollback | ✅ |
| 业务流程 | Flow-002 | STATE-003 | ✅ |
| 验收标准 | AC-007-01~04 | 事务接口 | ✅ |

---

## 变更历史

| 版本 | 日期 | 变更内容 | 作者 |
|-----|------|---------|------|
| 1.0 | 2026-03-14 | 初始版本 | Claude |
