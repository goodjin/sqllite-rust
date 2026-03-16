# V3: 无锁并发架构 (Lock-Free Concurrency)

## 1. 原理说明

### 1.1 当前问题

SQLite 使用**文件锁**：
- 单写多读（Writers don't block readers）
- WAL 模式下可实现多读者
- 但仍存在锁竞争，扩展性受限

### 1.2 无锁并发原理

**MVCC (多版本并发控制)**：
- 写操作创建新版本，不阻塞读
- 读操作看到一致的快照
- 无锁数据结构避免争用

**Lock-Free 数据结构**：
- 使用原子操作 (CAS)
- 保证至少一个线程前进
- 避免死锁和优先级反转

### 1.3 架构对比

```
SQLite:                          V3 Lock-Free:
┌─────────────┐                 ┌─────────────┐
│  文件锁      │                 │  Lock-Free  │
│  ├─ 排它锁   │                 │  ├─ MVCC    │
│  └─ 共享锁   │                 │  ├─ RCU     │
└─────────────┘                 │  └─ 原子操作 │
                                └─────────────┘
```

## 2. 实现方式

### 2.1 MVCC 核心结构

```rust
use crossbeam_epoch::{self as epoch, Atomic, Owned, Shared, Guard};
use std::sync::atomic::{AtomicU64, Ordering};

/// MVCC 事务管理器
pub struct MvccTransactionManager {
    /// 全局事务 ID 生成器
    txn_id_gen: AtomicU64,

    /// 全局时间戳 (单调递增)
    global_timestamp: AtomicU64,

    /// 活动事务集合
    active_txns: DashSet<u64>,

    /// 版本存储
    version_store: VersionStore,
}

/// 版本存储
pub struct VersionStore {
    /// 每页一个版本链
    versions: DashMap<PageId, Atomic<VersionNode>>,
}

/// 版本链表节点
#[repr(align(64))]
pub struct VersionNode {
    /// 事务 ID
    txn_id: u64,

    /// 开始时间戳
    begin_ts: u64,

    /// 结束时间戳 (0 = 当前有效)
    end_ts: AtomicU64,

    /// 页面数据
    data: Page,

    /// 下一个版本
    next: Atomic<VersionNode>,
}

/// 事务
pub struct Transaction {
    /// 事务 ID
    txn_id: u64,

    /// 读时间戳
    read_ts: u64,

    /// 写时间戳
    write_ts: u64,

    /// 本地写集
    write_set: HashMap<PageId, Page>,

    /// 本地读集 (用于冲突检测)
    read_set: HashSet<PageId>,

    /// 状态
    state: TransactionState,
}

#[derive(Clone, Copy, PartialEq)]
pub enum TransactionState {
    Active,
    Preparing,
    Committed,
    Aborted,
}

impl MvccTransactionManager {
    /// 开始事务
    pub fn begin(&self) -> Transaction {
        let txn_id = self.txn_id_gen.fetch_add(1, Ordering::SeqCst);
        let read_ts = self.global_timestamp.load(Ordering::SeqCst);

        self.active_txns.insert(txn_id);

        Transaction {
            txn_id,
            read_ts,
            write_ts: 0,
            write_set: HashMap::new(),
            read_set: HashSet::new(),
            state: TransactionState::Active,
        }
    }

    /// 读取页面 (MVCC)
    pub fn read(
        &self,
        txn: &mut Transaction,
        page_id: PageId,
    ) -> Result<Page> {
        txn.read_set.insert(page_id);

        // 检查本地写集
        if let Some(page) = txn.write_set.get(&page_id) {
            return Ok(page.clone());
        }

        // 从版本链读取可见版本
        let guard = &epoch::pin();

        let head = self.version_store.versions
            .get(&page_id)
            .ok_or(Error::PageNotFound)?;

        let mut current = head.load(Ordering::Acquire, guard);

        while let Some(node) = unsafe { current.as_ref() } {
            // 可见性判断
            if self.is_visible(txn, node) {
                return Ok(node.data.clone());
            }
            current = node.next.load(Ordering::Acquire, guard);
        }

        Err(Error::PageNotFound)
    }

    /// 写入页面
    pub fn write(
        &self,
        txn: &mut Transaction,
        page_id: PageId,
        data: Page,
    ) -> Result<()> {
        if txn.state != TransactionState::Active {
            return Err(Error::TransactionNotActive);
        }

        // 写入本地缓存
        txn.write_set.insert(page_id, data);
        Ok(())
    }

    /// 提交事务
    pub fn commit(&self, txn: &mut Transaction) -> Result<()> {
        txn.state = TransactionState::Preparing;

        // 1. 获取写时间戳
        txn.write_ts = self.global_timestamp.fetch_add(1, Ordering::SeqCst);

        // 2. 冲突检测
        if self.detect_conflict(txn) {
            txn.state = TransactionState::Aborted;
            return Err(Error::Conflict);
        }

        // 3. 写入版本链
        for (page_id, data) in &txn.write_set {
            self.install_version(txn, *page_id, data.clone())?;
        }

        // 4. 提交成功
        txn.state = TransactionState::Committed;
        self.active_txns.remove(&txn.txn_id);

        Ok(())
    }

    /// 冲突检测 (简化 OCC)
    fn detect_conflict(&self,
        txn: &Transaction,
    ) -> bool {
        // 检查读集中的页面是否被其他事务修改
        for page_id in &txn.read_set {
            let guard = &epoch::pin();

            if let Some(head) = self.version_store.versions.get(page_id) {
                let current = head.load(Ordering::Acquire, guard);

                if let Some(node) = unsafe { current.as_ref() } {
                    // 有事务在 [txn.read_ts, now] 期间修改了该页
                    if node.begin_ts > txn.read_ts
                        && node.txn_id != txn.txn_id
                        && self.active_txns.contains(&node.txn_id) {
                        return true; // 冲突
                    }
                }
            }
        }

        false
    }

    /// 安装新版本
    fn install_version(
        &self,
        txn: &Transaction,
        page_id: PageId,
        data: Page,
    ) -> Result<()> {
        let guard = &epoch::pin();

        // 创建新版本节点
        let new_version = Owned::new(VersionNode {
            txn_id: txn.txn_id,
            begin_ts: txn.write_ts,
            end_ts: AtomicU64::new(0),
            data,
            next: Atomic::null(),
        });

        loop {
            // 获取当前头节点
            let head = self.version_store.versions
                .get(&page_id)
                .ok_or(Error::PageNotFound)?;

            let current = head.load(Ordering::Acquire, guard);

            // 设置新版本的 next 指向当前头
            new_version.next.store(current, Ordering::Relaxed);

            // CAS 尝试将头节点替换为新版本
            match head.compare_exchange(
                current,
                new_version,
                Ordering::Release,
                Ordering::Relaxed,
                guard,
            ) {
                Ok(_) => {
                    // 成功，设置旧版本的 end_ts
                    if let Some(old) = unsafe { current.as_ref() } {
                        old.end_ts.store(txn.write_ts, Ordering::Release);
                    }
                    return Ok(());
                }
                Err(_) => {
                    // 失败，重试
                    continue;
                }
            }
        }
    }

    /// 可见性判断
    fn is_visible(
        &self,
        txn: &Transaction,
        node: &VersionNode,
    ) -> bool {
        // 自己的写可见
        if node.txn_id == txn.txn_id {
            return true;
        }

        // 已提交且 begin_ts <= read_ts
        if node.begin_ts <= txn.read_ts {
            let end_ts = node.end_ts.load(Ordering::Acquire);
            // end_ts = 0 表示未删除，或 end_ts > read_ts
            return end_ts == 0 || end_ts > txn.read_ts;
        }

        false
    }
}
```

### 2.2 Lock-Free B-Tree

```rust
/// 无锁 B-Tree 索引
pub struct LockFreeBTree {
    root: Atomic<Node>,
}

/// B-Tree 节点
#[repr(align(64))]
pub enum Node {
    Leaf {
        keys: Vec<Value>,
        values: Vec<Vec<u64>>,  // rowids
        next: Atomic<Node>,     // 叶子链表
    },
    Internal {
        keys: Vec<Value>,
        children: Vec<Atomic<Node>>,
    },
}

impl LockFreeBTree {
    /// 无锁查找
    pub fn search(&self,
        key: &Value,
    ) -> Option<Vec<u64>> {
        let guard = &epoch::pin();
        let mut current = self.root.load(Ordering::Acquire, guard);

        loop {
            match unsafe { current.as_ref() }? {
                Node::Leaf { keys, values, .. } => {
                    // 二分查找
                    match keys.binary_search(key) {
                        Ok(idx) => return Some(values[idx].clone()),
                        Err(_) => return None,
                    }
                }
                Node::Internal { keys, children } => {
                    // 找到子节点
                    let idx = keys.binary_search(key)
                        .unwrap_or_else(|i| i);
                    current = children[idx].load(Ordering::Acquire, guard);
                }
            }
        }
    }

    /// 乐观并发插入
    pub fn insert(
        &self,
        key: Value,
        rowid: u64,
    ) -> Result<()> {
        let guard = &epoch::pin();

        loop {
            // 乐观尝试插入
            match self.try_insert(&key, rowid, guard) {
                Ok(_) => return Ok(()),
                Err(Error::ConcurrentModification) => {
                    // 结构修改，重试
                    std::thread::yield_now();
                    continue;
                }
                Err(e) => return Err(e),
            }
        }
    }

    fn try_insert(
        &self,
        key: &Value,
        rowid: u64,
        guard: &Guard,
    ) -> Result<()> {
        // 找到叶子节点
        let path = self.find_leaf_path(key, guard)?;
        let leaf = path.last().unwrap();

        match unsafe { leaf.as_ref() } {
            Some(Node::Leaf { keys, values, .. }) => {
                // 检查是否已存在
                if let Ok(idx) = keys.binary_search(key) {
                    // 追加 rowid
                    return self.append_rowid(leaf, idx, rowid, guard);
                }

                // 检查是否需要分裂
                if keys.len() >= MAX_KEYS {
                    return Err(Error::NeedSplit);
                }

                // 插入新键值对
                self.insert_to_leaf(leaf, key.clone(), vec![rowid], guard)
            }
            _ => Err(Error::InvalidState),
        }
    }

    /// 无锁分裂
    fn split_leaf(
        &self,
        leaf: Shared<Node>,
        guard: &Guard,
    ) -> Result<()> {
        // 1. 创建新叶子节点
        let new_leaf = self.create_new_leaf(guard)?;

        // 2. 复制一半数据到新节点
        self.transfer_half_data(leaf, new_leaf, guard)?;

        // 3. 原子更新链表
        self.link_new_leaf(leaf, new_leaf, guard)?;

        // 4. 更新父节点 (可能需要递归分裂)
        self.update_parent(leaf, new_leaf, guard)
    }
}
```

### 2.3 无锁页面缓存

```rust
/// 无锁页面缓存 (基于 eviction-free 设计)
pub struct LockFreePageCache {
    /// 固定大小的槽数组
    slots: Vec<Atomic<CacheEntry>>,

    /// 哈希函数
    hasher: RandomState,
}

/// 缓存条目
#[repr(align(64))]
pub struct CacheEntry {
    /// 页面 ID (0 = 空槽)
    page_id: AtomicU64,

    /// 页面数据
    page: UnsafeCell<Option<Page>>,

    /// 访问计数 (用于近似 LRU)
    access_count: AtomicU64,
}

impl LockFreePageCache {
    pub fn with_capacity(capacity: usize) -> Self {
        let mut slots = Vec::with_capacity(capacity);
        for _ in 0..capacity {
            slots.push(Atomic::new(CacheEntry {
                page_id: AtomicU64::new(0),
                page: UnsafeCell::new(None),
                access_count: AtomicU64::new(0),
            }));
        }

        Self {
            slots,
            hasher: RandomState::new(),
        }
    }

    /// 无锁读取
    pub fn get(&self,
        page_id: PageId,
    ) -> Option<Page> {
        let hash = self.hasher.hash_one(page_id) as usize;
        let start_idx = hash % self.slots.len();

        // 线性探测
        for i in 0..self.slots.len() {
            let idx = (start_idx + i) % self.slots.len();
            let entry = &self.slots[idx];

            let stored_id = unsafe {
                (*entry.as_ptr()).page_id.load(Ordering::Acquire)
            };

            if stored_id == page_id as u64 {
                // 命中
                unsafe {
                    (*entry.as_ptr()).access_count.fetch_add(1, Ordering::Relaxed);
                    return (*entry.as_ptr()).page.get().as_ref()
                        .and_then(|p| p.clone());
                }
            }

            if stored_id == 0 {
                // 空槽，未找到
                return None;
            }
        }

        None
    }

    /// 无锁插入 (使用 CAS 重试)
    pub fn insert(
        &self,
        page_id: PageId,
        page: Page,
    ) -> Result<()> {
        let hash = self.hasher.hash_one(page_id) as usize;
        let start_idx = hash % self.slots.len();

        for i in 0..self.slots.len() {
            let idx = (start_idx + i) % self.slots.len();
            let entry = &self.slots[idx];

            let stored_id = unsafe {
                (*entry.as_ptr()).page_id.load(Ordering::Acquire)
            };

            if stored_id == page_id as u64 {
                // 已存在，更新
                unsafe {
                    *(*entry.as_ptr()).page.get() = Some(page);
                }
                return Ok(());
            }

            if stored_id == 0 {
                // 尝试占用空槽
                match unsafe {
                    (*entry.as_ptr()).page_id.compare_exchange(
                        0,
                        page_id as u64,
                        Ordering::Release,
                        Ordering::Acquire,
                    )
                } {
                    Ok(_) => {
                        // 成功占用
                        unsafe {
                            *(*entry.as_ptr()).page.get() = Some(page);
                            (*entry.as_ptr()).access_count.store(1, Ordering::Relaxed);
                        }
                        return Ok(());
                    }
                    Err(_) => {
                        // 被其他线程占用，继续探测
                        continue;
                    }
                }
            }
        }

        // 缓存满，随机驱逐
        self.random_evict_and_insert(page_id, page)
    }

    fn random_evict_and_insert(
        &self,
        page_id: PageId,
        page: Page,
    ) -> Result<()> {
        use rand::Rng;

        let mut rng = rand::thread_rng();
        let victim = rng.gen_range(0..self.slots.len());
        let entry = &self.slots[victim];

        // 简单驱逐 (不处理并发，依赖后续 CAS)
        unsafe {
            (*entry.as_ptr()).page_id.store(page_id as u64, Ordering::Release);
            *(*entry.as_ptr()).page.get() = Some(page);
            (*entry.as_ptr()).access_count.store(1, Ordering::Relaxed);
        }

        Ok(())
    }
}
```

## 3. Rust 实现方式

### 3.1 第三方库

```toml
[dependencies]
# 核心：epoch-based reclamation
crossbeam-epoch = "0.9"

# 并发哈希表
dashmap = "5"

# 并发集合
crossbeam-skiplist = "0.1"  # 可选，用于有序结构

# 随机数
rand = "0.8"

# 内存分配优化 (可选)
crossbeam-queue = "0.3"
```

### 3.2 自己实现的部分

| 组件 | 实现方式 | 原因 |
|------|---------|------|
| MVCC 管理器 | 自己实现 + crossbeam-epoch | 核心逻辑 |
| Lock-Free B-Tree | 自己实现 | 数据库核心 |
| Lock-Free 缓存 | 自己实现 | 定制化策略 |
| 冲突检测 | 自己实现 | OCC 算法 |
| 版本清理 | 自己实现 | 垃圾回收策略 |

## 4. 验证方法

### 4.1 正确性测试

```rust
#[test]
fn test_mvcc_read_your_writes() {
    let tm = MvccTransactionManager::new();

    // T1 写入并读取
    let mut t1 = tm.begin();
    tm.write(&mut t1, 1, page_data(b"A")).unwrap();

    let data = tm.read(&mut t1, 1).unwrap();
    assert_eq!(data, page_data(b"A"));
}

#[test]
fn test_mvcc_isolation() {
    let tm = MvccTransactionManager::new();

    // T1 写入但未提交
    let mut t1 = tm.begin();
    tm.write(&mut t1, 1, page_data(b"A")).unwrap();

    // T2 看不到 T1 的写入
    let t2 = tm.begin();
    let data = tm.read(&t2, 1);
    assert!(data.is_err()); // 页面不存在或旧版本

    // T1 提交
    tm.commit(&mut t1).unwrap();

    // T3 可以看到
    let mut t3 = tm.begin();
    let data = tm.read(&mut t3, 1).unwrap();
    assert_eq!(data, page_data(b"A"));
}

#[test]
fn test_concurrent_contention() {
    let tm = Arc::new(MvccTransactionManager::new());
    let mut handles = vec![];

    // 100 线程并发写入不同页面
    for i in 0..100 {
        let tm = tm.clone();
        handles.push(thread::spawn(move || {
            let mut txn = tm.begin();
            tm.write(&mut txn, i as u32, page_data(format!("data{}", i).as_bytes())).unwrap();
            tm.commit(&mut txn).unwrap();
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    // 验证所有写入
    let mut txn = tm.begin();
    for i in 0..100 {
        let data = tm.read(&mut txn, i as u32).unwrap();
        assert_eq!(data, page_data(format!("data{}", i).as_bytes()));
    }
}
```

### 4.2 性能基准测试

```rust
fn bench_concurrent_reads(c: &mut Criterion) {
    let tm = Arc::new(MvccTransactionManager::new());

    // 准备数据
    {
        let mut txn = tm.begin();
        for i in 0..10000 {
            tm.write(&mut txn, i, page_data(b"data")).unwrap();
        }
        tm.commit(&mut txn).unwrap();
    }

    let mut group = c.benchmark_group("concurrent_read");

    for num_threads in [1, 2, 4, 8, 16, 32] {
        group.bench_function(format!("{}_threads", num_threads), |b| {
            b.iter(|| {
                let handles: Vec<_> = (0..num_threads)
                    .map(|_| {
                        let tm = tm.clone();
                        thread::spawn(move || {
                            let mut txn = tm.begin();
                            for i in 0..1000 {
                                let _ = tm.read(&mut txn, i % 10000);
                            }
                        })
                    })
                    .collect();

                for h in handles {
                    h.join().unwrap();
                }
            });
        });
    }

    group.finish();
}
```

### 4.3 验证指标

| 指标 | 当前基线 | V3 目标 | 验证方法 |
|------|---------|--------|---------|
| 并发读扩展性 | 线性 | 完美线性 | 1-32 线程测试 |
| 读延迟 (P99) | - | < 1μs | 微基准测试 |
| 冲突率 | - | < 5% | 统计 |
| 内存使用 | 1x | 2-3x | 监控 |
| 垃圾回收开销 | - | < 10% CPU |  profiling |

## 5. 实施计划

### Week 1
- [ ] 实现 MVCC 基础结构
- [ ] 实现版本存储
- [ ] 事务生命周期管理
- [ ] 单元测试

### Week 2
- [ ] 实现 Lock-Free B-Tree
- [ ] 实现 Lock-Free 页面缓存
- [ ] 垃圾回收 (epoch reclamation)
- [ ] 性能基准测试

### Week 3
- [ ] 冲突检测优化
- [ ] 死锁避免
- [ ] 压力测试
- [ ] 与 SQLite 并发对比

## 6. 注意事项

### 6.1 内存管理
- 版本链需要定期清理
- crossbeam-epoch 自动处理，但要调参数

### 6.2 ABA 问题
- 使用版本号 (Tagged pointers)
- epoch 机制避免过早释放

### 6.3 平台兼容性
- x86_64: 强内存序，简单
- ARM: 需要显式内存屏障
