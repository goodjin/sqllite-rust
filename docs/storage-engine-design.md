# 生产级B-tree存储引擎设计

## 1. 架构概述

### 1.1 核心目标
- 支持单表百万级记录
- 单条记录最大4KB（溢出页支持）
- 支持范围查询和点查
- 事务ACID保证

### 1.2 架构图
```
┌─────────────────────────────────────────────────────────────┐
│                      SQL执行层 (Executor)                     │
└─────────────────────────┬───────────────────────────────────┘
                          │
┌─────────────────────────▼───────────────────────────────────┐
│                    B-tree存储引擎                            │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐  │
│  │ B-tree索引  │  │ 页管理器    │  │ 事务管理器(WAL)     │  │
│  │ - 搜索     │  │ - 分配/释放 │  │ - 原子性           │  │
│  │ - 插入     │  │ - 缓存      │  │ - 持久性           │  │
│  │ - 删除     │  │ - 刷盘      │  │ - 并发控制         │  │
│  │ - 分裂     │  │             │  │                    │  │
│  └─────────────┘  └─────────────┘  └─────────────────────┘  │
└─────────────────────────┬───────────────────────────────────┘
                          │
┌─────────────────────────▼───────────────────────────────────┐
│                      页存储层                                │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐   │
│  │ 数据页   │  │ 溢出页   │  │ 空闲页   │  │ WAL页    │   │
│  │ 4096字节 │  │ 大对象   │  │ 管理     │  │ 日志     │   │
│  └──────────┘  └──────────┘  └──────────┘  └──────────┘   │
└─────────────────────────────────────────────────────────────┘
```

## 2. 核心数据结构

### 2.1 页头结构 (96字节)
```rust
#[repr(C, packed)]
pub struct PageHeader {
    // 校验和 (4字节)
    pub checksum: u32,

    // 页类型 (1字节)
    pub page_type: PageType,  // 0=Data, 1=Index, 2=Overflow, 3=Free

    // 标志位 (1字节)
    pub flags: u8,  // bit0=叶子节点, bit1=根节点, bit2=已删除

    // 记录数量 (2字节)
    pub record_count: u16,

    // 空闲空间偏移 (2字节) - 从页尾开始增长的空闲空间
    pub free_offset: u16,

    // 空闲空间大小 (2字节)
    pub free_size: u16,

    // 右兄弟页ID (4字节) - B+树叶子链表
    pub right_sibling: PageId,

    // 左兄弟页ID (4字节)
    pub left_sibling: PageId,

    // 父页ID (4字节)
    pub parent_page: PageId,

    // 页级LSN (8字节) - 用于WAL恢复
    pub lsn: u64,

    // 预留 (64字节)
    pub _reserved: [u8; 64],
}

// 总大小: 4+1+1+2+2+2+4+4+4+8+64 = 96字节
```

### 2.2 页类型定义
```rust
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum PageType {
    Data = 0,      // 数据页（B-tree叶子）
    Index = 1,     // 索引页（B-tree内部节点）
    Overflow = 2,  // 溢出页（大对象）
    Free = 3,      // 空闲页
}
```

### 2.3 记录头结构 (16字节)
```rust
#[repr(C, packed)]
pub struct RecordHeader {
    // 记录总大小 (4字节) - 包含头和数据
    pub total_size: u32,

    // 键大小 (2字节)
    pub key_size: u16,

    // 值大小 (2字节)
    pub value_size: u16,

    // 标志位 (2字节)
    pub flags: u16,  // bit0=已删除, bit1=有溢出页

    // 溢出页ID (4字节) - 如果记录跨页
    pub overflow_page: PageId,
}

// 总大小: 4+2+2+2+4 = 14字节，对齐到16字节
```

### 2.4 B-tree节点条目
```rust
// 内部节点条目（指向子页）
pub struct IndexEntry {
    pub key: Vec<u8>,        // 分隔键
    pub child_page: PageId,  // 子页ID
}

// 叶子节点条目（实际记录）
pub struct LeafEntry {
    pub key: Vec<u8>,        // 主键
    pub value: Vec<u8>,      // 记录数据
}
```

### 2.5 空闲页管理
```rust
pub struct FreePageList {
    pub head_page: PageId,    // 空闲链表头
    pub tail_page: PageId,    // 空闲链表尾
    pub count: u32,           // 空闲页数量
}
```

## 3. 页面布局

### 3.1 数据页布局 (4096字节)
```
┌─────────────────────────────────────────────────────────────┐
│ 页头 (96字节)                                                │
├─────────────────────────────────────────────────────────────┤
│ 记录偏移数组 (slot array)                                     │
│ ┌────────┬────────┬────────┬────────┐                      │
│ │ slot 0 │ slot 1 │ slot 2 │  ...   │  每个slot 2字节       │
│ │ offset │ offset │ offset │        │  指向记录位置          │
│ └────────┴────────┴────────┴────────┘                      │
├─────────────────────────────────────────────────────────────┤
│                    空闲空间                                  │
│                                                              │
├─────────────────────────────────────────────────────────────┤
│ 记录数据（从页尾向前增长）                                     │
│ ┌──────────┬──────────┬──────────┐                          │
│ │ Record N │ Record 2 │ Record 1 │  记录头+数据              │
│ │ (尾部)   │          │ (头部)   │                          │
│ └──────────┴──────────┴──────────┘                          │
└─────────────────────────────────────────────────────────────┘

页面利用率计算：
- 可用空间 = 4096 - 96(页头) = 4000字节
- 最小记录大小 = 16(头) + 8(最小键值) = 24字节
- 理论最大记录数 ≈ 4000 / 24 ≈ 166条
- 实际考虑碎片和slot array，约100-120条
```

### 3.2 索引页布局
```
┌─────────────────────────────────────────────────────────────┐
│ 页头 (96字节)                                                │
├─────────────────────────────────────────────────────────────┤
│ 键偏移数组 (4字节 × n)                                       │
│ ┌────────┬────────┬────────┐                               │
│ │ key 0  │ key 1  │  ...   │  键的位置偏移                 │
│ │ offset │ offset │        │                               │
│ └────────┴────────┴────────┘                               │
├─────────────────────────────────────────────────────────────┤
│ 子页ID数组 (4字节 × (n+1))                                   │
│ ┌────────┬────────┬────────┐                               │
│ │page 0  │page 1  │  ...   │  子页ID                       │
│ └────────┴────────┴────────┘                               │
├─────────────────────────────────────────────────────────────┤
│ 键数据（从页尾向前）                                          │
│ ┌──────────┬──────────┐                                    │
│ │ Key N    │ Key 1    │                                    │
│ └──────────┴──────────┘                                    │
└─────────────────────────────────────────────────────────────┘
```

### 3.3 溢出页布局
```
┌─────────────────────────────────────────────────────────────┐
│ 页头 (96字节)                                                │
├─────────────────────────────────────────────────────────────┤
│ 数据大小 (4字节)                                             │
├─────────────────────────────────────────────────────────────┤
│ 下一溢出页ID (4字节)                                         │
├─────────────────────────────────────────────────────────────┤
│ 数据 (最多 3992字节)                                         │
│ ┌────────────────────────────────────────────────────────┐ │
│ │ 大对象数据片段                                          │ │
│ └────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘

最大单条记录大小 = 3992 × 溢出页链长度
理论上限 = 3992 × 2^32 ≈ 16TB
实际限制 = 4KB (配置参数)
```

## 4. B-tree算法

### 4.1 搜索算法
```rust
fn search(root_page: PageId, key: &[u8]) -> Option<Record> {
    let mut current_page = root_page;

    loop {
        let page = pager.get_page(current_page)?;

        if page.is_leaf() {
            // 叶子节点：二分查找
            return page.binary_search(key);
        } else {
            // 内部节点：找到合适的子页
            let child = page.find_child_page(key);
            current_page = child;
        }
    }
}

// 二分查找实现
fn binary_search(&self, key: &[u8]) -> Option<Record> {
    let mut left = 0;
    let mut right = self.record_count as usize;

    while left < right {
        let mid = (left + right) / 2;
        let record_key = self.get_key_at(mid);

        match compare_keys(&record_key, key) {
            Ordering::Equal => return self.get_record_at(mid),
            Ordering::Less => left = mid + 1,
            Ordering::Greater => right = mid,
        }
    }
    None
}
```

### 4.2 插入算法
```rust
fn insert(root_page: PageId, key: &[u8], value: &[u8]) -> Result<()> {
    let record_size = estimate_record_size(key, value);

    // 检查是否需要溢出页
    let (main_data, overflow_data) = if record_size > MAX_INLINE_SIZE {
        split_overflow_record(key, value)
    } else {
        (create_record(key, value), None)
    };

    // 从根开始查找插入位置
    let path = find_insert_path(root_page, key)?;

    // 尝试插入到叶子节点
    let leaf_page = path.last().unwrap();

    if leaf_page.has_space(&main_data) {
        // 直接插入
        leaf_page.insert_record(&main_data, overflow_data)?;
    } else {
        // 页满，需要分裂
        let (new_page, median_key) = split_leaf_page(leaf_page)?;

        // 插入到合适的页
        if compare_keys(key, &median_key) == Ordering::Less {
            leaf_page.insert_record(&main_data, overflow_data)?;
        } else {
            new_page.insert_record(&main_data, overflow_data)?;
        }

        // 向上传播分裂
        propagate_split(path, median_key, new_page.id)?;
    }

    Ok(())
}

// 叶子页分裂
fn split_leaf_page(page: &mut Page) -> Result<(Page, Vec<u8>)> {
    let new_page = pager.allocate_page()?;
    let records = page.get_all_records();

    // 找到中间记录
    let mid = records.len() / 2;
    let median_key = records[mid].key.clone();

    // 前半部分留在原页
    page.clear_records();
    for i in 0..mid {
        page.insert_record_raw(&records[i])?;
    }

    // 后半部分移到新页
    for i in mid..records.len() {
        new_page.insert_record_raw(&records[i])?;
    }

    // 更新B+树链表
    new_page.right_sibling = page.right_sibling;
    new_page.left_sibling = page.id;
    page.right_sibling = new_page.id;

    Ok((new_page, median_key))
}

// 向上传播分裂
fn propagate_split(path: &[Page], key: Vec<u8>, new_page_id: PageId) -> Result<()> {
    for i in (0..path.len()-1).rev() {
        let parent = &path[i];

        if parent.has_space_for_index_entry() {
            // 父页有空间，直接插入
            parent.insert_index_entry(&key, new_page_id)?;
            return Ok(());
        } else {
            // 父页也满了，继续分裂
            let (new_parent, new_median) = split_index_page(parent)?;

            if compare_keys(&key, &new_median) == Ordering::Less {
                parent.insert_index_entry(&key, new_page_id)?;
            } else {
                new_parent.insert_index_entry(&key, new_page_id)?;
            }

            // 继续向上（如果有祖父）
            if i == 0 {
                // 根页分裂，需要创建新根
                create_new_root(parent.id, new_median, new_parent.id)?;
                return Ok(());
            }
        }
    }

    Ok(())
}
```

### 4.3 删除算法
```rust
fn delete(root_page: PageId, key: &[u8]) -> Result<bool> {
    let path = find_path(root_page, key)?;
    let leaf = path.last().unwrap();

    // 标记删除（逻辑删除）
    let found = leaf.mark_deleted(key)?;

    if found {
        // 检查是否需要合并
        if leaf.should_merge() {
            merge_or_redistribute(&path)?;
        }

        // 记录到WAL
        wal.log_delete(leaf.id, key)?;
    }

    Ok(found)
}

// 合并或重新分配
fn merge_or_redistribute(path: &[Page]) -> Result<()> {
    let page = path.last().unwrap();

    // 尝试从左兄弟借记录
    if let Some(left) = get_left_sibling(page) {
        if left.can_lend_record() {
            let record = left.remove_last_record()?;
            page.insert_at_front(record)?;
            update_parent_key(path, &record.key)?;
            return Ok(());
        }
    }

    // 尝试从右兄弟借记录
    if let Some(right) = get_right_sibling(page) {
        if right.can_lend_record() {
            let record = right.remove_first_record()?;
            page.insert_at_end(record)?;
            update_parent_key(path, &record.key)?;
            return Ok(());
        }
    }

    // 无法借，尝试合并
    if let Some(left) = get_left_sibling(page) {
        if left.can_merge_with(page) {
            merge_pages(left, page)?;
            free_page(page.id)?;
            remove_parent_entry(path, page.id)?;
            return Ok(());
        }
    }

    Ok(())
}
```

### 4.4 范围查询
```rust
fn range_scan(
    root_page: PageId,
    start_key: Option<&[u8]>,
    end_key: Option<&[u8]>,
) -> impl Iterator<Item = Record> {
    // 找到起始叶子页
    let start_page = if let Some(key) = start_key {
        find_leaf_page(root_page, key)
    } else {
        find_leftmost_leaf(root_page)
    };

    RangeScanIterator {
        current_page: start_page,
        current_slot: 0,
        start_key,
        end_key,
    }
}

struct RangeScanIterator {
    current_page: PageId,
    current_slot: usize,
    start_key: Option<Vec<u8>>,
    end_key: Option<Vec<u8>>,
}

impl Iterator for RangeScanIterator {
    type Item = Record;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let page = pager.get_page(self.current_page).ok()?;

            if self.current_slot < page.record_count as usize {
                let record = page.get_record_at(self.current_slot)?;
                self.current_slot += 1;

                // 检查范围
                if let Some(ref start) = self.start_key {
                    if compare_keys(&record.key, start) == Ordering::Less {
                        continue;
                    }
                }

                if let Some(ref end) = self.end_key {
                    if compare_keys(&record.key, end) == Ordering::Greater {
                        return None;
                    }
                }

                return Some(record);
            } else {
                // 移动到下一页
                self.current_page = page.right_sibling?;
                self.current_slot = 0;

                if self.current_page == 0 {
                    return None;
                }
            }
        }
    }
}
```

## 5. 与现有代码集成方案

### 5.1 替换现有存储层
```
当前架构：
SQL → Parser → Executor → Database → Table → Page (单页堆表)

新架构：
SQL → Parser → Executor → BtreeStorage → Btree → Page (B-tree)
                              ↓
                         PageManager (页管理)
                              ↓
                         WAL (事务日志)
```

### 5.2 向后兼容
```rust
// 保留现有接口，内部改用B-tree
impl Storage for BtreeStorage {
    fn create_table(&mut self, name: &str, schema: Schema) -> Result<()> {
        // 创建B-tree根页
        let root_page = self.btree.create_tree(name)?;
        self.catalog.insert(name.to_string(), root_page);
        Ok(())
    }

    fn insert(&mut self, table: &str, record: Record) -> Result<RowId> {
        let root = self.catalog.get(table)?;
        let key = record.primary_key();
        let value = record.serialize();
        self.btree.insert(root, &key, &value)
    }

    fn get(&self, table: &str, rowid: RowId) -> Result<Option<Record>> {
        let root = self.catalog.get(table)?;
        let key = rowid.to_bytes();
        self.btree.search(root, &key)
            .map(|data| Record::deserialize(&data))
    }

    fn scan(&self, table: &str) -> Result<Box<dyn Iterator<Item = Record>>> {
        let root = self.catalog.get(table)?;
        Ok(self.btree.range_scan(root, None, None))
    }
}
```

### 5.3 迁移策略
```rust
// 检测旧格式并自动迁移
fn open_or_migrate(path: &str) -> Result<BtreeStorage> {
    if is_legacy_format(path) {
        info!("检测到旧格式数据库，开始迁移...");
        let legacy = LegacyStorage::open(path)?;
        let new_storage = BtreeStorage::create(path)?;

        // 迁移数据
        for table_name in legacy.list_tables() {
            let schema = legacy.get_schema(&table_name)?;
            new_storage.create_table(&table_name, schema)?;

            for record in legacy.scan(&table_name)? {
                new_storage.insert(&table_name, record)?;
            }
        }

        info!("迁移完成");
        Ok(new_storage)
    } else {
        BtreeStorage::open(path)
    }
}
```

## 6. 性能预期

| 操作 | 当前实现 | B-tree实现 | 提升 |
|------|---------|-----------|------|
| 点查 | O(n) | O(log n) | 1000x @ 100万条 |
| 插入 | O(1) | O(log n) | 稳定 |
| 范围查 | O(n) | O(log n + k) | 100x |
| 最大数据量 | ~3.5KB | 无限制 | ∞ |
| 单表记录数 | ~14条 | 100万+ | 70000x |

## 7. 开发计划

### Phase 1: 基础页管理 (2天)
- [ ] 新页头结构
- [ ] 页分配/释放
- [ ] 空闲页链表

### Phase 2: B-tree核心 (3天)
- [ ] 搜索
- [ ] 插入（含分裂）
- [ ] 删除（含合并）

### Phase 3: 溢出页 (1天)
- [ ] 大记录支持
- [ ] 溢出页链

### Phase 4: 集成测试 (2天)
- [ ] 替换现有存储
- [ ] 兼容性测试
- [ ] 性能基准测试

**总计: 约8天**
