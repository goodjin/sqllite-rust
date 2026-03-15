# 分层架构设计

## 文档信息
- **项目名称**: sqllite-rust
- **版本**: v1.0
- **对应PRD**: docs/v1/01-prd.md
- **更新日期**: 2026-03-14

---

## 1. 层次划分

| 层次 | 名称 | 职责 | 对应PRD功能 | 核心模块 |
|-----|------|------|------------|---------|
| L1 | 接口层 | SQL 解析，对外 API | FR-005, FR-011~013 | MOD-03 |
| L2 | 执行层 | 查询优化，字节码执行 | FR-006, FR-007~010, FR-018 | MOD-04 |
| L3 | 事务层 | 事务管理，并发控制 | FR-014~015, FR-017 | MOD-05 |
| L4 | 存储层 | B+ Tree，索引管理 | FR-001, FR-003~004, FR-016 | MOD-01, MOD-06 |
| L5 | 页管理层 | 页面缓存，I/O 管理 | FR-002 | MOD-02 |
| L6 | OS 层 | 文件系统抽象 | - | - |

---

## 2. 层间依赖

```
┌─────────────────────────────────────────────────────────────┐
│ L1: 接口层 (Interface Layer)                                 │
│    SQL Parser, Public API                                    │
└──────────────────────────┬──────────────────────────────────┘
                           │ 依赖: AST, 执行计划
                           ▼
┌─────────────────────────────────────────────────────────────┐
│ L2: 执行层 (Execution Layer)                                 │
│    Virtual Machine, Query Optimizer                          │
└──────────────────────────┬──────────────────────────────────┘
                           │ 依赖: 存储操作接口
                           ▼
┌─────────────────────────────────────────────────────────────┐
│ L3: 事务层 (Transaction Layer)                               │
│    Transaction Manager, WAL, Lock Manager                    │
└──────────────────────────┬──────────────────────────────────┘
                           │ 依赖: 页面读写接口
                           ▼
┌─────────────────────────────────────────────────────────────┐
│ L4: 存储层 (Storage Layer)                                   │
│    B+ Tree, Index Manager                                    │
└──────────────────────────┬──────────────────────────────────┘
                           │ 依赖: 页面管理接口
                           ▼
┌─────────────────────────────────────────────────────────────┐
│ L5: 页管理层 (Page Management Layer)                         │
│    Pager, Page Cache                                         │
└──────────────────────────┬──────────────────────────────────┘
                           │ 依赖: 文件 I/O
                           ▼
┌─────────────────────────────────────────────────────────────┐
│ L6: OS 层 (OS Abstraction Layer)                             │
│    File I/O, Memory Mapping                                  │
└─────────────────────────────────────────────────────────────┘
```

**依赖原则**: 上层可以调用下层，下层不能调用上层。同层模块间通过接口交互。

---

## 3. 各层详细设计

### 3.1 L1: 接口层

**职责**:
- 解析 SQL 文本为 AST
- 提供对外公共 API
- 参数验证和预处理

**核心组件**:
| 组件 | 职责 | 对应PRD |
|-----|------|---------|
| SqlParser | 将 SQL 字符串解析为 AST | FR-005 |
| Ast | 抽象语法树定义 | FR-005 |
| Database | 对外 API 入口 | US-001 |

**接口示例**:
```rust
// 公共 API
pub struct Database { ... }

impl Database {
    pub fn open(path: &str) -> Result<Self>;
    pub fn execute(&self, sql: &str) -> Result<ResultSet>;
    pub fn close(self) -> Result<()>;
}
```

### 3.2 L2: 执行层

**职责**:
- 查询优化
- 生成执行计划（字节码）
- 虚拟机执行字节码

**核心组件**:
| 组件 | 职责 | 对应PRD |
|-----|------|---------|
| QueryOptimizer | 优化查询计划 | FR-018 |
| CodeGenerator | AST 转字节码 | FR-006 |
| VirtualMachine | 执行字节码 | FR-006 |
| Bytecode | 字节码指令集 | FR-006 |

**字节码指令集**:
```rust
enum OpCode {
    // 数据操作
    OpenRead,      // 打开表用于读取
    OpenWrite,     // 打开表用于写入
    Close,         // 关闭游标

    // 记录操作
    Insert,        // 插入记录
    Delete,        // 删除记录
    Update,        // 更新记录

    // 查询操作
    Rewind,        // 移动到第一条
    Next,          // 移动到下一条
    Column,        // 读取列值
    ResultRow,     // 返回结果行

    // 条件操作
    Compare,       // 比较操作
    Jump,          // 无条件跳转
    JumpIfTrue,    // 条件跳转
    JumpIfFalse,   // 条件跳转

    // 事务操作
    Begin,         // 开始事务
    Commit,        // 提交事务
    Rollback,      // 回滚事务

    // 其他
    Halt,          // 停止执行
}
```

### 3.3 L3: 事务层

**职责**:
- 管理事务生命周期
- WAL 日志管理
- 并发控制（锁管理）

**核心组件**:
| 组件 | 职责 | 对应PRD |
|-----|------|---------|
| TransactionManager | 事务状态管理 | FR-014 |
| WalManager | WAL 日志读写 | FR-015 |
| LockManager | 读写锁管理 | FR-017 |

**事务状态**:
```rust
enum TransactionState {
    AutoCommit,    // 自动提交模式
    Active,        // 事务活跃
    Committing,    // 提交中
    Aborting,      // 回滚中
}
```

### 3.4 L4: 存储层

**职责**:
- B+ Tree 实现
- 索引管理
- 记录序列化/反序列化

**核心组件**:
| 组件 | 职责 | 对应PRD |
|-----|------|---------|
| BTree | B+ Tree 实现 | FR-001, FR-003 |
| IndexManager | 索引管理 | FR-016 |
| Record | 记录格式定义 | FR-004 |
| Serializer | 数据序列化 | FR-004 |

**B+ Tree 节点**:
```rust
struct BTreeNode {
    page_id: PageId,
    node_type: NodeType,     // Internal 或 Leaf
    keys: Vec<Key>,
    values: Vec<Value>,      // 叶子节点存储记录
    children: Vec<PageId>,   // 内部节点存储子节点
    next_leaf: Option<PageId>, // 叶子节点链表
}
```

### 3.5 L5: 页管理层

**职责**:
- 页面缓存管理
- 页面读写
- 页面分配和回收

**核心组件**:
| 组件 | 职责 | 对应PRD |
|-----|------|---------|
| Pager | 页面管理入口 | FR-002 |
| PageCache | LRU 页面缓存 | FR-002 |
| Page | 页面数据结构 | FR-002 |

**页面结构**:
```rust
const PAGE_SIZE: usize = 4096;

struct Page {
    id: PageId,
    data: [u8; PAGE_SIZE],
    is_dirty: bool,
    pin_count: u32,
}
```

### 3.6 L6: OS 层

**职责**:
- 文件系统抽象
- 内存映射（可选）
- 系统调用封装

**核心组件**:
| 组件 | 职责 |
|-----|------|
| FileHandle | 文件句柄抽象 |
| FileSystem | 文件系统操作 |

---

## 4. 功能-层次映射

| PRD功能 | 功能名称 | 所属层次 | 模块编号 | 实现组件 |
|---------|---------|---------|---------|---------|
| FR-001 | B+ Tree 存储引擎 | L4-存储层 | MOD-01 | BTree |
| FR-002 | 页面管理器 | L5-页管理层 | MOD-02 | Pager, PageCache |
| FR-003 | 基础 CRUD | L4-存储层 | MOD-01 | BTree::insert/delete/update/search |
| FR-004 | 定长记录存储 | L4-存储层 | MOD-01 | Record, Serializer |
| FR-005 | SQL 解析器 | L1-接口层 | MOD-03 | SqlParser, Ast |
| FR-006 | 虚拟机执行引擎 | L2-执行层 | MOD-04 | VirtualMachine, Bytecode |
| FR-007 | SELECT 查询 | L2-执行层 | MOD-04 | VM + BTree 游标 |
| FR-008 | INSERT 插入 | L2-执行层 | MOD-04 | VM::execute(Insert) |
| FR-009 | UPDATE 更新 | L2-执行层 | MOD-04 | VM::execute(Update) |
| FR-010 | DELETE 删除 | L2-执行层 | MOD-04 | VM::execute(Delete) |
| FR-011 | WHERE 子句 | L1+L2 | MOD-03,MOD-04 | Parser + VM(Jump*) |
| FR-012 | CREATE TABLE | L2+L4 | MOD-04,MOD-01 | VM + Schema 存储 |
| FR-013 | DROP TABLE | L2+L4 | MOD-04,MOD-01 | VM + BTree 删除 |
| FR-014 | 事务支持 | L3-事务层 | MOD-05 | TransactionManager |
| FR-015 | WAL 预写日志 | L3-事务层 | MOD-05 | WalManager |
| FR-016 | B+ Tree 索引 | L4-存储层 | MOD-06 | IndexManager |
| FR-017 | 并发控制 | L3-事务层 | MOD-05 | LockManager |
| FR-018 | 查询优化器 | L2-执行层 | MOD-04 | QueryOptimizer |

---

## 5. 层间接口

### 5.1 L1 → L2 接口

```rust
// 解析后的 AST 传递给执行层
trait ExecutionLayer {
    fn execute_ast(&self, ast: &Ast) -> Result<ResultSet>;
}
```

### 5.2 L2 → L3 接口

```rust
// 执行层通过事务层访问存储
trait TransactionLayer {
    fn begin_transaction(&self) -> Result<Transaction>;
    fn commit(&self, tx: Transaction) -> Result<()>;
    fn rollback(&self, tx: Transaction) -> Result<()>;

    // 在事务上下文中访问存储
    fn with_transaction<F, R>(&self, f: F) -> Result<R>
    where F: FnOnce(&StorageLayer) -> Result<R>;
}
```

### 5.3 L3 → L4 接口

```rust
// 事务层调用存储层
trait StorageLayer {
    fn btree_insert(&self, table: &str, record: &Record) -> Result<()>;
    fn btree_delete(&self, table: &str, key: &Key) -> Result<()>;
    fn btree_search(&self, table: &str, key: &Key) -> Result<Option<Record>>;
    fn btree_scan(&self, table: &str) -> Result<Cursor>;

    // 索引操作
    fn index_insert(&self, index: &str, key: &Key, row_id: RowId) -> Result<()>;
    fn index_search(&self, index: &str, key: &Key) -> Result<Vec<RowId>>;
}
```

### 5.4 L4 → L5 接口

```rust
// 存储层通过页管理层读写页面
trait PageManagementLayer {
    fn get_page(&self, page_id: PageId) -> Result<Page>;
    fn allocate_page(&self) -> Result<PageId>;
    fn free_page(&self, page_id: PageId) -> Result<()>;
    fn write_page(&self, page: &Page) -> Result<()>;
    fn flush(&self) -> Result<()>;
}
```

### 5.5 L5 → L6 接口

```rust
// 页管理层通过 OS 层读写文件
trait OsLayer {
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<usize>;
    fn write_at(&self, offset: u64, buf: &[u8]) -> Result<()>;
    fn sync(&self) -> Result<()>;
    fn file_size(&self) -> Result<u64>;
}
```

---

## 6. 数据流向

### 6.1 查询数据流 (SELECT)

```
SQL String
    ↓
[L1] Parser → AST
    ↓
[L2] Optimizer → Execution Plan → VM executes
    ↓
[L3] Transaction Manager (if in transaction)
    ↓
[L4] BTree/Index search
    ↓
[L5] Pager (check cache → load from disk if miss)
    ↓
[L6] File I/O
    ↓
ResultSet (return up through layers)
```

### 6.2 写入数据流 (INSERT/UPDATE/DELETE)

```
SQL String
    ↓
[L1] Parser → AST
    ↓
[L2] VM generates bytecode
    ↓
[L3] Transaction Manager
     ├─→ Write WAL record (durability)
     └─→ Acquire locks (isolation)
    ↓
[L4] BTree/Index modify
    ↓
[L5] Pager marks pages dirty
    ↓
[L3] On COMMIT: flush WAL, sync DB file
    ↓
[L6] File I/O
```

---

## 7. 覆盖映射

### 本文档对应的功能需求

| PRD编号 | 功能 | 覆盖状态 |
|---------|-----|---------|
| FR-001~018 | 所有功能需求 | ✅ |

### 本文档对应的用户故事

| PRD编号 | 用户故事 | 覆盖状态 |
|---------|---------|---------|
| US-001~008 | 所有用户故事 | ✅ |

---

## 变更历史

| 版本 | 日期 | 变更内容 | 作者 |
|-----|------|---------|------|
| 1.0 | 2026-03-14 | 初始版本 | Claude |
