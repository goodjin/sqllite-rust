# sqllite-rust

[![Rust](https://img.shields.io/badge/rust-1.70%2B-orange.svg)](https://www.rust-lang.org/)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

**一个使用 Rust 实现的嵌入式事务型数据库，目标完全替代 SQLite。**

> 🎯 **项目定位**: 保持 SQLite 的所有优点（简单、可靠、零配置、事务型），用 Rust 的内存安全和现代并发模型实现性能超越。

## ✨ 核心特性

### 当前已实现

- **SQL 解析**: 完整的 SQL 解析器，支持标准 SQL 语法
- **存储引擎**: B+ Tree 行式存储，4KB 页面管理
- **事务支持**: WAL (Write-Ahead Logging)，ACID 事务
- **索引**: B+ Tree 索引 + HNSW 向量索引
- **高级 SQL**: JOIN、聚合函数、GROUP BY、ORDER BY、LIMIT/OFFSET
- **预编译语句**: 语句缓存和参数化查询
- **批量插入**: 批量插入优化，提升写入性能
- **MVCC**: 多版本并发控制基础架构
- **外键约束**: 支持 ON DELETE CASCADE/SET NULL/RESTRICT
- **查询优化器**: 统计信息、代价模型、索引选择

### 与 SQLite 的对比

| 特性 | SQLite | sqllite-rust | 状态 |
|------|--------|--------------|------|
| 嵌入式 | ✅ | ✅ | 可用 |
| 单文件 | ✅ | ✅ | 可用 |
| 零配置 | ✅ | ✅ | 可用 |
| ACID 事务 | ✅ | ✅ | 可用 |
| SQL 兼容 | ✅ | ✅ | 95% |
| 内存安全 | ⚠️ C | ✅ Rust | **优势** |
| 并发读 | ⚠️ 有限 | 🚧 MVCC | **即将超越** |
| 预编译缓存 | ✅ 基础 | 🚧 优化中 | 追赶中 |

## 🚀 快速开始

### 环境要求

- Rust 1.70 或更高版本
- Cargo

### 安装

```bash
git clone https://github.com/goodjin/sqllite-rust.git
cd sqllite-rust
cargo build --release
```

### 运行

```bash
# 运行演示
cargo run

# 启动交互式 SQL Shell
cargo run -- shell
```

## 📊 性能目标

### 单线程性能（目标：SQLite 的 80%+）

| 场景 | SQLite | 当前 | 目标 |
|------|--------|------|------|
| 点查 | ~0.03ms | ~0.1ms | <0.05ms |
| 批量插入 | ~50K/s | ~10K/s | >40K/s |
| 范围查询(1K行) | ~1.5ms | ~5ms | <2ms |

### 并发性能（目标：100x 超越 SQLite）⭐

| 场景 | SQLite | 当前 | 目标 |
|------|--------|------|------|
| 100线程并发读 | ~10K/s | 串行 | **>500K/s** |
| 读写混合(90/10) | ~5K/s | 串行 | **>100K/s** |

**超越策略**: SQLite 使用读写锁，我们使用 MVCC 实现真正的无锁并发读。

## 🏗️ 架构

```
┌─────────────────────────────────────────────────────────────┐
│                     SQL 接口层                               │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐  │
│  │ SQL Parser  │→ │   Cache     │→ │  Query Optimizer    │  │
│  └─────────────┘  │  (预编译)   │    └─────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                     执行引擎层                               │
│              Virtual Machine (字节码执行)                     │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                     存储引擎层                               │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐  │
│  │  B+ Tree    │  │    Pager    │  │   MVCC Transaction  │  │
│  │  (行存)     │←→│  (缓存)     │←→│   Manager           │  │
│  └─────────────┘  └─────────────┘  └─────────────────────┘  │
│         ↑                                                   │
│  ┌─────────────┐                                             │
│  │Index Manager│                                             │
│  │ (B+Tree/    │                                             │
│  │  HNSW)      │                                             │
│  └─────────────┘                                             │
└─────────────────────────────────────────────────────────────┘
```

## 📁 项目结构

```
sqllite-rust/
├── src/
│   ├── main.rs            # CLI 入口
│   ├── lib.rs             # 库入口
│   ├── pager/             # 页面管理（LRU 缓存）
│   ├── storage/           # B+ Tree 存储引擎
│   ├── sql/               # SQL 解析器 + 预编译缓存
│   ├── vm/                # 虚拟机执行引擎
│   ├── transaction/       # 事务管理（MVCC）
│   ├── index/             # 索引（B+Tree、HNSW）
│   ├── executor/          # SQL 执行器
│   ├── optimizer/         # 查询优化器
│   └── concurrency/       # 并发控制
├── tests/                 # 测试套件
├── benches/               # 基准测试
├── examples/              # 示例代码
└── docs/                  # 设计文档
```

## 🛠️ 支持的 SQL

```sql
-- 基础 CRUD
SELECT * FROM users WHERE id = 1;
INSERT INTO users VALUES (1, 'Alice');
UPDATE users SET name = 'Bob' WHERE id = 1;
DELETE FROM users WHERE id = 1;

-- 表操作
CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER);
CREATE INDEX idx_name ON users (name);
DROP TABLE users;

-- 高级查询
SELECT * FROM users ORDER BY age DESC LIMIT 10 OFFSET 5;
SELECT category, COUNT(*), AVG(age) FROM users GROUP BY category;
SELECT * FROM users LEFT JOIN orders ON users.id = orders.user_id;

-- 事务
BEGIN TRANSACTION;
COMMIT;
ROLLBACK;

-- 向量搜索（HNSW 索引）
CREATE TABLE items (id INTEGER, embedding VECTOR(128));
CREATE INDEX idx_hnsw ON items USING HNSW (embedding);
```

## 🧪 测试

```bash
# 运行所有单元测试
cargo test

# 运行基准测试
cargo bench

# 与 SQLite 对比测试
./run_benchmark.sh

# 运行人工测试套件
python3 tests/manual/test_runner.py
```

## 📈 开发路线图

### Phase 1: 基础性能优化 (3周)
- 预编译语句持久化缓存
- B+Tree 节点缓存优化
- WAL 批量提交
- 覆盖索引

### Phase 2: 并发架构重构 (4周) ⭐ 核心
- MVCC 多版本并发控制
- 快照隔离无锁读
- 乐观锁并发写入

### Phase 3: 存储优化 (3周)
- B+Tree 前缀压缩
- 页面预读
- 自适应缓存

### Phase 4: 查询优化器 (3周)
- 统计信息收集
- 代价模型
- JOIN 重排序

### Phase 5: 功能完整性 (4周)
- 外键约束、触发器、视图
- 窗口函数、CTE

## 🤝 为什么是 Rust？

| 优势 | 说明 |
|------|------|
| **内存安全** | 杜绝 SQLite 中常见的内存漏洞 |
| **并发安全** | 编译期保证无数据竞争，适合 MVCC 实现 |
| **零成本抽象** | 高性能同时保持代码清晰 |
| **生态集成** | Rust 项目原生使用，无需 FFI |

## 📄 许可证

[MIT](LICENSE)

## 📚 参考

- [SQLite Architecture](https://www.sqlite.org/arch.html)
- [SQLite File Format](https://www.sqlite.org/fileformat2.html)
- [PostgreSQL MVCC](https://www.postgresql.org/docs/current/mvcc.html)

---

**我们的目标不是成为一个"不同的数据库"，而是成为"更好的 SQLite"。**
