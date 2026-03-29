# AGENTS.md - sqllite-rust

## 项目概述

**项目名称**: sqllite-rust  
**版本**: 0.1.0  
**Rust 版本要求**: 1.70+  
**许可证**: MIT

### 项目目标

**使命**: 构建一个能**完全替代 SQLite** 的生产级嵌入式事务型数据库。

**核心约束**:
- ✅ 事务型（OLTP）数据库，非分析型
- ✅ 行式存储 + B+Tree 索引
- ✅ 单文件、零配置、嵌入式
- ✅ SQL 方言兼容 SQLite
- ❌ 不做列式存储
- ❌ 不做大规模向量化
- ❌ 不做 GPU 加速

**超越策略**: 
- **并发性能**: MVCC 实现无锁读，100x 超越 SQLite
- **内存安全**: Rust 语言级保证
- **现代优化**: 更好的缓存设计、自适应查询优化

---

## Build and Test Commands

### 基本构建

```bash
# 开发构建
cargo build

# 发布构建（推荐）
cargo build --release
```

### 运行程序

```bash
# 运行演示
cargo run

# 启动交互式 SQL Shell
cargo run -- shell

# 查看帮助
cargo run -- --help
```

### 测试命令

```bash
# 运行所有单元测试
cargo test

# 运行特定模块测试
cargo test pager::tests
cargo test storage::record::tests

# 安静模式运行测试
cargo test --quiet

# 运行人工测试套件（Python）
python3 tests/manual/test_runner.py
```

### 基准测试

```bash
# 运行所有基准测试
cargo bench

# 与 SQLite 对比
./run_benchmark.sh

# 查看 HTML 报告
open target/criterion/report/index.html
```

---

## Technology Stack

### 核心依赖

| 依赖 | 用途 |
|------|------|
| `thiserror` | 结构化错误定义 |
| `anyhow` | 便捷的错误处理 |
| `hex` | 二进制数据编码 |
| `rustyline` | 交互式 Shell 行编辑 |
| `hashlink` | 高性能哈希表 |
| `regex` | 正则表达式支持 |
| `rand` | 随机数生成 |

### 开发依赖

| 依赖 | 用途 |
|------|------|
| `tempfile` | 临时文件管理（测试用） |
| `criterion` | 基准测试框架 |

---

## Code Organization

### 目录结构

```
sqllite-rust/
├── src/
│   ├── main.rs            # CLI 入口
│   ├── lib.rs             # 库入口
│   ├── pager/             # 页面管理
│   ├── storage/           # B+ Tree 存储引擎
│   ├── sql/               # SQL 解析器
│   ├── vm/                # 虚拟机执行引擎
│   ├── transaction/       # 事务管理 (MVCC)
│   ├── index/             # 索引管理
│   │   ├── btree.rs       # B+ Tree 索引
│   │   └── hnsw.rs        # HNSW 向量索引
│   ├── executor/          # SQL 执行器
│   ├── optimizer/         # 查询优化器
│   └── concurrency/       # 并发控制
├── tests/                 # 测试
├── benches/               # 基准测试
├── examples/              # 示例代码
└── docs/                  # 设计文档
```

---

## 优化路线图

### Phase 1: 基础性能优化 (3周)

**目标**: 单线程性能达到 SQLite 80%

| 任务 | 描述 | 预期提升 |
|------|------|---------|
| 预编译缓存 | 持久化语句缓存 | 3-5x |
| B+Tree 缓存 | 节点级缓存优化 | 2x |
| WAL 批量 | 组提交优化 | 2-3x |
| 覆盖索引 | 避免回表 | 2x |

### Phase 2: 并发架构重构 (4周) ⭐核心

**目标**: 并发读性能 100x 超越 SQLite

| 任务 | 描述 | 预期提升 |
|------|------|---------|
| MVCC | 多版本并发控制 | - |
| 快照读 | 无锁读路径 | 100x |
| 乐观锁 | 并发写入 | 5x |
| GC | 版本清理 | - |

### Phase 3: 存储优化 (3周)

| 任务 | 描述 | 预期提升 |
|------|------|---------|
| 前缀压缩 | B+Tree 键压缩 | 30% 空间 |
| 预读优化 | 顺序扫描 | 2x |
| 自适应缓存 | 智能淘汰 | 2x 命中率 |

### Phase 4: 查询优化器 (3周)

| 任务 | 描述 | 预期提升 |
|------|------|---------|
| 统计信息 | 表/列统计 | - |
| 代价模型 | 成本估算 | - |
| JOIN 优化 | 重排序 | 10x |

### Phase 5: 功能完整性 (4周)

- 外键约束
- 触发器
- 视图
- 窗口函数

---

## 性能目标

### 单线程性能（目标: SQLite 80%+）

| 场景 | SQLite | 目标 | 当前 |
|------|--------|------|------|
| 点查 | 0.03ms | <0.05ms | ~0.1ms |
| 批量插入 | 50K/s | >40K/s | ~10K/s |
| 范围查(1K) | 1.5ms | <2ms | ~5ms |

### 并发性能（目标: 100x 超越）

| 场景 | SQLite | 目标 | 当前 |
|------|--------|------|------|
| 100线程读 | 10K/s | >500K/s | 串行 |
| 10线程写 | 串行 | >5K/s | 串行 |

---

## Code Style Guidelines

### 错误处理模式

```rust
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ModuleError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    
    #[error("Invalid format")]
    InvalidFormat,
}

pub type Result<T> = std::result::Result<T, ModuleError>;
```

### 模块导出模式

```rust
// src/module/mod.rs
pub mod error;
pub mod submodule;

pub use error::{ModuleError, Result};
pub use submodule::SomeType;
```

### 测试模式

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn test_something() {
        let temp_file = NamedTempFile::new().unwrap();
        // ... 测试代码
    }
}
```

---

## Shell Commands

| 命令 | 说明 |
|------|------|
| `.quit` / `.exit` / `.q` | 退出程序 |
| `.tables` | 列出所有表 |
| `.schema [table]` | 显示表结构 |
| `.dbinfo` | 显示数据库信息 |
| `.open PATH` | 打开指定数据库 |
| `.help` | 显示帮助 |

### SQL 支持

```sql
-- 基础 CRUD
SELECT * FROM users WHERE id = 1;
INSERT INTO users VALUES (1, 'Alice');
UPDATE users SET name = 'Bob' WHERE id = 1;
DELETE FROM users WHERE id = 1;

-- 表操作
CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
CREATE INDEX idx_name ON users (name);
DROP TABLE users;

-- 事务
BEGIN TRANSACTION;
COMMIT;
ROLLBACK;

-- JOIN
SELECT * FROM users JOIN orders ON users.id = orders.user_id;

-- 聚合
SELECT category, COUNT(*), AVG(price) FROM products GROUP BY category;
```

---

## Common Tasks

### 添加新 SQL 语句支持

1. 在 `src/sql/ast.rs` 添加新的 Statement 变体
2. 在 `src/sql/parser.rs` 实现解析逻辑
3. 在 `src/executor/mod.rs` 实现执行逻辑
4. 添加单元测试

### 调试技巧

```bash
# 打印调试信息
cargo run -- 2>&1 | tee debug.log

# 检查生成的数据库文件
hexdump -C sqllite.db | head -20
```

---

## References

- [SQLite Architecture](https://www.sqlite.org/arch.html)
- [SQLite File Format](https://www.sqlite.org/fileformat2.html)
- [PostgreSQL MVCC](https://www.postgresql.org/docs/current/mvcc.html)
- Rust 错误处理: [thiserror](https://docs.rs/thiserror)
