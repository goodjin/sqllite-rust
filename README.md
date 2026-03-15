# SQLite Rust

[![Rust](https://img.shields.io/badge/rust-1.70%2B-orange.svg)](https://www.rust-lang.org/)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

一个使用 Rust 实现的 SQLite 克隆数据库，用于学习和教育目的。

## 特性

- **SQL 解析**: 支持基本的 SQL 语句解析（SELECT, INSERT, UPDATE, DELETE）
- **存储引擎**: 基于页面的存储管理（4KB 页面）
- **B+ Tree**: 基础索引结构
- **事务支持**: WAL (Write-Ahead Logging) 基础实现
- **VM 执行**: 字节码虚拟机执行引擎

## 快速开始

### 环境要求

- Rust 1.70 或更高版本
- Cargo

### 安装

```bash
git clone git@github.com:goodjin/sqllite-rust.git
cd sqllite-rust
cargo build --release
```

### 运行演示

```bash
cargo run
```

### 运行测试

```bash
# 运行所有单元测试
cargo test

# 运行人工测试套件
python3 tests/manual/test_runner.py
```

## 项目结构

```
sqllite-rust/
├── src/
│   ├── pager/          # 页面管理模块
│   ├── storage/        # 存储引擎
│   ├── sql/            # SQL 解析器
│   ├── vm/             # 虚拟机执行引擎
│   ├── transaction/    # 事务管理
│   ├── index/          # 索引管理
│   └── main.rs         # CLI 入口
├── tests/
│   └── manual/         # 人工测试套件
├── docs/               # 设计文档
└── Cargo.toml
```

## 支持的 SQL 语句

```sql
-- 查询
SELECT * FROM users;
SELECT id, name FROM users WHERE id = 1;

-- 插入
INSERT INTO users VALUES (1, 'Alice');

-- 更新
UPDATE users SET name = 'Bob' WHERE id = 1;

-- 删除
DELETE FROM users WHERE id = 1;

-- 表操作
CREATE TABLE users (id INTEGER, name TEXT);
DROP TABLE users;
CREATE INDEX idx_name ON users (name);

-- 事务
BEGIN TRANSACTION;
COMMIT;
ROLLBACK;
```

## 架构设计

### 核心模块

| 模块 | 说明 |
|------|------|
| Pager | 管理数据库页面，提供 LRU 缓存 |
| Storage | 记录序列化和 B+ Tree 存储 |
| SQL | SQL 词法分析和语法解析 |
| VM | 字节码生成和执行 |
| Transaction | WAL 事务管理 |
| Index | B+ Tree 索引 |

### 数据流

```
SQL 文本 → Tokenizer → Parser → AST → CodeGen → 字节码 → VM 执行 → Pager → 文件
```

## 开发计划

### 第一阶段 ✅ (已完成)
- [x] B+ Tree 存储引擎基础
- [x] Pager 页面管理
- [x] 基础 CRUD 操作
- [x] 单线程访问

### 第二阶段 ✅ (已完成)
- [x] SQL 解析器
- [x] 执行引擎 (VM)
- [x] 支持 WHERE 子句
- [x] CREATE/DROP TABLE

### 第三阶段 🚧 (进行中)
- [ ] 完整事务支持 (ACID)
- [ ] WAL 日志实现
- [ ] B+ Tree 索引完善
- [ ] 并发控制

## 测试

### 单元测试

```bash
cargo test
```

共 19 个测试用例，覆盖核心模块。

### 人工测试

```bash
# 运行测试脚本
python3 tests/manual/test_runner.py

# 查看测试 SQL
cat tests/manual/test_queries.sql
```

## 性能

当前版本为教育实现，未做性能优化。

## 贡献

欢迎提交 Issue 和 PR！

## 许可证

[MIT](LICENSE)

## 参考

- [SQLite Architecture](https://www.sqlite.org/arch.html)
- [Let's Build a Simple Database](https://cstack.github.io/db_tutorial/)
