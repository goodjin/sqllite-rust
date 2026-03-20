# SQLite Rust 克隆项目演示与 CLI 增强总结

本项目旨在用 Rust 实现一个超越 SQLite 的数据库引擎。本次工作主要完成了功能演示程序的创建、CLI 的增强以及核心 SQL 解析器和执行器的多项改进。

## 1. 主要更改内容

### 演示程序与示例
- **[NEW] [basic_usage.rs](file:///Users/jin/github/sqllite-rust/examples/basic_usage.rs)**: 这是一个全面的使用示例，涵盖了：
  - 数据库的打开与创建
  - 数据表的创建 (`CREATE TABLE`)
  - 数据插入 (`INSERT`)
  - 带条件和字段过滤的查询 (`SELECT ... WHERE`)
  - 数据更新与删除 (`UPDATE`, `DELETE`)
  - 事务处理 (`BEGIN`, `ROLLBACK`)
  - 表的删除 (`DROP TABLE`)

### CLI 增强 ([main.rs](file:///Users/jin/github/sqllite-rust/src/main.rs))
- **Shell 模式支持持久化**: 交互式 Shell 现在默认连接到 `sqllite.db` 文件，不再使用临时文件，确保数据可持久化。
- **实现的 Shell 命令**:
  - `.tables`: 列出所有数据表。
  - `.schema [TABLE]`: 显示指定表或所有表的建表语句。
  - `.dbinfo`: 显示数据库底层信息（页大小、数据库大小、编码等）。
  - `.open PATH`: 在 Shell 中切换数据库文件。
- **改进的 Demo 命令**: `cargo run -- demo` 现在执行一个包含事务回滚验证的完整业务场景演示。

### 核心引擎修复与改进
- **SQL 解析器增强**:
  - 支持 `CREATE TABLE` 中的 `PRIMARY KEY` 约束。
  - 支持 `UPDATE` 和 `SELECT` 表达式中的算术运算符 (`+`, `-`, `*`, `/`)。
- **执行器改进**:
  - 修复了 `StatementCache` 的自动规范化导致不同 SQL 共享相同执行方案的 Bug。
  - 改进了 `QueryResult::print()`，使其支持 `SELECT *` 的字段名显示以及多字段查询的投影（只显示请求的字段）。
- **健壮性修复**:
  - 修复了在读取对齐要求严格的结构体成员（如 PagerHeader）时导致的潜在未定义行为和编译错误。

## 2. 验证结果

### 基础用法示例运行 (`cargo run --example basic_usage`)

示例程序完整演示了数据库的生命周期，输出如下（截取关键部分）：

```text
4. Querying all users:
id | name | age | email
--------------------------------------------------
1 | Alice | 30 | alice@example.com
2 | Bob | 25 | bob@example.com
3 | Charlie | 35 | charlie@example.com
4 | David | 28 | david@example.com
(4 row(s))

5. Querying users where age > 29:
name | age
--------------------------------------------------
Alice | 30
Charlie | 35
(2 row(s))

8. Transaction Demo:
Inserted Eve inside transaction. Current count: 4
Rolling back transaction...
Count after rollback: 3
```

### CLI 演示运行 (`cargo run -- demo`)

演示了 `UPDATE` 算术运算和事务回滚：

```text
=== Transactional updates ===
> BEGIN TRANSACTION
✓ Transaction started
> UPDATE products SET price = price + 50 WHERE category = 'Electronics'
✓ 3 row(s) updated
> SELECT * FROM products WHERE category = 'Electronics'
id | name | price | category
--------------------------------------------------
1 | Laptop | 1250 | Electronics
2 | Smartphone | 850 | Electronics
4 | Headphones | 200 | Electronics (原为 150)
(3 row(s))
> ROLLBACK
✓ Transaction rolled back
```

### 交互式 Shell 验证

用户可以运行 `cargo run -- shell` 进入交互模式，使用 `.tables` 和 `.schema` 查看元数据。

## 3. 文档同步说明

按照要求，本 Walkthrough 已同步至：
- `docs/walkthrough/Walkthrough-SQLite-Demo-Enhancement-2026-03-20.md`

> [!TIP]
> 核心修复禁用了 `StatementCache` 的 `auto_normalize` 选项，这能保证执行的正确性。后续如果需要提升 OLTP 性能，建议重构参数化绑定逻辑。
