# SQLite Rust 克隆 - 人工测试方案

## 测试概述

本测试方案用于验证 sqllite-rust 数据库的核心功能，包括：
- SQL 解析和执行
- 数据持久化
- 页面管理
- 事务支持（基础）

## 测试环境要求

### 1. 系统要求
- Rust 1.70+ (已安装)
- Python 3.8+ (用于测试脚本)
- Linux/macOS/Windows

### 2. 构建项目
```bash
cd /Users/cat/github/sqllite-rust
cargo build --release
```

### 3. 运行单元测试
```bash
cargo test
```

## 测试流程

### 阶段 1: 基础功能测试
1. 运行 `cargo run` 查看演示
2. 验证 SQL 解析功能
3. 验证 Pager 页面管理

### 阶段 2: 数据操作测试
1. 使用 Python 脚本进行集成测试
2. 验证 CRUD 操作
3. 验证数据持久化

### 阶段 3: 边界条件测试
1. 大数据量测试
2. 并发访问测试
3. 错误处理测试

## 测试文件说明

| 文件 | 用途 |
|-----|------|
| `init_data.sql` | 初始化测试数据 |
| `test_queries.sql` | 测试用 SQL 语句 |
| `test_runner.py` | Python 测试脚本 |
| `test_report.md` | 测试报告模板 |

## 快速开始

```bash
# 1. 运行 Rust 演示
cargo run

# 2. 运行 Python 测试脚本
cd tests/manual
python3 test_runner.py

# 3. 查看测试报告
cat test_report.md
```

## 预期结果

- ✅ 所有 SQL 语句正确解析
- ✅ 数据正确持久化到文件
- ✅ 页面分配和读取正常
- ✅ 测试脚本执行无错误
