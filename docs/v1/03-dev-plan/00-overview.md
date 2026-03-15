# 开发计划总览 - sqllite-rust

## 文档信息
- **项目名称**: sqllite-rust
- **版本**: v1.0
- **对应架构**: docs/v1/02-architecture/
- **创建日期**: 2026-03-14

---

## 1. 项目概述

sqllite-rust 是一个使用 Rust 实现的嵌入式关系型数据库，功能对标 SQLite 核心引擎。项目分三个阶段实现：

- **阶段 1**: 存储引擎基础（B+ Tree、页面管理、基础 CRUD）
- **阶段 2**: SQL 层（SQL 解析、虚拟机执行引擎）
- **阶段 3**: 高级功能（事务、WAL、索引）

---

## 2. 模块开发计划

| 阶段 | 批次 | 模块 | 开发计划 | 任务数 | 预估工时 | 状态 |
|-----|-----|------|---------|-------|---------|------|
| 1 | 1 | MOD-02 Pager | 01-mod-02-pager.md | 6 | 2天 | 待开发 |
| 1 | 1 | MOD-01 Storage | 02-mod-01-storage.md | 8 | 3天 | 待开发 |
| 2 | 2 | MOD-03 Parser | 03-mod-03-parser.md | 5 | 2天 | 待开发 |
| 2 | 2 | MOD-04 VM | 04-mod-04-vm.md | 8 | 4天 | 待开发 |
| 3 | 3 | MOD-05 Transaction | 05-mod-05-transaction.md | 6 | 3天 | 待开发 |
| 3 | 3 | MOD-06 Index | 06-mod-06-index.md | 5 | 2天 | 待开发 |
| 3 | 3 | 集成测试 | 07-integration-test.md | 4 | 2天 | 待开发 |

**总计**: 42个任务，预估 18 天

---

## 3. 开发顺序

### 第1批：存储引擎基础（阶段 1）

**依赖关系**: Pager → Storage

1. **MOD-02 Pager** (01-mod-02-pager.md)
   - 页面管理、缓存、文件 I/O
   - 6个任务，2天

2. **MOD-01 Storage** (02-mod-01-storage.md)
   - B+ Tree、记录管理、表元数据
   - 8个任务，3天

### 第2批：SQL 层（阶段 2）

**依赖关系**: Parser → VM → (Storage, Pager)

1. **MOD-03 Parser** (03-mod-03-parser.md)
   - SQL 词法分析、语法分析、AST
   - 5个任务，2天

2. **MOD-04 VM** (04-mod-04-vm.md)
   - 字节码、虚拟机、代码生成
   - 8个任务，4天

### 第3批：高级功能（阶段 3）

**依赖关系**: Transaction → (Storage, Pager), Index → Storage

1. **MOD-05 Transaction** (05-mod-05-transaction.md)
   - 事务管理、WAL、锁管理
   - 6个任务，3天

2. **MOD-06 Index** (06-mod-06-index.md)
   - 索引管理、索引扫描
   - 5个任务，2天

3. **集成测试** (07-integration-test.md)
   - 端到端测试、性能测试
   - 4个任务，2天

---

## 4. 依赖关系图

```
阶段 1: 存储引擎基础
┌─────────────────────────────────────────┐
│  MOD-02 Pager                           │
│  (页面管理)                              │
└─────────────┬───────────────────────────┘
              │ 依赖
              ▼
┌─────────────────────────────────────────┐
│  MOD-01 Storage                         │
│  (B+ Tree, 记录管理)                      │
└─────────────────────────────────────────┘

阶段 2: SQL 层
┌─────────────────────────────────────────┐
│  MOD-03 Parser                          │
│  (SQL 解析)                              │
└─────────────┬───────────────────────────┘
              │ 依赖
              ▼
┌─────────────────────────────────────────┐
│  MOD-04 VM                              │
│  (虚拟机, 执行引擎)                        │
└──────┬──────────────────────────────────┘
       │ 依赖
       ├──────────────────────────────────┐
       ▼                                  ▼
┌──────────────────┐            ┌──────────────────┐
│  MOD-01 Storage  │            │  MOD-02 Pager    │
└──────────────────┘            └──────────────────┘

阶段 3: 高级功能
┌─────────────────────────────────────────┐
│  MOD-05 Transaction                     │
│  (事务, WAL)                             │
└──────┬──────────────────────────────────┘
       │ 依赖
       ├──────────────────────────────────┐
       ▼                                  ▼
┌──────────────────┐            ┌──────────────────┐
│  MOD-01 Storage  │            │  MOD-02 Pager    │
└──────────────────┘            └──────────────────┘

┌─────────────────────────────────────────┐
│  MOD-06 Index                           │
│  (索引管理)                              │
└──────┬──────────────────────────────────┘
       │ 依赖
       ▼
┌──────────────────┐
│  MOD-01 Storage  │
└──────────────────┘
```

---

## 5. 覆盖映射

| 架构模块 | 开发计划 | 任务数 | 覆盖状态 |
|---------|---------|-------|---------|
| MOD-01 Storage | 02-mod-01-storage.md | 8 | ✅ |
| MOD-02 Pager | 01-mod-02-pager.md | 6 | ✅ |
| MOD-03 Parser | 03-mod-03-parser.md | 5 | ✅ |
| MOD-04 VM | 04-mod-04-vm.md | 8 | ✅ |
| MOD-05 Transaction | 05-mod-05-transaction.md | 6 | ✅ |
| MOD-06 Index | 06-mod-06-index.md | 5 | ✅ |
| 集成测试 | 07-integration-test.md | 4 | ✅ |

---

## 6. 技术栈

| 类型 | 技术 | 版本 |
|-----|------|------|
| 编程语言 | Rust | 1.70+ |
| 构建工具 | Cargo | 内置 |
| 测试框架 | 内置 test + criterion | - |
| 错误处理 | thiserror + anyhow | 1.0+ |

---

## 7. 开发规范

### 7.1 代码规范
- 使用 `cargo fmt` 格式化代码
- 使用 `cargo clippy` 检查代码质量
- 遵循 Rust API Guidelines

### 7.2 提交规范
```
<type>(<scope>): <subject>

<body>

type:
- feat: 新功能
- fix: 修复
- docs: 文档
- test: 测试
- refactor: 重构
- perf: 性能优化
```

### 7.3 测试规范
- 单元测试覆盖率 ≥ 80%
- 每个公共函数都要有测试
- 使用 `cargo test` 运行测试

---

## 8. 项目结构

```
sqllite-rust/
├── Cargo.toml
├── src/
│   ├── main.rs              # 入口
│   ├── lib.rs               # 库入口
│   ├── pager/               # MOD-02: 页面管理
│   │   ├── mod.rs
│   │   ├── page.rs
│   │   ├── cache.rs
│   │   ├── freelist.rs
│   │   └── header.rs
│   ├── storage/             # MOD-01: 存储引擎
│   │   ├── mod.rs
│   │   ├── btree.rs
│   │   ├── node.rs
│   │   ├── record.rs
│   │   ├── schema.rs
│   │   └── cursor.rs
│   ├── sql/                 # MOD-03: SQL 解析
│   │   ├── mod.rs
│   │   ├── token.rs
│   │   ├── tokenizer.rs
│   │   ├── ast.rs
│   │   ├── parser.rs
│   │   └── error.rs
│   ├── vm/                  # MOD-04: 虚拟机
│   │   ├── mod.rs
│   │   ├── opcode.rs
│   │   ├── instruction.rs
│   │   ├── codegen.rs
│   │   ├── executor.rs
│   │   ├── cursor.rs
│   │   └── result.rs
│   ├── transaction/         # MOD-05: 事务
│   │   ├── mod.rs
│   │   ├── state.rs
│   │   ├── wal.rs
│   │   ├── lock.rs
│   │   └── error.rs
│   ├── index/               # MOD-06: 索引
│   │   ├── mod.rs
│   │   ├── metadata.rs
│   │   ├── key.rs
│   │   └── scan.rs
│   └── error.rs             # 全局错误定义
├── tests/                   # 集成测试
│   ├── integration_tests.rs
│   └── common/
└── docs/
    └── v1/
        ├── 01-prd.md
        ├── 02-architecture/
        └── 03-dev-plan/
```

---

## 9. 里程碑

| 里程碑 | 交付物 | 验收标准 |
|-------|-------|---------|
| M1: 阶段 1 完成 | 存储引擎 | B+ Tree CRUD 通过测试 |
| M2: 阶段 2 完成 | SQL 层 | 支持 SELECT/INSERT/UPDATE/DELETE |
| M3: 阶段 3 完成 | 高级功能 | 支持事务、索引 |
| M4: 项目完成 | 完整数据库 | 所有验收标准通过 |

---

## 变更历史

| 版本 | 日期 | 变更内容 | 作者 |
|-----|------|---------|------|
| 1.0 | 2026-03-14 | 初始版本 | Claude |
