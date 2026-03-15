# 产品需求文档 (PRD) - sqllite-rust

## 文档信息
- **项目名称**: sqllite-rust
- **版本**: v1.0
- **创建日期**: 2026-03-14
- **对标物**: SQLite 3.x
- **状态**: 已确认

---

## 1. 项目概述

### 1.1 背景与目标

**背景**: SQLite 是世界上最广泛部署的嵌入式数据库，但其 C 语言实现对于 Rust 生态来说存在集成复杂、内存安全问题。本项目旨在用 Rust 重新实现一个功能对标的嵌入式数据库。

**目标**: 实现一个单文件嵌入式关系型数据库，支持标准 SQL 查询、事务、索引等核心功能，提供与 SQLite 兼容的 API 接口。

**对标参考**: 本产品参考 SQLite 3.x 核心功能，主要差异为：
- **增量**: 使用 Rust 语言实现，提供内存安全保证
- **修改**: 简化部分高级功能（如 FTS、R-Tree），专注核心引擎
- **减量**: 暂不支持加密、外键约束、触发器、视图

### 1.2 目标用户

| 用户类型 | 描述 | 核心需求 |
|---------|------|---------|
| Rust 开发者 | 需要在 Rust 项目中使用嵌入式数据库 | 原生 Rust API，无需 FFI |
| 嵌入式系统开发者 | 资源受限环境下的数据库需求 | 小体积、低内存占用 |
| 学习研究者 | 想了解数据库内部实现原理 | 清晰的代码结构、模块化设计 |

### 1.3 核心价值主张

一个用 Rust 编写的轻量级嵌入式数据库，提供与 SQLite 兼容的 SQL 接口，同时利用 Rust 的内存安全特性避免常见的数据库引擎漏洞。

---

## 2. 功能需求

### 2.1 功能清单

| 编号 | 功能名称 | 优先级 | 阶段 | 对标物对比 | 描述 |
|-----|---------|-------|------|-----------|------|
| FR-001 | B+ Tree 存储引擎 | P0 | 1 | SQLite 有 | 实现磁盘上的 B+ Tree 结构存储表数据 |
| FR-002 | 页面管理器 (Pager) | P0 | 1 | SQLite 有 | 管理 4KB 固定大小的数据页，缓存管理 |
| FR-003 | 基础 CRUD 操作 | P0 | 1 | SQLite 有 | 支持创建、读取、更新、删除记录 |
| FR-004 | 定长记录存储 | P0 | 1 | SQLite 有 | 支持定长数据类型的存储 |
| FR-005 | SQL 解析器 | P0 | 2 | SQLite 有 | 解析 SQL 文本生成 AST |
| FR-006 | 虚拟机执行引擎 | P0 | 2 | SQLite 有 | 基于字节码的执行模型 |
| FR-007 | SELECT 查询 | P0 | 2 | SQLite 有 | 支持单表查询、列选择、* 通配符 |
| FR-008 | INSERT 插入 | P0 | 2 | SQLite 有 | 支持单条记录插入 |
| FR-009 | UPDATE 更新 | P0 | 2 | SQLite 有 | 支持带 WHERE 条件的更新 |
| FR-010 | DELETE 删除 | P0 | 2 | SQLite 有 | 支持带 WHERE 条件的删除 |
| FR-011 | WHERE 子句 | P0 | 2 | SQLite 有 | 支持基础条件过滤 (=, <>, <, >, <=, >=) |
| FR-012 | CREATE TABLE | P0 | 2 | SQLite 有 | 支持创建表，定义列和数据类型 |
| FR-013 | DROP TABLE | P1 | 2 | SQLite 有 | 支持删除表 |
| FR-014 | 事务支持 (ACID) | P0 | 3 | SQLite 有 | 支持 BEGIN/COMMIT/ROLLBACK |
| FR-015 | WAL 预写日志 | P0 | 3 | SQLite 有 | 实现 Write-Ahead Logging |
| FR-016 | B+ Tree 索引 | P0 | 3 | SQLite 有 | 支持 CREATE INDEX 创建索引 |
| FR-017 | 并发控制 | P1 | 3 | SQLite 有 | 实现读已提交隔离级别 |
| FR-018 | 查询优化器 | P1 | 3 | SQLite 有 | 基础的成本估算和计划选择 |

### 2.2 用户故事

#### US-001: 作为开发者，我想创建数据库文件，以便存储数据

**详细描述**:
- **角色**: Rust 开发者
- **功能**: 创建或打开一个数据库文件
- **价值**: 能够持久化存储数据
- **对标参考**: SQLite 的 `sqlite3_open()`

**验收标准**:
- AC-001-01: 可以创建新的数据库文件
- AC-001-02: 可以打开已存在的数据库文件
- AC-001-03: 数据库文件格式版本可识别

**边界条件**:
- 输入: 文件路径（有效路径或内存数据库":memory:"）
- 输出: 数据库连接句柄
- 异常: 路径无效时返回错误

#### US-002: 作为开发者，我想创建表，以便组织数据

**详细描述**:
- **角色**: Rust 开发者
- **功能**: 执行 CREATE TABLE 语句
- **价值**: 定义数据结构
- **对标参考**: SQLite 的 CREATE TABLE 语法

**验收标准**:
- AC-002-01: 支持 CREATE TABLE table_name (column1 TYPE1, column2 TYPE2, ...)
- AC-002-02: 支持 INTEGER 和 TEXT 数据类型
- AC-002-03: 表元数据持久化到数据库文件

**边界条件**:
- 输入: SQL 字符串
- 输出: 执行结果（成功/失败）
- 异常: 表已存在时返回错误

#### US-003: 作为开发者，我想插入数据，以便保存记录

**详细描述**:
- **角色**: Rust 开发者
- **功能**: 执行 INSERT 语句
- **价值**: 向表中添加数据
- **对标参考**: SQLite 的 INSERT INTO 语法

**验收标准**:
- AC-003-01: 支持 INSERT INTO table_name VALUES (value1, value2, ...)
- AC-003-02: 支持 INSERT INTO table_name (col1, col2) VALUES (...)
- AC-003-03: 数据正确存储到 B+ Tree

**边界条件**:
- 输入: SQL 字符串
- 输出: 插入的行数
- 异常: 表不存在、类型不匹配时返回错误

#### US-004: 作为开发者，我想查询数据，以便获取记录

**详细描述**:
- **角色**: Rust 开发者
- **功能**: 执行 SELECT 语句
- **价值**: 检索存储的数据
- **对标参考**: SQLite 的 SELECT 语法

**验收标准**:
- AC-004-01: 支持 SELECT * FROM table_name
- AC-004-02: 支持 SELECT col1, col2 FROM table_name
- AC-004-03: 支持 WHERE 子句过滤
- AC-004-04: 返回结果集可迭代访问

**边界条件**:
- 输入: SQL 字符串
- 输出: 结果集（行和列）
- 异常: 表不存在、列不存在时返回错误

#### US-005: 作为开发者，我想更新数据，以便修改记录

**详细描述**:
- **角色**: Rust 开发者
- **功能**: 执行 UPDATE 语句
- **价值**: 修改已存在的记录
- **对标参考**: SQLite 的 UPDATE 语法

**验收标准**:
- AC-005-01: 支持 UPDATE table_name SET col1 = value1 WHERE condition
- AC-005-02: 支持 WHERE 条件过滤
- AC-005-03: 返回更新的行数

**边界条件**:
- 输入: SQL 字符串
- 输出: 更新的行数
- 异常: 表不存在、列不存在、类型不匹配时返回错误

#### US-006: 作为开发者，我想删除数据，以便移除记录

**详细描述**:
- **角色**: Rust 开发者
- **功能**: 执行 DELETE 语句
- **价值**: 删除不需要的记录
- **对标参考**: SQLite 的 DELETE 语法

**验收标准**:
- AC-006-01: 支持 DELETE FROM table_name WHERE condition
- AC-006-02: 支持 WHERE 条件过滤
- AC-006-03: 返回删除的行数

**边界条件**:
- 输入: SQL 字符串
- 输出: 删除的行数
- 异常: 表不存在时返回错误

#### US-007: 作为开发者，我想使用事务，以确保数据一致性

**详细描述**:
- **角色**: Rust 开发者
- **功能**: 使用 BEGIN/COMMIT/ROLLBACK
- **价值**: 保证 ACID 特性
- **对标参考**: SQLite 的事务支持

**验收标准**:
- AC-007-01: 支持 BEGIN TRANSACTION 开始事务
- AC-007-02: 支持 COMMIT 提交事务
- AC-007-03: 支持 ROLLBACK 回滚事务
- AC-007-04: 事务失败时自动回滚

**边界条件**:
- 输入: SQL 字符串
- 输出: 执行结果
- 异常: 嵌套事务不支持

#### US-008: 作为开发者，我想创建索引，以提高查询性能

**详细描述**:
- **角色**: Rust 开发者
- **功能**: 执行 CREATE INDEX 语句
- **价值**: 加速 WHERE 子句查询
- **对标参考**: SQLite 的索引实现

**验收标准**:
- AC-008-01: 支持 CREATE INDEX idx_name ON table_name (column)
- AC-008-02: 索引自动用于 WHERE 条件查询
- AC-008-03: 插入/更新/删除时自动维护索引

**边界条件**:
- 输入: SQL 字符串
- 输出: 执行结果
- 异常: 表不存在、列不存在时返回错误

### 2.3 数据实体

#### Entity-001: 数据库文件头 (Database Header)

| 字段名 | 类型 | 约束 | 描述 | 对标参考 |
|-------|------|------|------|---------|
| magic | [u8; 16] | 固定值 | 文件魔数 "SQLite format 3\0" | SQLite 文件头 |
| page_size | u16 | 4096 | 页面大小 | SQLite 默认 4096 |
| file_format_write | u8 | 1 | 文件格式写版本 | SQLite |
| file_format_read | u8 | 1 | 文件格式读版本 | SQLite |
| reserved_space | u8 | 0 | 每页保留字节数 | SQLite |
| max_payload_frac | u8 | 64 | 最大负载比例 | SQLite |
| min_payload_frac | u8 | 32 | 最小负载比例 | SQLite |
| leaf_payload_frac | u8 | 32 | 叶子节点负载比例 | SQLite |
| file_change_counter | u32 | 递增 | 文件变更计数器 | SQLite |
| database_size | u32 | - | 数据库大小（页数） | SQLite |
| first_freelist_trunk | u32 | - | 第一个空闲列表主干页 | SQLite |
| freelist_pages | u32 | - | 空闲列表页总数 | SQLite |
| schema_cookie | u32 | 递增 | Schema cookie | SQLite |
| schema_format | u32 | 4 | Schema 格式号 | SQLite |
| default_cache_size | u32 | - | 默认缓存大小 | SQLite |
| largest_root_btree | u32 | - | 最大的根 B-tree 页号 | SQLite |
| text_encoding | u32 | 1 | 文本编码 (1=UTF-8) | SQLite |
| user_version | u32 | - | 用户版本号 | SQLite |
| incremental_vacuum | u32 | - | 增量真空模式 | SQLite |
| application_id | u32 | - | 应用程序 ID | SQLite |
| reserved | [u8; 20] | 0 | 保留扩展 | SQLite |
| version_valid_for | u32 | - | 版本验证号 | SQLite |
| sqlite_version | u32 | - | SQLite 版本号 | SQLite |

#### Entity-002: B+ Tree 页面 (B-Tree Page)

| 字段名 | 类型 | 约束 | 描述 | 对标参考 |
|-------|------|------|------|---------|
| page_type | u8 | 0x02/0x05/0x0A/0x0D | 页面类型 | SQLite B-tree |
| first_freeblock | u16 | - | 第一个空闲块偏移 | SQLite |
| cell_count | u16 | - | 单元格数量 | SQLite |
| cell_content_offset | u16 | - | 单元格内容起始偏移 | SQLite |
| fragmented_bytes | u1 | - | 碎片化空闲字节数 | SQLite |
| right_child | u32 | 可选 | 最右子节点页号（内部节点） | SQLite |
| cell_pointers | [u16] | - | 单元格指针数组 | SQLite |

#### Entity-003: 表元数据 (Table Schema)

| 字段名 | 类型 | 约束 | 描述 | 对标参考 |
|-------|------|------|------|---------|
| table_name | TEXT | PK | 表名 | sqlite_master |
| root_page | INTEGER | - | B+ Tree 根页号 | sqlite_master |
| sql | TEXT | - | 创建表的 SQL 语句 | sqlite_master |

#### Entity-004: 列定义 (Column Definition)

| 字段名 | 类型 | 约束 | 描述 | 对标参考 |
|-------|------|------|------|---------|
| name | TEXT | - | 列名 | SQLite |
| data_type | INTEGER | 1-5 | 数据类型 | SQLite |
| nullable | bool | - | 是否可为 NULL | SQLite |
| primary_key | bool | - | 是否主键 | SQLite |

#### Entity-005: 索引元数据 (Index Schema)

| 字段名 | 类型 | 约束 | 描述 | 对标参考 |
|-------|------|------|------|---------|
| index_name | TEXT | PK | 索引名 | sqlite_master |
| table_name | TEXT | FK | 所属表名 | sqlite_master |
| root_page | INTEGER | - | B+ Tree 根页号 | sqlite_master |
| column_name | TEXT | - | 索引列名 | sqlite_master |
| sql | TEXT | - | 创建索引的 SQL 语句 | sqlite_master |

### 2.4 业务流程

#### Flow-001: SQL 执行流程

**流程描述**: 从 SQL 文本到结果集的完整执行流程

**对标参考**: SQLite 的 SQL 执行流程

**状态定义**:
| 状态 | 描述 | 可转换到 |
|-----|------|---------|
| Idle | 空闲状态 | Parsing |
| Parsing | 解析 SQL | Compiling |
| Compiling | 生成执行计划 | Executing |
| Executing | 执行字节码 | Returning |
| Returning | 返回结果 | Idle |
| Error | 错误状态 | Idle |

**状态转换**:
| 当前状态 | 触发事件 | 下一状态 | 条件 |
|---------|---------|---------|------|
| Idle | 接收 SQL | Parsing | SQL 非空 |
| Parsing | 解析成功 | Compiling | 语法正确 |
| Parsing | 解析失败 | Error | 语法错误 |
| Compiling | 编译成功 | Executing | 计划有效 |
| Compiling | 编译失败 | Error | 语义错误 |
| Executing | 执行完成 | Returning | 正常完成 |
| Executing | 执行错误 | Error | 运行时错误 |
| Returning | 结果返回 | Idle | - |
| Error | 错误处理 | Idle | - |

#### Flow-002: 事务流程

**流程描述**: 事务的生命周期管理

**对标参考**: SQLite 的事务机制

**状态定义**:
| 状态 | 描述 | 可转换到 |
|-----|------|---------|
| AutoCommit | 自动提交模式 | Active |
| Active | 事务活跃 | Committed / RolledBack |
| Committed | 已提交 | AutoCommit |
| RolledBack | 已回滚 | AutoCommit |

**状态转换**:
| 当前状态 | 触发事件 | 下一状态 | 条件 |
|---------|---------|---------|------|
| AutoCommit | BEGIN | Active | - |
| Active | COMMIT | Committed | 无错误 |
| Active | ROLLBACK | RolledBack | - |
| Active | 错误 | RolledBack | 自动回滚 |
| Committed | 完成 | AutoCommit | - |
| RolledBack | 完成 | AutoCommit | - |

---

## 3. 非功能需求

### 3.1 性能需求

| 指标 | 要求 | 对标参考 |
|-----|------|---------|
| 单条插入 | < 1ms | SQLite 约 0.5ms |
| 单表查询(1000行) | < 10ms | SQLite 约 5ms |
| 索引查询 | < 1ms | SQLite 约 0.3ms |
| 并发读取 | 支持多读取器 | SQLite 支持 |
| 内存占用 | < 10MB 基础 | SQLite 约 2-5MB |

### 3.2 兼容性需求

| 类型 | 要求 |
|-----|------|
| 操作系统 | Linux, macOS, Windows |
| Rust 版本 | 1.70+ |
| 文件格式 | 自定义（参考 SQLite） |
| SQL 方言 | SQLite 兼容子集 |

### 3.3 可靠性需求

| 需求 | 描述 | 优先级 |
|-----|------|-------|
| 崩溃恢复 | 支持 WAL 回放恢复 | P0 |
| 数据校验 | 页面校验和 | P1 |
| 边界保护 | 防止缓冲区溢出 | P0 |

---

## 4. 研究分析附录

### 4.1 竞品功能模块对比

#### 模块1: 存储引擎

| 竞品 | 功能特性 | 用户体验 | 技术实现 |
|-----|---------|---------|---------|
| SQLite | B+ Tree，单文件，页大小可变 | 零配置，即开即用 | C 实现，高度优化 |
| LevelDB | LSM Tree，键值存储 | 高性能写入 | C++ 实现，Google |
| RocksDB | LSM Tree，优化版 LevelDB | 高性能读写 | C++ 实现，Facebook |
| sled | B+ Tree，Rust 实现 | 现代 API | Rust 实现，纯内存 B+ Tree |
| **我们的方案** | B+ Tree，单文件，固定页大小 | 类 SQLite API | Rust 实现，安全第一 |

#### 模块2: SQL 层

| 竞品 | 功能特性 | 用户体验 | 技术实现 |
|-----|---------|---------|---------|
| SQLite | 完整 SQL-92 + 扩展 | 功能丰富 | 自定义解析器 + VDBE |
| DuckDB | 分析型 SQL，向量化 | 分析查询快 | C++ 实现 |
| **我们的方案** | SQL 子集，基础 CRUD | 简单易用 | 手写解析器 + 字节码 VM |

### 4.2 竞品对比矩阵

| 维度 | SQLite | LevelDB | RocksDB | sled | 我们 |
|-----|--------|---------|---------|------|-----|
| **核心功能** |
| SQL 支持 | ✓ | ✗ | ✗ | ✗ | ✓ |
| 事务 ACID | ✓ | ✓ | ✓ | ✓ | ✓ |
| 索引 | ✓ | ✗ | ✗ | ✗ | ✓ |
| 单文件 | ✓ | ✗ | ✗ | ✗ | ✓ |
| **技术特性** |
| 内存安全 | ✗ | ✗ | ✗ | ✓ | ✓ |
| 零拷贝 | ✗ | ✓ | ✓ | ✓ | 目标 |
| 崩溃恢复 | ✓ | ✓ | ✓ | ✓ | ✓ |
| **开发体验** |
| Rust 原生 | ✗ | ✗ | ✗ | ✓ | ✓ |
| 嵌入式 | ✓ | ✓ | ✓ | ✓ | ✓ |
| 学习曲线 | 中等 | 低 | 低 | 低 | 中等 |

### 4.3 最佳实践建议

#### 推荐采用
1. **B+ Tree 存储**: SQLite 和 sled 都验证过的成熟方案，适合范围查询
2. **单文件设计**: SQLite 的核心优势，便于部署和备份
3. **WAL 模式**: SQLite 3.7+ 的默认模式，读写性能更好
4. **字节码虚拟机**: SQLite 的 VDBE 模式，便于优化和调试

#### 建议避免
1. **LSM Tree**: 虽然写入性能好，但实现复杂，且不适合小数据量嵌入式场景
2. **完整 SQL-92**: 过于复杂，先实现核心子集
3. **多文件日志**: 增加部署复杂度，单文件 WAL 更简洁

#### 创新机会
1. **纯 Rust 实现**: 利用 Rust 的类型系统和所有权模型，避免 SQLite 中常见的内存安全问题
2. **现代 API 设计**: 相比 SQLite 的 C API，提供更符合 Rust 习惯的接口
3. **模块化架构**: 存储引擎、SQL 层、执行引擎完全解耦，便于测试和扩展

---

## 5. 验收标准汇总

| 编号 | 验收标准 | 对应用户故事 | 优先级 |
|-----|---------|------------|-------|
| AC-001-01 | 可以创建新的数据库文件 | US-001 | P0 |
| AC-001-02 | 可以打开已存在的数据库文件 | US-001 | P0 |
| AC-001-03 | 数据库文件格式版本可识别 | US-001 | P0 |
| AC-002-01 | 支持 CREATE TABLE 语法 | US-002 | P0 |
| AC-002-02 | 支持 INTEGER 和 TEXT 数据类型 | US-002 | P0 |
| AC-002-03 | 表元数据持久化到数据库文件 | US-002 | P0 |
| AC-003-01 | 支持 INSERT INTO ... VALUES (...) | US-003 | P0 |
| AC-003-02 | 支持指定列插入 | US-003 | P0 |
| AC-003-03 | 数据正确存储到 B+ Tree | US-003 | P0 |
| AC-004-01 | 支持 SELECT * FROM table | US-004 | P0 |
| AC-004-02 | 支持指定列查询 | US-004 | P0 |
| AC-004-03 | 支持 WHERE 子句过滤 | US-004 | P0 |
| AC-004-04 | 返回结果集可迭代访问 | US-004 | P0 |
| AC-005-01 | 支持 UPDATE ... SET ... WHERE | US-005 | P0 |
| AC-005-02 | 支持 WHERE 条件过滤 | US-005 | P0 |
| AC-005-03 | 返回更新的行数 | US-005 | P0 |
| AC-006-01 | 支持 DELETE FROM ... WHERE | US-006 | P0 |
| AC-006-02 | 支持 WHERE 条件过滤 | US-006 | P0 |
| AC-006-03 | 返回删除的行数 | US-006 | P0 |
| AC-007-01 | 支持 BEGIN TRANSACTION | US-007 | P0 |
| AC-007-02 | 支持 COMMIT | US-007 | P0 |
| AC-007-03 | 支持 ROLLBACK | US-007 | P0 |
| AC-007-04 | 事务失败时自动回滚 | US-007 | P0 |
| AC-008-01 | 支持 CREATE INDEX | US-008 | P0 |
| AC-008-02 | 索引自动用于 WHERE 查询 | US-008 | P0 |
| AC-008-03 | 自动维护索引一致性 | US-008 | P0 |

---

## 6. 三阶段里程碑

### 阶段 1: 存储引擎 (MVP)
**目标**: 实现基础存储和 CRUD
**交付物**:
- B+ Tree 实现
- Pager 页面管理
- 基础 CRUD API
- 单线程访问

### 阶段 2: SQL 层
**目标**: 支持 SQL 查询
**交付物**:
- SQL 解析器
- 虚拟机执行引擎
- 完整 CRUD SQL 支持
- 简单 WHERE 过滤

### 阶段 3: 高级功能
**目标**: 生产就绪
**交付物**:
- 事务支持 (ACID)
- WAL 预写日志
- B+ Tree 索引
- 并发控制

---

## 变更历史

| 版本 | 日期 | 变更内容 | 作者 |
|-----|------|---------|------|
| 1.0 | 2026-03-14 | 初始版本 | Claude |
