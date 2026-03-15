# 集成测试计划

## 文档信息
- **项目名称**: sqllite-rust
- **版本**: v1.0
- **对应架构**: docs/v1/02-architecture/
- **优先级**: P0 (阶段 3)
- **预估工时**: 2天

---

## 1. 测试范围

### 1.1 测试目标
- 验证各模块集成后的功能正确性
- 验证 SQL 语句端到端执行
- 验证事务 ACID 特性
- 验证性能指标

### 1.2 测试边界

**包含**:
- SQL 解析 → 执行 → 存储 完整流程
- 事务管理流程
- 并发访问场景
- 崩溃恢复场景

**不包含**:
- 单元测试已覆盖的内容
- 纯内部实现细节

---

## 2. 测试场景

### 2.1 基础功能测试

| 场景编号 | 场景名称 | 前置条件 | 测试步骤 | 预期结果 |
|---------|---------|---------|---------|---------|
| INT-001 | 创建数据库 | 无 | 1. 调用 Database::open | 数据库创建成功 |
| INT-002 | 创建表 | 数据库已打开 | 1. 执行 CREATE TABLE | 表创建成功 |
| INT-003 | 插入数据 | 表已创建 | 1. 执行 INSERT | 数据插入成功 |
| INT-004 | 查询数据 | 已有数据 | 1. 执行 SELECT | 返回正确结果 |
| INT-005 | 更新数据 | 已有数据 | 1. 执行 UPDATE | 数据更新成功 |
| INT-006 | 删除数据 | 已有数据 | 1. 执行 DELETE | 数据删除成功 |
| INT-007 | 删除表 | 表已存在 | 1. 执行 DROP TABLE | 表删除成功 |

### 2.2 WHERE 条件测试

| 场景编号 | 场景名称 | 测试步骤 | 预期结果 |
|---------|---------|---------|---------|
| INT-008 | 等值查询 | SELECT * WHERE id = 1 | 返回匹配行 |
| INT-009 | 范围查询 | SELECT * WHERE id > 5 | 返回范围内行 |
| INT-010 | AND 条件 | SELECT * WHERE a = 1 AND b = 2 | 返回同时满足的行 |
| INT-011 | OR 条件 | SELECT * WHERE a = 1 OR b = 2 | 返回满足任一条件的行 |
| INT-012 | 复合条件 | SELECT * WHERE (a = 1 OR b = 2) AND c > 3 | 返回正确结果 |

### 2.3 事务测试

| 场景编号 | 场景名称 | 测试步骤 | 预期结果 |
|---------|---------|---------|---------|
| INT-013 | 事务提交 | 1. BEGIN 2. INSERT 3. COMMIT | 数据持久化 |
| INT-014 | 事务回滚 | 1. BEGIN 2. INSERT 3. ROLLBACK | 数据未改变 |
| INT-015 | 自动回滚 | 1. BEGIN 2. 执行错误语句 | 自动回滚 |
| INT-016 | 读已提交 | 1. 事务 A 插入未提交 2. 事务 B 查询 | B 看不到未提交数据 |

### 2.4 索引测试

| 场景编号 | 场景名称 | 测试步骤 | 预期结果 |
|---------|---------|---------|---------|
| INT-017 | 创建索引 | CREATE INDEX idx ON table(col) | 索引创建成功 |
| INT-018 | 索引加速 | 对比有无索引的查询时间 | 索引查询更快 |
| INT-019 | 索引维护 | INSERT/UPDATE/DELETE 后查询 | 索引数据正确 |
| INT-020 | 唯一索引 | 插入重复值 | 插入失败 |

### 2.5 崩溃恢复测试

| 场景编号 | 场景名称 | 测试步骤 | 预期结果 |
|---------|---------|---------|---------|
| INT-021 | WAL 恢复 | 1. 插入数据 2. 模拟崩溃 3. 恢复 | 数据恢复正确 |
| INT-022 | 未提交事务丢弃 | 1. BEGIN 2. 插入 3. 崩溃 4. 恢复 | 未提交数据丢失 |

---

## 3. 开发任务拆分

### 任务清单

| 任务编号 | 任务名称 | 涉及文件 | 代码行数 | 依赖 |
|---------|---------|---------|---------|------|
| T-01 | 测试环境搭建 | 2 | ~80 | - |
| T-02 | 基础功能测试 | 1 | ~150 | T-01 |
| T-03 | WHERE 条件测试 | 1 | ~120 | T-01 |
| T-04 | 事务测试 | 1 | ~150 | T-01 |
| T-05 | 索引测试 | 1 | ~120 | T-01 |
| T-06 | 崩溃恢复测试 | 1 | ~100 | T-01 |
| T-07 | 性能测试 | 1 | ~80 | T-01 |

---

## 4. 详细任务定义

### T-01: 测试环境搭建

**任务概述**: 搭建集成测试环境

**输出**:
- `tests/integration_tests.rs`
- `tests/common/mod.rs`

**实现要求**:
```rust
// tests/common/mod.rs
use sqllite_rust::Database;
use std::fs;
use std::path::Path;

pub fn setup() -> String {
    let test_db = format!("/tmp/test_{}.db", uuid::Uuid::new_v4());
    if Path::new(&test_db).exists() {
        fs::remove_file(&test_db).unwrap();
    }
    test_db
}

pub fn teardown(db_path: &str) {
    if Path::new(db_path).exists() {
        fs::remove_file(db_path).unwrap();
    }
    // 清理 WAL 文件
    let wal_path = format!("{}-wal", db_path);
    if Path::new(&wal_path).exists() {
        fs::remove_file(&wal_path).unwrap();
    }
}

pub fn create_test_db() -> (Database, String) {
    let path = setup();
    let db = Database::open(&path).unwrap();
    (db, path)
}
```

**验收标准**:
- [ ] 测试环境可重复创建
- [ ] 测试后自动清理

**预估工时**: 2小时

---

### T-02: 基础功能测试

**任务概述**: 测试基础 CRUD 功能

**输出**:
- `tests/integration_tests.rs` (基础部分)

**实现要求**:
```rust
#[test]
fn test_create_table() {
    let (mut db, path) = create_test_db();

    let result = db.execute("CREATE TABLE users (id INTEGER, name TEXT)");
    assert!(result.is_ok());

    teardown(&path);
}

#[test]
fn test_insert_and_select() {
    let (mut db, path) = create_test_db();

    db.execute("CREATE TABLE users (id INTEGER, name TEXT)").unwrap();
    db.execute("INSERT INTO users VALUES (1, 'Alice')").unwrap();

    let result = db.execute("SELECT * FROM users").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].values[0], Value::Integer(1));

    teardown(&path);
}

#[test]
fn test_update() {
    let (mut db, path) = create_test_db();

    db.execute("CREATE TABLE users (id INTEGER, name TEXT)").unwrap();
    db.execute("INSERT INTO users VALUES (1, 'Alice')").unwrap();
    db.execute("UPDATE users SET name = 'Bob' WHERE id = 1").unwrap();

    let result = db.execute("SELECT * FROM users WHERE id = 1").unwrap();
    assert_eq!(result.rows[0].values[1], Value::Text("Bob".to_string()));

    teardown(&path);
}

#[test]
fn test_delete() {
    let (mut db, path) = create_test_db();

    db.execute("CREATE TABLE users (id INTEGER, name TEXT)").unwrap();
    db.execute("INSERT INTO users VALUES (1, 'Alice')").unwrap();
    db.execute("DELETE FROM users WHERE id = 1").unwrap();

    let result = db.execute("SELECT * FROM users").unwrap();
    assert_eq!(result.rows.len(), 0);

    teardown(&path);
}
```

**验收标准**:
- [ ] 所有基础功能测试通过

**预估工时**: 4小时

**依赖**: T-01

---

### T-03: WHERE 条件测试

**任务概述**: 测试 WHERE 子句

**输出**:
- `tests/integration_tests.rs` (WHERE 部分)

**实现要求**:
```rust
#[test]
fn test_where_equal() {
    // 测试等值查询
}

#[test]
fn test_where_range() {
    // 测试范围查询
}

#[test]
fn test_where_and() {
    // 测试 AND 条件
}

#[test]
fn test_where_or() {
    // 测试 OR 条件
}

#[test]
fn test_where_complex() {
    // 测试复合条件
}
```

**预估工时**: 3小时

**依赖**: T-01

---

### T-04: 事务测试

**任务概述**: 测试事务功能

**输出**:
- `tests/integration_tests.rs` (事务部分)

**实现要求**:
```rust
#[test]
fn test_transaction_commit() {
    let (mut db, path) = create_test_db();

    db.execute("CREATE TABLE users (id INTEGER, name TEXT)").unwrap();
    db.execute("BEGIN").unwrap();
    db.execute("INSERT INTO users VALUES (1, 'Alice')").unwrap();
    db.execute("COMMIT").unwrap();

    // 重新打开数据库验证持久化
    drop(db);
    let db = Database::open(&path).unwrap();
    let result = db.execute("SELECT * FROM users").unwrap();
    assert_eq!(result.rows.len(), 1);

    teardown(&path);
}

#[test]
fn test_transaction_rollback() {
    let (mut db, path) = create_test_db();

    db.execute("CREATE TABLE users (id INTEGER, name TEXT)").unwrap();
    db.execute("INSERT INTO users VALUES (1, 'Alice')").unwrap();
    db.execute("BEGIN").unwrap();
    db.execute("INSERT INTO users VALUES (2, 'Bob')").unwrap();
    db.execute("ROLLBACK").unwrap();

    let result = db.execute("SELECT * FROM users").unwrap();
    assert_eq!(result.rows.len(), 1);  // 只有 Alice

    teardown(&path);
}

#[test]
fn test_transaction_auto_rollback() {
    // 测试自动回滚
}
```

**预估工时**: 4小时

**依赖**: T-01

---

### T-05: 索引测试

**任务概述**: 测试索引功能

**输出**:
- `tests/integration_tests.rs` (索引部分)

**实现要求**:
```rust
#[test]
fn test_create_index() {
    let (mut db, path) = create_test_db();

    db.execute("CREATE TABLE users (id INTEGER, name TEXT)").unwrap();
    let result = db.execute("CREATE INDEX idx_name ON users (name)");
    assert!(result.is_ok());

    teardown(&path);
}

#[test]
fn test_index_performance() {
    use std::time::Instant;

    let (mut db, path) = create_test_db();

    db.execute("CREATE TABLE users (id INTEGER, name TEXT)").unwrap();

    // 插入大量数据
    for i in 0..10000 {
        db.execute(&format!("INSERT INTO users VALUES ({}, 'User{}')", i, i)).unwrap();
    }

    // 无索引查询时间
    let start = Instant::now();
    db.execute("SELECT * FROM users WHERE name = 'User5000'").unwrap();
    let time_without_index = start.elapsed();

    // 创建索引
    db.execute("CREATE INDEX idx_name ON users (name)").unwrap();

    // 有索引查询时间
    let start = Instant::now();
    db.execute("SELECT * FROM users WHERE name = 'User5000'").unwrap();
    let time_with_index = start.elapsed();

    assert!(time_with_index < time_without_index / 10);

    teardown(&path);
}

#[test]
fn test_unique_index() {
    // 测试唯一索引
}
```

**预估工时**: 3小时

**依赖**: T-01

---

### T-06: 崩溃恢复测试

**任务概述**: 测试崩溃恢复

**输出**:
- `tests/integration_tests.rs` (恢复部分)

**实现要求**:
```rust
#[test]
fn test_wal_recovery() {
    let (mut db, path) = create_test_db();

    db.execute("CREATE TABLE users (id INTEGER, name TEXT)").unwrap();
    db.execute("BEGIN").unwrap();
    db.execute("INSERT INTO users VALUES (1, 'Alice')").unwrap();
    db.execute("COMMIT").unwrap();

    // 模拟崩溃：不关闭数据库直接丢弃
    drop(db);

    // 重新打开，应该能恢复
    let db = Database::open(&path).unwrap();
    let result = db.execute("SELECT * FROM users").unwrap();
    assert_eq!(result.rows.len(), 1);

    teardown(&path);
}

#[test]
fn test_uncommitted_transaction_discarded() {
    // 测试未提交事务被丢弃
}
```

**预估工时**: 2小时

**依赖**: T-01

---

### T-07: 性能测试

**任务概述**: 性能基准测试

**输出**:
- `benches/performance.rs`

**实现要求**:
```rust
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use sqllite_rust::Database;

fn bench_insert(c: &mut Criterion) {
    c.bench_function("insert 1000 rows", |b| {
        b.iter(|| {
            let mut db = Database::open(":memory:").unwrap();
            db.execute("CREATE TABLE users (id INTEGER, name TEXT)").unwrap();
            for i in 0..1000 {
                db.execute(&format!("INSERT INTO users VALUES ({}, 'User{}')", i, i)).unwrap();
            }
        });
    });
}

fn bench_select(c: &mut Criterion) {
    c.bench_function("select 1000 rows", |b| {
        let mut db = Database::open(":memory:").unwrap();
        db.execute("CREATE TABLE users (id INTEGER, name TEXT)").unwrap();
        for i in 0..1000 {
            db.execute(&format!("INSERT INTO users VALUES ({}, 'User{}')", i, i)).unwrap();
        }

        b.iter(|| {
            db.execute("SELECT * FROM users").unwrap();
        });
    });
}

criterion_group!(benches, bench_insert, bench_select);
criterion_main!(benches);
```

**预估工时**: 2小时

**依赖**: T-01

---

## 5. 验收清单

- [ ] 所有集成测试通过
- [ ] 事务 ACID 验证通过
- [ ] 崩溃恢复验证通过
- [ ] 性能测试达到预期指标

---

## 6. 性能指标

| 指标 | 目标值 | 测试方法 |
|-----|-------|---------|
| 单条插入 | < 1ms | bench_insert |
| 单表查询(1000行) | < 10ms | bench_select |
| 索引查询 | < 1ms | bench_index_select |
| 事务提交 | < 5ms | bench_transaction |

---

## 变更历史

| 版本 | 日期 | 变更内容 | 作者 |
|-----|------|---------|------|
| 1.0 | 2026-03-14 | 初始版本 | Claude |
