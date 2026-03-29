# 性能测试方案 - OLTP 聚焦

## 测试目标

验证 sqllite-rust 作为**事务型数据库（OLTP）**替代 SQLite 的能力。

**核心原则**:
- 不测大规模分析查询（不是我们的目标场景）
- 不测 GPU/向量化（不适合 OLTP）
- 重点测并发、点查、小范围查询、事务

---

## 测试环境

```bash
# 安装 SQLite 对比基准
brew install sqlite3  # macOS
sudo apt-get install sqlite3  # Ubuntu

# 验证
sqlite3 --version
```

---

## 测试方案

### 测试 1: 点查性能 (point_select)

**场景**: 通过主键查询单条记录（最常见 OLTP 操作）

**数据规模**: 10万条记录

**SQL**:
```sql
SELECT * FROM users WHERE id = ?;
```

**测试方法**:
- 随机选择 id，执行 10000 次
- 测量平均延迟、P99 延迟

**目标**:
| 指标 | SQLite | 当前 | 目标 |
|------|--------|------|------|
| 平均延迟 | 0.03ms | 0.1ms | <0.05ms |
| P99 延迟 | 0.05ms | 0.2ms | <0.1ms |

---

### 测试 2: 索引查询 (index_select)

**场景**: 通过二级索引查询（覆盖索引 vs 非覆盖索引）

**数据规模**: 10万条记录

**SQL**:
```sql
-- 覆盖索引（只需读索引页）
SELECT email FROM users WHERE email = ?;

-- 非覆盖索引（需要回表）
SELECT * FROM users WHERE email = ?;
```

**目标**:
| 指标 | SQLite | 当前 | 目标 |
|------|--------|------|------|
| 覆盖索引 | 0.02ms | 0.08ms | <0.04ms |
| 非覆盖索引 | 0.05ms | 0.15ms | <0.08ms |

**优化方向**: 覆盖索引避免回表

---

### 测试 3: 范围查询 (range_select)

**场景**: 查询一定范围内的记录（分页查询）

**数据规模**: 100万条记录

**SQL**:
```sql
SELECT * FROM orders WHERE user_id = ? ORDER BY created_at DESC LIMIT 10;
```

**目标**:
| 返回行数 | SQLite | 当前 | 目标 |
|----------|--------|------|------|
| 10行 | 0.5ms | 2ms | <1ms |
| 100行 | 2ms | 8ms | <3ms |
| 1000行 | 15ms | 50ms | <20ms |

**优化方向**: B+Tree 预读、更好的缓存策略

---

### 测试 4: 单条插入 (single_insert)

**场景**: 自动提交模式下的单条插入

**SQL**:
```sql
INSERT INTO logs VALUES (?, ?, ?);
-- 自动 COMMIT
```

**目标**:
| 指标 | SQLite | 当前 | 目标 |
|------|--------|------|------|
| 吞吐 | ~1000 ops/s | ~100 ops/s | >500 ops/s |
| 延迟 | 1ms | 10ms | <2ms |

**优化方向**: WAL 优化、异步刷盘

---

### 测试 5: 批量插入 (batch_insert)

**场景**: 事务包裹的批量插入

**SQL**:
```sql
BEGIN;
INSERT INTO logs VALUES (?, ?, ?);
-- 重复 N 次
COMMIT;
```

**目标**:
| 批量大小 | SQLite | 当前 | 目标 |
|----------|--------|------|------|
| 100条 | 5K ops/s | 1K ops/s | >4K ops/s |
| 1000条 | 20K ops/s | 5K ops/s | >15K ops/s |
| 10000条 | 50K ops/s | 10K ops/s | >40K ops/s |

**优化方向**: WAL 组提交、批量处理

---

### 测试 6: 事务更新 (transaction_update)

**场景**: 读取-修改-写入事务（典型银行转账场景）

**SQL**:
```sql
BEGIN;
SELECT balance FROM accounts WHERE id = ?;
-- 应用层: new_balance = balance - 100
UPDATE accounts SET balance = ? WHERE id = ?;
COMMIT;
```

**目标**:
| 指标 | SQLite | 当前 | 目标 |
|------|--------|------|------|
| 吞吐 | ~500 TPS | ~50 TPS | >400 TPS |

---

### 测试 7: 并发读取 (concurrent_read) ⭐ 核心

**场景**: 多线程同时读取（这是我们要超越 SQLite 的核心场景）

**测试方法**:
- 100万条记录预热
- 10/50/100 个线程并发
- 每个线程执行 1000 次点查

**SQL**:
```sql
SELECT * FROM users WHERE id = ?;
```

**目标**:
| 并发数 | SQLite | 当前 | 目标 |
|--------|--------|------|------|
| 1线程 | 30K ops/s | 10K ops/s | 25K ops/s |
| 10线程 | 20K ops/s | 串行 | **200K ops/s** |
| 100线程 | 10K ops/s | 串行 | **500K ops/s** |

**SQLite 限制**: 多读者单写者，并发度有限  
**我们的优势**: MVCC 无锁读，线性扩展

---

### 测试 8: 读写混合 (mixed_workload)

**场景**: 模拟真实应用负载（读多写少）

**比例**: 读 90% : 写 10%

**SQL**:
```sql
-- 90%: 读
SELECT * FROM users WHERE id = ?;

-- 10%: 写
BEGIN;
UPDATE users SET last_login = ? WHERE id = ?;
COMMIT;
```

**目标**:
| 并发数 | SQLite | 当前 | 目标 |
|--------|--------|------|------|
| 10线程 | 5K ops/s | 串行 | **50K ops/s** |
| 100线程 | 2K ops/s | 串行 | **100K ops/s** |

---

### 测试 9: 预编译缓存 (prepared_cache)

**场景**: 重复执行相同 SQL（测试预编译缓存效果）

**SQL**:
```sql
-- 重复执行 10000 次
SELECT * FROM users WHERE id = ?;
```

**目标**:
| 指标 | SQLite | 当前 | 目标 |
|------|--------|------|------|
| 首次执行 | 0.5ms | 2ms | <1ms |
| 缓存命中 | 0.03ms | 0.1ms | <0.03ms |
| 提升倍数 | 15x | 20x | **>30x** |

---

### 测试 10: 连接开销 (connection_overhead)

**场景**: 嵌入式数据库通常不需要网络连接，测试重新打开数据库的开销

**方法**:
- 循环: 打开 → 执行1条查询 → 关闭

**目标**:
| 指标 | SQLite | 当前 | 目标 |
|------|--------|------|------|
| 单次开销 | 1ms | 5ms | <2ms |

---

## 测试执行

### 运行所有测试

```bash
# 运行基准测试
cargo bench

# 与 SQLite 对比
./run_benchmark.sh

# 生成可视化报告
python3 visualize.py
```

### 查看报告

```bash
open target/criterion/report/index.html
```

---

## 性能目标汇总

### 短期目标 (Phase 1: 3周)

| 场景 | SQLite | 当前 | 目标 |
|------|--------|------|------|
| 点查 | 0.03ms | 0.1ms | <0.05ms |
| 批量插入 | 50K/s | 10K/s | >40K/s |
| 预编译缓存 | 15x | 5x | >20x |

### 中期目标 (Phase 2: 7周) ⭐

| 场景 | SQLite | 当前 | 目标 |
|------|--------|------|------|
| 100线程并发读 | 10K/s | 串行 | **>500K/s** |
| 读写混合(100线程) | 2K/s | 串行 | **>100K/s** |

### 长期目标 (Phase 3-5: 17周)

| 场景 | SQLite | 目标 |
|------|--------|------|
| 单线程全面达标 | 100% | >80% |
| 并发性能 | 1x | **100x** |
| 功能完整性 | 100% | >95% |

---

## 附: SQLite 基准测试命令

```bash
# 创建测试数据
sqlite3 bench.db "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT);"

# 生成测试数据
sqlite3 bench.db <<EOF
INSERT INTO users SELECT 
    value,
    'User' || value,
    'user' || value || '@example.com'
FROM generate_series(1, 100000);
EOF

# 点查测试
time sqlite3 bench.db "SELECT * FROM users WHERE id = 50000;"

# 并发测试 (使用多个进程)
for i in {1..10}; do
    sqlite3 bench.db "SELECT * FROM users WHERE id = $i;" &
done
wait
```
