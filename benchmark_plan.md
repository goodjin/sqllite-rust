# SQLite 性能对比测试方案

## 测试环境准备

### 1. 安装 SQLite (如果尚未安装)
```bash
# macOS
brew install sqlite3

# Ubuntu/Debian
sudo apt-get install sqlite3

# 验证安装
sqlite3 --version
```

### 2. 运行基准测试
```bash
# 运行所有基准测试
cargo bench

# 运行特定测试组
cargo bench single_insert
cargo bench indexed_select

# 生成详细报告（包含图表）
cargo bench -- --verbose
```

---

## 测试方案详解

### 方案 1: 单条插入性能 (single_insert)
**目的**: 测试无事务包裹的单条 INSERT 性能

**测试规模**: 100, 1000, 5000 条记录

**SQL 示例**:
```sql
CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT);
INSERT INTO users VALUES (1, 'User1', 'user1@example.com');
-- ... 每条单独执行
```

**关注指标**:
- 每条 INSERT 的平均耗时
- 与 SQLite 的性能差距倍数

**预期差异原因**:
- 原生 SQLite 使用 C 语言，执行效率更高
- 本实现每次执行需要解析 SQL、编译 VM 指令

---

### 方案 2: 批量插入性能 (batch_insert)
**目的**: 测试事务包裹的批量插入性能

**测试规模**: 1000, 10000, 50000 条记录

**SQL 示例**:
```sql
BEGIN;
INSERT INTO logs VALUES (1, 'Log message 1', 123456);
-- ... 多条
COMMIT;
```

**关注指标**:
- 批量插入吞吐量 (条/秒)
- 事务提交耗时

**预期差异原因**:
- WAL 模式 vs 普通日志模式
- 页面缓存策略差异

---

### 方案 3: 简单查询性能 (simple_select)
**目的**: 测试全表扫描查询性能

**测试规模**: 1000, 10000, 100000 条记录

**SQL 示例**:
```sql
SELECT * FROM products WHERE price > 50.0;
```

**关注指标**:
- 全表扫描速度
- 数据过滤效率

---

### 方案 4: 索引查询性能 (indexed_select)
**目的**: 对比有索引 vs 无索引的查询性能

**测试规模**: 1000, 10000, 100000 条记录

**SQL 示例**:
```sql
-- 带索引
CREATE INDEX idx_email ON users(email);
SELECT * FROM users WHERE email = 'user500@example.com';

-- 无索引（全表扫描）
SELECT * FROM users_no_idx WHERE email = 'user500@example.com';
```

**关注指标**:
- 索引查找 vs 全表扫描的时间比
- B-tree 索引效率

---

### 方案 5: JOIN 查询性能 (join_query)
**目的**: 测试多表 JOIN 性能

**测试规模**: 100, 1000, 5000 订单，每订单 5 个商品项

**SQL 示例**:
```sql
SELECT o.*, oi.product_name
FROM orders o
JOIN order_items oi ON o.id = oi.order_id
WHERE o.amount > 100;
```

**关注指标**:
- JOIN 算法效率（Nested Loop Join）
- 大表 JOIN 性能

---

### 方案 6: 更新性能 (update)
**目的**: 测试 UPDATE 语句性能

**测试规模**: 100, 1000, 5000 次更新

**SQL 示例**:
```sql
UPDATE inventory SET quantity = quantity + 1 WHERE id = 100;
```

**关注指标**:
- 单行更新耗时
- 索引更新开销

---

### 方案 7: 删除性能 (delete)
**目的**: 测试 DELETE 语句性能

**测试规模**: 删除 10%, 50%, 90% 的数据

**SQL 示例**:
```sql
DELETE FROM events WHERE id < 5000;
```

**关注指标**:
- 批量删除效率
- 数据页回收情况

---

### 方案 8: 聚合查询性能 (aggregation)
**目的**: 测试 GROUP BY 和聚合函数性能

**测试规模**: 1000, 10000, 100000 条记录

**SQL 示例**:
```sql
SELECT region, COUNT(*), SUM(amount), AVG(amount), MAX(amount), MIN(amount)
FROM sales
GROUP BY region;
```

**关注指标**:
- 分组计算效率
- 聚合函数性能

---

## 预期性能对比

| 操作类型 | SQLite (参考) | 本实现 (预期) | 差距 |
|---------|--------------|--------------|------|
| 单条插入 | ~1000 ops/s | ~100-300 ops/s | 3-10x |
| 批量插入 | ~50000 ops/s | ~10000-20000 ops/s | 2-5x |
| 索引查询 | ~0.01ms | ~0.1-0.5ms | 10-50x |
| 全表扫描 | ~10ms/10k rows | ~50-100ms/10k rows | 5-10x |
| JOIN | ~5ms/1k rows | ~20-50ms/1k rows | 4-10x |

---

## 改进方向

根据测试结果，可能的优化方向：

1. **SQL 解析缓存**: 缓存解析后的 AST
2. **预编译语句**: 支持参数化查询
3. **连接池**: 减少重复初始化开销
4. **批量写入优化**: 优化 WAL 写入策略
5. **索引优化**: B-tree 节点缓存
6. **查询计划器**: 简单的成本估算

---

## 运行测试结果

测试报告生成位置: `target/criterion/`

查看 HTML 报告:
```bash
open target/criterion/report/index.html
```
