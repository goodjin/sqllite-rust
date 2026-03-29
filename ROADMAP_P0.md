# P0 功能补齐计划 - 生产就绪路线图

## 目标
补齐缺失的核心功能，单线程性能达到 SQLite 的 80%，实现生产可用。

## 现状评估

| 维度 | 当前 | 目标 | 差距 |
|------|------|------|------|
| 功能完整性 | 60% | 90%+ | 外键、ALTER、子查询 |
| 单线程点查 | 0.1ms | <0.05ms | 2x 优化 |
| 单线程写入 | 10K/s | >40K/s | 4x 优化 |
| 测试覆盖 | 100+ | 500+ | 补充集成测试 |

---

## 阶段计划

### Phase 5: 外键约束系统 (3周)

**目标**: 实现完整的外键约束支持

#### Week 1: 外键元数据与创建
- [ ] `CREATE TABLE` 支持 `REFERENCES` 语法
- [ ] 外键元数据存储（sqlite_master 表扩展）
- [ ] 多列外键支持
- [ ] 自引用外键（层级数据）

#### Week 2: 外键约束检查
- [ ] INSERT 时检查父表存在性
- [ ] UPDATE 时级联检查
- [ ] `ON DELETE CASCADE` 实现
- [ ] `ON DELETE SET NULL` 实现
- [ ] `ON DELETE RESTRICT`（默认）

#### Week 3: 延迟约束与测试
- [ ] `DEFERRABLE` / `NOT DEFERRABLE` 支持
- [ ] `DEFERRED` / `IMMEDIATE` 模式
- [ ] 事务提交时批量检查
- [ ] 外键循环检测
- [ ] 100+ 单元测试

**验收标准**:
```sql
-- 所有以下语句正常工作
CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
CREATE TABLE orders (
    id INTEGER PRIMARY KEY,
    user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
    amount REAL
);

-- 约束检查
INSERT INTO orders VALUES (1, 999, 100.0); -- ❌ 报错：父表不存在
DELETE FROM users WHERE id = 1; -- ✅ 级联删除关联订单
```

---

### Phase 6: ALTER TABLE (2周)

**目标**: 支持在线表结构变更

#### Week 1: 基础 ALTER TABLE
- [ ] `ALTER TABLE ... ADD COLUMN ...`
  - 默认值处理
  - NULL/NOT NULL 约束
  - 列位置指定（FIRST/AFTER）
- [ ] `ALTER TABLE ... DROP COLUMN ...`
  - 检查约束依赖
  - 索引清理
  - 外键清理

#### Week 2: 高级 ALTER TABLE
- [ ] `ALTER TABLE ... RENAME TO ...`
  - 表名更新
  - 索引更新
  - 外键引用更新
- [ ] `ALTER TABLE ... RENAME COLUMN ...`
  - 列名更新
  - 约束更新
- [ ] 兼容性：与视图/触发器的交互

**验收标准**:
```sql
ALTER TABLE users ADD COLUMN email TEXT DEFAULT 'unknown';
ALTER TABLE users DROP COLUMN age;
ALTER TABLE users RENAME TO customers;
ALTER TABLE customers RENAME COLUMN name TO full_name;
```

---

### Phase 7: 子查询优化 (3周)

**目标**: 子查询性能 10-100x 提升

#### Week 1: 子查询解析与执行框架
- [ ] 相关子查询支持（correlated subquery）
- [ ] 非相关子查询支持
- [ ] `EXISTS` / `NOT EXISTS` 优化
- [ ] `IN` / `NOT IN` 子查询

#### Week 2: 子查询重写与优化
- [ ] 子查询 → JOIN 重写（Semi-Join）
- [ ] 标量子查询缓存
- [ ] `ANY` / `ALL` 支持
- [ ] 派生表（Derived Table）支持

#### Week 3: 集成与测试
- [ ] 复杂嵌套子查询
- [ ] 子查询 + JOIN 组合
- [ ] 性能基准测试
- [ ] 与现有优化器集成

**验收标准**:
```sql
-- 相关子查询
SELECT * FROM users u WHERE salary > (SELECT AVG(salary) FROM users WHERE dept = u.dept);

-- IN 子查询
SELECT * FROM users WHERE id IN (SELECT user_id FROM orders WHERE amount > 100);

-- EXISTS
SELECT * FROM users u WHERE EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id);

-- 派生表
SELECT * FROM (SELECT dept, AVG(salary) as avg_sal FROM users GROUP BY dept) t WHERE avg_sal > 5000;
```

**性能目标**:
- 非相关子查询: 提升到 JOIN 性能级别
- 相关子查询: 10x+ 提升（当前是全表扫描）

---

### Phase 8: 单线程性能优化 (4周)

**目标**: 点查 <0.05ms, 批量写入 >40K/s

#### Week 1: B+Tree 热点优化
- [ ] 前缀压缩集成到 B+Tree 页面
- [ ] 页面内二分查找（替换线性扫描）
- [ ] 缓存行对齐（64字节对齐）
- [ ] 批量插入优化（fill factor 调整）

#### Week 2: 解析与执行优化
- [ ] 表达式求值缓存
- [ ] WHERE 条件下推优化
- [ ] 预编译语句二进制缓存（跨进程）
- [ ] 零拷贝序列化

#### Week 3: I/O 优化
- [ ] 异步 WAL 写入（后台线程）
- [ ] 页面预读（多页顺序读）
- [ ] 写入合并（group commit 完善）
- [ ] mmap 选项（可选）

#### Week 4: 微优化与测试
- [ ] 内存池（减少 allocator 开销）
- [ ] SIMD 字符串比较（可选）
- [ ] 火焰图分析
- [ ] 与 SQLite 对比基准

**性能目标**:
| 场景 | 当前 | SQLite | 目标 |
|------|------|--------|------|
| 点查 | 0.1ms | 0.03ms | **0.04ms** |
| 批量插入 | 10K/s | 50K/s | **40K/s** |
| 范围查询(1K) | 5ms | 1.5ms | **2ms** |

---

### Phase 9: 测试与稳定性 (2周)

**目标**: 达到生产可用质量

#### Week 1: 测试覆盖
- [ ] 单元测试 100+ → 500+
- [ ] 集成测试（SQL 标准兼容性）
- [ ] 压力测试（长时间运行）
- [ ] 边界测试（超大值、空值等）
- [ ] 崩溃恢复测试

#### Week 2: 性能验证
- [ ] 完整基准测试套件
- [ ] 与 SQLite 对比报告
- [ ] 内存使用分析
- [ ] 并发压力测试

---

## 时间线汇总

```
Phase 5: 外键约束      [████████░░░░░░░░░░] 3周
Phase 6: ALTER TABLE   [████░░░░░░░░░░░░░░] 2周  
Phase 7: 子查询优化    [████████░░░░░░░░░░] 3周
Phase 8: 性能优化      [██████████░░░░░░░░] 4周
Phase 9: 测试验证      [████░░░░░░░░░░░░░░] 2周
                       
总计: 14周 (3.5个月)
```

---

## 风险与缓解

| 风险 | 影响 | 缓解措施 |
|------|------|---------|
| 外键实现复杂度高 | 延期 | 先实现基础 RESTRICT/CASCADE |
| 性能优化收益有限 | 目标不达 | 引入 profiler，针对性优化 |
| 测试发现严重 bug | 延期 | 每周进行集成测试 |

---

## 成功定义

**Phase 5-9 完成后**:
- ✅ 90%+ SQL 标准兼容性
- ✅ 单线程性能达到 SQLite 80%
- ✅ 并发读 100x 超越（已有 MVCC）
- ✅ 500+ 测试用例，无严重 bug
- ✅ 文档完整，可独立部署

**是否开始 Phase 5？**