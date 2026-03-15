-- SQLite Rust 克隆 - 测试用 SQL 语句
-- 包含各种 SQL 操作，用于功能验证

-- ==================== SELECT 查询测试 ====================

-- 测试 1: 查询所有用户
SELECT * FROM users;

-- 测试 2: 查询特定列
SELECT name, email FROM users;

-- 测试 3: 带 WHERE 条件的查询
SELECT * FROM users WHERE id = 1;

-- 测试 4: 范围查询
SELECT * FROM users WHERE age > 25;

-- 测试 5: 字符串匹配查询
SELECT * FROM users WHERE name = 'Alice';

-- 测试 6: 多条件查询 (AND)
SELECT * FROM users WHERE age > 25 AND age < 35;

-- 测试 7: 多条件查询 (OR)
SELECT * FROM users WHERE name = 'Alice' OR name = 'Bob';

-- 测试 8: 关联查询
SELECT u.name, o.product, o.amount
FROM users u, orders o
WHERE u.id = o.user_id;

-- 测试 9: 聚合查询 (如果支持)
SELECT user_id, COUNT(*) as order_count
FROM orders
GROUP BY user_id;

-- 测试 10: 排序查询 (如果支持)
SELECT * FROM users ORDER BY age DESC;

-- ==================== INSERT 插入测试 ====================

-- 测试 11: 插入新用户
INSERT INTO users VALUES (6, 'Frank', 'frank@example.com', 40);

-- 测试 12: 插入部分列 (如果支持)
INSERT INTO users (id, name) VALUES (7, 'Grace');

-- 测试 13: 批量插入
INSERT INTO orders VALUES (9, 1, 'Tablet', 500, 'pending');
INSERT INTO orders VALUES (10, 2, 'Phone Case', 20, 'completed');

-- ==================== UPDATE 更新测试 ====================

-- 测试 14: 更新单条记录
UPDATE users SET age = 29 WHERE id = 1;

-- 测试 15: 更新多条记录
UPDATE orders SET status = 'shipped' WHERE status = 'pending';

-- 测试 16: 更新多个字段
UPDATE users SET name = 'Alice Smith', email = 'alice.smith@example.com' WHERE id = 1;

-- ==================== DELETE 删除测试 ====================

-- 测试 17: 删除单条记录
DELETE FROM orders WHERE id = 10;

-- 测试 18: 删除多条记录 (带条件)
DELETE FROM orders WHERE status = 'cancelled';

-- ==================== 事务测试 ====================

-- 测试 19: 事务提交
BEGIN TRANSACTION;
INSERT INTO users VALUES (8, 'Henry', 'henry@example.com', 33);
UPDATE orders SET amount = 1300 WHERE id = 1;
COMMIT;

-- 测试 20: 事务回滚
BEGIN TRANSACTION;
DELETE FROM users WHERE id = 8;
INSERT INTO orders VALUES (11, 8, 'Test Product', 100, 'pending');
ROLLBACK;

-- ==================== 边界条件测试 ====================

-- 测试 21: 查询空结果
SELECT * FROM users WHERE id = 999;

-- 测试 22: 空值处理
INSERT INTO users VALUES (9, 'Test', NULL, NULL);
SELECT * FROM users WHERE email IS NULL;

-- 测试 23: 特殊字符
INSERT INTO users VALUES (10, 'O''Brien', 'obrien@example.com', 45);

-- 测试 24: 长字符串
INSERT INTO users VALUES (11, 'VeryLongNameThatMightCauseIssuesIfNotHandledProperly', 'long@example.com', 25);

-- ==================== 性能测试查询 ====================

-- 测试 25: 大数据量查询 (需要预先插入大量数据)
-- SELECT COUNT(*) FROM users;
-- SELECT * FROM orders WHERE amount > 100;

-- ==================== 错误处理测试 ====================

-- 测试 26: 语法错误 (应该失败)
-- SELECT * FORM users;  -- 拼写错误

-- 测试 27: 表不存在 (应该失败)
-- SELECT * FROM non_existent_table;

-- 测试 28: 列不存在 (应该失败)
-- SELECT non_existent_column FROM users;

-- ==================== 清理测试数据 ====================

-- 可选: 清理测试数据
-- DELETE FROM users WHERE id > 5;
-- DELETE FROM orders WHERE id > 8;
