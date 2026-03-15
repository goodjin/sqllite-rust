-- SQLite Rust 克隆 - 初始化测试数据
-- 用于创建测试数据库和初始数据

-- 创建用户表
CREATE TABLE users (
    id INTEGER,
    name TEXT,
    email TEXT,
    age INTEGER
);

-- 创建订单表
CREATE TABLE orders (
    id INTEGER,
    user_id INTEGER,
    product TEXT,
    amount INTEGER,
    status TEXT
);

-- 创建索引
CREATE INDEX idx_users_name ON users (name);
CREATE INDEX idx_orders_user_id ON orders (user_id);

-- 插入用户数据
INSERT INTO users VALUES (1, 'Alice', 'alice@example.com', 28);
INSERT INTO users VALUES (2, 'Bob', 'bob@example.com', 35);
INSERT INTO users VALUES (3, 'Charlie', 'charlie@example.com', 22);
INSERT INTO users VALUES (4, 'Diana', 'diana@example.com', 30);
INSERT INTO users VALUES (5, 'Eve', 'eve@example.com', 26);

-- 插入订单数据
INSERT INTO orders VALUES (1, 1, 'Laptop', 1200, 'completed');
INSERT INTO orders VALUES (2, 1, 'Mouse', 25, 'completed');
INSERT INTO orders VALUES (3, 2, 'Keyboard', 80, 'pending');
INSERT INTO orders VALUES (4, 3, 'Monitor', 300, 'completed');
INSERT INTO orders VALUES (5, 4, 'Headphones', 150, 'pending');
INSERT INTO orders VALUES (6, 5, 'Webcam', 60, 'completed');
INSERT INTO orders VALUES (7, 2, 'USB Cable', 15, 'completed');
INSERT INTO orders VALUES (8, 3, 'Desk Lamp', 45, 'pending');
