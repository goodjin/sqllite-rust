#!/bin/bash

# SQLite 与本实现性能对比脚本

set -e

echo "=========================================="
echo "SQLite Rust 性能对比测试"
echo "=========================================="

# 颜色定义
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo ""
echo -e "${BLUE}Step 1: 检查环境${NC}"

# 检查 SQLite
if ! command -v sqlite3 &> /dev/null; then
    echo -e "${YELLOW}警告: sqlite3 未安装${NC}"
    echo "请安装 SQLite:"
    echo "  macOS: brew install sqlite3"
    exit 1
fi

echo "SQLite 版本: $(sqlite3 --version | head -1)"

# 检查 Rust
if ! command -v cargo &> /dev/null; then
    echo "错误: Rust/Cargo 未安装"
    exit 1
fi

echo "Rust 版本: $(rustc --version)"

echo ""
echo -e "${BLUE}Step 2: 构建项目${NC}"
cargo build --release

echo ""
echo -e "${BLUE}Step 3: 运行 SQLite 原生测试${NC}"
echo ""

# SQLite 单条插入测试
echo "测试 SQLite 单条插入性能..."
for count in 100 1000; do
    start_time=$(date +%s%N)
    temp_db=$(mktemp)

    sqlite3 "$temp_db" "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT);"
    for i in $(seq 1 $count); do
        sqlite3 "$temp_db" "INSERT INTO users VALUES ($i, 'User$i', 'user$i@example.com');"
    done

    end_time=$(date +%s%N)
    elapsed=$(( (end_time - start_time) / 1000000 ))  # 转换为毫秒

    echo "  $count 条记录: ${elapsed}ms"
    rm -f "$temp_db"
done

# SQLite 批量插入测试
echo ""
echo "测试 SQLite 批量插入性能..."
for count in 1000 10000; do
    start_time=$(date +%s%N)
    temp_db=$(mktemp)

    sqlite3 "$temp_db" <<EOF
CREATE TABLE logs (id INTEGER PRIMARY KEY, message TEXT, timestamp INTEGER);
BEGIN;
$(for i in $(seq 1 $count); do echo "INSERT INTO logs VALUES ($i, 'Log message $i', $i);"; done)
COMMIT;
EOF

    end_time=$(date +%s%N)
    elapsed=$(( (end_time - start_time) / 1000000 ))

    echo "  $count 条记录: ${elapsed}ms"
    rm -f "$temp_db"
done

# SQLite 查询测试
echo ""
echo "测试 SQLite 查询性能..."
temp_db=$(mktemp)
sqlite3 "$temp_db" <<EOF
CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL);
BEGIN;
$(for i in $(seq 1 10000); do echo "INSERT INTO products VALUES ($i, 'Product$i', $((i % 100)).99);"; done)
COMMIT;
EOF

start_time=$(date +%s%N)
result=$(sqlite3 "$temp_db" "SELECT * FROM products WHERE price > 50.0;")
end_time=$(date +%s%N)
elapsed=$(( (end_time - start_time) / 1000000 ))
echo "  10000 条记录全表扫描: ${elapsed}ms"
rm -f "$temp_db"

echo ""
echo -e "${BLUE}Step 4: 运行本实现基准测试${NC}"
echo ""

# 运行 Criterion 基准测试
cargo bench --bench sqllite_rust_bench 2>&1 | tee /tmp/bench_output.txt || true

echo ""
echo -e "${BLUE}Step 5: 生成对比报告${NC}"
echo ""

# 提取关键结果
echo "本实现基准测试结果（摘要）:"
echo "----------------------------------------"
if [ -f "/tmp/bench_output.txt" ]; then
    grep -E "(time:|thrpt:)" /tmp/bench_output.txt | head -20 || echo "  基准测试运行完成，详细结果见 target/criterion/"
fi

echo ""
echo "详细报告位置:"
echo "  target/criterion/report/index.html"
echo ""

# 生成简单的对比摘要
echo "=========================================="
echo "对比测试完成"
echo "=========================================="
echo ""
echo "建议的对比方式:"
echo "1. 本测试仅测试了 SQLite 原生性能"
echo "2. 完整的对比需要:"
echo "   - 运行 'cargo bench --bench sqllite_rust_bench' 获取本实现数据"
echo "   - 运行 'cargo bench --bench sqlite_comparison' 获取对比数据"
echo "   - 或使用 Python 脚本生成可视化对比图表"
echo ""
echo "查看 HTML 报告:"
echo "  open target/criterion/report/index.html"
