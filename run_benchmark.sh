#!/bin/bash

# SQLite 性能对比测试运行脚本

set -e

echo "=========================================="
echo "SQLite Rust 性能对比测试"
echo "=========================================="

# 检查 SQLite 是否安装
if ! command -v sqlite3 &> /dev/null; then
    echo "错误: sqlite3 未安装"
    echo "请安装 SQLite:"
    echo "  macOS: brew install sqlite3"
    echo "  Ubuntu: sudo apt-get install sqlite3"
    exit 1
fi

echo ""
echo "SQLite 版本:"
sqlite3 --version

echo ""
echo "=========================================="
echo "运行基准测试..."
echo "=========================================="

# 检查是否有特定测试参数
if [ $# -eq 0 ]; then
    echo ""
    echo "运行所有测试..."
    cargo bench
else
    echo ""
    echo "运行测试组: $1"
    cargo bench "$1"
fi

echo ""
echo "=========================================="
echo "测试完成!"
echo "=========================================="

# 检查是否生成了报告
if [ -f "target/criterion/report/index.html" ]; then
    echo ""
    echo "查看详细报告:"
    echo "  open target/criterion/report/index.html"
fi

echo ""
echo "原始数据位置:"
echo "  target/criterion/"
