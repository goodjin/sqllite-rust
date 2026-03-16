#!/usr/bin/env python3
"""
SQLite 性能对比可视化脚本

使用方法:
    python3 visualize.py

该脚本会:
1. 解析 Criterion 生成的 JSON 结果
2. 生成对比图表
3. 输出 Markdown 报告
"""

import json
import os
import subprocess
from pathlib import Path
from typing import Dict, List, Optional

# 尝试导入 matplotlib，如果没有则只生成文本报告
try:
    import matplotlib.pyplot as plt
    import matplotlib
    matplotlib.use('Agg')  # 非交互式后端
    HAS_MATPLOTLIB = True
except ImportError:
    HAS_MATPLOTLIB = False
    print("警告: matplotlib 未安装，将只生成文本报告")
    print("安装: pip install matplotlib")


def parse_criterion_results(bench_dir: Path) -> Dict[str, Dict]:
    """解析 Criterion 生成的基准测试结果"""
    results = {}

    if not bench_dir.exists():
        return results

    for est_file in bench_dir.rglob("estimates.json"):
        try:
            with open(est_file) as f:
                data = json.load(f)

            # 提取测试名称
            rel_path = est_file.relative_to(bench_dir)
            test_name = str(rel_path.parent).replace(os.sep, "/")

            # 提取中位数执行时间 (单位: 纳秒)
            if "median" in data and "point_estimate" in data["median"]:
                results[test_name] = {
                    "median_ns": data["median"]["point_estimate"],
                    "mean_ns": data.get("mean", {}).get("point_estimate", 0),
                    "stddev_ns": data.get("std_dev", {}).get("point_estimate", 0),
                }
        except Exception as e:
            print(f"解析失败 {est_file}: {e}")

    return results


def run_sqlite_benchmark() -> Dict[str, float]:
    """运行 SQLite 基准测试并返回结果 (毫秒)"""
    results = {}

    # 使用 subprocess 运行 SQLite 测试
    test_cases = [
        ("single_insert_100", 100),
        ("single_insert_1000", 1000),
    ]

    for name, count in test_cases:
        import tempfile
        import time

        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
            db_path = f.name

        try:
            start = time.perf_counter()

            # 创建表
            subprocess.run(
                ['sqlite3', db_path, 'CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);'],
                capture_output=True
            )

            # 插入数据
            for i in range(count):
                subprocess.run(
                    ['sqlite3', db_path, f"INSERT INTO users VALUES ({i}, 'User{i}');"],
                    capture_output=True
                )

            elapsed = (time.perf_counter() - start) * 1000  # 转换为毫秒
            results[name] = elapsed

        finally:
            os.unlink(db_path)

    return results


def generate_text_report(
    rust_results: Dict[str, Dict],
    sqlite_results: Dict[str, float]
) -> str:
    """生成文本对比报告"""

    report = []
    report.append("# SQLite Rust 性能对比报告")
    report.append("")
    report.append("## 测试环境")
    report.append("- SQLite 版本: " + get_sqlite_version())
    report.append("- Rust 版本: " + get_rust_version())
    report.append("")

    report.append("## 测试结果对比")
    report.append("")
    report.append("| 测试项 | SQLite (ms) | 本实现 (ms) | 性能差距 |")
    report.append("|--------|-------------|-------------|----------|")

    # 匹配测试项进行对比
    comparisons = [
        ("single_insert/100", "single_insert_100", "单条插入 100 条"),
        ("single_insert/1000", "single_insert_1000", "单条插入 1000 条"),
    ]

    for rust_key, sqlite_key, desc in comparisons:
        rust_time = rust_results.get(rust_key, {}).get("median_ns", 0) / 1_000_000  # 转换为 ms
        sqlite_time = sqlite_results.get(sqlite_key, 0)

        if rust_time > 0 and sqlite_time > 0:
            ratio = rust_time / sqlite_time
            report.append(f"| {desc} | {sqlite_time:.2f} | {rust_time:.2f} | {ratio:.1f}x |")

    report.append("")
    report.append("## 本实现详细结果")
    report.append("")

    for test_name, data in sorted(rust_results.items()):
        median_ms = data["median_ns"] / 1_000_000
        mean_ms = data["mean_ns"] / 1_000_000
        report.append(f"### {test_name}")
        report.append(f"- 中位数: {median_ms:.3f} ms")
        report.append(f"- 平均值: {mean_ms:.3f} ms")
        report.append(f"- 标准差: {data['stddev_ns'] / 1_000_000:.3f} ms")
        report.append("")

    return "\n".join(report)


def generate_charts(rust_results: Dict[str, Dict], output_dir: Path):
    """生成对比图表"""
    if not HAS_MATPLOTLIB:
        return

    # 按类别分组
    categories = {
        "insert": [],
        "select": [],
        "parse": [],
        "mixed": [],
        "other": [],
    }

    for test_name, data in rust_results.items():
        median_ms = data["median_ns"] / 1_000_000

        if "insert" in test_name.lower():
            categories["insert"].append((test_name, median_ms))
        elif "select" in test_name.lower():
            categories["select"].append((test_name, median_ms))
        elif "parse" in test_name.lower():
            categories["parse"].append((test_name, median_ms))
        elif "mixed" in test_name.lower():
            categories["mixed"].append((test_name, median_ms))
        else:
            categories["other"].append((test_name, median_ms))

    # 生成图表
    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    fig.suptitle('SQLite Rust Benchmark Results', fontsize=16)

    for idx, (category, data) in enumerate(list(categories.items())[:4]):
        if not data:
            continue

        ax = axes[idx // 2, idx % 2]
        names = [d[0].split('/')[-1] for d in data]
        values = [d[1] for d in data]

        ax.barh(names, values)
        ax.set_xlabel('Time (ms)')
        ax.set_title(f'{category.capitalize()} Operations')
        ax.set_xscale('log')

    plt.tight_layout()
    chart_path = output_dir / "benchmark_charts.png"
    plt.savefig(chart_path, dpi=150)
    print(f"图表已保存: {chart_path}")


def get_sqlite_version() -> str:
    """获取 SQLite 版本"""
    try:
        result = subprocess.run(
            ['sqlite3', '--version'],
            capture_output=True,
            text=True
        )
        return result.stdout.strip().split()[0]
    except:
        return "未知"


def get_rust_version() -> str:
    """获取 Rust 版本"""
    try:
        result = subprocess.run(
            ['rustc', '--version'],
            capture_output=True,
            text=True
        )
        return result.stdout.strip()
    except:
        return "未知"


def main():
    print("SQLite Rust 性能对比可视化")
    print("=" * 50)

    # 基准结果目录
    bench_dir = Path("target/criterion")

    if not bench_dir.exists():
        print("错误: 未找到基准测试结果")
        print("请先运行: cargo bench")
        return

    # 解析本实现的测试结果
    print("\n解析本实现基准测试结果...")
    rust_results = parse_criterion_results(bench_dir)
    print(f"找到 {len(rust_results)} 个测试项")

    # 运行 SQLite 测试
    print("\n运行 SQLite 基准测试...")
    sqlite_results = run_sqlite_benchmark()

    # 生成报告
    print("\n生成对比报告...")
    report = generate_text_report(rust_results, sqlite_results)

    # 保存报告
    report_path = Path("benchmark_report.md")
    with open(report_path, 'w') as f:
        f.write(report)
    print(f"报告已保存: {report_path}")

    # 生成图表
    if HAS_MATPLOTLIB:
        print("\n生成对比图表...")
        generate_charts(rust_results, Path("."))

    # 打印摘要
    print("\n" + "=" * 50)
    print(report[:2000])  # 打印报告前部分
    print("\n... (完整报告见 benchmark_report.md)")


if __name__ == "__main__":
    main()
