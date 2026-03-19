//! 预编译语句缓存 + 参数绑定 性能基准测试

use std::time::Instant;

fn main() {
    // 清理测试数据库
    let _ = std::fs::remove_file("/tmp/bench_prepared.db");

    println!("=== 预编译语句缓存 + 参数绑定 基准测试 ===\n");

    // 测试1：使用参数绑定（推荐方式）
    println!("【测试1】使用参数绑定（相同模板，不同参数）");
    bench_with_params();

    // 测试2：不使用参数绑定（每次都是新 SQL）
    println!("\n【测试2】不使用参数绑定（每次都是新 SQL）");
    bench_without_params();

    // 测试3：混合场景
    println!("\n【测试3】参数绑定缓存效果");
    bench_caching();

    // 清理
    let _ = std::fs::remove_file("/tmp/bench_prepared.db");
}

/// 使用参数绑定：相同的 SQL 模板，不同的参数值
fn bench_with_params() {
    let _ = std::fs::remove_file("/tmp/bench_prepared.db");
    let mut executor = sqllite_rust::executor::Executor::open("/tmp/bench_prepared.db").unwrap();

    // 创建表
    executor.execute_sql("CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)").unwrap();

    // 使用参数绑定的 SQL 模板
    let sql = "INSERT INTO users (name, age) VALUES (?, ?)";

    let start = Instant::now();
    for i in 0..100 {
        executor.execute_prepared(
            sql,
            &[
                sqllite_rust::sql::Expression::String(format!("User{}", i)),
                sqllite_rust::sql::Expression::Integer(i as i64),
            ],
        ).unwrap();
    }
    let time = start.elapsed();

    println!("  100 次参数绑定插入: {:?}", time);
    println!("  缓存统计: {:?}", executor.cache_stats());
}

/// 不使用参数绑定：每次都是完整的 SQL 字符串
fn bench_without_params() {
    let _ = std::fs::remove_file("/tmp/bench_prepared.db");
    let mut executor = sqllite_rust::executor::Executor::open("/tmp/bench_prepared.db").unwrap();

    // 创建表
    executor.execute_sql("CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)").unwrap();

    // 每次都是完整的 SQL 字符串（没有参数绑定）
    let start = Instant::now();
    for i in 0..100 {
        let sql = format!("INSERT INTO users (name, age) VALUES ('User{}', {})", i, i);
        executor.execute_sql(&sql).unwrap();
    }
    let time = start.elapsed();

    println!("  100 次非参数化插入: {:?}", time);
    println!("  缓存统计: {:?}", executor.cache_stats());
}

/// 测试参数绑定的缓存效果
fn bench_caching() {
    let _ = std::fs::remove_file("/tmp/bench_prepared.db");
    let mut executor = sqllite_rust::executor::Executor::open("/tmp/bench_prepared.db").unwrap();

    // 创建表
    executor.execute_sql("CREATE TABLE users (id INTEGER, name TEXT)").unwrap();

    // 查询场景：使用参数绑定
    let query_sql = "SELECT * FROM users WHERE id = ?";

    // 第一次查询（缓存未命中）
    executor.execute_prepared(
        query_sql,
        &[sqllite_rust::sql::Expression::Integer(1)],
    ).unwrap();

    // 后续 99 次查询（应该命中缓存）
    let start = Instant::now();
    for i in 1..=99 {
        executor.execute_prepared(
            query_sql,
            &[sqllite_rust::sql::Expression::Integer(i as i64)],
        ).unwrap();
    }
    let time = start.elapsed();

    println!("  100 次参数化查询: {:?}", time);
    println!("  缓存统计: {:?}", executor.cache_stats());
}
