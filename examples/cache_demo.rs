//! 缓存功能演示 (Phase 1 Week 1)
//!
//! 演示 P1-1 预编译语句缓存和 P1-4 查询计划缓存的使用

use sqllite_rust::executor::{Executor, ExecuteResult};
use sqllite_rust::sql::Expression;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== SQLite Rust Cache Demo ===\n");

    // 创建临时数据库
    let temp_file = tempfile::NamedTempFile::new()?;
    let path = temp_file.path().to_str().unwrap();
    
    let mut executor = Executor::open(path)?;

    // ========== P1-1: 预编译语句缓存演示 ==========
    println!("--- P1-1: Statement Cache Demo ---\n");

    // 创建表
    executor.execute_sql("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")?;
    println!("✓ Created table 'users'");

    // 插入数据
    executor.execute_sql("INSERT INTO users VALUES (1, 'Alice', 30)")?;
    executor.execute_sql("INSERT INTO users VALUES (2, 'Bob', 25)")?;
    executor.execute_sql("INSERT INTO users VALUES (3, 'Charlie', 35)")?;
    println!("✓ Inserted 3 rows\n");

    // 清除缓存统计，准备演示
    executor.clear_cache();
    executor.clear_plan_cache();

    // 第一次执行查询 - 应该 miss
    println!("Executing: SELECT * FROM users WHERE age > 25");
    let result1 = executor.execute_sql("SELECT * FROM users WHERE age > 25")?;
    if let ExecuteResult::Query(ref qr) = result1 {
        println!("  Returned {} rows", qr.rows.len());
    }

    // 第二次执行相同查询 - 应该 hit
    println!("Executing same query again (should hit cache)...");
    let result2 = executor.execute_sql("SELECT * FROM users WHERE age > 25")?;
    if let ExecuteResult::Query(ref qr) = result2 {
        println!("  Returned {} rows", qr.rows.len());
    }

    // 显示语句缓存统计
    let stmt_stats = executor.cache_stats();
    println!("\nStatement Cache Stats:");
    println!("  Hits: {}", stmt_stats.hit_count);
    println!("  Misses: {}", stmt_stats.miss_count);
    println!("  Hit Rate: {:.1}%", stmt_stats.hit_rate * 100.0);
    println!("  Time Saved: {:.2}ms", stmt_stats.saved_parse_time_ms);

    // ========== P1-4: 查询计划缓存演示 ==========
    println!("\n--- P1-4: Query Plan Cache Demo ---\n");

    // 多次执行查询以触发计划缓存
    for i in 1..=3 {
        let sql = "SELECT * FROM users WHERE id = 1";
        let _ = executor.execute_sql(sql)?;
        println!("Query #{}: {}", i, sql);
    }

    // 显示计划缓存统计
    let plan_stats = executor.plan_cache_stats();
    println!("\nPlan Cache Stats:");
    println!("  Enabled: {}", plan_stats.enabled);
    println!("  Hits: {}", plan_stats.hit_count);
    println!("  Misses: {}", plan_stats.miss_count);
    println!("  Hit Rate: {:.1}%", plan_stats.hit_rate * 100.0);
    println!("  Time Saved: {:.2}ms", plan_stats.saved_plan_time_ms);

    // ========== 组合统计 ==========
    println!("\n--- Combined Cache Statistics ---");
    let combined = executor.all_cache_stats();
    println!("{}", combined);

    // ========== 预编译语句带参数演示 ==========
    println!("--- Prepared Statement with Parameters ---\n");

    // 使用 prepare 方法预编译
    let prepared = executor.prepare("SELECT * FROM users WHERE age > ?")?;
    println!("✓ Prepared statement with {} parameter(s)", prepared.param_count);

    // 使用 execute_prepared 执行参数化查询
    let result = executor.execute_prepared(
        "SELECT * FROM users WHERE age > ?",
        &[Expression::Integer(28)]
    )?;
    
    if let ExecuteResult::Query(qr) = result {
        println!("✓ Query returned {} row(s)", qr.rows.len());
        for row in &qr.rows {
            println!("  Row: {:?}", row.values);
        }
    }

    // ========== 缓存控制演示 ==========
    println!("\n--- Cache Control Demo ---");
    
    // 禁用语句缓存
    executor.disable_statement_cache();
    println!("✓ Statement cache disabled: {}", !executor.is_statement_cache_enabled());
    
    // 重新启用
    executor.enable_statement_cache();
    println!("✓ Statement cache enabled: {}", executor.is_statement_cache_enabled());
    
    // 调整缓存大小
    executor.set_statement_cache_size(50);
    println!("✓ Statement cache size set to 50");
    
    // 计划缓存控制
    executor.disable_plan_cache();
    println!("✓ Plan cache disabled: {}", !executor.is_plan_cache_enabled());
    executor.enable_plan_cache();
    println!("✓ Plan cache enabled: {}", executor.is_plan_cache_enabled());
    executor.set_plan_cache_size(25);
    println!("✓ Plan cache size set to 25");

    println!("\n=== Demo Complete ===");
    Ok(())
}
