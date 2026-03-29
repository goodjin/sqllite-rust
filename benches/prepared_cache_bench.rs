use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};
use sqllite_rust::sql::{StatementCache, bind_params};
use sqllite_rust::sql::ast::Expression;

/// 基准测试：预编译缓存性能
/// 
/// 测试场景：
/// 1. 无缓存：每次都解析 SQL
/// 2. 有缓存：使用 StatementCache
fn benchmark_prepared_cache(c: &mut Criterion) {
    let mut group = c.benchmark_group("prepared_cache");
    
    // 测试不同缓存大小
    for cache_size in [10, 100, 1000].iter() {
        // 测试：重复执行相同 SQL（最佳缓存场景）
        group.bench_with_input(
            BenchmarkId::new("cached_same_sql", cache_size),
            cache_size,
            |b, &size| {
                let mut cache = StatementCache::new(size);
                let sql = "SELECT * FROM users WHERE id = ? AND name = ?";
                
                // 预热缓存
                cache.get_or_prepare(sql).unwrap();
                
                b.iter(|| {
                    let prepared = cache.get_or_prepare(sql).unwrap();
                    let params = [Expression::Integer(42), Expression::String("Alice".to_string())];
                    let _stmt = bind_params(&prepared, &params).unwrap();
                    black_box(&prepared);
                });
            }
        );
        
        // 测试：执行不同 SQL（缓存压力场景）
        group.bench_with_input(
            BenchmarkId::new("cached_mixed_sql", cache_size),
            cache_size,
            |b, &size| {
                let mut cache = StatementCache::new(size);
                let sqls: Vec<String> = (0..100)
                    .map(|i| format!("SELECT * FROM users WHERE id = {}", i))
                    .collect();
                
                let mut counter = 0usize;
                b.iter(|| {
                    let sql = &sqls[counter % sqls.len()];
                    let prepared = cache.get_or_prepare(sql).unwrap();
                    black_box(&prepared);
                    counter += 1;
                });
            }
        );
    }
    
    group.finish();
}

/// 基准测试：无缓存 vs 有缓存对比
fn benchmark_cache_vs_no_cache(c: &mut Criterion) {
    let mut group = c.benchmark_group("cache_comparison");
    
    let sql = "SELECT * FROM users WHERE id = ? AND status = ? AND created_at > ?";
    
    // 无缓存：每次都解析
    group.bench_function("no_cache", |b| {
        b.iter(|| {
            let mut parser = sqllite_rust::sql::Parser::new(sql).unwrap();
            let _stmt = parser.parse().unwrap();
        });
    });
    
    // 有缓存：使用 StatementCache
    group.bench_function("with_cache", |b| {
        let mut cache = StatementCache::new(100);
        // 预热
        cache.get_or_prepare(sql).unwrap();
        
        b.iter(|| {
            let prepared = cache.get_or_prepare(sql).unwrap();
            black_box(&prepared);
        });
    });
    
    group.finish();
}

/// 基准测试：不同复杂度 SQL 的缓存性能
fn benchmark_sql_complexity(c: &mut Criterion) {
    let mut group = c.benchmark_group("sql_complexity");
    
    let simple_sql = "SELECT * FROM users WHERE id = ?";
    let medium_sql = "SELECT * FROM users WHERE id = ? AND status = ? AND created_at > ? ORDER BY name LIMIT 10";
    let complex_sql = "SELECT * FROM users WHERE id = ? AND status = ? AND created_at > ? AND name LIKE ? ORDER BY name LIMIT 10";
    
    for (name, sql) in [("simple", simple_sql), ("medium", medium_sql), ("complex", complex_sql)].iter() {
        group.bench_with_input(
            BenchmarkId::new(*name, "cache_hit"),
            *sql,
            |b, sql| {
                let mut cache = StatementCache::new(100);
                cache.get_or_prepare(sql).unwrap(); // 预热
                
                b.iter(|| {
                    let prepared = cache.get_or_prepare(sql).unwrap();
                    black_box(&prepared);
                });
            }
        );
    }
    
    group.finish();
}

criterion_group!(
    benches,
    benchmark_prepared_cache,
    benchmark_cache_vs_no_cache,
    benchmark_sql_complexity
);
criterion_main!(benches);
