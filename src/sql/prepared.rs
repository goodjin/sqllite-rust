//! 预编译语句缓存 (Prepared Statement Cache)
//!
//! 针对 OLTP 场景优化：
//! - 高频重复的相似查询（如 `SELECT * FROM users WHERE id = ?`）
//! - 真正的 LRU 淘汰策略
//! - 解析耗时统计，量化缓存收益

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use crate::sql::ast::{Statement, Expression};
use crate::sql::Parser;

/// 预编译语句
#[derive(Clone)]
pub struct PreparedStatement {
    /// SQL 模板
    pub sql: String,
    /// 解析后的 AST
    pub statement: Statement,
    /// 占位符数量
    pub param_count: usize,
    /// 解析耗时（用于统计）
    pub parse_time_ms: f64,
}

/// 增强版语句缓存（真正 LRU + 性能统计）
pub struct StatementCache {
    /// 缓存表：SQL -> 预编译语句
    cache: HashMap<String, Arc<PreparedStatement>>,
    /// LRU 顺序队列（头部是最久未使用）
    lru_queue: std::collections::VecDeque<String>,
    /// 最大缓存数量
    max_size: usize,
    /// 缓存命中次数
    hit_count: usize,
    /// 缓存未命中次数
    miss_count: usize,
    /// 总解析耗时（毫秒）
    total_parse_time_ms: f64,
    /// 跳过的解析耗时（即缓存带来的收益）
    saved_parse_time_ms: f64,
}

impl StatementCache {
    /// 创建新的语句缓存
    pub fn new(max_size: usize) -> Self {
        Self {
            cache: HashMap::with_capacity(max_size),
            lru_queue: std::collections::VecDeque::with_capacity(max_size),
            max_size,
            hit_count: 0,
            miss_count: 0,
            total_parse_time_ms: 0.0,
            saved_parse_time_ms: 0.0,
        }
    }

    /// 获取或创建预编译语句（真正 LRU 版本）
    pub fn get_or_prepare(&mut self, sql: &str) -> Result<Arc<PreparedStatement>, String> {
        // 尝试从缓存获取
        if let Some(stmt) = self.cache.get(sql) {
            let stmt_clone = stmt.clone();
            let parse_time = stmt.parse_time_ms;
            self.hit_count += 1;
            self.saved_parse_time_ms += parse_time;
            // 更新 LRU 顺序（移动到队尾表示最近使用）
            self.update_lru(sql);
            return Ok(stmt_clone);
        }

        // 缓存未命中，需要解析
        self.miss_count += 1;
        let start = Instant::now();

        // 解析 SQL
        let statement = self.parse_sql(sql)?;
        let parse_time = start.elapsed().as_secs_f64() * 1000.0;
        self.total_parse_time_ms += parse_time;

        // 计算占位符数量
        let param_count = count_placeholders(&statement);

        // 创建预编译语句
        let prepared = Arc::new(PreparedStatement {
            sql: sql.to_string(),
            statement,
            param_count,
            parse_time_ms: parse_time,
        });

        // 加入缓存（带 LRU 淘汰）
        self.add_to_cache_lru(sql, prepared.clone());

        Ok(prepared)
    }

    /// 更新 LRU 顺序
    fn update_lru(&mut self, key: &str) {
        // 从队列中移除旧位置
        if let Some(pos) = self.lru_queue.iter().position(|k| k == key) {
            let mut split_queue = self.lru_queue.split_off(pos);
            split_queue.pop_front(); // 移除当前元素
            self.lru_queue.extend(split_queue);
        }
        // 添加到队尾（最近使用）
        self.lru_queue.push_back(key.to_string());
    }

    /// 添加到缓存（LRU 淘汰）
    fn add_to_cache_lru(&mut self, key: &str, prepared: Arc<PreparedStatement>) {
        // 如果缓存已满，淘汰最久未使用的
        if self.cache.len() >= self.max_size && !self.cache.contains_key(key) {
            if let Some(old_key) = self.lru_queue.pop_front() {
                self.cache.remove(&old_key);
            }
        }

        // 插入新条目
        self.cache.insert(key.to_string(), prepared);
        self.lru_queue.push_back(key.to_string());
    }

    /// 解析 SQL
    fn parse_sql(&self, sql: &str) -> Result<Statement, String> {
        let mut parser = Parser::new(sql)
            .map_err(|e| format!("Parse error: {:?}", e))?;
        parser.parse()
            .map_err(|e| format!("Parse error: {:?}", e))
    }

    /// 获取缓存统计信息
    pub fn stats(&self) -> CacheStats {
        let total = self.hit_count + self.miss_count;
        let hit_rate = if total > 0 {
            self.hit_count as f64 / total as f64
        } else {
            0.0
        };

        // 计算平均解析时间
        let avg_parse_time = if self.miss_count > 0 {
            self.total_parse_time_ms / self.miss_count as f64
        } else {
            0.0
        };

        CacheStats {
            size: self.cache.len(),
            max_size: self.max_size,
            hit_count: self.hit_count,
            miss_count: self.miss_count,
            hit_rate,
            total_parse_time_ms: self.total_parse_time_ms,
            saved_parse_time_ms: self.saved_parse_time_ms,
            avg_parse_time_ms: avg_parse_time,
            speedup_ratio: if self.total_parse_time_ms > 0.0 {
                (self.total_parse_time_ms + self.saved_parse_time_ms) / self.total_parse_time_ms
            } else {
                1.0
            },
        }
    }

    /// 清除缓存
    pub fn clear(&mut self) {
        self.cache.clear();
        self.lru_queue.clear();
        self.hit_count = 0;
        self.miss_count = 0;
        self.total_parse_time_ms = 0.0;
        self.saved_parse_time_ms = 0.0;
    }

    /// 调整缓存大小
    pub fn resize(&mut self, new_size: usize) {
        self.max_size = new_size;
        // 如果新大小小于当前大小，淘汰多余条目
        while self.cache.len() > self.max_size {
            if let Some(old_key) = self.lru_queue.pop_front() {
                self.cache.remove(&old_key);
            } else {
                break;
            }
        }
    }
}

/// 详细的缓存统计信息
#[derive(Debug, Clone)]
pub struct CacheStats {
    pub size: usize,
    pub max_size: usize,
    pub hit_count: usize,
    pub miss_count: usize,
    pub hit_rate: f64,
    /// 总解析耗时（毫秒）
    pub total_parse_time_ms: f64,
    /// 节省的解析耗时（毫秒）
    pub saved_parse_time_ms: f64,
    /// 平均解析耗时（毫秒）
    pub avg_parse_time_ms: f64,
    /// 加速比（包含缓存的总时间 / 无缓存总时间）
    pub speedup_ratio: f64,
}

impl std::fmt::Display for CacheStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Statement Cache Statistics:")?;
        writeln!(f, "  Size: {}/{} ({:.1}%)", 
            self.size, self.max_size, 
            self.size as f64 / self.max_size as f64 * 100.0)?;
        writeln!(f, "  Hit Rate: {:.2}% ({} hits / {} misses)", 
            self.hit_rate * 100.0, self.hit_count, self.miss_count)?;
        writeln!(f, "  Parse Time: {:.2}ms total, {:.3}ms avg", 
            self.total_parse_time_ms, self.avg_parse_time_ms)?;
        writeln!(f, "  Time Saved: {:.2}ms", self.saved_parse_time_ms)?;
        writeln!(f, "  Speedup: {:.2}x", self.speedup_ratio)
    }
}

/// 绑定参数到预编译语句
pub fn bind_params(stmt: &PreparedStatement, params: &[Expression]) -> Result<Statement, String> {
    if params.len() != stmt.param_count {
        return Err(format!(
            "Parameter count mismatch: expected {}, got {}",
            stmt.param_count,
            params.len()
        ));
    }

    let new_statement = replace_statement_placeholders(&stmt.statement, params);
    Ok(new_statement)
}

/// 递归替换表达式中的占位符
fn replace_placeholders(expr: &Expression, params: &[Expression]) -> Expression {
    match expr {
        Expression::Placeholder(idx) => {
            let idx = (*idx as usize).saturating_sub(1);
            params.get(idx).cloned().unwrap_or(Expression::Null)
        }
        Expression::Binary { left, op, right } => Expression::Binary {
            left: Box::new(replace_placeholders(left, params)),
            op: op.clone(),
            right: Box::new(replace_placeholders(right, params)),
        },
        Expression::Vector(elements) => Expression::Vector(
            elements.iter().map(|e| replace_placeholders(e, params)).collect()
        ),
        Expression::FunctionCall { name, args } => Expression::FunctionCall {
            name: name.clone(),
            args: args.iter().map(|arg| replace_placeholders(arg, params)).collect(),
        },
        other => other.clone(),
    }
}

/// 替换 Statement 中的占位符
fn replace_statement_placeholders(stmt: &Statement, params: &[Expression]) -> Statement {
    match stmt {
        Statement::Insert(ins) => Statement::Insert(crate::sql::ast::InsertStmt {
            table: ins.table.clone(),
            columns: ins.columns.clone(),
            values: ins.values.iter().map(|row| {
                row.iter().map(|expr| replace_placeholders(expr, params)).collect()
            }).collect(),
        }),
        Statement::Select(sel) => Statement::Select(crate::sql::ast::SelectStmt {
            ctes: sel.ctes.clone(),
            columns: sel.columns.clone(),
            from: sel.from.clone(),
            joins: sel.joins.clone(),
            where_clause: sel.where_clause.as_ref().map(|expr| replace_placeholders(expr, params)),
            group_by: sel.group_by.clone(),
            having: sel.having.as_ref().map(|expr| replace_placeholders(expr, params)),
            order_by: sel.order_by.clone(),
            limit: sel.limit,
            offset: sel.offset,
        }),
        Statement::Update(upd) => Statement::Update(crate::sql::ast::UpdateStmt {
            table: upd.table.clone(),
            set_clauses: upd.set_clauses.clone(),
            where_clause: upd.where_clause.as_ref().map(|expr| replace_placeholders(expr, params)),
        }),
        Statement::Delete(del) => Statement::Delete(crate::sql::ast::DeleteStmt {
            table: del.table.clone(),
            where_clause: del.where_clause.as_ref().map(|expr| replace_placeholders(expr, params)),
        }),
        other => other.clone(),
    }
}

/// 计算语句中的占位符数量
fn count_placeholders(stmt: &Statement) -> usize {
    match stmt {
        Statement::Insert(ins) => {
            ins.values.iter().map(|row| {
                row.iter().filter(|e| matches!(e, Expression::Placeholder(_))).count()
            }).sum()
        }
        Statement::Select(sel) => {
            let mut count = 0;
            if let Some(ref where_clause) = sel.where_clause {
                count += count_expr_placeholders(where_clause);
            }
            count
        }
        Statement::Update(upd) => {
            let mut count = 0;
            if let Some(ref where_clause) = upd.where_clause {
                count += count_expr_placeholders(where_clause);
            }
            count
        }
        Statement::Delete(del) => {
            if let Some(ref where_clause) = del.where_clause {
                count_expr_placeholders(where_clause)
            } else {
                0
            }
        }
        _ => 0,
    }
}

/// 计算表达式中的占位符数量
fn count_expr_placeholders(expr: &Expression) -> usize {
    match expr {
        Expression::Placeholder(_) => 1,
        Expression::Binary { left, right, .. } => {
            count_expr_placeholders(left) + count_expr_placeholders(right)
        }
        Expression::Vector(elements) => {
            elements.iter().map(|e| count_expr_placeholders(e)).sum()
        }
        Expression::FunctionCall { args, .. } => {
            args.iter().map(|arg| count_expr_placeholders(arg)).sum()
        }
        _ => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cache_basic() {
        let mut cache = StatementCache::new(10);

        // 第一次解析
        let result = cache.get_or_prepare("SELECT * FROM users WHERE id = 1");
        assert!(result.is_ok());

        // 第二次应该命中缓存
        let result2 = cache.get_or_prepare("SELECT * FROM users WHERE id = 1");
        assert!(result2.is_ok());

        let stats = cache.stats();
        assert_eq!(stats.hit_count, 1);
        assert_eq!(stats.miss_count, 1);
        assert!(stats.hit_rate == 0.5);
    }

    #[test]
    fn test_cache_lru_eviction() {
        let mut cache = StatementCache::new(2);

        // 添加 3 个条目（超过容量）
        // t1: miss (插入 t1)
        // t2: miss (插入 t2, 缓存满)
        // t3: miss (插入 t3, 淘汰 t1)
        cache.get_or_prepare("SELECT * FROM t1").unwrap();
        cache.get_or_prepare("SELECT * FROM t2").unwrap();
        cache.get_or_prepare("SELECT * FROM t3").unwrap();

        // 检查大小
        let stats = cache.stats();
        assert_eq!(stats.size, 2);
        assert_eq!(stats.miss_count, 3);

        // 此时缓存中有 t2, t3（t1 被淘汰）
        // 访问 t2，使其变为最近使用
        let hit_t2 = cache.get_or_prepare("SELECT * FROM t2").is_ok();
        assert!(hit_t2, "t2 should be in cache");
        
        // 访问 t3，使其变为最近使用
        let hit_t3 = cache.get_or_prepare("SELECT * FROM t3").is_ok();
        assert!(hit_t3, "t3 should be in cache");

        let stats = cache.stats();
        // 3 个初始 miss + 2 个 hit
        assert_eq!(stats.miss_count, 3);
        assert_eq!(stats.hit_count, 2);
    }

    #[test]
    fn test_cache_lru_order() {
        let mut cache = StatementCache::new(3);

        // 添加 3 个条目
        cache.get_or_prepare("SELECT * FROM t1").unwrap();
        cache.get_or_prepare("SELECT * FROM t2").unwrap();
        cache.get_or_prepare("SELECT * FROM t3").unwrap();

        // 访问 t1，使其变为最近使用
        cache.get_or_prepare("SELECT * FROM t1").unwrap();

        // 添加 t4，应该淘汰 t2（最久未使用）
        cache.get_or_prepare("SELECT * FROM t4").unwrap();

        // t1 和 t3 应该在，t2 应该被淘汰
        let hit_t1 = cache.cache.contains_key("SELECT * FROM t1");
        let hit_t2 = cache.cache.contains_key("SELECT * FROM t2");
        let hit_t3 = cache.cache.contains_key("SELECT * FROM t3");

        assert!(hit_t1, "t1 should be in cache (recently used)");
        assert!(!hit_t2, "t2 should be evicted (least recently used)");
        assert!(hit_t3, "t3 should be in cache");
    }

    #[test]
    fn test_cache_with_placeholders() {
        let mut cache = StatementCache::new(10);

        let sql = "SELECT * FROM users WHERE id = ?";
        let prepared = cache.get_or_prepare(sql).unwrap();
        assert_eq!(prepared.param_count, 1);

        // 绑定参数
        let params = [Expression::Integer(42)];
        let bound_stmt = bind_params(&prepared, &params).unwrap();

        // 验证占位符被替换
        match &bound_stmt {
            Statement::Select(sel) => {
                if let Some(Expression::Binary { right, .. }) = &sel.where_clause {
                    if let Expression::Integer(n) = right.as_ref() {
                        assert_eq!(*n, 42);
                    } else {
                        panic!("Expected Integer 42");
                    }
                } else {
                    panic!("Expected Binary expression in WHERE");
                }
            }
            _ => panic!("Expected Select statement"),
        }
    }

    #[test]
    fn test_cache_stats() {
        let mut cache = StatementCache::new(10);

        // 执行多次查询
        for i in 0..100 {
            let sql = format!("SELECT * FROM t{}", i % 10); // 10 个不同 SQL，各 10 次
            cache.get_or_prepare(&sql).unwrap();
        }

        let stats = cache.stats();
        println!("{}", stats);

        assert_eq!(stats.size, 10); // 最多 10 个条目
        assert_eq!(stats.miss_count, 10); // 前 10 次是 miss
        assert_eq!(stats.hit_count, 90); // 后 90 次是 hit
        assert!((stats.hit_rate - 0.9).abs() < 0.01); // 90% 命中率
        assert!(stats.speedup_ratio > 5.0); // 加速比应该很高
    }

    #[test]
    fn test_cache_resize() {
        let mut cache = StatementCache::new(5);

        // 添加 5 个条目
        for i in 0..5 {
            cache.get_or_prepare(&format!("SELECT * FROM t{}", i)).unwrap();
        }

        assert_eq!(cache.stats().size, 5);

        // 缩小到 3
        cache.resize(3);
        assert_eq!(cache.stats().size, 3);
        assert_eq!(cache.stats().max_size, 3);
    }

    #[test]
    fn test_cache_clear() {
        let mut cache = StatementCache::new(10);

        cache.get_or_prepare("SELECT * FROM t1").unwrap();
        cache.get_or_prepare("SELECT * FROM t2").unwrap();

        let stats_before = cache.stats();
        assert_eq!(stats_before.size, 2);
        assert_eq!(stats_before.miss_count, 2);

        cache.clear();

        let stats_after = cache.stats();
        assert_eq!(stats_after.size, 0);
        assert_eq!(stats_after.hit_count, 0);
        assert_eq!(stats_after.miss_count, 0);
    }

    #[test]
    fn test_bind_params_count_mismatch() {
        let mut cache = StatementCache::new(10);
        let sql = "SELECT * FROM users WHERE id = ? AND name = ?";
        let prepared = cache.get_or_prepare(sql).unwrap();
        assert_eq!(prepared.param_count, 2);

        // 只提供 1 个参数
        let params = [Expression::Integer(42)];
        let result = bind_params(&prepared, &params);

        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Parameter count mismatch"));
    }
}
