//! 查询计划缓存 (Query Plan Cache)
//!
//! 缓存 SQL 语句到执行计划的映射，避免重复优化
//! - 针对频繁执行的相同查询进行优化
//! - 支持参数化查询的计划复用
//! - LRU 淘汰策略

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use crate::sql::ast::{SelectStmt, Expression};
use crate::executor::planner::QueryPlan;

/// 缓存的查询计划项
#[derive(Clone)]
pub struct CachedPlan {
    /// SQL 模板（可能包含占位符）
    pub sql_template: String,
    /// 解析后的 SELECT 语句（参数化形式）
    pub select_stmt: SelectStmt,
    /// 生成的执行计划
    pub plan: QueryPlan,
    /// 计划生成耗时（毫秒）
    pub plan_time_ms: f64,
    /// 计划复杂度评分（越高表示计划越复杂，缓存价值越大）
    pub complexity_score: u32,
}

/// 查询计划缓存
pub struct PlanCache {
    /// 缓存表：SQL 模板 -> 缓存计划
    cache: HashMap<String, Arc<CachedPlan>>,
    /// LRU 顺序队列
    lru_queue: std::collections::VecDeque<String>,
    /// 最大缓存数量
    max_size: usize,
    /// 是否启用缓存
    enabled: bool,
    /// 缓存命中次数
    hit_count: usize,
    /// 缓存未命中次数
    miss_count: usize,
    /// 总计划生成耗时（毫秒）
    total_plan_time_ms: f64,
    /// 节省的计划生成耗时（毫秒）
    saved_plan_time_ms: f64,
    /// 参数化查询命中次数（相同模板不同参数）
    param_hit_count: usize,
}

impl PlanCache {
    /// 创建新的计划缓存
    pub fn new(max_size: usize) -> Self {
        Self {
            cache: HashMap::with_capacity(max_size),
            lru_queue: std::collections::VecDeque::with_capacity(max_size),
            max_size,
            enabled: true,
            hit_count: 0,
            miss_count: 0,
            total_plan_time_ms: 0.0,
            saved_plan_time_ms: 0.0,
            param_hit_count: 0,
        }
    }

    /// 是否启用缓存
    pub fn enabled(&self) -> bool {
        self.enabled
    }

    /// 启用缓存
    pub fn enable(&mut self) {
        self.enabled = true;
    }

    /// 禁用缓存
    pub fn disable(&mut self) {
        self.enabled = false;
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

    /// 获取或创建查询计划
    /// 
    /// 对于参数化查询，使用归一化的 SQL 模板作为 key
    pub fn get_or_plan<F>(
        &mut self,
        sql: &str,
        select_stmt: &SelectStmt,
        plan_generator: F,
    ) -> Result<Arc<CachedPlan>, String>
    where
        F: FnOnce(&SelectStmt) -> Result<QueryPlan, String>,
    {
        // 如果缓存被禁用，直接生成计划
        if !self.enabled {
            let start = Instant::now();
            let plan = plan_generator(select_stmt)?;
            let plan_time = start.elapsed().as_secs_f64() * 1000.0;
            
            return Ok(Arc::new(CachedPlan {
                sql_template: sql.to_string(),
                select_stmt: select_stmt.clone(),
                plan,
                plan_time_ms: plan_time,
                complexity_score: 0,
            }));
        }

        // 生成归一化的 key（处理参数化查询）
        let cache_key = self.normalize_sql(sql);

        // 尝试从缓存获取
        if let Some(cached) = self.cache.get(&cache_key) {
            let cached_clone = cached.clone();
            let plan_time = cached.plan_time_ms;
            self.hit_count += 1;
            self.saved_plan_time_ms += plan_time;
            
            // 检查是否是参数化查询命中（SQL 相同但参数可能不同）
            if sql != cached.sql_template {
                self.param_hit_count += 1;
            }
            
            // 更新 LRU 顺序
            self.update_lru(&cache_key);
            return Ok(cached_clone);
        }

        // 缓存未命中，生成计划
        self.miss_count += 1;
        let start = Instant::now();
        
        let plan = plan_generator(select_stmt)?;
        let plan_time = start.elapsed().as_secs_f64() * 1000.0;
        self.total_plan_time_ms += plan_time;

        // 计算复杂度评分
        let complexity_score = self.compute_complexity_score(select_stmt, &plan);

        // 创建缓存项
        let cached_plan = Arc::new(CachedPlan {
            sql_template: cache_key.clone(),
            select_stmt: select_stmt.clone(),
            plan,
            plan_time_ms: plan_time,
            complexity_score,
        });

        // 加入缓存
        self.add_to_cache(&cache_key, cached_plan.clone());

        Ok(cached_plan)
    }

    /// 查找匹配的计划（用于参数化查询）
    /// 
    /// 返回的 plan 可能需要根据实际参数进行调整
    pub fn find_plan_for_params(&self, sql: &str) -> Option<Arc<CachedPlan>> {
        if !self.enabled {
            return None;
        }

        let cache_key = self.normalize_sql(sql);
        self.cache.get(&cache_key).cloned()
    }

    /// 归一化 SQL（提取模板用于缓存 key）
    /// 
    /// 将具体的字面值替换为占位符，使得参数化查询可以复用计划
    fn normalize_sql(&self, sql: &str) -> String {
        // 简单实现：如果 SQL 包含 ? 或 :param 等占位符，直接使用
        // 否则，尝试识别常量值并替换
        
        // 检查是否已经是参数化形式
        if sql.contains('?') || sql.contains("$") {
            return sql.to_string();
        }
        
        // 对于简单的点查，尝试提取模式
        // 例如：SELECT * FROM users WHERE id = 1 -> SELECT * FROM users WHERE id = ?
        // 这是一个简化版本，实际生产环境可能需要更复杂的 SQL 解析
        sql.to_string()
    }

    /// 计算查询复杂度评分
    /// 
    /// 复杂度越高，缓存价值越大
    fn compute_complexity_score(&self, stmt: &SelectStmt, plan: &QueryPlan) -> u32 {
        let mut score = 0u32;

        // 基于语句特征的评分
        if !stmt.joins.is_empty() {
            score += stmt.joins.len() as u32 * 10;
        }
        
        if stmt.where_clause.is_some() {
            score += 5;
        }
        
        if !stmt.group_by.is_empty() {
            score += 15;
        }
        
        if stmt.having.is_some() {
            score += 10;
        }
        
        if !stmt.order_by.is_empty() {
            score += 5;
        }

        // 基于计划类型的评分
        match plan {
            QueryPlan::FullTableScan { .. } => score += 20,
            QueryPlan::IndexScan { .. } | QueryPlan::IndexRangeScan { .. } => score += 10,
            QueryPlan::CoveringIndexScan { .. } | QueryPlan::CoveringIndexRangeScan { .. } => score += 5,
            QueryPlan::RowidPointScan { .. } => score += 2,
            QueryPlan::RowidRangeScan { .. } => score += 5,
            QueryPlan::HnswVectorScan { .. } => score += 15,
        }

        score
    }

    /// 更新 LRU 顺序
    fn update_lru(&mut self, key: &str) {
        if let Some(pos) = self.lru_queue.iter().position(|k| k == key) {
            let mut split_queue = self.lru_queue.split_off(pos);
            split_queue.pop_front();
            self.lru_queue.extend(split_queue);
        }
        self.lru_queue.push_back(key.to_string());
    }

    /// 添加到缓存（带 LRU 淘汰）
    fn add_to_cache(&mut self, key: &str, plan: Arc<CachedPlan>) {
        // 如果缓存已满，淘汰最久未使用的
        if self.cache.len() >= self.max_size && !self.cache.contains_key(key) {
            if let Some(old_key) = self.lru_queue.pop_front() {
                self.cache.remove(&old_key);
            }
        }

        self.cache.insert(key.to_string(), plan);
        self.lru_queue.push_back(key.to_string());
    }

    /// 获取缓存统计信息
    pub fn stats(&self) -> PlanCacheStats {
        let total = self.hit_count + self.miss_count;
        let hit_rate = if total > 0 {
            self.hit_count as f64 / total as f64
        } else {
            0.0
        };

        let avg_plan_time = if self.miss_count > 0 {
            self.total_plan_time_ms / self.miss_count as f64
        } else {
            0.0
        };

        PlanCacheStats {
            size: self.cache.len(),
            max_size: self.max_size,
            enabled: self.enabled,
            hit_count: self.hit_count,
            miss_count: self.miss_count,
            hit_rate,
            param_hit_count: self.param_hit_count,
            total_plan_time_ms: self.total_plan_time_ms,
            saved_plan_time_ms: self.saved_plan_time_ms,
            avg_plan_time_ms: avg_plan_time,
            speedup_ratio: if self.total_plan_time_ms > 0.0 {
                (self.total_plan_time_ms + self.saved_plan_time_ms) / self.total_plan_time_ms
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
        self.total_plan_time_ms = 0.0;
        self.saved_plan_time_ms = 0.0;
        self.param_hit_count = 0;
    }

    /// 获取缓存项数量
    pub fn len(&self) -> usize {
        self.cache.len()
    }

    /// 检查缓存是否为空
    pub fn is_empty(&self) -> bool {
        self.cache.is_empty()
    }
}

/// 计划缓存统计信息
#[derive(Debug, Clone)]
pub struct PlanCacheStats {
    pub size: usize,
    pub max_size: usize,
    pub enabled: bool,
    pub hit_count: usize,
    pub miss_count: usize,
    pub hit_rate: f64,
    /// 参数化查询命中次数
    pub param_hit_count: usize,
    /// 总计划生成耗时（毫秒）
    pub total_plan_time_ms: f64,
    /// 节省的计划生成耗时（毫秒）
    pub saved_plan_time_ms: f64,
    /// 平均计划生成耗时（毫秒）
    pub avg_plan_time_ms: f64,
    /// 加速比
    pub speedup_ratio: f64,
}

impl std::fmt::Display for PlanCacheStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Query Plan Cache Statistics:")?;
        writeln!(f, "  Status: {}", if self.enabled { "enabled" } else { "disabled" })?;
        writeln!(f, "  Size: {}/{} ({:.1}%)", 
            self.size, self.max_size, 
            self.size as f64 / self.max_size.max(1) as f64 * 100.0)?;
        writeln!(f, "  Hit Rate: {:.2}% ({} hits / {} misses)", 
            self.hit_rate * 100.0, self.hit_count, self.miss_count)?;
        writeln!(f, "  Param Hits: {} (parameterized queries)", self.param_hit_count)?;
        writeln!(f, "  Plan Time: {:.2}ms total, {:.3}ms avg", 
            self.total_plan_time_ms, self.avg_plan_time_ms)?;
        writeln!(f, "  Time Saved: {:.2}ms", self.saved_plan_time_ms)?;
        writeln!(f, "  Speedup: {:.2}x", self.speedup_ratio)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::ast::{SelectColumn, ColumnDef, DataType};

    fn create_simple_select() -> SelectStmt {
        SelectStmt {
            ctes: vec![],
            columns: vec![SelectColumn::All],
            from: "users".to_string(),
            joins: vec![],
            where_clause: None,
            group_by: vec![],
            having: None,
            order_by: vec![],
            limit: None,
            offset: None,
        }
    }

    #[test]
    fn test_plan_cache_basic() {
        let mut cache = PlanCache::new(10);
        let stmt = create_simple_select();

        // 第一次：应该 miss
        let result1 = cache.get_or_plan("SELECT * FROM users", &stmt, |_s| {
            Ok(QueryPlan::FullTableScan {
                table: "users".to_string(),
                filter: None,
                columns: vec![SelectColumn::All],
                limit: None,
            })
        });
        assert!(result1.is_ok());

        // 第二次：应该 hit
        let result2 = cache.get_or_plan("SELECT * FROM users", &stmt, |_s| {
            panic!("Should not be called - cache hit expected");
        });
        assert!(result2.is_ok());

        let stats = cache.stats();
        assert_eq!(stats.hit_count, 1);
        assert_eq!(stats.miss_count, 1);
        assert!(stats.hit_rate == 0.5);
    }

    #[test]
    fn test_plan_cache_disabled() {
        let mut cache = PlanCache::new(10);
        cache.disable();
        
        let stmt = create_simple_select();
        let mut call_count = 0;

        // 禁用缓存时，每次都应该调用生成器
        for _ in 0..3 {
            let _ = cache.get_or_plan("SELECT * FROM users", &stmt, |_s| {
                call_count += 1;
                Ok(QueryPlan::FullTableScan {
                    table: "users".to_string(),
                    filter: None,
                    columns: vec![SelectColumn::All],
                    limit: None,
                })
            });
        }

        assert_eq!(call_count, 3);
        assert_eq!(cache.stats().miss_count, 0); // 禁用时不计入 miss
    }

    #[test]
    fn test_plan_cache_lru_eviction() {
        let mut cache = PlanCache::new(2);

        let create_plan = |table: &str| QueryPlan::FullTableScan {
            table: table.to_string(),
            filter: None,
            columns: vec![SelectColumn::All],
            limit: None,
        };

        // 添加 3 个计划（超过容量）
        for i in 1..=3 {
            let sql = format!("SELECT * FROM t{}", i);
            let stmt = SelectStmt {
                ctes: vec![],
                columns: vec![SelectColumn::All],
                from: format!("t{}", i),
                joins: vec![],
                where_clause: None,
                group_by: vec![],
                having: None,
                order_by: vec![],
                limit: None,
                offset: None,
            };
            
            let table = format!("t{}", i);
            let _ = cache.get_or_plan(&sql, &stmt, |_s| Ok(create_plan(&table)));
        }

        let stats = cache.stats();
        assert_eq!(stats.size, 2); // 最多 2 个
        assert_eq!(stats.miss_count, 3);
    }

    #[test]
    fn test_plan_cache_stats() {
        let mut cache = PlanCache::new(10);
        let stmt = create_simple_select();

        // 执行多次相同查询
        for _ in 0..100 {
            let _ = cache.get_or_plan("SELECT * FROM users", &stmt, |_s| {
                Ok(QueryPlan::FullTableScan {
                    table: "users".to_string(),
                    filter: None,
                    columns: vec![SelectColumn::All],
                    limit: None,
                })
            });
        }

        let stats = cache.stats();
        println!("{}", stats);

        assert_eq!(stats.size, 1);
        assert_eq!(stats.miss_count, 1);
        assert_eq!(stats.hit_count, 99);
        assert!(stats.speedup_ratio > 50.0); // 应该有很高的加速比
    }

    #[test]
    fn test_plan_cache_clear() {
        let mut cache = PlanCache::new(10);
        let stmt = create_simple_select();

        let _ = cache.get_or_plan("SELECT * FROM t1", &stmt, |_s| {
            Ok(QueryPlan::FullTableScan {
                table: "t1".to_string(),
                filter: None,
                columns: vec![SelectColumn::All],
                limit: None,
            })
        });

        assert_eq!(cache.len(), 1);
        
        cache.clear();
        
        assert_eq!(cache.len(), 0);
        assert_eq!(cache.stats().hit_count, 0);
    }
}
