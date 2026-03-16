# V2: 自适应查询优化器 (Adaptive Query Optimizer)

## 1. 原理说明

### 1.1 当前问题

SQLite 使用**基于代价的静态优化器**：
- 编译时确定执行计划
- 基于表的统计信息（可能过时）
- 无法适应运行时数据分布变化

### 1.2 自适应优化原理

**运行时动态调整**：
```
执行计划 ──► 运行时收集反馈 ──► 动态调整策略 ──► 更优执行
    ▲                                              │
    └────────────── 持续学习优化 ◄─────────────────┘
```

**核心机制**：
1. **运行时统计**：收集实际行数、选择性
2. **计划切换**：发现更好策略时中途切换
3. **机器学习**：轻量级模型预测最优策略

### 1.3 代价模型

```
Cost = IO_Cost + CPU_Cost + Memory_Cost

IO_Cost = 页面读取数 × 磁盘延迟 (通常 10ms)
CPU_Cost = 处理行数 × 每行处理时间
Memory_Cost = 使用内存量 × 内存权重
```

### 1.4 优化策略选择

| 场景 | 传统选择 | 自适应选择 |
|------|---------|-----------|
| 小表 JOIN | Nested Loop | Hash Join (发现更快) |
| 有索引 | Index Scan | 统计发现全表更快时切换 |
| 聚合 | 排序聚合 | 小表用哈希，大表用排序 |
| 数据倾斜 | 固定计划 | 动态检测，使用倾斜优化 |

## 2. 实现方式

### 2.1 核心数据结构

```rust
/// 表统计信息
#[derive(Clone, Debug)]
pub struct TableStats {
    pub table_name: String,
    pub row_count: u64,
    pub page_count: u64,
    pub last_analyze: SystemTime,

    /// 列统计信息
    pub column_stats: HashMap<String, ColumnStats>,

    /// 索引统计
    pub index_stats: HashMap<String, IndexStats>,
}

/// 列统计信息
#[derive(Clone, Debug)]
pub struct ColumnStats {
    pub null_count: u64,
    pub distinct_count: u64,
    pub min_value: Option<Value>,
    pub max_value: Option<Value>,

    /// 直方图 (等深分桶)
    pub histogram: Option<Histogram>,

    /// 高频值 (Top-K)
    pub most_common_values: Vec<(Value, u64)>,
}

/// 直方图
#[derive(Clone, Debug)]
pub struct Histogram {
    pub buckets: Vec<Bucket>,
}

#[derive(Clone, Debug)]
pub struct Bucket {
    pub lower_bound: Value,
    pub upper_bound: Value,
    pub count: u64,
}

/// 索引统计
#[derive(Clone, Debug)]
pub struct IndexStats {
    pub index_name: String,
    pub index_height: u32,      // B-tree 高度
    pub leaf_pages: u64,        // 叶子页面数
    pub distinct_keys: u64,     // 不同键数
    pub avg_key_size: u32,      // 平均键大小
}

/// 查询计划
#[derive(Clone, Debug)]
pub struct QueryPlan {
    pub root: PlanNode,
    pub estimated_cost: f64,
    pub estimated_rows: u64,
}

/// 计划节点
#[derive(Clone, Debug)]
pub enum PlanNode {
    Scan {
        table: String,
        index: Option<String>,  // None = 全表扫描
        predicates: Vec<Expression>,
    },
    Filter {
        child: Box<PlanNode>,
        predicate: Expression,
        selectivity: f64,  // 选择性估计 (0-1)
    },
    Join {
        join_type: JoinType,
        left: Box<PlanNode>,
        right: Box<PlanNode>,
        condition: Expression,
        strategy: JoinStrategy,
    },
    Aggregate {
        child: Box<PlanNode>,
        group_by: Vec<Expression>,
        aggregates: Vec<AggregateExpr>,
        strategy: AggregateStrategy,
    },
    Sort {
        child: Box<PlanNode>,
        keys: Vec<SortKey>,
        limit: Option<usize>,
    },
    Limit {
        child: Box<PlanNode>,
        limit: usize,
        offset: usize,
    },
}

#[derive(Clone, Copy, Debug)]
pub enum JoinStrategy {
    NestedLoop,      // 嵌套循环
    HashJoin,        // 哈希连接
    MergeJoin,       // 归并连接
    IndexNestedLoop, // 索引嵌套循环
}

#[derive(Clone, Copy, Debug)]
pub enum AggregateStrategy {
    Hash,       // 哈希聚合 (内存中)
    Sort,       // 排序聚合 (磁盘友好)
    Stream,     // 流式聚合 (已排序输入)
}
```

### 2.2 统计信息收集器

```rust
/// 统计信息收集器
pub struct StatisticsCollector {
    catalog: Arc<RwLock<Catalog>>,

    /// 采样率 (收集统计时读取的行比例)
    sample_rate: f64,
}

impl StatisticsCollector {
    /// 分析表，收集统计信息
    pub async fn analyze_table(
        &self,
        table_name: &str,
    ) -> Result<TableStats> {
        let table = self.catalog.read().await.get_table(table_name)?;

        // 1. 计算总行数
        let row_count = self.count_rows(table).await?;

        // 2. 采样收集列统计
        let mut column_stats = HashMap::new();
        for column in &table.columns {
            let stats = self.analyze_column(table, column, row_count).await?;
            column_stats.insert(column.name.clone(), stats);
        }

        // 3. 收集索引统计
        let index_stats = self.analyze_indexes(table).await?;

        Ok(TableStats {
            table_name: table_name.to_string(),
            row_count,
            page_count: table.page_count(),
            last_analyze: SystemTime::now(),
            column_stats,
            index_stats,
        })
    }

    async fn analyze_column(
        &self,
        table: &Table,
        column: &Column,
        total_rows: u64,
    ) -> Result<ColumnStats> {
        let sample_size = (total_rows as f64 * self.sample_rate) as u64;

        // 采样
        let samples = self.sample_column(table, column, sample_size).await?;

        // 计算统计值
        let null_count = samples.iter().filter(|v| v.is_null()).count() as u64;
        let distinct_values: HashSet<_> = samples.iter().cloned().collect();
        let distinct_count = distinct_values.len() as u64;

        // 排序后计算 min/max
        let mut sorted = samples.clone();
        sorted.sort();
        let min_value = sorted.first().cloned();
        let max_value = sorted.last().cloned();

        // 构建直方图 (等深分桶)
        let histogram = if samples.len() >= 100 {
            Some(self.build_histogram(&sorted, 100))
        } else {
            None
        };

        // 高频值 (Top-10)
        let most_common_values = self.find_most_common(&samples, 10);

        Ok(ColumnStats {
            null_count,
            distinct_count,
            min_value,
            max_value,
            histogram,
            most_common_values,
        })
    }

    fn build_histogram(&self, sorted_values: &[Value], num_buckets: usize) -> Histogram {
        let bucket_size = sorted_values.len() / num_buckets;
        let mut buckets = Vec::new();

        for i in 0..num_buckets {
            let start = i * bucket_size;
            let end = if i == num_buckets - 1 {
                sorted_values.len()
            } else {
                (i + 1) * bucket_size
            };

            buckets.push(Bucket {
                lower_bound: sorted_values[start].clone(),
                upper_bound: sorted_values[end - 1].clone(),
                count: (end - start) as u64,
            });
        }

        Histogram { buckets }
    }
}
```

### 2.3 代价估计器

```rust
/// 代价估计器
pub struct CostEstimator {
    stats: Arc<RwLock<HashMap<String, TableStats>>>,

    /// 系统参数
    config: CostConfig,
}

#[derive(Clone, Debug)]
pub struct CostConfig {
    /// 顺序 IO 代价 (每页)
    pub seq_page_cost: f64,
    /// 随机 IO 代价 (每页)
    pub random_page_cost: f64,
    /// CPU 处理每行代价
    pub cpu_tuple_cost: f64,
    /// CPU 处理每个索引条目代价
    pub cpu_index_tuple_cost: f64,
    /// 比较操作代价
    pub cpu_operator_cost: f64,
}

impl Default for CostConfig {
    fn default() -> Self {
        Self {
            seq_page_cost: 1.0,
            random_page_cost: 4.0,
            cpu_tuple_cost: 0.01,
            cpu_index_tuple_cost: 0.005,
            cpu_operator_cost: 0.0025,
        }
    }
}

impl CostEstimator {
    /// 估计计划代价
    pub fn estimate_cost(&self, plan: &PlanNode) -> f64 {
        match plan {
            PlanNode::Scan { table, index, .. } => {
                self.estimate_scan_cost(table, index.as_deref())
            }
            PlanNode::Filter { child, selectivity, .. } => {
                let child_cost = self.estimate_cost(child);
                let input_rows = self.estimate_rows(child);
                let output_rows = (input_rows as f64 * selectivity) as u64;

                child_cost + (input_rows as f64 * self.config.cpu_operator_cost)
            }
            PlanNode::Join { left, right, strategy, .. } => {
                self.estimate_join_cost(left, right, *strategy)
            }
            PlanNode::Aggregate { child, strategy, .. } => {
                self.estimate_aggregate_cost(child, *strategy)
            }
            PlanNode::Sort { child, .. } => {
                let child_cost = self.estimate_cost(child);
                let rows = self.estimate_rows(child);
                // 排序代价: O(n log n)
                child_cost + (rows as f64) * (rows as f64).log2() * self.config.cpu_tuple_cost
            }
            PlanNode::Limit { child, limit, .. } => {
                let child_cost = self.estimate_cost(child);
                // Limit 可以截断子节点执行
                child_cost * (*limit as f64 / self.estimate_rows(child) as f64)
            }
        }
    }

    fn estimate_scan_cost(&self, table_name: &str, index_name: Option<&str>) -> f64 {
        let stats = self.stats.read().unwrap();
        let table_stats = stats.get(table_name)?;

        match index_name {
            None => {
                // 全表扫描
                table_stats.page_count as f64 * self.config.seq_page_cost
                    + table_stats.row_count as f64 * self.config.cpu_tuple_cost
            }
            Some(idx_name) => {
                // 索引扫描
                let idx_stats = table_stats.index_stats.get(idx_name)?;
                let index_pages = idx_stats.leaf_pages + idx_stats.index_height as u64;

                index_pages as f64 * self.config.random_page_cost
                    + table_stats.row_count as f64 * self.config.cpu_index_tuple_cost
            }
        }
    }

    fn estimate_join_cost(
        &self,
        left: &PlanNode,
        right: &PlanNode,
        strategy: JoinStrategy,
    ) -> f64 {
        let left_cost = self.estimate_cost(left);
        let right_cost = self.estimate_cost(right);
        let left_rows = self.estimate_rows(left);
        let right_rows = self.estimate_rows(right);

        match strategy {
            JoinStrategy::NestedLoop => {
                left_cost + left_rows as f64 * right_cost
            }
            JoinStrategy::HashJoin => {
                // 哈希连接: 构建哈希表 + 探测
                left_cost + right_cost
                    + (left_rows + right_rows) as f64 * self.config.cpu_tuple_cost
            }
            JoinStrategy::MergeJoin => {
                // 归并连接: 需要排序输入
                let sort_cost = (left_rows + right_rows) as f64
                    * ((left_rows + right_rows) as f64).log2()
                    * self.config.cpu_tuple_cost;
                left_cost + right_cost + sort_cost
            }
            JoinStrategy::IndexNestedLoop => {
                // 索引嵌套循环
                left_cost + left_rows as f64 * self.config.random_page_cost
            }
        }
    }

    /// 估计选择性 (谓词过滤后的行数比例)
    pub fn estimate_selectivity(
        &self,
        predicate: &Expression,
        table: &str,
    ) -> f64 {
        match predicate {
            Expression::Binary { left, op, right } => {
                match op {
                    BinaryOp::Equal => {
                        // 等值选择性 = 1 / distinct_count
                        if let Expression::Column(col) = left.as_ref() {
                            if let Some(stats) = self.get_column_stats(table, col) {
                                return 1.0 / stats.distinct_count.max(1) as f64;
                            }
                        }
                        0.1 // 默认值
                    }
                    BinaryOp::LessThan | BinaryOp::GreaterThan => {
                        // 范围选择性 = 1/3 (假设均匀分布)
                        0.33
                    }
                    BinaryOp::And => {
                        // AND: 选择性相乘
                        let left_sel = self.estimate_selectivity(left, table);
                        let right_sel = self.estimate_selectivity(right, table);
                        left_sel * right_sel
                    }
                    BinaryOp::Or => {
                        // OR: 选择性相加 (上限 1)
                        let left_sel = self.estimate_selectivity(left, table);
                        let right_sel = self.estimate_selectivity(right, table);
                        (left_sel + right_sel - left_sel * right_sel).min(1.0)
                    }
                    _ => 0.5,
                }
            }
            _ => 0.5, // 默认值
        }
    }
}
```

### 2.4 计划生成器

```rust
/// 计划生成器 (使用动态规划)
pub struct PlanGenerator {
    estimator: CostEstimator,
}

impl PlanGenerator {
    /// 为查询生成最优计划
    pub fn generate_plan(&self, query: &Query) -> Result<QueryPlan> {
        // 1. 枚举所有可能的扫描路径
        let scan_plans = self.enumerate_scan_plans(&query.tables, &query.predicates)?;

        // 2. 动态规划生成 JOIN 计划
        let join_plan = self.optimize_joins(
            &query.tables,
            &scan_plans,
            &query.join_conditions,
        )?;

        // 3. 添加其他操作
        let mut plan = join_plan;

        if let Some(ref having) = query.having {
            plan = self.add_filter(plan, having.clone());
        }

        if !query.aggregates.is_empty() || !query.group_by.is_empty() {
            plan = self.add_aggregate(plan, &query.group_by, &query.aggregates)?;
        }

        if !query.order_by.is_empty() {
            plan = self.add_sort(plan, &query.order_by, query.limit)?;
        }

        if let Some(limit) = query.limit {
            plan = self.add_limit(plan, limit, query.offset);
        }

        let cost = self.estimator.estimate_cost(&plan);
        let rows = self.estimator.estimate_rows(&plan);

        Ok(QueryPlan {
            root: plan,
            estimated_cost: cost,
            estimated_rows: rows,
        })
    }

    /// 动态规划优化 JOIN 顺序
    fn optimize_joins(
        &self,
        tables: &[TableRef],
        scan_plans: &HashMap<String, PlanNode>,
        join_conditions: &[JoinCondition],
    ) -> Result<PlanNode> {
        let n = tables.len();

        // dp[subset] = 最优计划
        let mut dp: HashMap<u64, PlanNode> = HashMap::new();

        // 初始化: 单表
        for (i, table) in tables.iter().enumerate() {
            let mask = 1u64 << i;
            dp.insert(mask, scan_plans[&table.name].clone());
        }

        // 动态规划: 从小到大构建
        for size in 2..=n {
            for subset in self.subsets_of_size(n, size) {
                let mut best_plan: Option<PlanNode> = None;
                let mut best_cost = f64::INFINITY;

                // 尝试所有可能的划分
                for left_mask in self.subsets(subset) {
                    if left_mask == 0 || left_mask == subset {
                        continue;
                    }
                    let right_mask = subset - left_mask;

                    if let (Some(left_plan), Some(right_plan)) =
                        (dp.get(&left_mask), dp.get(&right_mask))
                    {
                        // 尝试不同的 JOIN 策略
                        for strategy in [
                            JoinStrategy::HashJoin,
                            JoinStrategy::NestedLoop,
                            JoinStrategy::MergeJoin,
                        ] {
                            let join_plan = PlanNode::Join {
                                join_type: JoinType::Inner,
                                left: Box::new(left_plan.clone()),
                                right: Box::new(right_plan.clone()),
                                condition: self.find_join_condition(
                                    join_conditions,
                                    left_mask,
                                    right_mask,
                                ),
                                strategy,
                            };

                            let cost = self.estimator.estimate_cost(&join_plan);
                            if cost < best_cost {
                                best_cost = cost;
                                best_plan = Some(join_plan);
                            }
                        }
                    }
                }

                if let Some(plan) = best_plan {
                    dp.insert(subset, plan);
                }
            }
        }

        // 返回完整计划
        let full_mask = (1u64 << n) - 1;
        dp.get(&full_mask)
            .cloned()
            .ok_or_else(|| Error::PlanGenerationFailed)
    }

    /// 为聚合选择最优策略
    fn add_aggregate(
        &self,
        child: PlanNode,
        group_by: &[Expression],
        aggregates: &[AggregateExpr],
    ) -> Result<PlanNode> {
        let child_rows = self.estimator.estimate_rows(&child);

        // 选择聚合策略
        let strategy = if group_by.is_empty() {
            // 无 GROUP BY: 简单聚合
            AggregateStrategy::Hash
        } else {
            let estimated_groups = self.estimate_group_count(&child, group_by);

            if estimated_groups < 10000 {
                // 小分组数: 哈希聚合
                AggregateStrategy::Hash
            } else {
                // 大分组数: 排序聚合 (内存友好)
                AggregateStrategy::Sort
            }
        };

        Ok(PlanNode::Aggregate {
            child: Box::new(child),
            group_by: group_by.to_vec(),
            aggregates: aggregates.to_vec(),
            strategy,
        })
    }
}
```

### 2.5 自适应执行器

```rust
/// 自适应执行器 - 运行时调整
pub struct AdaptiveExecutor {
    plan: QueryPlan,

    /// 运行时收集的统计
    runtime_stats: RuntimeStats,

    /// 是否允许重新优化
    allow_reoptimization: bool,
}

#[derive(Default)]
pub struct RuntimeStats {
    /// 实际扫描行数
    pub actual_rows: HashMap<PlanNodeId, u64>,

    /// 实际执行时间
    pub execution_time: HashMap<PlanNodeId, Duration>,

    /// 缓存命中率
    pub cache_hit_rate: f64,
}

impl AdaptiveExecutor {
    pub async fn execute(&mut self) -> Result<ResultSet> {
        self.execute_node(&self.plan.root.clone()).await
    }

    async fn execute_node(&mut self, node: &PlanNode) -> Result<ResultSet> {
        let start = Instant::now();

        let result = match node {
            PlanNode::Join { left, right, strategy, .. } => {
                // 运行时 JOIN 策略切换
                let actual_strategy = self.choose_join_strategy_at_runtime(left, right, *strategy);

                match actual_strategy {
                    JoinStrategy::HashJoin => {
                        self.execute_hash_join(left, right).await
                    }
                    JoinStrategy::NestedLoop => {
                        self.execute_nested_loop_join(left, right).await
                    }
                    _ => self.execute_join(left, right, *strategy).await,
                }
            }
            PlanNode::Aggregate { child, strategy, .. } => {
                // 运行时聚合策略切换
                let child_rows = self.estimate_runtime_rows(child).await?;

                let actual_strategy = if child_rows > 100000 && *strategy == AggregateStrategy::Hash {
                    // 内存压力大，切换到排序聚合
                    AggregateStrategy::Sort
                } else {
                    *strategy
                };

                self.execute_aggregate(child, actual_strategy).await
            }
            _ => self.execute_standard(node).await,
        }?;

        // 记录运行时统计
        let elapsed = start.elapsed();
        self.runtime_stats.actual_rows.insert(node.id(), result.row_count() as u64);
        self.runtime_stats.execution_time.insert(node.id(), elapsed);

        Ok(result)
    }

    fn choose_join_strategy_at_runtime(
        &self,
        left: &PlanNode,
        right: &PlanNode,
        planned: JoinStrategy,
    ) -> JoinStrategy {
        // 如果实际数据分布与估计相差很大，切换策略
        let left_rows = self.runtime_stats.actual_rows.get(&left.id()).copied();
        let right_rows = self.runtime_stats.actual_rows.get(&right.id()).copied();

        if let (Some(l), Some(r)) = (left_rows, right_rows) {
            if planned == JoinStrategy::HashJoin && l * r > 10000000 {
                // 哈希表太大，使用嵌套循环
                return JoinStrategy::NestedLoop;
            }
        }

        planned
    }
}
```

## 3. Rust 实现方式

### 3.1 自己实现的部分

| 组件 | 实现方式 | 原因 |
|------|---------|------|
| 统计信息收集器 | 自己实现 | 深度集成存储层 |
| 代价模型 | 自己实现 | 可定制化参数 |
| 计划生成器 | 自己实现 | 动态规划算法 |
| 选择性估计 | 自己实现 | 直方图等结构自定义 |
| 自适应执行器 | 自己实现 | 运行时决策逻辑 |

### 3.2 使用的第三方库

```toml
[dependencies]
# 数据分析 (直方图等)
datafusion-expr = "35"  # 可选，如果要用 DataFusion 的表达式系统

# 无主要依赖 - 自己实现核心算法
```

**推荐：自己实现**
- 查询优化器是数据库核心
- 需要与现有执行器深度集成
- 算法相对固定（动态规划）

### 3.3 代码结构

```
src/
├── optimizer/
│   ├── mod.rs
│   ├── statistics/         # 统计信息
│   │   ├── mod.rs
│   │   ├── collector.rs
│   │   ├── histogram.rs
│   │   └── catalog.rs
│   ├── cost/               # 代价模型
│   │   ├── mod.rs
│   │   ├── estimator.rs
│   │   └── config.rs
│   ├── plan/               # 执行计划
│   │   ├── mod.rs
│   │   ├── node.rs
│   │   └── builder.rs
│   ├── generator.rs        # 计划生成器
│   ├── selectivity.rs      # 选择性估计
│   └── adaptive.rs         # 自适应执行
```

## 4. 验证方法

### 4.1 单元测试

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_selectivity_estimation() {
        let estimator = CostEstimator::new(mock_stats());

        // 等值选择性
        let eq_expr = parse("id = 100");
        let sel = estimator.estimate_selectivity(&eq_expr, "users");
        assert!((sel - 0.001).abs() < 0.01); // 1000 不同值

        // 范围选择性
        let range_expr = parse("age > 18");
        let sel = estimator.estimate_selectivity(&range_expr, "users");
        assert!((sel - 0.33).abs() < 0.1);
    }

    #[test]
    fn test_join_order_optimization() {
        let generator = PlanGenerator::new(mock_estimator());

        // 3 表 JOIN
        let query = Query {
            tables: vec!["A", "B", "C"],
            ..Default::default()
        };

        let plan = generator.generate_plan(&query).unwrap();

        // 验证选择了最优顺序
        // 小表优先 JOIN
    }

    #[test]
    fn test_index_selection() {
        let generator = PlanGenerator::new(mock_estimator());

        let query = parse("SELECT * FROM users WHERE id = 1");
        let plan = generator.generate_plan(&query).unwrap();

        // 应该使用索引扫描
        if let PlanNode::Scan { index: Some(idx), .. } = &plan.root {
            assert_eq!(idx, "users_id_idx");
        } else {
            panic!("Should use index scan");
        }
    }
}
```

### 4.2 性能基准测试

```rust
#[cfg(bench)]
mod benches {
    use criterion::*;

    fn bench_join_order(c: &mut Criterion) {
        let mut group = c.benchmark_group("join_order");

        // 6 表 JOIN - 优化器应该优于随机顺序
        group.bench_function("optimized_6way_join", |b| {
            let db = setup_6_tables();
            b.iter(|| {
                db.query("SELECT * FROM A JOIN B ON ... JOIN C ON ...")
            });
        });

        group.finish();
    }

    fn bench_adaptive_execution(c: &mut Criterion) {
        let mut group = c.benchmark_group("adaptive");

        group.bench_function("static_plan", |b| {
            let db = setup_with_skewed_data();
            b.iter(|| {
                db.query_with_static_plan("SELECT ...")
            });
        });

        group.bench_function("adaptive_plan", |b| {
            let db = setup_with_skewed_data();
            b.iter(|| {
                db.query_with_adaptive_plan("SELECT ...")
            });
        });

        group.finish();
    }
}
```

### 4.3 验证指标

| 指标 | 当前基线 | V2 目标 | 验证方法 |
|------|---------|--------|---------|
| JOIN 顺序优化 | 随机顺序 | 最优顺序 | 执行时间对比 |
| 索引选择准确率 | - | > 95% | 人工检查计划 |
| 代价估计误差 | - | < 50% | 估计 vs 实际 |
| 自适应切换收益 | 无 | > 20% | 静态 vs 自适应 |
| 计划生成时间 | - | < 10ms | 计时 |

## 5. 实施计划

### Week 1
- [ ] 实现统计信息收集器
- [ ] 实现直方图和 Top-K
- [ ] 单元测试

### Week 2
- [ ] 实现代价估计器
- [ ] 实现选择性估计
- [ ] 实现计划生成器 (动态规划)
- [ ] 索引选择逻辑
- [ ] 性能基准测试

## 6. 关键算法复杂度

| 算法 | 时间复杂度 | 空间复杂度 | 说明 |
|------|-----------|-----------|------|
| 统计收集 | O(n × sample_rate) | O(n) | 采样扫描 |
| JOIN 优化 (DP) | O(3^n) | O(2^n) | n ≤ 10 可行 |
| 选择性估计 | O(log k) | O(k) | k = 桶数 |
| 计划生成 | O(3^n × m) | O(2^n) | m = 策略数 |
