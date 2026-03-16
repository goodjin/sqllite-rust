# V1: 向量化执行引擎 (Vectorized Execution Engine)

## 1. 原理说明

### 1.1 当前问题

SQLite 和传统数据库使用**火山模型 (Volcano Model)**：
```
Iterator {
    next() -> Row
}
```
- 每行一次函数调用开销
- 缓存不友好（跳转随机）
- 无法使用 SIMD 加速

### 1.2 向量化模型

一次处理一批数据（通常 1024 行）：
```
VectorizedOperator {
    next_batch() -> ColumnVector[1024]
}
```
- 摊销函数调用开销
- 缓存局部性好
- 可使用 SIMD 指令并行计算

### 1.3 SIMD 加速原理

现代 CPU 支持 AVX2 (256位) / AVX-512 (512位) 指令：
- 一次处理 4 个 i64 (AVX2)
- 一次处理 8 个 i64 (AVX-512)
- 理论加速比 4-8x

## 2. 实现方式

### 2.1 核心数据结构

```rust
/// 列向量 - 批量存储同类型数据
pub struct ColumnVector {
    /// 数据缓冲区
    data: Vec<Value>,
    /// NULL 位图 (每行 1 bit)
    null_bitmap: Vec<u64>,
    /// 有效行数 (<= BATCH_SIZE)
    len: usize,
}

/// 批量行数据
pub struct Batch {
    /// 每列一个向量
    columns: Vec<ColumnVector>,
    /// 行数
    row_count: usize,
}

/// 向量化算子 trait
pub trait VectorizedOperator {
    fn next_batch(&mut self) -> Result<Batch>;
    fn reset(&mut self);
}
```

### 2.2 向量化算子实现

```rust
/// 向量化扫描算子
pub struct VectorizedScan {
    table: String,
    cursor: TableCursor,
    batch_size: usize,
}

impl VectorizedOperator for VectorizedScan {
    fn next_batch(&mut self) -> Result<Batch> {
        let mut batch = Batch::new(self.batch_size);

        for i in 0..self.batch_size {
            match self.cursor.next()? {
                Some(row) => batch.push(row),
                None => break,
            }
        }

        Ok(batch)
    }
}

/// 向量化过滤器
pub struct VectorizedFilter {
    child: Box<dyn VectorizedOperator>,
    predicate: Expression,
    /// 编译后的谓词函数
    compiled_pred: Option<CompiledPredicate>,
}

impl VectorizedOperator for VectorizedFilter {
    fn next_batch(&mut self) -> Result<Batch> {
        let mut batch = self.child.next_batch()?;

        // 向量化过滤
        let mut selection_vector: Vec<usize> = Vec::new();

        for i in 0..batch.row_count {
            if self.eval_predicate(&batch, i) {
                selection_vector.push(i);
            }
        }

        // 压缩 batch (保留选中的行)
        batch.compact(&selection_vector);
        Ok(batch)
    }
}

/// 向量化聚合算子
pub struct VectorizedAggregate {
    child: Box<dyn VectorizedOperator>,
    aggregates: Vec<AggregateExpr>,
    /// 哈希表 (用于 GROUP BY)
    hash_table: HashMap<Vec<Value>, Vec<Accumulator>>,
    /// 是否无 GROUP BY
    scalar_mode: bool,
    /// 标量累加器
    scalar_accumulators: Vec<Accumulator>,
}

impl VectorizedOperator for VectorizedAggregate {
    fn next_batch(&mut self) -> Result<Batch> {
        // 消费所有输入
        loop {
            let batch = self.child.next_batch()?;
            if batch.row_count == 0 {
                break;
            }
            self.update_aggregates(&batch)?;
        }

        // 返回结果
        self.build_result_batch()
    }

    fn update_aggregates(&mut self, batch: &Batch) -> Result<()> {
        if self.scalar_mode {
            // 无 GROUP BY: 直接累加
            for (i, agg) in self.aggregates.iter().enumerate() {
                let acc = &mut self.scalar_accumulators[i];
                acc.update_batch(batch, &agg.expr)?;
            }
        } else {
            // 有 GROUP BY: 哈希聚合
            for row_idx in 0..batch.row_count {
                let group_key = self.compute_group_key(batch, row_idx);
                let entry = self.hash_table.entry(group_key).or_insert_with(|| {
                    self.aggregates.iter().map(|a| a.create_accumulator()).collect()
                });

                for (i, acc) in entry.iter_mut().enumerate() {
                    acc.update_row(batch, row_idx)?;
                }
            }
        }
        Ok(())
    }
}
```

### 2.3 SIMD 加速实现

```rust
#[cfg(target_arch = "x86_64")]
pub mod simd {
    use std::arch::x86_64::*;

    /// AVX2 加速的整数加法
    pub unsafe fn sum_i64_avx2(values: &[i64]) -> i64 {
        let mut sum = _mm256_setzero_si256();
        let chunks = values.chunks_exact(4);
        let remainder = chunks.remainder();

        for chunk in chunks {
            let a = _mm256_loadu_si256(chunk.as_ptr() as *const __m256i);
            sum = _mm256_add_epi64(sum, a);
        }

        // 水平求和
        let hi = _mm256_extracti128_si256(sum, 1);
        let lo = _mm256_castsi256_si128(sum);
        let sum128 = _mm_add_epi64(hi, lo);
        let result = _mm_extract_epi64(sum128, 0) + _mm_extract_epi64(sum128, 1);

        // 处理剩余
        result + remainder.iter().sum::<i64>()
    }

    /// AVX2 加速的比较 (用于过滤)
    pub unsafe fn compare_eq_i64_avx2(a: &[i64], b: i64, result: &mut [bool]) {
        let b_vec = _mm256_set1_epi64x(b);

        for (i, chunk) in a.chunks_exact(4).enumerate() {
            let a_vec = _mm256_loadu_si256(chunk.as_ptr() as *const __m256i);
            let eq = _mm256_cmpeq_epi64(a_vec, b_vec);
            let mask = _mm256_movemask_pd(_mm256_castsi256_pd(eq));

            result[i * 4] = (mask & 1) != 0;
            result[i * 4 + 1] = (mask & 2) != 0;
            result[i * 4 + 2] = (mask & 4) != 0;
            result[i * 4 + 3] = (mask & 8) != 0;
        }
    }

    /// 检查 CPU 是否支持 AVX2
    pub fn has_avx2() -> bool {
        is_x86_feature_detected!("avx2")
    }
}
```

### 2.4 累加器实现

```rust
/// 聚合累加器 trait
pub trait Accumulator: Send + Sync {
    fn update(&mut self, value: &Value) -> Result<()>;
    fn update_batch(&mut self, batch: &Batch, expr: &Expression) -> Result<()> {
        // 默认实现：逐行处理
        for i in 0..batch.row_count {
            let value = self.eval_expr(batch, i, expr)?;
            self.update(&value)?;
        }
        Ok(())
    }
    fn finalize(&self) -> Value;
    fn clone_box(&self) -> Box<dyn Accumulator>;
}

/// COUNT 累加器
pub struct CountAccumulator {
    count: i64,
}

impl Accumulator for CountAccumulator {
    fn update(&mut self, _value: &Value) -> Result<()> {
        self.count += 1;
        Ok(())
    }

    fn finalize(&self) -> Value {
        Value::Integer(self.count)
    }

    fn clone_box(&self) -> Box<dyn Accumulator> {
        Box::new(Self { count: self.count })
    }
}

/// SUM 累加器 (支持 SIMD)
pub struct SumAccumulator {
    sum: i64,
    partial_sums: Vec<i64>, // 用于向量化
}

impl Accumulator for SumAccumulator {
    fn update(&mut self, value: &Value) -> Result<()> {
        if let Value::Integer(n) = value {
            self.sum += n;
        }
        Ok(())
    }

    fn update_batch(&mut self, batch: &Batch, expr: &Expression) -> Result<()> {
        // 提取列数据
        let values = self.extract_i64_column(batch, expr)?;

        // SIMD 加速求和
        if simd::has_avx2() && values.len() >= 4 {
            unsafe {
                self.sum += simd::sum_i64_avx2(&values);
            }
        } else {
            self.sum += values.iter().sum::<i64>();
        }

        Ok(())
    }

    fn finalize(&self) -> Value {
        Value::Integer(self.sum)
    }

    fn clone_box(&self) -> Box<dyn Accumulator> {
        Box::new(Self {
            sum: self.sum,
            partial_sums: self.partial_sums.clone(),
        })
    }
}
```

## 3. Rust 实现方式

### 3.1 自己实现的部分

| 组件 | 实现方式 | 原因 |
|------|---------|------|
| ColumnVector | 自己实现 | 紧密集成 Value 类型 |
| Batch | 自己实现 | 自定义内存布局 |
| VectorizedOperator trait | 自己实现 | 定义算子接口 |
| 各种算子 (Scan/Filter/Aggregate) | 自己实现 | 核心逻辑 |
| Accumulator | 自己实现 | 聚合语义自定义 |
| SIMD wrapper | 自己实现 | 安全封装 intrinsics |

### 3.2 使用的第三方库

```toml
[dependencies]
# SIMD 安全抽象 (可选，可以自己封装)
# packed_simd = "0.3"  # 如果使用第三方库

# 无 - 主要使用 std::arch 中的 intrinsics
```

**推荐：自己实现 SIMD 封装**
- Rust 标准库提供 `std::arch::*` intrinsics
- 自己封装可以更好控制行为和错误处理
- 避免引入不必要的依赖

### 3.3 代码结构

```
src/
├── executor/
│   ├── mod.rs              # 原有执行器
│   └── vectorized/         # 新增向量化模块
│       ├── mod.rs
│       ├── batch.rs        # Batch/ColumnVector
│       ├── operator.rs     # VectorizedOperator trait
│       ├── operators/      # 各种算子实现
│       │   ├── scan.rs
│       │   ├── filter.rs
│       │   ├── project.rs
│       │   ├── aggregate.rs
│       │   └── join.rs
│       ├── accumulator.rs  # 聚合累加器
│       └── simd.rs         # SIMD 加速模块
```

## 4. 验证方法

### 4.1 单元测试

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_column_vector_basic() {
        let mut cv = ColumnVector::with_capacity(1024);
        cv.push(Value::Integer(1));
        cv.push(Value::Integer(2));
        cv.push(Value::Null);

        assert_eq!(cv.len(), 3);
        assert!(cv.is_null(2));
        assert_eq!(cv.get(0), Some(&Value::Integer(1)));
    }

    #[test]
    fn test_batch_operations() {
        let mut batch = Batch::new(4);
        batch.add_column("id", DataType::Integer);
        batch.add_column("name", DataType::Text);

        batch.push_row(vec![Value::Integer(1), Value::Text("Alice".to_string())]);
        batch.push_row(vec![Value::Integer(2), Value::Text("Bob".to_string())]);

        assert_eq!(batch.row_count, 2);
        assert_eq!(batch.columns.len(), 2);
    }

    #[test]
    fn test_vectorized_scan() {
        // 创建测试表
        let db = create_test_db();
        db.execute("CREATE TABLE t (id INTEGER)").unwrap();
        for i in 0..100 {
            db.execute(&format!("INSERT INTO t VALUES ({)", i)).unwrap();
        }

        // 测试向量化扫描
        let mut scan = VectorizedScan::new("t", 16); // batch_size = 16

        let mut total_rows = 0;
        loop {
            let batch = scan.next_batch().unwrap();
            if batch.row_count == 0 {
                break;
            }
            total_rows += batch.row_count;
        }

        assert_eq!(total_rows, 100);
    }

    #[test]
    fn test_vectorized_aggregate() {
        let db = create_test_db();
        db.execute("CREATE TABLE t (x INTEGER)").unwrap();
        for i in 1..=100 {
            db.execute(&format!("INSERT INTO t VALUES ({)", i)).unwrap();
        }

        let mut scan = VectorizedScan::new("t", 32);
        let mut agg = VectorizedAggregate::new(
            Box::new(scan),
            vec![AggregateExpr::new(AggregateFunc::Sum, col("x"))],
        );

        let result = agg.next_batch().unwrap();
        assert_eq!(result.row_count, 1);
        assert_eq!(result.columns[0].get(0), Some(&Value::Integer(5050)));
    }

    #[cfg(target_arch = "x86_64")]
    #[test]
    fn test_simd_sum() {
        use super::simd::*;

        let values: Vec<i64> = (1..=1000).map(|i| i).collect();

        // 标量求和
        let scalar_sum: i64 = values.iter().sum();

        // SIMD 求和
        let simd_sum = unsafe { sum_i64_avx2(&values) };

        assert_eq!(scalar_sum, simd_sum);
    }
}
```

### 4.2 性能基准测试

```rust
#[cfg(bench)]
mod benches {
    use criterion::{black_box, criterion_group, criterion_main, Criterion};
    use super::*;

    fn bench_aggregate_scalar_vs_vectorized(c: &mut Criterion) {
        let mut group = c.benchmark_group("aggregate");

        // 准备数据
        let data: Vec<i64> = (1..=1_000_000).collect();

        group.bench_function("scalar_sum", |b| {
            b.iter(|| {
                let sum: i64 = data.iter().sum();
                black_box(sum);
            });
        });

        group.bench_function("vectorized_sum_avx2", |b| {
            b.iter(|| {
                let sum = unsafe { simd::sum_i64_avx2(&data) };
                black_box(sum);
            });
        });

        group.finish();
    }

    fn bench_sql_aggregate(c: &mut Criterion) {
        let mut group = c.benchmark_group("sql_aggregate");

        // 创建测试数据库
        let db = create_test_db_with_data(100_000);

        group.bench_function("sqlite_sum", |b| {
            b.iter(|| {
                let result: i64 = sqlite_query("SELECT SUM(x) FROM t");
                black_box(result);
            });
        });

        group.bench_function("sqllite_vectorized_sum", |b| {
            b.iter(|| {
                let result = db.query_vectorized("SELECT SUM(x) FROM t");
                black_box(result);
            });
        });

        group.finish();
    }

    criterion_group!(benches, bench_aggregate_scalar_vs_vectorized, bench_sql_aggregate);
    criterion_main!(benches);
}
```

### 4.3 验证指标

| 指标 | 当前基线 | V1 目标 | 验证方法 |
|------|---------|--------|---------|
| SUM 100万行 | 100ms | < 20ms (5x) | criterion 基准测试 |
| AVG 100万行 | 120ms | < 24ms (5x) | criterion 基准测试 |
| COUNT | 50ms | < 10ms (5x) | criterion 基准测试 |
| 缓存命中率 | - | > 90% | perf cache-misses |
| SIMD 使用比例 | 0% | > 80% | 代码审查 |

## 5. 实施计划

### Week 1
- [ ] 实现 ColumnVector 和 Batch
- [ ] 实现 VectorizedOperator trait
- [ ] 实现 VectorizedScan
- [ ] 基础单元测试

### Week 2
- [ ] 实现 VectorizedFilter
- [ ] 实现 VectorizedAggregate
- [ ] 实现 Accumulator (COUNT, SUM, AVG, MIN, MAX)
- [ ] SIMD 加速 (AVX2)
- [ ] 性能基准测试
- [ ] 与 SQLite 对比测试

## 6. 风险评估

| 风险 | 可能性 | 影响 | 缓解措施 |
|------|--------|------|---------|
| SIMD 代码复杂度高 | 中 | 维护困难 | 充分注释，单元测试覆盖 |
| 内存使用增加 | 中 | OOM | Batch 大小可调，流式处理 |
| 某些场景退化 | 低 | 性能下降 | 保留原始执行路径作为 fallback |
| 平台兼容性 | 低 | ARM 不支持 AVX2 | 运行时检测，降级到标量代码 |
