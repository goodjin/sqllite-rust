# V5: JIT 编译执行 (Just-In-Time Compilation)

## 1. 原理说明

### 1.1 解释执行 vs JIT 编译

**解释执行 (SQLite 当前)**：
```
SQL → Parse → AST → Bytecode → Interpreter Loop → Result
                                      ↓
                               每条指令一次 match
                               分支预测失败率高
```

**JIT 编译**：
```
SQL → Parse → AST → Compile to Machine Code → Execute → Result
                          ↓
                    直接执行机器码
                    无解释开销
```

### 1.2 为什么 JIT 更快

| 因素 | 解释执行 | JIT 编译 | 提升 |
|------|---------|---------|------|
| 指令解码 | 每条都 decode | 一次性编译 | 消除 |
| 分支预测 | 难以预测 | 顺序执行 | 大幅提升 |
| 内联优化 | 函数调用 | 内联展开 | 减少 call |
| 寄存器使用 | 虚拟机栈 | 真实寄存器 | 减少内存访问 |
| SIMD | 难利用 | 自动生成 | 向量化 |

### 1.3 JIT 触发策略

```
热代码检测:
- 查询执行次数 > N (如 100 次)
- 表达式复杂度 > 阈值
- 显式提示 (PRAGMA jit=on)

编译缓存:
- 缓存编译后的机器码
- 参数化查询复用
- 内存限制 LRU 淘汰
```

## 2. 实现方式

### 2.1 架构设计

```rust
/// JIT 编译器
pub struct JitCompiler {
    /// 代码生成上下文
    ctx: codegen::Context,

    /// 模块
    module: JITModule,

    /// 编译缓存
    cache: LruCache<QuerySignature, CompiledCode>,
}

/// 编译后的代码
pub struct CompiledCode {
    /// 函数指针
    func_ptr: *const u8,

    /// 参数布局
    param_layout: ParamLayout,

    /// 代码大小
    code_size: usize,
}

/// 查询签名 (用于缓存)
#[derive(Hash, Eq, PartialEq)]
pub struct QuerySignature {
    /// SQL 模板哈希
    sql_hash: u64,

    /// 参数类型
    param_types: Vec<DataType>,
}

/// JIT 执行器
pub struct JitExecutor {
    compiler: JitCompiler,

    /// 执行统计
    stats: ExecutionStats,
}

impl JitExecutor {
    /// 执行查询 (自动选择解释或 JIT)
    pub fn execute(
        &mut self,
        plan: &QueryPlan,
    ) -> Result<ResultSet> {
        // 判断是否值得 JIT
        if self.should_jit(plan) {
            let signature = self.compute_signature(plan);

            // 检查缓存
            if let Some(compiled) = self.compiler.cache.get(&signature) {
                return self.execute_compiled(compiled, plan);
            }

            // 编译并执行
            let compiled = self.compiler.compile(plan)?;
            let result = self.execute_compiled(&compiled, plan)?;

            self.compiler.cache.put(signature, compiled);
            return Ok(result);
        }

        // 回退到解释执行
        self.execute_interpreted(plan)
    }

    fn should_jit(&self, plan: &QueryPlan) -> bool {
        // 简单查询不值得 JIT
        if plan.estimated_cost < 1000.0 {
            return false;
        }

        // 聚合、复杂表达式值得 JIT
        match &plan.root {
            PlanNode::Aggregate { .. } => true,
            PlanNode::Filter { predicate, .. } if is_complex(predicate) => true,
            _ => false,
        }
    }
}
```

### 2.2 使用 Cranelift 编译

```rust
use cranelift::prelude::*;
use cranelift_jit::{JITBuilder, JITModule};
use cranelift_module::{Linkage, Module};

impl JitCompiler {
    pub fn new() -> Self {
        let builder = JITBuilder::new(cranelift_module::default_libcall_names());
        let module = JITModule::new(builder);

        Self {
            ctx: module.make_context(),
            module,
            cache: LruCache::new(100),
        }
    }

    /// 编译查询计划
    pub fn compile(
        &mut self,
        plan: &QueryPlan,
    ) -> Result<CompiledCode> {
        // 清空上下文
        self.ctx.func.signature = self.make_signature();

        // 构建函数
        let mut builder_ctx = FunctionBuilderContext::new();
        let mut builder = FunctionBuilder::new(
            &mut self.ctx.func,
            &mut builder_ctx,
        );

        let entry_block = builder.create_block();
        builder.append_block_params_for_function_params(entry_block);
        builder.switch_to_block(entry_block);

        // 编译计划节点
        let result = self.compile_node(&mut builder, &plan.root)?;

        // 返回结果
        builder.ins().return_(&[result]);

        builder.finalize();

        // 声明函数
        let id = self.module.declare_function(
            "query",
            Linkage::Local,
            &self.ctx.func.signature,
        )?;

        // 定义函数
        self.module.define_function(id, &mut self.ctx)?;
        self.module.finalize_definitions();

        // 获取函数指针
        let func_ptr = self.module.get_finalized_function(id);

        Ok(CompiledCode {
            func_ptr,
            param_layout: self.infer_param_layout(plan),
            code_size: self.ctx.func.body_size(),
        })
    }

    fn compile_node(
        &mut self,
        builder: &mut FunctionBuilder,
        node: &PlanNode,
    ) -> Result<Value> {
        match node {
            PlanNode::Scan { table, .. } => {
                self.compile_scan(builder, table)
            }
            PlanNode::Filter { child, predicate } => {
                self.compile_filter(builder, child, predicate)
            }
            PlanNode::Aggregate { child, aggregates, .. } => {
                self.compile_aggregate(builder, child, aggregates)
            }
            PlanNode::Project { child, expressions } => {
                self.compile_project(builder, child, expressions)
            }
            _ => unimplemented!(),
        }
    }

    /// 编译过滤器
    fn compile_filter(
        &mut self,
        builder: &mut FunctionBuilder,
        child: &PlanNode,
        predicate: &Expression,
    ) -> Result<Value> {
        // 创建循环结构
        let loop_block = builder.create_block();
        let body_block = builder.create_block();
        let end_block = builder.create_block();

        // 初始化计数器
        let zero = builder.ins().iconst(types::I64, 0);
        let count = builder.append_block_param(loop_block, types::I64);

        builder.ins().jump(loop_block, &[zero]);

        // 循环头
        builder.switch_to_block(loop_block);

        // 检查循环条件
        let total = self.compile_get_row_count(builder, child);
        let cond = builder.ins().icmp(
            IntCC::SignedLessThan,
            count,
            total,
        );

        builder.ins().brif(cond, body_block, &[], end_block, &[]);

        // 循环体
        builder.switch_to_block(body_block);

        // 编译谓词
        let pred_val = self.compile_expression(builder, predicate)?;

        // 条件处理
        let then_block = builder.create_block();
        let else_block = builder.create_block();

        builder.ins().brif(pred_val, then_block, &[], else_block, &[]);

        // 谓词为真：处理行
        builder.switch_to_block(then_block);
        self.compile_process_row(builder, child, count)?;
        builder.ins().fallthrough(loop_block, &[count]);

        // 谓词为假：跳过
        builder.switch_to_block(else_block);
        let next_count = builder.ins().iadd(count, builder.ins().iconst(types::I64, 1));
        builder.ins().jump(loop_block, &[next_count]);

        // 循环结束
        builder.switch_to_block(end_block);

        Ok(zero)
    }

    /// 编译表达式
    fn compile_expression(
        &mut self,
        builder: &mut FunctionBuilder,
        expr: &Expression,
    ) -> Result<Value> {
        match expr {
            Expression::Integer(n) => {
                Ok(builder.ins().iconst(types::I64, *n))
            }
            Expression::Real(r) => {
                Ok(builder.ins().f64const(*r))
            }
            Expression::Column(col) => {
                self.compile_column_ref(builder, col)
            }
            Expression::Binary { left, op, right } => {
                let l = self.compile_expression(builder, left)?;
                let r = self.compile_expression(builder, right)?;

                match op {
                    BinaryOp::Add => Ok(builder.ins().iadd(l, r)),
                    BinaryOp::Subtract => Ok(builder.ins().isub(l, r)),
                    BinaryOp::Multiply => Ok(builder.ins().imul(l, r)),
                    BinaryOp::Divide => Ok(builder.ins().sdiv(l, r)),
                    BinaryOp::Equal => {
                        Ok(builder.ins().icmp(IntCC::Equal, l, r))
                    }
                    BinaryOp::LessThan => {
                        Ok(builder.ins().icmp(IntCC::SignedLessThan, l, r))
                    }
                    _ => unimplemented!(),
                }
            }
            Expression::Function { name, args } => {
                self.compile_function_call(builder, name, args)
            }
            _ => unimplemented!(),
        }
    }

    /// 编译聚合
    fn compile_aggregate(
        &mut self,
        builder: &mut FunctionBuilder,
        child: &PlanNode,
        aggregates: &[AggregateExpr],
    ) -> Result<Value> {
        // 为每个聚合创建累加器变量
        let mut accumulators = Vec::new();
        for agg in aggregates {
            let init_val = match agg.func {
                AggregateFunc::Count => builder.ins().iconst(types::I64, 0),
                AggregateFunc::Sum => builder.ins().iconst(types::I64, 0),
                AggregateFunc::Min => builder.ins().iconst(types::I64, i64::MAX),
                AggregateFunc::Max => builder.ins().iconst(types::I64, i64::MIN),
                _ => unimplemented!(),
            };
            accumulators.push(init_val);
        }

        // 创建扫描循环
        let loop_block = builder.create_block();
        let body_block = builder.create_block();
        let end_block = builder.create_block();

        let zero = builder.ins().iconst(types::I64, 0);
        let count = builder.append_block_param(loop_block, types::I64);

        builder.ins().jump(loop_block, &[zero]);
        builder.switch_to_block(loop_block);

        let total = self.compile_get_row_count(builder, child);
        let cond = builder.ins().icmp(IntCC::SignedLessThan, count, total);
        builder.ins().brif(cond, body_block, &[], end_block, &[]);

        // 循环体：更新累加器
        builder.switch_to_block(body_block);

        for (i, agg) in aggregates.iter().enumerate() {
            let val = self.compile_expression(builder, &agg.expr)?;

            let new_acc = match agg.func {
                AggregateFunc::Count => {
                    builder.ins().iadd(accumulators[i], builder.ins().iconst(types::I64, 1))
                }
                AggregateFunc::Sum => {
                    builder.ins().iadd(accumulators[i], val)
                }
                AggregateFunc::Min => {
                    let is_less = builder.ins().icmp(IntCC::SignedLessThan, val, accumulators[i]);
                    builder.ins().select(is_less, val, accumulators[i])
                }
                AggregateFunc::Max => {
                    let is_greater = builder.ins().icmp(IntCC::SignedGreaterThan, val, accumulators[i]);
                    builder.ins().select(is_greater, val, accumulators[i])
                }
                _ => accumulators[i],
            };
            accumulators[i] = new_acc;
        }

        let next_count = builder.ins().iadd(count, builder.ins().iconst(types::I64, 1));
        builder.ins().jump(loop_block, &[next_count]);

        // 返回累加器
        builder.switch_to_block(end_block);

        // 返回第一个聚合结果 (简化)
        Ok(accumulators[0])
    }
}
```

### 2.3 表达式特化

```rust
/// 为特定表达式生成特化代码
pub struct ExpressionSpecializer;

impl ExpressionSpecializer {
    /// 生成特化的 WHERE 子句代码
    pub fn specialize_filter(
        &self,
        builder: &mut FunctionBuilder,
        predicate: &Expression,
        row_accessor: RowAccessor,
    ) -> Value {
        match predicate {
            // 特化常见模式: age > 18 AND age < 65
            Expression::Binary {
                left: l1,
                op: BinaryOp::And,
                right: r1,
            } if matches!((l1.as_ref(), r1.as_ref()), (
                Expression::Binary { op: BinaryOp::GreaterThan, .. },
                Expression::Binary { op: BinaryOp::LessThan, .. },
            )) => {
                self.specialize_range_check(builder, l1, r1, row_accessor)
            }

            // 特化: status = 'ACTIVE'
            Expression::Binary {
                left,
                op: BinaryOp::Equal,
                right: Expression::Text(s),
            } if s == "ACTIVE" => {
                self.specialize_status_active(builder, left, row_accessor)
            }

            // 通用回退
            _ => self.compile_generic(builder, predicate, row_accessor),
        }
    }

    /// SIMD 范围检查
    fn specialize_range_check(
        &self,
        builder: &mut FunctionBuilder,
        lower: &Expression,
        upper: &Expression,
        row_accessor: RowAccessor,
    ) -> Value {
        // 加载值
        let val = row_accessor.load_column(builder, "age");

        // 下界检查
        let lower_bound = builder.ins().iconst(types::I64, 18);
        let gt_lower = builder.ins().icmp(IntCC::SignedGreaterThan, val, lower_bound);

        // 上界检查
        let upper_bound = builder.ins().iconst(types::I64, 65);
        let lt_upper = builder.ins().icmp(IntCC::SignedLessThan, val, upper_bound);

        // 合并
        builder.ins().band(gt_lower, lt_upper)
    }
}
```

### 2.4 缓存管理

```rust
/// JIT 代码缓存
pub struct JitCodeCache {
    cache: LruCache<QuerySignature, CompiledCode>,

    /// 总内存限制
    memory_limit: usize,

    /// 当前使用内存
    current_memory: usize,
}

impl JitCodeCache {
    pub fn with_memory_limit(limit: usize) -> Self {
        Self {
            cache: LruCache::new(1000),
            memory_limit: limit,
            current_memory: 0,
        }
    }

    pub fn get(&self,
        signature: &QuerySignature,
    ) -> Option<&CompiledCode> {
        self.cache.get(signature)
    }

    pub fn put(
        &mut self,
        signature: QuerySignature,
        code: CompiledCode,
    ) {
        // 检查内存限制
        while self.current_memory + code.code_size > self.memory_limit {
            if let Some((_, evicted)) = self.cache.pop_lru() {
                self.current_memory -= evicted.code_size;
            } else {
                break;
            }
        }

        self.current_memory += code.code_size;
        self.cache.put(signature, code);
    }
}
```

## 3. Rust 实现方式

### 3.1 第三方库

```toml
[dependencies]
# JIT 编译器
cranelift = "0.104"
cranelift-jit = "0.104"
cranelift-module = "0.104"

# 汇编器 (备用)
dynasmrt = "2"  # 如果需要手写汇编

# 缓存
lru = "0.12"
```

### 3.2 自己实现的部分

| 组件 | 实现方式 | 原因 |
|------|---------|------|
| 查询编译 | 使用 Cranelift | 成熟、Rust-native |
| 表达式编译 | 自己实现 | 特化优化 |
| 缓存管理 | 自己实现 | LRU + 内存限制 |
| 热代码检测 | 自己实现 | 统计计数 |
| 回退机制 | 自己实现 | 异常处理 |

### 3.3 代码结构

```
src/
├── jit/
│   ├── mod.rs
│   ├── compiler.rs       # JIT 编译器
│   ├── codegen/          # 代码生成
│   │   ├── mod.rs
│   │   ├── scan.rs
│   │   ├── filter.rs
│   │   ├── aggregate.rs
│   │   └── expression.rs
│   ├── cache.rs          # 代码缓存
│   ├── executor.rs       # JIT 执行器
│   └── specializer.rs    # 特化优化
```

## 4. 验证方法

### 4.1 单元测试

```rust
#[test]
fn test_jit_simple_expression() {
    let mut compiler = JitCompiler::new();

    let expr = parse("1 + 2 * 3");
    let compiled = compiler.compile_expression(&expr).unwrap();

    let result: i64 = unsafe {
        std::mem::transmute::<_, fn() -> i64>(compiled.func_ptr)()
    };

    assert_eq!(result, 7);
}

#[test]
fn test_jit_filter() {
    let mut compiler = JitCompiler::new();

    let plan = QueryPlan {
        root: PlanNode::Filter {
            child: Box::new(PlanNode::Scan { table: "t".to_string() }),
            predicate: parse("age > 18"),
        },
    };

    let compiled = compiler.compile(&plan).unwrap();

    // 验证生成的代码可执行
    assert!(!compiled.func_ptr.is_null());
}
```

### 4.2 性能基准测试

```rust
fn bench_jit_vs_interpreted(c: &mut Criterion) {
    let mut group = c.benchmark_group("jit");

    // 复杂表达式计算
    group.bench_function("interpreted_complex", |b| {
        let executor = Interpreter::new();
        let expr = complex_expression();
        b.iter(|| executor.eval(&expr));
    });

    group.bench_function("jit_complex", |b| {
        let mut jit = JitCompiler::new();
        let expr = complex_expression();
        let compiled = jit.compile_expression(&expr).unwrap();

        b.iter(|| unsafe {
            let f: fn() -> i64 = std::mem::transmute(compiled.func_ptr);
            f()
        });
    });

    group.finish();
}
```

### 4.3 验证指标

| 指标 | 当前基线 | V5 目标 | 验证方法 |
|------|---------|--------|---------|
| 表达式计算 | 1x | 5-20x | 基准测试 |
| 过滤操作 | 1x | 3-10x | 基准测试 |
| 编译时间 | - | < 10ms | 计时 |
| 代码质量 | - | 接近 -O2 | 对比 GCC |
| 缓存命中率 | - | > 80% | 统计 |

## 5. 实施计划

### Week 1
- [ ] 集成 Cranelift
- [ ] 实现表达式编译
- [ ] 基础单元测试

### Week 2
- [ ] 实现计划节点编译
- [ ] 实现缓存管理
- [ ] 热代码检测
- [ ] 性能基准测试

### Week 3
- [ ] 表达式特化优化
- [ ] SIMD 代码生成
- [ ] 高级优化
- [ ] 与 SQLite 对比

## 6. 注意事项

### 6.1 安全性
- JIT 代码在可执行内存中运行
- 需要确保编译器不会生成危险代码
- 使用 Cranelift 的沙箱特性

### 6.2 调试
- 可生成调试信息
- 使用 perf 分析 JIT 代码

### 6.3 可移植性
- Cranelift 支持 x86_64, ARM64
- 自动适配目标平台
