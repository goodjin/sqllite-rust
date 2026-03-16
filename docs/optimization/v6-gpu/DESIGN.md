# V6: GPU 加速 (GPU Acceleration)

## 1. 原理说明

### 1.1 为什么用 GPU

**CPU vs GPU 架构差异**:

```
CPU (少量核心):
┌─────┬─────┬─────┬─────┐
│ Core│ Core│ Core│ Core│  ← 4-32 核心
│ ALU │ ALU │ ALU │ ALU │  ← 复杂控制逻辑
│Cache│Cache│Cache│Cache│  ← 大缓存
└─────┴─────┴─────┴─────┘
适合: 顺序执行、复杂逻辑

GPU (大量简单核心):
┌────┬────┬────┬────┐
│ALU │ALU │ALU │ALU │  ← 成千上万个
│ALU │ALU │ALU │ALU │
│ALU │ALU │ALU │ALU │
└────┴────┴────┴────┘
适合: 数据并行、简单计算
```

**数据库操作的并行性**:
- 扫描: 每个元素独立处理
- 过滤: 每个元素独立判断
- 聚合: 使用归约算法并行
- 排序: 并行排序网络

### 1.2 适用场景

| 场景 | 数据量 | CPU | GPU | 加速比 |
|------|--------|-----|-----|--------|
| 简单查询 | < 1万 | 快 | 慢 (启动开销) | 0.1x |
| 分析查询 | > 100万 | 慢 | 快 | 10-100x |
| 大表聚合 | > 1000万 | 很慢 | 快 | 50-200x |
| 多表 JOIN | > 100万 | 慢 | 快 | 10-50x |

## 2. 实现方式

### 2.1 CUDA 基础架构

```rust
// 使用 rustacuda 库
use rustacuda::prelude::*;
use rustacuda::memory::{DeviceBox, DeviceBuffer};
use rustacuda::stream::{Stream, StreamFlags};

/// GPU 执行器
pub struct GpuExecutor {
    context: Context,
    module: Module,
    stream: Stream,
    memory_pool: DeviceMemoryPool,
}

impl GpuExecutor {
    pub fn new() -> Result<Self> {
        rustacuda::init(CudaFlags::empty())?;
        let device = Device::get_device(0)?;
        let context = Context::create_and_push(
            ContextFlags::MAP_HOST | ContextFlags::SCHED_AUTO,
            device
        )?;
        let module = Module::load_from_string(include_str!("../kernels/aggregates.ptx"))?;
        let stream = Stream::new(StreamFlags::NON_BLOCKING, None)?;

        Ok(Self {
            context,
            module,
            stream,
            memory_pool: DeviceMemoryPool::new(1024 * 1024 * 1024)?,
        })
    }

    pub fn should_use_gpu(&self, plan: &QueryPlan) -> bool {
        let min_rows = 100_000;
        if plan.estimated_rows < min_rows {
            return false;
        }
        match &plan.root {
            PlanNode::Aggregate { .. } => true,
            PlanNode::Scan { predicates, .. } if !predicates.is_empty() => true,
            PlanNode::Project { .. } => true,
            _ => false,
        }
    }
}
```

### 2.2 CUDA 核函数

```cuda
#define BLOCK_SIZE 256

extern "C" __global__ void sum_kernel(
    const int64_t* __restrict__ data,
    size_t n,
    int64_t* __restrict__ result
) {
    __shared__ int64_t sdata[BLOCK_SIZE];

    unsigned int tid = threadIdx.x;
    unsigned int i = blockIdx.x * blockDim.x + threadIdx.x;

    int64_t sum = 0;
    if (i < n) {
        sum = data[i];
    }
    sdata[tid] = sum;
    __syncthreads();

    for (unsigned int s = blockDim.x / 2; s > 0; s >>= 1) {
        if (tid < s) {
            sdata[tid] += sdata[tid + s];
        }
        __syncthreads();
    }

    if (tid == 0) {
        result[blockIdx.x] = sdata[0];
    }
}
```

### 2.3 Rust 调用层

```rust
impl GpuExecutor {
    pub fn execute_aggregate(
        &mut self,
        column: &ColumnVector,
        func: AggregateFunc,
    ) -> Result<Value> {
        let device_data = self.upload_column(column)?;
        let num_blocks = (column.len() + BLOCK_SIZE - 1) / BLOCK_SIZE;
        let mut block_results = DeviceBuffer::<i64>::zeroed(num_blocks)?;

        let kernel = match func {
            AggregateFunc::Sum => self.module.get_function("sum_kernel")?,
            _ => return Err(Error::UnsupportedAggregate),
        };

        unsafe {
            launch!(kernel<<<
                (num_blocks as u32, 1, 1),
                (BLOCK_SIZE, 1, 1),
                0, self.stream
            >>>(
                device_data.as_device_ptr(),
                column.len(),
                block_results.as_device_ptr()
            ))?;
        }

        self.stream.synchronize()?;

        let mut host_results = vec![0i64; num_blocks];
        block_results.copy_to(&mut host_results)?;

        Ok(Value::Integer(host_results.iter().sum()))
    }
}
```

## 3. Rust 实现方式

### 3.1 第三方库

```toml
[dependencies]
rustacuda = "0.1"
rustacuda_core = "0.1"
tokio = { version = "1", features = ["rt-multi-thread"] }
```

### 3.2 自己实现的部分

| 组件 | 实现方式 | 原因 |
|------|---------|------|
| CUDA 核函数 | 手写 CUDA C | 性能关键 |
| 执行器集成 | 自己实现 | 与现有系统整合 |
| 数据传输优化 | 自己实现 | 批量、异步 |
| 内存池 | 自己实现 | 减少分配开销 |

## 4. 验证方法

### 4.1 单元测试

```rust
#[test]
#[ignore] // 需要 GPU
fn test_gpu_sum() {
    let mut gpu = GpuExecutor::new().unwrap();
    let data: Vec<Value> = (1..=1_000_000)
        .map(|i| Value::Integer(i))
        .collect();
    let column = ColumnVector::from(data);

    let result = gpu.execute_aggregate(&column, AggregateFunc::Sum).unwrap();
    assert_eq!(result, Value::Integer(500000500000));
}
```

### 4.2 验证指标

| 指标 | 目标 | 验证方法 |
|------|------|---------|
| SUM 1000万行 | > 100x CPU | 基准测试 |
| GPU 利用率 | > 90% | nvidia-smi |
| 结果正确性 | 100% | 对比 CPU 结果 |

## 5. 实施计划

### Week 1-2
- [ ] 搭建 CUDA 环境
- [ ] 实现基础核函数 (SUM, COUNT, MIN, MAX)
- [ ] 实现内存管理

### Week 3-4
- [ ] 与查询执行器集成
- [ ] 自动 CPU/GPU 选择
- [ ] 性能基准测试

## 6. 注意事项

### 6.1 硬件要求
- NVIDIA GPU (Compute Capability >= 7.0)
- CUDA Toolkit 11.0+
- 足够的 GPU 内存

### 6.2 性能陷阱
- 数据传输是瓶颈，尽量合并
- 小数据量 (< 10万) 不要用 GPU
- 注意内存对齐 (128B)

### 6.3 可移植性
- 仅支持 NVIDIA GPU
- 需要条件编译 #[cfg(feature = "cuda")]
- 提供纯 CPU 回退
