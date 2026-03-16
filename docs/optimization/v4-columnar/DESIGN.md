# V4: 列式存储与压缩 (Columnar Storage & Compression)

## 1. 原理说明

### 1.1 行式 vs 列式存储

**行式存储 (SQLite 当前)**：
```
行1: [id=1, name="Alice", age=30]
行2: [id=2, name="Bob", age=25]
行3: [id=3, name="Charlie", age=35]
```
- 适合 OLTP (点查、单行操作)
- 缓存利用率低 (只读 age 也要加载 name)

**列式存储 (本方案)**：
```
id:    [1, 2, 3]
name:  ["Alice", "Bob", "Charlie"]
age:   [30, 25, 35]
```
- 适合 OLAP (分析型查询)
- 缓存友好、SIMD 友好
- 压缩率高 (同类型数据)

### 1.2 压缩原理

| 编码方式 | 适用场景 | 压缩率 | 解压速度 |
|---------|---------|--------|---------|
| **字典编码** | 低基数 (性别、状态) | 10-100x | 极快 |
| **RLE** | 连续重复值 | 10-1000x | 极快 |
| **差分编码** | 时间序列、递增 ID | 5-10x | 快 |
| **位压缩** | 小范围整数 | 2-4x | 极快 |
| **轻量级通用** | 任意数据 | 2-5x | 快 |

### 1.3 向量化 + 列式优势

```
查询: SELECT SUM(age) FROM users WHERE age > 25

行式: 加载整行 → 提取 age → 比较 → 累加 (缓存不友好)
列式: 直接加载 age 列 → SIMD 批量比较 → SIMD 批量累加 (缓存友好)
```

## 2. 实现方式

### 2.1 核心数据结构

```rust
/// 存储格式枚举
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum StorageFormat {
    /// 行式存储 (OLTP)
    Row,
    /// 列式存储 (OLAP)
    Columnar,
}

/// 列式表
pub struct ColumnarTable {
    /// 表名
    name: String,
    /// 列定义
    column_defs: Vec<ColumnDef>,
    /// 列存储
    columns: Vec<ColumnStore>,
    /// 行数
    row_count: u64,
    /// 可见性位图 (用于 MVCC)
    visibility_bitmap: BitVec,
}

/// 列存储
pub struct ColumnStore {
    /// 列名
    name: String,
    /// 数据类型
    data_type: DataType,
    /// 编码方式
    encoding: Encoding,
    /// 压缩数据块
    blocks: Vec<DataBlock>,
    /// 统计信息
    stats: ColumnStats,
}

/// 编码方式
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Encoding {
    /// 原始格式 (未压缩)
    Raw,
    /// 字典编码
    Dictionary,
    /// 游程编码 (RLE)
    RunLength,
    /// 差分编码
    Delta,
    /// 位压缩
    BitPack { bits: u8 },
    /// 轻量级压缩 (LZ4)
    Lightweight,
}

/// 数据块 (固定大小，便于向量化处理)
pub const BLOCK_SIZE: usize = 65536; // 64K 行/块

pub struct DataBlock {
    /// 行数
    row_count: usize,
    /// 编码后的数据
    encoded_data: Vec<u8>,
    /// 原始数据大小 (用于计算压缩率)
    uncompressed_size: usize,
    /// 每行的偏移 (变长编码时需要)
    row_offsets: Option<Vec<u32>>,
}

/// 列统计信息
#[derive(Clone, Debug)]
pub struct ColumnStats {
    pub min_value: Option<Value>,
    pub max_value: Option<Value>,
    pub null_count: u64,
    pub distinct_count: u64,
    pub total_size: u64,
    pub compressed_size: u64,
}
```

### 2.2 编码器实现

```rust
/// 编码器 trait
pub trait Encoder: Send + Sync {
    /// 编码数据
    fn encode(&self, values: &[Value]) -> Result<Vec<u8>>;
    /// 解码整个块
    fn decode(&self, data: &[u8], count: usize) -> Result<Vec<Value>>;
    /// 解码单值 (随机访问)
    fn decode_at(&self, data: &[u8], index: usize) -> Result<Value>;
    /// 编码类型
    fn encoding_type(&self) -> Encoding;
}

/// 字典编码器
pub struct DictionaryEncoder {
    /// 字典大小限制
    max_dict_size: usize,
}

impl Encoder for DictionaryEncoder {
    fn encode(&self, values: &[Value]) -> Result<Vec<u8>> {
        // 1. 构建字典
        let mut dict: IndexSet<Value> = IndexSet::new();
        for v in values {
            if !v.is_null() {
                dict.insert(v.clone());
                if dict.len() > self.max_dict_size {
                    return Err(Error::DictTooLarge);
                }
            }
        }

        // 2. 编码数据
        let dict_size = dict.len();
        let index_bits = (dict_size as f64).log2().ceil() as u8;

        let mut encoded = Vec::new();

        // 写入字典
        encoded.write_u32(dict_size as u32)?;
        for v in &dict {
            serialize_value(&mut encoded, v)?;
        }

        // 写入索引位宽
        encoded.push(index_bits);

        // 写入索引数据
        match index_bits {
            0..=8 => {
                for v in values {
                    let idx = dict.get_index_of(v).unwrap_or(dict_size) as u8;
                    encoded.push(idx);
                }
            }
            9..=16 => {
                for v in values {
                    let idx = dict.get_index_of(v).unwrap_or(dict_size) as u16;
                    encoded.write_u16(idx)?;
                }
            }
            _ => {
                for v in values {
                    let idx = dict.get_index_of(v).unwrap_or(dict_size) as u32;
                    encoded.write_u32(idx)?;
                }
            }
        }

        Ok(encoded)
    }

    fn decode(&self, data: &[u8], count: usize) -> Result<Vec<Value>> {
        let mut cursor = Cursor::new(data);

        // 读取字典
        let dict_size = cursor.read_u32()? as usize;
        let mut dict: Vec<Value> = Vec::with_capacity(dict_size);
        for _ in 0..dict_size {
            dict.push(deserialize_value(&mut cursor)?);
        }

        // 读取位宽
        let index_bits = cursor.read_u8()?;

        // 读取索引并解码
        let mut values = Vec::with_capacity(count);

        match index_bits {
            0..=8 => {
                for _ in 0..count {
                    let idx = cursor.read_u8()? as usize;
                    if idx < dict_size {
                        values.push(dict[idx].clone());
                    } else {
                        values.push(Value::Null);
                    }
                }
            }
            9..=16 => {
                for _ in 0..count {
                    let idx = cursor.read_u16()? as usize;
                    if idx < dict_size {
                        values.push(dict[idx].clone());
                    } else {
                        values.push(Value::Null);
                    }
                }
            }
            _ => {
                for _ in 0..count {
                    let idx = cursor.read_u32()? as usize;
                    if idx < dict_size {
                        values.push(dict[idx].clone());
                    } else {
                        values.push(Value::Null);
                    }
                }
            }
        }

        Ok(values)
    }

    fn encoding_type(&self) -> Encoding {
        Encoding::Dictionary
    }
}

/// RLE 编码器
pub struct RleEncoder;

impl Encoder for RleEncoder {
    fn encode(&self, values: &[Value]) -> Result<Vec<u8>> {
        let mut encoded = Vec::new();
        let mut runs: Vec<(Value, u32)> = Vec::new();

        let mut current = Value::Null;
        let mut count = 0u32;

        for v in values {
            if *v == current && count < u32::MAX {
                count += 1;
            } else {
                if count > 0 {
                    runs.push((current.clone(), count));
                }
                current = v.clone();
                count = 1;
            }
        }
        runs.push((current, count));

        // 写入 run 数量
        encoded.write_u32(runs.len() as u32)?;

        // 写入 runs
        for (value, count) in runs {
            serialize_value(&mut encoded, &value)?;
            encoded.write_u32(count)?;
        }

        Ok(encoded)
    }

    fn decode(&self, data: &[u8], count: usize) -> Result<Vec<Value>> {
        let mut cursor = Cursor::new(data);
        let run_count = cursor.read_u32()? as usize;

        let mut values = Vec::with_capacity(count);

        for _ in 0..run_count {
            let value = deserialize_value(&mut cursor)?;
            let run_len = cursor.read_u32()? as usize;
            values.extend(std::iter::repeat(value).take(run_len));
        }

        Ok(values)
    }

    fn encoding_type(&self) -> Encoding {
        Encoding::RunLength
    }
}

/// 差分编码器 (适合递增序列)
pub struct DeltaEncoder;

impl Encoder for DeltaEncoder {
    fn encode(&self, values: &[Value]) -> Result<Vec<u8>> {
        let mut encoded = Vec::new();

        // 写入基准值
        let base = match values.first() {
            Some(Value::Integer(n)) => *n,
            _ => return Err(Error::InvalidType),
        };
        encoded.write_i64(base)?;

        // 计算差分并压缩
        let mut deltas: Vec<i64> = Vec::with_capacity(values.len() - 1);
        let mut prev = base;

        for v in &values[1..] {
            if let Value::Integer(n) = v {
                deltas.push(n - prev);
                prev = *n;
            } else {
                return Err(Error::InvalidType);
            }
        }

        // 使用变长编码存储差分
        for delta in deltas {
            encode_varint(&mut encoded, zigzag_encode(delta));
        }

        Ok(encoded)
    }

    fn decode(&self, data: &[u8], count: usize) -> Result<Vec<Value>> {
        let mut cursor = Cursor::new(data);
        let base = cursor.read_i64()?;

        let mut values = vec![Value::Integer(base)];
        let mut current = base;

        for _ in 1..count {
            let delta = zigzag_decode(decode_varint(&mut cursor)?);
            current += delta;
            values.push(Value::Integer(current));
        }

        Ok(values)
    }

    fn encoding_type(&self) -> Encoding {
        Encoding::Delta
    }
}

/// 位压缩编码器
pub struct BitPackEncoder;

impl Encoder for BitPackEncoder {
    fn encode(&self, values: &[Value]) -> Result<Vec<u8>> {
        // 1. 找出范围
        let (min, max) = values.iter().fold((i64::MAX, i64::MIN), |(min, max), v| {
            if let Value::Integer(n) = v {
                (min.min(*n), max.max(*n))
            } else {
                (min, max)
            }
        });

        let range = (max - min) as u64;
        let bits = 64 - range.leading_zeros();

        // 2. 写入元数据
        let mut encoded = Vec::new();
        encoded.write_i64(min)?;
        encoded.write_u8(bits as u8)?;
        encoded.write_u32(values.len() as u32)?;

        // 3. 位压缩
        let mut current_byte: u8 = 0;
        let mut bit_pos = 0;

        for v in values {
            let val = if let Value::Integer(n) = v {
                (*n - min) as u64
            } else {
                0
            };

            let mut remaining_bits = bits;
            let mut val_pos = 0;

            while remaining_bits > 0 {
                let avail = 8 - bit_pos;
                let to_write = remaining_bits.min(avail);

                let mask = (1u64 << to_write) - 1;
                let bits_to_write = ((val >> val_pos) & mask) as u8;

                current_byte |= bits_to_write << bit_pos;
                bit_pos += to_write;
                val_pos += to_write;
                remaining_bits -= to_write;

                if bit_pos == 8 {
                    encoded.push(current_byte);
                    current_byte = 0;
                    bit_pos = 0;
                }
            }
        }

        if bit_pos > 0 {
            encoded.push(current_byte);
        }

        Ok(encoded)
    }

    fn decode(&self, data: &[u8], count: usize) -> Result<Vec<Value>> {
        let mut cursor = Cursor::new(data);
        let min = cursor.read_i64()?;
        let bits = cursor.read_u8()? as u32;
        let len = cursor.read_u32()? as usize;

        let mut values = Vec::with_capacity(len);
        let data_start = cursor.position() as usize;
        let bytes = &data[data_start..];

        let mut byte_idx = 0;
        let mut bit_pos = 0;

        for _ in 0..len {
            let mut val: u64 = 0;
            let mut remaining = bits;
            let mut val_pos = 0;

            while remaining > 0 {
                let avail = 8 - bit_pos;
                let to_read = remaining.min(avail);

                let mask = ((1u16 << to_read) - 1) as u8;
                let bits_read = (bytes[byte_idx] >> bit_pos) & mask;

                val |= (bits_read as u64) << val_pos;

                bit_pos += to_read;
                val_pos += to_read;
                remaining -= to_read;

                if bit_pos == 8 {
                    byte_idx += 1;
                    bit_pos = 0;
                }
            }

            values.push(Value::Integer(min + val as i64));
        }

        Ok(values)
    }

    fn encoding_type(&self) -> Encoding {
        Encoding::BitPack { bits: 0 }
    }
}
```

### 2.3 自动编码选择

```rust
/// 编码选择器
pub struct EncodingSelector;

impl EncodingSelector {
    /// 分析数据特征，选择最优编码
    pub fn select(values: &[Value]) -> (Encoding, Box<dyn Encoder>) {
        // 1. 基础统计
        let total = values.len();
        let null_count = values.iter().filter(|v| v.is_null()).count();
        let non_null = total - null_count;

        if non_null == 0 {
            return (Encoding::Raw, Box::new(RawEncoder));
        }

        // 2. 检测 RLE 适用性
        let runs = Self::count_runs(values);
        if runs * 10 < total {
            // 压缩率 > 10x
            return (Encoding::RunLength, Box::new(RleEncoder));
        }

        // 3. 检测字典编码适用性
        let distinct: HashSet<_> = values.iter().cloned().collect();
        if distinct.len() * 10 < non_null {
            // 低基数
            return (Encoding::Dictionary, Box::new(DictionaryEncoder::new(65536)));
        }

        // 4. 检测差分编码适用性 (仅整数)
        if Self::is_all_integer(values) && Self::is_monotonic(values) {
            return (Encoding::Delta, Box::new(DeltaEncoder));
        }

        // 5. 检测位压缩适用性
        if Self::is_all_integer(values) {
            let (min, max) = Self::int_range(values);
            let range = max - min;
            if range > 0 && (range as f64) < (non_null as f64) * 0.5 {
                return (Encoding::BitPack { bits: 0 }, Box::new(BitPackEncoder));
            }
        }

        // 6. 默认：原始格式
        (Encoding::Raw, Box::new(RawEncoder))
    }

    fn count_runs(values: &[Value]) -> usize {
        if values.is_empty() {
            return 0;
        }
        let mut runs = 1;
        let mut prev = &values[0];
        for v in &values[1..] {
            if v != prev {
                runs += 1;
                prev = v;
            }
        }
        runs
    }

    fn is_monotonic(values: &[Value]) -> bool {
        // 检查是否单调递增/递减
        let ints: Vec<i64> = values.iter().filter_map(|v| match v {
            Value::Integer(n) => Some(*n),
            _ => None,
        }).collect();

        if ints.len() < 2 {
            return false;
        }

        let increasing = ints.windows(2).all(|w| w[0] <= w[1]);
        let decreasing = ints.windows(2).all(|w| w[0] >= w[1]);

        increasing || decreasing
    }
}
```

### 2.4 向量化查询执行

```rust
/// 列式查询执行器
pub struct ColumnarExecutor;

impl ColumnarExecutor {
    /// 执行聚合查询 (直接在压缩数据上计算)
    pub fn execute_aggregate(
        &self,
        table: &ColumnarTable,
        column: &str,
        agg_func: AggregateFunc,
    ) -> Result<Value> {
        let col_idx = table.column_index(column)?;
        let col_store = &table.columns[col_idx];

        let mut result: Option<Value> = None;

        for block in &col_store.blocks {
            let block_result = self.aggregate_block(block, agg_func)?;
            result = Self::combine_aggregate(result, block_result, agg_func);
        }

        result.ok_or(Error::EmptyResult)
    }

    /// 在压缩块上直接聚合 (无需解压)
    fn aggregate_block(
        &self,
        block: &DataBlock,
        func: AggregateFunc,
    ) -> Result<Value> {
        match block.encoding {
            Encoding::RunLength => {
                // RLE 可以直接计算: sum(value * count)
                self.aggregate_rle(&block.encoded_data, func)
            }
            Encoding::Dictionary => {
                // 字典编码: 先解码字典，然后按索引计数
                self.aggregate_dict(&block.encoded_data, func)
            }
            Encoding::Delta => {
                // 差分编码: 快速解码后聚合
                self.aggregate_delta(&block.encoded_data, func)
            }
            Encoding::BitPack { .. } => {
                // 位压缩: SIMD 解压后聚合
                self.aggregate_bitpack(&block.encoded_data, func)
            }
            _ => {
                // 其他: 完全解码后聚合
                let values = block.decode()?;
                self.aggregate_raw(&values, func)
            }
        }
    }

    /// RLE 直接聚合
    fn aggregate_rle(
        &self,
        data: &[u8],
        func: AggregateFunc,
    ) -> Result<Value> {
        let mut cursor = Cursor::new(data);
        let run_count = cursor.read_u32()? as usize;

        match func {
            AggregateFunc::Count => {
                let mut total: u64 = 0;
                for _ in 0..run_count {
                    let _value = deserialize_value(&mut cursor)?;
                    let count = cursor.read_u32()? as u64;
                    total += count;
                }
                Ok(Value::Integer(total as i64))
            }
            AggregateFunc::Sum => {
                let mut sum: i64 = 0;
                for _ in 0..run_count {
                    let value = deserialize_value(&mut cursor)?;
                    let count = cursor.read_u32()? as i64;
                    if let Value::Integer(n) = value {
                        sum += n * count;
                    }
                }
                Ok(Value::Integer(sum))
            }
            _ => {
                // 其他函数需要解码
                Err(Error::NotImplemented)
            }
        }
    }
}
```

## 3. Rust 实现方式

### 3.1 第三方库

```toml
[dependencies]
# 位操作
bitvec = "1"

# 压缩 (可选)
lz4 = "1"  # 轻量级通用压缩

# 有序集合 (字典编码)
indexmap = "2"

# 字节序处理
byteorder = "1"

# 无标准库支持
# 所有编码自己实现，不依赖 heavy 库
```

### 3.2 自己实现的部分

| 组件 | 实现方式 | 原因 |
|------|---------|------|
| 列存储格式 | 自己实现 | 与现有系统整合 |
| 所有编码器 | 自己实现 | 轻量级、可控 |
| 编码选择器 | 自己实现 | 启发式算法 |
| 向量化执行 | 自己实现 | SIMD 优化 |
| 块管理 | 自己实现 | 固定大小块 |

### 3.3 代码结构

```
src/
├── storage/
│   ├── columnar/           # 新增列式存储模块
│   │   ├── mod.rs
│   │   ├── table.rs        # ColumnarTable
│   │   ├── column.rs       # ColumnStore
│   │   ├── block.rs        # DataBlock
│   │   └── stats.rs        # 统计信息
│   ├── encoding/           # 编码模块
│   │   ├── mod.rs
│   │   ├── raw.rs
│   │   ├── dictionary.rs
│   │   ├── rle.rs
│   │   ├── delta.rs
│   │   ├── bitpack.rs
│   │   └── selector.rs
│   └── executor/
│       └── columnar.rs     # 列式查询执行
```

## 4. 验证方法

### 4.1 单元测试

```rust
#[test]
fn test_dictionary_encoding() {
    let values = vec![
        Value::Text("Male".to_string()),
        Value::Text("Female".to_string()),
        Value::Text("Male".to_string()),
        Value::Text("Female".to_string()),
        Value::Text("Male".to_string()),
    ];

    let encoder = DictionaryEncoder::new(100);
    let encoded = encoder.encode(&values).unwrap();

    // 验证压缩率
    let original_size = values.iter().map(|v| v.size()).sum::<usize>();
    println!("Original: {}, Encoded: {}, Ratio: {:.2}x",
        original_size, encoded.len(), original_size as f64 / encoded.len() as f64);

    // 验证解码
    let decoded = encoder.decode(&encoded, values.len()).unwrap();
    assert_eq!(values, decoded);
}

#[test]
fn test_rle_encoding() {
    let values: Vec<Value> = (0..1000)
        .map(|i| Value::Integer(i / 100)) // 0,0,0...,1,1,1...,2,2,2...
        .collect();

    let encoder = RleEncoder;
    let encoded = encoder.encode(&values).unwrap();

    // 应该压缩得很好
    assert!(encoded.len() < values.len() * 2);

    let decoded = encoder.decode(&encoded, values.len()).unwrap();
    assert_eq!(values, decoded);
}

#[test]
fn test_encoding_selector() {
    // RLE 数据
    let rle_data: Vec<Value> = vec![Value::Integer(1); 1000];
    let (enc, _) = EncodingSelector::select(&rle_data);
    assert_eq!(enc, Encoding::RunLength);

    // 字典数据
    let dict_data: Vec<Value> = vec![Value::Text("A".to_string()); 100];
    let (enc, _) = EncodingSelector::select(&dict_data);
    assert_eq!(enc, Encoding::Dictionary);

    // 差分数据
    let delta_data: Vec<Value> = (0..1000).map(Value::Integer).collect();
    let (enc, _) = EncodingSelector::select(&delta_data);
    assert_eq!(enc, Encoding::Delta);
}
```

### 4.2 性能基准测试

```rust
fn bench_columnar_vs_row(c: &mut Criterion) {
    let mut group = c.benchmark_group("storage_format");

    let row_table = create_row_table(1_000_000);
    let col_table = create_columnar_table(1_000_000);

    group.bench_function("row_scan", |b| {
        b.iter(|| row_table.scan_column("age"));
    });

    group.bench_function("columnar_scan", |b| {
        b.iter(|| col_table.scan_column("age"));
    });

    group.finish();
}

fn bench_compressed_aggregate(c: &mut Criterion) {
    let mut group = c.benchmark_group("compressed_aggregate");

    let table = create_compressed_table(1_000_000);

    group.bench_function("sum_on_compressed", |b| {
        b.iter(|| table.execute_aggregate("age", AggregateFunc::Sum));
    });

    group.finish();
}
```

### 4.3 验证指标

| 指标 | 当前基线 | V4 目标 | 验证方法 |
|------|---------|--------|---------|
| 扫描速度 | 1x | 5-10x | 基准测试 |
| 压缩率 | 1x | 2-10x | 存储空间对比 |
| 聚合速度 | 1x | 10-50x | 基准测试 |
| 缓存命中率 | - | > 95% | perf |
| 编码选择准确率 | - | > 90% | 手动检查 |

## 5. 实施计划

### Week 1
- [ ] 实现列存储基础结构
- [ ] 实现字典编码
- [ ] 实现 RLE 编码
- [ ] 单元测试

### Week 2
- [ ] 实现差分编码
- [ ] 实现位压缩
- [ ] 实现编码选择器
- [ ] 列式查询执行器
- [ ] 性能基准测试

## 6. 注意事项

### 6.1 混合存储
- 允许表同时有行式和列式分区
- 查询时自动选择最优格式

### 6.2 更新性能
- 列式更新代价高
- 考虑使用 delta 存储 + 合并

### 6.3 内存使用
- 解压缩需要额外内存
- 使用流式处理避免 OOM
