//! V4: Columnar Storage & Compression
//!
//! 列式存储格式，优化 OLAP 工作负载

use crate::storage::Value;

/// 编码类型
#[derive(Clone, Debug, PartialEq)]
pub enum EncodingType {
    /// 纯值（无编码）
    Plain,
    /// 字典编码
    Dictionary,
    /// 行程长度编码
    RunLength,
    /// Delta 编码
    Delta,
    /// 位打包
    BitPack,
}

/// 列数据块
#[derive(Clone, Debug)]
pub struct ColumnBlock {
    /// 编码类型
    pub encoding: EncodingType,
    /// 行数
    pub row_count: usize,
    /// 编码后的数据
    pub data: Vec<u8>,
}

impl ColumnBlock {
    /// 创建空块
    pub fn new() -> Self {
        Self {
            encoding: EncodingType::Plain,
            row_count: 0,
            data: Vec::new(),
        }
    }

    /// 编码整数列（使用 Delta 编码）
    pub fn encode_integers(values: &[i64]) -> Self {
        if values.is_empty() {
            return Self::new();
        }

        // 简单 Delta 编码
        let mut deltas = vec![values[0]];
        for i in 1..values.len() {
            deltas.push(values[i] - values[i - 1]);
        }

        // 序列化为字节
        let mut data = Vec::with_capacity(deltas.len() * 8);
        for v in deltas {
            data.extend_from_slice(&v.to_le_bytes());
        }

        Self {
            encoding: EncodingType::Delta,
            row_count: values.len(),
            data,
        }
    }

    /// 解码为整数
    pub fn decode_integers(&self) -> Vec<i64> {
        if self.encoding != EncodingType::Delta || self.data.is_empty() {
            return Vec::new();
        }

        let mut result = Vec::with_capacity(self.row_count);
        let mut prev = 0i64;

        for chunk in self.data.chunks_exact(8) {
            let delta = i64::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3],
                                           chunk[4], chunk[5], chunk[6], chunk[7]]);
            if result.is_empty() {
                prev = delta;
            } else {
                prev += delta;
            }
            result.push(prev);
        }

        result
    }

    /// 计算压缩比
    pub fn compression_ratio(&self, original_size: usize) -> f64 {
        if original_size == 0 {
            return 1.0;
        }
        original_size as f64 / self.data.len() as f64
    }
}

impl Default for ColumnBlock {
    fn default() -> Self {
        Self::new()
    }
}

/// 列存储
#[derive(Clone, Debug)]
pub struct ColumnStore {
    pub name: String,
    pub blocks: Vec<ColumnBlock>,
}

impl ColumnStore {
    pub fn new(name: String) -> Self {
        Self {
            name,
            blocks: Vec::new(),
        }
    }

    pub fn add_block(&mut self, block: ColumnBlock) {
        self.blocks.push(block);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_delta_encoding() {
        let values = vec![100, 105, 110, 115, 120];
        let block = ColumnBlock::encode_integers(&values);

        assert_eq!(block.encoding, EncodingType::Delta);
        assert_eq!(block.row_count, 5);

        let decoded = block.decode_integers();
        assert_eq!(decoded, values);
    }

    #[test]
    fn test_empty_block() {
        let block = ColumnBlock::new();
        assert_eq!(block.row_count, 0);
        assert!(block.data.is_empty());
    }
}
