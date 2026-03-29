//! Prefix Compression for B-tree Keys
//!
//! Reduces storage by storing common prefix once per page.
//! Especially effective for string keys with common prefixes like:
//! - URLs: "https://example.com/..."
//! - Timestamps: "2024-01-01T..."
//! - IDs with prefixes: "user_123", "user_124"

/// Compressed key storage
#[derive(Debug, Clone, PartialEq)]
pub struct CompressedKey {
    /// Common prefix length (shared with previous key)
    pub prefix_len: u16,
    /// Unique suffix bytes
    pub suffix: Vec<u8>,
}

impl CompressedKey {
    /// Create a new compressed key
    pub fn new(prefix_len: u16, suffix: Vec<u8>) -> Self {
        Self { prefix_len, suffix }
    }
    
    /// Compress a key against a reference (previous) key
    pub fn compress(key: &[u8], reference: &[u8]) -> Self {
        let prefix_len = Self::common_prefix_len(key, reference);
        let suffix = key[prefix_len..].to_vec();
        
        Self {
            prefix_len: prefix_len as u16,
            suffix,
        }
    }
    
    /// Decompress by combining with reference key
    pub fn decompress(&self, reference: &[u8]) -> Vec<u8> {
        let mut result = Vec::with_capacity(self.prefix_len as usize + self.suffix.len());
        result.extend_from_slice(&reference[..self.prefix_len as usize]);
        result.extend_from_slice(&self.suffix);
        result
    }
    
    /// Find common prefix length between two byte slices
    fn common_prefix_len(a: &[u8], b: &[u8]) -> usize {
        a.iter()
            .zip(b.iter())
            .take_while(|(x, y)| x == y)
            .count()
    }
    
    /// Total uncompressed size
    pub fn uncompressed_size(&self) -> usize {
        self.prefix_len as usize + self.suffix.len()
    }
    
    /// Compressed size (storage used)
    pub fn compressed_size(&self) -> usize {
        2 + self.suffix.len() // 2 bytes for prefix_len + suffix
    }
    
    /// Compression ratio (uncompressed / compressed)
    pub fn compression_ratio(&self) -> f64 {
        self.uncompressed_size() as f64 / self.compressed_size().max(1) as f64
    }
}

/// Batch compressor for a page of keys
/// All keys are compressed against the first key (page prefix)
pub struct PagePrefixCompressor {
    /// The common prefix for all keys on this page
    page_prefix: Vec<u8>,
    /// Keys compressed against page_prefix
    compressed_keys: Vec<CompressedKey>,
}

impl PagePrefixCompressor {
    /// Create compressor from a set of keys
    pub fn new(keys: &[Vec<u8>]) -> Self {
        if keys.is_empty() {
            return Self {
                page_prefix: Vec::new(),
                compressed_keys: Vec::new(),
            };
        }
        
        // Find longest common prefix among all keys
        let page_prefix = Self::find_common_prefix(keys);
        
        // Compress each key against page prefix
        let compressed_keys: Vec<_> = keys.iter()
            .map(|key| {
                let suffix = key[page_prefix.len()..].to_vec();
                CompressedKey {
                    prefix_len: page_prefix.len() as u16,
                    suffix,
                }
            })
            .collect();
        
        Self {
            page_prefix,
            compressed_keys,
        }
    }
    
    /// Find longest common prefix among all keys
    fn find_common_prefix(keys: &[Vec<u8>]) -> Vec<u8> {
        if keys.is_empty() {
            return Vec::new();
        }
        
        let first = &keys[0];
        let mut prefix_len = first.len();
        
        for key in &keys[1..] {
            prefix_len = prefix_len.min(
                first.iter()
                    .zip(key.iter())
                    .take_while(|(a, b)| a == b)
                    .count()
            );
            
            if prefix_len == 0 {
                break;
            }
        }
        
        first[..prefix_len].to_vec()
    }
    
    /// Get page prefix
    pub fn page_prefix(&self) -> &[u8] {
        &self.page_prefix
    }
    
    /// Get compressed keys
    pub fn compressed_keys(&self) -> &[CompressedKey] {
        &self.compressed_keys
    }
    
    /// Decompress all keys
    pub fn decompress_all(&self) -> Vec<Vec<u8>> {
        self.compressed_keys.iter()
            .map(|ck| ck.decompress(&self.page_prefix))
            .collect()
    }
    
    /// Get specific key by index
    pub fn get_key(&self, index: usize) -> Option<Vec<u8>> {
        self.compressed_keys.get(index)
            .map(|ck| ck.decompress(&self.page_prefix))
    }
    
    /// Total compression statistics
    pub fn stats(&self) -> CompressionStats {
        let total_uncompressed: usize = self.compressed_keys.iter()
            .map(|ck| ck.uncompressed_size())
            .sum();
        
        let total_compressed: usize = self.compressed_keys.iter()
            .map(|ck| ck.compressed_size())
            .sum::<usize>() + self.page_prefix.len();
        
        CompressionStats {
            key_count: self.compressed_keys.len(),
            page_prefix_len: self.page_prefix.len(),
            total_uncompressed,
            total_compressed,
            space_saved: total_uncompressed.saturating_sub(total_compressed),
            compression_ratio: if total_compressed > 0 {
                total_uncompressed as f64 / total_compressed as f64
            } else {
                1.0
            },
        }
    }
}

/// Compression statistics
#[derive(Debug, Clone)]
pub struct CompressionStats {
    pub key_count: usize,
    pub page_prefix_len: usize,
    pub total_uncompressed: usize,
    pub total_compressed: usize,
    pub space_saved: usize,
    pub compression_ratio: f64,
}

impl std::fmt::Display for CompressionStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Prefix Compression: {} keys, {:.1}% saved ({} -> {} bytes, {:.2}x)",
            self.key_count,
            (1.0 - 1.0 / self.compression_ratio) * 100.0,
            self.total_uncompressed,
            self.total_compressed,
            self.compression_ratio
        )
    }
}

/// Delta compression for sequential integers
/// Stores the difference between consecutive values
pub struct DeltaCompressor;

impl DeltaCompressor {
    /// Compress a sequence of integers
    pub fn compress(values: &[u64]) -> (u64, Vec<u64>) {
        if values.is_empty() {
            return (0, Vec::new());
        }
        
        let base = values[0];
        let deltas: Vec<u64> = values.windows(2)
            .map(|w| w[1] - w[0])
            .collect();
        
        (base, deltas)
    }
    
    /// Decompress a sequence
    pub fn decompress(base: u64, deltas: &[u64]) -> Vec<u64> {
        let mut result = vec![base];
        let mut current = base;
        
        for delta in deltas {
            current += delta;
            result.push(current);
        }
        
        result
    }
    
    /// Calculate potential space savings
    pub fn estimated_savings(values: &[u64]) -> (usize, usize) {
        let (base, deltas) = Self::compress(values);
        
        let original_size = values.len() * 8; // 8 bytes per u64
        let compressed_size = 8 + deltas.len() * 8; // base + deltas
        
        (original_size, compressed_size)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compressed_key() {
        let key1 = b"https://example.com/page1";
        let key2 = b"https://example.com/page2";
        
        let compressed = CompressedKey::compress(key2, key1);
        
        // Should share prefix "https://example.com/page"
        assert!(compressed.prefix_len > 0);
        assert_eq!(compressed.suffix, b"2");
        
        // Decompress should give original
        let decompressed = compressed.decompress(key1);
        assert_eq!(decompressed, key2);
    }

    #[test]
    fn test_page_prefix_compressor() {
        let keys = vec![
            b"user_alice".to_vec(),
            b"user_bob".to_vec(),
            b"user_charlie".to_vec(),
            b"user_david".to_vec(),
        ];
        
        let compressor = PagePrefixCompressor::new(&keys);
        
        // Should find "user_" as common prefix
        assert_eq!(compressor.page_prefix(), b"user_");
        
        // Decompress all should match original
        let decompressed = compressor.decompress_all();
        assert_eq!(decompressed, keys);
        
        // Should save space
        let stats = compressor.stats();
        assert!(stats.space_saved > 0);
        assert!(stats.compression_ratio > 1.0);
    }

    #[test]
    fn test_no_common_prefix() {
        let keys = vec![
            b"alice".to_vec(),
            b"bob".to_vec(),
            b"charlie".to_vec(),
        ];
        
        let compressor = PagePrefixCompressor::new(&keys);
        
        // No common prefix
        assert!(compressor.page_prefix().is_empty());
        
        // Should still work
        let decompressed = compressor.decompress_all();
        assert_eq!(decompressed, keys);
    }

    #[test]
    fn test_delta_compressor() {
        let values = vec![100, 101, 103, 106, 110, 115];
        
        let (base, deltas) = DeltaCompressor::compress(&values);
        assert_eq!(base, 100);
        assert_eq!(deltas, vec![1, 2, 3, 4, 5]);
        
        let decompressed = DeltaCompressor::decompress(base, &deltas);
        assert_eq!(decompressed, values);
    }

    #[test]
    fn test_compression_stats() {
        let keys = vec![
            b"2024-01-01T00:00:00Z_event1".to_vec(),
            b"2024-01-01T00:00:00Z_event2".to_vec(),
            b"2024-01-01T00:00:00Z_event3".to_vec(),
        ];
        
        let compressor = PagePrefixCompressor::new(&keys);
        let stats = compressor.stats();
        
        println!("{}", stats);
        
        assert!(stats.compression_ratio > 2.0, "Should achieve >2x compression");
    }
}
