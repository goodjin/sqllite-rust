//! Prefix Compression Page Operations
//!
//! This module integrates prefix compression with B-tree pages.
//! Each page stores a common prefix in the header, and records store only the suffix.

use crate::pager::PageId;
use crate::pager::page::PAGE_SIZE;
use crate::pager::page::Page;
use crate::storage::{Result, StorageError};
use crate::storage::btree_engine::{
    PageHeader, PageType, BtreePageOps, compare_keys,
};
use std::cmp::Ordering;

/// Extended page header for prefix compression (128 bytes total)
/// 
/// Layout:
/// - Base PageHeader: 96 bytes
/// - prefix_len: u16 (2 bytes) - length of common prefix
/// - prefix_offset: u16 (2 bytes) - offset to prefix data in page
/// - flags2: u8 (1 byte) - bit0=prefix_compression_enabled
/// - _reserved: [u8; 27] (27 bytes)
#[derive(Debug, Clone, Copy)]
pub struct PrefixPageHeader {
    pub base: PageHeader,
    pub prefix_len: u16,
    pub prefix_offset: u16,
    pub flags2: u8,
    pub _reserved: [u8; 27],
}

impl PrefixPageHeader {
    pub const SIZE: usize = 128;
    
    // Flags2 bits
    pub const FLAG_PREFIX_COMPRESSION: u8 = 0x01;
    
    pub fn new(page_type: PageType) -> Self {
        Self {
            base: PageHeader::new(page_type),
            prefix_len: 0,
            prefix_offset: 0,
            flags2: 0,
            _reserved: [0; 27],
        }
    }
    
    pub fn is_prefix_compression_enabled(&self) -> bool {
        (self.flags2 & Self::FLAG_PREFIX_COMPRESSION) != 0
    }
    
    pub fn set_prefix_compression(&mut self, enabled: bool) {
        if enabled {
            self.flags2 |= Self::FLAG_PREFIX_COMPRESSION;
        } else {
            self.flags2 &= !Self::FLAG_PREFIX_COMPRESSION;
        }
    }
    
    /// Serialize to bytes
    pub fn to_bytes(&self) -> [u8; Self::SIZE] {
        let mut bytes = [0u8; Self::SIZE];
        let mut pos = 0;
        
        // Base header (96 bytes)
        bytes[pos..pos+PageHeader::SIZE].copy_from_slice(&self.base.to_bytes());
        pos += PageHeader::SIZE;
        
        // prefix_len (2 bytes)
        bytes[pos..pos+2].copy_from_slice(&self.prefix_len.to_le_bytes());
        pos += 2;
        
        // prefix_offset (2 bytes)
        bytes[pos..pos+2].copy_from_slice(&self.prefix_offset.to_le_bytes());
        pos += 2;
        
        // flags2 (1 byte)
        bytes[pos] = self.flags2;
        pos += 1;
        
        // reserved (27 bytes)
        bytes[pos..pos+27].copy_from_slice(&self._reserved);
        
        bytes
    }
    
    /// Deserialize from bytes
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < Self::SIZE {
            return Err(StorageError::Corrupted("Prefix page header too small".to_string()));
        }
        
        let mut pos = 0;
        
        let base = PageHeader::from_bytes(&bytes[pos..pos+PageHeader::SIZE])?;
        pos += PageHeader::SIZE;
        
        let prefix_len = u16::from_le_bytes([bytes[pos], bytes[pos+1]]);
        pos += 2;
        
        let prefix_offset = u16::from_le_bytes([bytes[pos], bytes[pos+1]]);
        pos += 2;
        
        let flags2 = bytes[pos];
        pos += 1;
        
        let mut reserved = [0u8; 27];
        reserved.copy_from_slice(&bytes[pos..pos+27]);
        
        Ok(Self {
            base,
            prefix_len,
            prefix_offset,
            flags2,
            _reserved: reserved,
        })
    }
}

/// Compressed record header - stores only suffix info
#[derive(Debug, Clone, Copy)]
pub struct CompressedRecordHeader {
    pub total_size: u32,
    pub suffix_len: u16,      // Length of key suffix (instead of full key)
    pub value_size: u16,
    pub flags: u16,
    pub overflow_page: PageId,
}

impl CompressedRecordHeader {
    pub const SIZE: usize = 16;
    pub const FLAG_DELETED: u16 = 0x01;
    
    pub fn new(suffix_len: u16, value_size: u16) -> Self {
        Self {
            total_size: (suffix_len as u32) + (value_size as u32) + Self::SIZE as u32,
            suffix_len,
            value_size,
            flags: 0,
            overflow_page: 0,
        }
    }
    
    pub fn is_deleted(&self) -> bool {
        (self.flags & Self::FLAG_DELETED) != 0
    }
    
    pub fn to_bytes(&self) -> [u8; Self::SIZE] {
        let mut bytes = [0u8; Self::SIZE];
        let mut pos = 0;
        
        bytes[pos..pos+4].copy_from_slice(&self.total_size.to_le_bytes());
        pos += 4;
        bytes[pos..pos+2].copy_from_slice(&self.suffix_len.to_le_bytes());
        pos += 2;
        bytes[pos..pos+2].copy_from_slice(&self.value_size.to_le_bytes());
        pos += 2;
        bytes[pos..pos+2].copy_from_slice(&self.flags.to_le_bytes());
        pos += 2;
        bytes[pos..pos+4].copy_from_slice(&self.overflow_page.to_le_bytes());
        
        bytes
    }
    
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < Self::SIZE {
            return Err(StorageError::Corrupted("Compressed record header too small".to_string()));
        }
        
        Ok(Self {
            total_size: u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]),
            suffix_len: u16::from_le_bytes([bytes[4], bytes[5]]),
            value_size: u16::from_le_bytes([bytes[6], bytes[7]]),
            flags: u16::from_le_bytes([bytes[8], bytes[9]]),
            overflow_page: u32::from_le_bytes([bytes[10], bytes[11], bytes[12], bytes[13]]),
        })
    }
}

/// Find the longest common prefix among a set of keys
pub fn find_common_prefix(keys: &[Vec<u8>]) -> Vec<u8> {
    if keys.is_empty() {
        return Vec::new();
    }
    if keys.len() == 1 {
        return keys[0].clone();
    }
    
    let first = &keys[0];
    let mut prefix_len = first.len();
    
    for key in &keys[1..] {
        let mut common = 0;
        for (i, (a, b)) in first.iter().zip(key.iter()).enumerate() {
            if a != b {
                break;
            }
            common = i + 1;
        }
        prefix_len = prefix_len.min(common);
        
        if prefix_len == 0 {
            break;
        }
    }
    
    first[..prefix_len].to_vec()
}

/// Compress keys against a common prefix
pub fn compress_keys(keys: &[Vec<u8>], prefix: &[u8]) -> Vec<Vec<u8>> {
    keys.iter()
        .map(|key| {
            if key.starts_with(prefix) {
                key[prefix.len()..].to_vec()
            } else {
                key.clone() // Fallback: store full key if it doesn't share prefix
            }
        })
        .collect()
}

/// Decompress a suffix using the common prefix
pub fn decompress_key(suffix: &[u8], prefix: &[u8]) -> Vec<u8> {
    let mut result = Vec::with_capacity(prefix.len() + suffix.len());
    result.extend_from_slice(prefix);
    result.extend_from_slice(suffix);
    result
}

/// B-tree configuration for prefix compression
#[derive(Debug, Clone, Copy)]
pub struct BtreeConfig {
    pub enable_prefix_compression: bool,
    pub min_prefix_ratio: f64,  // Minimum ratio of prefix to key length to enable compression
}

impl Default for BtreeConfig {
    fn default() -> Self {
        Self {
            enable_prefix_compression: false, // Disabled by default until tested
            min_prefix_ratio: 0.3,            // At least 30% common prefix
        }
    }
}

impl BtreeConfig {
    pub fn with_prefix_compression(mut self, enabled: bool) -> Self {
        self.enable_prefix_compression = enabled;
        self
    }
}

/// Trait for prefix compression page operations
pub trait PrefixCompressionOps {
    /// Enable prefix compression on this page
    fn enable_prefix_compression(&mut self, keys: &[Vec<u8>]) -> Result<()>;
    
    /// Check if prefix compression is enabled
    fn is_prefix_compression_enabled(&self) -> Result<bool>;
    
    /// Get the page prefix
    fn get_page_prefix(&self) -> Result<Option<Vec<u8>>>;
    
    /// Insert a record with prefix compression
    fn insert_compressed_record(&mut self, key: &[u8], value: &[u8], prefix: &[u8]) -> Result<()>;
    
    /// Get a record with decompression
    fn get_decompressed_record(&self, slot_idx: usize, prefix: &[u8]) -> Result<(Vec<u8>, Vec<u8>)>;
    
    /// Calculate space savings from compression
    fn calculate_compression_stats(&self) -> Result<PrefixCompressionStats>;
}

/// Statistics for prefix compression
#[derive(Debug, Clone)]
pub struct PrefixCompressionStats {
    pub enabled: bool,
    pub prefix_len: usize,
    pub record_count: usize,
    pub uncompressed_size: usize,
    pub compressed_size: usize,
    pub space_saved: usize,
    pub compression_ratio: f64,
}

impl PrefixCompressionOps for Page {
    fn enable_prefix_compression(&mut self, keys: &[Vec<u8>]) -> Result<()> {
        if keys.is_empty() {
            return Ok(());
        }
        
        // Find common prefix
        let prefix = find_common_prefix(keys);
        
        if prefix.is_empty() {
            return Ok(()); // No common prefix, don't enable compression
        }
        
        // Read current header
        let base_header = self.read_header()?;
        
        // Create extended header
        let mut ext_header = PrefixPageHeader {
            base: base_header,
            prefix_len: prefix.len() as u16,
            prefix_offset: (PrefixPageHeader::SIZE + 2) as u16, // After header + slot array start
            flags2: 0,
            _reserved: [0; 27],
        };
        ext_header.set_prefix_compression(true);
        
        // Write prefix at the end of the page (before record data starts)
        let prefix_storage_offset = PAGE_SIZE - prefix.len();
        self.data[prefix_storage_offset..PAGE_SIZE].copy_from_slice(&prefix);
        ext_header.prefix_offset = prefix_storage_offset as u16;
        
        // Update header
        self.data[0..PrefixPageHeader::SIZE].copy_from_slice(&ext_header.to_bytes());
        
        Ok(())
    }
    
    fn is_prefix_compression_enabled(&self) -> Result<bool> {
        // Check if extended header exists (flags2 byte at position 100)
        if self.data.len() < PrefixPageHeader::SIZE {
            return Ok(false);
        }
        
        let flags2 = self.data[100]; // Position after base header (96) + prefix_len (2) + prefix_offset (2)
        Ok((flags2 & PrefixPageHeader::FLAG_PREFIX_COMPRESSION) != 0)
    }
    
    fn get_page_prefix(&self) -> Result<Option<Vec<u8>>> {
        if !self.is_prefix_compression_enabled()? {
            return Ok(None);
        }
        
        let ext_header = PrefixPageHeader::from_bytes(&self.data[0..PrefixPageHeader::SIZE])?;
        let prefix_len = ext_header.prefix_len as usize;
        let prefix_offset = ext_header.prefix_offset as usize;
        
        if prefix_len == 0 || prefix_offset + prefix_len > PAGE_SIZE {
            return Ok(None);
        }
        
        Ok(Some(self.data[prefix_offset..prefix_offset + prefix_len].to_vec()))
    }
    
    fn insert_compressed_record(&mut self, key: &[u8], value: &[u8], prefix: &[u8]) -> Result<()> {
        let mut ext_header = PrefixPageHeader::from_bytes(&self.data[0..PrefixPageHeader::SIZE])?;
        
        // Extract suffix
        let suffix = if key.starts_with(prefix) {
            &key[prefix.len()..]
        } else {
            key // Store full key if it doesn't match prefix (rare case)
        };
        
        let suffix_len = suffix.len();
        let value_size = value.len();
        let total_record_size = CompressedRecordHeader::SIZE + suffix_len + value_size;
        
        // Check space
        let required_space = total_record_size + 2; // +2 for slot entry
        if (ext_header.base.free_size as usize) < required_space {
            return Err(StorageError::PageFull);
        }
        
        // Find insertion position
        let prefix_bytes = self.get_page_prefix()?.unwrap_or_default();
        let mut insert_idx = 0;
        while insert_idx < ext_header.base.record_count as usize {
            let slot_offset = PrefixPageHeader::SIZE + insert_idx * 2;
            let record_offset = u16::from_le_bytes([
                self.data[slot_offset],
                self.data[slot_offset + 1]
            ]) as usize;
            
            let rec_header = CompressedRecordHeader::from_bytes(&self.data[record_offset..])?;
            let suffix_start = record_offset + CompressedRecordHeader::SIZE;
            let suffix_end = suffix_start + rec_header.suffix_len as usize;
            let stored_suffix = &self.data[suffix_start..suffix_end];
            let stored_key = decompress_key(stored_suffix, &prefix_bytes);
            
            if compare_keys(key, &stored_key) == Ordering::Less {
                break;
            }
            insert_idx += 1;
        }
        
        // Move existing slots
        if insert_idx < ext_header.base.record_count as usize {
            let src = PrefixPageHeader::SIZE + insert_idx * 2;
            let dst = PrefixPageHeader::SIZE + (insert_idx + 1) * 2;
            let len = (ext_header.base.record_count as usize - insert_idx) * 2;
            self.data.copy_within(src..src + len, dst);
        }
        
        // Calculate record position
        let record_offset = (ext_header.base.free_offset as usize + 1) - total_record_size;
        
        // Write compressed record header
        let rec_header = CompressedRecordHeader::new(suffix_len as u16, value_size as u16);
        self.data[record_offset..record_offset + CompressedRecordHeader::SIZE]
            .copy_from_slice(&rec_header.to_bytes());
        
        // Write suffix and value
        let suffix_start = record_offset + CompressedRecordHeader::SIZE;
        self.data[suffix_start..suffix_start + suffix_len].copy_from_slice(suffix);
        self.data[suffix_start + suffix_len..suffix_start + suffix_len + value_size].copy_from_slice(value);
        
        // Update slot array
        let slot_offset = PrefixPageHeader::SIZE + insert_idx * 2;
        self.data[slot_offset..slot_offset + 2]
            .copy_from_slice(&(record_offset as u16).to_le_bytes());
        
        // Update header
        ext_header.base.record_count += 1;
        ext_header.base.free_offset = record_offset as u16 - 1;
        ext_header.base.free_size -= required_space as u16;
        self.data[0..PrefixPageHeader::SIZE].copy_from_slice(&ext_header.to_bytes());
        
        Ok(())
    }
    
    fn get_decompressed_record(&self, slot_idx: usize, prefix: &[u8]) -> Result<(Vec<u8>, Vec<u8>)> {
        let ext_header = PrefixPageHeader::from_bytes(&self.data[0..PrefixPageHeader::SIZE])?;
        
        if slot_idx >= ext_header.base.record_count as usize {
            return Err(StorageError::KeyNotFound);
        }
        
        // Read slot offset
        let slot_offset = PrefixPageHeader::SIZE + slot_idx * 2;
        let record_offset = u16::from_le_bytes([
            self.data[slot_offset],
            self.data[slot_offset + 1]
        ]) as usize;
        
        // Read compressed record header
        let rec_header = CompressedRecordHeader::from_bytes(&self.data[record_offset..])?;
        
        if rec_header.is_deleted() {
            return Err(StorageError::KeyNotFound);
        }
        
        // Extract suffix and value
        let suffix_start = record_offset + CompressedRecordHeader::SIZE;
        let suffix_end = suffix_start + rec_header.suffix_len as usize;
        let value_end = suffix_end + rec_header.value_size as usize;
        
        let suffix = &self.data[suffix_start..suffix_end];
        let value = self.data[suffix_end..value_end].to_vec();
        
        // Decompress key
        let key = decompress_key(suffix, prefix);
        
        Ok((key, value))
    }
    
    fn calculate_compression_stats(&self) -> Result<PrefixCompressionStats> {
        let enabled = self.is_prefix_compression_enabled()?;
        
        if !enabled {
            return Ok(PrefixCompressionStats {
                enabled: false,
                prefix_len: 0,
                record_count: 0,
                uncompressed_size: 0,
                compressed_size: 0,
                space_saved: 0,
                compression_ratio: 1.0,
            });
        }
        
        let ext_header = PrefixPageHeader::from_bytes(&self.data[0..PrefixPageHeader::SIZE])?;
        let prefix = self.get_page_prefix()?.unwrap_or_default();
        let prefix_len = prefix.len();
        let record_count = ext_header.base.record_count as usize;
        
        // Calculate sizes
        let uncompressed_size = record_count * prefix_len; // What we saved by not storing prefix per key
        let compressed_size = PrefixPageHeader::SIZE - PageHeader::SIZE + prefix_len; // Extra header + stored prefix
        let space_saved = uncompressed_size.saturating_sub(compressed_size);
        let compression_ratio = if compressed_size > 0 {
            (uncompressed_size + compressed_size) as f64 / compressed_size.max(1) as f64
        } else {
            1.0
        };
        
        Ok(PrefixCompressionStats {
            enabled: true,
            prefix_len,
            record_count,
            uncompressed_size,
            compressed_size,
            space_saved,
            compression_ratio,
        })
    }
}

/// Compress a page's records using prefix compression
pub fn compress_page(page: &mut Page, config: &BtreeConfig) -> Result<bool> {
    if !config.enable_prefix_compression {
        return Ok(false);
    }
    
    // Check if already compressed
    if page.is_prefix_compression_enabled()? {
        return Ok(false);
    }
    
    // Get all existing records
    let records = page.get_all_records()?;
    if records.len() < 2 {
        return Ok(false); // Not enough records to justify compression
    }
    
    // Extract keys
    let keys: Vec<Vec<u8>> = records.iter().map(|(k, _)| k.clone()).collect();
    
    // Find common prefix
    let prefix = find_common_prefix(&keys);
    
    // Check if compression is worthwhile
    let avg_key_len = keys.iter().map(|k| k.len()).sum::<usize>() as f64 / keys.len() as f64;
    let prefix_ratio = prefix.len() as f64 / avg_key_len;
    
    if prefix_ratio < config.min_prefix_ratio || prefix.len() < 4 {
        return Ok(false); // Not enough common prefix
    }
    
    // Enable compression on the page
    page.enable_prefix_compression(&keys)?;
    
    // Get the stored prefix
    let stored_prefix = page.get_page_prefix()?.unwrap_or_default();
    
    // Re-insert all records with compression
    // Note: In a real implementation, we'd do this in-place for efficiency
    // For now, clear and re-insert
    let header = PageHeader::new(PageType::Data);
    page.write_header(&header)?;
    page.enable_prefix_compression(&keys)?;
    
    for (key, value) in records {
        page.insert_compressed_record(&key, &value, &stored_prefix)?;
    }
    
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_find_common_prefix() {
        let keys = vec![
            b"user:001:profile".to_vec(),
            b"user:002:profile".to_vec(),
            b"user:003:profile".to_vec(),
        ];
        
        let prefix = find_common_prefix(&keys);
        assert_eq!(prefix, b"user:00");
        
        // Test with empty prefix
        let keys2 = vec![
            b"alice".to_vec(),
            b"bob".to_vec(),
        ];
        assert!(find_common_prefix(&keys2).is_empty());
    }
    
    #[test]
    fn test_compress_decompress_keys() {
        let keys = vec![
            b"user:001".to_vec(),
            b"user:002".to_vec(),
            b"user:003".to_vec(),
        ];
        
        let prefix = find_common_prefix(&keys);
        assert_eq!(prefix, b"user:00");
        
        let compressed = compress_keys(&keys, &prefix);
        assert_eq!(compressed, vec![b"1", b"2", b"3"]);
        
        // Decompress
        for (i, suffix) in compressed.iter().enumerate() {
            let decompressed = decompress_key(suffix, &prefix);
            assert_eq!(decompressed, keys[i]);
        }
    }
    
    #[test]
    fn test_prefix_page_header_serialization() {
        let header = PrefixPageHeader::new(PageType::Data);
        let bytes = header.to_bytes();
        assert_eq!(bytes.len(), PrefixPageHeader::SIZE);
        
        let parsed = PrefixPageHeader::from_bytes(&bytes).unwrap();
        assert_eq!(parsed.prefix_len, header.prefix_len);
        assert_eq!(parsed.flags2, header.flags2);
    }
    
    #[test]
    fn test_prefix_compression_space_savings() {
        // Create test keys with common prefix
        let keys: Vec<Vec<u8>> = (0..100)
            .map(|i| format!("user:{:08x}:profile:data", i).into_bytes())
            .collect();
        
        let prefix = find_common_prefix(&keys);
        
        // Calculate space savings
        let total_uncompressed: usize = keys.iter().map(|k| k.len()).sum();
        let compressed_suffixes = compress_keys(&keys, &prefix);
        let total_compressed: usize = compressed_suffixes.iter().map(|s| s.len()).sum();
        
        let savings_ratio = (total_uncompressed - total_compressed) as f64 / total_uncompressed as f64;
        
        println!("Prefix: {:?}", String::from_utf8_lossy(&prefix));
        println!("Uncompressed: {} bytes", total_uncompressed);
        println!("Compressed: {} bytes (prefix: {})", total_compressed + prefix.len(), prefix.len());
        println!("Savings: {:.1}%", savings_ratio * 100.0);
        
        // Should save at least 30%
        assert!(savings_ratio > 0.30, "Expected >30% space savings, got {:.1}%", savings_ratio * 100.0);
    }
    
    #[test]
    fn test_page_enable_prefix_compression() {
        let mut page = Page::new(1);
        
        // Initialize as data page
        let header = PageHeader::new(PageType::Data);
        page.write_header(&header).unwrap();
        
        // Insert records
        let keys: Vec<Vec<u8>> = vec![
            b"user:001".to_vec(),
            b"user:002".to_vec(),
            b"user:003".to_vec(),
        ];
        
        for key in &keys {
            page.insert_record(key, b"value").unwrap();
        }
        
        // Enable compression
        page.enable_prefix_compression(&keys).unwrap();
        
        // Check compression is enabled
        assert!(page.is_prefix_compression_enabled().unwrap());
        
        // Check prefix is stored correctly
        let prefix = page.get_page_prefix().unwrap().unwrap();
        assert_eq!(prefix, b"user:00");
    }
    
    #[test]
    fn test_prefix_compression_stats() {
        let mut page = Page::new(1);
        let header = PageHeader::new(PageType::Data);
        page.write_header(&header).unwrap();
        
        // Insert records with common prefix
        let keys: Vec<Vec<u8>> = (0..10)
            .map(|i| format!("user:{:03}", i).into_bytes())
            .collect();
        
        for key in &keys {
            page.insert_record(key, b"data").unwrap();
        }
        
        // Enable compression
        page.enable_prefix_compression(&keys).unwrap();
        
        // Get stats
        let stats = page.calculate_compression_stats().unwrap();
        assert!(stats.enabled);
        assert!(stats.space_saved > 0);
        assert!(stats.compression_ratio > 1.0);
        
        println!("Stats: {:?}", stats);
    }
}
