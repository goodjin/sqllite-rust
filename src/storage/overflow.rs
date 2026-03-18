//! B-tree Storage Engine - Phase 3: Overflow Page Support
//!
//! This module implements overflow pages for storing large records
//! that don't fit within a single data page (up to 4KB per record).
//!
//! Overflow Page Layout:
//! - Page header (96 bytes)
//! - data_size: u32 (4 bytes) - amount of data in this page
//! - next_overflow_page: PageId (4 bytes) - link to next overflow page
//! - data: up to 3992 bytes

use crate::pager::{PageId, Pager};
use crate::pager::page::PAGE_SIZE;
use crate::pager::page::Page;
use crate::storage::{Result, StorageError};
use crate::storage::btree_engine::{PageHeader, PageType, RecordHeader, BtreePageOps};

/// Maximum data per overflow page
pub const OVERFLOW_DATA_SIZE: usize = PAGE_SIZE - 96 - 4 - 4; // 3992 bytes

/// Overflow page header (follows the page header)
#[derive(Debug, Clone, Copy)]
pub struct OverflowHeader {
    pub data_size: u32,           // Amount of data stored in this page
    pub next_page: PageId,        // Next overflow page in chain (0 if none)
}

impl OverflowHeader {
    pub const SIZE: usize = 8; // 4 + 4 bytes

    pub fn new(data_size: u32, next_page: PageId) -> Self {
        Self {
            data_size,
            next_page,
        }
    }

    pub fn to_bytes(&self) -> [u8; Self::SIZE] {
        let mut bytes = [0u8; Self::SIZE];
        bytes[0..4].copy_from_slice(&self.data_size.to_le_bytes());
        bytes[4..8].copy_from_slice(&self.next_page.to_le_bytes());
        bytes
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < Self::SIZE {
            return Err(StorageError::Corrupted("Overflow header too small".to_string()));
        }
        Ok(Self {
            data_size: u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]),
            next_page: u32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]),
        })
    }
}

/// Manages overflow page chains for large records
pub struct OverflowManager;

impl OverflowManager {
    /// Create a new overflow manager
    pub fn new() -> Self {
        Self
    }

    /// Calculate the number of overflow pages needed for data of given size
    pub fn pages_needed(data_size: usize) -> usize {
        if data_size == 0 {
            return 0;
        }
        (data_size + OVERFLOW_DATA_SIZE - 1) / OVERFLOW_DATA_SIZE
    }

    /// Write large data to overflow pages
    /// Returns the first overflow page ID
    pub fn write_overflow_data(
        &self,
        pager: &mut Pager,
        data: &[u8],
    ) -> Result<PageId> {
        if data.is_empty() {
            return Ok(0);
        }

        let num_pages = Self::pages_needed(data.len());
        let mut first_page_id: PageId = 0;
        let mut prev_page_id: PageId = 0;

        for i in 0..num_pages {
            // Allocate overflow page
            let page_id = pager.allocate_page()?;

            if i == 0 {
                first_page_id = page_id;
            }

            // Calculate data slice for this page
            let start = i * OVERFLOW_DATA_SIZE;
            let end = ((i + 1) * OVERFLOW_DATA_SIZE).min(data.len());
            let page_data = &data[start..end];

            // Initialize overflow page
            let mut page = Page::new(page_id);

            // Write page header
            let header = PageHeader::new(PageType::Overflow);
            let header_bytes = header.to_bytes();
            page.as_mut_slice()[0..96].copy_from_slice(&header_bytes);

            // Write overflow header
            let overflow_header = OverflowHeader::new(
                page_data.len() as u32,
                0, // Will be updated if there's a next page
            );
            page.as_mut_slice()[96..96 + OverflowHeader::SIZE]
                .copy_from_slice(&overflow_header.to_bytes());

            // Write data
            let data_start = 96 + OverflowHeader::SIZE;
            page.as_mut_slice()[data_start..data_start + page_data.len()]
                .copy_from_slice(page_data);

            pager.write_page(&page)?;

            // Update previous page's next pointer
            if prev_page_id != 0 {
                let mut prev_page = pager.get_page(prev_page_id)?;
                let data_offset = 96 + 4; // Skip to next_page field
                prev_page.as_mut_slice()[data_offset..data_offset + 4]
                    .copy_from_slice(&page_id.to_le_bytes());
                pager.write_page(&prev_page)?;
            }

            prev_page_id = page_id;
        }

        Ok(first_page_id)
    }

    /// Read large data from overflow pages
    pub fn read_overflow_data(
        &self,
        pager: &mut Pager,
        first_page_id: PageId,
        total_size: usize,
    ) -> Result<Vec<u8>> {
        if first_page_id == 0 {
            return Ok(Vec::new());
        }

        let mut result = Vec::with_capacity(total_size);
        let mut current_page_id = first_page_id;

        while current_page_id != 0 {
            let page = pager.get_page(current_page_id)?;

            // Read overflow header
            let overflow_header = OverflowHeader::from_bytes(
                &page.as_slice()[96..96 + OverflowHeader::SIZE]
            )?;

            // Read data
            let data_start = 96 + OverflowHeader::SIZE;
            let data_end = data_start + overflow_header.data_size as usize;
            result.extend_from_slice(&page.as_slice()[data_start..data_end]);

            current_page_id = overflow_header.next_page;
        }

        // Truncate to expected size
        result.truncate(total_size);

        Ok(result)
    }

    /// Free a chain of overflow pages
    pub fn free_overflow_chain(
        &self,
        pager: &mut Pager,
        first_page_id: PageId,
    ) -> Result<()> {
        let mut current_page_id = first_page_id;

        while current_page_id != 0 {
            let page = pager.get_page(current_page_id)?;

            // Read next page before potentially modifying
            let overflow_header = OverflowHeader::from_bytes(
                &page.as_slice()[96..96 + OverflowHeader::SIZE]
            )?;
            let next_page_id = overflow_header.next_page;

            // Mark page as free
            // For now, we just leave it allocated
            // TODO: Add to free list in Phase 4

            current_page_id = next_page_id;
        }

        Ok(())
    }

    /// Update overflow data (may require reallocating pages)
    pub fn update_overflow_data(
        &self,
        pager: &mut Pager,
        old_first_page: PageId,
        new_data: &[u8],
    ) -> Result<PageId> {
        // Free old chain
        self.free_overflow_chain(pager, old_first_page)?;

        // Write new data
        self.write_overflow_data(pager, new_data)
    }
}

/// Split a large record into inline portion and overflow data
pub struct RecordSplitter;

impl RecordSplitter {
    /// Maximum size for inline portion of a record
    /// This leaves room for the record header and some data
    pub const MAX_INLINE_DATA: usize = 100; // Small inline portion

    /// Split a record into inline and overflow portions
    /// Returns (inline_key, inline_value, overflow_data)
    /// If the record fits entirely inline, overflow_data is None
    pub fn split_record(
        key: &[u8],
        value: &[u8],
    ) -> (Vec<u8>, Vec<u8>, Option<Vec<u8>>) {
        let total_size = key.len() + value.len() + RecordHeader::SIZE;

        // Check if record fits inline
        if total_size <= crate::storage::btree_engine::MAX_INLINE_SIZE {
            return (key.to_vec(), value.to_vec(), None);
        }

        // Need to split - keep key inline, move part/all of value to overflow
        let inline_value_size = Self::MAX_INLINE_DATA.saturating_sub(key.len() + RecordHeader::SIZE);

        if inline_value_size >= value.len() {
            // Key is large, value fits inline
            let inline_key = key[..key.len().min(Self::MAX_INLINE_DATA)].to_vec();
            let overflow_key = key[inline_key.len()..].to_vec();

            // Store overflow portion: remaining key + value
            let mut overflow_data = overflow_key;
            overflow_data.extend_from_slice(value);

            (inline_key, value.to_vec(), Some(overflow_data))
        } else {
            // Value needs to be split
            let inline_value = value[..inline_value_size].to_vec();
            let overflow_value = value[inline_value_size..].to_vec();

            (key.to_vec(), inline_value, Some(overflow_value))
        }
    }

    /// Reconstruct a complete record from inline and overflow portions
    pub fn reconstruct_record(
        inline_key: &[u8],
        inline_value: &[u8],
        overflow_data: Option<&[u8]>,
    ) -> (Vec<u8>, Vec<u8>) {
        match overflow_data {
            None => (inline_key.to_vec(), inline_value.to_vec()),
            Some(data) => {
                // Check if key was split
                let key_len = u16::from_le_bytes([data[0], data[1]]) as usize;

                if key_len > 0 {
                    // Key was split
                    let full_key = [inline_key, &data[2..2 + key_len]].concat();
                    let full_value = [&data[2 + key_len..], inline_value].concat();
                    (full_key, full_value)
                } else {
                    // Only value was split
                    let full_value = [inline_value, data].concat();
                    (inline_key.to_vec(), full_value)
                }
            }
        }
    }
}

/// Extension trait for BtreePageOps to handle overflow records
pub trait OverflowPageOps {
    /// Insert a record that may require overflow pages
    fn insert_record_with_overflow(
        &mut self,
        pager: &mut Pager,
        key: &[u8],
        value: &[u8],
        overflow_mgr: &OverflowManager,
    ) -> Result<()>;

    /// Read a record, handling overflow if necessary
    fn read_record_with_overflow(
        &self,
        pager: &mut Pager,
        slot_idx: usize,
        overflow_mgr: &OverflowManager,
    ) -> Result<(Vec<u8>, Vec<u8>)>;
}

impl OverflowPageOps for Page {
    fn insert_record_with_overflow(
        &mut self,
        pager: &mut Pager,
        key: &[u8],
        value: &[u8],
        overflow_mgr: &OverflowManager,
    ) -> Result<()> {
        let total_size = key.len() + value.len();

        if total_size <= crate::storage::btree_engine::MAX_INLINE_SIZE {
            // Record fits inline, use normal insert
            self.insert_record(key, value)?;
        } else {
            // Need overflow pages
            let (inline_key, inline_value, overflow_data) = RecordSplitter::split_record(key, value);

            let overflow_page_id = if let Some(data) = overflow_data {
                overflow_mgr.write_overflow_data(pager, &data)?
            } else {
                0
            };

            // Insert inline portion with overflow pointer
            // We need to modify the record header to include overflow info
            // For now, we'll prepend the overflow page ID to the inline value
            let mut modified_value = overflow_page_id.to_le_bytes().to_vec();
            modified_value.extend_from_slice(&inline_value);

            self.insert_record(&inline_key, &modified_value)?;
        }

        Ok(())
    }

    fn read_record_with_overflow(
        &self,
        pager: &mut Pager,
        slot_idx: usize,
        overflow_mgr: &OverflowManager,
    ) -> Result<(Vec<u8>, Vec<u8>)> {
        let (key, value) = self.get_record_at(slot_idx)?;

        // Check if this record has overflow
        if value.len() >= 4 {
            let potential_overflow_id = u32::from_le_bytes([value[0], value[1], value[2], value[3]]);

            // Heuristic: if first 4 bytes look like a valid page ID and rest looks like data
            // This is a simplification - in production, use a flag in record header
            if potential_overflow_id > 0 && potential_overflow_id < 1000000 {
                // Likely has overflow data
                let inline_value = &value[4..];

                // Read overflow data
                let overflow_data = overflow_mgr.read_overflow_data(
                    pager,
                    potential_overflow_id,
                    4000, // Max expected size
                )?;

                let (full_key, full_value) = RecordSplitter::reconstruct_record(
                    &key,
                    inline_value,
                    Some(&overflow_data),
                );

                return Ok((full_key, full_value));
            }
        }

        // No overflow, return as-is
        Ok((key, value))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    fn create_test_pager() -> (Pager, String) {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap().to_string();
        let pager = Pager::open(&path).unwrap();
        (pager, path)
    }

    #[test]
    fn test_overflow_pages_needed() {
        assert_eq!(OverflowManager::pages_needed(0), 0);
        assert_eq!(OverflowManager::pages_needed(100), 1);
        assert_eq!(OverflowManager::pages_needed(OVERFLOW_DATA_SIZE), 1);
        assert_eq!(OverflowManager::pages_needed(OVERFLOW_DATA_SIZE + 1), 2);
        assert_eq!(OverflowManager::pages_needed(OVERFLOW_DATA_SIZE * 2), 2);
        assert_eq!(OverflowManager::pages_needed(OVERFLOW_DATA_SIZE * 2 + 1), 3);
    }

    #[test]
    fn test_overflow_write_and_read() {
        let (mut pager, _path) = create_test_pager();
        let overflow_mgr = OverflowManager::new();

        // Create test data larger than one overflow page
        let test_data: Vec<u8> = (0..OVERFLOW_DATA_SIZE * 2 + 100)
            .map(|i| (i % 256) as u8)
            .collect();

        // Write to overflow pages
        let first_page_id = overflow_mgr.write_overflow_data(&mut pager, &test_data).unwrap();
        assert!(first_page_id > 0);

        // Read back
        let read_data = overflow_mgr.read_overflow_data(&mut pager, first_page_id, test_data.len()).unwrap();
        assert_eq!(read_data, test_data);
    }

    #[test]
    fn test_record_splitter() {
        // Small record - no split needed
        let (k, v, overflow) = RecordSplitter::split_record(b"key", b"value");
        assert_eq!(k, b"key");
        assert_eq!(v, b"value");
        assert!(overflow.is_none());

        // Large record - needs split
        let large_value = vec![0u8; 3000];
        let (k, v, overflow) = RecordSplitter::split_record(b"key", &large_value);
        assert_eq!(k, b"key");
        assert!(v.len() < large_value.len());
        assert!(overflow.is_some());
    }

    #[test]
    fn test_overflow_header_serialization() {
        let header = OverflowHeader::new(1000, 42);
        let bytes = header.to_bytes();
        let parsed = OverflowHeader::from_bytes(&bytes).unwrap();

        assert_eq!(parsed.data_size, header.data_size);
        assert_eq!(parsed.next_page, header.next_page);
    }
}
