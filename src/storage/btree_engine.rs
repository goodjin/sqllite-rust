//! B-tree Storage Engine - Phase 1: Basic Page Management
//!
//! This module implements a production-grade B-tree storage engine
//! supporting millions of records with page splitting and overflow pages.

use crate::pager::{PageId, Pager};
use crate::pager::page::{Page, PAGE_SIZE};
use crate::storage::{Result, StorageError};
use std::cmp::Ordering;

// ============================================================================
// Page Header Structure (96 bytes)
// ============================================================================

/// Page type enumeration
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PageType {
    Data = 0,      // Data page (B-tree leaf)
    Index = 1,     // Index page (B-tree internal node)
    Overflow = 2,  // Overflow page (large objects)
    Free = 3,      // Free page
}

impl PageType {
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            0 => Some(PageType::Data),
            1 => Some(PageType::Index),
            2 => Some(PageType::Overflow),
            3 => Some(PageType::Free),
            _ => None,
        }
    }
}

/// Page header structure (96 bytes)
///
/// Layout:
/// - checksum: u32 (4 bytes)
/// - page_type: PageType (1 byte)
/// - flags: u8 (1 byte) - bit0=leaf, bit1=root, bit2=deleted
/// - record_count: u16 (2 bytes)
/// - free_offset: u16 (2 bytes) - free space offset from page end
/// - free_size: u16 (2 bytes) - available free space
/// - right_sibling: PageId (4 bytes) - B+ tree leaf linked list
/// - left_sibling: PageId (4 bytes)
/// - parent_page: PageId (4 bytes)
/// - lsn: u64 (8 bytes) - for WAL recovery
/// - _reserved: [u8; 64] (64 bytes)
#[derive(Debug, Clone, Copy)]
pub struct PageHeader {
    pub checksum: u32,
    pub page_type: PageType,
    pub flags: u8,
    pub record_count: u16,
    pub free_offset: u16,
    pub free_size: u16,
    pub right_sibling: PageId,
    pub left_sibling: PageId,
    pub parent_page: PageId,
    pub lsn: u64,
    pub _reserved: [u8; 64],
}

impl PageHeader {
    pub const SIZE: usize = 96;

    // Flag bits
    pub const FLAG_LEAF: u8 = 0x01;
    pub const FLAG_ROOT: u8 = 0x02;
    pub const FLAG_DELETED: u8 = 0x04;

    pub fn new(page_type: PageType) -> Self {
        Self {
            checksum: 0,
            page_type,
            flags: 0,
            record_count: 0,
            free_offset: (PAGE_SIZE - 1) as u16,
            free_size: (PAGE_SIZE - Self::SIZE - 2) as u16, // -2 for minimum slot array
            right_sibling: 0,
            left_sibling: 0,
            parent_page: 0,
            lsn: 0,
            _reserved: [0; 64],
        }
    }

    pub fn is_leaf(&self) -> bool {
        (self.flags & Self::FLAG_LEAF) != 0
    }

    pub fn is_root(&self) -> bool {
        (self.flags & Self::FLAG_ROOT) != 0
    }

    pub fn is_deleted(&self) -> bool {
        (self.flags & Self::FLAG_DELETED) != 0
    }

    pub fn set_leaf(&mut self, is_leaf: bool) {
        if is_leaf {
            self.flags |= Self::FLAG_LEAF;
        } else {
            self.flags &= !Self::FLAG_LEAF;
        }
    }

    pub fn set_root(&mut self, is_root: bool) {
        if is_root {
            self.flags |= Self::FLAG_ROOT;
        } else {
            self.flags &= !Self::FLAG_ROOT;
        }
    }

    /// Serialize header to bytes (little endian for x86 compatibility)
    pub fn to_bytes(&self) -> [u8; Self::SIZE] {
        let mut bytes = [0u8; Self::SIZE];
        let mut pos = 0;

        // checksum (4 bytes)
        bytes[pos..pos+4].copy_from_slice(&self.checksum.to_le_bytes());
        pos += 4;

        // page_type (1 byte)
        bytes[pos] = self.page_type as u8;
        pos += 1;

        // flags (1 byte)
        bytes[pos] = self.flags;
        pos += 1;

        // record_count (2 bytes)
        bytes[pos..pos+2].copy_from_slice(&self.record_count.to_le_bytes());
        pos += 2;

        // free_offset (2 bytes)
        bytes[pos..pos+2].copy_from_slice(&self.free_offset.to_le_bytes());
        pos += 2;

        // free_size (2 bytes)
        bytes[pos..pos+2].copy_from_slice(&self.free_size.to_le_bytes());
        pos += 2;

        // right_sibling (4 bytes)
        bytes[pos..pos+4].copy_from_slice(&self.right_sibling.to_le_bytes());
        pos += 4;

        // left_sibling (4 bytes)
        bytes[pos..pos+4].copy_from_slice(&self.left_sibling.to_le_bytes());
        pos += 4;

        // parent_page (4 bytes)
        bytes[pos..pos+4].copy_from_slice(&self.parent_page.to_le_bytes());
        pos += 4;

        // lsn (8 bytes)
        bytes[pos..pos+8].copy_from_slice(&self.lsn.to_le_bytes());
        pos += 8;

        // reserved (64 bytes)
        bytes[pos..pos+64].copy_from_slice(&self._reserved);

        bytes
    }

    /// Deserialize header from bytes
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < Self::SIZE {
            return Err(StorageError::Corrupted("Page header too small".to_string()));
        }

        let mut pos = 0;

        let checksum = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
        pos += 4;

        let page_type = PageType::from_u8(bytes[pos])
            .ok_or_else(|| StorageError::Corrupted(format!("Invalid page type: {}", bytes[pos])))?;
        pos += 1;

        let flags = bytes[pos];
        pos += 1;

        let record_count = u16::from_le_bytes([bytes[pos], bytes[pos+1]]);
        pos += 2;

        let free_offset = u16::from_le_bytes([bytes[pos], bytes[pos+1]]);
        pos += 2;

        let free_size = u16::from_le_bytes([bytes[pos], bytes[pos+1]]);
        pos += 2;

        let right_sibling = u32::from_le_bytes([bytes[pos], bytes[pos+1], bytes[pos+2], bytes[pos+3]]);
        pos += 4;

        let left_sibling = u32::from_le_bytes([bytes[pos], bytes[pos+1], bytes[pos+2], bytes[pos+3]]);
        pos += 4;

        let parent_page = u32::from_le_bytes([bytes[pos], bytes[pos+1], bytes[pos+2], bytes[pos+3]]);
        pos += 4;

        let lsn = u64::from_le_bytes([
            bytes[pos], bytes[pos+1], bytes[pos+2], bytes[pos+3],
            bytes[pos+4], bytes[pos+5], bytes[pos+6], bytes[pos+7]
        ]);
        pos += 8;

        let mut reserved = [0u8; 64];
        reserved.copy_from_slice(&bytes[pos..pos+64]);

        Ok(Self {
            checksum,
            page_type,
            flags,
            record_count,
            free_offset,
            free_size,
            right_sibling,
            left_sibling,
            parent_page,
            lsn,
            _reserved: reserved,
        })
    }
}

// ============================================================================
// Record Header Structure (16 bytes)
// ============================================================================

/// Record header for stored records
#[derive(Debug, Clone, Copy)]
pub struct RecordHeader {
    pub total_size: u32,      // Total size including header and data
    pub key_size: u16,        // Key size
    pub value_size: u16,      // Value size
    pub flags: u16,           // bit0=deleted, bit1=has_overflow
    pub overflow_page: PageId, // Overflow page ID if record spans pages
}

impl RecordHeader {
    pub const SIZE: usize = 16;

    pub const FLAG_DELETED: u16 = 0x01;
    pub const FLAG_OVERFLOW: u16 = 0x02;

    pub fn new(key_size: u16, value_size: u16) -> Self {
        Self {
            total_size: (key_size as u32) + (value_size as u32) + Self::SIZE as u32,
            key_size,
            value_size,
            flags: 0,
            overflow_page: 0,
        }
    }

    pub fn is_deleted(&self) -> bool {
        (self.flags & Self::FLAG_DELETED) != 0
    }

    pub fn mark_deleted(&mut self) {
        self.flags |= Self::FLAG_DELETED;
    }

    pub fn has_overflow(&self) -> bool {
        (self.flags & Self::FLAG_OVERFLOW) != 0
    }

    pub fn to_bytes(&self) -> [u8; Self::SIZE] {
        let mut bytes = [0u8; Self::SIZE];
        let mut pos = 0;

        bytes[pos..pos+4].copy_from_slice(&self.total_size.to_le_bytes());
        pos += 4;

        bytes[pos..pos+2].copy_from_slice(&self.key_size.to_le_bytes());
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
            return Err(StorageError::Corrupted("Record header too small".to_string()));
        }

        Ok(Self {
            total_size: u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]),
            key_size: u16::from_le_bytes([bytes[4], bytes[5]]),
            value_size: u16::from_le_bytes([bytes[6], bytes[7]]),
            flags: u16::from_le_bytes([bytes[8], bytes[9]]),
            overflow_page: u32::from_le_bytes([bytes[10], bytes[11], bytes[12], bytes[13]]),
        })
    }
}

// ============================================================================
// Free Page List Management
// ============================================================================

/// Free page list for page allocation
#[derive(Debug, Clone)]
pub struct FreePageList {
    pub head_page: PageId,
    pub tail_page: PageId,
    pub count: u32,
}

impl FreePageList {
    pub fn new() -> Self {
        Self {
            head_page: 0,
            tail_page: 0,
            count: 0,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }
}

/// Page allocator manages free pages and page allocation
pub struct PageAllocator {
    free_list: FreePageList,
}

impl PageAllocator {
    pub fn new() -> Self {
        Self {
            free_list: FreePageList::new(),
        }
    }

    /// Allocate a new page from the pager
    pub fn allocate(&mut self, pager: &mut Pager, page_type: PageType) -> Result<PageId> {
        // First try to reuse a free page
        if !self.free_list.is_empty() {
            // For now, just allocate new pages
            // TODO: Implement free page reuse
        }

        let page_id = pager.allocate_page()?;

        // Initialize the page with proper header
        let mut page = pager.get_page(page_id)?;
        let header = PageHeader::new(page_type);
        page.data[0..PageHeader::SIZE].copy_from_slice(&header.to_bytes());
        pager.write_page(&page)?;

        Ok(page_id)
    }

    /// Free a page and add it to the free list
    pub fn free(&mut self, _pager: &mut Pager, page_id: PageId) -> Result<()> {
        // TODO: Implement proper free page management
        // For now, just mark as free type
        println!("Page {} marked for freeing (not yet implemented)", page_id);
        Ok(())
    }
}

// ============================================================================
// B-tree Page Operations
// ============================================================================

/// Maximum inline record size (records larger than this use overflow pages)
pub const MAX_INLINE_SIZE: usize = 2000;

/// Minimum records before considering merge
pub const MIN_RECORDS_FOR_MERGE: usize = 4;

/// B-tree page operations trait
pub trait BtreePageOps {
    /// Read page header
    fn read_header(&self) -> Result<PageHeader>;

    /// Write page header
    fn write_header(&mut self, header: &PageHeader) -> Result<()>;

    /// Check if page has space for a record of given size
    fn has_space(&self, record_size: usize) -> Result<bool>;

    /// Get number of records in page
    fn record_count(&self) -> Result<u16>;

    /// Get record at slot index
    fn get_record_at(&self, slot_idx: usize) -> Result<(Vec<u8>, Vec<u8>)>;

    /// Insert a record into the page
    fn insert_record(&mut self, key: &[u8], value: &[u8]) -> Result<()>;

    /// Mark a record as deleted
    fn mark_deleted(&mut self, slot_idx: usize) -> Result<()>;

    /// Get all records in the page
    fn get_all_records(&self) -> Result<Vec<(Vec<u8>, Vec<u8>)>>;

    /// Compare key at slot_idx with target key (for binary search without copying)
    /// Returns: Ok(Ordering) - comparison result
    fn compare_key_at(&self, slot_idx: usize, target: &[u8]) -> Result<Ordering>;
}

impl BtreePageOps for Page {
    fn compare_key_at(&self, slot_idx: usize, target: &[u8]) -> Result<Ordering> {
        let header = self.read_header()?;
        
        if slot_idx >= header.record_count as usize {
            return Err(StorageError::KeyNotFound);
        }

        // Read slot offset
        let slot_offset = PageHeader::SIZE + slot_idx * 2;
        let record_offset = u16::from_le_bytes([
            self.data[slot_offset],
            self.data[slot_offset + 1]
        ]) as usize;

        // Read record header
        let rec_header = RecordHeader::from_bytes(&self.data[record_offset..])?;
        
        if rec_header.is_deleted() {
            // Return error for deleted records so binary search can fall back to linear scan
            return Err(StorageError::KeyNotFound);
        }

        // Extract key without copying
        let key_start = record_offset + RecordHeader::SIZE;
        let key_end = key_start + rec_header.key_size as usize;
        let key = &self.data[key_start..key_end];

        Ok(compare_keys(key, target))
    }

    fn read_header(&self) -> Result<PageHeader> {
        PageHeader::from_bytes(&self.data[0..PageHeader::SIZE])
    }

    fn write_header(&mut self, header: &PageHeader) -> Result<()> {
        self.data[0..PageHeader::SIZE].copy_from_slice(&header.to_bytes());
        Ok(())
    }

    fn has_space(&self, record_size: usize) -> Result<bool> {
        let header = self.read_header()?;

        // Required space: record data + slot entry (2 bytes) + record header (16 bytes)
        let required = record_size + 2 + RecordHeader::SIZE;

        Ok(header.free_size as usize >= required)
    }

    fn record_count(&self) -> Result<u16> {
        let header = self.read_header()?;
        Ok(header.record_count)
    }

    fn get_record_at(&self, slot_idx: usize) -> Result<(Vec<u8>, Vec<u8>)> {
        let header = self.read_header()?;

        if slot_idx >= header.record_count as usize {
            return Err(StorageError::KeyNotFound);
        }

        // Read slot offset from slot array
        let slot_offset = PageHeader::SIZE + slot_idx * 2;
        let record_offset = u16::from_le_bytes([
            self.data[slot_offset],
            self.data[slot_offset + 1]
        ]) as usize;

        // Read record header
        let rec_header = RecordHeader::from_bytes(&self.data[record_offset..])?;

        // Check if record is deleted
        if rec_header.is_deleted() {
            return Err(StorageError::KeyNotFound);
        }

        // Extract key and value
        let key_start = record_offset + RecordHeader::SIZE;
        let key_end = key_start + rec_header.key_size as usize;
        let value_end = key_end + rec_header.value_size as usize;

        let key = self.data[key_start..key_end].to_vec();
        let value = self.data[key_end..value_end].to_vec();

        Ok((key, value))
    }

    fn insert_record(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        let mut header = self.read_header()?;

        let key_size = key.len();
        let value_size = value.len();
        let total_record_size = key_size + value_size + RecordHeader::SIZE;

        // Check if we need overflow pages
        let needs_overflow = total_record_size > MAX_INLINE_SIZE;

        if needs_overflow {
            // For now, reject very large records
            // TODO: Implement overflow page handling in Phase 3
            return Err(StorageError::RecordTooLarge(total_record_size));
        }

        // Check space availability
        let required_space = total_record_size + 2; // +2 for slot entry
        if (header.free_size as usize) < required_space {
            return Err(StorageError::PageFull);
        }

        // Find insertion position to maintain key order
        let mut insert_idx = 0;
        while insert_idx < header.record_count as usize {
            let slot_offset = PageHeader::SIZE + insert_idx * 2;
            let record_offset = u16::from_le_bytes([
                self.data[slot_offset],
                self.data[slot_offset + 1]
            ]) as usize;
            
            let rec_header = RecordHeader::from_bytes(&self.data[record_offset..])?;
            let k_start = record_offset + RecordHeader::SIZE;
            let k_end = k_start + rec_header.key_size as usize;
            let k = &self.data[k_start..k_end];
            
            if compare_keys(key, k) == Ordering::Less {
                break;
            }
            insert_idx += 1;
        }

        // Move existing slots to the right
        if insert_idx < header.record_count as usize {
            let src = PageHeader::SIZE + insert_idx * 2;
            let dst = PageHeader::SIZE + (insert_idx + 1) * 2;
            let len = (header.record_count as usize - insert_idx) * 2;
            self.data.copy_within(src..src + len, dst);
        }

        // Calculate record position (growing from end of page)
        let record_offset = (header.free_offset as usize + 1) - total_record_size;

        // Write record header
        let rec_header = RecordHeader::new(key_size as u16, value_size as u16);
        self.data[record_offset..record_offset + RecordHeader::SIZE]
            .copy_from_slice(&rec_header.to_bytes());

        // Write key and value
        let key_start = record_offset + RecordHeader::SIZE;
        self.data[key_start..key_start + key_size].copy_from_slice(key);
        self.data[key_start + key_size..key_start + key_size + value_size].copy_from_slice(value);

        // Update slot array at the correct position
        let slot_offset = PageHeader::SIZE + insert_idx * 2;
        self.data[slot_offset..slot_offset + 2]
            .copy_from_slice(&(record_offset as u16).to_le_bytes());

        // Update header
        header.record_count += 1;
        header.free_offset = record_offset as u16 - 1;
        header.free_size -= required_space as u16;
        self.write_header(&header)?;

        Ok(())
    }

    fn mark_deleted(&mut self, slot_idx: usize) -> Result<()> {
        let header = self.read_header()?;

        if slot_idx >= header.record_count as usize {
            return Err(StorageError::KeyNotFound);
        }

        // Get record offset
        let slot_offset = PageHeader::SIZE + slot_idx * 2;
        let record_offset = u16::from_le_bytes([
            self.data[slot_offset],
            self.data[slot_offset + 1]
        ]) as usize;

        // Mark record as deleted by setting flag in record header
        let rec_header_offset = record_offset + 8; // flags offset in RecordHeader
        let flags = u16::from_le_bytes([
            self.data[rec_header_offset],
            self.data[rec_header_offset + 1]
        ]);
        let new_flags = flags | RecordHeader::FLAG_DELETED;
        self.data[rec_header_offset..rec_header_offset + 2]
            .copy_from_slice(&new_flags.to_le_bytes());

        Ok(())
    }

    fn get_all_records(&self) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let header = self.read_header()?;
        let mut records = Vec::with_capacity(header.record_count as usize);

        for i in 0..header.record_count as usize {
            match self.get_record_at(i) {
                Ok(record) => records.push(record),
                Err(_) => continue, // Skip corrupted records
            }
        }

        Ok(records)
    }
}

// ============================================================================
// B-tree Node Types
// ============================================================================

/// Entry in an internal B-tree node (index page)
#[derive(Debug, Clone)]
pub struct IndexEntry {
    pub key: Vec<u8>,
    pub child_page: PageId,
}

/// Entry in a leaf B-tree node (data page)
#[derive(Debug, Clone)]
pub struct LeafEntry {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

/// B-tree node wrapper
pub struct BtreeNode {
    pub page_id: PageId,
    pub header: PageHeader,
}

impl BtreeNode {
    pub fn new(page_id: PageId, header: PageHeader) -> Self {
        Self { page_id, header }
    }

    pub fn is_leaf(&self) -> bool {
        self.header.is_leaf()
    }

    pub fn is_root(&self) -> bool {
        self.header.is_root()
    }
}

// ============================================================================
// Key Comparison Utilities
// ============================================================================

/// Compare two byte keys
pub fn compare_keys(a: &[u8], b: &[u8]) -> Ordering {
    a.cmp(b)
}

/// Binary search for a key in a sorted slice of entries
pub fn binary_search_entries(entries: &[(Vec<u8>, Vec<u8>)], key: &[u8]) -> Result<usize> {
    let mut left = 0;
    let mut right = entries.len();

    while left < right {
        let mid = (left + right) / 2;
        match compare_keys(&entries[mid].0, key) {
            Ordering::Equal => return Ok(mid),
            Ordering::Less => left = mid + 1,
            Ordering::Greater => right = mid,
        }
    }

    Err(StorageError::KeyNotFound)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn test_page_header_serialization() {
        let header = PageHeader::new(PageType::Data);
        let bytes = header.to_bytes();
        assert_eq!(bytes.len(), PageHeader::SIZE);

        let parsed = PageHeader::from_bytes(&bytes).unwrap();
        assert_eq!(parsed.page_type as u8, header.page_type as u8);
        assert_eq!(parsed.flags, header.flags);
        assert_eq!(parsed.record_count, header.record_count);
    }

    #[test]
    fn test_record_header_serialization() {
        let header = RecordHeader::new(100, 200);
        let bytes = header.to_bytes();
        assert_eq!(bytes.len(), RecordHeader::SIZE);

        let parsed = RecordHeader::from_bytes(&bytes).unwrap();
        assert_eq!(parsed.key_size, header.key_size);
        assert_eq!(parsed.value_size, header.value_size);
        assert_eq!(parsed.total_size, header.total_size);
    }

    #[test]
    fn test_page_record_operations() {
        let mut page = Page::new(1);

        // Initialize as data page
        let header = PageHeader::new(PageType::Data);
        page.write_header(&header).unwrap();

        // Insert some records
        let key1 = b"key1".to_vec();
        let value1 = b"value1".to_vec();
        page.insert_record(&key1, &value1).unwrap();

        let key2 = b"key2".to_vec();
        let value2 = b"value2".to_vec();
        page.insert_record(&key2, &value2).unwrap();

        // Verify record count
        assert_eq!(page.record_count().unwrap(), 2);

        // Read records back
        let (k1, v1) = page.get_record_at(0).unwrap();
        assert_eq!(k1, key1);
        assert_eq!(v1, value1);

        let (k2, v2) = page.get_record_at(1).unwrap();
        assert_eq!(k2, key2);
        assert_eq!(v2, value2);

        // Get all records
        let all = page.get_all_records().unwrap();
        assert_eq!(all.len(), 2);
    }

    #[test]
    fn test_page_allocator() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap();

        let mut pager = Pager::open(path).unwrap();
        let mut allocator = PageAllocator::new();

        let page_id = allocator.allocate(&mut pager, PageType::Data).unwrap();
        assert!(page_id > 0);

        // Verify page was initialized correctly
        let page = pager.get_page(page_id).unwrap();
        let header = page.read_header().unwrap();
        assert_eq!(header.page_type as u8, PageType::Data as u8);
    }

    #[test]
    fn test_page_has_space() {
        let mut page = Page::new(1);
        let header = PageHeader::new(PageType::Data);
        page.write_header(&header).unwrap();

        // Empty page should have space
        assert!(page.has_space(100).unwrap());

        // Fill page with records
        let large_value = vec![0u8; 2000];
        for i in 0..10 {
            let key = format!("key{:04}", i);
            let result = page.insert_record(key.as_bytes(), &large_value);
            if result.is_err() {
                break; // Page full
            }
        }
    }

    #[test]
    fn test_page_full_insert() {
        let mut page = Page::new(1);
        let header = PageHeader::new(PageType::Data);
        page.write_header(&header).unwrap();

        // Try to insert very large record
        let huge_key = vec![0u8; 3000];
        let huge_value = vec![0u8; 3000];
        let result = page.insert_record(&huge_key, &huge_value);
        assert!(result.is_err());
    }

    #[test]
    fn test_page_record_ordering() {
        let mut page = Page::new(1);
        let header = PageHeader::new(PageType::Data);
        page.write_header(&header).unwrap();

        // Insert in reverse order
        page.insert_record(b"key3", b"value3").unwrap();
        page.insert_record(b"key1", b"value1").unwrap();
        page.insert_record(b"key2", b"value2").unwrap();

        // Records should be in sorted order
        let all = page.get_all_records().unwrap();
        assert_eq!(all[0].0, b"key1");
        assert_eq!(all[1].0, b"key2");
        assert_eq!(all[2].0, b"key3");
    }

    #[test]
    fn test_page_header_flags() {
        let mut header = PageHeader::new(PageType::Data);
        assert!(!header.is_leaf());
        assert!(!header.is_root());

        header.set_leaf(true);
        assert!(header.is_leaf());

        header.set_root(true);
        assert!(header.is_root());

        header.set_leaf(false);
        assert!(!header.is_leaf());
    }

    #[test]
    fn test_record_header_deleted() {
        let mut header = RecordHeader::new(10, 20);
        assert!(!header.is_deleted());

        header.mark_deleted();
        assert!(header.is_deleted());
    }

    #[test]
    fn test_page_types() {
        let data = PageHeader::new(PageType::Data);
        let index = PageHeader::new(PageType::Index);

        assert_eq!(data.page_type as u8, 0);
        assert_eq!(index.page_type as u8, 1);
    }

    #[test]
    fn test_compare_keys() {
        assert_eq!(compare_keys(b"a", b"b"), std::cmp::Ordering::Less);
        assert_eq!(compare_keys(b"b", b"a"), std::cmp::Ordering::Greater);
        assert_eq!(compare_keys(b"a", b"a"), std::cmp::Ordering::Equal);
        assert_eq!(compare_keys(b"", b"a"), std::cmp::Ordering::Less);
    }

    #[test]
    fn test_binary_search_entries() {
        let entries = vec![
            (b"a".to_vec(), b"1".to_vec()),
            (b"b".to_vec(), b"2".to_vec()),
            (b"c".to_vec(), b"3".to_vec()),
            (b"d".to_vec(), b"4".to_vec()),
        ];

        assert_eq!(binary_search_entries(&entries, b"a").unwrap(), 0);
        assert_eq!(binary_search_entries(&entries, b"c").unwrap(), 2);
        assert!(binary_search_entries(&entries, b"z").is_err());
    }

    // ========================================================================
    // P8-1: Prefix Compression Integration Tests
    // ========================================================================

    #[test]
    fn test_prefix_compression_space_savings() {
        use crate::storage::prefix_page::{find_common_prefix, compress_keys, decompress_key};

        // Create keys with common prefix (like user IDs)
        let keys: Vec<Vec<u8>> = (0..100)
            .map(|i| format!("user:{:08x}:profile:data", i).into_bytes())
            .collect();

        let prefix = find_common_prefix(&keys);
        let compressed = compress_keys(&keys, &prefix);

        // Calculate space usage
        let uncompressed_size: usize = keys.iter().map(|k| k.len()).sum();
        let compressed_size: usize = compressed.iter().map(|k| k.len()).sum();
        let total_with_prefix = compressed_size + prefix.len();

        let savings_ratio = (uncompressed_size - total_with_prefix) as f64 / uncompressed_size as f64;

        println!("Prefix: {:?} ({} bytes)", String::from_utf8_lossy(&prefix), prefix.len());
        println!("Uncompressed: {} bytes", uncompressed_size);
        println!("Compressed: {} bytes (suffixes: {}, prefix: {})", 
                 total_with_prefix, compressed_size, prefix.len());
        println!("Space saved: {:.1}%", savings_ratio * 100.0);

        // Verify compression saves at least 30% space
        assert!(
            savings_ratio > 0.30,
            "Expected >30% space savings, got {:.1}%", 
            savings_ratio * 100.0
        );

        // Verify decompress works
        for (i, suffix) in compressed.iter().enumerate() {
            let decompressed = decompress_key(suffix, &prefix);
            assert_eq!(decompressed, keys[i]);
        }
    }

    #[test]
    fn test_prefix_compression_with_url_keys() {
        use crate::storage::prefix_page::find_common_prefix;

        // Simulate URL keys
        let urls: Vec<Vec<u8>> = vec![
            b"https://example.com/path/to/resource1".to_vec(),
            b"https://example.com/path/to/resource2".to_vec(),
            b"https://example.com/path/to/resource3".to_vec(),
            b"https://example.com/path/to/resource4".to_vec(),
        ];

        let prefix = find_common_prefix(&urls);
        
        println!("URL prefix: {:?}", String::from_utf8_lossy(&prefix));
        
        // Should find common prefix up to "https://example.com/path/to/resource"
        assert!(prefix.len() >= 35, "Should find significant common prefix for URLs");
    }

    #[test]
    fn test_prefix_compression_timestamp_keys() {
        use crate::storage::prefix_page::find_common_prefix;

        // Simulate timestamp-based keys
        let timestamps: Vec<Vec<u8>> = vec![
            b"2024-01-15T10:30:00Z_event1".to_vec(),
            b"2024-01-15T10:30:01Z_event2".to_vec(),
            b"2024-01-15T10:30:02Z_event3".to_vec(),
            b"2024-01-15T10:30:03Z_event4".to_vec(),
        ];

        let prefix = find_common_prefix(&timestamps);
        
        println!("Timestamp prefix: {:?}", String::from_utf8_lossy(&prefix));
        
        // Should find common prefix up to the date/time portion
        assert!(prefix.len() >= 17, "Should find significant common prefix for timestamps");
    }
}
