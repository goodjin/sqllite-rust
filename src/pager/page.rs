pub const PAGE_SIZE: usize = 4096;

pub type PageId = u32;

/// Cache-line optimized page structure (P8-3)
/// 
/// Layout optimized for CPU cache efficiency:
/// - Hot data (page_id, flags, access_count) in first cache line (64 bytes)
/// - Payload follows immediately after
/// 
/// Using #[repr(align(64))] ensures the page starts at a cache line boundary.
#[repr(align(64))]
#[derive(Debug, Clone)]
pub struct Page {
    /// Page ID - most frequently accessed
    pub id: PageId,
    /// Access counter for cache management
    pub access_count: u32,
    /// Page flags (dirty, pinned, etc.)
    pub flags: u32,
    /// Last access timestamp for LRU
    pub last_access: u64,
    /// Padding to fill first cache line (64 bytes total for hot data)
    /// 4 + 4 + 4 + 8 = 20 bytes, need 44 bytes padding
    _cache_line_padding: [u8; 44],
    /// Page data payload
    pub data: [u8; PAGE_SIZE],
}

// Verify Page is exactly cache-line aligned at start
const _: () = assert!(
    std::mem::align_of::<Page>() == 64,
    "Page must be 64-byte aligned"
);

// Verify hot data fits in one cache line
const _: () = assert!(
    std::mem::offset_of!(Page, data) <= 64,
    "Hot data must fit in first cache line"
);

/// Page flags
impl Page {
    pub const FLAG_DIRTY: u32 = 0x01;
    pub const FLAG_PINNED: u32 = 0x02;
    pub const FLAG_PREFETCH: u32 = 0x04;
}

impl Page {
    pub fn new(id: PageId) -> Self {
        Self {
            id,
            access_count: 0,
            flags: 0,
            last_access: 0,
            _cache_line_padding: [0; 44],
            data: [0; PAGE_SIZE],
        }
    }

    pub fn from_bytes(id: PageId, bytes: Vec<u8>) -> Self {
        let mut data = [0u8; PAGE_SIZE];
        let len = bytes.len().min(PAGE_SIZE);
        data[..len].copy_from_slice(&bytes[..len]);
        Self {
            id,
            access_count: 0,
            flags: 0,
            last_access: 0,
            _cache_line_padding: [0; 44],
            data,
        }
    }

    pub fn id(&self) -> PageId {
        self.id
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.data
    }

    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.data
    }
    
    /// Mark page as dirty
    pub fn mark_dirty(&mut self) {
        self.flags |= Self::FLAG_DIRTY;
    }
    
    /// Check if page is dirty
    pub fn is_dirty(&self) -> bool {
        (self.flags & Self::FLAG_DIRTY) != 0
    }
    
    /// Clear dirty flag
    pub fn clear_dirty(&mut self) {
        self.flags &= !Self::FLAG_DIRTY;
    }
    
    /// Mark page as pinned
    pub fn pin(&mut self) {
        self.flags |= Self::FLAG_PINNED;
    }
    
    /// Unpin page
    pub fn unpin(&mut self) {
        self.flags &= !Self::FLAG_PINNED;
    }
    
    /// Check if page is pinned
    pub fn is_pinned(&self) -> bool {
        (self.flags & Self::FLAG_PINNED) != 0
    }
    
    /// Record an access to this page
    pub fn record_access(&mut self, timestamp: u64) {
        self.access_count = self.access_count.wrapping_add(1);
        self.last_access = timestamp;
    }
}

/// Cache-friendly page header for hot data access
/// This is a separate structure that can be used for page cache metadata
#[repr(align(64))]
#[derive(Debug, Clone, Copy)]
pub struct PageCacheMeta {
    /// Page ID
    pub page_id: PageId,
    /// Access counter
    pub access_count: u32,
    /// Flags
    pub flags: u32,
    /// Last access time
    pub last_access: u64,
    /// Reference to page data (pointer or offset)
    pub data_ptr: usize,
    /// Padding to 64 bytes
    _padding: [u8; 40],
}

impl PageCacheMeta {
    pub fn new(page_id: PageId, data_ptr: usize) -> Self {
        Self {
            page_id,
            access_count: 0,
            flags: 0,
            last_access: 0,
            data_ptr,
            _padding: [0; 40],
        }
    }
    
    pub fn record_access(&mut self, timestamp: u64) {
        self.access_count = self.access_count.wrapping_add(1);
        self.last_access = timestamp;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_page_creation() {
        let page = Page::new(1);
        assert_eq!(page.id, 1);
        assert_eq!(page.data.len(), PAGE_SIZE);
        assert!(page.data.iter().all(|&b| b == 0));
    }

    #[test]
    fn test_page_access() {
        let mut page = Page::new(1);
        page.as_mut_slice()[0] = 42;
        assert_eq!(page.as_slice()[0], 42);
    }
    
    #[test]
    fn test_page_alignment() {
        // Verify page is 64-byte aligned
        assert_eq!(std::mem::align_of::<Page>(), 64);
        
        // Verify hot data fits in first cache line
        let hot_data_size = std::mem::offset_of!(Page, data);
        assert!(
            hot_data_size <= 64,
            "Hot data size {} exceeds cache line (64 bytes)",
            hot_data_size
        );
        
        println!("Page size: {} bytes", std::mem::size_of::<Page>());
        println!("Page alignment: {} bytes", std::mem::align_of::<Page>());
        println!("Hot data offset: {} bytes", hot_data_size);
    }
    
    #[test]
    fn test_page_flags() {
        let mut page = Page::new(1);
        
        assert!(!page.is_dirty());
        page.mark_dirty();
        assert!(page.is_dirty());
        page.clear_dirty();
        assert!(!page.is_dirty());
        
        assert!(!page.is_pinned());
        page.pin();
        assert!(page.is_pinned());
        page.unpin();
        assert!(!page.is_pinned());
    }
    
    #[test]
    fn test_page_access_tracking() {
        let mut page = Page::new(1);
        
        assert_eq!(page.access_count, 0);
        page.record_access(1000);
        assert_eq!(page.access_count, 1);
        assert_eq!(page.last_access, 1000);
        
        page.record_access(2000);
        assert_eq!(page.access_count, 2);
        assert_eq!(page.last_access, 2000);
    }
    
    #[test]
    fn test_page_cache_meta_alignment() {
        assert_eq!(std::mem::align_of::<PageCacheMeta>(), 64);
        assert_eq!(std::mem::size_of::<PageCacheMeta>(), 64);
    }
}
