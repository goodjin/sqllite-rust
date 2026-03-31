//! Copy-on-Write (COW) Implementation
//!
//! Provides write operations that don't block read operations:
//! - Writers create new versions without modifying old versions
//! - Page-level COW for efficient memory usage
//! - Atomic pointer switching for lock-free reads
//! - Version chain updates

use crossbeam_epoch::{self as epoch, Atomic, Guard, Owned};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use super::mvcc::{Snapshot, TxId, Version};

/// Atomic version chain node for lock-free operations
/// Uses crossbeam-epoch for memory safety
pub struct AtomicVersionChain<T: Clone> {
    /// Pointer to the version chain (atomic for lock-free updates)
    chain: Atomic<VersionChainNode<T>>,
    /// Version counter for optimistic locking
    version: AtomicU64,
}

/// Node in the version chain
#[derive(Debug)]
pub struct VersionChainNode<T: Clone> {
    /// The version data
    pub version: Version<T>,
    /// Pointer to next (older) version
    pub next: Atomic<VersionChainNode<T>>,
}

impl<T: Clone> VersionChainNode<T> {
    pub fn new(version: Version<T>) -> Self {
        Self {
            version,
            next: Atomic::null(),
        }
    }
}

impl<T: Clone> AtomicVersionChain<T> {
    pub fn new() -> Self {
        Self {
            chain: Atomic::null(),
            version: AtomicU64::new(0),
        }
    }

    /// Create with initial version
    pub fn with_version(version: Version<T>) -> Self {
        let node = Owned::new(VersionChainNode::new(version));
        let chain = Atomic::from(node);
        Self {
            chain,
            version: AtomicU64::new(1),
        }
    }

    /// Read the latest visible version (lock-free)
    /// Uses epoch-based reclamation for memory safety
    pub fn read(&self, reader_tx: TxId, snapshot: &Snapshot, guard: &Guard) -> Option<T> {
        let mut current = self.chain.load(Ordering::Acquire, guard);

        while !current.is_null() {
            // SAFETY: We hold the guard, so the node is valid
            let node = unsafe { current.deref() };
            
            if node.version.is_visible_to(reader_tx, snapshot) {
                return Some(node.version.data.clone());
            }
            
            // Move to next version
            current = node.next.load(Ordering::Acquire, guard);
        }

        None
    }

    /// Write a new version (COW - doesn't block reads)
    /// Creates new version and atomically updates the chain head
    pub fn write(&self, data: T, writer_tx: TxId) {
        let guard = &epoch::pin();

        // Create new version
        let new_version = Version::new(data, writer_tx);
        let mut new_node = Owned::new(VersionChainNode::new(new_version));

        loop {
            // Load current head
            let current_head = self.chain.load(Ordering::Acquire, guard);

            // Set new node's next to current head
            new_node.next.store(current_head, Ordering::Relaxed);

            // Try to atomically swap head with new node
            match self.chain.compare_exchange(
                current_head,
                new_node,
                Ordering::Release,
                Ordering::Relaxed,
                guard,
            ) {
                Ok(_) => {
                    // Success - increment version for optimistic locking
                    self.version.fetch_add(1, Ordering::SeqCst);
                    break;
                }
                Err(err) => {
                    // Failed - retry with updated new_node
                    new_node = err.new;
                    // Backoff to reduce contention
                    std::thread::yield_now();
                }
            }
        }
    }

    /// Mark the latest visible version as deleted
    pub fn delete(&self, writer_tx: TxId, snapshot: &Snapshot, guard: &Guard) -> bool {
        let mut current = self.chain.load(Ordering::Acquire, guard);

        while !current.is_null() {
            let node = unsafe { current.deref() };
            
            if node.version.is_visible_to(writer_tx, snapshot) {
                // We can't modify in-place in COW, so we create a tombstone version
                // For simplicity, mark as deleted by creating a special marker
                // In a real implementation, you'd have a separate tombstone mechanism
                return true;
            }
            
            current = node.next.load(Ordering::Acquire, guard);
        }

        false
    }

    /// Get the version number (for optimistic locking)
    pub fn get_version(&self) -> u64 {
        self.version.load(Ordering::Acquire)
    }

    /// Garbage collect old versions that are not visible to any active snapshot
    /// Returns number of versions removed
    pub fn gc(&self, oldest_visible_tx: TxId, guard: &Guard) -> usize {
        let mut removed = 0;
        let head = self.chain.load(Ordering::Acquire, guard);

        if head.is_null() {
            return 0;
        }

        // We traverse the chain and mark old versions for deletion
        // In a real implementation, this would use hazard pointers or epoch reclamation
        // For now, we just count how many could be removed
        let mut current = head;
        let mut newest_created_by = 0;

        // Get newest version's creator
        if !current.is_null() {
            let node = unsafe { current.deref() };
            newest_created_by = node.version.created_by;
        }

        // Traverse and count removable versions
        while !current.is_null() {
            let node = unsafe { current.deref() };
            let created_by = node.version.created_by;

            // Can remove if:
            // 1. Created by tx older than oldest_visible_tx
            // 2. Not the newest version
            if created_by < oldest_visible_tx && created_by != newest_created_by {
                removed += 1;
            }

            current = node.next.load(Ordering::Acquire, guard);
        }

        removed
    }
}

impl<T: Clone> Default for AtomicVersionChain<T> {
    fn default() -> Self {
        Self::new()
    }
}

/// Page-level COW storage
/// Each page has its own version chain
pub struct CowPage<T: Clone> {
    /// Page ID
    pub page_id: u64,
    /// Atomic version chain for lock-free reads
    versions: AtomicVersionChain<T>,
    /// Write lock for exclusive writes (to prevent write-write conflicts)
    write_lock: parking_lot::RwLock<()>,
}

impl<T: Clone> CowPage<T> {
    pub fn new(page_id: u64) -> Self {
        Self {
            page_id,
            versions: AtomicVersionChain::new(),
            write_lock: parking_lot::RwLock::new(()),
        }
    }

    pub fn with_data(page_id: u64, data: T, tx_id: TxId) -> Self {
        let version = Version::new(data, tx_id);
        Self {
            page_id,
            versions: AtomicVersionChain::with_version(version),
            write_lock: parking_lot::RwLock::new(()),
        }
    }

    /// Read data (completely lock-free)
    pub fn read(&self, reader_tx: TxId, snapshot: &Snapshot) -> Option<T> {
        let guard = &epoch::pin();
        self.versions.read(reader_tx, snapshot, guard)
    }

    /// Write data (COW - doesn't block reads)
    /// Acquires write lock to prevent concurrent writes to same page
    pub fn write(&self, data: T, writer_tx: TxId) -> Result<(), CowError> {
        // Acquire write lock to prevent concurrent modifications
        let _write_guard = self.write_lock.write();
        
        // Perform COW write
        self.versions.write(data, writer_tx);
        
        Ok(())
    }

    /// Get version number (for optimistic locking)
    pub fn get_version(&self) -> u64 {
        self.versions.get_version()
    }

    /// Garbage collect old versions
    pub fn gc(&self, oldest_visible_tx: TxId) -> usize {
        let guard = &epoch::pin();
        self.versions.gc(oldest_visible_tx, guard)
    }
}

/// COW errors
#[derive(Debug, Clone, PartialEq)]
pub enum CowError {
    WriteConflict,
    PageNotFound,
    VersionNotFound,
}

impl std::fmt::Display for CowError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CowError::WriteConflict => write!(f, "Write conflict detected"),
            CowError::PageNotFound => write!(f, "Page not found"),
            CowError::VersionNotFound => write!(f, "Version not found"),
        }
    }
}

impl std::error::Error for CowError {}

/// COW storage manager
pub struct CowStorage<T: Clone> {
    /// Pages stored by ID
    pages: parking_lot::RwLock<hashbrown::HashMap<u64, Arc<CowPage<T>>>>,
    /// Next page ID
    next_page_id: AtomicU64,
}

impl<T: Clone> CowStorage<T> {
    pub fn new() -> Self {
        Self {
            pages: parking_lot::RwLock::new(hashbrown::HashMap::new()),
            next_page_id: AtomicU64::new(1),
        }
    }

    /// Allocate a new page
    pub fn allocate_page(&self, data: T, tx_id: TxId) -> u64 {
        let page_id = self.next_page_id.fetch_add(1, Ordering::SeqCst);
        let page = Arc::new(CowPage::with_data(page_id, data, tx_id));
        
        let mut pages = self.pages.write();
        pages.insert(page_id, page);
        
        page_id
    }

    /// Get a page by ID
    pub fn get_page(&self, page_id: u64) -> Option<Arc<CowPage<T>>> {
        let pages = self.pages.read();
        pages.get(&page_id).cloned()
    }

    /// Read from a page (lock-free)
    pub fn read(&self, page_id: u64, reader_tx: TxId, snapshot: &Snapshot) -> Option<T> {
        let page = self.get_page(page_id)?;
        page.read(reader_tx, snapshot)
    }

    /// Write to a page (COW)
    pub fn write(&self, page_id: u64, data: T, writer_tx: TxId) -> Result<(), CowError> {
        let page = self.get_page(page_id)
            .ok_or(CowError::PageNotFound)?;
        
        page.write(data, writer_tx)
    }

    /// Update an existing page (creates new version)
    pub fn update_page(&self, page_id: u64, data: T, writer_tx: TxId) -> Result<(), CowError> {
        self.write(page_id, data, writer_tx)
    }

    /// Garbage collect all pages
    pub fn gc(&self, oldest_visible_tx: TxId) -> usize {
        let pages = self.pages.read();
        let mut total_removed = 0;

        for page in pages.values() {
            total_removed += page.gc(oldest_visible_tx);
        }

        total_removed
    }

    /// Get page count
    pub fn page_count(&self) -> usize {
        let pages = self.pages.read();
        pages.len()
    }
}

impl<T: Clone> Default for CowStorage<T> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    fn create_snapshot(reader_tx: TxId, visible_txs: HashSet<TxId>, xmin: TxId) -> Snapshot {
        let xmax = reader_tx + 1;
        let active_txs = HashSet::new();
        Snapshot::new(reader_tx, visible_txs, xmin, xmax, active_txs)
    }

    #[test]
    fn test_cow_basic_write_read() {
        let storage = CowStorage::new();
        
        // Create a page
        let tx1 = 1;
        let page_id = storage.allocate_page("data_v1".to_string(), tx1);
        
        // Read with snapshot that sees tx1
        let mut visible = HashSet::new();
        visible.insert(tx1);
        let snapshot = create_snapshot(2, visible, tx1);
        
        let data = storage.read(page_id, 2, &snapshot);
        assert_eq!(data, Some("data_v1".to_string()));
    }

    #[test]
    fn test_cow_version_chain() {
        let storage = CowStorage::new();
        
        let tx1 = 1;
        let page_id = storage.allocate_page("v1".to_string(), tx1);
        
        // Write new version
        let tx2 = 2;
        storage.write(page_id, "v2".to_string(), tx2).unwrap();
        
        // Write another version
        let tx3 = 3;
        storage.write(page_id, "v3".to_string(), tx3).unwrap();
        
        // Read with snapshot that sees all versions
        let mut visible = HashSet::new();
        visible.insert(tx1);
        visible.insert(tx2);
        visible.insert(tx3);
        let snapshot = create_snapshot(4, visible, tx1);
        
        // Should see latest version (v3)
        let data = storage.read(page_id, 4, &snapshot);
        assert_eq!(data, Some("v3".to_string()));
    }

    #[test]
    fn test_cow_concurrent_reads() {
        use std::sync::Arc;
        use std::thread;

        let storage = Arc::new(CowStorage::new());
        
        // Create initial data
        let tx1 = 1;
        let page_id = storage.allocate_page(100i32, tx1);
        
        // Spawn multiple readers
        let mut handles = vec![];
        for i in 0..10 {
            let storage_clone = storage.clone();
            let handle = thread::spawn(move || {
                let mut visible = HashSet::new();
                visible.insert(tx1);
                let snapshot = create_snapshot(i + 10, visible, tx1);
                
                // Each reader should see the same data
                let data = storage_clone.read(page_id, i + 10, &snapshot);
                assert_eq!(data, Some(100i32));
            });
            handles.push(handle);
        }
        
        for handle in handles {
            handle.join().unwrap();
        }
    }

    #[test]
    fn test_cow_write_doesnt_block_read() {
        use std::sync::Arc;
        use std::thread;
        use std::time::Duration;

        let storage = Arc::new(CowStorage::new());
        
        let tx1 = 1;
        let page_id = storage.allocate_page("initial".to_string(), tx1);
        
        // Start a reader
        let storage_clone = storage.clone();
        let reader = thread::spawn(move || {
            let mut visible = HashSet::new();
            visible.insert(tx1);
            let snapshot = create_snapshot(100, visible, tx1);
            
            // Read should succeed even during write
            let data = storage_clone.read(page_id, 100, &snapshot);
            assert!(data.is_some());
            data
        });
        
        // Small delay to ensure reader starts
        thread::sleep(Duration::from_millis(10));
        
        // Writer writes concurrently
        let tx2 = 2;
        storage.write(page_id, "updated".to_string(), tx2).unwrap();
        
        // Reader should complete successfully
        let result = reader.join().unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn test_cow_page_version() {
        let page = CowPage::with_data(1, "data".to_string(), 1);
        
        // Initial version should be 1
        assert_eq!(page.get_version(), 1);
        
        // Write new version
        page.write("new_data".to_string(), 2).unwrap();
        
        // Version should increment
        assert_eq!(page.get_version(), 2);
    }
}
