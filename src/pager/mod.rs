pub mod cache;
pub mod error;
pub mod header;
pub mod page;
pub mod prefetch;
pub mod checksum;  // P3-6: Page checksum module

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use crate::pager::cache::PageCache;
use crate::pager::header::DatabaseHeader;
pub use crate::pager::page::Page;
use crate::pager::page::PAGE_SIZE;
use crate::storage::wal::Wal;
use crate::pager::checksum::{ChecksumManager, ChecksumConfig, PageChecksumOps};  // P3-6

pub use crate::pager::error::{PagerError, Result};
pub use crate::pager::page::PageId;

/// Pager with integrated checksum support (P3-6)
pub struct Pager {
    file: File,
    cache: PageCache,
    header: DatabaseHeader,
    wal: Option<Wal>,
    _path: String,
    checksum_manager: ChecksumManager,  // P3-6
}

impl Pager {
    pub fn open(path: &str) -> Result<Self> {
        Self::open_with_config(path, ChecksumConfig::default())  // P3-6: With default checksum config
    }

    /// P3-6: Open with checksum configuration
    pub fn open_with_config(path: &str, checksum_config: ChecksumConfig) -> Result<Self> {
        let path_obj = Path::new(path);
        let file_exists = path_obj.exists();
        let file_size = if file_exists {
            std::fs::metadata(path_obj)?.len()
        } else {
            0
        };
        let is_new = !file_exists || file_size < DatabaseHeader::SIZE as u64;

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path_obj)?;

        let header = if is_new {
            let header = DatabaseHeader::new(PAGE_SIZE as u16);
            file.write_all(&header.to_bytes())?;
            file.sync_all()?;
            header
        } else {
            let mut header_bytes = [0u8; DatabaseHeader::SIZE];
            file.read_exact(&mut header_bytes)?;
            DatabaseHeader::from_bytes(&header_bytes)?
        };

        // Open WAL for this database
        let wal = Wal::open(path, PAGE_SIZE).ok();

        Ok(Self {
            file,
            cache: PageCache::new(1000),
            header,
            wal,
            _path: path.to_string(),
            checksum_manager: ChecksumManager::new(checksum_config),
        })
    }

    pub fn get_page(&mut self, page_id: PageId) -> Result<Page> {
        // Check cache first
        if let Some(page) = self.cache.get(page_id) {
            // P3-6: Verify checksum on cache hit if enabled
            if self.checksum_manager.config().verify_on_read {
                if let Err(e) = page.verify_checksum() {
                    return Err(e);
                }
            }
            return Ok(page.clone());
        }

        // Check WAL for uncheckpointed pages
        if let Some(ref mut wal) = self.wal {
            if let Some(data) = wal.read_page(page_id)? {
                let page = Page::from_bytes(page_id, data);
                
                // P3-6: Verify checksum for WAL pages
                self.checksum_manager.verify_page(&page)?;
                
                self.cache.put(page.clone(), false);
                return Ok(page);
            }
        }

        // Read from data file
        let page = self.read_page_from_file(page_id)?;
        
        // P3-6: Verify checksum for disk pages
        self.checksum_manager.verify_page(&page)?;
        
        self.cache.put(page.clone(), false);

        Ok(page)
    }

    pub fn write_page(&mut self, page: &Page) -> Result<()> {
        // P3-6: Calculate checksum before writing
        let mut page_with_checksum = page.clone();
        self.checksum_manager.calculate_page(&mut page_with_checksum);

        // Write to WAL first (for durability)
        if let Some(ref mut wal) = self.wal {
            wal.write_page(&page_with_checksum)?;
        }
        // Also update cache
        self.cache.put(page_with_checksum.clone(), true);
        Ok(())
    }

    pub fn allocate_page(&mut self) -> Result<PageId> {
        let page_id = self.header.database_size;
        self.header.database_size += 1;

        let mut page = Page::new(page_id);
        
        // P3-6: Calculate initial checksum for new page
        self.checksum_manager.calculate_page(&mut page);
        
        self.cache.put(page, true);

        Ok(page_id)
    }

    pub fn flush(&mut self) -> Result<()> {
        // Flush WAL first (this batches all writes into single fsync)
        if let Some(ref mut wal) = self.wal {
            wal.flush()?;
        }

        // Periodically checkpoint WAL to data file
        if let Some(ref mut wal) = self.wal {
            if wal.needs_checkpoint() {
                self.checkpoint()?;
            }
        }

        // Write header directly for metadata updates
        self.file.seek(SeekFrom::Start(0))?;
        self.file.write_all(&self.header.to_bytes())?;
        self.file.sync_all()?;

        Ok(())
    }

    /// Checkpoint WAL: flush accumulated changes to data file
    pub fn checkpoint(&mut self) -> Result<()> {
        // Take wal out temporarily to avoid borrow checker issues
        let mut wal = match self.wal.take() {
            Some(wal) => wal,
            None => return Ok(()),
        };

        let file = &mut self.file;
        let checksum_mgr = &mut self.checksum_manager;
        
        let result = wal.checkpoint(|page_id, data| {
            let offset = if page_id == 0 {
                0
            } else {
                DatabaseHeader::SIZE as u64 + (page_id as u64 - 1) * PAGE_SIZE as u64
            };
            file.seek(SeekFrom::Start(offset))?;
            if page_id == 0 {
                file.write_all(&data[..DatabaseHeader::SIZE])?;
            } else {
                // P3-6: Verify checksum before writing to data file
                let page = Page::from_bytes(page_id, data.to_vec());
                checksum_mgr.verify_page(&page)?;
                file.write_all(data)?;
            }
            Ok(())
        });

        let checkpointed = result?;

        if checkpointed > 0 {
            // Sync data file after checkpoint
            self.file.sync_all()?;
        }

        // Put wal back
        self.wal = Some(wal);

        Ok(())
    }

    /// P3-6: Verify checksums for all pages in a range
    pub fn verify_checksums(&mut self, start_page: PageId, end_page: PageId) -> Result<Vec<PageId>> {
        let mut failed_pages = Vec::new();

        for page_id in start_page..=end_page {
            match self.get_page(page_id) {
                Ok(_) => {}
                Err(PagerError::CorruptedPage { .. }) => {
                    failed_pages.push(page_id);
                }
                Err(_) => {
                    // Other errors (e.g., page not found) - skip
                }
            }
        }

        Ok(failed_pages)
    }

    /// P3-6: Get checksum manager reference
    pub fn checksum_manager(&self) -> &ChecksumManager {
        &self.checksum_manager
    }

    /// P3-6: Get checksum manager mutable reference
    pub fn checksum_manager_mut(&mut self) -> &mut ChecksumManager {
        &mut self.checksum_manager
    }

    fn read_page_from_file(&mut self, page_id: PageId) -> Result<Page> {
        if page_id >= self.header.database_size {
            return Err(PagerError::PageNotFound(page_id));
        }

        let mut page = Page::new(page_id);

        if page_id == 0 {
            // Page 0 contains the database header
            self.file.seek(SeekFrom::Start(0))?;
            self.file.read_exact(&mut page.data[..DatabaseHeader::SIZE])?;
        } else {
            // Other pages are stored after the header
            let offset = DatabaseHeader::SIZE as u64 + (page_id as u64 - 1) * PAGE_SIZE as u64;
            self.file.seek(SeekFrom::Start(offset))?;
            self.file.read_exact(&mut page.data)?;
        }

        Ok(page)
    }

    fn _write_page_to_file(&mut self, page: &Page) -> Result<()> {
        if page.id == 0 {
            // Page 0 contains the database header
            self.file.seek(SeekFrom::Start(0))?;
            self.file.write_all(&page.data[..DatabaseHeader::SIZE])?;
        } else {
            // Other pages are stored after the header
            let offset = DatabaseHeader::SIZE as u64 + (page.id as u64 - 1) * PAGE_SIZE as u64;
            self.file.seek(SeekFrom::Start(offset))?;
            self.file.write_all(&page.data)?;
        }

        Ok(())
    }

    pub fn header(&self) -> &DatabaseHeader {
        &self.header
    }

    pub fn header_mut(&mut self) -> &mut DatabaseHeader {
        &mut self.header
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn test_pager_create_new() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap();

        let pager = Pager::open(path);
        assert!(pager.is_ok());
    }

    #[test]
    fn test_pager_allocate_page() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap();

        let mut pager = Pager::open(path).unwrap();
        let page_id = pager.allocate_page().unwrap();
        assert_eq!(page_id, 1);
    }

    #[test]
    fn test_pager_read_write_page() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap();

        let mut pager = Pager::open(path).unwrap();
        let page_id = pager.allocate_page().unwrap();

        let mut page = pager.get_page(page_id).unwrap();
        page.data[0] = 42;
        pager.write_page(&page).unwrap();

        pager.flush().unwrap();

        let mut pager2 = Pager::open(path).unwrap();
        let page = pager2.get_page(page_id).unwrap();
        assert_eq!(page.data[0], 42);
    }

    // P3-6: Checksum tests
    #[test]
    fn test_pager_checksum_on_write() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap();

        let mut pager = Pager::open(path).unwrap();
        let page_id = pager.allocate_page().unwrap();

        let mut page = pager.get_page(page_id).unwrap();
        page.data[10] = 0xAB;
        page.data[11] = 0xCD;
        pager.write_page(&page).unwrap();

        // Checksum should be calculated
        assert!(pager.checksum_manager().stats().checksums_calculated > 0);
    }

    #[test]
    fn test_pager_checksum_verification() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap();

        let mut pager = Pager::open(path).unwrap();
        let page_id = pager.allocate_page().unwrap();

        let mut page = pager.get_page(page_id).unwrap();
        page.data[10] = 0xAB;
        pager.write_page(&page).unwrap();
        pager.flush().unwrap();

        // Reopen and read - should verify checksum
        let mut pager2 = Pager::open(path).unwrap();
        let result = pager2.get_page(page_id);
        assert!(result.is_ok());
        
        // Stats should show verification
        assert!(pager2.checksum_manager().stats().pages_verified > 0);
    }

    #[test]
    fn test_pager_with_disabled_checksum() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap();

        let mut pager = Pager::open_with_config(path, ChecksumConfig::disabled()).unwrap();
        let page_id = pager.allocate_page().unwrap();

        let page = pager.get_page(page_id).unwrap();
        pager.write_page(&page).unwrap();

        // No checksums should be calculated
        assert_eq!(pager.checksum_manager().stats().checksums_calculated, 0);
        assert!(!pager.checksum_manager().is_enabled());
    }
}
