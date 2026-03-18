pub mod cache;
pub mod error;
pub mod header;
pub mod page;

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use crate::pager::cache::PageCache;
use crate::pager::header::DatabaseHeader;
pub use crate::pager::page::Page;
use crate::pager::page::PAGE_SIZE;
use crate::storage::wal::Wal;

pub use crate::pager::error::{PagerError, Result};
pub use crate::pager::page::PageId;

pub struct Pager {
    file: File,
    cache: PageCache,
    header: DatabaseHeader,
    wal: Option<Wal>,
    path: String,
}

impl Pager {
    pub fn open(path: &str) -> Result<Self> {
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
            path: path.to_string(),
        })
    }

    pub fn get_page(&mut self, page_id: PageId) -> Result<Page> {
        // Check cache first
        if let Some(page) = self.cache.get(page_id) {
            return Ok(page.clone());
        }

        // Check WAL for uncheckpointed pages
        if let Some(ref mut wal) = self.wal {
            if let Some(data) = wal.read_page(page_id)? {
                let page = Page::from_bytes(page_id, data);
                self.cache.put(page.clone(), false);
                return Ok(page);
            }
        }

        // Read from data file
        let page = self.read_page_from_file(page_id)?;
        self.cache.put(page.clone(), false);

        Ok(page)
    }

    pub fn write_page(&mut self, page: &Page) -> Result<()> {
        // Write to WAL first (for durability)
        if let Some(ref mut wal) = self.wal {
            wal.write_page(page)?;
        }
        // Also update cache
        self.cache.put(page.clone(), true);
        Ok(())
    }

    pub fn allocate_page(&mut self) -> Result<PageId> {
        let page_id = self.header.database_size;
        self.header.database_size += 1;

        let page = Page::new(page_id);
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

    fn write_page_to_file(&mut self, page: &Page) -> Result<()> {
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
}
