#[repr(C, packed)]
pub struct DatabaseHeader {
    pub magic: [u8; 16],
    pub page_size: u16,
    pub file_format_write: u8,
    pub file_format_read: u8,
    pub reserved_space: u8,
    pub max_payload_frac: u8,
    pub min_payload_frac: u8,
    pub leaf_payload_frac: u8,
    pub file_change_counter: u32,
    pub database_size: u32,
    pub first_freelist_trunk: u32,
    pub freelist_pages: u32,
    pub schema_cookie: u32,
    pub schema_format: u32,
    pub default_cache_size: u32,
    pub largest_root_btree: u32,
    pub text_encoding: u32,
    pub user_version: u32,
    pub incremental_vacuum: u32,
    pub application_id: u32,
    pub reserved: [u8; 20],
    pub version_valid_for: u32,
    pub sqlite_version: u32,
}

impl DatabaseHeader {
    pub const SIZE: usize = 100;
    pub const MAGIC: &[u8] = b"SQLite format 3\0";

    pub fn new(page_size: u16) -> Self {
        Self {
            magic: *b"SQLite format 3\0",
            page_size,
            file_format_write: 1,
            file_format_read: 1,
            reserved_space: 0,
            max_payload_frac: 64,
            min_payload_frac: 32,
            leaf_payload_frac: 32,
            file_change_counter: 0,
            database_size: 1,
            first_freelist_trunk: 0,
            freelist_pages: 0,
            schema_cookie: 0,
            schema_format: 4,
            default_cache_size: 0,
            largest_root_btree: 0,
            text_encoding: 1,
            user_version: 0,
            incremental_vacuum: 0,
            application_id: 0,
            reserved: [0; 20],
            version_valid_for: 0,
            sqlite_version: 3045000,
        }
    }

    pub fn validate(&self) -> Result<(), crate::pager::error::PagerError> {
        if &self.magic != Self::MAGIC {
            return Err(crate::pager::error::PagerError::InvalidFormat);
        }
        if self.page_size < 512 || self.page_size > 32768 || (self.page_size & (self.page_size - 1)) != 0 {
            return Err(crate::pager::error::PagerError::InvalidFormat);
        }
        Ok(())
    }

    pub fn to_bytes(&self) -> [u8; Self::SIZE] {
        let mut bytes = [0u8; Self::SIZE];
        let ptr = self as *const _ as *const u8;
        unsafe {
            std::ptr::copy_nonoverlapping(ptr, bytes.as_mut_ptr(), Self::SIZE);
        }
        bytes
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, crate::pager::error::PagerError> {
        if bytes.len() < Self::SIZE {
            return Err(crate::pager::error::PagerError::InvalidFormat);
        }
        let header = unsafe {
            std::ptr::read(bytes.as_ptr() as *const Self)
        };
        header.validate()?;
        Ok(header)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_header_creation() {
        let header = DatabaseHeader::new(4096);
        let magic = header.magic;
        assert_eq!(&magic, b"SQLite format 3\0");
        let page_size = header.page_size;
        assert_eq!(page_size, 4096);
    }

    #[test]
    fn test_header_serialization() {
        let header = DatabaseHeader::new(4096);
        let page_size1 = header.page_size;
        let bytes = header.to_bytes();
        let header2 = DatabaseHeader::from_bytes(&bytes).unwrap();
        let page_size2 = header2.page_size;
        assert_eq!(page_size1, page_size2);
    }

    #[test]
    fn test_header_validation() {
        let header = DatabaseHeader::new(4096);
        assert!(header.validate().is_ok());
    }
}
