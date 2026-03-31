//! Page Checksum - Data Integrity Verification (P3-6)
//!
//! This module implements CRC32 checksums for page integrity:
//! - Checksum calculation on page write
//! - Checksum verification on page read
//! - Corrupted page detection and reporting
//! - Optional checksum verification modes

use crate::pager::page::{Page, PAGE_SIZE};
use crate::pager::error::{PagerError, Result};
use crate::pager::header::DatabaseHeader;

/// Checksum algorithm type
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ChecksumAlgorithm {
    /// CRC32 (default, good balance of speed and detection)
    Crc32,
    /// No checksum (for performance testing only)
    None,
}

impl Default for ChecksumAlgorithm {
    fn default() -> Self {
        ChecksumAlgorithm::Crc32
    }
}

/// Checksum verification mode
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ChecksumMode {
    /// Always verify checksums on read
    Strict,
    /// Verify only on debug builds or when explicitly requested
    Relaxed,
    /// Skip verification (not recommended for production)
    Skip,
}

impl Default for ChecksumMode {
    fn default() -> Self {
        ChecksumMode::Strict
    }
}

/// Checksum configuration
#[derive(Debug, Clone, Copy)]
pub struct ChecksumConfig {
    pub algorithm: ChecksumAlgorithm,
    pub mode: ChecksumMode,
    /// Verify checksums on every read (performance impact)
    pub verify_on_read: bool,
    /// Calculate checksums on every write
    pub calculate_on_write: bool,
}

impl Default for ChecksumConfig {
    fn default() -> Self {
        Self {
            algorithm: ChecksumAlgorithm::default(),
            mode: ChecksumMode::default(),
            verify_on_read: true,
            calculate_on_write: true,
        }
    }
}

impl ChecksumConfig {
    /// Strict mode - always verify
    pub fn strict() -> Self {
        Self {
            algorithm: ChecksumAlgorithm::Crc32,
            mode: ChecksumMode::Strict,
            verify_on_read: true,
            calculate_on_write: true,
        }
    }

    /// Relaxed mode - verify only when needed
    pub fn relaxed() -> Self {
        Self {
            algorithm: ChecksumAlgorithm::Crc32,
            mode: ChecksumMode::Relaxed,
            verify_on_read: cfg!(debug_assertions), // Verify in debug builds
            calculate_on_write: true,
        }
    }

    /// Disabled - no checksums (not recommended)
    pub fn disabled() -> Self {
        Self {
            algorithm: ChecksumAlgorithm::None,
            mode: ChecksumMode::Skip,
            verify_on_read: false,
            calculate_on_write: false,
        }
    }
}

/// CRC32 lookup table for fast calculation
static CRC32_TABLE: [u32; 256] = {
    let mut table = [0u32; 256];
    let mut i = 0;
    while i < 256 {
        let mut crc = i as u32;
        let mut j = 0;
        while j < 8 {
            crc = if crc & 1 == 1 {
                (crc >> 1) ^ 0xEDB88320
            } else {
                crc >> 1
            };
            j += 1;
        }
        table[i] = crc;
        i += 1;
    }
    table
};

/// Calculate CRC32 checksum for data
pub fn calculate_crc32(data: &[u8]) -> u32 {
    let mut crc: u32 = 0xFFFFFFFF;
    for &byte in data {
        let idx = ((crc ^ byte as u32) & 0xFF) as usize;
        crc = (crc >> 8) ^ CRC32_TABLE[idx];
    }
    !crc
}

/// Verify CRC32 checksum
pub fn verify_crc32(data: &[u8], expected: u32) -> bool {
    calculate_crc32(data) == expected
}

/// Page checksum operations trait
pub trait PageChecksumOps {
    /// Calculate and store checksum for the page
    fn calculate_checksum(&mut self) -> u32;
    
    /// Verify page checksum
    fn verify_checksum(&self) -> Result<()>;
    
    /// Get stored checksum
    fn get_checksum(&self) -> u32;
    
    /// Set checksum directly (use with caution)
    fn set_checksum(&mut self, checksum: u32);
    
    /// Check if page has valid checksum
    fn has_valid_checksum(&self) -> bool;
    
    /// Calculate checksum for page data (excluding checksum field itself)
    fn calculate_data_checksum(&self) -> u32;
}

impl PageChecksumOps for Page {
    fn calculate_checksum(&mut self) -> u32 {
        let checksum = self.calculate_data_checksum();
        self.set_checksum(checksum);
        checksum
    }
    
    fn verify_checksum(&self) -> Result<()> {
        let stored = self.get_checksum();
        let calculated = self.calculate_data_checksum();
        
        if stored != calculated {
            return Err(PagerError::CorruptedPage {
                page_id: self.id,
                stored_checksum: stored,
                calculated_checksum: calculated,
            });
        }
        
        Ok(())
    }
    
    fn get_checksum(&self) -> u32 {
        // Checksum is stored in the first 4 bytes of page data (PageHeader.checksum)
        u32::from_le_bytes([
            self.data[0],
            self.data[1],
            self.data[2],
            self.data[3],
        ])
    }
    
    fn set_checksum(&mut self, checksum: u32) {
        let bytes = checksum.to_le_bytes();
        self.data[0..4].copy_from_slice(&bytes);
    }
    
    fn has_valid_checksum(&self) -> bool {
        self.verify_checksum().is_ok()
    }
    
    fn calculate_data_checksum(&self) -> u32 {
        // Calculate checksum over data excluding the checksum field itself
        // Skip first 4 bytes (checksum), calculate over remaining data
        calculate_crc32(&self.data[4..])
    }
}

/// Checksum manager for database-wide checksum operations
pub struct ChecksumManager {
    config: ChecksumConfig,
    /// Statistics
    stats: ChecksumStats,
}

/// Checksum verification statistics
#[derive(Debug, Clone, Default)]
pub struct ChecksumStats {
    pub pages_verified: u64,
    pub pages_failed: u64,
    pub checksums_calculated: u64,
    pub last_error: Option<String>,
}

impl ChecksumManager {
    pub fn new(config: ChecksumConfig) -> Self {
        Self {
            config,
            stats: ChecksumStats::default(),
        }
    }

    pub fn with_default_config() -> Self {
        Self::new(ChecksumConfig::default())
    }

    /// Verify page checksum if enabled
    pub fn verify_page(&mut self, page: &Page) -> Result<()> {
        if !self.config.verify_on_read || self.config.algorithm == ChecksumAlgorithm::None {
            return Ok(());
        }

        self.stats.pages_verified += 1;

        match page.verify_checksum() {
            Ok(()) => Ok(()),
            Err(e) => {
                self.stats.pages_failed += 1;
                self.stats.last_error = Some(format!("Page {} checksum failed: {:?}", page.id, e));
                Err(e)
            }
        }
    }

    /// Calculate and store checksum for page if enabled
    pub fn calculate_page(&mut self, page: &mut Page) -> Option<u32> {
        if !self.config.calculate_on_write || self.config.algorithm == ChecksumAlgorithm::None {
            return None;
        }

        self.stats.checksums_calculated += 1;
        Some(page.calculate_checksum())
    }

    /// Get current statistics
    pub fn stats(&self) -> &ChecksumStats {
        &self.stats
    }

    /// Reset statistics
    pub fn reset_stats(&mut self) {
        self.stats = ChecksumStats::default();
    }

    /// Check if checksums are enabled
    pub fn is_enabled(&self) -> bool {
        self.config.algorithm != ChecksumAlgorithm::None
    }

    /// Get configuration
    pub fn config(&self) -> &ChecksumConfig {
        &self.config
    }

    /// Update configuration
    pub fn set_config(&mut self, config: ChecksumConfig) {
        self.config = config;
    }
}

/// Verify checksum for all pages in a range (useful for consistency checks)
pub fn verify_page_range<F>(
    mut page_reader: F,
    start_page: u32,
    end_page: u32,
) -> Result<Vec<u32>>
where
    F: FnMut(u32) -> Result<Page>,
{
    let mut failed_pages = Vec::new();

    for page_id in start_page..=end_page {
        match page_reader(page_id) {
            Ok(page) => {
                if let Err(_) = page.verify_checksum() {
                    failed_pages.push(page_id);
                }
            }
            Err(_) => {
                failed_pages.push(page_id);
            }
        }
    }

    if failed_pages.is_empty() {
        Ok(failed_pages)
    } else {
        Err(PagerError::Corrupted(format!(
            "Checksum verification failed for pages: {:?}",
            failed_pages
        )))
    }
}

/// Checksum error types
#[derive(Debug, Clone)]
pub enum ChecksumError {
    InvalidChecksum {
        page_id: u32,
        expected: u32,
        actual: u32,
    },
    UnsupportedAlgorithm,
}

impl std::fmt::Display for ChecksumError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ChecksumError::InvalidChecksum { page_id, expected, actual } => {
                write!(f, "Checksum mismatch on page {}: expected {:08X}, got {:08X}", 
                    page_id, expected, actual)
            }
            ChecksumError::UnsupportedAlgorithm => {
                write!(f, "Unsupported checksum algorithm")
            }
        }
    }
}

impl std::error::Error for ChecksumError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_crc32_calculation() {
        // Test vector: "123456789" should give 0xCBF43926
        let data = b"123456789";
        let checksum = calculate_crc32(data);
        assert_eq!(checksum, 0xCBF43926);
    }

    #[test]
    fn test_crc32_empty() {
        let checksum = calculate_crc32(b"");
        assert_eq!(checksum, 0x00000000);
    }

    #[test]
    fn test_crc32_verification() {
        let data = b"Hello, World!";
        let checksum = calculate_crc32(data);
        assert!(verify_crc32(data, checksum));
        assert!(!verify_crc32(data, checksum + 1));
    }

    #[test]
    fn test_page_checksum_ops() {
        let mut page = Page::new(1);
        
        // Fill with some data
        for i in 0..100 {
            page.data[i + 4] = (i % 256) as u8;
        }
        
        // Calculate checksum
        let checksum = page.calculate_checksum();
        
        // Verify checksum is stored
        assert_eq!(page.get_checksum(), checksum);
        
        // Verify passes
        assert!(page.has_valid_checksum());
        assert!(page.verify_checksum().is_ok());
    }

    #[test]
    fn test_page_checksum_corruption() {
        let mut page = Page::new(1);
        
        // Fill with data and calculate checksum
        for i in 0..100 {
            page.data[i + 4] = (i % 256) as u8;
        }
        page.calculate_checksum();
        
        // Corrupt the data
        page.data[10] ^= 0xFF;
        
        // Verification should fail
        assert!(!page.has_valid_checksum());
        assert!(page.verify_checksum().is_err());
    }

    #[test]
    fn test_checksum_manager() {
        let mut manager = ChecksumManager::with_default_config();
        
        let mut page = Page::new(1);
        for i in 0..100 {
            page.data[i + 4] = i as u8;
        }
        
        // Calculate checksum
        let checksum = manager.calculate_page(&mut page);
        assert!(checksum.is_some());
        
        // Verify
        assert!(manager.verify_page(&page).is_ok());
        
        // Check stats
        assert_eq!(manager.stats().checksums_calculated, 1);
        assert_eq!(manager.stats().pages_verified, 1);
    }

    #[test]
    fn test_checksum_manager_corruption() {
        let mut manager = ChecksumManager::with_default_config();
        
        let mut page = Page::new(1);
        for i in 0..100 {
            page.data[i + 4] = i as u8;
        }
        
        // Calculate and then corrupt
        manager.calculate_page(&mut page);
        page.data[10] ^= 0xFF;
        
        // Verify should fail
        assert!(manager.verify_page(&page).is_err());
        assert_eq!(manager.stats().pages_failed, 1);
    }

    #[test]
    fn test_disabled_checksum() {
        let mut manager = ChecksumManager::new(ChecksumConfig::disabled());
        
        let mut page = Page::new(1);
        
        // Should not calculate
        assert!(manager.calculate_page(&mut page).is_none());
        
        // Should always pass verification
        assert!(manager.verify_page(&page).is_ok());
        assert!(!manager.is_enabled());
    }

    #[test]
    fn test_checksum_config_presets() {
        let strict = ChecksumConfig::strict();
        assert!(strict.verify_on_read);
        assert!(strict.calculate_on_write);
        
        let relaxed = ChecksumConfig::relaxed();
        assert!(relaxed.calculate_on_write);
        // verify_on_read depends on build mode
        
        let disabled = ChecksumConfig::disabled();
        assert!(!disabled.verify_on_read);
        assert!(!disabled.calculate_on_write);
    }

    #[test]
    fn test_data_integrity() {
        // Test that different data produces different checksums
        let data1 = b"Hello, World!";
        let data2 = b"Hello, World?";
        
        let crc1 = calculate_crc32(data1);
        let crc2 = calculate_crc32(data2);
        
        assert_ne!(crc1, crc2);
        
        // Same data should produce same checksum
        let crc1_again = calculate_crc32(data1);
        assert_eq!(crc1, crc1_again);
    }
}
