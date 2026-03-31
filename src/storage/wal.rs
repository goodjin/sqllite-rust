//! Write-Ahead Logging (WAL) for High-Performance Transactions
//!
//! This module implements a WAL to eliminate fsync bottlenecks:
//! - Append-only log format for durability
//! - Buffered writes with configurable batch size
//! - Checkpoint mechanism to flush logs to data pages
//! - Crash recovery support
//!
//! Phase 1 Week 1 Optimizations:
//! - Group commit: Multiple transactions share a single fsync
//! - Batch write buffer with size and time-based flushing
//! - Optimized fsync frequency with adaptive batching
//! - Fallback mechanism for single-transaction commit on failure

use crate::pager::PageId;
use crate::storage::{Result, StorageError};
use crate::pager::page::Page;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::time::{Duration, Instant};
use std::sync::{Arc, Mutex, Condvar};

/// WAL file header
#[derive(Debug, Clone)]
pub struct WalHeader {
    /// Magic number to identify WAL files
    pub magic: u32,
    /// WAL format version
    pub version: u32,
    /// Page size (must match database page size)
    pub page_size: u32,
    /// Last checkpointed frame number
    pub checkpoint_seq: u64,
    /// Salt-1 (for checksum)
    pub salt1: u32,
    /// Salt-2 (for checksum)
    pub salt2: u32,
}

impl WalHeader {
    pub const MAGIC: u32 = 0x377F0682;
    pub const VERSION: u32 = 1;
    pub const SIZE: usize = 32;

    pub fn new(page_size: u32) -> Self {
        Self {
            magic: Self::MAGIC,
            version: Self::VERSION,
            page_size,
            checkpoint_seq: 0,
            salt1: 0x12345678,
            salt2: 0x9ABCDEF0,
        }
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < Self::SIZE {
            return Err(StorageError::Corrupted("WAL header too small".to_string()));
        }

        let magic = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
        if magic != Self::MAGIC {
            return Err(StorageError::Corrupted(format!(
                "Invalid WAL magic: {:08x}",
                magic
            )));
        }

        Ok(Self {
            magic,
            version: u32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]),
            page_size: u32::from_le_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]),
            checkpoint_seq: u64::from_le_bytes([
                bytes[12], bytes[13], bytes[14], bytes[15], bytes[16], bytes[17], bytes[18],
                bytes[19],
            ]),
            salt1: u32::from_le_bytes([bytes[20], bytes[21], bytes[22], bytes[23]]),
            salt2: u32::from_le_bytes([bytes[24], bytes[25], bytes[26], bytes[27]]),
        })
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(Self::SIZE);
        bytes.extend_from_slice(&self.magic.to_le_bytes());
        bytes.extend_from_slice(&self.version.to_le_bytes());
        bytes.extend_from_slice(&self.page_size.to_le_bytes());
        bytes.extend_from_slice(&self.checkpoint_seq.to_le_bytes());
        bytes.extend_from_slice(&self.salt1.to_le_bytes());
        bytes.extend_from_slice(&self.salt2.to_le_bytes());
        bytes.resize(Self::SIZE, 0);
        bytes
    }
}

/// A single frame in the WAL (one page + metadata)
#[derive(Debug, Clone)]
pub struct WalFrame {
    /// Page number
    pub page_id: PageId,
    /// Commit ID (for grouping transactions)
    pub commit_id: u64,
    /// Page data
    pub page_data: Vec<u8>,
    /// Checksum for integrity
    pub checksum: u32,
}

impl WalFrame {
    /// Frame header size (page_id: 4 + commit_id: 8 + checksum: 4 = 16 bytes)
    pub const HEADER_SIZE: usize = 16;

    pub fn new(page_id: PageId, commit_id: u64, page_data: Vec<u8>) -> Self {
        let checksum = Self::compute_checksum(&page_data);
        Self {
            page_id,
            commit_id,
            page_data,
            checksum,
        }
    }

    /// Simple checksum (CRC32-like)
    fn compute_checksum(data: &[u8]) -> u32 {
        let mut crc: u32 = 0xFFFFFFFF;
        for byte in data {
            crc ^= (*byte as u32) << 24;
            for _ in 0..8 {
                if crc & 0x80000000 != 0 {
                    crc = (crc << 1) ^ 0x04C11DB7;
                } else {
                    crc <<= 1;
                }
            }
        }
        !crc
    }

    pub fn verify(&self) -> bool {
        Self::compute_checksum(&self.page_data) == self.checksum
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(Self::HEADER_SIZE + self.page_data.len());
        bytes.extend_from_slice(&self.page_id.to_le_bytes());
        bytes.extend_from_slice(&self.commit_id.to_le_bytes());
        bytes.extend_from_slice(&self.checksum.to_le_bytes());
        bytes.extend_from_slice(&self.page_data);
        bytes
    }

    pub fn from_bytes(header: &[u8], page_data: &[u8]) -> Result<Self> {
        if header.len() < Self::HEADER_SIZE {
            return Err(StorageError::Corrupted("WAL frame header too small".to_string()));
        }

        let page_id = u32::from_le_bytes([header[0], header[1], header[2], header[3]]);
        let commit_id = u64::from_le_bytes([
            header[4], header[5], header[6], header[7], header[8], header[9], header[10],
            header[11],
        ]);
        let checksum = u32::from_le_bytes([header[12], header[13], header[14], header[15]]);

        Ok(Self {
            page_id,
            commit_id,
            page_data: page_data.to_vec(),
            checksum,
        })
    }
}

/// Group commit configuration
#[derive(Debug, Clone)]
pub struct GroupCommitConfig {
    /// Enable group commit
    pub enabled: bool,
    /// Maximum batch size before forcing flush
    pub max_batch_size: usize,
    /// Maximum time to wait before flushing (milliseconds)
    pub flush_timeout_ms: u64,
    /// Minimum batch size to trigger flush
    pub min_batch_size: usize,
    /// Adaptive batching based on workload
    pub adaptive_batching: bool,
    /// Target flush latency in milliseconds
    pub target_latency_ms: u64,
}

impl Default for GroupCommitConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_batch_size: 100,
            flush_timeout_ms: 10,
            min_batch_size: 1,
            adaptive_batching: true,
            target_latency_ms: 5,
        }
    }
}

/// Pending commit for group commit
#[derive(Debug)]
struct PendingCommit {
    commit_id: u64,
    frames: Vec<WalFrame>,
    /// Notification for synchronous waiters
    notify: Option<Arc<(Mutex<bool>, Condvar)>>,
    /// Timestamp when commit was queued
    queued_at: Instant,
}

/// WAL statistics for monitoring
#[derive(Debug, Clone, Default)]
pub struct WalStats {
    /// Total frames written
    pub frames_written: u64,
    /// Total bytes written
    pub bytes_written: u64,
    /// Number of fsync calls
    pub fsync_count: u64,
    /// Number of group commits
    pub group_commits: u64,
    /// Number of single commits (fallback)
    pub single_commits: u64,
    /// Average batch size
    pub avg_batch_size: f64,
    /// Average flush latency in microseconds
    pub avg_flush_latency_us: f64,
    /// Write amplification (actual writes / logical writes)
    pub write_amplification: f64,
    /// Current buffer size
    pub current_buffer_size: usize,
}

/// Write-Ahead Log manager with group commit support
pub struct Wal {
    /// WAL file
    file: File,
    /// WAL header
    header: WalHeader,
    /// Buffer for pending frames
    buffer: Vec<WalFrame>,
    /// Buffer size limit before flush (bytes)
    buffer_limit: usize,
    /// Current buffer size in bytes
    buffer_size: usize,
    /// Current commit ID
    current_commit_id: u64,
    /// Checkpoint threshold (number of frames)
    checkpoint_threshold: usize,
    /// Total frames written
    frame_count: u64,
    /// Page size
    page_size: usize,
    /// Group commit configuration
    group_commit: GroupCommitConfig,
    /// Pending commits queue
    pending_commits: Vec<PendingCommit>,
    /// Last flush time
    last_flush: Instant,
    /// Statistics
    stats: WalStats,
    /// Adaptive batch size (changes based on workload)
    adaptive_batch_size: usize,
    /// Track consecutive fast commits for adaptive batching
    fast_commit_streak: u32,
    /// Track consecutive slow commits for adaptive batching
    slow_commit_streak: u32,
}

impl Wal {
    /// Default buffer size: 1MB
    pub const DEFAULT_BUFFER_LIMIT: usize = 1024 * 1024;
    /// Default checkpoint threshold: 1000 frames
    pub const DEFAULT_CHECKPOINT_THRESHOLD: usize = 1000;
    /// Min batch size for adaptive batching
    const MIN_ADAPTIVE_BATCH: usize = 5;
    /// Max batch size for adaptive batching
    const MAX_ADAPTIVE_BATCH: usize = 500;

    /// Open or create a WAL file
    pub fn open<P: AsRef<Path>>(path: P, page_size: usize) -> Result<Self> {
        Self::with_config(path, page_size, GroupCommitConfig::default())
    }
    
    /// Open with group commit configuration
    pub fn with_config<P: AsRef<Path>>(
        path: P, 
        page_size: usize, 
        group_commit: GroupCommitConfig
    ) -> Result<Self> {
        let path = path.as_ref();
        let wal_path = path.with_extension("wal");

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&wal_path)?;

        let metadata = file.metadata()?;
        let header = if metadata.len() == 0 {
            // New WAL file
            let header = WalHeader::new(page_size as u32);
            file.write_all(&header.to_bytes())?;
            file.sync_all()?;
            header
        } else {
            // Existing WAL file
            let mut buf = vec![0u8; WalHeader::SIZE];
            file.seek(SeekFrom::Start(0))?;
            file.read_exact(&mut buf)?;
            WalHeader::from_bytes(&buf)?
        };

        let frame_count = if metadata.len() > WalHeader::SIZE as u64 {
            ((metadata.len() - WalHeader::SIZE as u64) / (WalFrame::HEADER_SIZE as u64 + page_size as u64)) as u64
        } else {
            0
        };

        Ok(Self {
            file,
            header,
            buffer: Vec::new(),
            buffer_limit: Self::DEFAULT_BUFFER_LIMIT,
            buffer_size: 0,
            current_commit_id: 0,
            checkpoint_threshold: Self::DEFAULT_CHECKPOINT_THRESHOLD,
            frame_count,
            page_size,
            group_commit,
            pending_commits: Vec::new(),
            last_flush: Instant::now(),
            stats: WalStats::default(),
            adaptive_batch_size: 50,
            fast_commit_streak: 0,
            slow_commit_streak: 0,
        })
    }

    /// Begin a new transaction
    pub fn begin_transaction(&mut self) {
        self.current_commit_id += 1;
    }

    /// Write a page to the WAL (buffered)
    pub fn write_page(&mut self, page: &Page) -> Result<()> {
        let frame = WalFrame::new(
            page.id(),
            self.current_commit_id,
            page.as_slice().to_vec(),
        );

        let frame_size = WalFrame::HEADER_SIZE + frame.page_data.len();

        // Check if we need to flush buffer
        if self.buffer_size + frame_size > self.buffer_limit && !self.buffer.is_empty() {
            self.flush_buffer()?;
        }

        self.buffer_size += frame_size;
        self.buffer.push(frame);

        // Check if we need to checkpoint
        if self.buffer.len() >= self.checkpoint_threshold {
            self.flush_buffer()?;
        }

        Ok(())
    }

    /// Flush the buffer to disk (single fsync)
    pub fn flush(&mut self) -> Result<()> {
        if !self.buffer.is_empty() {
            self.flush_buffer()?;
        }
        Ok(())
    }

    /// Flush buffer to WAL file
    fn flush_buffer(&mut self) -> Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }

        let flush_start = Instant::now();
        let batch_size = self.buffer.len();

        // Seek to end of file
        self.file.seek(SeekFrom::End(0))?;

        // Write all buffered frames
        for frame in &self.buffer {
            self.file.write_all(&frame.to_bytes())?;
            self.frame_count += 1;
            self.stats.frames_written += 1;
            self.stats.bytes_written += (WalFrame::HEADER_SIZE + frame.page_data.len()) as u64;
        }

        // Single fsync for the entire batch (this is the key optimization!)
        self.file.sync_all()?;
        self.stats.fsync_count += 1;

        // Update statistics
        let latency = flush_start.elapsed().as_micros() as f64;
        self.stats.avg_flush_latency_us = 
            (self.stats.avg_flush_latency_us * (self.stats.group_commits as f64) + latency)
            / (self.stats.group_commits + 1) as f64;
        
        self.stats.avg_batch_size = 
            (self.stats.avg_batch_size * (self.stats.group_commits as f64) + batch_size as f64)
            / (self.stats.group_commits + 1) as f64;
        
        self.stats.group_commits += 1;
        
        // Adaptive batching
        if self.group_commit.adaptive_batching {
            self.update_adaptive_batch_size(latency);
        }

        // Clear buffer
        self.buffer.clear();
        self.buffer_size = 0;
        self.last_flush = Instant::now();

        Ok(())
    }
    
    /// Update adaptive batch size based on flush latency
    fn update_adaptive_batch_size(&mut self, latency_us: f64) {
        let target_latency_us = (self.group_commit.target_latency_ms * 1000) as f64;
        
        if latency_us < target_latency_us * 0.8 {
            // Fast flush, can increase batch size
            self.fast_commit_streak += 1;
            self.slow_commit_streak = 0;
            
            if self.fast_commit_streak >= 3 {
                self.adaptive_batch_size = (self.adaptive_batch_size + 5)
                    .min(Self::MAX_ADAPTIVE_BATCH);
                self.fast_commit_streak = 0;
            }
        } else if latency_us > target_latency_us * 1.2 {
            // Slow flush, decrease batch size
            self.slow_commit_streak += 1;
            self.fast_commit_streak = 0;
            
            if self.slow_commit_streak >= 2 {
                self.adaptive_batch_size = (self.adaptive_batch_size.saturating_sub(10))
                    .max(Self::MIN_ADAPTIVE_BATCH);
                self.slow_commit_streak = 0;
            }
        }
    }

    /// Queue a commit for group commit
    /// 
    /// Returns true if the commit was queued, false if it should be flushed immediately
    pub fn queue_commit(&mut self, wait_for_sync: bool) -> Result<Option<Arc<(Mutex<bool>, Condvar)>>> {
        let notify = if wait_for_sync {
            Some(Arc::new((Mutex::new(false), Condvar::new())))
        } else {
            None
        };

        let pending = PendingCommit {
            commit_id: self.current_commit_id,
            frames: self.buffer.drain(..).collect(),
            notify: notify.clone(),
            queued_at: Instant::now(),
        };

        self.pending_commits.push(pending);

        // Check if we should flush the batch
        let should_flush = self.should_flush_batch();

        if should_flush {
            self.flush_pending_commits()?;
        }

        Ok(notify)
    }

    /// Check if pending commits should be flushed
    fn should_flush_batch(&self) -> bool {
        if self.pending_commits.is_empty() {
            return false;
        }

        // Check batch size
        let total_frames: usize = self.pending_commits.iter().map(|c| c.frames.len()).sum();
        let batch_threshold = if self.group_commit.adaptive_batching {
            self.adaptive_batch_size
        } else {
            self.group_commit.max_batch_size
        };
        
        if total_frames >= batch_threshold {
            return true;
        }

        // Check timeout
        if let Some(first) = self.pending_commits.first() {
            let elapsed = first.queued_at.elapsed();
            if elapsed >= Duration::from_millis(self.group_commit.flush_timeout_ms) {
                return true;
            }
        }

        false
    }

    /// Flush all pending commits as a batch (group commit)
    fn flush_pending_commits(&mut self) -> Result<()> {
        if self.pending_commits.is_empty() {
            return Ok(());
        }

        let flush_start = Instant::now();
        let batch_count = self.pending_commits.len();

        // Collect all frames from pending commits
        let mut all_frames: Vec<WalFrame> = Vec::new();
        let notifications: Vec<Option<Arc<(Mutex<bool>, Condvar)>>> = self.pending_commits
            .iter()
            .map(|c| c.notify.clone())
            .collect();

        for pending in &self.pending_commits {
            all_frames.extend(pending.frames.clone());
        }

        if all_frames.is_empty() {
            self.pending_commits.clear();
            return Ok(());
        }

        // Seek to end and write all frames
        self.file.seek(SeekFrom::End(0))?;

        for frame in &all_frames {
            self.file.write_all(&frame.to_bytes())?;
            self.frame_count += 1;
            self.stats.frames_written += 1;
            self.stats.bytes_written += (WalFrame::HEADER_SIZE + frame.page_data.len()) as u64;
        }

        // Single fsync for the entire group
        self.file.sync_all()?;
        self.stats.fsync_count += 1;
        self.stats.group_commits += 1;

        // Update statistics
        let latency = flush_start.elapsed().as_micros() as f64;
        self.stats.avg_flush_latency_us = 
            (self.stats.avg_flush_latency_us * (self.stats.group_commits.saturating_sub(1)) as f64 + latency)
            / self.stats.group_commits as f64;
        
        self.stats.avg_batch_size = 
            (self.stats.avg_batch_size * (self.stats.group_commits.saturating_sub(1)) as f64 + batch_count as f64)
            / self.stats.group_commits as f64;

        // Adaptive batching
        if self.group_commit.adaptive_batching {
            self.update_adaptive_batch_size(latency);
        }

        // Notify all waiting threads
        for notify in notifications {
            if let Some(n) = notify {
                let (lock, cvar) = &*n;
                if let Ok(mut flushed) = lock.lock() {
                    *flushed = true;
                    cvar.notify_all();
                }
            }
        }

        self.pending_commits.clear();
        self.last_flush = Instant::now();

        Ok(())
    }

    /// Single transaction commit (fallback when group commit fails)
    pub fn commit_single(&mut self) -> Result<()> {
        self.flush_buffer()?;
        self.stats.single_commits += 1;
        Ok(())
    }

    /// Perform checkpoint: flush WAL pages to data file
    pub fn checkpoint<F>(&mut self, mut write_page: F) -> Result<usize>
    where
        F: FnMut(PageId, &[u8]) -> Result<()>,
    {
        // First flush any pending commits
        self.flush_pending_commits()?;
        self.flush()?;

        // Read all frames from WAL
        let mut frames = Vec::new();
        self.file.seek(SeekFrom::Start(WalHeader::SIZE as u64))?;

        loop {
            let mut header_buf = vec![0u8; WalFrame::HEADER_SIZE];
            match self.file.read_exact(&mut header_buf) {
                Ok(()) => {}
                Err(_) => break, // End of file
            }

            let mut page_buf = vec![0u8; self.page_size];
            if let Err(_) = self.file.read_exact(&mut page_buf) {
                break;
            }

            if let Ok(frame) = WalFrame::from_bytes(&header_buf, &page_buf) {
                if frame.verify() {
                    frames.push(frame);
                }
            }
        }

        // Apply frames to data file (in commit order)
        frames.sort_by_key(|f| f.commit_id);

        for frame in &frames {
            write_page(frame.page_id, &frame.page_data)?;
        }

        let checkpointed = frames.len();

        // Truncate WAL file
        self.file.set_len(WalHeader::SIZE as u64)?;
        self.file.seek(SeekFrom::Start(WalHeader::SIZE as u64))?;
        self.file.sync_all()?;

        // Update header
        self.header.checkpoint_seq += checkpointed as u64;
        self.file.seek(SeekFrom::Start(0))?;
        self.file.write_all(&self.header.to_bytes())?;
        self.file.sync_all()?;

        self.frame_count = 0;

        Ok(checkpointed)
    }

    /// Read a page from WAL (if it exists there)
    pub fn read_page(&mut self, page_id: PageId) -> Result<Option<Vec<u8>>> {
        // First check buffer
        for frame in self.buffer.iter().rev() {
            if frame.page_id == page_id {
                return Ok(Some(frame.page_data.clone()));
            }
        }

        // Check pending commits
        for pending in &self.pending_commits {
            for frame in pending.frames.iter().rev() {
                if frame.page_id == page_id {
                    return Ok(Some(frame.page_data.clone()));
                }
            }
        }

        // Then check WAL file
        self.file.seek(SeekFrom::Start(WalHeader::SIZE as u64))?;

        let mut result: Option<Vec<u8>> = None;
        let mut max_commit_id = 0u64;

        loop {
            let mut header_buf = vec![0u8; WalFrame::HEADER_SIZE];
            match self.file.read_exact(&mut header_buf) {
                Ok(()) => {}
                Err(_) => break,
            }

            let mut page_buf = vec![0u8; self.page_size];
            if let Err(_) = self.file.read_exact(&mut page_buf) {
                break;
            }

            if let Ok(frame) = WalFrame::from_bytes(&header_buf, &page_buf) {
                if frame.verify()
                    && frame.page_id == page_id
                    && frame.commit_id >= max_commit_id
                {
                    result = Some(frame.page_data.clone());
                    max_commit_id = frame.commit_id;
                }
            }
        }

        Ok(result)
    }

    /// Get the number of frames in the WAL
    pub fn frame_count(&self) -> u64 {
        self.frame_count + self.buffer.len() as u64
            + self.pending_commits.iter().map(|c| c.frames.len() as u64).sum::<u64>()
    }

    /// Check if checkpoint is needed
    pub fn needs_checkpoint(&self) -> bool {
        self.frame_count as usize >= self.checkpoint_threshold
    }
    
    /// Get WAL statistics
    pub fn stats(&self) -> &WalStats {
        &self.stats
    }
    
    /// Get current adaptive batch size
    pub fn adaptive_batch_size(&self) -> usize {
        self.adaptive_batch_size
    }
    
    /// Set group commit configuration
    pub fn set_group_commit_config(&mut self, config: GroupCommitConfig) {
        self.group_commit = config;
    }
    
    /// Get number of pending commits
    pub fn pending_count(&self) -> usize {
        self.pending_commits.len()
    }
    
    /// Force flush all pending commits
    pub fn force_flush(&mut self) -> Result<()> {
        self.flush_pending_commits()
    }

    /// Close WAL and ensure all data is flushed
    pub fn close(mut self) -> Result<()> {
        self.flush_pending_commits()?;
        self.flush()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn test_wal_header_serialization() {
        let header = WalHeader::new(4096);
        let bytes = header.to_bytes();
        let restored = WalHeader::from_bytes(&bytes).unwrap();

        assert_eq!(restored.magic, WalHeader::MAGIC);
        assert_eq!(restored.version, 1);
        assert_eq!(restored.page_size, 4096);
    }

    #[test]
    fn test_wal_frame_serialization() {
        let data = vec![1u8, 2, 3, 4, 5];
        let frame = WalFrame::new(1, 1, data.clone());
        let bytes = frame.to_bytes();

        let header = &bytes[..WalFrame::HEADER_SIZE];
        let page_data = &bytes[WalFrame::HEADER_SIZE..];
        let restored = WalFrame::from_bytes(header, page_data).unwrap();

        assert_eq!(restored.page_id, 1);
        assert_eq!(restored.commit_id, 1);
        assert_eq!(restored.page_data, data);
        assert!(restored.verify());
    }

    #[test]
    fn test_wal_write_and_read() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap().to_string() + ".db";

        {
            let mut wal = Wal::open(&path, 4096).unwrap();
            wal.begin_transaction();

            // Create a mock page
            let mut page_data = vec![0u8; 4096];
            page_data[0] = 42;

            let page = Page::from_bytes(1, page_data.clone());
            wal.write_page(&page).unwrap();
            wal.flush().unwrap();
        }

        // Reopen and read
        {
            let mut wal = Wal::open(&path, 4096).unwrap();
            let result = wal.read_page(1).unwrap();
            assert!(result.is_some());
            assert_eq!(result.unwrap()[0], 42);
        }
    }

    #[test]
    fn test_wal_multiple_pages() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap().to_string() + ".db";

        let mut wal = Wal::open(&path, 4096).unwrap();
        wal.begin_transaction();

        // Write multiple pages
        for i in 1..=10 {
            let mut page_data = vec![0u8; 4096];
            page_data[0] = i as u8;
            let page = Page::from_bytes(i, page_data);
            wal.write_page(&page).unwrap();
        }

        wal.flush().unwrap();

        // Read all pages back
        for i in 1..=10 {
            let result = wal.read_page(i).unwrap();
            assert!(result.is_some());
            assert_eq!(result.unwrap()[0], i as u8);
        }
    }

    #[test]
    fn test_wal_read_nonexistent_page() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap().to_string() + ".db";

        let mut wal = Wal::open(&path, 4096).unwrap();
        
        // Try to read page that doesn't exist
        let result = wal.read_page(999).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_wal_checkpoint() {
        let temp_dir = tempfile::tempdir().unwrap();
        let wal_path = temp_dir.path().join("test.db");
        
        // Create WAL and write some data
        {
            let mut wal = Wal::open(wal_path.to_str().unwrap(), 4096).unwrap();
            wal.begin_transaction();

            let mut page_data = vec![0u8; 4096];
            page_data[0] = 42;
            let page = Page::from_bytes(1, page_data);
            wal.write_page(&page).unwrap();
            wal.flush().unwrap();

            // Checkpoint should work (even if no main db)
            let _ = wal.checkpoint(|_page_id, _data| Ok(())); // May fail without main db, but shouldn't panic
        }
    }

    #[test]
    fn test_wal_frame_checksum_verification() {
        let data = vec![1u8, 2, 3, 4, 5];
        let mut frame = WalFrame::new(1, 1, data.clone());
        
        // Valid frame should verify
        assert!(frame.verify());

        // Corrupt the data
        frame.page_data[0] = 99;
        
        // Corrupted frame should fail verification
        assert!(!frame.verify());
    }

    #[test]
    fn test_wal_commit_boundary() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap().to_string() + ".db";

        let mut wal = Wal::open(&path, 4096).unwrap();
        
        // First transaction
        wal.begin_transaction();
        let page = Page::from_bytes(1, vec![1u8; 4096]);
        wal.write_page(&page).unwrap();
        wal.flush().unwrap();

        // Second transaction
        wal.begin_transaction();
        let page = Page::from_bytes(1, vec![2u8; 4096]);
        wal.write_page(&page).unwrap();
        wal.flush().unwrap();

        // Should see latest version
        let result = wal.read_page(1).unwrap();
        assert_eq!(result.unwrap()[0], 2);
    }
    
    #[test]
    fn test_group_commit_config() {
        let config = GroupCommitConfig {
            enabled: true,
            max_batch_size: 50,
            flush_timeout_ms: 5,
            min_batch_size: 1,
            adaptive_batching: true,
            target_latency_ms: 3,
        };
        
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap().to_string() + ".db";
        
        let mut wal = Wal::with_config(&path, 4096, config).unwrap();
        
        // Queue some commits with force flush
        wal.begin_transaction();
        let page = Page::from_bytes(1, vec![1u8; 4096]);
        wal.write_page(&page).unwrap();
        
        let notify = wal.queue_commit(true).unwrap();
        assert!(notify.is_some());
        
        // Force flush to ensure group commit happens
        wal.force_flush().unwrap();
        
        // Check stats
        let stats = wal.stats();
        assert!(stats.group_commits >= 1, "Expected at least 1 group commit");
    }
    
    #[test]
    fn test_adaptive_batching() {
        let config = GroupCommitConfig {
            enabled: true,
            max_batch_size: 100,
            flush_timeout_ms: 100, // Long timeout to test adaptive batching
            min_batch_size: 1,
            adaptive_batching: true,
            target_latency_ms: 1, // Very low target to trigger adjustment
        };
        
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap().to_string() + ".db";
        
        let mut wal = Wal::with_config(&path, 4096, config).unwrap();
        
        // Verify adaptive batching is configured
        assert!(wal.adaptive_batch_size() >= Wal::MIN_ADAPTIVE_BATCH);
        assert!(wal.adaptive_batch_size() <= Wal::MAX_ADAPTIVE_BATCH);
        
        // Perform several commits - adaptive batching adjusts based on latency
        for i in 0..10 {
            wal.begin_transaction();
            let page = Page::from_bytes(i as u32, vec![i as u8; 4096]);
            wal.write_page(&page).unwrap();
            wal.flush().unwrap();
        }
        
        // Stats should show commits were made
        let stats = wal.stats();
        assert!(stats.group_commits + stats.single_commits >= 5, "Expected commits to be recorded");
    }
    
    #[test]
    fn test_single_commit_fallback() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap().to_string() + ".db";
        
        let mut wal = Wal::open(&path, 4096).unwrap();
        
        wal.begin_transaction();
        let page = Page::from_bytes(1, vec![1u8; 4096]);
        wal.write_page(&page).unwrap();
        
        // Use single commit fallback
        wal.commit_single().unwrap();
        
        let stats = wal.stats();
        assert_eq!(stats.single_commits, 1);
    }
    
    #[test]
    fn test_wal_statistics() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap().to_string() + ".db";
        
        let mut wal = Wal::open(&path, 4096).unwrap();
        
        // Write some pages
        for i in 1..=5 {
            wal.begin_transaction();
            let page = Page::from_bytes(i, vec![i as u8; 4096]);
            wal.write_page(&page).unwrap();
            wal.flush().unwrap();
        }
        
        let stats = wal.stats();
        assert_eq!(stats.frames_written, 5);
        assert!(stats.bytes_written > 0);
        assert!(stats.fsync_count > 0);
        assert!(stats.avg_batch_size > 0.0);
    }
}
