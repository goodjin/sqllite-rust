//! Async WAL Writer - Background Thread for WAL I/O
//!
//! This module implements asynchronous WAL writing to reduce transaction commit latency:
//! - Background writer thread with batch processing
//! - Group commit: multiple transactions share a single fsync
//! - Configurable sync/async mode
//! - Graceful shutdown with pending flush

use crate::pager::PageId;
use crate::pager::page::Page;
use crate::storage::{Result, StorageError};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{mpsc, Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

/// WAL entry types sent to background writer
#[derive(Debug, Clone)]
pub enum WalEntry {
    /// Write a page to WAL
    Write {
        page_id: PageId,
        commit_id: u64,
        page_data: Vec<u8>,
        /// Notify when this entry is persisted
        notify: Option<Arc<(Mutex<bool>, Condvar)>>,
    },
    /// Flush all pending entries (force fsync)
    Flush {
        notify: Arc<(Mutex<bool>, Condvar)>,
    },
    /// Shutdown the writer thread
    Shutdown,
}

/// Configuration for async WAL
#[derive(Debug, Clone, Copy)]
pub struct AsyncWalConfig {
    /// Enable async mode (if false, behaves like sync WAL)
    pub async_mode: bool,
    /// Batch size before auto-flush
    pub batch_size: usize,
    /// Maximum time to wait before flushing (milliseconds)
    pub flush_timeout_ms: u64,
    /// Buffer capacity for channel
    pub channel_capacity: usize,
}

impl Default for AsyncWalConfig {
    fn default() -> Self {
        Self {
            async_mode: true,
            batch_size: 100,
            flush_timeout_ms: 10,
            channel_capacity: 1000,
        }
    }
}

/// Async WAL Writer - manages background thread for WAL I/O
pub struct AsyncWalWriter {
    /// Channel sender for WAL entries
    sender: mpsc::Sender<WalEntry>,
    /// Handle to the background thread
    handle: Option<JoinHandle<Result<()>>>,
    /// Current commit ID counter
    commit_id: Arc<Mutex<u64>>,
    /// Configuration
    config: AsyncWalConfig,
    /// Page size for WAL frames
    page_size: usize,
    /// WAL file path
    wal_path: PathBuf,
}

/// Internal state for the background writer thread
struct WriterState {
    /// WAL file handle
    file: File,
    /// Current commit ID being written
    current_commit_id: u64,
    /// Batch buffer for pending writes
    batch: Vec<WalEntry>,
    /// Total frames written
    frame_count: u64,
    /// Page size
    page_size: usize,
    /// Last flush time
    last_flush: Instant,
}

impl AsyncWalWriter {
    /// Open or create an async WAL file
    pub fn open<P: AsRef<Path>>(path: P, page_size: usize, config: AsyncWalConfig) -> Result<Self> {
        let path = path.as_ref();
        let wal_path = path.with_extension("wal");

        // Create channel for communication with background thread
        let (sender, receiver) = mpsc::channel();

        let commit_id = Arc::new(Mutex::new(0u64));
        let commit_id_clone = Arc::clone(&commit_id);
        let wal_path_clone = wal_path.clone();

        // Spawn background writer thread
        let handle = if config.async_mode {
            Some(thread::spawn(move || {
                Self::writer_thread(
                    receiver,
                    wal_path_clone,
                    page_size,
                    config,
                    commit_id_clone,
                )
            }))
        } else {
            None
        };

        Ok(Self {
            sender,
            handle,
            commit_id,
            config,
            page_size,
            wal_path,
        })
    }

    /// Check if running in async mode
    pub fn is_async(&self) -> bool {
        self.config.async_mode && self.handle.is_some()
    }

    /// Begin a new transaction - returns new commit ID
    pub fn begin_transaction(&self) -> u64 {
        let mut id = self.commit_id.lock().unwrap();
        *id += 1;
        *id
    }

    /// Write a page to WAL (async or sync depending on config)
    pub fn write_page(&self, page: &Page, commit_id: u64) -> Result<()> {
        let entry = WalEntry::Write {
            page_id: page.id(),
            commit_id,
            page_data: page.as_slice().to_vec(),
            notify: None,
        };

        if self.is_async() {
            // Async mode: send to background thread
            self.sender
                .send(entry)
                .map_err(|_| StorageError::Other("WAL channel closed".to_string()))?;
            Ok(())
        } else {
            // Sync mode: write directly
            self.write_sync(&entry)
        }
    }

    /// Write a page and wait for it to be persisted (for commit)
    pub fn write_page_sync(&self, page: &Page, commit_id: u64) -> Result<()> {
        if self.is_async() {
            let notify = Arc::new((Mutex::new(false), Condvar::new()));
            let entry = WalEntry::Write {
                page_id: page.id(),
                commit_id,
                page_data: page.as_slice().to_vec(),
                notify: Some(Arc::clone(&notify)),
            };

            self.sender
                .send(entry)
                .map_err(|_| StorageError::Other("WAL channel closed".to_string()))?;

            // Wait for notification
            let (lock, cvar) = &*notify;
            let mut flushed = lock.lock().unwrap();
            while !*flushed {
                flushed = cvar.wait(flushed).unwrap();
            }
            Ok(())
        } else {
            // Sync mode: write directly and fsync
            self.write_page(page, commit_id)?;
            self.flush_sync()
        }
    }

    /// Flush all pending writes (async or sync)
    pub fn flush(&self) -> Result<()> {
        if self.is_async() {
            let notify = Arc::new((Mutex::new(false), Condvar::new()));
            let entry = WalEntry::Flush {
                notify: Arc::clone(&notify),
            };

            self.sender
                .send(entry)
                .map_err(|_| StorageError::Other("WAL channel closed".to_string()))?;

            // Wait for flush to complete
            let (lock, cvar) = &*notify;
            let mut flushed = lock.lock().unwrap();
            while !*flushed {
                flushed = cvar.wait(flushed).unwrap();
            }
            Ok(())
        } else {
            self.flush_sync()
        }
    }

    /// Sync write (for non-async mode)
    fn write_sync(&self, entry: &WalEntry) -> Result<()> {
        match entry {
            WalEntry::Write {
                page_id,
                commit_id,
                page_data,
                ..
            } => {
                let frame = WalFrame::new(*page_id, *commit_id, page_data.clone());
                let mut file = OpenOptions::new()
                    .write(true)
                    .create(true)
                    .open(&self.wal_path)?;
                file.seek(SeekFrom::End(0))?;
                file.write_all(&frame.to_bytes())?;
            }
            _ => {}
        }
        Ok(())
    }

    /// Sync flush (for non-async mode)
    fn flush_sync(&self) -> Result<()> {
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(&self.wal_path)?;
        file.sync_all()?;
        Ok(())
    }

    /// Close the async WAL and signal pending writes
    /// Note: This doesn't wait for the background thread to finish to avoid blocking
    pub fn close(mut self) -> Result<()> {
        // Send shutdown signal
        let _ = self.sender.send(WalEntry::Shutdown);

        // Drop the sender to signal the receiver that no more messages will come
        drop(self.sender);

        // Don't wait for the thread to join - let it exit asynchronously
        // This prevents blocking during shutdown
        if let Some(handle) = self.handle.take() {
            // Spawn a detached thread to wait for the worker
            std::thread::spawn(move || {
                let _ = handle.join();
            });
        }

        Ok(())
    }

    /// Get the current commit ID
    pub fn current_commit_id(&self) -> u64 {
        *self.commit_id.lock().unwrap()
    }

    /// Background writer thread
    fn writer_thread(
        receiver: mpsc::Receiver<WalEntry>,
        wal_path: PathBuf,
        page_size: usize,
        config: AsyncWalConfig,
        _commit_id: Arc<Mutex<u64>>,
    ) -> Result<()> {
        // Open or create WAL file
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&wal_path)?;

        // Write header if new file
        let metadata = file.metadata()?;
        if metadata.len() == 0 {
            let header = WalHeader::new(page_size as u32);
            file.write_all(&header.to_bytes())?;
            file.sync_all()?;
        }

        let mut state = WriterState {
            file,
            current_commit_id: 0,
            batch: Vec::with_capacity(config.batch_size),
            frame_count: 0,
            page_size,
            last_flush: Instant::now(),
        };

        let flush_timeout = Duration::from_millis(config.flush_timeout_ms.max(1));

        loop {
            // Try to receive with timeout for batching
            match receiver.recv_timeout(flush_timeout) {
                Ok(entry) => {
                    match entry {
                        WalEntry::Shutdown => {
                            // Flush remaining batch and exit
                            let _ = state.flush_batch();
                            break;
                        }
                        WalEntry::Flush { notify } => {
                            let _ = state.flush_batch();
                            let (lock, cvar) = &*notify;
                            if let Ok(mut guard) = lock.lock() {
                                *guard = true;
                                cvar.notify_all();
                            }
                        }
                        entry => {
                            state.batch.push(entry);
                            // Check if batch is full
                            if state.batch.len() >= config.batch_size {
                                let _ = state.flush_batch();
                            }
                        }
                    }
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    // Timeout - flush pending batch
                    if !state.batch.is_empty() {
                        let _ = state.flush_batch();
                    }
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    // Channel closed - flush and exit
                    let _ = state.flush_batch();
                    break;
                }
            }
        }

        Ok(())
    }
}

impl WriterState {
    /// Flush the current batch to disk
    fn flush_batch(&mut self) -> Result<()> {
        if self.batch.is_empty() {
            return Ok(());
        }

        // Seek to end of file
        self.file.seek(SeekFrom::End(0))?;

        // Collect notifications to send after write
        let mut notifications: Vec<Arc<(Mutex<bool>, Condvar)>> = Vec::new();

        // Write all entries in batch
        for entry in &self.batch {
            match entry {
                WalEntry::Write {
                    page_id,
                    commit_id,
                    page_data,
                    notify,
                } => {
                    let frame = WalFrame::new(*page_id, *commit_id, page_data.clone());
                    self.file.write_all(&frame.to_bytes())?;
                    self.frame_count += 1;
                    self.current_commit_id = self.current_commit_id.max(*commit_id);

                    if let Some(n) = notify {
                        notifications.push(Arc::clone(n));
                    }
                }
                _ => {}
            }
        }

        // Single fsync for entire batch (group commit optimization)
        self.file.sync_all()?;

        // Notify all waiting threads
        for notify in notifications {
            let (lock, cvar) = &*notify;
            *lock.lock().unwrap() = true;
            cvar.notify_all();
        }

        // Clear batch
        self.batch.clear();
        self.last_flush = Instant::now();

        Ok(())
    }
}

/// WAL file header (same as sync WAL)
#[derive(Debug, Clone)]
pub struct WalHeader {
    pub magic: u32,
    pub version: u32,
    pub page_size: u32,
    pub checkpoint_seq: u64,
    pub salt1: u32,
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

/// WAL frame (same as sync WAL)
#[derive(Debug, Clone)]
pub struct WalFrame {
    pub page_id: PageId,
    pub commit_id: u64,
    pub page_data: Vec<u8>,
    pub checksum: u32,
}

impl WalFrame {
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

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(Self::HEADER_SIZE + self.page_data.len());
        bytes.extend_from_slice(&self.page_id.to_le_bytes());
        bytes.extend_from_slice(&self.commit_id.to_le_bytes());
        bytes.extend_from_slice(&self.checksum.to_le_bytes());
        bytes.extend_from_slice(&self.page_data);
        bytes
    }
}

/// Thread-safe async WAL wrapper for shared access
pub struct SharedAsyncWal {
    inner: Arc<Mutex<AsyncWalWriter>>,
}

impl SharedAsyncWal {
    pub fn open<P: AsRef<Path>>(path: P, page_size: usize, config: AsyncWalConfig) -> Result<Self> {
        let writer = AsyncWalWriter::open(path, page_size, config)?;
        Ok(Self {
            inner: Arc::new(Mutex::new(writer)),
        })
    }

    pub fn write_page(&self, page: &Page, commit_id: u64) -> Result<()> {
        self.inner.lock().unwrap().write_page(page, commit_id)
    }

    pub fn write_page_sync(&self, page: &Page, commit_id: u64) -> Result<()> {
        self.inner.lock().unwrap().write_page_sync(page, commit_id)
    }

    pub fn flush(&self) -> Result<()> {
        self.inner.lock().unwrap().flush()
    }

    pub fn begin_transaction(&self) -> u64 {
        self.inner.lock().unwrap().begin_transaction()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn test_async_wal_basic_write() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap().to_string() + ".db";

        let config = AsyncWalConfig {
            async_mode: true,
            batch_size: 10,
            flush_timeout_ms: 100,
            ..Default::default()
        };

        let wal = AsyncWalWriter::open(&path, 4096, config).unwrap();

        // Write some pages
        let commit_id = wal.begin_transaction();
        for i in 1..=5 {
            let page = Page::from_bytes(i, vec![i as u8; 4096]);
            wal.write_page(&page, commit_id).unwrap();
        }

        // Flush and close
        wal.flush().unwrap();
        wal.close().unwrap();

        // Verify WAL file exists and has content
        let wal_path = std::path::Path::new(&path).with_extension("wal");
        assert!(wal_path.exists());
        let metadata = std::fs::metadata(&wal_path).unwrap();
        assert!(metadata.len() > 0);
    }

    #[test]
    fn test_async_wal_sync_mode() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap().to_string() + ".db";

        let config = AsyncWalConfig {
            async_mode: false, // Sync mode
            ..Default::default()
        };

        let wal = AsyncWalWriter::open(&path, 4096, config).unwrap();
        assert!(!wal.is_async());

        let commit_id = wal.begin_transaction();
        let page = Page::from_bytes(1, vec![1u8; 4096]);
        wal.write_page(&page, commit_id).unwrap();
        wal.flush().unwrap();

        wal.close().unwrap();
    }

    #[test]
    fn test_async_wal_sync_write() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap().to_string() + ".db";

        let config = AsyncWalConfig {
            async_mode: true,
            batch_size: 100, // Large batch to test sync write
            flush_timeout_ms: 10000, // Long timeout
            ..Default::default()
        };

        let wal = AsyncWalWriter::open(&path, 4096, config).unwrap();

        // Use sync write - should return only after persist
        let commit_id = wal.begin_transaction();
        let page = Page::from_bytes(1, vec![42u8; 4096]);
        wal.write_page_sync(&page, commit_id).unwrap();

        // At this point, data should be persisted
        wal.close().unwrap();
    }
}
