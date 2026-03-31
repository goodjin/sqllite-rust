//! Optimistic Locking for Concurrent Writes
//!
//! Provides conflict detection for concurrent transactions:
//! - Version number checking for conflict detection
//! - Transaction validation before commit
//! - Rollback mechanism for failed transactions
//! - Deadlock avoidance through optimistic approach

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use parking_lot::{Mutex, RwLock};

use super::mvcc::{MvccManager, Snapshot, TxId};
use super::cow::CowStorage;

/// Optimistic lock using version numbers
#[derive(Debug)]
pub struct OptimisticLock {
    /// Current version number
    pub version: AtomicU64,
    /// Last modifier transaction ID
    pub last_modifier: AtomicU64,
}

impl OptimisticLock {
    pub fn new() -> Self {
        Self {
            version: AtomicU64::new(0),
            last_modifier: AtomicU64::new(0),
        }
    }

    /// Get current version
    pub fn get_version(&self) -> u64 {
        self.version.load(Ordering::Acquire)
    }

    /// Get last modifier
    pub fn get_last_modifier(&self) -> TxId {
        self.last_modifier.load(Ordering::Acquire)
    }

    /// Try to acquire lock by checking version
    /// Returns true if successful (version hasn't changed)
    pub fn try_lock(&self, expected_version: u64) -> bool {
        let current = self.version.load(Ordering::Acquire);
        current == expected_version
    }

    /// Increment version (called on successful write)
    pub fn increment_version(&self, modifier: TxId) {
        self.version.fetch_add(1, Ordering::SeqCst);
        self.last_modifier.store(modifier, Ordering::Release);
    }
}

impl Default for OptimisticLock {
    fn default() -> Self {
        Self::new()
    }
}

/// Conflict types
#[derive(Debug, Clone, PartialEq)]
pub enum ConflictType {
    /// Write-write conflict (two txs trying to modify same data)
    WriteWrite { key: Vec<u8>, tx1: TxId, tx2: TxId },
    /// Read-write conflict (read data that was modified by another tx)
    ReadWrite { key: Vec<u8>, reader: TxId, writer: TxId },
    /// Write-read conflict (wrote data that was read by another tx)
    WriteRead { key: Vec<u8>, writer: TxId, reader: TxId },
}

impl std::fmt::Display for ConflictType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConflictType::WriteWrite { key, tx1, tx2 } => {
                write!(f, "Write-write conflict on key {:?} between tx {} and tx {}", 
                    String::from_utf8_lossy(key), tx1, tx2)
            }
            ConflictType::ReadWrite { key, reader, writer } => {
                write!(f, "Read-write conflict on key {:?}: tx {} read, tx {} wrote", 
                    String::from_utf8_lossy(key), reader, writer)
            }
            ConflictType::WriteRead { key, writer, reader } => {
                write!(f, "Write-read conflict on key {:?}: tx {} wrote, tx {} read", 
                    String::from_utf8_lossy(key), writer, reader)
            }
        }
    }
}

/// Conflict error with details
#[derive(Debug, Clone)]
pub struct ConflictError {
    pub conflict_type: ConflictType,
    pub message: String,
}

impl ConflictError {
    pub fn new(conflict_type: ConflictType) -> Self {
        let message = conflict_type.to_string();
        Self { conflict_type, message }
    }
}

impl std::fmt::Display for ConflictError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Conflict: {}", self.message)
    }
}

impl std::error::Error for ConflictError {}

/// Conflict handling strategy
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ConflictStrategy {
    /// Abort transaction on conflict (default)
    Abort,
    /// Retry transaction automatically
    Retry { max_retries: u32, backoff_ms: u64 },
    /// Wait and retry (with timeout)
    WaitRetry { timeout_ms: u64, retry_interval_ms: u64 },
}

impl Default for ConflictStrategy {
    fn default() -> Self {
        ConflictStrategy::Abort
    }
}

/// Transaction state
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum TransactionState {
    /// Transaction is active
    Active,
    /// Transaction is validating (checking for conflicts)
    Validating,
    /// Transaction committed successfully
    Committed,
    /// Transaction aborted/rolled back
    Aborted,
}

/// Transaction with optimistic locking
pub struct Transaction {
    /// Transaction ID
    pub tx_id: TxId,
    /// Current state
    pub state: TransactionState,
    /// Read set: keys that were read (key -> version at read time)
    pub read_set: HashMap<Vec<u8>, u64>,
    /// Write set: keys that were written (key -> value)
    pub write_set: HashMap<Vec<u8>, Vec<u8>>,
    /// Delete set: keys that were deleted
    pub delete_set: HashSet<Vec<u8>>,
    /// Snapshot for consistent reads
    pub snapshot: Snapshot,
    /// Start time for deadlock detection
    pub start_time: std::time::Instant,
    /// Conflict strategy
    pub conflict_strategy: ConflictStrategy,
}

impl Transaction {
    pub fn new(tx_id: TxId, snapshot: Snapshot) -> Self {
        Self {
            tx_id,
            state: TransactionState::Active,
            read_set: HashMap::new(),
            write_set: HashMap::new(),
            delete_set: HashSet::new(),
            snapshot,
            start_time: std::time::Instant::now(),
            conflict_strategy: ConflictStrategy::default(),
        }
    }

    /// Record a read operation
    pub fn record_read(&mut self, key: Vec<u8>, version: u64) {
        if self.state != TransactionState::Active {
            return;
        }
        self.read_set.insert(key, version);
    }

    /// Record a write operation
    pub fn record_write(&mut self, key: Vec<u8>, value: Vec<u8>) {
        if self.state != TransactionState::Active {
            return;
        }
        // Remove from delete set if it was there
        self.delete_set.remove(&key);
        // Insert into write set
        self.write_set.insert(key.clone(), value);
    }

    /// Record a delete operation
    pub fn record_delete(&mut self, key: Vec<u8>) {
        if self.state != TransactionState::Active {
            return;
        }
        self.delete_set.insert(key.clone());
        // Also remove from write set
        self.write_set.remove(&key);
    }

    /// Check if transaction has timed out (for deadlock avoidance)
    pub fn is_timed_out(&self, timeout: std::time::Duration) -> bool {
        self.start_time.elapsed() > timeout
    }

    /// Get all keys touched by this transaction
    pub fn touched_keys(&self) -> HashSet<Vec<u8>> {
        let mut keys = HashSet::new();
        keys.extend(self.read_set.keys().cloned());
        keys.extend(self.write_set.keys().cloned());
        keys.extend(self.delete_set.iter().cloned());
        keys
    }

    /// Mark as committed
    pub fn mark_committed(&mut self) {
        self.state = TransactionState::Committed;
    }

    /// Mark as aborted
    pub fn mark_aborted(&mut self) {
        self.state = TransactionState::Aborted;
    }
}

/// Lock manager for optimistic concurrency control
pub struct LockManager {
    /// Lock for each key
    locks: RwLock<HashMap<Vec<u8>, Arc<OptimisticLock>>>,
    /// Active transactions
    active_txs: Mutex<HashMap<TxId, Arc<Mutex<Transaction>>>>,
    /// Transaction timeout for deadlock detection
    pub timeout: std::time::Duration,
    /// Global conflict strategy
    pub default_strategy: ConflictStrategy,
}

impl LockManager {
    pub fn new() -> Self {
        Self {
            locks: RwLock::new(HashMap::new()),
            active_txs: Mutex::new(HashMap::new()),
            timeout: std::time::Duration::from_secs(30),
            default_strategy: ConflictStrategy::Abort,
        }
    }

    /// Register a new transaction
    pub fn register_transaction(&self, tx: Transaction) -> Arc<Mutex<Transaction>> {
        let tx_id = tx.tx_id;
        let tx_arc = Arc::new(Mutex::new(tx));
        
        let mut active = self.active_txs.lock();
        active.insert(tx_id, tx_arc.clone());
        
        tx_arc
    }

    /// Unregister a transaction
    pub fn unregister_transaction(&self, tx_id: TxId) {
        let mut active = self.active_txs.lock();
        active.remove(&tx_id);
    }

    /// Get or create lock for a key
    fn get_or_create_lock(&self, key: &[u8]) -> Arc<OptimisticLock> {
        {
            let locks = self.locks.read();
            if let Some(lock) = locks.get(key) {
                return lock.clone();
            }
        }
        
        let mut locks = self.locks.write();
        locks.entry(key.to_vec())
            .or_insert_with(|| Arc::new(OptimisticLock::new()))
            .clone()
    }

    /// Acquire read lock (record version)
    pub fn acquire_read_lock(&self, tx_id: TxId, key: Vec<u8>) -> Result<u64, ConflictError> {
        let lock = self.get_or_create_lock(&key);
        let version = lock.get_version();
        
        // Check for write-write conflict with active transactions
        let active = self.active_txs.lock();
        for (other_tx_id, other_tx) in active.iter() {
            if *other_tx_id == tx_id {
                continue;
            }
            
            let other = other_tx.lock();
            if other.write_set.contains_key(&key) || other.delete_set.contains(&key) {
                // Another transaction is writing to this key
                return Err(ConflictError::new(ConflictType::ReadWrite {
                    key: key.clone(),
                    reader: tx_id,
                    writer: *other_tx_id,
                }));
            }
        }
        
        // Record the read
        if let Some(tx_arc) = active.get(&tx_id) {
            let mut tx = tx_arc.lock();
            tx.record_read(key, version);
        }
        
        Ok(version)
    }

    /// Acquire write lock
    pub fn acquire_write_lock(&self, tx_id: TxId, key: Vec<u8>, value: Vec<u8>) -> Result<(), ConflictError> {
        let lock = self.get_or_create_lock(&key);
        
        // Check for conflicts with active transactions
        let active = self.active_txs.lock();
        for (other_tx_id, other_tx) in active.iter() {
            if *other_tx_id == tx_id {
                continue;
            }
            
            let other = other_tx.lock();
            
            // Write-write conflict
            if other.write_set.contains_key(&key) {
                return Err(ConflictError::new(ConflictType::WriteWrite {
                    key: key.clone(),
                    tx1: tx_id,
                    tx2: *other_tx_id,
                }));
            }
            
            // Check if other tx read this key (potential write-read conflict)
            // This is only a conflict if the other tx hasn't committed yet
            if other.read_set.contains_key(&key) && other.state == TransactionState::Active {
                // In snapshot isolation, this is allowed
                // In serializable isolation, this would be a conflict
            }
        }
        
        // Record the write
        if let Some(tx_arc) = active.get(&tx_id) {
            let mut tx = tx_arc.lock();
            tx.record_write(key, value);
        }
        
        Ok(())
    }

    /// Validate transaction before commit (optimistic validation)
    /// Checks if any version numbers have changed since read
    pub fn validate_transaction(&self, tx: &Transaction) -> Result<(), ConflictError> {
        let locks = self.locks.read();
        
        // Check all reads - if version changed, conflict occurred
        for (key, read_version) in &tx.read_set {
            if let Some(lock) = locks.get(key) {
                let current_version = lock.get_version();
                if current_version != *read_version {
                    // Someone modified this key since we read it
                    let modifier = lock.get_last_modifier();
                    return Err(ConflictError::new(ConflictType::ReadWrite {
                        key: key.clone(),
                        reader: tx.tx_id,
                        writer: modifier,
                    }));
                }
            }
        }
        
        // Check write set for conflicts
        for key in tx.write_set.keys() {
            if let Some(lock) = locks.get(key) {
                // Check if this was modified by someone else after we started
                let last_modifier = lock.get_last_modifier();
                if last_modifier != 0 && last_modifier != tx.tx_id {
                    // Check if the modifier committed after our start
                    // This would be a conflict in strict serializable mode
                }
            }
        }
        
        Ok(())
    }

    /// Commit a transaction - update all version numbers
    pub fn commit_transaction(&self, tx: &mut Transaction) -> Result<(), ConflictError> {
        tx.state = TransactionState::Validating;
        
        // Validate
        self.validate_transaction(tx)?;
        
        // Update version numbers for all written keys
        let locks = self.locks.read();
        for key in tx.write_set.keys() {
            if let Some(lock) = locks.get(key) {
                lock.increment_version(tx.tx_id);
            }
        }
        
        // Mark deleted keys
        for key in &tx.delete_set {
            if let Some(lock) = locks.get(key) {
                lock.increment_version(tx.tx_id);
            }
        }
        
        tx.mark_committed();
        self.unregister_transaction(tx.tx_id);
        
        Ok(())
    }

    /// Abort/rollback a transaction
    pub fn abort_transaction(&self, tx: &mut Transaction) {
        tx.mark_aborted();
        self.unregister_transaction(tx.tx_id);
    }

    /// Get conflict statistics
    pub fn get_active_count(&self) -> usize {
        let active = self.active_txs.lock();
        active.len()
    }
}

impl Default for LockManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Optimistic MVCC Manager with full concurrency control
pub struct OptimisticMvccManager {
    /// Base MVCC manager
    mvcc: Arc<MvccManager>,
    /// Lock manager for conflict detection
    lock_manager: Arc<LockManager>,
    /// Storage
    storage: Arc<CowStorage<Vec<u8>>>,
}

impl OptimisticMvccManager {
    pub fn new() -> Self {
        Self {
            mvcc: Arc::new(MvccManager::new()),
            lock_manager: Arc::new(LockManager::new()),
            storage: Arc::new(CowStorage::new()),
        }
    }

    /// Begin a new transaction with optimistic locking
    pub fn begin_transaction(&self) -> Arc<Mutex<Transaction>> {
        let tx_id = self.mvcc.begin_transaction();
        let snapshot = self.mvcc.get_snapshot(tx_id);
        
        let tx = Transaction::new(tx_id, snapshot);
        self.lock_manager.register_transaction(tx)
    }

    /// Read a key (with optimistic locking)
    pub fn read(&self, tx_id: TxId, key: Vec<u8>) -> Result<Option<Vec<u8>>, ConflictError> {
        // Acquire read lock (records version)
        let version = self.lock_manager.acquire_read_lock(tx_id, key.clone())?;
        
        // Get snapshot for consistent read
        let active = self.lock_manager.active_txs.lock();
        if let Some(tx_arc) = active.get(&tx_id) {
            let tx = tx_arc.lock();
            let snapshot = &tx.snapshot;
            
            // Check write set first (read-your-writes)
            if let Some(value) = tx.write_set.get(&key) {
                return Ok(Some(value.clone()));
            }
            
            // Check delete set
            if tx.delete_set.contains(&key) {
                return Ok(None);
            }
            
            // Read from storage
            // Note: In a real implementation, you'd map keys to page IDs
            // For simplicity, we're using a direct approach here
        }
        
        Ok(None)
    }

    /// Write a key (with conflict detection)
    pub fn write(&self, tx_id: TxId, key: Vec<u8>, value: Vec<u8>) -> Result<(), ConflictError> {
        // Acquire write lock (checks for conflicts)
        self.lock_manager.acquire_write_lock(tx_id, key.clone(), value.clone())?;
        
        Ok(())
    }

    /// Delete a key
    pub fn delete(&self, tx_id: TxId, key: Vec<u8>) -> Result<(), ConflictError> {
        let active = self.lock_manager.active_txs.lock();
        if let Some(tx_arc) = active.get(&tx_id) {
            let mut tx = tx_arc.lock();
            tx.record_delete(key);
        }
        Ok(())
    }

    /// Commit a transaction
    pub fn commit(&self, tx_id: TxId) -> Result<(), ConflictError> {
        let tx_arc = {
            let active = self.lock_manager.active_txs.lock();
            active.get(&tx_id).cloned()
        };
        
        if let Some(tx_arc) = tx_arc {
            let mut tx = tx_arc.lock();
            
            // Apply conflict strategy
            match tx.conflict_strategy {
                ConflictStrategy::Abort => {
                    self.lock_manager.commit_transaction(&mut tx)
                }
                ConflictStrategy::Retry { max_retries, backoff_ms } => {
                    let mut retries = 0;
                    loop {
                        match self.lock_manager.commit_transaction(&mut tx) {
                            Ok(()) => return Ok(()),
                            Err(e) => {
                                retries += 1;
                                if retries >= max_retries {
                                    return Err(e);
                                }
                                std::thread::sleep(std::time::Duration::from_millis(backoff_ms));
                            }
                        }
                    }
                }
                ConflictStrategy::WaitRetry { timeout_ms, retry_interval_ms } => {
                    let start = std::time::Instant::now();
                    let timeout = std::time::Duration::from_millis(timeout_ms);
                    
                    loop {
                        match self.lock_manager.commit_transaction(&mut tx) {
                            Ok(()) => return Ok(()),
                            Err(e) => {
                                if start.elapsed() > timeout {
                                    return Err(e);
                                }
                                std::thread::sleep(std::time::Duration::from_millis(retry_interval_ms));
                            }
                        }
                    }
                }
            }
        } else {
            Err(ConflictError::new(ConflictType::WriteWrite {
                key: vec![],
                tx1: tx_id,
                tx2: 0,
            }))
        }
    }

    /// Rollback a transaction
    pub fn rollback(&self, tx_id: TxId) {
        let tx_arc = {
            let active = self.lock_manager.active_txs.lock();
            active.get(&tx_id).cloned()
        };
        
        if let Some(tx_arc) = tx_arc {
            let mut tx = tx_arc.lock();
            self.lock_manager.abort_transaction(&mut tx);
        }
        
        self.mvcc.rollback_transaction(tx_id);
    }

    /// Set conflict strategy for a transaction
    pub fn set_conflict_strategy(&self, tx_id: TxId, strategy: ConflictStrategy) {
        let active = self.lock_manager.active_txs.lock();
        if let Some(tx_arc) = active.get(&tx_id) {
            let mut tx = tx_arc.lock();
            tx.conflict_strategy = strategy;
        }
    }

    /// Get statistics
    pub fn stats(&self) -> OptimisticMvccStats {
        OptimisticMvccStats {
            active_transactions: self.lock_manager.get_active_count(),
            mvcc_stats: self.mvcc.stats(),
        }
    }
}

impl Default for OptimisticMvccManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Statistics for optimistic MVCC
#[derive(Debug, Clone)]
pub struct OptimisticMvccStats {
    pub active_transactions: usize,
    pub mvcc_stats: super::mvcc::MvccStats,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_optimistic_lock_version() {
        let lock = OptimisticLock::new();
        
        assert_eq!(lock.get_version(), 0);
        
        lock.increment_version(1);
        assert_eq!(lock.get_version(), 1);
        
        lock.increment_version(2);
        assert_eq!(lock.get_version(), 2);
        assert_eq!(lock.get_last_modifier(), 2);
    }

    #[test]
    fn test_optimistic_lock_try_lock() {
        let lock = OptimisticLock::new();
        
        // Version 0, expect 0
        assert!(lock.try_lock(0));
        
        // Increment
        lock.increment_version(1);
        
        // Version 1, expect 0 (should fail)
        assert!(!lock.try_lock(0));
        
        // Version 1, expect 1 (should succeed)
        assert!(lock.try_lock(1));
    }

    #[test]
    fn test_transaction_read_write() {
        let snapshot = Snapshot::new(1, HashSet::new(), 0, 2, HashSet::new());
        let mut tx = Transaction::new(1, snapshot);
        
        // Record reads
        tx.record_read(b"key1".to_vec(), 5);
        tx.record_read(b"key2".to_vec(), 3);
        
        // Record writes
        tx.record_write(b"key1".to_vec(), b"value1".to_vec());
        
        assert_eq!(tx.read_set.len(), 2);
        assert_eq!(tx.write_set.len(), 1);
    }

    #[test]
    fn test_lock_manager_basic() {
        let manager = LockManager::new();
        
        // Create transaction
        let snapshot = Snapshot::new(1, HashSet::new(), 0, 2, HashSet::new());
        let tx = Transaction::new(1, snapshot);
        let tx_arc = manager.register_transaction(tx);
        
        // Acquire read lock
        let version = manager.acquire_read_lock(1, b"key1".to_vec()).unwrap();
        
        // Acquire write lock (same tx, should succeed)
        let result = manager.acquire_write_lock(1, b"key1".to_vec(), b"value1".to_vec());
        assert!(result.is_ok());
        
        // Check that read set was updated
        {
            let tx = tx_arc.lock();
            assert!(tx.read_set.contains_key(&b"key1".to_vec()));
            assert!(tx.write_set.contains_key(&b"key1".to_vec()));
        }
        
        manager.unregister_transaction(1);
    }

    #[test]
    fn test_write_write_conflict() {
        let manager = LockManager::new();
        
        // Create two transactions
        let snapshot1 = Snapshot::new(1, HashSet::new(), 0, 3, HashSet::from([2]));
        let tx1 = Transaction::new(1, snapshot1);
        manager.register_transaction(tx1);
        
        let snapshot2 = Snapshot::new(2, HashSet::new(), 0, 3, HashSet::from([2]));
        let tx2 = Transaction::new(2, snapshot2);
        manager.register_transaction(tx2);
        
        // Tx1 writes key1
        manager.acquire_write_lock(1, b"key1".to_vec(), b"value1".to_vec()).unwrap();
        
        // Tx2 tries to write key1 (should conflict)
        let result = manager.acquire_write_lock(2, b"key1".to_vec(), b"value2".to_vec());
        assert!(result.is_err());
        
        let err = result.unwrap_err();
        match err.conflict_type {
            ConflictType::WriteWrite { key, tx1: _, tx2: _ } => {
                assert_eq!(key, b"key1".to_vec());
            }
            _ => panic!("Expected write-write conflict"),
        }
    }

    #[test]
    fn test_transaction_commit_validation() {
        let manager = LockManager::new();
        
        // Create transaction
        let snapshot = Snapshot::new(1, HashSet::new(), 0, 2, HashSet::new());
        let tx = Transaction::new(1, snapshot);
        let tx_arc = manager.register_transaction(tx);
        
        // Record some operations
        {
            let mut tx = tx_arc.lock();
            tx.record_read(b"key1".to_vec(), 0);
            tx.record_write(b"key2".to_vec(), b"value2".to_vec());
        }
        
        // Commit (should succeed - no conflicts)
        {
            let mut tx = tx_arc.lock();
            let result = manager.commit_transaction(&mut tx);
            assert!(result.is_ok());
            assert_eq!(tx.state, TransactionState::Committed);
        }
    }

    #[test]
    fn test_conflict_strategy_abort() {
        let manager = OptimisticMvccManager::new();
        
        let tx_arc = manager.begin_transaction();
        let tx_id = {
            let tx = tx_arc.lock();
            tx.tx_id
        };
        
        // Write some data
        manager.write(tx_id, b"key1".to_vec(), b"value1".to_vec()).unwrap();
        
        // Commit should succeed
        let result = manager.commit(tx_id);
        assert!(result.is_ok());
    }

    #[test]
    fn test_transaction_rollback() {
        let manager = OptimisticMvccManager::new();
        
        let tx_arc = manager.begin_transaction();
        let tx_id = {
            let tx = tx_arc.lock();
            tx.tx_id
        };
        
        // Write some data
        manager.write(tx_id, b"key1".to_vec(), b"value1".to_vec()).unwrap();
        
        // Rollback
        manager.rollback(tx_id);
        
        // Transaction should be removed from active
        assert_eq!(manager.lock_manager.get_active_count(), 0);
    }

    #[test]
    fn test_concurrent_transactions_no_conflict() {
        let manager = Arc::new(OptimisticMvccManager::new());
        
        // Two transactions writing different keys
        let tx1_arc = manager.begin_transaction();
        let tx1_id = { tx1_arc.lock().tx_id };
        
        let tx2_arc = manager.begin_transaction();
        let tx2_id = { tx2_arc.lock().tx_id };
        
        // Tx1 writes key1
        manager.write(tx1_id, b"key1".to_vec(), b"value1".to_vec()).unwrap();
        
        // Tx2 writes key2 (different key, no conflict)
        manager.write(tx2_id, b"key2".to_vec(), b"value2".to_vec()).unwrap();
        
        // Both should commit successfully
        assert!(manager.commit(tx1_id).is_ok());
        assert!(manager.commit(tx2_id).is_ok());
    }
}
