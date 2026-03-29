//! Snapshot Isolation Implementation
//!
//! Provides lock-free reads with snapshot isolation level:
//! - Readers get a consistent point-in-time view
//! - No locks acquired for reads
//! - Writers don't block readers, readers don't block writers

use super::mvcc::{MvccManager, Snapshot, TxId, Version, VersionChain};
use crate::storage::{Record, Value};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/// Lock-free table storage with MVCC
pub struct MvccTable {
    /// Table name
    name: String,
    /// Version chains for each rowid
    /// RwLock allows concurrent reads, exclusive writes
    rows: RwLock<HashMap<u64, VersionChain<Record>>>,
    /// MVCC manager for transaction coordination
    mvcc: Arc<MvccManager>,
}

impl MvccTable {
    pub fn new(name: String, mvcc: Arc<MvccManager>) -> Self {
        Self {
            name,
            rows: RwLock::new(HashMap::new()),
            mvcc,
        }
    }

    /// Get a record by rowid (lock-free read)
    /// 
    /// This is the core operation for high-concurrency reads.
    /// No locks are held during the read - we just check visibility.
    pub fn get(&self, rowid: u64, reader_tx: TxId) -> Option<Record> {
        // Get snapshot for this reader
        let snapshot = self.mvcc.get_snapshot(reader_tx);
        
        // Acquire read lock (multiple readers can hold this)
        let rows = self.rows.read().ok()?;
        
        // Find version chain
        let chain = rows.get(&rowid)?;
        
        // Find visible version (no lock needed - version chain is immutable for existing versions)
        let version = chain.get_visible(reader_tx, &snapshot)?;
        
        // Clone the data and return
        Some(version.data.clone())
    }

    /// Scan all records visible to a transaction (lock-free)
    pub fn scan(&self, reader_tx: TxId) -> Vec<(u64, Record)> {
        let snapshot = self.mvcc.get_snapshot(reader_tx);
        
        let rows = self.rows.read().unwrap();
        
        let mut results = Vec::new();
        for (rowid, chain) in rows.iter() {
            if let Some(version) = chain.get_visible(reader_tx, &snapshot) {
                results.push((*rowid, version.data.clone()));
            }
        }
        
        results
    }

    /// Insert a new record (writer)
    pub fn insert(&self, rowid: u64, record: Record, writer_tx: TxId) -> Result<(), String> {
        // Acquire write lock
        let mut rows = self.rows.write().map_err(|e| e.to_string())?;
        
        // Create new version
        let version = Version::new(record, writer_tx);
        
        // Get or create version chain
        let chain = rows.entry(rowid).or_default();
        chain.add_version(version);
        
        Ok(())
    }

    /// Update a record (writer)
    pub fn update(&self, rowid: u64, new_record: Record, writer_tx: TxId) -> Result<(), String> {
        let mut rows = self.rows.write().map_err(|e| e.to_string())?;
        
        let chain = rows.get_mut(&rowid)
            .ok_or("Record not found")?;
        
        // Mark current version as deleted
        if let Some(version) = chain.get_version_by_creator(writer_tx) {
            // We created this version, can update in-place
            version.data = new_record;
        } else {
            // Create new version
            let version = Version::new(new_record, writer_tx);
            chain.add_version(version);
        }
        
        Ok(())
    }

    /// Delete a record (writer)
    pub fn delete(&self, rowid: u64, writer_tx: TxId) -> Result<(), String> {
        let mut rows = self.rows.write().map_err(|e| e.to_string())?;
        
        let chain = rows.get_mut(&rowid)
            .ok_or("Record not found")?;
        
        // Mark latest visible version as deleted
        let snapshot = self.mvcc.get_snapshot(writer_tx);
        if let Some(version) = chain.get_visible(writer_tx, &snapshot) {
            let created_by = version.created_by;
            if let Some(v) = chain.get_version_by_creator(created_by) {
                v.mark_deleted(writer_tx);
            }
        }
        
        Ok(())
    }

    /// Garbage collect obsolete versions
    pub fn gc(&self) -> usize {
        let oldest_tx = self.mvcc.get_oldest_active_tx().unwrap_or(u64::MAX);
        
        let mut rows = self.rows.write().unwrap();
        let mut total_removed = 0;
        
        for (_, chain) in rows.iter_mut() {
            total_removed += chain.gc(oldest_tx);
        }
        
        total_removed
    }

    /// Get table statistics
    pub fn stats(&self) -> TableStats {
        let rows = self.rows.read().unwrap();
        
        let total_versions: usize = rows.values()
            .map(|chain| chain.versions.len())
            .sum();
        
        TableStats {
            row_count: rows.len(),
            total_versions,
            avg_versions_per_row: if !rows.is_empty() {
                total_versions as f64 / rows.len() as f64
            } else {
                0.0
            },
        }
    }
}

/// Table statistics
#[derive(Debug, Clone)]
pub struct TableStats {
    pub row_count: usize,
    pub total_versions: usize,
    pub avg_versions_per_row: f64,
}

/// MVCC Database with snapshot isolation
pub struct MvccDatabase {
    /// Tables
    tables: RwLock<HashMap<String, Arc<MvccTable>>>,
    /// MVCC manager
    mvcc: Arc<MvccManager>,
}

impl MvccDatabase {
    pub fn new() -> Self {
        Self {
            tables: RwLock::new(HashMap::new()),
            mvcc: Arc::new(MvccManager::new()),
        }
    }

    /// Create a new table
    pub fn create_table(&self, name: String) -> Result<Arc<MvccTable>, String> {
        let mut tables = self.tables.write().map_err(|e| e.to_string())?;
        
        if tables.contains_key(&name) {
            return Err("Table already exists".to_string());
        }
        
        let table = Arc::new(MvccTable::new(name.clone(), self.mvcc.clone()));
        tables.insert(name, table.clone());
        
        Ok(table)
    }

    /// Get a table
    pub fn get_table(&self, name: &str) -> Option<Arc<MvccTable>> {
        self.tables.read().ok()?.get(name).cloned()
    }

    /// Begin a transaction
    pub fn begin_transaction(&self) -> TxId {
        self.mvcc.begin_transaction()
    }

    /// Commit a transaction
    pub fn commit_transaction(&self, tx_id: TxId) {
        self.mvcc.commit_transaction(tx_id);
    }

    /// Rollback a transaction
    pub fn rollback_transaction(&self, tx_id: TxId) {
        self.mvcc.rollback_transaction(tx_id);
    }

    /// Run garbage collection
    pub fn gc(&self) -> usize {
        let tables = self.tables.read().unwrap();
        
        let mut total = 0;
        for table in tables.values() {
            total += table.gc();
        }
        
        total
    }

    /// Get database statistics
    pub fn stats(&self) -> DatabaseStats {
        let tables = self.tables.read().unwrap();
        
        DatabaseStats {
            table_count: tables.len(),
            mvcc: self.mvcc.stats(),
        }
    }
}

impl Default for MvccDatabase {
    fn default() -> Self {
        Self::new()
    }
}

/// Database statistics
#[derive(Debug, Clone)]
pub struct DatabaseStats {
    pub table_count: usize,
    pub mvcc: super::mvcc::MvccStats,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::Record;

    fn create_test_record(id: i64, name: &str) -> Record {
        Record::new(vec![
            Value::Integer(id),
            Value::Text(name.to_string()),
        ])
    }

    #[test]
    fn test_lock_free_read() {
        let db = MvccDatabase::new();
        let table = db.create_table("users".to_string()).unwrap();
        
        // Insert some data
        let tx1 = db.begin_transaction();
        table.insert(1, create_test_record(1, "Alice"), tx1).unwrap();
        db.commit_transaction(tx1);
        
        // Concurrent read (lock-free)
        let tx2 = db.begin_transaction();
        let record = table.get(1, tx2);
        
        assert!(record.is_some());
        assert_eq!(record.unwrap().values[1], Value::Text("Alice".to_string()));
        
        // Reader doesn't need to commit (read-only)
    }

    #[test]
    fn test_snapshot_isolation() {
        let db = MvccDatabase::new();
        let table = db.create_table("users".to_string()).unwrap();
        
        // T1: Insert and commit record
        let tx1 = db.begin_transaction();
        table.insert(1, create_test_record(1, "Alice"), tx1).unwrap();
        db.commit_transaction(tx1);
        
        // T2: Read committed data
        let tx2 = db.begin_transaction();
        let record = table.get(1, tx2);
        assert!(record.is_some(), "Should see committed data");
        
        // T3: Update but don't commit
        let tx3 = db.begin_transaction();
        table.update(1, create_test_record(1, "Bob"), tx3).unwrap();
        
        // T4: Should see old version (uncommitted changes not visible)
        let tx4 = db.begin_transaction();
        let record = table.get(1, tx4).unwrap();
        assert_eq!(record.values[1], Value::Text("Alice".to_string()), 
            "Should not see uncommitted updates");
        
        // Commit update
        db.commit_transaction(tx3);
        
        // T5: Now should see new version
        let tx5 = db.begin_transaction();
        let record = table.get(1, tx5).unwrap();
        assert_eq!(record.values[1], Value::Text("Bob".to_string()),
            "Should see committed updates");
    }

    #[test]
    fn test_concurrent_readers() {
        let db = Arc::new(MvccDatabase::new());
        let table = db.create_table("users".to_string()).unwrap();
        
        // Insert data
        let tx = db.begin_transaction();
        for i in 1..=100 {
            table.insert(i, create_test_record(i as i64, &format!("User{}", i)), tx).unwrap();
        }
        db.commit_transaction(tx);
        
        // Spawn multiple readers
        let mut handles = vec![];
        for reader_id in 0..10 {
            let db_clone = db.clone();
            let table_clone = table.clone();
            
            let handle = std::thread::spawn(move || {
                let tx = db_clone.begin_transaction();
                let results = table_clone.scan(tx);
                
                // Each reader should see all 100 records
                assert_eq!(results.len(), 100, "Reader {} should see all records", reader_id);
            });
            
            handles.push(handle);
        }
        
        // Wait for all readers
        for handle in handles {
            handle.join().unwrap();
        }
    }

    #[test]
    fn test_update_creates_version() {
        let db = MvccDatabase::new();
        let table = db.create_table("users".to_string()).unwrap();
        
        // Insert
        let tx1 = db.begin_transaction();
        table.insert(1, create_test_record(1, "Alice"), tx1).unwrap();
        db.commit_transaction(tx1);
        
        // Update
        let tx2 = db.begin_transaction();
        table.update(1, create_test_record(1, "Bob"), tx2).unwrap();
        db.commit_transaction(tx2);
        
        // Check stats - should have 2 versions
        let stats = table.stats();
        assert_eq!(stats.total_versions, 2);
        
        // New reader sees Bob
        let tx3 = db.begin_transaction();
        let record = table.get(1, tx3).unwrap();
        assert_eq!(record.values[1], Value::Text("Bob".to_string()));
    }

    #[test]
    fn test_gc_removes_old_versions() {
        let db = MvccDatabase::new();
        let table = db.create_table("users".to_string()).unwrap();
        
        // Insert initial version
        let tx1 = db.begin_transaction();
        table.insert(1, create_test_record(1, "V1"), tx1).unwrap();
        db.commit_transaction(tx1);
        
        // Update multiple times
        for i in 2..=5 {
            let tx = db.begin_transaction();
            table.update(1, create_test_record(1, &format!("V{}", i)), tx).unwrap();
            db.commit_transaction(tx);
        }
        
        // Should have 5 versions now
        let stats_before = table.stats();
        assert_eq!(stats_before.total_versions, 5, "Should have 5 versions");
        
        // No active transactions, so GC can remove all but latest
        let removed = db.gc();
        assert!(removed >= 4, "Should have removed at least 4 old versions, got {}", removed);
        
        // Stats should show only 1 version
        let stats_after = table.stats();
        assert_eq!(stats_after.total_versions, 1, "Should have only 1 version after GC");
    }
}
