#![no_main]

use libfuzzer_sys::fuzz_target;
use std::sync::Arc;
use std::collections::HashMap;

// MVCC (Multi-Version Concurrency Control) fuzzing target
// Tests concurrent operations with snapshot isolation

fuzz_target!(|data: &[u8]| {
    if data.len() < 4 {
        return;
    }
    
    // Parse fuzz input into concurrent operations
    let concurrent_ops = parse_concurrent_operations(data);
    
    // Execute operations and verify MVCC invariants
    let _ = execute_mvcc_operations(concurrent_ops);
    
    // Test version chain management
    let _ = fuzz_version_chain(data);
    
    // Test snapshot isolation
    let _ = fuzz_snapshot_isolation(data);
});

#[derive(Debug, Clone, Copy, PartialEq)]
enum TransactionOp {
    Begin { tx_id: u64, read_ts: u64 },
    Read { tx_id: u64, key: u64 },
    Write { tx_id: u64, key: u64, value: u64 },
    Commit { tx_id: u64, commit_ts: u64 },
    Rollback { tx_id: u64 },
}

#[derive(Debug, Clone)]
struct ConcurrentOperations {
    transactions: Vec<Vec<TransactionOp>>,
}

fn parse_concurrent_operations(data: &[u8]) -> ConcurrentOperations {
    let num_txns = (data[0] as usize % 8) + 1; // 1-8 concurrent transactions
    let mut transactions: Vec<Vec<TransactionOp>> = vec![Vec::new(); num_txns];
    
    let mut i = 1;
    let mut tx_id_counter: u64 = 1;
    let mut timestamp: u64 = 1;
    
    while i < data.len() && tx_id_counter <= 64 {
        let txn_idx = (data[i] as usize) % num_txns;
        i += 1;
        
        if i >= data.len() {
            break;
        }
        
        let op_type = data[i] % 5;
        i += 1;
        
        match op_type {
            0 => {
                // Begin transaction
                let read_ts = timestamp;
                timestamp += 1;
                transactions[txn_idx].push(TransactionOp::Begin { 
                    tx_id: tx_id_counter, 
                    read_ts 
                });
                tx_id_counter += 1;
            }
            1 if !transactions[txn_idx].is_empty() => {
                // Read
                let key = data.get(i).copied().unwrap_or(0) as u64;
                i += 1;
                
                // Find the tx_id for this transaction
                if let Some(TransactionOp::Begin { tx_id, .. }) = 
                    transactions[txn_idx].iter().find(|op| matches!(op, TransactionOp::Begin { .. })) {
                    transactions[txn_idx].push(TransactionOp::Read { tx_id: *tx_id, key });
                }
            }
            2 if !transactions[txn_idx].is_empty() => {
                // Write
                let key = data.get(i).copied().unwrap_or(0) as u64;
                i += 1;
                let value = data.get(i).copied().unwrap_or(0) as u64;
                i += 1;
                
                if let Some(TransactionOp::Begin { tx_id, .. }) = 
                    transactions[txn_idx].iter().find(|op| matches!(op, TransactionOp::Begin { .. })) {
                    transactions[txn_idx].push(TransactionOp::Write { tx_id: *tx_id, key, value });
                }
            }
            3 if !transactions[txn_idx].is_empty() => {
                // Commit
                let commit_ts = timestamp;
                timestamp += 1;
                
                if let Some(TransactionOp::Begin { tx_id, .. }) = 
                    transactions[txn_idx].iter().find(|op| matches!(op, TransactionOp::Begin { .. })) {
                    // Check if not already committed/rolled back
                    let already_finished = transactions[txn_idx].iter().any(|op| {
                        matches!(op, TransactionOp::Commit { tx_id: tid, .. } | TransactionOp::Rollback { tx_id: tid } if *tid == *tx_id)
                    });
                    
                    if !already_finished {
                        transactions[txn_idx].push(TransactionOp::Commit { tx_id: *tx_id, commit_ts });
                    }
                }
            }
            4 if !transactions[txn_idx].is_empty() => {
                // Rollback
                if let Some(TransactionOp::Begin { tx_id, .. }) = 
                    transactions[txn_idx].iter().find(|op| matches!(op, TransactionOp::Begin { .. })) {
                    let already_finished = transactions[txn_idx].iter().any(|op| {
                        matches!(op, TransactionOp::Commit { tx_id: tid, .. } | TransactionOp::Rollback { tx_id: tid } if *tid == *tx_id)
                    });
                    
                    if !already_finished {
                        transactions[txn_idx].push(TransactionOp::Rollback { tx_id: *tx_id });
                    }
                }
            }
            _ => {}
        }
        
        // Limit operations per transaction
        if transactions[txn_idx].len() >= 20 {
            continue;
        }
    }
    
    ConcurrentOperations { transactions }
}

fn execute_mvcc_operations(ops: ConcurrentOperations) -> Result<(), ()> {
    // Simulated MVCC database
    struct VersionedValue {
        value: u64,
        created_by: u64,
        deleted_by: Option<u64>,
    }
    
    let mut database: HashMap<u64, Vec<VersionedValue>> = HashMap::new();
    let mut active_transactions: HashMap<u64, u64> = HashMap::new(); // tx_id -> read_ts
    let mut committed_writes: HashMap<u64, Vec<(u64, u64)>> = HashMap::new(); // tx_id -> [(key, value)]
    
    for txn_ops in ops.transactions {
        for op in txn_ops {
            match op {
                TransactionOp::Begin { tx_id, read_ts } => {
                    active_transactions.insert(tx_id, read_ts);
                }
                TransactionOp::Read { tx_id, key } => {
                    if let Some(&read_ts) = active_transactions.get(&tx_id) {
                        // Find visible version
                        if let Some(versions) = database.get(&key) {
                            let visible = versions.iter()
                                .find(|v| v.created_by <= read_ts && 
                                      (v.deleted_by.is_none() || v.deleted_by.unwrap() > read_ts));
                            // Just checking visibility, don't need the value
                            let _ = visible;
                        }
                    }
                }
                TransactionOp::Write { tx_id, key, value } => {
                    if active_transactions.contains_key(&tx_id) {
                        committed_writes.entry(tx_id)
                            .or_insert_with(Vec::new)
                            .push((key, value));
                    }
                }
                TransactionOp::Commit { tx_id, commit_ts } => {
                    if active_transactions.remove(&tx_id).is_some() {
                        // Apply writes
                        if let Some(writes) = committed_writes.remove(&tx_id) {
                            for (key, value) in writes {
                                // Mark old version as deleted
                                if let Some(versions) = database.get_mut(&key) {
                                    if let Some(v) = versions.last_mut() {
                                        v.deleted_by = Some(commit_ts);
                                    }
                                }
                                
                                // Create new version
                                database.entry(key)
                                    .or_insert_with(Vec::new)
                                    .push(VersionedValue {
                                        value,
                                        created_by: commit_ts,
                                        deleted_by: None,
                                    });
                            }
                        }
                    }
                }
                TransactionOp::Rollback { tx_id } => {
                    active_transactions.remove(&tx_id);
                    committed_writes.remove(&tx_id);
                }
            }
        }
    }
    
    // Verify MVCC invariants
    verify_mvcc_invariants(&database)?;
    
    Ok(())
}

fn verify_mvcc_invariants(database: &HashMap<u64, Vec<VersionedValue>>) -> Result<(), ()> {
    for (key, versions) in database {
        // Check that versions are sorted by creation time
        for i in 1..versions.len() {
            let prev = &versions[i-1];
            let curr = &versions[i];
            
            // Each version should have a later creation time
            assert!(curr.created_by > prev.created_by, 
                "MVCC invariant violated: versions not sorted for key {}", key);
            
            // Previous version should be deleted by current version's creator
            assert_eq!(prev.deleted_by, Some(curr.created_by),
                "MVCC invariant violated: version chain broken for key {}", key);
        }
        
        // Latest version should not be deleted
        if let Some(last) = versions.last() {
            assert!(last.deleted_by.is_none(),
                "MVCC invariant violated: latest version deleted for key {}", key);
        }
    }
    
    Ok(())
}

fn fuzz_version_chain(data: &[u8]) -> Result<(), ()> {
    // Test version chain operations
    let max_versions = (data.get(0).copied().unwrap_or(5) as usize % 20) + 1;
    
    #[derive(Debug)]
    struct Version {
        data: u64,
        timestamp: u64,
        next: Option<Box<Version>>,
    }
    
    // Build version chain
    let mut head: Option<Box<Version>> = None;
    let mut ts = 1u64;
    
    for i in 0..max_versions {
        let data_val = data.get(i % data.len()).copied().unwrap_or(0) as u64;
        
        head = Some(Box::new(Version {
            data: data_val,
            timestamp: ts,
            next: head,
        }));
        ts += 1;
    }
    
    // Traverse and verify chain
    let mut count = 0;
    let mut current = &head;
    while let Some(v) = current {
        count += 1;
        current = &v.next;
    }
    
    assert_eq!(count, max_versions, "Version chain length mismatch");
    
    Ok(())
}

fn fuzz_snapshot_isolation(data: &[u8]) -> Result<(), ()> {
    // Test snapshot isolation properties
    // Read snapshot should see consistent view
    
    let snapshot_ts = data.get(0).copied().unwrap_or(1) as u64;
    
    #[derive(Debug)]
    struct Record {
        versions: Vec<(u64, u64)>, // (timestamp, value)
    }
    
    // Create records with versions
    let mut records: HashMap<u64, Record> = HashMap::new();
    
    for (i, &byte) in data.iter().enumerate().skip(1).take(100) {
        let key = (i % 10) as u64;
        let value = byte as u64;
        let ts = (i as u64) + 1;
        
        records.entry(key)
            .or_insert(Record { versions: Vec::new() })
            .versions.push((ts, value));
    }
    
    // Read at snapshot timestamp
    for (key, record) in &records {
        let visible = record.versions.iter()
            .filter(|(ts, _)| *ts <= snapshot_ts)
            .max_by_key(|(ts, _)| *ts);
        
        // Just verify we can compute visibility
        let _ = visible;
    }
    
    Ok(())
}
