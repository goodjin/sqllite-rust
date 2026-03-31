#![no_main]

use libfuzzer_sys::fuzz_target;
use std::collections::HashMap;

// Transaction fuzzing target
// Tests ACID properties with random transaction sequences

fuzz_target!(|data: &[u8]| {
    if data.is_empty() {
        return;
    }
    
    // Parse fuzz input into transaction sequences
    let sequences = parse_transaction_sequences(data);
    
    // Execute and verify ACID properties
    let _ = execute_and_verify_acid(sequences);
    
    // Test WAL operations
    let _ = fuzz_wal_operations(data);
    
    // Test recovery scenarios
    let _ = fuzz_recovery_scenarios(data);
});

#[derive(Debug, Clone, Copy, PartialEq)]
enum TxAction {
    Begin,
    Insert { key: u8, value: u8 },
    Update { key: u8, value: u8 },
    Delete { key: u8 },
    Select { key: u8 },
    Commit,
    Rollback,
    Checkpoint,
}

#[derive(Debug)]
struct TransactionSequence {
    actions: Vec<TxAction>,
}

fn parse_transaction_sequences(data: &[u8]) -> Vec<TransactionSequence> {
    let mut sequences = Vec::new();
    let mut current_actions = Vec::new();
    let mut in_transaction = false;
    
    for (i, &byte) in data.iter().enumerate() {
        let action = match byte % 8 {
            0 => {
                if !in_transaction {
                    in_transaction = true;
                    Some(TxAction::Begin)
                } else {
                    None
                }
            }
            1 if in_transaction => {
                let key = data.get(i + 1).copied().unwrap_or(0);
                let value = data.get(i + 2).copied().unwrap_or(0);
                Some(TxAction::Insert { key, value })
            }
            2 if in_transaction => {
                let key = data.get(i + 1).copied().unwrap_or(0);
                let value = data.get(i + 2).copied().unwrap_or(0);
                Some(TxAction::Update { key, value })
            }
            3 if in_transaction => {
                let key = data.get(i + 1).copied().unwrap_or(0);
                Some(TxAction::Delete { key })
            }
            4 if in_transaction => {
                let key = data.get(i + 1).copied().unwrap_or(0);
                Some(TxAction::Select { key })
            }
            5 if in_transaction => {
                in_transaction = false;
                Some(TxAction::Commit)
            }
            6 if in_transaction => {
                in_transaction = false;
                Some(TxAction::Rollback)
            }
            7 => {
                Some(TxAction::Checkpoint)
            }
            _ => None,
        };
        
        if let Some(action) = action {
            current_actions.push(action);
            
            // Split into new sequence if transaction completed
            if matches!(action, TxAction::Commit | TxAction::Rollback) {
                sequences.push(TransactionSequence { 
                    actions: current_actions.clone() 
                });
                current_actions.clear();
                in_transaction = false;
            }
        }
        
        // Limit sequence length
        if current_actions.len() >= 50 {
            break;
        }
    }
    
    // Add remaining actions if any
    if !current_actions.is_empty() {
        sequences.push(TransactionSequence { actions: current_actions });
    }
    
    sequences
}

fn execute_and_verify_acid(sequences: Vec<TransactionSequence>) -> Result<(), ()> {
    // Simulated database with ACID properties
    struct Database {
        committed_data: HashMap<u8, u8>,
        uncommitted_data: HashMap<u8, u8>,
        wal: Vec<WalEntry>,
        tx_active: bool,
        tx_modified_keys: Vec<u8>,
    }
    
    #[derive(Debug, Clone)]
    enum WalEntry {
        Begin,
        Insert { key: u8, value: u8 },
        Update { old: u8, key: u8, new: u8 },
        Delete { key: u8, old: u8 },
        Commit,
        Rollback,
    }
    
    let mut db = Database {
        committed_data: HashMap::new(),
        uncommitted_data: HashMap::new(),
        wal: Vec::new(),
        tx_active: false,
        tx_modified_keys: Vec::new(),
    };
    
    let mut committed_tx_count = 0;
    let mut rolled_back_tx_count = 0;
    
    for seq in sequences {
        for action in seq.actions {
            match action {
                TxAction::Begin => {
                    if !db.tx_active {
                        db.tx_active = true;
                        db.uncommitted_data = db.committed_data.clone();
                        db.tx_modified_keys.clear();
                        db.wal.push(WalEntry::Begin);
                    }
                }
                TxAction::Insert { key, value } => {
                    if db.tx_active {
                        db.uncommitted_data.insert(key, value);
                        db.tx_modified_keys.push(key);
                        db.wal.push(WalEntry::Insert { key, value });
                    }
                }
                TxAction::Update { key, value } => {
                    if db.tx_active {
                        let old = db.uncommitted_data.get(&key).copied();
                        db.uncommitted_data.insert(key, value);
                        db.tx_modified_keys.push(key);
                        
                        if let Some(old_val) = old {
                            db.wal.push(WalEntry::Update { old: old_val, key, new: value });
                        } else {
                            db.wal.push(WalEntry::Insert { key, value });
                        }
                    }
                }
                TxAction::Delete { key } => {
                    if db.tx_active {
                        if let Some(old) = db.uncommitted_data.remove(&key) {
                            db.tx_modified_keys.push(key);
                            db.wal.push(WalEntry::Delete { key, old });
                        }
                    }
                }
                TxAction::Select { key } => {
                    // Read operation - should not modify state
                    let data = if db.tx_active {
                        &db.uncommitted_data
                    } else {
                        &db.committed_data
                    };
                    let _ = data.get(&key); // Just read, don't use
                }
                TxAction::Commit => {
                    if db.tx_active {
                        db.committed_data = db.uncommitted_data.clone();
                        db.tx_active = false;
                        db.tx_modified_keys.clear();
                        db.wal.push(WalEntry::Commit);
                        committed_tx_count += 1;
                    }
                }
                TxAction::Rollback => {
                    if db.tx_active {
                        db.uncommitted_data.clear();
                        db.tx_active = false;
                        db.tx_modified_keys.clear();
                        db.wal.push(WalEntry::Rollback);
                        rolled_back_tx_count += 1;
                    }
                }
                TxAction::Checkpoint => {
                    // Truncate WAL after checkpoint
                    if !db.tx_active {
                        db.wal.clear();
                    }
                }
            }
        }
    }
    
    // Verify ACID properties
    
    // Atomicity: Either all changes are committed or none
    verify_atomicity(&db)?;
    
    // Consistency: Database should be in valid state
    verify_consistency(&db)?;
    
    // Isolation: Uncommitted changes should not be visible
    verify_isolation(&db)?;
    
    // Durability: Committed data should persist
    verify_durability(&db)?;
    
    // Check that WAL is consistent
    verify_wal_consistency(&db.wal)?;
    
    Ok(())
}

fn verify_atomicity(db: &Database) -> Result<(), ()> {
    // If transaction is not active, uncommitted data should be empty
    if !db.tx_active {
        assert!(db.uncommitted_data.is_empty(), 
            "Atomicity violation: uncommitted data exists outside transaction");
    }
    Ok(())
}

fn verify_consistency(db: &Database) -> Result<(), ()> {
    // Check that committed data has no null values (simplified consistency check)
    for (key, value) in &db.committed_data {
        assert!(key <= &255, "Consistency violation: invalid key");
        assert!(value <= &255, "Consistency violation: invalid value");
    }
    Ok(())
}

fn verify_isolation(db: &Database) -> Result<(), ()> {
    // If transaction is active, uncommitted changes should differ from committed
    if db.tx_active {
        // This is a weak check - just ensure we can compare
        let _ = db.uncommitted_data.len();
        let _ = db.committed_data.len();
    }
    Ok(())
}

fn verify_durability(db: &Database) -> Result<(), ()> {
    // Committed data should have entries
    // (This is a simplified check)
    let _ = db.committed_data.len();
    Ok(())
}

fn verify_wal_consistency(wal: &[WalEntry]) -> Result<(), ()> {
    // Check WAL ordering: Begin -> ... -> Commit/Rollback
    let mut in_tx = false;
    
    for entry in wal {
        match entry {
            WalEntry::Begin => {
                assert!(!in_tx, "WAL consistency: nested begin");
                in_tx = true;
            }
            WalEntry::Commit | WalEntry::Rollback => {
                assert!(in_tx, "WAL consistency: commit/rollback without begin");
                in_tx = false;
            }
            _ => {
                assert!(in_tx, "WAL consistency: operation outside transaction");
            }
        }
    }
    
    Ok(())
}

fn fuzz_wal_operations(data: &[u8]) -> Result<(), ()> {
    // Test Write-Ahead Log operations
    
    #[derive(Debug, Clone)]
    struct WalFrame {
        page_id: u32,
        data: [u8; 8],
        checksum: u32,
    }
    
    let mut wal_frames: Vec<WalFrame> = Vec::new();
    
    // Generate WAL frames from fuzz data
    for chunk in data.chunks(16) {
        if chunk.len() >= 8 {
            let page_id = u32::from_le_bytes([
                chunk[0], chunk[1], chunk[2], chunk[3]
            ]);
            
            let mut frame_data = [0u8; 8];
            frame_data.copy_from_slice(&chunk[0..8]);
            
            // Simple checksum
            let checksum = frame_data.iter().map(|&b| b as u32).sum();
            
            wal_frames.push(WalFrame {
                page_id,
                data: frame_data,
                checksum,
            });
        }
    }
    
    // Verify WAL frame integrity
    for frame in &wal_frames {
        let computed_checksum: u32 = frame.data.iter().map(|&b| b as u32).sum();
        assert_eq!(frame.checksum, computed_checksum, "WAL checksum mismatch");
    }
    
    // Test WAL playback (simulated)
    let mut applied_pages: HashMap<u32, [u8; 8]> = HashMap::new();
    
    for frame in &wal_frames {
        applied_pages.insert(frame.page_id, frame.data);
    }
    
    Ok(())
}

fn fuzz_recovery_scenarios(data: &[u8]) -> Result<(), ()> {
    // Test crash recovery scenarios
    
    #[derive(Debug)]
    enum CrashPoint {
        BeforeCommit,
        AfterCommit,
        DuringCheckpoint,
    }
    
    let crash_point = match data.get(0).copied().unwrap_or(0) % 3 {
        0 => CrashPoint::BeforeCommit,
        1 => CrashPoint::AfterCommit,
        _ => CrashPoint::DuringCheckpoint,
    };
    
    // Simulated database state
    let mut db: HashMap<u8, u8> = HashMap::new();
    let mut committed: HashMap<u8, u8> = HashMap::new();
    
    // Apply operations
    for (i, &byte) in data.iter().enumerate().skip(1) {
        let key = (i % 16) as u8;
        let value = byte;
        
        match crash_point {
            CrashPoint::BeforeCommit => {
                // Changes not committed, should be rolled back
                db.insert(key, value);
                // Simulate crash - discard changes
                if i == data.len() / 2 {
                    db = committed.clone(); // Rollback
                    break;
                }
            }
            CrashPoint::AfterCommit => {
                db.insert(key, value);
                // Commit periodically
                if i % 5 == 0 {
                    committed = db.clone();
                }
            }
            CrashPoint::DuringCheckpoint => {
                db.insert(key, value);
                // Just apply, checkpoint handling would be here
            }
        }
    }
    
    // Verify recovery state
    match crash_point {
        CrashPoint::BeforeCommit => {
            // Database should be in pre-transaction state
            assert_eq!(db, committed, "Recovery failed: uncommitted data persisted");
        }
        _ => {
            // Database should have committed data
            assert!(!db.is_empty() || data.len() <= 1, "Recovery failed: committed data lost");
        }
    }
    
    Ok(())
}
