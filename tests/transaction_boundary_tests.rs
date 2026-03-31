//! Transaction Manager Boundary Tests
//!
//! Tests for transaction manager edge cases and boundary conditions

use sqllite_rust::transaction::manager::{
    TransactionManager, TransactionConfig, TransactionState
};
use sqllite_rust::transaction::TransactionError;
use sqllite_rust::pager::Page;

// ============================================================================
// Transaction State Boundary Tests
// ============================================================================

#[test]
fn test_transaction_state_transitions() {
    let temp_file = tempfile::NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap().to_string() + ".db";
    
    let mut manager = TransactionManager::new(&path, 4096).unwrap();
    
    // Initial state
    assert_eq!(manager.state(), TransactionState::None);
    
    // Begin
    manager.begin().unwrap();
    assert_eq!(manager.state(), TransactionState::Active);
    
    // Commit
    manager.commit().unwrap();
    assert_eq!(manager.state(), TransactionState::Committed);
    
    // Begin again
    manager.begin().unwrap();
    assert_eq!(manager.state(), TransactionState::Active);
    
    // Rollback
    manager.rollback().unwrap();
    assert_eq!(manager.state(), TransactionState::RolledBack);
}

#[test]
fn test_begin_already_active() {
    let temp_file = tempfile::NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap().to_string() + ".db";
    
    let mut manager = TransactionManager::new(&path, 4096).unwrap();
    
    manager.begin().unwrap();
    let result = manager.begin();
    
    assert!(matches!(result, Err(TransactionError::AlreadyActive)));
}

#[test]
fn test_commit_not_active() {
    let temp_file = tempfile::NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap().to_string() + ".db";
    
    let mut manager = TransactionManager::new(&path, 4096).unwrap();
    
    let result = manager.commit();
    assert!(matches!(result, Err(TransactionError::NotActive)));
}

#[test]
fn test_rollback_not_active() {
    let temp_file = tempfile::NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap().to_string() + ".db";
    
    let mut manager = TransactionManager::new(&path, 4096).unwrap();
    
    let result = manager.rollback();
    assert!(matches!(result, Err(TransactionError::NotActive)));
}

// ============================================================================
// Configuration Boundary Tests
// ============================================================================

#[test]
fn test_transaction_config_default() {
    let config = TransactionConfig::default();
    
    assert!(config.group_commit);
    assert_eq!(config.group_commit_timeout_ms, 10);
    assert_eq!(config.max_pending_transactions, 100);
    assert!(!config.async_commit);
    assert!(config.use_async_wal);
    assert_eq!(config.wal_batch_size, 100);
}

#[test]
fn test_transaction_config_sync_mode() {
    let config = TransactionConfig::sync_mode();
    
    assert!(!config.group_commit);
    assert_eq!(config.group_commit_timeout_ms, 0);
    assert_eq!(config.max_pending_transactions, 1);
    assert!(!config.async_commit);
    assert!(!config.use_async_wal);
}

#[test]
fn test_transaction_config_async_mode() {
    let config = TransactionConfig::async_mode();
    
    assert!(config.group_commit);
    assert_eq!(config.max_pending_transactions, 1000);
    assert!(config.async_commit);
    assert!(config.use_async_wal);
}

#[test]
fn test_config_with_various_batch_sizes() {
    let sizes = vec![1, 10, 100, 1000, 10000];
    
    for size in sizes {
        let config = TransactionConfig {
            wal_batch_size: size,
            max_pending_transactions: size,
            ..Default::default()
        };
        
        assert_eq!(config.wal_batch_size, size);
        assert_eq!(config.max_pending_transactions, size);
    }
}

#[test]
fn test_config_with_various_timeouts() {
    let timeouts = vec![0, 1, 10, 100, 1000, 10000];
    
    for timeout in timeouts {
        let config = TransactionConfig {
            group_commit_timeout_ms: timeout,
            wal_flush_timeout_ms: timeout,
            ..Default::default()
        };
        
        assert_eq!(config.group_commit_timeout_ms, timeout);
        assert_eq!(config.wal_flush_timeout_ms, timeout);
    }
}

// ============================================================================
// Page Write Boundary Tests
// ============================================================================

#[test]
fn test_write_page_boundaries() {
    let temp_file = tempfile::NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap().to_string() + ".db";
    
    let mut manager = TransactionManager::new(&path, 4096).unwrap();
    manager.begin().unwrap();
    
    // Write various page IDs
    let page_ids = vec![1u32, 2, 10, 100, 1000, u32::MAX];
    
    for page_id in page_ids {
        let page = Page::from_bytes(page_id, vec![0u8; 4096]);
        let result = manager.write_page(&page);
        assert!(result.is_ok());
    }
    
    manager.commit().unwrap();
}

#[test]
fn test_write_many_pages() {
    let temp_file = tempfile::NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap().to_string() + ".db";
    
    let mut manager = TransactionManager::new(&path, 4096).unwrap();
    manager.begin().unwrap();
    
    // Write many pages
    for i in 1..=1000 {
        let page = Page::from_bytes(i, vec![(i % 256) as u8; 4096]);
        manager.write_page(&page).unwrap();
    }
    
    manager.commit().unwrap();
}

#[test]
fn test_write_page_sync_boundaries() {
    let temp_file = tempfile::NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap().to_string() + ".db";
    
    let mut manager = TransactionManager::new_sync(&path, 4096).unwrap();
    manager.begin().unwrap();
    
    let page = Page::from_bytes(1, vec![1u8; 4096]);
    let result = manager.write_page_sync(&page);
    assert!(result.is_ok());
    
    manager.commit().unwrap();
}

// ============================================================================
// Batch Commit Boundary Tests
// ============================================================================

#[test]
fn test_empty_batch_flush() {
    let temp_file = tempfile::NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap().to_string() + ".db";
    
    let mut manager = TransactionManager::new(&path, 4096).unwrap();
    
    // Flush empty batch should succeed
    let result = manager.flush_batch();
    assert!(result.is_ok());
}

#[test]
fn test_batch_commit_many_transactions() {
    let temp_file = tempfile::NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap().to_string() + ".db";
    
    let config = TransactionConfig {
        group_commit: true,
        max_pending_transactions: 1000,
        ..Default::default()
    };
    
    let mut manager = TransactionManager::with_config(&path, 4096, config).unwrap();
    
    // Many transactions
    for _ in 0..100 {
        manager.begin().unwrap();
        let page = Page::from_bytes(1, vec![1u8; 4096]);
        manager.write_page(&page).unwrap();
        manager.commit().unwrap();
    }
    
    let stats = manager.stats();
    assert!(stats.total_commits >= 100);
}

#[test]
fn test_batch_commit_with_timeout() {
    let temp_file = tempfile::NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap().to_string() + ".db";
    
    let config = TransactionConfig {
        group_commit: true,
        group_commit_timeout_ms: 1,
        ..Default::default()
    };
    
    let mut manager = TransactionManager::with_config(&path, 4096, config).unwrap();
    
    manager.begin().unwrap();
    manager.write_page(&Page::from_bytes(1, vec![1u8; 4096])).unwrap();
    manager.commit().unwrap();
    
    // Wait for timeout
    std::thread::sleep(std::time::Duration::from_millis(10));
    
    let _ = manager.flush_batch();
}

// ============================================================================
// Async Commit Tests
// ============================================================================

#[test]
fn test_async_commit_boundaries() {
    let temp_file = tempfile::NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap().to_string() + ".db";
    
    let config = TransactionConfig::async_mode();
    
    let mut manager = TransactionManager::with_config(&path, 4096, config).unwrap();
    
    manager.begin().unwrap();
    let page = Page::from_bytes(1, vec![1u8; 4096]);
    manager.write_page(&page).unwrap();
    
    let result = manager.commit_async();
    assert!(result.is_ok());
}

#[test]
fn test_async_commit_when_disabled() {
    let temp_file = tempfile::NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap().to_string() + ".db";
    
    let config = TransactionConfig::sync_mode();
    
    let mut manager = TransactionManager::with_config(&path, 4096, config).unwrap();
    
    manager.begin().unwrap();
    let result = manager.commit_async();
    
    // Should fall back to sync commit
    assert!(result.is_ok());
}

// ============================================================================
// Statistics Tests
// ============================================================================

#[test]
fn test_transaction_stats_empty() {
    let temp_file = tempfile::NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap().to_string() + ".db";
    
    let manager = TransactionManager::new(&path, 4096).unwrap();
    let stats = manager.stats();
    
    assert_eq!(stats.total_commits, 0);
    assert_eq!(stats.batch_commits, 0);
}

#[test]
fn test_transaction_stats_after_operations() {
    let temp_file = tempfile::NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap().to_string() + ".db";
    
    let mut manager = TransactionManager::new(&path, 4096).unwrap();
    
    // Some commits
    for _ in 0..10 {
        manager.begin().unwrap();
        manager.commit().unwrap();
    }
    
    let stats = manager.stats();
    assert_eq!(stats.total_commits, 10);
}

#[test]
fn test_transaction_stats_after_rollback() {
    let temp_file = tempfile::NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap().to_string() + ".db";
    
    let mut manager = TransactionManager::new(&path, 4096).unwrap();
    
    // Commit some
    for _ in 0..5 {
        manager.begin().unwrap();
        manager.commit().unwrap();
    }
    
    // Rollback some
    for _ in 0..3 {
        manager.begin().unwrap();
        manager.rollback().unwrap();
    }
    
    let stats = manager.stats();
    assert_eq!(stats.total_commits, 5);
}

// ============================================================================
// Pending Transaction Tests
// ============================================================================

#[test]
fn test_pending_count_boundaries() {
    let temp_file = tempfile::NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap().to_string() + ".db";
    
    let config = TransactionConfig::async_mode();
    let mut manager = TransactionManager::with_config(&path, 4096, config).unwrap();
    
    assert_eq!(manager.pending_count(), 0);
    
    // Async commits add to pending
    for _ in 0..5 {
        manager.begin().unwrap();
        manager.commit_async().unwrap();
    }
    
    // May or may not have pending depending on flush timing
    let _ = manager.pending_count();
}

// ============================================================================
// Checkpoint Tests
// ============================================================================

#[test]
fn test_checkpoint_empty() {
    let temp_file = tempfile::NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap().to_string() + ".db";
    
    let mut manager = TransactionManager::new(&path, 4096).unwrap();
    
    let result = manager.checkpoint(|_page_id, _data| Ok(()));
    assert!(result.is_ok());
}

#[test]
fn test_checkpoint_after_writes() {
    let temp_file = tempfile::NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap().to_string() + ".db";
    
    let mut manager = TransactionManager::new(&path, 4096).unwrap();
    
    // Write some data
    manager.begin().unwrap();
    for i in 1..=10 {
        let page = Page::from_bytes(i, vec![i as u8; 4096]);
        manager.write_page(&page).unwrap();
    }
    manager.commit().unwrap();
    
    // Checkpoint
    let result = manager.checkpoint(|_page_id, _data| Ok(()));
    assert!(result.is_ok());
}

// ============================================================================
// Isolation Tests
// ============================================================================

#[test]
fn test_read_version_tracking() {
    let temp_file = tempfile::NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap().to_string() + ".db";
    
    let mut manager = TransactionManager::new(&path, 4096).unwrap();
    
    manager.begin().unwrap();
    let read_version = manager.read_version;
    
    manager.write_page(&Page::from_bytes(1, vec![1u8; 4096])).unwrap();
    manager.commit().unwrap();
    
    let write_version = manager.write_version;
    
    // Write version should advance
    assert!(write_version > read_version);
}

// ============================================================================
// Error Handling Tests
// ============================================================================

#[test]
fn test_error_display() {
    let errors = vec![
        TransactionError::AlreadyActive,
        TransactionError::NotActive,
        TransactionError::WalError("test error".to_string()),
        TransactionError::Io(std::io::Error::new(std::io::ErrorKind::Other, "io error")),
        TransactionError::Other("other error".to_string()),
    ];
    
    for err in errors {
        let _ = format!("{}", err);
    }
}

#[test]
fn test_error_debug() {
    let err = TransactionError::NotActive;
    let _ = format!("{:?}", err);
}

// ============================================================================
// Close and Cleanup Tests
// ============================================================================

#[test]
fn test_close_with_pending() {
    let temp_file = tempfile::NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap().to_string() + ".db";
    
    let config = TransactionConfig::async_mode();
    let mut manager = TransactionManager::with_config(&path, 4096, config).unwrap();
    
    manager.begin().unwrap();
    manager.write_page(&Page::from_bytes(1, vec![1u8; 4096])).unwrap();
    manager.commit_async().unwrap();
    
    // Close should flush pending
    let result = manager.close();
    assert!(result.is_ok());
}

#[test]
fn test_drop_with_pending() {
    let temp_file = tempfile::NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap().to_string() + ".db";
    
    let config = TransactionConfig::async_mode();
    let mut manager = TransactionManager::with_config(&path, 4096, config).unwrap();
    
    manager.begin().unwrap();
    manager.write_page(&Page::from_bytes(1, vec![1u8; 4096])).unwrap();
    manager.commit_async().unwrap();
    
    // Drop should handle pending gracefully
    drop(manager);
}

// ============================================================================
// Configuration Update Tests
// ============================================================================

#[test]
fn test_set_config_mid_transaction() {
    let temp_file = tempfile::NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap().to_string() + ".db";
    
    let mut manager = TransactionManager::new(&path, 4096).unwrap();
    
    let new_config = TransactionConfig::async_mode();
    manager.set_config(new_config);
    
    assert!(manager.config().async_commit);
}

#[test]
fn test_is_async_wal() {
    let temp_file = tempfile::NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap().to_string() + ".db";
    
    let manager_sync = TransactionManager::new_sync(&path, 4096).unwrap();
    assert!(!manager_sync.is_async_wal());
    
    drop(manager_sync);
    
    let manager_async = TransactionManager::new_async(&path, 4096).unwrap();
    assert!(manager_async.is_async_wal());
}

// ============================================================================
// Concurrent Transaction Tests
// ============================================================================

#[test]
fn test_concurrent_transaction_managers() {
    use std::thread;
    
    let temp_dir = tempfile::tempdir().unwrap();
    let mut handles = vec![];
    
    for i in 0..10 {
        let path = temp_dir.path().join(format!("test{}.db", i));
        let handle = thread::spawn(move || {
            let mut manager = TransactionManager::new(
                path.to_str().unwrap(), 
                4096
            ).unwrap();
            
            for _ in 0..10 {
                manager.begin().unwrap();
                let page = Page::from_bytes(1, vec![i as u8; 4096]);
                manager.write_page(&page).unwrap();
                manager.commit().unwrap();
            }
        });
        handles.push(handle);
    }
    
    for handle in handles {
        handle.join().unwrap();
    }
}

// ============================================================================
// Stress Tests
// ============================================================================

#[test]
fn test_rapid_begin_commit() {
    let temp_file = tempfile::NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap().to_string() + ".db";
    
    let mut manager = TransactionManager::new(&path, 4096).unwrap();
    
    for _ in 0..1000 {
        manager.begin().unwrap();
        manager.commit().unwrap();
    }
    
    let stats = manager.stats();
    assert_eq!(stats.total_commits, 1000);
}

#[test]
fn test_rapid_begin_rollback() {
    let temp_file = tempfile::NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap().to_string() + ".db";
    
    let mut manager = TransactionManager::new(&path, 4096).unwrap();
    
    for _ in 0..1000 {
        manager.begin().unwrap();
        manager.rollback().unwrap();
    }
}

#[test]
fn test_alternating_commit_rollback() {
    let temp_file = tempfile::NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap().to_string() + ".db";
    
    let mut manager = TransactionManager::new(&path, 4096).unwrap();
    
    for i in 0..100 {
        manager.begin().unwrap();
        if i % 2 == 0 {
            manager.commit().unwrap();
        } else {
            manager.rollback().unwrap();
        }
    }
    
    let stats = manager.stats();
    assert_eq!(stats.total_commits, 50);
}

// ============================================================================
// Page Size Tests
// ============================================================================

#[test]
fn test_various_page_sizes() {
    let page_sizes = vec![512, 1024, 2048, 4096, 8192, 16384, 32768, 65536];
    
    for page_size in page_sizes {
        let temp_file = tempfile::NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap().to_string() + ".db";
        
        let result = TransactionManager::new(&path, page_size);
        assert!(result.is_ok());
    }
}

// ============================================================================
// Write Pattern Tests
// ============================================================================

#[test]
fn test_write_same_page_multiple_times() {
    let temp_file = tempfile::NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap().to_string() + ".db";
    
    let mut manager = TransactionManager::new(&path, 4096).unwrap();
    
    manager.begin().unwrap();
    
    // Write same page multiple times
    for i in 0..100 {
        let page = Page::from_bytes(1, vec![i as u8; 4096]);
        manager.write_page(&page).unwrap();
    }
    
    manager.commit().unwrap();
}

#[test]
fn test_write_pattern_sequential() {
    let temp_file = tempfile::NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap().to_string() + ".db";
    
    let mut manager = TransactionManager::new(&path, 4096).unwrap();
    
    manager.begin().unwrap();
    
    // Sequential writes
    for i in 1..=1000 {
        let page = Page::from_bytes(i, vec![(i % 256) as u8; 4096]);
        manager.write_page(&page).unwrap();
    }
    
    manager.commit().unwrap();
}

#[test]
fn test_write_pattern_random() {
    use rand::seq::SliceRandom;
    
    let temp_file = tempfile::NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap().to_string() + ".db";
    
    let mut manager = TransactionManager::new(&path, 4096).unwrap();
    
    manager.begin().unwrap();
    
    // Random writes
    let mut page_ids: Vec<u32> = (1..=1000).collect();
    page_ids.shuffle(&mut rand::thread_rng());
    
    for page_id in page_ids {
        let page = Page::from_bytes(page_id, vec![(page_id % 256) as u8; 4096]);
        manager.write_page(&page).unwrap();
    }
    
    manager.commit().unwrap();
}

// ============================================================================
// Recovery Tests
// ============================================================================

#[test]
fn test_recovery_after_crash() {
    let temp_file = tempfile::NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap().to_string() + ".db";
    
    // Simulate work
    {
        let mut manager = TransactionManager::new(&path, 4096).unwrap();
        
        for _ in 0..10 {
            manager.begin().unwrap();
            for i in 1..=10 {
                let page = Page::from_bytes(i, vec![i as u8; 4096]);
                manager.write_page(&page).unwrap();
            }
            manager.commit().unwrap();
        }
    }
    
    // Reopen (simulate recovery)
    {
        let mut manager = TransactionManager::new(&path, 4096).unwrap();
        
        // Should be able to start new transaction
        manager.begin().unwrap();
        manager.commit().unwrap();
    }
}

// ============================================================================
// Nested Transaction Simulation Tests
// ============================================================================

#[test]
fn test_savepoint_simulation() {
    let temp_file = tempfile::NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap().to_string() + ".db";
    
    let mut manager = TransactionManager::new(&path, 4096).unwrap();
    
    // Outer transaction
    manager.begin().unwrap();
    manager.write_page(&Page::from_bytes(1, vec![1u8; 4096])).unwrap();
    
    // Note: Actual savepoints would need explicit support
    // This tests basic nested behavior
    
    manager.commit().unwrap();
}
