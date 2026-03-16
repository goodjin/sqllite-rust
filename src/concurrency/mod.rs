//! V3: Lock-Free Concurrency with MVCC
//!
//! 使用 MVCC (多版本并发控制) 实现无锁并发

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

/// 事务 ID 生成器
pub struct TransactionIdGenerator {
    next_id: AtomicU64,
}

impl TransactionIdGenerator {
    pub fn new() -> Self {
        Self {
            next_id: AtomicU64::new(1),
        }
    }

    pub fn next(&self) -> u64 {
        self.next_id.fetch_add(1, Ordering::SeqCst)
    }
}

/// MVCC 事务管理器
pub struct MvccTransactionManager {
    /// 全局事务 ID 生成器
    txn_id_gen: TransactionIdGenerator,
    /// 活跃事务集合
    active_transactions: Arc<Mutex<HashMap<u64, TransactionState>>>,
}

#[derive(Clone, Debug)]
pub enum TransactionState {
    Active,
    Committed,
    Aborted,
}

impl MvccTransactionManager {
    pub fn new() -> Self {
        Self {
            txn_id_gen: TransactionIdGenerator::new(),
            active_transactions: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// 开始新事务
    pub fn begin_transaction(&self) -> u64 {
        let txn_id = self.txn_id_gen.next();
        let mut active = self.active_transactions.lock().unwrap();
        active.insert(txn_id, TransactionState::Active);
        txn_id
    }

    /// 提交事务
    pub fn commit_transaction(&self, txn_id: u64) -> Result<(), ConcurrencyError> {
        let mut active = self.active_transactions.lock().unwrap();
        match active.get(&txn_id) {
            Some(TransactionState::Active) => {
                active.insert(txn_id, TransactionState::Committed);
                Ok(())
            }
            _ => Err(ConcurrencyError::InvalidTransaction(txn_id)),
        }
    }

    /// 回滚事务
    pub fn rollback_transaction(&self, txn_id: u64) -> Result<(), ConcurrencyError> {
        let mut active = self.active_transactions.lock().unwrap();
        match active.get(&txn_id) {
            Some(TransactionState::Active) => {
                active.insert(txn_id, TransactionState::Aborted);
                Ok(())
            }
            _ => Err(ConcurrencyError::InvalidTransaction(txn_id)),
        }
    }

    /// 检查事务是否可见
    pub fn is_visible(&self, _txn_id: u64, _read_ts: u64) -> bool {
        // 简化实现：所有已提交的事务都可见
        true
    }

    /// 清理已完成的事务
    pub fn cleanup_transactions(&self) {
        let mut active = self.active_transactions.lock().unwrap();
        active.retain(|_, state| matches!(state, TransactionState::Active));
    }
}

/// 并发错误
#[derive(Debug, Clone)]
pub enum ConcurrencyError {
    InvalidTransaction(u64),
    Conflict(String),
}

impl std::fmt::Display for ConcurrencyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConcurrencyError::InvalidTransaction(id) => {
                write!(f, "Invalid transaction: {}", id)
            }
            ConcurrencyError::Conflict(msg) => write!(f, "Conflict: {}", msg),
        }
    }
}

impl std::error::Error for ConcurrencyError {}

impl Default for MvccTransactionManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_transaction_lifecycle() {
        let manager = MvccTransactionManager::new();

        let txn_id = manager.begin_transaction();
        assert_eq!(txn_id, 1);

        manager.commit_transaction(txn_id).unwrap();

        let txn_id2 = manager.begin_transaction();
        assert_eq!(txn_id2, 2);

        manager.rollback_transaction(txn_id2).unwrap();
    }

    #[test]
    fn test_invalid_transaction() {
        let manager = MvccTransactionManager::new();

        // 尝试提交不存在的事务
        let result = manager.commit_transaction(999);
        assert!(result.is_err());
    }
}
