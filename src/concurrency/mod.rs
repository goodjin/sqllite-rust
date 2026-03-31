//! Concurrency Control Module
//!
//! Provides MVCC (Multi-Version Concurrency Control) for high-performance
//! concurrent access to the database.
//!
//! Key features:
//! - Snapshot isolation for lock-free reads
//! - Write-ahead versioning for conflict-free writes
//! - Garbage collection for obsolete versions
//! - Lock-free version chains using crossbeam-epoch
//!
//! Phase 2 - MVCC Concurrency Architecture:
//! - P2-1: Version chain design with lock-free linked lists
//! - P2-2: Snapshot isolation with xmin/xmax visibility rules
//! - P2-3: Lock-free read path with hazard pointer protection
//! - P2-4: Copy-on-Write (COW) for non-blocking writes
//! - P2-5: Garbage collector with configurable strategies
//! - P2-6: Optimistic locking for concurrent writes

pub mod cow;
pub mod gc;
pub mod mvcc;
pub mod optimistic_lock;
pub mod snapshot;

// Core MVCC types
pub use mvcc::{
    LockFreeMvccTable as MvccTableV2,
    LockFreeVersionChain, 
    MvccManager, 
    MvccStats,
    Snapshot, 
    Timestamp,
    TxId, 
    Version, 
    VersionChain,
    VersionNode,
};

// Legacy types for backward compatibility
pub use mvcc::{
    Version as MvccVersion,
    VersionChain as MvccVersionChain,
};

// Snapshot isolation types
pub use snapshot::{
    DatabaseStats,
    LockFreeMvccTable,
    MvccDatabase,
    MvccTable,
    TableStats,
    Transaction,
};

// COW exports (P2-4)
pub use cow::{
    AtomicVersionChain, 
    CowPage, 
    CowStorage, 
    CowError,
    VersionChainNode,
};

// Optimistic locking exports (P2-6)
pub use optimistic_lock::{
    OptimisticLock,
    OptimisticMvccManager,
    OptimisticMvccStats,
    LockManager,
    Transaction as OptimisticTransaction,
    TransactionState,
    ConflictType,
    ConflictError,
    ConflictStrategy,
};

// GC exports (P2-5)
pub use gc::{
    GarbageCollector,
    GcManager,
    GcMode,
    GcStats,
    BackgroundGcWorker,
    VersionChainStorage,
};

/// Re-export crossbeam-epoch for advanced users
pub use crossbeam_epoch;
