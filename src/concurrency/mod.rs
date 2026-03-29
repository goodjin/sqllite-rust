//! Concurrency Control Module
//!
//! Provides MVCC (Multi-Version Concurrency Control) for high-performance
//! concurrent access to the database.
//!
//! Key features:
//! - Snapshot isolation for lock-free reads
//! - Write-ahead versioning for conflict-free writes
//! - Garbage collection for obsolete versions

pub mod mvcc;
pub mod snapshot;

pub use mvcc::{MvccManager, Snapshot, TxId, Version, VersionChain, MvccStats};
pub use snapshot::{MvccDatabase, MvccTable, DatabaseStats, TableStats};
