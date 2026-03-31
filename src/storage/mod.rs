pub mod error;
pub mod record;
pub mod table;
pub mod btree;
pub mod btree_engine;
pub mod btree_core;
pub mod overflow;
pub mod btree_database;
pub mod wal;
pub mod btree_cache;
pub mod prefix_compression;
pub mod prefix_page;
pub mod foreign_key;
pub mod async_wal;
pub mod mvcc_wrapper;

pub use error::{StorageError, Result};
pub use mvcc_wrapper::{MvccDatabase, Transaction, TransactionState};
pub use record::{Record, Value};
pub use table::{Database, Table};
pub use btree::BPlusTreeIndex;
pub use btree_engine::{
    PageHeader, PageType, RecordHeader, BtreePageOps,
    PageAllocator, FreePageList, BtreeNode, IndexEntry, LeafEntry,
    MAX_INLINE_SIZE, compare_keys, binary_search_entries,
};
pub use btree_core::{BtreeStorage, RangeScanIterator, BTREE_ORDER};
pub use overflow::{OverflowManager, OverflowHeader, OverflowPageOps, RecordSplitter, OVERFLOW_DATA_SIZE};
pub use btree_database::{BtreeDatabase, BtreeTable};
pub use btree_cache::{BtreeNodeCache, BtreeNodeInfo, BtreeCacheStats};
pub use wal::{Wal, WalFrame, WalHeader};
pub use prefix_compression::{CompressedKey, PagePrefixCompressor, CompressionStats};
pub use prefix_page::{
    PrefixPageHeader, CompressedRecordHeader, PrefixCompressionOps, 
    PrefixCompressionStats, BtreeConfig, find_common_prefix, compress_page
};
pub use async_wal::{AsyncWalWriter, AsyncWalConfig, SharedAsyncWal, WalEntry};
