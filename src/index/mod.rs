pub mod error;
pub mod btree;
pub mod hnsw;
pub mod pushdown;  // P3-4: Index pushdown filter

pub use error::{IndexError, Result};
pub use btree::{BTreeIndex, NodeType};
pub use hnsw::HnswIndex;
pub use pushdown::{
    IndexFilter, 
    IndexScanIterator, 
    IndexScanStats,
    IndexPushdownOptimizer,
    PushdownBenefit,
    extract_index_filter,
    filter_to_range_scan,
};  // P3-4: Export pushdown types
