pub mod pager;
pub mod storage;
pub mod sql;
pub mod vm;
pub mod transaction;
pub mod index;
pub mod executor;
pub mod optimizer;
pub mod concurrency;
pub mod columnar;
pub mod jit;
pub mod gpu;

// Phase 5: Feature Completeness
pub mod trigger;   // P5-2: Triggers
pub mod window;    // P5-4: Window Functions
pub mod fts;       // P5-6: Full Text Search
pub mod rtree;     // P5-7: R-Tree Spatial Index
pub mod json;      // P5-8: JSON Support
