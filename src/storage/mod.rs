pub mod error;
pub mod record;
pub mod table;
pub mod btree;

pub use error::{StorageError, Result};
pub use record::{Record, Value};
pub use table::{Database, Table};
pub use btree::BPlusTreeIndex;
