pub mod error;
pub mod btree;

pub use error::{IndexError, Result};
pub use btree::{BTreeIndex, NodeType};
