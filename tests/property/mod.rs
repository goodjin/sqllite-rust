//! Property Testing Module
//! 
//! 使用 proptest 框架进行属性测试，目标生成1000+测试用例

pub mod btree_props;
pub mod storage_props;
pub mod sql_props;
pub mod transaction_props;
pub mod mvcc_props;
pub mod record_props;
pub mod pager_props;
pub mod index_props;
pub mod optimizer_props;
pub mod concurrency_props;

// 共享的测试工具和策略
pub mod strategies;
pub mod arbitrary;
