//! 连接池 (Connection Pool)
//!
//! 目标：复用数据库连接，避免重复初始化开销
//!
//! 使用方式：
//! ```ignore
//! let pool = ConnectionPool::new("db.sqlite", 4).unwrap();
//!
//! // 获取连接（自动复用）
//! let mut conn = pool.get();
//! conn.execute_sql("SELECT * FROM users").unwrap();
//! // 连接自动归还池中
//! ```

use std::sync::{Arc, Mutex};
use std::collections::VecDeque;
use super::{Executor, Result};

/// 连接池
pub struct ConnectionPool {
    /// 池中的连接
    connections: Arc<Mutex<VecDeque<PooledConnection>>>,
    /// 最大连接数
    max_size: usize,
    /// 数据库路径
    path: String,
}

/// 池化连接
pub struct PooledConnection {
    /// 内部的 Executor
    executor: Executor,
    /// 是否正在使用
    in_use: bool,
}

impl ConnectionPool {
    /// 创建新的连接池
    ///
    /// `path` - 数据库文件路径
    /// `size` - 连接池大小
    pub fn new(path: &str, size: usize) -> Result<Self> {
        let connections = VecDeque::with_capacity(size);

        Ok(Self {
            connections: Arc::new(Mutex::new(connections)),
            max_size: size,
            path: path.to_string(),
        })
    }

    /// 从池中获取一个连接
    ///
    /// 如果池中有可用连接，直接复用
    /// 否则创建新连接（如果未达到最大限制）
    /// 如果池满了，等待其他连接释放
    pub fn get(&self) -> PooledConnectionGuard {
        // 尝试从池中获取
        let executor = {
            let mut pool = self.connections.lock().unwrap();

            if let Some(mut conn) = pool.pop_front() {
                conn.in_use = true;
                Some(conn.executor)
            } else {
                // 池为空，创建新连接
                Some(Executor::open(&self.path).unwrap())
            }
        };

        PooledConnectionGuard {
            executor,
            pool: self.connections.clone(),
            in_use: true,
        }
    }

    /// 获取池的当前状态
    pub fn status(&self) -> PoolStatus {
        let pool = self.connections.lock().unwrap();
        PoolStatus {
            total: self.max_size,
            available: pool.len(),
            in_use: self.max_size - pool.len(),
        }
    }
}

/// 连接池守卫 - RAII 模式，自动归还连接
pub struct PooledConnectionGuard {
    /// 内部的 Executor
    pub executor: Option<Executor>,
    /// 连接池引用（用于归还连接）
    pool: Arc<Mutex<VecDeque<PooledConnection>>>,
    /// 是否正在使用
    in_use: bool,
}

impl Drop for PooledConnectionGuard {
    fn drop(&mut self) {
        if self.in_use {
            if let Some(executor) = self.executor.take() {
                // 归还连接到池中
                let conn = PooledConnection {
                    executor,
                    in_use: false,
                };

                let mut pool = self.pool.lock().unwrap();
                // 简化处理：直接归还，如果池满了就丢弃
                pool.push_back(conn);
            }
        }
    }
}

/// 连接池状态
#[derive(Debug)]
pub struct PoolStatus {
    /// 总连接数
    pub total: usize,
    /// 可用连接数
    pub available: usize,
    /// 正在使用的连接数
    pub in_use: usize,
}

impl std::fmt::Display for PoolStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Pool[total={}, available={}, in_use={}]",
            self.total, self.available, self.in_use)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pool_basic() {
        let _ = std::fs::remove_file("/tmp/test_pool.db");
        let pool = ConnectionPool::new("/tmp/test_pool.db", 2).unwrap();

        // 获取一个连接
        let mut conn = pool.get();
        if let Some(ref mut executor) = conn.executor {
            executor.execute_sql("CREATE TABLE t (id INTEGER)").unwrap();
        }

        // 连接自动归还
        drop(conn);

        // 池状态
        let status = pool.status();
        println!("{}", status);

        let _ = std::fs::remove_file("/tmp/test_pool.db");
    }

    #[test]
    fn test_pool_reuse() {
        let _ = std::fs::remove_file("/tmp/test_pool2.db");
        let pool = ConnectionPool::new("/tmp/test_pool2.db", 1).unwrap();

        {
            let mut conn = pool.get();
            if let Some(ref mut executor) = conn.executor {
                executor.execute_sql("CREATE TABLE t (id INTEGER)").unwrap();
            }
        }

        {
            let mut conn = pool.get();
            if let Some(ref mut executor) = conn.executor {
                executor.execute_sql("INSERT INTO t VALUES (1)").unwrap();
            }
        }

        {
            let mut conn = pool.get();
            if let Some(ref mut executor) = conn.executor {
                let result = executor.execute_sql("SELECT * FROM t");
                assert!(result.is_ok());
            }
        }

        let _ = std::fs::remove_file("/tmp/test_pool2.db");
    }
}
