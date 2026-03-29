//! Expression Cache for Optimized Query Execution
//!
//! This module provides caching for expression evaluation results to avoid
//! redundant computations, especially useful for repeated expressions in
//! SELECT projections and WHERE clauses.

use crate::sql::ast::Expression;
use crate::storage::Value;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};

/// Statistics for expression cache performance monitoring
#[derive(Debug, Clone, Copy, Default)]
pub struct ExpressionCacheStats {
    pub hit_count: u64,
    pub miss_count: u64,
    pub eviction_count: u64,
}

impl ExpressionCacheStats {
    /// Calculate cache hit rate as a percentage
    pub fn hit_rate(&self) -> f64 {
        let total = self.hit_count + self.miss_count;
        if total == 0 {
            0.0
        } else {
            (self.hit_count as f64 / total as f64) * 100.0
        }
    }

    /// Get total number of cache lookups
    pub fn total_lookups(&self) -> u64 {
        self.hit_count + self.miss_count
    }
}

/// Cache key for expression evaluation
/// 
/// Uses a string representation of the expression for hashing to avoid
/// issues with f64 and other non-Hash types in Expression.
#[derive(Debug, Clone)]
pub struct ExpressionCacheKey {
    /// String representation of the expression
    expr_str: String,
    /// Parameter values for placeholder expressions
    param_values: Vec<Value>,
}

impl PartialEq for ExpressionCacheKey {
    fn eq(&self, other: &Self) -> bool {
        self.expr_str == other.expr_str && 
        Self::values_eq(&self.param_values, &other.param_values)
    }
}

impl Eq for ExpressionCacheKey {}

impl std::hash::Hash for ExpressionCacheKey {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.expr_str.hash(state);
        for val in &self.param_values {
            Self::hash_value(val, state);
        }
    }
}

impl ExpressionCacheKey {
    /// Compare two Value vectors for equality
    fn values_eq(a: &[Value], b: &[Value]) -> bool {
        if a.len() != b.len() {
            return false;
        }
        a.iter().zip(b.iter()).all(|(va, vb)| Self::value_eq(va, vb))
    }
    
    /// Compare two Values for equality
    fn value_eq(a: &Value, b: &Value) -> bool {
        match (a, b) {
            (Value::Null, Value::Null) => true,
            (Value::Integer(a), Value::Integer(b)) => a == b,
            (Value::Real(a), Value::Real(b)) => a.to_bits() == b.to_bits(),
            (Value::Text(a), Value::Text(b)) => a == b,
            (Value::Blob(a), Value::Blob(b)) => a == b,
            (Value::Vector(a), Value::Vector(b)) => {
                a.len() == b.len() && 
                a.iter().zip(b.iter()).all(|(va, vb)| va.to_bits() == vb.to_bits())
            }
            _ => false,
        }
    }
    
    /// Hash a Value
    fn hash_value<H: Hasher>(val: &Value, state: &mut H) {
        match val {
            Value::Null => 0u8.hash(state),
            Value::Integer(n) => {
                1u8.hash(state);
                n.hash(state);
            }
            Value::Real(f) => {
                2u8.hash(state);
                f.to_bits().hash(state);
            }
            Value::Text(s) => {
                3u8.hash(state);
                s.hash(state);
            }
            Value::Blob(b) => {
                4u8.hash(state);
                b.hash(state);
            }
            Value::Vector(v) => {
                5u8.hash(state);
                for f in v {
                    f.to_bits().hash(state);
                }
            }
        }
    }
}

impl ExpressionCacheKey {
    /// Create a new cache key for a simple expression (no record context)
    pub fn new(expr: &Expression) -> Self {
        Self {
            expr_str: format!("{:?}", expr),
            param_values: Vec::new(),
        }
    }

    /// Create a new cache key with parameter values
    pub fn with_params(expr: &Expression, params: &[Value]) -> Self {
        Self {
            expr_str: format!("{:?}", expr),
            param_values: params.to_vec(),
        }
    }

    /// Create a new cache key with record context
    pub fn with_record(expr: &Expression, record_values: &[Value]) -> Self {
        // Include record values hash in the key
        let record_hash = Self::hash_record_values(record_values);
        Self {
            expr_str: format!("{:?}#record:{}", expr, record_hash),
            param_values: Vec::new(),
        }
    }

    /// Create a new cache key with both parameters and record context
    pub fn with_params_and_record(expr: &Expression, params: &[Value], record_values: &[Value]) -> Self {
        let record_hash = Self::hash_record_values(record_values);
        Self {
            expr_str: format!("{:?}#record:{}", expr, record_hash),
            param_values: params.to_vec(),
        }
    }

    /// Hash record values for cache key
    fn hash_record_values(values: &[Value]) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        for value in values {
            // Hash based on type and value
            match value {
                Value::Null => 0.hash(&mut hasher),
                Value::Integer(n) => n.hash(&mut hasher),
                Value::Real(f) => f.to_bits().hash(&mut hasher),
                Value::Text(s) => s.hash(&mut hasher),
                Value::Blob(b) => b.hash(&mut hasher),
                Value::Vector(v) => {
                    for f in v {
                        f.to_bits().hash(&mut hasher);
                    }
                }
            }
        }
        hasher.finish()
    }
}

/// Expression evaluation cache
/// 
/// Caches the results of expression evaluations to avoid redundant computations.
/// Particularly effective for:
/// - Repeated expressions in SELECT projections
/// - Subexpressions in complex WHERE clauses
/// - Computed columns in ORDER BY
pub struct ExpressionCache {
    /// The cache storage
    cache: HashMap<ExpressionCacheKey, Value>,
    /// Maximum number of entries in the cache
    max_size: usize,
    /// Cache statistics
    hit_count: AtomicU64,
    miss_count: AtomicU64,
    eviction_count: AtomicU64,
}

impl ExpressionCache {
    /// Create a new expression cache with default size
    pub fn new() -> Self {
        Self::with_capacity(1000)
    }

    /// Create a new expression cache with specified capacity
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            cache: HashMap::with_capacity(capacity),
            max_size: capacity,
            hit_count: AtomicU64::new(0),
            miss_count: AtomicU64::new(0),
            eviction_count: AtomicU64::new(0),
        }
    }

    /// Get a cached value if it exists
    pub fn get(&self, key: &ExpressionCacheKey) -> Option<Value> {
        if let Some(value) = self.cache.get(key) {
            self.hit_count.fetch_add(1, Ordering::Relaxed);
            Some(value.clone())
        } else {
            self.miss_count.fetch_add(1, Ordering::Relaxed);
            None
        }
    }

    /// Insert a value into the cache
    pub fn insert(&mut self, key: ExpressionCacheKey, value: Value) {
        // Check if we need to evict entries
        if self.cache.len() >= self.max_size && !self.cache.contains_key(&key) {
            self.evict_oldest();
        }

        self.cache.insert(key, value);
    }

    /// Check if a key exists in the cache without updating stats
    pub fn contains_key(&self, key: &ExpressionCacheKey) -> bool {
        self.cache.contains_key(key)
    }

    /// Get the number of entries in the cache
    pub fn len(&self) -> usize {
        self.cache.len()
    }

    /// Check if the cache is empty
    pub fn is_empty(&self) -> bool {
        self.cache.is_empty()
    }

    /// Clear all entries from the cache
    pub fn clear(&mut self) {
        self.cache.clear();
        self.hit_count.store(0, Ordering::Relaxed);
        self.miss_count.store(0, Ordering::Relaxed);
        self.eviction_count.store(0, Ordering::Relaxed);
    }

    /// Get cache statistics
    pub fn stats(&self) -> ExpressionCacheStats {
        ExpressionCacheStats {
            hit_count: self.hit_count.load(Ordering::Relaxed),
            miss_count: self.miss_count.load(Ordering::Relaxed),
            eviction_count: self.eviction_count.load(Ordering::Relaxed),
        }
    }

    /// Get the hit count
    pub fn hit_count(&self) -> u64 {
        self.hit_count.load(Ordering::Relaxed)
    }

    /// Get the miss count
    pub fn miss_count(&self) -> u64 {
        self.miss_count.load(Ordering::Relaxed)
    }

    /// Evict oldest entries (simple random eviction for now)
    fn evict_oldest(&mut self) {
        // Simple strategy: remove 10% of entries when full
        let to_remove = self.max_size / 10;
        let keys: Vec<_> = self.cache.keys().take(to_remove).cloned().collect();
        for key in keys {
            self.cache.remove(&key);
        }
        self.eviction_count.fetch_add(to_remove as u64, Ordering::Relaxed);
    }
}

impl Default for ExpressionCache {
    fn default() -> Self {
        Self::new()
    }
}

/// Helper function to check if an expression is cacheable
/// 
/// Some expressions should not be cached:
/// - Non-deterministic functions (random(), now(), etc.)
/// - Subqueries (they have their own caching mechanism)
pub fn is_cacheable(expr: &Expression) -> bool {
    match expr {
        // Simple literals are always cacheable
        Expression::Integer(_) |
        Expression::String(_) |
        Expression::Float(_) |
        Expression::Boolean(_) |
        Expression::Null |
        Expression::Placeholder(_) => true,

        // Columns are cacheable within a record context
        Expression::Column(_) => true,

        // Vectors are cacheable if all elements are cacheable
        Expression::Vector(elements) => {
            elements.iter().all(is_cacheable)
        }

        // Binary operations are cacheable if both sides are cacheable
        Expression::Binary { left, right, .. } => {
            is_cacheable(left) && is_cacheable(right)
        }

        // Function calls: check if function is deterministic
        Expression::FunctionCall { name, args } => {
            is_deterministic_function(name) && args.iter().all(is_cacheable)
        }

        // Subqueries have their own caching, don't cache here
        Expression::Subquery(_) => false,
    }
}

/// Check if a function is deterministic (safe to cache)
fn is_deterministic_function(name: &str) -> bool {
    let deterministic_functions: &[&str] = &[
        "L2_DISTANCE",
        "VECTOR_L2_DISTANCE",
        "COSINE_SIMILARITY",
        "VECTOR_COSINE_SIMILARITY",
        "LENGTH",
        "ABS",
        "UPPER",
        "LOWER",
        "ROUND",
        "FLOOR",
        "CEIL",
        "COALESCE",
        "NULLIF",
    ];

    let non_deterministic_functions: &[&str] = &[
        "RANDOM",
        "RAND",
        "NOW",
        "CURRENT_TIMESTAMP",
        "CURRENT_DATE",
        "CURRENT_TIME",
        "UUID",
        "ROW_COUNT",
    ];

    let upper = name.to_uppercase();
    
    if non_deterministic_functions.contains(&upper.as_str()) {
        return false;
    }
    
    // Default to allowing caching for unknown functions
    // (can be restricted later)
    deterministic_functions.contains(&upper.as_str()) || true
}

/// Expression cache guard for temporary caching during query execution
/// 
/// This provides a scoped cache that's cleared after query execution
pub struct ExpressionCacheGuard {
    cache: ExpressionCache,
}

impl ExpressionCacheGuard {
    /// Create a new cache guard
    pub fn new() -> Self {
        Self {
            cache: ExpressionCache::new(),
        }
    }

    /// Create a new cache guard with capacity
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            cache: ExpressionCache::with_capacity(capacity),
        }
    }

    /// Get a value from the cache
    pub fn get(&self, key: &ExpressionCacheKey) -> Option<Value> {
        self.cache.get(key)
    }

    /// Insert a value into the cache
    pub fn insert(&mut self, key: ExpressionCacheKey, value: Value) {
        self.cache.insert(key, value);
    }

    /// Get statistics
    pub fn stats(&self) -> ExpressionCacheStats {
        self.cache.stats()
    }
}

impl Default for ExpressionCacheGuard {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::ast::BinaryOp;

    #[test]
    fn test_cache_basic_operations() {
        let mut cache = ExpressionCache::new();

        let expr = Expression::Integer(42);
        let key = ExpressionCacheKey::new(&expr);
        let value = Value::Integer(42);

        // First get should miss
        assert_eq!(cache.get(&key), None);
        
        // Insert and retrieve
        cache.insert(key.clone(), value.clone());
        assert_eq!(cache.get(&key), Some(value));

        // Stats should show a hit and a miss
        let stats = cache.stats();
        assert_eq!(stats.hit_count, 1);
        assert_eq!(stats.miss_count, 1);
    }

    #[test]
    fn test_cache_with_params() {
        let mut cache = ExpressionCache::new();

        let expr = Expression::Placeholder(0);
        let params = vec![Value::Integer(100)];
        let key = ExpressionCacheKey::with_params(&expr, &params);
        let value = Value::Integer(100);

        cache.insert(key.clone(), value.clone());
        
        // Should hit with same params
        assert_eq!(cache.get(&key), Some(value));
        
        // Should miss with different params
        let different_params = vec![Value::Integer(200)];
        let different_key = ExpressionCacheKey::with_params(&expr, &different_params);
        assert_eq!(cache.get(&different_key), None);
    }

    #[test]
    fn test_cache_with_record() {
        let mut cache = ExpressionCache::new();

        let expr = Expression::Column("salary".to_string());
        let record_values = vec![Value::Integer(1), Value::Integer(50000)];
        let key = ExpressionCacheKey::with_record(&expr, &record_values);
        let value = Value::Integer(50000);

        cache.insert(key.clone(), value.clone());
        
        // Should hit with same record values
        assert_eq!(cache.get(&key), Some(value));
        
        // Should miss with different record values
        let different_record = vec![Value::Integer(2), Value::Integer(60000)];
        let different_key = ExpressionCacheKey::with_record(&expr, &different_record);
        assert_eq!(cache.get(&different_key), None);
    }

    #[test]
    fn test_cache_stats() {
        let mut cache = ExpressionCache::new();

        let expr = Expression::Binary {
            left: Box::new(Expression::Integer(10)),
            op: BinaryOp::Add,
            right: Box::new(Expression::Integer(20)),
        };
        let key = ExpressionCacheKey::new(&expr);
        let value = Value::Integer(30);

        // Miss
        assert_eq!(cache.get(&key), None);
        
        // Insert
        cache.insert(key.clone(), value);
        
        // Hit
        cache.get(&key);
        cache.get(&key);

        let stats = cache.stats();
        assert_eq!(stats.hit_count, 2);
        assert_eq!(stats.miss_count, 1);
        assert!(stats.hit_rate() > 66.0); // 2/3 = 66.67%
    }

    #[test]
    fn test_is_cacheable() {
        // Literals are cacheable
        assert!(is_cacheable(&Expression::Integer(42)));
        assert!(is_cacheable(&Expression::String("test".to_string())));

        // Columns are cacheable
        assert!(is_cacheable(&Expression::Column("id".to_string())));

        // Binary ops with cacheable children are cacheable
        let binary = Expression::Binary {
            left: Box::new(Expression::Integer(10)),
            op: BinaryOp::Add,
            right: Box::new(Expression::Integer(20)),
        };
        assert!(is_cacheable(&binary));

        // Subqueries are not cacheable
        let subquery = Expression::Subquery(crate::sql::ast::SubqueryExpr::Exists(
            Box::new(crate::sql::ast::SelectStmt {
                ctes: vec![],
                columns: vec![crate::sql::ast::SelectColumn::All],
                from: "test".to_string(),
                joins: vec![],
                where_clause: None,
                group_by: vec![],
                having: None,
                order_by: vec![],
                limit: None,
                offset: None,
            })
        ));
        assert!(!is_cacheable(&subquery));
    }

    #[test]
    fn test_cache_eviction() {
        let mut cache = ExpressionCache::with_capacity(10);

        // Insert 15 items (over capacity)
        for i in 0..15 {
            let expr = Expression::Integer(i);
            let key = ExpressionCacheKey::new(&expr);
            cache.insert(key, Value::Integer(i));
        }

        // Should have evicted some entries
        assert!(cache.len() <= 10);
        
        let stats = cache.stats();
        assert!(stats.eviction_count > 0);
    }
}
