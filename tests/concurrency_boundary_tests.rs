//! Concurrency Boundary Tests
//!
//! Tests for concurrency edge cases and boundary conditions

use std::sync::Arc;
use std::thread;

// ============================================================================
// Basic Concurrency Tests
// ============================================================================

#[test]
fn test_thread_spawning() {
    let mut handles = vec![];
    
    for i in 0..100 {
        let handle = thread::spawn(move || {
            i * i
        });
        handles.push(handle);
    }
    
    for (i, handle) in handles.into_iter().enumerate() {
        let result = handle.join().unwrap();
        assert_eq!(result, i * i);
    }
}

#[test]
fn test_arc_sharing() {
    let data = Arc::new(vec![1, 2, 3, 4, 5]);
    let mut handles = vec![];
    
    for _ in 0..10 {
        let data = Arc::clone(&data);
        let handle = thread::spawn(move || {
            let sum: i32 = data.iter().sum();
            sum
        });
        handles.push(handle);
    }
    
    for handle in handles {
        let result = handle.join().unwrap();
        assert_eq!(result, 15);
    }
}

#[test]
fn test_mutex_contention() {
    use std::sync::Mutex;
    
    let counter = Arc::new(Mutex::new(0));
    let mut handles = vec![];
    
    for _ in 0..100 {
        let counter = Arc::clone(&counter);
        let handle = thread::spawn(move || {
            let mut num = counter.lock().unwrap();
            *num += 1;
        });
        handles.push(handle);
    }
    
    for handle in handles {
        handle.join().unwrap();
    }
    
    assert_eq!(*counter.lock().unwrap(), 100);
}

#[test]
fn test_rwlock_contention() {
    use std::sync::RwLock;
    
    let data = Arc::new(RwLock::new(0));
    let mut handles = vec![];
    
    // Spawn readers
    for _ in 0..50 {
        let data = Arc::clone(&data);
        let handle = thread::spawn(move || {
            let val = data.read().unwrap();
            *val
        });
        handles.push(handle);
    }
    
    // Spawn writers
    for _ in 0..10 {
        let data = Arc::clone(&data);
        let handle = thread::spawn(move || {
            let mut val = data.write().unwrap();
            *val += 1;
        });
        handles.push(handle);
    }
    
    for handle in handles {
        handle.join().unwrap();
    }
}

// ============================================================================
// Atomic Operations Tests
// ============================================================================

#[test]
fn test_atomic_counter() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    
    let counter = Arc::new(AtomicUsize::new(0));
    let mut handles = vec![];
    
    for _ in 0..100 {
        let counter = Arc::clone(&counter);
        let handle = thread::spawn(move || {
            counter.fetch_add(1, Ordering::SeqCst);
        });
        handles.push(handle);
    }
    
    for handle in handles {
        handle.join().unwrap();
    }
    
    assert_eq!(counter.load(Ordering::SeqCst), 100);
}

// ============================================================================
// Channel Tests
// ============================================================================

#[test]
fn test_channel_communication() {
    use std::sync::mpsc;
    
    let (tx, rx) = mpsc::channel();
    
    for i in 0..100 {
        let tx = tx.clone();
        thread::spawn(move || {
            tx.send(i).unwrap();
        });
    }
    
    drop(tx);
    
    let mut received = vec![];
    for val in rx {
        received.push(val);
    }
    
    assert_eq!(received.len(), 100);
}
