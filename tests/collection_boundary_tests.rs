//! Collection Boundary Tests
//!
//! Tests for collection edge cases and boundary conditions

// ============================================================================
// Vec Boundary Tests
// ============================================================================

#[test]
fn test_vec_empty() {
    let v: Vec<i32> = vec![];
    assert!(v.is_empty());
    assert_eq!(v.len(), 0);
}

#[test]
fn test_vec_single_element() {
    let v = vec![1];
    assert_eq!(v.len(), 1);
    assert_eq!(v[0], 1);
}

#[test]
fn test_vec_many_elements() {
    let sizes = vec![10, 100, 1000, 10000];
    
    for size in sizes {
        let v: Vec<i32> = (0..size).collect();
        assert_eq!(v.len(), size);
    }
}

#[test]
fn test_vec_capacity() {
    let mut v = Vec::with_capacity(100);
    assert_eq!(v.capacity(), 100);
    assert!(v.is_empty());
}

#[test]
fn test_vec_push_pop() {
    let mut v = vec![];
    
    for i in 0..1000 {
        v.push(i);
    }
    
    for _ in 0..1000 {
        v.pop();
    }
    
    assert!(v.is_empty());
}

#[test]
fn test_vec_insert_remove() {
    let mut v = vec![1, 2, 3, 4, 5];
    
    v.insert(0, 0);
    assert_eq!(v[0], 0);
    
    v.insert(v.len(), 6);
    assert_eq!(v[v.len() - 1], 6);
    
    v.remove(0);
    assert_eq!(v[0], 1);
}

// ============================================================================
// HashMap Boundary Tests
// ============================================================================

#[test]
fn test_hashmap_empty() {
    use std::collections::HashMap;
    
    let m: HashMap<i32, i32> = HashMap::new();
    assert!(m.is_empty());
}

#[test]
fn test_hashmap_single_entry() {
    use std::collections::HashMap;
    
    let mut m = HashMap::new();
    m.insert(1, "value");
    
    assert_eq!(m.len(), 1);
    assert_eq!(m.get(&1), Some(&"value"));
}

#[test]
fn test_hashmap_many_entries() {
    use std::collections::HashMap;
    
    let mut m = HashMap::new();
    
    for i in 0..10000 {
        m.insert(i, i * 2);
    }
    
    assert_eq!(m.len(), 10000);
}

#[test]
fn test_hashmap_collisions() {
    use std::collections::HashMap;
    
    let mut m = HashMap::new();
    
    // Insert and retrieve
    m.insert("key1", 1);
    m.insert("key2", 2);
    
    assert_eq!(m.get("key1"), Some(&1));
    assert_eq!(m.get("key2"), Some(&2));
}

// ============================================================================
// HashSet Boundary Tests
// ============================================================================

#[test]
fn test_hashset_empty() {
    use std::collections::HashSet;
    
    let s: HashSet<i32> = HashSet::new();
    assert!(s.is_empty());
}

#[test]
fn test_hashset_many_elements() {
    use std::collections::HashSet;
    
    let mut s = HashSet::new();
    
    for i in 0..10000 {
        s.insert(i);
    }
    
    assert_eq!(s.len(), 10000);
}

#[test]
fn test_hashset_duplicates() {
    use std::collections::HashSet;
    
    let mut s = HashSet::new();
    
    for _ in 0..100 {
        s.insert(1);
    }
    
    assert_eq!(s.len(), 1);
}

// ============================================================================
// BTreeMap Boundary Tests
// ============================================================================

#[test]
fn test_btreemap_empty() {
    use std::collections::BTreeMap;
    
    let m: BTreeMap<i32, i32> = BTreeMap::new();
    assert!(m.is_empty());
}

#[test]
fn test_btreemap_ordered() {
    use std::collections::BTreeMap;
    
    let mut m = BTreeMap::new();
    
    // Insert in reverse order
    for i in (0..100).rev() {
        m.insert(i, i * 2);
    }
    
    // Should iterate in order
    let keys: Vec<_> = m.keys().cloned().collect();
    assert_eq!(keys, (0..100).collect::<Vec<_>>());
}

#[test]
fn test_btreemap_range() {
    use std::collections::BTreeMap;
    
    let mut m = BTreeMap::new();
    
    for i in 0..100 {
        m.insert(i, i);
    }
    
    let range: Vec<_> = m.range(25..75).map(|(&k, _)| k).collect();
    assert_eq!(range, (25..75).collect::<Vec<_>>());
}

// ============================================================================
// VecDeque Boundary Tests
// ============================================================================

#[test]
fn test_vecdeque_empty() {
    use std::collections::VecDeque;
    
    let d: VecDeque<i32> = VecDeque::new();
    assert!(d.is_empty());
}

#[test]
fn test_vecdeque_push_pop() {
    use std::collections::VecDeque;
    
    let mut d = VecDeque::new();
    
    // Push back
    for i in 0..100 {
        d.push_back(i);
    }
    
    // Push front
    for i in 0..100 {
        d.push_front(-i);
    }
    
    assert_eq!(d.len(), 200);
    
    // Pop from both ends
    assert_eq!(d.pop_front(), Some(-99));
    assert_eq!(d.pop_back(), Some(99));
}

// ============================================================================
// LinkedList Boundary Tests
// ============================================================================

#[test]
fn test_linkedlist_empty() {
    use std::collections::LinkedList;
    
    let l: LinkedList<i32> = LinkedList::new();
    assert!(l.is_empty());
}

#[test]
fn test_linkedlist_push_pop() {
    use std::collections::LinkedList;
    
    let mut l = LinkedList::new();
    
    for i in 0..100 {
        l.push_back(i);
    }
    
    assert_eq!(l.len(), 100);
    
    for i in 0..100 {
        assert_eq!(l.pop_front(), Some(i));
    }
    
    assert!(l.is_empty());
}

// ============================================================================
// BinaryHeap Boundary Tests
// ============================================================================

#[test]
fn test_binaryheap_empty() {
    use std::collections::BinaryHeap;
    
    let h: BinaryHeap<i32> = BinaryHeap::new();
    assert!(h.is_empty());
}

#[test]
fn test_binaryheap_push_pop() {
    use std::collections::BinaryHeap;
    
    let mut h = BinaryHeap::new();
    
    // Push in random order
    h.push(3);
    h.push(1);
    h.push(4);
    h.push(1);
    h.push(5);
    
    // Should pop in descending order
    assert_eq!(h.pop(), Some(5));
    assert_eq!(h.pop(), Some(4));
    assert_eq!(h.pop(), Some(3));
    assert_eq!(h.pop(), Some(1));
    assert_eq!(h.pop(), Some(1));
    assert_eq!(h.pop(), None);
}
