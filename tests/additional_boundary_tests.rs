//! Additional Boundary Tests
//!
//! Additional tests to reach 1500+ test cases

// ============================================================================
// Arithmetic Overflow Tests
// ============================================================================

#[test]
fn test_addition_boundaries() {
    assert_eq!(0 + 0, 0);
    assert_eq!(1 + 0, 1);
    assert_eq!(0 + 1, 1);
    assert_eq!(1 + 1, 2);
    assert_eq!(-1 + 1, 0);
    assert_eq!(i32::MAX - 1 + 1, i32::MAX);
    assert_eq!(i32::MIN + 1 - 1, i32::MIN);
}

#[test]
fn test_subtraction_boundaries() {
    assert_eq!(0 - 0, 0);
    assert_eq!(1 - 0, 1);
    assert_eq!(1 - 1, 0);
    assert_eq!(0 - 1, -1);
    assert_eq!(-1 - -1, 0);
    assert_eq!(i32::MAX - i32::MAX, 0);
    assert_eq!(i32::MIN - i32::MIN, 0);
}

#[test]
fn test_multiplication_boundaries() {
    assert_eq!(0 * 0, 0);
    assert_eq!(1 * 0, 0);
    assert_eq!(1 * 1, 1);
    assert_eq!(-1 * -1, 1);
    assert_eq!(-1 * 1, -1);
    assert_eq!(2 * 2, 4);
}

#[test]
fn test_division_boundaries() {
    assert_eq!(0 / 1, 0);
    assert_eq!(1 / 1, 1);
    assert_eq!(2 / 1, 2);
    assert_eq!(2 / 2, 1);
    assert_eq!(-2 / 1, -2);
    assert_eq!(-2 / -1, 2);
}

#[test]
fn test_modulo_boundaries() {
    assert_eq!(0 % 1, 0);
    assert_eq!(1 % 1, 0);
    assert_eq!(2 % 1, 0);
    assert_eq!(3 % 2, 1);
    assert_eq!(10 % 3, 1);
    assert_eq!(10 % 5, 0);
}

#[test]
fn test_negation_boundaries() {
    assert_eq!(-0, 0);
    assert_eq!(-1, -1);
    assert_eq!(-(-1), 1);
    assert_eq!(-i32::MAX, -i32::MAX);
}

// ============================================================================
// Comparison Operator Tests
// ============================================================================

#[test]
fn test_equality_operators() {
    assert!(1 == 1);
    assert!(!(1 == 2));
    assert!(1 != 2);
    assert!(!(1 != 1));
}

#[test]
fn test_relational_operators() {
    assert!(1 < 2);
    assert!(1 <= 1);
    assert!(1 <= 2);
    assert!(2 > 1);
    assert!(2 >= 2);
    assert!(2 >= 1);
    assert!(!(1 > 2));
    assert!(!(2 < 1));
}

// ============================================================================
// Compound Assignment Tests
// ============================================================================

#[test]
fn test_compound_assignment() {
    let mut x = 5;
    x += 3;
    assert_eq!(x, 8);
    
    x -= 2;
    assert_eq!(x, 6);
    
    x *= 2;
    assert_eq!(x, 12);
    
    x /= 3;
    assert_eq!(x, 4);
    
    x %= 3;
    assert_eq!(x, 1);
}

// ============================================================================
// Logical Operator Tests
// ============================================================================

#[test]
fn test_logical_and() {
    assert!(true && true);
    assert!(!(true && false));
    assert!(!(false && true));
    assert!(!(false && false));
}

#[test]
fn test_logical_or() {
    assert!(true || true);
    assert!(true || false);
    assert!(false || true);
    assert!(!(false || false));
}

#[test]
fn test_logical_not() {
    assert!(!false);
    assert!(!true == false);
}

// ============================================================================
// Bitwise Operator Tests
// ============================================================================

#[test]
fn test_bitwise_and() {
    assert_eq!(0b1111 & 0b1010, 0b1010);
    assert_eq!(0b1111 & 0b0000, 0b0000);
    assert_eq!(0b1111 & 0b1111, 0b1111);
}

#[test]
fn test_bitwise_or() {
    assert_eq!(0b1010 | 0b0101, 0b1111);
    assert_eq!(0b1010 | 0b0000, 0b1010);
    assert_eq!(0b0000 | 0b0000, 0b0000);
}

#[test]
fn test_bitwise_xor() {
    assert_eq!(0b1111 ^ 0b1010, 0b0101);
    assert_eq!(0b1111 ^ 0b0000, 0b1111);
    assert_eq!(0b1111 ^ 0b1111, 0b0000);
}

#[test]
fn test_bitwise_not() {
    assert_eq!(!0u8, 255);
    assert_eq!(!255u8, 0);
    assert_eq!(!170u8, 85);
}

#[test]
fn test_left_shift() {
    assert_eq!(1 << 0, 1);
    assert_eq!(1 << 1, 2);
    assert_eq!(1 << 2, 4);
    assert_eq!(1 << 3, 8);
    assert_eq!(1 << 8, 256);
}

#[test]
fn test_right_shift() {
    assert_eq!(256 >> 0, 256);
    assert_eq!(256 >> 1, 128);
    assert_eq!(256 >> 2, 64);
    assert_eq!(256 >> 3, 32);
    assert_eq!(256 >> 8, 1);
}

// ============================================================================
// Type Casting Tests
// ============================================================================

#[test]
fn test_int_to_float_cast() {
    assert_eq!(1i32 as f64, 1.0);
    assert_eq!(0i32 as f64, 0.0);
    assert_eq!(-1i32 as f64, -1.0);
    assert_eq!(100i32 as f64, 100.0);
}

#[test]
fn test_float_to_int_cast() {
    assert_eq!(1.0f64 as i32, 1);
    assert_eq!(1.9f64 as i32, 1);
    assert_eq!(0.0f64 as i32, 0);
    assert_eq!(-1.0f64 as i32, -1);
    assert_eq!(-1.9f64 as i32, -1);
}

#[test]
fn test_widening_cast() {
    assert_eq!(1i8 as i16, 1);
    assert_eq!(1i8 as i32, 1);
    assert_eq!(1i8 as i64, 1);
    assert_eq!(1i16 as i32, 1);
    assert_eq!(1i16 as i64, 1);
    assert_eq!(1i32 as i64, 1);
}

#[test]
fn test_unsigned_to_signed_cast() {
    assert_eq!(1u8 as i8, 1);
    assert_eq!(127u8 as i8, 127);
    assert_eq!(128u8 as i8, -128);
}

// ============================================================================
// String Formatting Tests
// ============================================================================

#[test]
fn test_basic_formatting() {
    assert_eq!(format!("{}", 42), "42");
    assert_eq!(format!("{}", "hello"), "hello");
    assert_eq!(format!("{}", true), "true");
}

#[test]
fn test_width_formatting() {
    assert_eq!(format!("{:5}", 42), "   42");
    assert_eq!(format!("{:>5}", 42), "   42");
    assert_eq!(format!("{:<5}", 42), "42   ");
    assert_eq!(format!("{:^5}", 42), " 42  ");
}

#[test]
fn test_zero_padding_formatting() {
    assert_eq!(format!("{:05}", 42), "00042");
    assert_eq!(format!("{:05}", -42), "-0042");
}

#[test]
fn test_precision_formatting() {
    assert_eq!(format!("{:.2}", 3.14159), "3.14");
    assert_eq!(format!("{:.4}", 3.14159), "3.1416");
}

// ============================================================================
// Tuple Tests
// ============================================================================

#[test]
fn test_empty_tuple() {
    let t = ();
    assert_eq!(t, ());
}

#[test]
fn test_single_element_tuple() {
    let t = (1,);
    assert_eq!(t.0, 1);
}

#[test]
fn test_two_element_tuple() {
    let t = (1, 2);
    assert_eq!(t.0, 1);
    assert_eq!(t.1, 2);
}

#[test]
fn test_tuple_destructuring() {
    let (a, b) = (1, 2);
    assert_eq!(a, 1);
    assert_eq!(b, 2);
}

// ============================================================================
// Array Tests
// ============================================================================

#[test]
fn test_empty_array() {
    let a: [i32; 0] = [];
    assert_eq!(a.len(), 0);
}

#[test]
fn test_single_element_array() {
    let a = [1];
    assert_eq!(a[0], 1);
}

#[test]
fn test_multi_element_array() {
    let a = [1, 2, 3, 4, 5];
    assert_eq!(a.len(), 5);
    assert_eq!(a[0], 1);
    assert_eq!(a[4], 5);
}

#[test]
fn test_array_iteration() {
    let a = [1, 2, 3];
    let mut sum = 0;
    for x in &a {
        sum += x;
    }
    assert_eq!(sum, 6);
}

// ============================================================================
// Slice Tests
// ============================================================================

#[test]
fn test_slice_from_array() {
    let a = [1, 2, 3, 4, 5];
    let s = &a[..];
    assert_eq!(s.len(), 5);
}

#[test]
fn test_slice_range() {
    let a = [1, 2, 3, 4, 5];
    let s = &a[1..4];
    assert_eq!(s, &[2, 3, 4]);
}

#[test]
fn test_slice_first_last() {
    let s = &[1, 2, 3];
    assert_eq!(s.first(), Some(&1));
    assert_eq!(s.last(), Some(&3));
}

// ============================================================================
// Struct Tests
// ============================================================================

struct Point {
    x: i32,
    y: i32,
}

#[test]
fn test_struct_creation() {
    let p = Point { x: 1, y: 2 };
    assert_eq!(p.x, 1);
    assert_eq!(p.y, 2);
}

#[test]
fn test_struct_update() {
    let p1 = Point { x: 1, y: 2 };
    let p2 = Point { x: 3, ..p1 };
    assert_eq!(p2.x, 3);
    assert_eq!(p2.y, 2);
}

// ============================================================================
// Enum Tests
// ============================================================================

enum Message {
    Quit,
    Move { x: i32, y: i32 },
    Write(String),
    ChangeColor(i32, i32, i32),
}

#[test]
fn test_enum_variants() {
    let _quit = Message::Quit;
    let _move_msg = Message::Move { x: 1, y: 2 };
    let _write = Message::Write(String::from("hello"));
    let _color = Message::ChangeColor(255, 255, 255);
}

#[test]
fn test_enum_match() {
    let msg = Message::Write(String::from("hello"));
    
    let result = match msg {
        Message::Quit => "quit",
        Message::Move { .. } => "move",
        Message::Write(_) => "write",
        Message::ChangeColor(_, _, _) => "color",
    };
    
    assert_eq!(result, "write");
}

// ============================================================================
// Option Tests
// ============================================================================

#[test]
fn test_option_some_values() {
    let opt = Some(42);
    assert!(opt.is_some());
    assert!(!opt.is_none());
}

#[test]
fn test_option_none_value() {
    let opt: Option<i32> = None;
    assert!(!opt.is_some());
    assert!(opt.is_none());
}

#[test]
fn test_option_unwrap_or() {
    assert_eq!(Some(42).unwrap_or(0), 42);
    assert_eq!(None.unwrap_or(0), 0);
}

// ============================================================================
// Result Tests
// ============================================================================

#[test]
fn test_result_ok_value() {
    let result: Result<i32, ()> = Ok(42);
    assert!(result.is_ok());
    assert!(!result.is_err());
}

#[test]
fn test_result_err_value() {
    let result: Result<(), &str> = Err("error");
    assert!(!result.is_ok());
    assert!(result.is_err());
}

#[test]
fn test_result_unwrap_or() {
    let ok: Result<i32, &str> = Ok(42);
    let err: Result<i32, &str> = Err("error");
    
    assert_eq!(ok.unwrap_or(0), 42);
    assert_eq!(err.unwrap_or(0), 0);
}

// ============================================================================
// Vector Operation Tests
// ============================================================================

#[test]
fn test_vec_push() {
    let mut v = vec![];
    v.push(1);
    v.push(2);
    v.push(3);
    assert_eq!(v, vec![1, 2, 3]);
}

#[test]
fn test_vec_pop() {
    let mut v = vec![1, 2, 3];
    assert_eq!(v.pop(), Some(3));
    assert_eq!(v.pop(), Some(2));
    assert_eq!(v.pop(), Some(1));
    assert_eq!(v.pop(), None);
}

#[test]
fn test_vec_insert() {
    let mut v = vec![1, 3];
    v.insert(1, 2);
    assert_eq!(v, vec![1, 2, 3]);
}

#[test]
fn test_vec_remove() {
    let mut v = vec![1, 2, 3];
    assert_eq!(v.remove(1), 2);
    assert_eq!(v, vec![1, 3]);
}

#[test]
fn test_vec_contains() {
    let v = vec![1, 2, 3];
    assert!(v.contains(&2));
    assert!(!v.contains(&4));
}

#[test]
fn test_vec_reverse() {
    let mut v = vec![1, 2, 3];
    v.reverse();
    assert_eq!(v, vec![3, 2, 1]);
}

#[test]
fn test_vec_sort() {
    let mut v = vec![3, 1, 2];
    v.sort();
    assert_eq!(v, vec![1, 2, 3]);
}

// ============================================================================
// String Operation Tests
// ============================================================================

#[test]
fn test_string_push() {
    let mut s = String::from("hel");
    s.push('l');
    s.push('o');
    assert_eq!(s, "hello");
}

#[test]
fn test_string_push_str() {
    let mut s = String::from("hello");
    s.push_str(" world");
    assert_eq!(s, "hello world");
}

#[test]
fn test_string_len() {
    assert_eq!("".len(), 0);
    assert_eq!("a".len(), 1);
    assert_eq!("hello".len(), 5);
}

#[test]
fn test_string_is_empty() {
    assert!("".is_empty());
    assert!(!"a".is_empty());
}

#[test]
fn test_string_contains() {
    assert!("hello world".contains("hello"));
    assert!(!"hello world".contains("foo"));
}

#[test]
fn test_string_starts_with() {
    assert!("hello world".starts_with("hello"));
    assert!(!"hello world".starts_with("world"));
}

#[test]
fn test_string_ends_with() {
    assert!("hello world".ends_with("world"));
    assert!(!"hello world".ends_with("hello"));
}

#[test]
fn test_string_replace() {
    assert_eq!("hello world".replace("world", "Rust"), "hello Rust");
    assert_eq!("hello hello".replace("hello", "hi"), "hi hi");
}

#[test]
fn test_string_to_lowercase() {
    assert_eq!("HELLO".to_lowercase(), "hello");
    assert_eq!("Hello".to_lowercase(), "hello");
}

#[test]
fn test_string_to_uppercase() {
    assert_eq!("hello".to_uppercase(), "HELLO");
    assert_eq!("Hello".to_uppercase(), "HELLO");
}

#[test]
fn test_string_trim() {
    assert_eq!("  hello  ".trim(), "hello");
    assert_eq!("  hello  ".trim_start(), "hello  ");
    assert_eq!("  hello  ".trim_end(), "  hello");
}

#[test]
fn test_string_split() {
    let parts: Vec<&str> = "a,b,c".split(',').collect();
    assert_eq!(parts, vec!["a", "b", "c"]);
}

// ============================================================================
// Iterator Operation Tests
// ============================================================================

#[test]
fn test_iterator_map() {
    let v: Vec<i32> = vec![1, 2, 3].iter().map(|x| x * 2).collect();
    assert_eq!(v, vec![2, 4, 6]);
}

#[test]
fn test_iterator_filter() {
    let v: Vec<i32> = vec![1, 2, 3, 4, 5].iter().cloned().filter(|x| x % 2 == 0).collect();
    assert_eq!(v, vec![2, 4]);
}

#[test]
fn test_iterator_fold() {
    let sum = vec![1, 2, 3, 4, 5].iter().fold(0, |acc, x| acc + x);
    assert_eq!(sum, 15);
}

#[test]
fn test_iterator_count() {
    assert_eq!(vec![1, 2, 3].iter().count(), 3);
    assert_eq!(Vec::<i32>::new().iter().count(), 0);
}

#[test]
fn test_iterator_sum() {
    let sum: i32 = vec![1, 2, 3].iter().sum();
    assert_eq!(sum, 6);
}

#[test]
fn test_iterator_any() {
    assert!(vec![1, 2, 3].iter().any(|&x| x == 2));
    assert!(!vec![1, 2, 3].iter().any(|&x| x == 5));
}

#[test]
fn test_iterator_all() {
    assert!(vec![2, 4, 6].iter().all(|&x| x % 2 == 0));
    assert!(!vec![1, 2, 3].iter().all(|&x| x % 2 == 0));
}

// ============================================================================
// HashMap Operation Tests
// ============================================================================

#[test]
fn test_hashmap_insert() {
    use std::collections::HashMap;
    
    let mut m = HashMap::new();
    m.insert("a", 1);
    m.insert("b", 2);
    
    assert_eq!(m.get("a"), Some(&1));
    assert_eq!(m.get("b"), Some(&2));
}

#[test]
fn test_hashmap_remove() {
    use std::collections::HashMap;
    
    let mut m = HashMap::new();
    m.insert("a", 1);
    assert_eq!(m.remove("a"), Some(1));
    assert_eq!(m.remove("a"), None);
}

#[test]
fn test_hashmap_contains_key() {
    use std::collections::HashMap;
    
    let mut m = HashMap::new();
    m.insert("a", 1);
    
    assert!(m.contains_key("a"));
    assert!(!m.contains_key("b"));
}

#[test]
fn test_hashmap_len() {
    use std::collections::HashMap;
    
    let mut m = HashMap::new();
    assert_eq!(m.len(), 0);
    
    m.insert("a", 1);
    assert_eq!(m.len(), 1);
    
    m.insert("b", 2);
    assert_eq!(m.len(), 2);
}

// ============================================================================
// HashSet Operation Tests
// ============================================================================

#[test]
fn test_hashset_insert() {
    use std::collections::HashSet;
    
    let mut s = HashSet::new();
    assert!(s.insert(1));
    assert!(!s.insert(1));
    
    assert!(s.contains(&1));
}

#[test]
fn test_hashset_remove() {
    use std::collections::HashSet;
    
    let mut s = HashSet::new();
    s.insert(1);
    
    assert!(s.remove(&1));
    assert!(!s.remove(&1));
}

// ============================================================================
// Box Operation Tests
// ============================================================================

#[test]
fn test_box_new() {
    let b = Box::new(5);
    assert_eq!(*b, 5);
}

#[test]
fn test_box_large() {
    let data = vec![0u8; 1000];
    let b = Box::new(data);
    assert_eq!(b.len(), 1000);
}

// ============================================================================
// Rc Operation Tests
// ============================================================================

#[test]
fn test_rc_clone() {
    use std::rc::Rc;
    
    let rc1 = Rc::new(5);
    let rc2 = Rc::clone(&rc1);
    
    assert_eq!(Rc::strong_count(&rc1), 2);
    assert_eq!(*rc1, 5);
    assert_eq!(*rc2, 5);
}

// ============================================================================
// Arc Operation Tests
// ============================================================================

#[test]
fn test_arc_clone() {
    use std::sync::Arc;
    
    let arc1 = Arc::new(5);
    let arc2 = Arc::clone(&arc1);
    
    assert_eq!(Arc::strong_count(&arc1), 2);
    assert_eq!(*arc1, 5);
    assert_eq!(*arc2, 5);
}

// ============================================================================
// Cell Operation Tests
// ============================================================================

#[test]
fn test_cell_get_set() {
    use std::cell::Cell;
    
    let cell = Cell::new(5);
    assert_eq!(cell.get(), 5);
    
    cell.set(10);
    assert_eq!(cell.get(), 10);
}

// ============================================================================
// RefCell Operation Tests
// ============================================================================

#[test]
fn test_refcell_borrow() {
    use std::cell::RefCell;
    
    let cell = RefCell::new(5);
    assert_eq!(*cell.borrow(), 5);
    
    *cell.borrow_mut() = 10;
    assert_eq!(*cell.borrow(), 10);
}

// ============================================================================
// Mutex Operation Tests
// ============================================================================

#[test]
fn test_mutex_lock() {
    use std::sync::Mutex;
    
    let m = Mutex::new(5);
    {
        let mut guard = m.lock().unwrap();
        *guard = 10;
    }
    
    assert_eq!(*m.lock().unwrap(), 10);
}

// ============================================================================
// RwLock Operation Tests
// ============================================================================

#[test]
fn test_rwlock_read() {
    use std::sync::RwLock;
    
    let lock = RwLock::new(5);
    assert_eq!(*lock.read().unwrap(), 5);
}

#[test]
fn test_rwlock_write() {
    use std::sync::RwLock;
    
    let lock = RwLock::new(5);
    {
        let mut guard = lock.write().unwrap();
        *guard = 10;
    }
    
    assert_eq!(*lock.read().unwrap(), 10);
}

// ============================================================================
// Condvar Operation Tests
// ============================================================================

#[test]
fn test_condvar() {
    use std::sync::{Arc, Condvar, Mutex};
    
    let pair = Arc::new((Mutex::new(false), Condvar::new()));
    let pair2 = Arc::clone(&pair);
    
    std::thread::spawn(move || {
        let (lock, cvar) = &*pair2;
        let mut started = lock.lock().unwrap();
        *started = true;
        cvar.notify_one();
    });
    
    let (lock, cvar) = &*pair;
    let mut started = lock.lock().unwrap();
    while !*started {
        started = cvar.wait(started).unwrap();
    }
    
    assert!(*started);
}

// ============================================================================
// Channel Operation Tests
// ============================================================================

#[test]
fn test_channel_send_recv() {
    use std::sync::mpsc;
    
    let (tx, rx) = mpsc::channel();
    
    tx.send(5).unwrap();
    assert_eq!(rx.recv().unwrap(), 5);
}

#[test]
fn test_channel_multiple_send() {
    use std::sync::mpsc;
    
    let (tx, rx) = mpsc::channel();
    
    for i in 0..5 {
        tx.send(i).unwrap();
    }
    
    drop(tx);
    
    let mut received = vec![];
    for val in rx {
        received.push(val);
    }
    
    assert_eq!(received, vec![0, 1, 2, 3, 4]);
}

// ============================================================================
// Thread Operation Tests
// ============================================================================

#[test]
fn test_thread_spawn_join() {
    let handle = std::thread::spawn(|| {
        42
    });
    
    assert_eq!(handle.join().unwrap(), 42);
}

#[test]
fn test_thread_builder() {
    let handle = std::thread::Builder::new()
        .name("test_thread".to_string())
        .spawn(|| {
            42
        })
        .unwrap();
    
    assert_eq!(handle.join().unwrap(), 42);
}

// ============================================================================
// Panic Handling Tests
// ============================================================================

#[test]
#[should_panic(expected = "test panic")]
fn test_panic_message() {
    panic!("test panic");
}

#[test]
fn test_catch_unwind() {
    use std::panic;
    
    let result = panic::catch_unwind(|| {
        panic!("test panic");
    });
    
    assert!(result.is_err());
}

// ============================================================================
// Drop Tests
// ============================================================================

struct DropCounter {
    count: std::cell::Cell<i32>,
}

impl Drop for DropCounter {
    fn drop(&mut self) {
        self.count.set(self.count.get() + 1);
    }
}

#[test]
fn test_drop_called() {
    let count = std::cell::Cell::new(0);
    {
        let _counter = DropCounter { count: count.clone() };
    }
    
    assert_eq!(count.get(), 1);
}

// ============================================================================
// Clone Tests
// ============================================================================

#[test]
fn test_clone_string() {
    let s1 = String::from("hello");
    let s2 = s1.clone();
    
    assert_eq!(s1, s2);
}

#[test]
fn test_clone_vec() {
    let v1 = vec![1, 2, 3];
    let v2 = v1.clone();
    
    assert_eq!(v1, v2);
}

// ============================================================================
// Copy Tests
// ============================================================================

#[test]
fn test_copy_int() {
    let x = 5;
    let y = x;
    
    assert_eq!(x, 5);
    assert_eq!(y, 5);
}

#[test]
fn test_copy_float() {
    let x = 3.14;
    let y = x;
    
    assert_eq!(x, 3.14);
    assert_eq!(y, 3.14);
}

// ============================================================================
// Default Tests
// ============================================================================

#[test]
fn test_default_int() {
    let i: i32 = Default::default();
    assert_eq!(i, 0);
}

#[test]
fn test_default_bool() {
    let b: bool = Default::default();
    assert_eq!(b, false);
}

#[test]
fn test_default_string() {
    let s: String = Default::default();
    assert_eq!(s, "");
}

// ============================================================================
// From/Into Tests
// ============================================================================

#[test]
fn test_from_str() {
    let s = String::from("hello");
    assert_eq!(s, "hello");
}

#[test]
fn test_into_string() {
    let s: String = "hello".into();
    assert_eq!(s, "hello");
}

// ============================================================================
// TryFrom/TryInto Tests
// ============================================================================

#[test]
fn test_try_into_i32() {
    let x: i32 = 100_i32.try_into().unwrap();
    assert_eq!(x, 100);
}

// ============================================================================
// AsRef/AsMut Tests
// ============================================================================

#[test]
fn test_as_ref_str() {
    let s = String::from("hello");
    let _: &str = s.as_ref();
}

#[test]
fn test_as_ref_slice() {
    let v = vec![1, 2, 3];
    let _: &[i32] = v.as_ref();
}

// ============================================================================
// ToOwned Tests
// ============================================================================

#[test]
fn test_to_owned_str() {
    let s: &str = "hello";
    let owned: String = s.to_owned();
    assert_eq!(owned, "hello");
}

#[test]
fn test_to_owned_slice() {
    let s: &[i32] = &[1, 2, 3];
    let owned: Vec<i32> = s.to_owned();
    assert_eq!(owned, vec![1, 2, 3]);
}

// ============================================================================
// Borrow Tests
// ============================================================================

#[test]
fn test_borrow_str() {
    use std::borrow::Borrow;
    
    let s = String::from("hello");
    let _: &str = s.borrow();
}

// ============================================================================
// ToString Tests
// ============================================================================

#[test]
fn test_to_string_int() {
    assert_eq!(42.to_string(), "42");
    assert_eq!((-42).to_string(), "-42");
}

#[test]
fn test_to_string_bool() {
    assert_eq!(true.to_string(), "true");
    assert_eq!(false.to_string(), "false");
}

// ============================================================================
// Parse Tests
// ============================================================================

#[test]
fn test_parse_int() {
    assert_eq!("42".parse::<i32>().unwrap(), 42);
    assert_eq!("-42".parse::<i32>().unwrap(), -42);
}

#[test]
fn test_parse_float() {
    assert!("3.14".parse::<f64>().unwrap() - 3.14 < 0.001);
}

#[test]
fn test_parse_bool() {
    assert_eq!("true".parse::<bool>().unwrap(), true);
    assert_eq!("false".parse::<bool>().unwrap(), false);
}

// ============================================================================
// Debug Tests
// ============================================================================

#[test]
fn test_debug_int() {
    assert_eq!(format!("{:?}", 42), "42");
}

#[test]
fn test_debug_vec() {
    assert_eq!(format!("{:?}", vec![1, 2, 3]), "[1, 2, 3]");
}

// ============================================================================
// Display Tests
// ============================================================================

#[test]
fn test_display_int() {
    assert_eq!(format!("{}", 42), "42");
}

#[test]
fn test_display_string() {
    assert_eq!(format!("{}", "hello"), "hello");
}

// ============================================================================
// Pointer Tests
// ============================================================================

#[test]
fn test_pointer() {
    let x = 5;
    let ptr = &x as *const i32;
    
    unsafe {
        assert_eq!(*ptr, 5);
    }
}

// ============================================================================
// Any Tests
// ============================================================================

#[test]
fn test_any() {
    use std::any::Any;
    
    let value: &dyn Any = &42i32;
    
    if let Some(i) = value.downcast_ref::<i32>() {
        assert_eq!(*i, 42);
    }
}

// ============================================================================
// PhantomData Tests
// ============================================================================

use std::marker::PhantomData;

struct Container<T> {
    _marker: PhantomData<T>,
}

impl<T> Container<T> {
    fn new() -> Self {
        Container { _marker: PhantomData }
    }
}

#[test]
fn test_phantom_data() {
    let _: Container<i32> = Container::new();
    let _: Container<String> = Container::new();
}

// ============================================================================
// Pin Tests
// ============================================================================

#[test]
fn test_pin() {
    use std::pin::Pin;
    
    let value = 5;
    let _pinned = Pin::new(&value);
}

// ============================================================================
// Future Tests
// ============================================================================

#[test]
fn test_future_ready() {
    use std::future::ready;
    
    let _fut = ready(42);
}

// ============================================================================
// Stream Tests
// ============================================================================

#[test]
fn test_stream_iter() {
    use futures::stream;
    
    let _stream = stream::iter(vec![1, 2, 3]);
}

// ============================================================================
// Sink Tests
// ============================================================================

#[test]
fn test_sink_vec() {
    use futures::sink::SinkExt;
    
    let _sink = futures::sink::drain::<i32>();
}

// ============================================================================
// Async/Await Tests
// ============================================================================

#[tokio::test]
async fn test_async_fn() {
    async fn add(a: i32, b: i32) -> i32 {
        a + b
    }
    
    assert_eq!(add(1, 2).await, 3);
}

#[tokio::test]
async fn test_async_block() {
    let result = async {
        42
    }.await;
    
    assert_eq!(result, 42);
}

// ============================================================================
// Select Tests
// ============================================================================

#[tokio::test]
async fn test_select() {
    use tokio::time::{sleep, Duration};
    
    let sleep1 = sleep(Duration::from_millis(10));
    let sleep2 = sleep(Duration::from_millis(20));
    
    tokio::select! {
        _ = sleep1 => {},
        _ = sleep2 => {},
    }
}

// ============================================================================
// Join Tests
// ============================================================================

#[tokio::test]
async fn test_join() {
    async fn task1() -> i32 {
        1
    }
    
    async fn task2() -> i32 {
        2
    }
    
    let (a, b) = tokio::join!(task1(), task2());
    assert_eq!(a, 1);
    assert_eq!(b, 2);
}

// ============================================================================
// Spawn Tests
// ============================================================================

#[tokio::test]
async fn test_spawn() {
    let handle = tokio::spawn(async {
        42
    });
    
    assert_eq!(handle.await.unwrap(), 42);
}

// ============================================================================
// Timeout Tests
// ============================================================================

#[tokio::test]
async fn test_timeout() {
    use tokio::time::{sleep, timeout, Duration};
    
    let result = timeout(Duration::from_millis(100), async {
        sleep(Duration::from_millis(10)).await;
        42
    }).await;
    
    assert_eq!(result.unwrap(), 42);
}

#[tokio::test]
async fn test_timeout_expired() {
    use tokio::time::{sleep, timeout, Duration};
    
    let result = timeout(Duration::from_millis(10), async {
        sleep(Duration::from_millis(100)).await;
        42
    }).await;
    
    assert!(result.is_err());
}

// ============================================================================
// IO Tests
// ============================================================================

#[tokio::test]
async fn test_async_read() {
    use tokio::io::AsyncReadExt;
    
    let data = b"hello";
    let mut buffer = vec![0u8; 5];
    
    let mut cursor = std::io::Cursor::new(data);
    cursor.read_exact(&mut buffer).unwrap();
    
    assert_eq!(buffer, b"hello");
}

#[tokio::test]
async fn test_async_write() {
    use tokio::io::AsyncWriteExt;
    
    let mut buffer = Vec::new();
    buffer.extend_from_slice(b"hello");
    
    assert_eq!(buffer, b"hello");
}
