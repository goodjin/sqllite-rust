//! Comprehensive Boundary Tests
//!
//! Additional comprehensive boundary condition tests

// ============================================================================
// Boolean Logic Tests
// ============================================================================

#[test]
fn test_boolean_logic() {
    // AND
    assert_eq!(true && true, true);
    assert_eq!(true && false, false);
    assert_eq!(false && true, false);
    assert_eq!(false && false, false);
    
    // OR
    assert_eq!(true || true, true);
    assert_eq!(true || false, true);
    assert_eq!(false || true, true);
    assert_eq!(false || false, false);
    
    // NOT
    assert_eq!(!true, false);
    assert_eq!(!false, true);
}

#[test]
fn test_boolean_short_circuit() {
    // Short circuit AND
    let result = false && panic!("should not reach");
    assert!(!result);
    
    // Short circuit OR
    let result = true || panic!("should not reach");
    assert!(result);
}

// ============================================================================
// Bitwise Operation Tests
// ============================================================================

#[test]
fn test_bitwise_and() {
    assert_eq!(0b1111 & 0b1010, 0b1010);
    assert_eq!(0xFF & 0x00, 0x00);
    assert_eq!(0xFF & 0xFF, 0xFF);
}

#[test]
fn test_bitwise_or() {
    assert_eq!(0b1111 | 0b1010, 0b1111);
    assert_eq!(0xFF | 0x00, 0xFF);
    assert_eq!(0x00 | 0x00, 0x00);
}

#[test]
fn test_bitwise_xor() {
    assert_eq!(0b1111 ^ 0b1010, 0b0101);
    assert_eq!(0xFF ^ 0xFF, 0x00);
    assert_eq!(0x00 ^ 0x00, 0x00);
}

#[test]
fn test_bitwise_not() {
    assert_eq!(!0u8, 255);
    assert_eq!(!255u8, 0);
}

#[test]
fn test_bit_shifts() {
    assert_eq!(1 << 0, 1);
    assert_eq!(1 << 1, 2);
    assert_eq!(1 << 8, 256);
    assert_eq!(256 >> 8, 1);
    assert_eq!(256 >> 0, 256);
}

// ============================================================================
// Option Tests
// ============================================================================

#[test]
fn test_option_some() {
    let opt = Some(42);
    assert!(opt.is_some());
    assert!(!opt.is_none());
    assert_eq!(opt.unwrap(), 42);
}

#[test]
fn test_option_none() {
    let opt: Option<i32> = None;
    assert!(!opt.is_some());
    assert!(opt.is_none());
}

#[test]
fn test_option_unwrap_or() {
    let some = Some(42);
    let none: Option<i32> = None;
    
    assert_eq!(some.unwrap_or(0), 42);
    assert_eq!(none.unwrap_or(0), 0);
}

#[test]
fn test_option_map() {
    let some = Some(5);
    let doubled = some.map(|x| x * 2);
    assert_eq!(doubled, Some(10));
}

// ============================================================================
// Result Tests
// ============================================================================

#[test]
fn test_result_ok() {
    let result: Result<i32, ()> = Ok(42);
    assert!(result.is_ok());
    assert!(!result.is_err());
    assert_eq!(result.unwrap(), 42);
}

#[test]
fn test_result_err() {
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

#[test]
fn test_result_map() {
    let ok: Result<i32, ()> = Ok(5);
    let doubled = ok.map(|x| x * 2);
    assert_eq!(doubled, Ok(10));
}

// ============================================================================
// Iterator Tests
// ============================================================================

#[test]
fn test_iterator_empty() {
    let v: Vec<i32> = vec![];
    let sum: i32 = v.iter().sum();
    assert_eq!(sum, 0);
}

#[test]
fn test_iterator_single() {
    let v = vec![42];
    let sum: i32 = v.iter().sum();
    assert_eq!(sum, 42);
}

#[test]
fn test_iterator_many() {
    let v: Vec<i32> = (0..100).collect();
    let sum: i32 = v.iter().sum();
    assert_eq!(sum, 4950);
}

#[test]
fn test_iterator_map() {
    let v = vec![1, 2, 3];
    let doubled: Vec<i32> = v.iter().map(|x| x * 2).collect();
    assert_eq!(doubled, vec![2, 4, 6]);
}

#[test]
fn test_iterator_filter() {
    let v = vec![1, 2, 3, 4, 5];
    let evens: Vec<i32> = v.iter().cloned().filter(|x| x % 2 == 0).collect();
    assert_eq!(evens, vec![2, 4]);
}

#[test]
fn test_iterator_fold() {
    let v = vec![1, 2, 3, 4, 5];
    let sum = v.iter().fold(0, |acc, x| acc + x);
    assert_eq!(sum, 15);
}

#[test]
fn test_iterator_zip() {
    let a = vec![1, 2, 3];
    let b = vec![4, 5, 6];
    let zipped: Vec<(i32, i32)> = a.iter().cloned().zip(b.iter().cloned()).collect();
    assert_eq!(zipped, vec![(1, 4), (2, 5), (3, 6)]);
}

#[test]
fn test_iterator_chain() {
    let a = vec![1, 2, 3];
    let b = vec![4, 5, 6];
    let chained: Vec<i32> = a.iter().cloned().chain(b.iter().cloned()).collect();
    assert_eq!(chained, vec![1, 2, 3, 4, 5, 6]);
}

#[test]
fn test_iterator_take() {
    let v: Vec<i32> = (0..100).collect();
    let first_10: Vec<i32> = v.iter().cloned().take(10).collect();
    assert_eq!(first_10, vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
}

#[test]
fn test_iterator_skip() {
    let v: Vec<i32> = (0..10).collect();
    let skipped: Vec<i32> = v.iter().cloned().skip(5).collect();
    assert_eq!(skipped, vec![5, 6, 7, 8, 9]);
}

#[test]
fn test_iterator_enumerate() {
    let v = vec!['a', 'b', 'c'];
    let enumerated: Vec<(usize, char)> = v.iter().cloned().enumerate().collect();
    assert_eq!(enumerated, vec![(0, 'a'), (1, 'b'), (2, 'c')]);
}

#[test]
fn test_iterator_rev() {
    let v = vec![1, 2, 3];
    let reversed: Vec<i32> = v.iter().cloned().rev().collect();
    assert_eq!(reversed, vec![3, 2, 1]);
}

// ============================================================================
// Range Tests
// ============================================================================

#[test]
fn test_range_exclusive() {
    let r = 0..5;
    let v: Vec<i32> = r.collect();
    assert_eq!(v, vec![0, 1, 2, 3, 4]);
}

#[test]
fn test_range_inclusive() {
    let r = 0..=5;
    let v: Vec<i32> = r.collect();
    assert_eq!(v, vec![0, 1, 2, 3, 4, 5]);
}

#[test]
fn test_range_empty() {
    let r = 0..0;
    let v: Vec<i32> = r.collect();
    assert!(v.is_empty());
}

#[test]
fn test_range_single() {
    let r = 0..1;
    let v: Vec<i32> = r.collect();
    assert_eq!(v, vec![0]);
}

// ============================================================================
// Pattern Matching Tests
// ============================================================================

#[test]
fn test_match_exhaustive() {
    let x = 5;
    
    let result = match x {
        0 => "zero",
        1 => "one",
        2 => "two",
        3..=10 => "three to ten",
        _ => "other",
    };
    
    assert_eq!(result, "three to ten");
}

#[test]
fn test_match_guard() {
    let x = 5;
    
    let result = match x {
        n if n < 0 => "negative",
        n if n > 0 => "positive",
        _ => "zero",
    };
    
    assert_eq!(result, "positive");
}

#[test]
fn test_match_destructuring() {
    let pair = (1, 2);
    
    let result = match pair {
        (0, 0) => "origin",
        (x, 0) => &format!("on x-axis at {}", x),
        (0, y) => &format!("on y-axis at {}", y),
        (x, y) => &format!("at ({}, {})", x, y),
    };
    
    assert_eq!(result, "at (1, 2)");
}

// ============================================================================
// Closure Tests
// ============================================================================

#[test]
fn test_closure_basic() {
    let add = |a, b| a + b;
    assert_eq!(add(1, 2), 3);
}

#[test]
fn test_closure_capture() {
    let x = 5;
    let add_x = |y| x + y;
    assert_eq!(add_x(3), 8);
}

#[test]
fn test_closure_mut_capture() {
    let mut x = 5;
    let mut increment = || {
        x += 1;
        x
    };
    assert_eq!(increment(), 6);
    assert_eq!(increment(), 7);
}

// ============================================================================
// Trait Tests
// ============================================================================

#[test]
fn test_clone_trait() {
    let s = String::from("hello");
    let cloned = s.clone();
    assert_eq!(s, cloned);
}

#[test]
fn test_copy_trait() {
    let x = 5;
    let y = x;
    assert_eq!(x, y); // x is still valid
}

#[test]
fn test_default_trait() {
    let i: i32 = Default::default();
    assert_eq!(i, 0);
    
    let s: String = Default::default();
    assert!(s.is_empty());
}

#[test]
fn test_display_trait() {
    let s = format!("{}", 42);
    assert_eq!(s, "42");
}

#[test]
fn test_debug_trait() {
    let s = format!("{:?}", vec![1, 2, 3]);
    assert!(s.contains('['));
}

#[test]
fn test_eq_trait() {
    assert_eq!(1, 1);
    assert_ne!(1, 2);
}

#[test]
fn test_ord_trait() {
    assert!(1 < 2);
    assert!(2 > 1);
    assert!(1 <= 1);
    assert!(1 >= 1);
}

// ============================================================================
// Generic Tests
// ============================================================================

fn generic_identity<T>(x: T) -> T {
    x
}

#[test]
fn test_generic_identity() {
    assert_eq!(generic_identity(5), 5);
    assert_eq!(generic_identity("hello"), "hello");
    assert_eq!(generic_identity(vec![1, 2, 3]), vec![1, 2, 3]);
}

fn generic_max<T: Ord>(a: T, b: T) -> T {
    if a > b { a } else { b }
}

#[test]
fn test_generic_max() {
    assert_eq!(generic_max(1, 2), 2);
    assert_eq!(generic_max("a", "b"), "b");
}

// ============================================================================
// Lifetime Tests
// ============================================================================

fn longest<'a>(x: &'a str, y: &'a str) -> &'a str {
    if x.len() > y.len() { x } else { y }
}

#[test]
fn test_lifetime() {
    let string1 = String::from("long string is long");
    {
        let string2 = String::from("xyz");
        let result = longest(&string1, &string2);
        assert_eq!(result, string1);
    }
}

// ============================================================================
// Smart Pointer Tests
// ============================================================================

#[test]
fn test_box() {
    let b = Box::new(5);
    assert_eq!(*b, 5);
}

#[test]
fn test_box_large() {
    let large = vec![0u8; 1000000];
    let b = Box::new(large);
    assert_eq!(b.len(), 1000000);
}

#[test]
fn test_rc() {
    use std::rc::Rc;
    
    let rc = Rc::new(5);
    let rc2 = Rc::clone(&rc);
    
    assert_eq!(Rc::strong_count(&rc), 2);
    assert_eq!(*rc, 5);
    assert_eq!(*rc2, 5);
}

#[test]
fn test_arc() {
    use std::sync::Arc;
    
    let arc = Arc::new(5);
    let arc2 = Arc::clone(&arc);
    
    assert_eq!(Arc::strong_count(&arc), 2);
    assert_eq!(*arc, 5);
    assert_eq!(*arc2, 5);
}

#[test]
fn test_refcell() {
    use std::cell::RefCell;
    
    let cell = RefCell::new(5);
    
    {
        let mut x = cell.borrow_mut();
        *x = 10;
    }
    
    assert_eq!(*cell.borrow(), 10);
}

#[test]
fn test_cell() {
    use std::cell::Cell;
    
    let cell = Cell::new(5);
    cell.set(10);
    assert_eq!(cell.get(), 10);
}

// ============================================================================
// Macro Tests
// ============================================================================

#[test]
fn test_vec_macro() {
    let v = vec![1, 2, 3];
    assert_eq!(v, vec![1, 2, 3]);
}

#[test]
fn test_vec_repeat_macro() {
    let v = vec![0; 5];
    assert_eq!(v, vec![0, 0, 0, 0, 0]);
}

#[test]
fn test_string_macro() {
    let s = String::from("hello");
    assert_eq!(s, "hello");
}

#[test]
fn test_format_macro() {
    let s = format!("Hello, {}!", "world");
    assert_eq!(s, "Hello, world!");
}

#[test]
fn test_assert_macro() {
    assert!(true);
    assert_eq!(1, 1);
    assert_ne!(1, 2);
}
