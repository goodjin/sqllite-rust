//! Numeric Boundary Tests
//!
//! Tests for numeric edge cases and boundary conditions

// ============================================================================
// Integer Boundary Tests
// ============================================================================

#[test]
fn test_i8_boundaries() {
    let values = vec![
        i8::MIN,
        i8::MIN + 1,
        -1i8,
        0i8,
        1i8,
        i8::MAX - 1,
        i8::MAX,
    ];
    
    for v in values {
        let _ = v;
    }
}

#[test]
fn test_i16_boundaries() {
    let values = vec![
        i16::MIN,
        i16::MIN + 1,
        -1i16,
        0i16,
        1i16,
        i16::MAX - 1,
        i16::MAX,
    ];
    
    for v in values {
        let _ = v;
    }
}

#[test]
fn test_i32_boundaries() {
    let values = vec![
        i32::MIN,
        i32::MIN + 1,
        -1i32,
        0i32,
        1i32,
        i32::MAX - 1,
        i32::MAX,
    ];
    
    for v in values {
        let _ = v;
    }
}

#[test]
fn test_i64_boundaries() {
    let values = vec![
        i64::MIN,
        i64::MIN + 1,
        -1i64,
        0i64,
        1i64,
        i64::MAX - 1,
        i64::MAX,
    ];
    
    for v in values {
        let _ = v;
    }
}

#[test]
fn test_u8_boundaries() {
    let values = vec![
        u8::MIN,
        u8::MIN + 1,
        u8::MAX - 1,
        u8::MAX,
    ];
    
    for v in values {
        let _ = v;
    }
}

#[test]
fn test_u16_boundaries() {
    let values = vec![
        u16::MIN,
        u16::MIN + 1,
        u16::MAX - 1,
        u16::MAX,
    ];
    
    for v in values {
        let _ = v;
    }
}

#[test]
fn test_u32_boundaries() {
    let values = vec![
        u32::MIN,
        u32::MIN + 1,
        u32::MAX - 1,
        u32::MAX,
    ];
    
    for v in values {
        let _ = v;
    }
}

#[test]
fn test_u64_boundaries() {
    let values = vec![
        u64::MIN,
        u64::MIN + 1,
        u64::MAX - 1,
        u64::MAX,
    ];
    
    for v in values {
        let _ = v;
    }
}

// ============================================================================
// Float Boundary Tests
// ============================================================================

#[test]
fn test_f32_boundaries() {
    let values = vec![
        f32::NEG_INFINITY,
        f32::MIN,
        -1.0f32,
        -0.0f32,
        0.0f32,
        1.0f32,
        f32::MIN_POSITIVE,
        f32::MAX,
        f32::INFINITY,
        f32::NAN,
        f32::EPSILON,
    ];
    
    for v in values {
        let _ = v;
    }
}

#[test]
fn test_f64_boundaries() {
    let values = vec![
        f64::NEG_INFINITY,
        f64::MIN,
        -1.0f64,
        -0.0f64,
        0.0f64,
        1.0f64,
        f64::MIN_POSITIVE,
        f64::MAX,
        f64::INFINITY,
        f64::NAN,
        f64::EPSILON,
    ];
    
    for v in values {
        let _ = v;
    }
}

#[test]
fn test_float_special_values() {
    // Test NaN behavior
    let nan = f64::NAN;
    assert!(nan.is_nan());
    assert!(!nan.is_finite());
    
    // Test infinity
    let inf = f64::INFINITY;
    assert!(inf.is_infinite());
    assert!(!inf.is_finite());
    
    // Test negative infinity
    let neg_inf = f64::NEG_INFINITY;
    assert!(neg_inf.is_infinite());
    assert!(neg_inf.is_sign_negative());
}

#[test]
fn test_float_zero() {
    let pos_zero = 0.0f64;
    let neg_zero = -0.0f64;
    
    assert!(pos_zero.is_sign_positive());
    assert!(neg_zero.is_sign_negative());
    
    // They compare equal
    assert_eq!(pos_zero, neg_zero);
}

// ============================================================================
// Integer Overflow Tests
// ============================================================================

#[test]
fn test_checked_arithmetic() {
    let a = i32::MAX;
    let b = i32::MAX;
    
    assert_eq!(a.checked_add(1), None);
    assert_eq!(a.checked_sub(-1), None);
    assert_eq!(a.checked_mul(2), None);
    
    let c = i32::MIN;
    assert_eq!(c.checked_sub(1), None);
}

#[test]
fn test_wrapping_arithmetic() {
    let a = i32::MAX;
    assert_eq!(a.wrapping_add(1), i32::MIN);
    
    let b = i32::MIN;
    assert_eq!(b.wrapping_sub(1), i32::MAX);
}

#[test]
fn test_saturating_arithmetic() {
    let a = i32::MAX;
    assert_eq!(a.saturating_add(1), i32::MAX);
    
    let b = i32::MIN;
    assert_eq!(b.saturating_sub(1), i32::MIN);
}

// ============================================================================
// Conversion Tests
// ============================================================================

#[test]
fn test_int_to_float_conversion() {
    let values = vec![
        i64::MIN,
        i64::MIN / 2,
        -1i64,
        0i64,
        1i64,
        i64::MAX / 2,
        i64::MAX,
    ];
    
    for v in values {
        let f = v as f64;
        let _ = f;
    }
}

#[test]
fn test_float_to_int_conversion() {
    let values = vec![
        -1.9f64,
        -1.5f64,
        -1.1f64,
        -1.0f64,
        -0.9f64,
        -0.5f64,
        -0.1f64,
        0.0f64,
        0.1f64,
        0.5f64,
        0.9f64,
        1.0f64,
        1.1f64,
        1.5f64,
        1.9f64,
    ];
    
    for v in values {
        let trunc = v.trunc() as i64;
        let _ = trunc;
    }
}

// ============================================================================
// Comparison Tests
// ============================================================================

#[test]
fn test_float_comparisons() {
    assert!(f64::NAN != f64::NAN);
    assert!(f64::INFINITY > f64::MAX);
    assert!(f64::NEG_INFINITY < f64::MIN);
    assert!(0.0 == -0.0);
}

#[test]
fn test_integer_comparisons() {
    assert!(i64::MIN < i64::MAX);
    assert!(u64::MIN < u64::MAX);
    assert!(i64::MIN as u64 > i64::MAX as u64);
}
