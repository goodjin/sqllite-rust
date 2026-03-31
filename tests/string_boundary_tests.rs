//! String Handling Boundary Tests
//!
//! Tests for string handling edge cases and boundary conditions

// ============================================================================
// Empty String Tests
// ============================================================================

#[test]
fn test_empty_string() {
    let s = "";
    assert_eq!(s.len(), 0);
}

#[test]
fn test_whitespace_strings() {
    let strings = vec![
        " ",
        "  ",
        "\t",
        "\n",
        "\r\n",
        " \t\n ",
    ];
    
    for s in strings {
        assert!(!s.is_empty());
    }
}

// ============================================================================
// Unicode Tests
// ============================================================================

#[test]
fn test_unicode_strings() {
    let strings = vec![
        "hello",           // ASCII
        "你好世界",         // Chinese
        "Привет мир",      // Russian
        "こんにちは",       // Japanese
        "🎉🎊🎁",          // Emoji
        "café",            // Accented
        "naïve",           // Diaeresis
        "résumé",          // Multiple accents
    ];
    
    for s in strings {
        assert!(!s.is_empty());
    }
}

#[test]
fn test_unicode_boundaries() {
    // Test characters at code point boundaries
    let chars = vec![
        '\u{0000}',     // NUL
        '\u{007F}',     // DEL
        '\u{0080}',     // Extended ASCII start
        '\u{00FF}',     // Extended ASCII end
        '\u{0100}',     // Latin Extended A start
        '\u{07FF}',     // 2-byte UTF-8 end
        '\u{0800}',     // 3-byte UTF-8 start
        '\u{FFFF}',     // BMP end
        '\u{10000}',    // Plane 1 start
        '\u{10FFFF}',   // Max code point
    ];
    
    for c in chars {
        let s = c.to_string();
        assert_eq!(s.chars().next(), Some(c));
    }
}

// ============================================================================
// Special Character Tests
// ============================================================================

#[test]
fn test_special_characters() {
    let strings = vec![
        "\0",           // Null
        "\x01",         // SOH
        "\x1F",         // US
        "\x7F",         // DEL
        "\x80",         // Extended
        "\xFF",         // Max byte
    ];
    
    for s in strings {
        let _ = s.len();
    }
}

#[test]
fn test_escape_sequences() {
    let strings = vec![
        "\\",           // Backslash
        "\"",           // Double quote
        "\'",           // Single quote
        "\n",           // Newline
        "\t",           // Tab
        "\r",           // Carriage return
    ];
    
    for s in strings {
        assert!(!s.is_empty());
    }
}

// ============================================================================
// Long String Tests
// ============================================================================

#[test]
fn test_long_strings() {
    let sizes = vec![
        100,
        1000,
        10000,
        100000,
    ];
    
    for size in sizes {
        let s = "a".repeat(size);
        assert_eq!(s.len(), size);
    }
}

#[test]
fn test_repeated_patterns() {
    let patterns = vec![
        "ab".repeat(1000),
        "abc".repeat(1000),
        "0123456789".repeat(100),
    ];
    
    for pattern in patterns {
        assert!(!pattern.is_empty());
    }
}

// ============================================================================
// String Manipulation Tests
// ============================================================================

#[test]
fn test_string_split() {
    let s = "a,b,c,d,e";
    let parts: Vec<&str> = s.split(',').collect();
    assert_eq!(parts.len(), 5);
}

#[test]
fn test_string_trim() {
    let s = "  hello  ";
    assert_eq!(s.trim(), "hello");
    assert_eq!(s.trim_start(), "hello  ");
    assert_eq!(s.trim_end(), "  hello");
}

#[test]
fn test_string_case() {
    let s = "Hello World";
    assert_eq!(s.to_lowercase(), "hello world");
    assert_eq!(s.to_uppercase(), "HELLO WORLD");
}

// ============================================================================
// String Comparison Tests
// ============================================================================

#[test]
fn test_string_comparison() {
    let a = "apple";
    let b = "banana";
    let c = "apple";
    
    assert!(a < b);
    assert_eq!(a, c);
    assert!(b > a);
}

#[test]
fn test_string_equality() {
    assert_eq!("test", "test");
    assert_ne!("test", "Test");
    assert_ne!("test", "testing");
}
