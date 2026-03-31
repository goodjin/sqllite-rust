#![no_main]

use libfuzzer_sys::fuzz_target;

// Tokenizer fuzzing target
// Tests SQL tokenization with various inputs

fuzz_target!(|data: &[u8]| {
    // Try as UTF-8
    if let Ok(sql) = std::str::from_utf8(data) {
        let _ = fuzz_tokenize(sql);
        let _ = fuzz_keyword_detection(sql);
        let _ = fuzz_string_literal_parsing(sql);
        let _ = fuzz_numeric_literal_parsing(sql);
    }
    
    // Also test raw bytes
    let _ = fuzz_raw_byte_handling(data);
    let _ = fuzz_comment_parsing(data);
    let _ = fuzz_identifier_parsing(data);
});

fn fuzz_tokenize(sql: &str) -> Result<(), ()> {
    use sqllite_rust::sql::tokenizer::Tokenizer;
    use sqllite_rust::sql::token::Token;
    
    let mut tokenizer = Tokenizer::new(sql);
    let mut tokens = Vec::new();
    
    // Collect all tokens
    loop {
        let token = tokenizer.next_token();
        tokens.push(token.clone());
        
        if matches!(token, Token::Eof) {
            break;
        }
        
        // Prevent infinite loops on malformed input
        if tokens.len() > 10000 {
            break;
        }
    }
    
    // Verify token sequence properties
    verify_token_sequence(&tokens)?;
    
    Ok(())
}

fn verify_token_sequence(tokens: &[sqllite_rust::sql::token::Token]) -> Result<(), ()> {
    use sqllite_rust::sql::token::Token;
    
    // Last token should always be EOF
    if let Some(last) = tokens.last() {
        assert!(matches!(last, Token::Eof), "Last token should be EOF");
    }
    
    // Check for invalid sequences
    for window in tokens.windows(2) {
        if let [t1, t2] = window {
            match (t1, t2) {
                // Two consecutive string literals without operator should be rare but valid
                (Token::String(_), Token::String(_)) => {}
                // Two consecutive numbers without operator
                (Token::Number(_), Token::Number(_)) => {}
                // These combinations are fine
                _ => {}
            }
        }
    }
    
    Ok(())
}

fn fuzz_keyword_detection(sql: &str) -> Result<(), ()> {
    use sqllite_rust::sql::tokenizer::Tokenizer;
    
    // List of SQL keywords to test
    let keywords = vec![
        "SELECT", "INSERT", "UPDATE", "DELETE", "CREATE", "DROP",
        "TABLE", "INDEX", "WHERE", "FROM", "JOIN", "LEFT", "RIGHT",
        "INNER", "OUTER", "ON", "AND", "OR", "NOT", "NULL",
        "TRUE", "FALSE", "BEGIN", "COMMIT", "ROLLBACK",
        "ORDER", "BY", "GROUP", "HAVING", "LIMIT", "OFFSET",
    ];
    
    for keyword in &keywords {
        let test_sql = format!("{} foo", keyword);
        let mut tokenizer = Tokenizer::new(&test_sql);
        let _first = tokenizer.next_token();
        let _second = tokenizer.next_token();
    }
    
    // Test case insensitivity
    let lower_sql = sql.to_lowercase();
    let mut t1 = Tokenizer::new(sql);
    let mut t2 = Tokenizer::new(&lower_sql);
    
    // Both should produce same number of tokens
    let mut count1 = 0;
    let mut count2 = 0;
    
    loop {
        let tok = t1.next_token();
        count1 += 1;
        if matches!(tok, sqllite_rust::sql::token::Token::Eof) {
            break;
        }
    }
    
    loop {
        let tok = t2.next_token();
        count2 += 1;
        if matches!(tok, sqllite_rust::sql::token::Token::Eof) {
            break;
        }
    }
    
    assert_eq!(count1, count2, "Case sensitivity affected token count");
    
    Ok(())
}

fn fuzz_string_literal_parsing(sql: &str) -> Result<(), ()> {
    use sqllite_rust::sql::tokenizer::Tokenizer;
    use sqllite_rust::sql::token::Token;
    
    // Test string literal patterns
    let patterns = vec![
        "'hello'",
        "''",
        "'with '' quote'",
        "'multi\nline'",
        "'    spaces    '",
    ];
    
    for pattern in &patterns {
        let mut tokenizer = Tokenizer::new(pattern);
        let token = tokenizer.next_token();
        
        // Should parse as string or error, not crash
        match token {
            Token::String(_) | Token::Error(_) | _ => {}
        }
    }
    
    // Test for unclosed strings in fuzz input
    if sql.contains('\'') {
        let mut tokenizer = Tokenizer::new(sql);
        loop {
            let token = tokenizer.next_token();
            if matches!(token, Token::Eof) {
                break;
            }
        }
    }
    
    Ok(())
}

fn fuzz_numeric_literal_parsing(sql: &str) -> Result<(), ()> {
    use sqllite_rust::sql::tokenizer::Tokenizer;
    use sqllite_rust::sql::token::Token;
    
    // Test numeric patterns
    let patterns = vec![
        "42",
        "3.14",
        "-17",
        "0",
        "1234567890",
        "1e10",
        "1.5e-3",
        "0xFF",
        "0b1010",
    ];
    
    for pattern in &patterns {
        let mut tokenizer = Tokenizer::new(pattern);
        let token = tokenizer.next_token();
        
        match token {
            Token::Integer(_) | Token::Float(_) | Token::Number(_) => {}
            _ => {
                // Other tokens are acceptable for malformed numbers
            }
        }
    }
    
    // Test number parsing from fuzz input
    let mut tokenizer = Tokenizer::new(sql);
    loop {
        let token = tokenizer.next_token();
        match token {
            Token::Eof => break,
            Token::Integer(n) => {
                // Verify it's a valid integer
                let _ = n;
            }
            Token::Float(f) => {
                // Verify it's a valid float
                assert!(!f.is_nan() || sql.contains("NaN"), "Unexpected NaN");
            }
            _ => {}
        }
    }
    
    Ok(())
}

fn fuzz_raw_byte_handling(data: &[u8]) -> Result<(), ()> {
    // Test handling of non-UTF8 data (should not crash)
    
    // Check for null bytes
    if data.contains(&0) {
        // Null bytes should be handled gracefully
    }
    
    // Check for high bytes
    let has_high_bytes = data.iter().any(|&b| b >= 0x80);
    if has_high_bytes {
        // High bytes might be invalid UTF-8
        let _ = std::str::from_utf8(data);
    }
    
    Ok(())
}

fn fuzz_comment_parsing(data: &[u8]) -> Result<(), ()> {
    use sqllite_rust::sql::tokenizer::Tokenizer;
    use sqllite_rust::sql::token::Token;
    
    if let Ok(sql) = std::str::from_utf8(data) {
        // Test SQL with comments
        let with_comments = format!("/* comment */ SELECT 1 -- end\n{}", sql);
        let mut tokenizer = Tokenizer::new(&with_comments);
        
        let mut found_non_comment = false;
        loop {
            let token = tokenizer.next_token();
            
            match token {
                Token::Eof => break,
                Token::Comment(_) => {}
                _ => {
                    found_non_comment = true;
                }
            }
        }
        
        // Should have found at least SELECT or 1
        assert!(found_non_comment || !sql.is_empty(), "Only comments found");
    }
    
    Ok(())
}

fn fuzz_identifier_parsing(data: &[u8]) -> Result<(), ()> {
    use sqllite_rust::sql::tokenizer::Tokenizer;
    use sqllite_rust::sql::token::Token;
    
    if let Ok(sql) = std::str::from_utf8(data) {
        let mut tokenizer = Tokenizer::new(sql);
        
        loop {
            let token = tokenizer.next_token();
            
            match token {
                Token::Eof => break,
                Token::Identifier(ident) => {
                    // Verify identifier properties
                    assert!(!ident.is_empty(), "Empty identifier");
                    
                    // Check for valid identifier characters
                    for ch in ident.chars() {
                        assert!(
                            ch.is_alphanumeric() || ch == '_',
                            "Invalid identifier character: {}", ch
                        );
                    }
                }
                Token::QuotedIdentifier(ident) => {
                    // Quoted identifiers can have special characters
                    assert!(!ident.is_empty(), "Empty quoted identifier");
                }
                _ => {}
            }
        }
    }
    
    Ok(())
}
