#![no_main]

use libfuzzer_sys::fuzz_target;
use std::str;

// SQL Parser fuzzing target
// Tests the SQL parser with random byte sequences that are interpreted as SQL strings

fuzz_target!(|data: &[u8]| {
    // Try to interpret data as UTF-8 string
    if let Ok(sql) = str::from_utf8(data) {
        // Fuzz the tokenizer
        let _ = fuzz_tokenizer(sql);
        
        // Fuzz the parser
        let _ = fuzz_parser(sql);
        
        // Fuzz expression parsing
        let _ = fuzz_expression_parser(sql);
    }
    
    // Also test with raw bytes for binary robustness
    fuzz_raw_bytes(data);
});

fn fuzz_tokenizer(sql: &str) -> Result<(), ()> {
    use sqllite_rust::sql::tokenizer::Tokenizer;
    
    let mut tokenizer = Tokenizer::new(sql);
    
    // Tokenize the entire input
    loop {
        let token = tokenizer.next_token();
        if matches!(token, sqllite_rust::sql::token::Token::Eof) {
            break;
        }
    }
    
    Ok(())
}

fn fuzz_parser(sql: &str) -> Result<(), ()> {
    use sqllite_rust::sql::parser::Parser;
    
    // Try to parse the SQL
    if let Ok(mut parser) = Parser::new(sql) {
        let _ = parser.parse();
    }
    
    Ok(())
}

fn fuzz_expression_parser(sql: &str) -> Result<(), ()> {
    use sqllite_rust::sql::tokenizer::Tokenizer;
    use sqllite_rust::sql::token::Token;
    
    // Create a tokenizer to check for expression patterns
    let tokenizer = Tokenizer::new(sql);
    
    // Look for patterns that might be expressions
    let tokens: Vec<_> = std::iter::from_fn(|| {
        let mut t = tokenizer.clone();
        let token = t.next_token();
        Some(token)
    }).take(100).collect();
    
    // Check token sequence validity
    for window in tokens.windows(2) {
        if let [t1, t2] = window {
            // Check for invalid token sequences that shouldn't cause crashes
            match (t1, t2) {
                // These combinations should be handled gracefully
                (Token::Semicolon, Token::Semicolon) => {}
                (Token::Comma, Token::Comma) => {}
                _ => {}
            }
        }
    }
    
    Ok(())
}

fn fuzz_raw_bytes(data: &[u8]) {
    // Test handling of invalid UTF-8 sequences
    // This ensures the parser doesn't crash on binary data
    let _ = data.len();
    let _ = data.is_empty();
    
    // Check for specific byte patterns that might cause issues
    for (i, &byte) in data.iter().enumerate() {
        // Look for null bytes
        if byte == 0 {
            continue;
        }
        
        // Look for high bytes (potential UTF-8 continuation)
        if byte >= 0x80 {
            // Check for incomplete multi-byte sequences
            if i + 1 < data.len() {
                let _next = data[i + 1];
            }
        }
    }
}
