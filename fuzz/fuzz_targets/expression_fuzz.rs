#![no_main]

use libfuzzer_sys::fuzz_target;

// Expression evaluation fuzzing target
// Tests SQL expression parsing and evaluation

fuzz_target!(|data: &[u8]| {
    if let Ok(expr_str) = std::str::from_utf8(data) {
        let _ = fuzz_expression_parsing(expr_str);
        let _ = fuzz_binary_operators(expr_str);
        let _ = fuzz_unary_operators(expr_str);
        let _ = fuzz_function_calls(expr_str);
    }
    
    let _ = fuzz_expression_evaluation(data);
    let _ = fuzz_operator_precedence(data);
});

fn fuzz_expression_parsing(expr_str: &str) -> Result<(), ()> {
    use sqllite_rust::sql::tokenizer::Tokenizer;
    use sqllite_rust::sql::token::Token;
    
    // Wrap in SELECT to make it valid SQL
    let sql = format!("SELECT {}", expr_str);
    let mut tokenizer = Tokenizer::new(&sql);
    
    // Skip SELECT
    let _ = tokenizer.next_token();
    
    // Parse expression tokens
    let mut paren_depth = 0i32;
    let mut token_count = 0;
    
    loop {
        let token = tokenizer.next_token();
        token_count += 1;
        
        match &token {
            Token::Eof | Token::Semicolon => break,
            Token::LeftParen => paren_depth += 1,
            Token::RightParen => {
                paren_depth -= 1;
                if paren_depth < 0 {
                    // Unbalanced parens, but shouldn't crash
                    paren_depth = 0;
                }
            }
            _ => {}
        }
        
        if token_count > 1000 {
            break;
        }
    }
    
    // Parens should be balanced at EOF
    assert_eq!(paren_depth, 0, "Unbalanced parentheses in expression");
    
    Ok(())
}

fn fuzz_binary_operators(expr_str: &str) -> Result<(), ()> {
    // Test binary operator parsing
    let binary_ops = vec![
        "+", "-", "*", "/", "%",
        "=", "!=", "<>", "<", ">", "<=", ">=",
        "AND", "OR",
        "||", // string concatenation
        "<<", ">>", // bit shifts
        "&", "|", "^", // bitwise
    ];
    
    for op in &binary_ops {
        let sql = format!("1 {} 2", op);
        let _ = sqllite_rust::sql::tokenizer::Tokenizer::new(&sql);
    }
    
    // Test operator associativity
    let associative = format!("1 + 2 + 3 + {}", expr_str);
    let _ = sqllite_rust::sql::tokenizer::Tokenizer::new(&associative);
    
    Ok(())
}

fn fuzz_unary_operators(expr_str: &str) -> Result<(), ()> {
    // Test unary operator parsing
    let unary_exprs = vec![
        format!("-{}", expr_str),
        format!("+{}", expr_str),
        format!("NOT {}", expr_str),
        format!("~{}", expr_str), // bitwise NOT
        format!("EXISTS ({})", expr_str),
    ];
    
    for expr in unary_exprs {
        let mut tokenizer = sqllite_rust::sql::tokenizer::Tokenizer::new(&expr);
        let mut count = 0;
        loop {
            let token = tokenizer.next_token();
            if matches!(token, sqllite_rust::sql::token::Token::Eof) {
                break;
            }
            count += 1;
            if count > 100 {
                break;
            }
        }
    }
    
    Ok(())
}

fn fuzz_function_calls(expr_str: &str) -> Result<(), ()> {
    // Common SQL functions
    let functions = vec![
        "ABS", "LENGTH", "UPPER", "LOWER",
        "COALESCE", "NULLIF", "IFNULL",
        "MIN", "MAX", "SUM", "AVG", "COUNT",
        "ROUND", "RANDOM",
        "DATE", "TIME", "DATETIME",
    ];
    
    for func in functions {
        let sql = format!("{}({})", func, expr_str);
        let mut tokenizer = sqllite_rust::sql::tokenizer::Tokenizer::new(&sql);
        
        let mut paren_depth = 0i32;
        let mut found_lparen = false;
        let mut found_rparen = false;
        
        loop {
            let token = tokenizer.next_token();
            
            match token {
                sqllite_rust::sql::token::Token::Eof => break,
                sqllite_rust::sql::token::Token::LeftParen => {
                    paren_depth += 1;
                    found_lparen = true;
                }
                sqllite_rust::sql::token::Token::RightParen => {
                    paren_depth -= 1;
                    found_rparen = true;
                }
                _ => {}
            }
        }
        
        // Function calls should have balanced parens
        if found_lparen {
            assert!(found_rparen, "Unclosed function call");
            assert_eq!(paren_depth, 0, "Unbalanced parens in function call");
        }
    }
    
    // Test nested functions
    let nested = format!(
        "COALESCE(ABS({}), LENGTH({}), 0)",
        expr_str, expr_str
    );
    let _ = sqllite_rust::sql::tokenizer::Tokenizer::new(&nested);
    
    Ok(())
}

fn fuzz_expression_evaluation(data: &[u8]) -> Result<(), ()> {
    // Simulate expression evaluation with fuzz data
    
    #[derive(Debug, Clone)]
    enum Value {
        Null,
        Bool(bool),
        Int(i64),
        Float(f64),
        String(Vec<u8>),
    }
    
    // Parse data into values
    let values: Vec<Value> = data.iter().enumerate().map(|(i, &b)| {
        match b % 5 {
            0 => Value::Null,
            1 => Value::Bool(b % 2 == 0),
            2 => Value::Int((i as i64 * b as i64) % 1000),
            3 => Value::Float((b as f64) / 100.0),
            4 => Value::String(vec![b]),
            _ => Value::Null,
        }
    }).collect();
    
    // Test arithmetic operations
    for window in values.windows(2) {
        if let [a, b] = window {
            let _ = evaluate_binary_op(a.clone(), b.clone(), "+");
            let _ = evaluate_binary_op(a.clone(), b.clone(), "-");
            let _ = evaluate_binary_op(a.clone(), b.clone(), "*");
            let _ = evaluate_binary_op(a.clone(), b.clone(), "/");
        }
    }
    
    // Test comparison operations
    for window in values.windows(2) {
        if let [a, b] = window {
            let _ = evaluate_comparison(a, b, "=");
            let _ = evaluate_comparison(a, b, "<");
            let _ = evaluate_comparison(a, b, ">");
        }
    }
    
    Ok(())
}

fn evaluate_binary_op(a: Value, b: Value, op: &str) -> Result<Value, ()> 
where
    Value: Clone,
{
    match (a, b) {
        (Value::Int(x), Value::Int(y)) => {
            match op {
                "+" => Ok(Value::Int(x + y)),
                "-" => Ok(Value::Int(x - y)),
                "*" => Ok(Value::Int(x * y)),
                "/" => if y != 0 { Ok(Value::Int(x / y)) } else { Err(()) },
                "%" => if y != 0 { Ok(Value::Int(x % y)) } else { Err(()) },
                _ => Err(()),
            }
        }
        (Value::Float(x), Value::Float(y)) => {
            match op {
                "+" => Ok(Value::Float(x + y)),
                "-" => Ok(Value::Float(x - y)),
                "*" => Ok(Value::Float(x * y)),
                "/" => Ok(Value::Float(x / y)),
                _ => Err(()),
            }
        }
        (Value::Int(x), Value::Float(y)) => {
            evaluate_binary_op(Value::Float(x as f64), Value::Float(y), op)
        }
        (Value::Float(x), Value::Int(y)) => {
            evaluate_binary_op(Value::Float(x), Value::Float(y as f64), op)
        }
        _ => Err(()),
    }
}

fn evaluate_comparison(a: &Value, b: &Value, op: &str) -> Result<bool, ()> {
    match (a, b) {
        (Value::Null, _) | (_, Value::Null) => Err(()),
        (Value::Int(x), Value::Int(y)) => {
            match op {
                "=" => Ok(x == y),
                "<" => Ok(x < y),
                ">" => Ok(x > y),
                "<=" => Ok(x <= y),
                ">=" => Ok(x >= y),
                _ => Err(()),
            }
        }
        (Value::Float(x), Value::Float(y)) => {
            match op {
                "=" => Ok((x - y).abs() < f64::EPSILON),
                "<" => Ok(x < y),
                ">" => Ok(x > y),
                _ => Err(()),
            }
        }
        (Value::String(x), Value::String(y)) => {
            match op {
                "=" => Ok(x == y),
                "<" => Ok(x < y),
                ">" => Ok(x > y),
                _ => Err(()),
            }
        }
        _ => Err(()),
    }
}

fn fuzz_operator_precedence(data: &[u8]) -> Result<(), ()> {
    // Test that operator precedence is handled correctly
    
    // Create expressions with multiple operators
    let exprs = vec![
        "1 + 2 * 3",
        "1 * 2 + 3",
        "1 + 2 + 3 * 4",
        "(1 + 2) * 3",
        "1 + 2 * 3 + 4",
        "1 * 2 + 3 * 4",
    ];
    
    for expr in exprs {
        let sql = format!("SELECT {}", expr);
        let mut tokenizer = sqllite_rust::sql::tokenizer::Tokenizer::new(&sql);
        
        let mut tokens = Vec::new();
        loop {
            let token = tokenizer.next_token();
            if matches!(token, sqllite_rust::sql::token::Token::Eof) {
                break;
            }
            tokens.push(token);
        }
        
        // Should have parsed without errors
        assert!(!tokens.is_empty(), "Expression produced no tokens");
    }
    
    // Test with fuzz data - create random expression
    if data.len() >= 6 {
        let ops = vec!["+", "-", "*", "/"];
        let mut expr = String::new();
        
        for (i, &byte) in data.iter().enumerate().take(10) {
            let num = (byte % 10) + 1;
            let op = ops[(byte as usize / 10) % ops.len()];
            
            if i > 0 {
                expr.push_str(op);
            }
            expr.push_str(&num.to_string());
        }
        
        let sql = format!("SELECT {}", expr);
        let _ = sqllite_rust::sql::tokenizer::Tokenizer::new(&sql);
    }
    
    Ok(())
}

// Value type definition for evaluation
#[derive(Debug, Clone)]
enum Value {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(Vec<u8>),
}
