//! P5-8: JSON Support Implementation
//!
//! JSON functions for parsing, querying, and manipulating JSON data.

use std::collections::HashMap;

pub mod error;
pub use error::{JsonError, Result};

/// JSON value types
#[derive(Debug, Clone, PartialEq)]
pub enum JsonValue {
    Null,
    Bool(bool),
    Number(f64),
    String(String),
    Array(Vec<JsonValue>),
    Object(HashMap<String, JsonValue>),
}

impl JsonValue {
    /// Parse a JSON string
    pub fn parse(input: &str) -> Result<Self> {
        let mut parser = JsonParser::new(input);
        parser.parse()
    }
    
    /// Convert to string representation
    pub fn to_string(&self) -> String {
        match self {
            JsonValue::Null => "null".to_string(),
            JsonValue::Bool(b) => b.to_string(),
            JsonValue::Number(n) => {
                if n.fract() == 0.0 {
                    format!("{:.0}", n)
                } else {
                    n.to_string()
                }
            }
            JsonValue::String(s) => format!("\"{}\"", s.replace('"', "\\\"")),
            JsonValue::Array(arr) => {
                let elements: Vec<String> = arr.iter().map(|v| v.to_string()).collect();
                format!("[{}]", elements.join(","))
            }
            JsonValue::Object(obj) => {
                let pairs: Vec<String> = obj.iter()
                    .map(|(k, v)| format!("\"{}\":{}", k, v.to_string()))
                    .collect();
                format!("{{{}}}", pairs.join(","))
            }
        }
    }
    
    /// Extract value by path (e.g., "$.name" or "$.items[0].price")
    pub fn extract(&self, path: &str) -> Option<&JsonValue> {
        if path == "$" {
            return Some(self);
        }
        
        let path = if path.starts_with("$.") {
            &path[2..]
        } else if path.starts_with('$') {
            &path[1..]
        } else {
            path
        };
        
        let parts: Vec<&str> = path.split('.').collect();
        self.extract_path(&parts, 0)
    }
    
    fn extract_path(&self, parts: &[&str], index: usize) -> Option<&JsonValue> {
        if index >= parts.len() {
            return Some(self);
        }
        
        let part = parts[index];
        
        // Handle array access like "items[0]"
        if let Some(bracket_pos) = part.find('[') {
            let key = &part[..bracket_pos];
            let array_part = &part[bracket_pos..];
            
            // Get the object/array
            let next = if key.is_empty() {
                Some(self)
            } else {
                match self {
                    JsonValue::Object(obj) => obj.get(key),
                    _ => None,
                }
            };
            
            // Parse array indices
            let mut current = next?;
            let mut pos = 0;
            while let Some(start) = array_part[pos..].find('[') {
                let start = pos + start;
                if let Some(end) = array_part[start..].find(']') {
                    let end = start + end;
                    let idx_str = &array_part[start + 1..end];
                    if let Ok(idx) = idx_str.parse::<usize>() {
                        match current {
                            JsonValue::Array(arr) => {
                                current = arr.get(idx)?;
                            }
                            _ => return None,
                        }
                    }
                    pos = end + 1;
                } else {
                    break;
                }
            }
            
            current.extract_path(parts, index + 1)
        } else {
            match self {
                JsonValue::Object(obj) => {
                    obj.get(part).and_then(|v| v.extract_path(parts, index + 1))
                }
                _ => None,
            }
        }
    }
    
    /// Get the type name
    pub fn type_name(&self) -> &'static str {
        match self {
            JsonValue::Null => "null",
            JsonValue::Bool(_) => "boolean",
            JsonValue::Number(_) => "real",
            JsonValue::String(_) => "text",
            JsonValue::Array(_) => "array",
            JsonValue::Object(_) => "object",
        }
    }
}

/// Simple JSON parser
struct JsonParser<'a> {
    input: &'a str,
    position: usize,
}

impl<'a> JsonParser<'a> {
    fn new(input: &'a str) -> Self {
        Self { input, position: 0 }
    }
    
    fn parse(&mut self) -> Result<JsonValue> {
        self.skip_whitespace();
        let value = self.parse_value()?;
        self.skip_whitespace();
        if self.position < self.input.len() {
            return Err(JsonError::InvalidSyntax(format!(
                "Unexpected characters at position {}", self.position
            )));
        }
        Ok(value)
    }
    
    fn parse_value(&mut self) -> Result<JsonValue> {
        self.skip_whitespace();
        
        if self.is_at_end() {
            return Err(JsonError::UnexpectedEof);
        }
        
        match self.peek() {
            'n' => self.parse_null(),
            't' => self.parse_true(),
            'f' => self.parse_false(),
            '"' => self.parse_string(),
            '[' => self.parse_array(),
            '{' => self.parse_object(),
            ch if ch.is_ascii_digit() || ch == '-' => self.parse_number(),
            _ => Err(JsonError::InvalidSyntax(format!(
                "Unexpected character '{}' at position {}",
                self.peek(), self.position
            ))),
        }
    }
    
    fn parse_null(&mut self) -> Result<JsonValue> {
        self.expect_literal("null")?;
        Ok(JsonValue::Null)
    }
    
    fn parse_true(&mut self) -> Result<JsonValue> {
        self.expect_literal("true")?;
        Ok(JsonValue::Bool(true))
    }
    
    fn parse_false(&mut self) -> Result<JsonValue> {
        self.expect_literal("false")?;
        Ok(JsonValue::Bool(false))
    }
    
    fn parse_string(&mut self) -> Result<JsonValue> {
        self.consume('"')?;
        let mut result = String::new();
        
        while !self.is_at_end() && self.peek() != '"' {
            let ch = self.advance();
            if ch == '\\' {
                if self.is_at_end() {
                    return Err(JsonError::UnexpectedEof);
                }
                match self.advance() {
                    '"' => result.push('"'),
                    '\\' => result.push('\\'),
                    '/' => result.push('/'),
                    'b' => result.push('\x08'),
                    'f' => result.push('\x0c'),
                    'n' => result.push('\n'),
                    'r' => result.push('\r'),
                    't' => result.push('\t'),
                    'u' => {
                        // Unicode escape (simplified)
                        let hex: String = (0..4).filter_map(|_| {
                            if !self.is_at_end() { Some(self.advance()) } else { None }
                        }).collect();
                        if let Ok(code) = u32::from_str_radix(&hex, 16) {
                            if let Some(ch) = std::char::from_u32(code) {
                                result.push(ch);
                            }
                        }
                    }
                    c => result.push(c),
                }
            } else {
                result.push(ch);
            }
        }
        
        self.consume('"')?;
        Ok(JsonValue::String(result))
    }
    
    fn parse_number(&mut self) -> Result<JsonValue> {
        let start = self.position;
        
        // Optional minus
        if self.peek() == '-' {
            self.advance();
        }
        
        // Integer part
        if self.peek() == '0' {
            self.advance();
        } else if self.peek().is_ascii_digit() {
            while !self.is_at_end() && self.peek().is_ascii_digit() {
                self.advance();
            }
        }
        
        // Decimal part
        if !self.is_at_end() && self.peek() == '.' {
            self.advance();
            while !self.is_at_end() && self.peek().is_ascii_digit() {
                self.advance();
            }
        }
        
        // Exponent part
        if !self.is_at_end() && (self.peek() == 'e' || self.peek() == 'E') {
            self.advance();
            if !self.is_at_end() && (self.peek() == '+' || self.peek() == '-') {
                self.advance();
            }
            while !self.is_at_end() && self.peek().is_ascii_digit() {
                self.advance();
            }
        }
        
        let num_str = &self.input[start..self.position];
        match num_str.parse::<f64>() {
            Ok(n) => Ok(JsonValue::Number(n)),
            Err(_) => Err(JsonError::InvalidNumber(num_str.to_string())),
        }
    }
    
    fn parse_array(&mut self) -> Result<JsonValue> {
        self.consume('[')?;
        self.skip_whitespace();
        
        let mut elements = Vec::new();
        
        if !self.is_at_end() && self.peek() != ']' {
            loop {
                elements.push(self.parse_value()?);
                self.skip_whitespace();
                
                if self.is_at_end() || self.peek() != ',' {
                    break;
                }
                self.advance(); // consume ','
                self.skip_whitespace();
            }
        }
        
        self.consume(']')?;
        Ok(JsonValue::Array(elements))
    }
    
    fn parse_object(&mut self) -> Result<JsonValue> {
        self.consume('{')?;
        self.skip_whitespace();
        
        let mut obj = HashMap::new();
        
        if !self.is_at_end() && self.peek() != '}' {
            loop {
                // Parse key (must be a string)
                self.skip_whitespace();
                let key = match self.parse_value()? {
                    JsonValue::String(s) => s,
                    _ => return Err(JsonError::InvalidSyntax(
                        "Object key must be a string".to_string()
                    )),
                };
                
                self.skip_whitespace();
                self.consume(':')?;
                self.skip_whitespace();
                
                // Parse value
                let value = self.parse_value()?;
                obj.insert(key, value);
                
                self.skip_whitespace();
                if self.is_at_end() || self.peek() != ',' {
                    break;
                }
                self.advance(); // consume ','
            }
        }
        
        self.consume('}')?;
        Ok(JsonValue::Object(obj))
    }
    
    fn skip_whitespace(&mut self) {
        while !self.is_at_end() && self.peek().is_whitespace() {
            self.advance();
        }
    }
    
    fn expect_literal(&mut self, literal: &str) -> Result<()> {
        for ch in literal.chars() {
            if self.is_at_end() || self.peek() != ch {
                return Err(JsonError::InvalidSyntax(format!(
                    "Expected '{}' at position {}", literal, self.position
                )));
            }
            self.advance();
        }
        Ok(())
    }
    
    fn consume(&mut self, expected: char) -> Result<()> {
        if self.is_at_end() || self.peek() != expected {
            return Err(JsonError::ExpectedChar {
                expected,
                position: self.position,
            });
        }
        self.advance();
        Ok(())
    }
    
    fn peek(&self) -> char {
        self.input.chars().nth(self.position).unwrap_or('\0')
    }
    
    fn advance(&mut self) -> char {
        let ch = self.peek();
        self.position += 1;
        ch
    }
    
    fn is_at_end(&self) -> bool {
        self.position >= self.input.len()
    }
}

/// JSON functions implementation
pub struct JsonFunctions;

impl JsonFunctions {
    /// json(value) - Returns JSON representation
    pub fn json(value: &str) -> Result<String> {
        let parsed = JsonValue::parse(value)?;
        Ok(parsed.to_string())
    }
    
    /// json_array(values...) - Create JSON array
    pub fn json_array(values: &[JsonValue]) -> String {
        JsonValue::Array(values.to_vec()).to_string()
    }
    
    /// json_object(key, value, ...) - Create JSON object
    pub fn json_object(pairs: &[(String, JsonValue)]) -> String {
        let obj: HashMap<String, JsonValue> = pairs.iter().cloned().collect();
        JsonValue::Object(obj).to_string()
    }
    
    /// json_extract(json, path) - Extract value at path
    pub fn json_extract(json: &str, path: &str) -> Result<Option<String>> {
        let value = JsonValue::parse(json)?;
        match value.extract(path) {
            Some(v) => Ok(Some(v.to_string())),
            None => Ok(None),
        }
    }
    
    /// json_type(json, path) - Get type of JSON value
    pub fn json_type(json: &str, path: Option<&str>) -> Result<String> {
        let value = JsonValue::parse(json)?;
        let target = match path {
            Some(p) => value.extract(p),
            None => Some(&value),
        };
        
        match target {
            Some(v) => Ok(v.type_name().to_string()),
            None => Ok("null".to_string()),
        }
    }
    
    /// json_valid(json) - Check if string is valid JSON
    pub fn json_valid(json: &str) -> bool {
        JsonValue::parse(json).is_ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_null() {
        let json = JsonValue::parse("null").unwrap();
        assert_eq!(json, JsonValue::Null);
    }

    #[test]
    fn test_parse_bool() {
        assert_eq!(JsonValue::parse("true").unwrap(), JsonValue::Bool(true));
        assert_eq!(JsonValue::parse("false").unwrap(), JsonValue::Bool(false));
    }

    #[test]
    fn test_parse_number() {
        match JsonValue::parse("42").unwrap() {
            JsonValue::Number(n) => assert_eq!(n, 42.0),
            _ => panic!("Expected number"),
        }
        
        match JsonValue::parse("-3.14").unwrap() {
            JsonValue::Number(n) => assert!((n - (-3.14)).abs() < 0.001),
            _ => panic!("Expected number"),
        }
    }

    #[test]
    fn test_parse_string() {
        assert_eq!(
            JsonValue::parse("\"hello\"").unwrap(),
            JsonValue::String("hello".to_string())
        );
    }

    #[test]
    fn test_parse_array() {
        let json = JsonValue::parse("[1, 2, 3]").unwrap();
        match json {
            JsonValue::Array(arr) => assert_eq!(arr.len(), 3),
            _ => panic!("Expected array"),
        }
    }

    #[test]
    fn test_parse_object() {
        let json = JsonValue::parse(r#"{"name": "John", "age": 30}"#).unwrap();
        match json {
            JsonValue::Object(obj) => {
                assert_eq!(obj.len(), 2);
                assert!(obj.contains_key("name"));
                assert!(obj.contains_key("age"));
            }
            _ => panic!("Expected object"),
        }
    }

    #[test]
    fn test_extract() {
        let json = JsonValue::parse(r#"{"person": {"name": "John", "age": 30}}"#).unwrap();
        
        assert_eq!(json.extract("$.person.name"), Some(&JsonValue::String("John".to_string())));
        assert_eq!(json.extract("$.person.age"), Some(&JsonValue::Number(30.0)));
    }

    #[test]
    fn test_json_functions() {
        assert!(JsonFunctions::json_valid("{\"a\": 1}"));
        assert!(!JsonFunctions::json_valid("invalid"));
        
        assert_eq!(
            JsonFunctions::json_type("{\"a\": 1}", Some("$.a")).unwrap(),
            "real"
        );
        
        assert_eq!(
            JsonFunctions::json_extract(r#"{"name": "John"}"#, "$.name").unwrap(),
            Some("\"John\"".to_string())
        );
    }
}
