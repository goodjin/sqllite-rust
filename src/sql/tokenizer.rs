use crate::sql::token::Token;

pub struct Tokenizer<'a> {
    input: &'a str,
    position: usize,
}

impl<'a> Tokenizer<'a> {
    pub fn new(input: &'a str) -> Self {
        Self { input, position: 0 }
    }

    pub fn next_token(&mut self) -> Token {
        self.skip_whitespace();

        if self.is_at_end() {
            return Token::Eof;
        }

        let ch = self.peek();

        match ch {
            ';' => { self.advance(); Token::Semicolon }
            ',' => { self.advance(); Token::Comma }
            '(' => { self.advance(); Token::LParen }
            ')' => { self.advance(); Token::RParen }
            '[' => { self.advance(); Token::LBracket }
            ']' => { self.advance(); Token::RBracket }
            '?' => { self.advance(); Token::QuestionMark }  // 占位符
            '*' => { self.advance(); Token::Star }
            '+' => { self.advance(); Token::Plus }
            '-' => { self.advance(); Token::Minus }
            '/' => { self.advance(); Token::Slash }
            '=' => { self.advance(); Token::Equal }
            '<' => self.match_less(),
            '>' => self.match_greater(),
            '!' => self.match_bang(),
            '\'' => self.read_string(),
            '0'..='9' => self.read_number(),
            'a'..='z' | 'A'..='Z' | '_' => self.read_identifier(),
            _ => {
                self.advance();
                Token::Eof
            }
        }
    }

    fn match_less(&mut self) -> Token {
        self.advance();
        if self.peek() == '=' {
            self.advance();
            Token::LessEqual
        } else if self.peek() == '>' {
            self.advance();
            Token::NotEqual
        } else {
            Token::Less
        }
    }

    fn match_greater(&mut self) -> Token {
        self.advance();
        if self.peek() == '=' {
            self.advance();
            Token::GreaterEqual
        } else {
            Token::Greater
        }
    }

    fn match_bang(&mut self) -> Token {
        self.advance();
        if self.peek() == '=' {
            self.advance();
            Token::NotEqual
        } else {
            Token::Eof
        }
    }

    fn read_string(&mut self) -> Token {
        self.advance();
        let start = self.position;
        while self.peek() != '\'' && !self.is_at_end() {
            self.advance();
        }
        let text = &self.input[start..self.position];
        if self.peek() == '\'' {
            self.advance();
        }
        Token::StringLiteral(text.to_string())
    }

    fn read_number(&mut self) -> Token {
        let start = self.position;
        let mut is_float = false;

        while self.peek().is_ascii_digit() {
            self.advance();
        }

        if self.peek() == '.' {
            is_float = true;
            self.advance();
            while self.peek().is_ascii_digit() {
                self.advance();
            }
        }

        let text = &self.input[start..self.position];
        if is_float {
            Token::FloatLiteral(text.parse().unwrap_or(0.0))
        } else {
            Token::NumberLiteral(text.parse().unwrap_or(0))
        }
    }

    fn read_identifier(&mut self) -> Token {
        let start = self.position;
        while self.peek().is_alphanumeric() || self.peek() == '_' {
            self.advance();
        }
        let text = &self.input[start..self.position];
        Self::keyword_or_identifier(text)
    }

    fn keyword_or_identifier(text: &str) -> Token {
        match text.to_uppercase().as_str() {
            "SELECT" => Token::Select,
            "INSERT" => Token::Insert,
            "UPDATE" => Token::Update,
            "DELETE" => Token::Delete,
            "CREATE" => Token::Create,
            "DROP" => Token::Drop,
            "TABLE" => Token::Table,
            "INDEX" => Token::Index,
            "FROM" => Token::From,
            "WHERE" => Token::Where,
            "SET" => Token::Set,
            "VALUES" => Token::Values,
            "INTO" => Token::Into,
            "ON" => Token::On,
            "AND" => Token::And,
            "OR" => Token::Or,
            "NOT" => Token::Not,
            "NULL" => Token::Null,
            "TRUE" => Token::True,
            "FALSE" => Token::False,
            "BEGIN" => Token::Begin,
            "COMMIT" => Token::Commit,
            "ROLLBACK" => Token::Rollback,
            "TRANSACTION" => Token::Transaction,
            "PRIMARY" => Token::Primary,
            "KEY" => Token::Key,
            "INTEGER" => Token::Integer,
            "TEXT" => Token::Text,
            "BLOB" => Token::Blob,
            "VECTOR" => Token::Vector,
            "LIMIT" => Token::Limit,
            "OFFSET" => Token::Offset,
            "ORDER" => Token::Order,
            "BY" => Token::By,
            "ASC" => Token::Asc,
            "DESC" => Token::Desc,
            "COUNT" => Token::Count,
            "SUM" => Token::Sum,
            "AVG" => Token::Avg,
            "MIN" => Token::Min,
            "MAX" => Token::Max,
            "JOIN" => Token::Join,
            "INNER" => Token::Inner,
            "LEFT" => Token::Left,
            "GROUP" => Token::Group,
            "HAVING" => Token::Having,
            "USING" => Token::Using,
            "UNIQUE" => Token::Unique,
            "EXISTS" => Token::Exists,
            "IN" => Token::In,
            // Foreign key keywords
            "FOREIGN" => Token::Foreign,
            "REFERENCES" => Token::References,
            "CASCADE" => Token::Cascade,
            "RESTRICT" => Token::Restrict,
            "DEFAULT" => Token::Default,
            "ACTION" => Token::Action,
            "NO" => Token::No,
            "DEFERRABLE" => Token::Deferrable,
            "DEFERRED" => Token::Deferred,
            "IMMEDIATE" => Token::Immediate,
            // ALTER TABLE keywords
            "ALTER" => Token::Alter,
            "ADD" => Token::Add,
            "COLUMN" => Token::Column,
            "RENAME" => Token::Rename,
            "TO" => Token::To,
            // View keywords
            "VIEW" => Token::View,
            "AS" => Token::As,
            "IF" => Token::If,
            // CTE keywords
            "WITH" => Token::With,
            "RECURSIVE" => Token::Recursive,
            _ => Token::Identifier(text.to_string()),
        }
    }

    fn skip_whitespace(&mut self) {
        while self.peek().is_whitespace() {
            self.advance();
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_keywords() {
        let mut tokenizer = Tokenizer::new("SELECT FROM WHERE");
        assert!(matches!(tokenizer.next_token(), Token::Select));
        assert!(matches!(tokenizer.next_token(), Token::From));
        assert!(matches!(tokenizer.next_token(), Token::Where));
    }

    #[test]
    fn test_identifier() {
        let mut tokenizer = Tokenizer::new("users");
        assert!(matches!(tokenizer.next_token(), Token::Identifier(s) if s == "users"));
    }

    #[test]
    fn test_string() {
        let mut tokenizer = Tokenizer::new("'hello'");
        assert!(matches!(tokenizer.next_token(), Token::StringLiteral(s) if s == "hello"));
    }

    #[test]
    fn test_number() {
        let mut tokenizer = Tokenizer::new("42");
        assert!(matches!(tokenizer.next_token(), Token::NumberLiteral(42)));
    }
}
