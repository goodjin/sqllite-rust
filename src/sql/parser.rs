use crate::sql::ast::*;
use crate::sql::error::{ParseError, Result};
use crate::sql::token::Token;
use crate::sql::tokenizer::Tokenizer;

pub struct Parser<'a> {
    tokenizer: Tokenizer<'a>,
    current: Token,
    peek: Token,
}

impl<'a> Parser<'a> {
    pub fn new(input: &'a str) -> Result<Self> {
        let mut tokenizer = Tokenizer::new(input);
        let current = tokenizer.next_token();
        let peek = tokenizer.next_token();

        Ok(Self {
            tokenizer,
            current,
            peek,
        })
    }

    pub fn parse(&mut self) -> Result<Statement> {
        let stmt = match &self.current {
            Token::Select => self.parse_select(),
            Token::Insert => self.parse_insert(),
            Token::Update => self.parse_update(),
            Token::Delete => self.parse_delete(),
            Token::Create => self.parse_create(),
            Token::Drop => self.parse_drop(),
            Token::Begin => self.parse_begin(),
            Token::Commit => self.parse_commit(),
            Token::Rollback => self.parse_rollback(),
            _ => Err(ParseError::UnexpectedToken(format!("{:?}", self.current))),
        }?;

        if !matches!(self.current, Token::Semicolon | Token::Eof) {
            return Err(ParseError::ExpectedSemicolon);
        }

        Ok(stmt)
    }

    fn parse_select(&mut self) -> Result<Statement> {
        self.consume(Token::Select)?;
        let columns = self.parse_select_columns()?;
        self.consume(Token::From)?;
        let table = self.consume_identifier()?;

        let where_clause = if self.match_token(Token::Where) {
            Some(self.parse_expression()?)
        } else {
            None
        };

        Ok(Statement::Select(SelectStmt {
            columns,
            from: table,
            where_clause,
        }))
    }

    fn parse_select_columns(&mut self) -> Result<Vec<SelectColumn>> {
        let mut columns = Vec::new();

        if self.match_token(Token::Star) {
            columns.push(SelectColumn::All);
        } else {
            loop {
                let col = self.consume_identifier()?;
                columns.push(SelectColumn::Column(col));

                if !self.match_token(Token::Comma) {
                    break;
                }
            }
        }

        Ok(columns)
    }

    fn parse_insert(&mut self) -> Result<Statement> {
        self.consume(Token::Insert)?;
        self.consume(Token::Into)?;
        let table = self.consume_identifier()?;

        let columns = if self.match_token(Token::LParen) {
            let cols = self.parse_column_list()?;
            self.consume(Token::RParen)?;
            Some(cols)
        } else {
            None
        };

        self.consume(Token::Values)?;
        let values = self.parse_values_list()?;

        Ok(Statement::Insert(InsertStmt {
            table,
            columns,
            values,
        }))
    }

    fn parse_update(&mut self) -> Result<Statement> {
        self.consume(Token::Update)?;
        let table = self.consume_identifier()?;
        self.consume(Token::Set)?;

        let mut set_clauses = Vec::new();
        loop {
            let column = self.consume_identifier()?;
            self.consume(Token::Equal)?;
            let value = self.parse_expression()?;
            set_clauses.push(SetClause { column, value });

            if !self.match_token(Token::Comma) {
                break;
            }
        }

        let where_clause = if self.match_token(Token::Where) {
            Some(self.parse_expression()?)
        } else {
            None
        };

        Ok(Statement::Update(UpdateStmt {
            table,
            set_clauses,
            where_clause,
        }))
    }

    fn parse_delete(&mut self) -> Result<Statement> {
        self.consume(Token::Delete)?;
        self.consume(Token::From)?;
        let table = self.consume_identifier()?;

        let where_clause = if self.match_token(Token::Where) {
            Some(self.parse_expression()?)
        } else {
            None
        };

        Ok(Statement::Delete(DeleteStmt {
            table,
            where_clause,
        }))
    }

    fn parse_create(&mut self) -> Result<Statement> {
        self.advance();
        match &self.current {
            Token::Table => self.parse_create_table(),
            Token::Index => self.parse_create_index(),
            _ => Err(ParseError::UnexpectedToken(format!("{:?}", self.current))),
        }
    }

    fn parse_create_table(&mut self) -> Result<Statement> {
        self.consume(Token::Table)?;
        let table = self.consume_identifier()?;
        self.consume(Token::LParen)?;

        let mut columns = Vec::new();
        loop {
            let name = self.consume_identifier()?;
            let data_type = self.parse_data_type()?;
            columns.push(ColumnDef {
                name,
                data_type,
                nullable: true,
                primary_key: false,
            });

            if !self.match_token(Token::Comma) {
                break;
            }
        }

        self.consume(Token::RParen)?;

        Ok(Statement::CreateTable(CreateTableStmt { table, columns }))
    }

    fn parse_create_index(&mut self) -> Result<Statement> {
        self.consume(Token::Index)?;
        let index_name = self.consume_identifier()?;
        self.consume(Token::On)?;
        let table = self.consume_identifier()?;
        self.consume(Token::LParen)?;
        let column = self.consume_identifier()?;
        self.consume(Token::RParen)?;

        Ok(Statement::CreateIndex(CreateIndexStmt {
            index_name,
            table,
            column,
            unique: false,
        }))
    }

    fn parse_drop(&mut self) -> Result<Statement> {
        self.advance();
        self.consume(Token::Table)?;
        let table = self.consume_identifier()?;

        Ok(Statement::DropTable(DropTableStmt {
            table,
            if_exists: false,
        }))
    }

    fn parse_begin(&mut self) -> Result<Statement> {
        self.advance();
        if self.match_token(Token::Transaction) {}
        Ok(Statement::BeginTransaction)
    }

    fn parse_commit(&mut self) -> Result<Statement> {
        self.advance();
        Ok(Statement::Commit)
    }

    fn parse_rollback(&mut self) -> Result<Statement> {
        self.advance();
        Ok(Statement::Rollback)
    }

    fn parse_expression(&mut self) -> Result<Expression> {
        self.parse_or()
    }

    fn parse_or(&mut self) -> Result<Expression> {
        let mut left = self.parse_and()?;

        while self.match_token(Token::Or) {
            let right = self.parse_and()?;
            left = Expression::Binary {
                left: Box::new(left),
                op: BinaryOp::Or,
                right: Box::new(right),
            };
        }

        Ok(left)
    }

    fn parse_and(&mut self) -> Result<Expression> {
        let mut left = self.parse_equality()?;

        while self.match_token(Token::And) {
            let right = self.parse_equality()?;
            left = Expression::Binary {
                left: Box::new(left),
                op: BinaryOp::And,
                right: Box::new(right),
            };
        }

        Ok(left)
    }

    fn parse_equality(&mut self) -> Result<Expression> {
        let mut left = self.parse_comparison()?;

        loop {
            let op = match &self.current {
                Token::Equal => BinaryOp::Equal,
                Token::NotEqual => BinaryOp::NotEqual,
                _ => break,
            };
            self.advance();
            let right = self.parse_comparison()?;
            left = Expression::Binary {
                left: Box::new(left),
                op,
                right: Box::new(right),
            };
        }

        Ok(left)
    }

    fn parse_comparison(&mut self) -> Result<Expression> {
        let mut left = self.parse_primary()?;

        loop {
            let op = match &self.current {
                Token::Less => BinaryOp::Less,
                Token::Greater => BinaryOp::Greater,
                Token::LessEqual => BinaryOp::LessEqual,
                Token::GreaterEqual => BinaryOp::GreaterEqual,
                _ => break,
            };
            self.advance();
            let right = self.parse_primary()?;
            left = Expression::Binary {
                left: Box::new(left),
                op,
                right: Box::new(right),
            };
        }

        Ok(left)
    }

    fn parse_primary(&mut self) -> Result<Expression> {
        match &self.current {
            Token::NumberLiteral(n) => {
                let n = *n;
                self.advance();
                Ok(Expression::Integer(n))
            }
            Token::StringLiteral(s) => {
                let s = s.clone();
                self.advance();
                Ok(Expression::String(s))
            }
            Token::Null => {
                self.advance();
                Ok(Expression::Null)
            }
            Token::Identifier(name) => {
                let name = name.clone();
                self.advance();
                Ok(Expression::Column(name))
            }
            Token::LParen => {
                self.advance();
                let expr = self.parse_expression()?;
                self.consume(Token::RParen)?;
                Ok(expr)
            }
            _ => Err(ParseError::UnexpectedToken(format!("{:?}", self.current))),
        }
    }

    fn parse_data_type(&mut self) -> Result<DataType> {
        match &self.current {
            Token::Integer => {
                self.advance();
                Ok(DataType::Integer)
            }
            Token::Text => {
                self.advance();
                Ok(DataType::Text)
            }
            _ => Err(ParseError::UnexpectedToken(format!("{:?}", self.current))),
        }
    }

    fn parse_column_list(&mut self) -> Result<Vec<String>> {
        let mut columns = Vec::new();
        loop {
            columns.push(self.consume_identifier()?);
            if !self.match_token(Token::Comma) {
                break;
            }
        }
        Ok(columns)
    }

    fn parse_values_list(&mut self) -> Result<Vec<Vec<Expression>>> {
        let mut values_list = Vec::new();
        loop {
            self.consume(Token::LParen)?;
            let mut values = Vec::new();
            loop {
                values.push(self.parse_expression()?);
                if !self.match_token(Token::Comma) {
                    break;
                }
            }
            self.consume(Token::RParen)?;
            values_list.push(values);

            if !self.match_token(Token::Comma) {
                break;
            }
        }
        Ok(values_list)
    }

    fn advance(&mut self) {
        self.current = self.peek.clone();
        self.peek = self.tokenizer.next_token();
    }

    fn match_token(&mut self, token: Token) -> bool {
        if std::mem::discriminant(&self.current) == std::mem::discriminant(&token) {
            self.advance();
            true
        } else {
            false
        }
    }

    fn consume(&mut self, expected: Token) -> Result<()> {
        if std::mem::discriminant(&self.current) == std::mem::discriminant(&expected) {
            self.advance();
            Ok(())
        } else {
            Err(ParseError::ExpectedToken {
                expected: format!("{:?}", expected),
                found: format!("{:?}", self.current),
            })
        }
    }

    fn consume_identifier(&mut self) -> Result<String> {
        match &self.current {
            Token::Identifier(name) => {
                let name = name.clone();
                self.advance();
                Ok(name)
            }
            _ => Err(ParseError::ExpectedIdentifier),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_select() {
        let mut parser = Parser::new("SELECT * FROM users").unwrap();
        let stmt = parser.parse().unwrap();
        assert!(matches!(stmt, Statement::Select(_)));
    }

    #[test]
    fn test_parse_insert() {
        let mut parser = Parser::new("INSERT INTO users VALUES (1, 'Alice')").unwrap();
        let stmt = parser.parse().unwrap();
        assert!(matches!(stmt, Statement::Insert(_)));
    }

    #[test]
    fn test_parse_create_table() {
        let mut parser = Parser::new("CREATE TABLE users (id INTEGER, name TEXT)").unwrap();
        let stmt = parser.parse().unwrap();
        assert!(matches!(stmt, Statement::CreateTable(_)));
    }
}
