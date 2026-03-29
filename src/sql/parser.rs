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
            Token::Select | Token::With => self.parse_select(),
            Token::Insert => self.parse_insert(),
            Token::Update => self.parse_update(),
            Token::Delete => self.parse_delete(),
            Token::Create => self.parse_create(),
            Token::Drop => self.parse_drop(),
            Token::Alter => self.parse_alter(),
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
        // Parse optional WITH clause (CTEs)
        let ctes = if self.match_token(Token::With) {
            self.parse_cte_list()?
        } else {
            Vec::new()
        };

        self.consume(Token::Select)?;
        let columns = self.parse_select_columns()?;
        self.consume(Token::From)?;
        let table = self.consume_identifier()?;

        // Parse JOIN clauses
        let mut joins = Vec::new();
        loop {
            let join_type = if self.match_token(Token::Join) {
                Some(crate::sql::ast::JoinType::Inner)
            } else if self.match_token(Token::Inner) {
                self.consume(Token::Join)?;
                Some(crate::sql::ast::JoinType::Inner)
            } else if self.match_token(Token::Left) {
                self.consume(Token::Join)?;
                Some(crate::sql::ast::JoinType::Left)
            } else {
                None
            };

            if let Some(join_type) = join_type {
                let join_table = self.consume_identifier()?;
                self.consume(Token::On)?;
                let on_condition = self.parse_expression()?;
                joins.push(crate::sql::ast::Join {
                    table: join_table,
                    join_type,
                    on_condition,
                });
            } else {
                break;
            }
        }

        let where_clause = if self.match_token(Token::Where) {
            Some(self.parse_expression()?)
        } else {
            None
        };

        let group_by = if self.match_token(Token::Group) {
            self.consume(Token::By)?;
            self.parse_column_list()?
        } else {
            Vec::new()
        };

        let having = if self.match_token(Token::Having) {
            Some(self.parse_expression()?)
        } else {
            None
        };

        let order_by = if self.match_token(Token::Order) {
            self.consume(Token::By)?;
            self.parse_order_by_list()?
        } else {
            Vec::new()
        };

        let limit = if self.match_token(Token::Limit) {
            if let Token::NumberLiteral(n) = &self.current {
                let n = *n;
                self.advance();
                Some(n)
            } else {
                return Err(ParseError::UnexpectedToken(format!("{:?}", self.current)));
            }
        } else {
            None
        };

        let offset = if self.match_token(Token::Offset) {
            if let Token::NumberLiteral(n) = &self.current {
                let n = *n;
                self.advance();
                Some(n)
            } else {
                return Err(ParseError::UnexpectedToken(format!("{:?}", self.current)));
            }
        } else {
            None
        };

        Ok(Statement::Select(SelectStmt {
            ctes,
            columns,
            from: table,
            joins,
            where_clause,
            group_by,
            having,
            order_by,
            limit,
            offset,
        }))
    }

    /// Parse SELECT statement and return SelectStmt directly (for subqueries)
    fn parse_select_stmt(&mut self) -> Result<SelectStmt> {
        // Parse optional WITH clause (CTEs)
        let ctes = if self.match_token(Token::With) {
            self.parse_cte_list()?
        } else {
            Vec::new()
        };

        self.consume(Token::Select)?;
        let columns = self.parse_select_columns()?;
        self.consume(Token::From)?;
        let table = self.consume_identifier()?;

        // Parse JOIN clauses
        let mut joins = Vec::new();
        loop {
            let join_type = if self.match_token(Token::Join) {
                Some(crate::sql::ast::JoinType::Inner)
            } else if self.match_token(Token::Inner) {
                self.consume(Token::Join)?;
                Some(crate::sql::ast::JoinType::Inner)
            } else if self.match_token(Token::Left) {
                self.consume(Token::Join)?;
                Some(crate::sql::ast::JoinType::Left)
            } else {
                None
            };

            if let Some(join_type) = join_type {
                let join_table = self.consume_identifier()?;
                self.consume(Token::On)?;
                let on_condition = self.parse_expression()?;
                joins.push(crate::sql::ast::Join {
                    table: join_table,
                    join_type,
                    on_condition,
                });
            } else {
                break;
            }
        }

        let where_clause = if self.match_token(Token::Where) {
            Some(self.parse_expression()?)
        } else {
            None
        };

        let group_by = if self.match_token(Token::Group) {
            self.consume(Token::By)?;
            self.parse_column_list()?
        } else {
            Vec::new()
        };

        let having = if self.match_token(Token::Having) {
            Some(self.parse_expression()?)
        } else {
            None
        };

        let order_by = if self.match_token(Token::Order) {
            self.consume(Token::By)?;
            self.parse_order_by_list()?
        } else {
            Vec::new()
        };

        let limit = if self.match_token(Token::Limit) {
            if let Token::NumberLiteral(n) = &self.current {
                let n = *n;
                self.advance();
                Some(n)
            } else {
                return Err(ParseError::UnexpectedToken(format!("{:?}", self.current)));
            }
        } else {
            None
        };

        let offset = if self.match_token(Token::Offset) {
            if let Token::NumberLiteral(n) = &self.current {
                let n = *n;
                self.advance();
                Some(n)
            } else {
                return Err(ParseError::UnexpectedToken(format!("{:?}", self.current)));
            }
        } else {
            None
        };

        Ok(SelectStmt {
            ctes,
            columns,
            from: table,
            joins,
            where_clause,
            group_by,
            having,
            order_by,
            limit,
            offset,
        })
    }

    /// Parse CTE list for WITH clause
    fn parse_cte_list(&mut self) -> Result<Vec<CommonTableExpr>> {
        let mut ctes = Vec::new();
        
        // Check for RECURSIVE
        let recursive = self.match_token(Token::Recursive);
        
        loop {
            let name = self.consume_identifier()?;
            
            // Optional column list: WITH cte (col1, col2) AS ...
            let columns = if self.match_token(Token::LParen) {
                let cols = self.parse_column_list()?;
                self.consume(Token::RParen)?;
                Some(cols)
            } else {
                None
            };
            
            self.consume(Token::As)?;
            self.consume(Token::LParen)?;
            
            // Parse the CTE query (which may itself contain WITH, but we handle that)
            let query = self.parse_select_stmt()?;
            
            self.consume(Token::RParen)?;
            
            ctes.push(CommonTableExpr {
                name,
                columns,
                query,
                recursive,
            });
            
            if !self.match_token(Token::Comma) {
                break;
            }
        }
        
        Ok(ctes)
    }

    fn parse_select_columns(&mut self) -> Result<Vec<SelectColumn>> {
        let mut columns = Vec::new();

        if self.match_token(Token::Star) {
            columns.push(SelectColumn::All);
        } else {
            loop {
                // Check for aggregate functions
                let (select_col, is_aggregate) = match &self.current {
                    Token::Count => {
                        self.advance();
                        self.consume(Token::LParen)?;
                        let agg = if self.match_token(Token::Star) {
                            AggregateFunc::CountStar
                        } else {
                            let expr = self.parse_expression()?;
                            AggregateFunc::Count(expr)
                        };
                        (SelectColumn::Aggregate(agg, None), true)
                    }
                    Token::Sum => {
                        self.advance();
                        self.consume(Token::LParen)?;
                        let expr = self.parse_expression()?;
                        (SelectColumn::Aggregate(AggregateFunc::Sum(expr), None), true)
                    }
                    Token::Avg => {
                        self.advance();
                        self.consume(Token::LParen)?;
                        let expr = self.parse_expression()?;
                        (SelectColumn::Aggregate(AggregateFunc::Avg(expr), None), true)
                    }
                    Token::Min => {
                        self.advance();
                        self.consume(Token::LParen)?;
                        let expr = self.parse_expression()?;
                        (SelectColumn::Aggregate(AggregateFunc::Min(expr), None), true)
                    }
                    Token::Max => {
                        self.advance();
                        self.consume(Token::LParen)?;
                        let expr = self.parse_expression()?;
                        (SelectColumn::Aggregate(AggregateFunc::Max(expr), None), true)
                    }
                    _ => {
                        let expr = self.parse_expression()?;
                        let alias = self.parse_optional_alias()?;
                        (SelectColumn::Expression(expr, alias), false)
                    }
                };

                // Consume closing paren for aggregate functions
                let select_col = if is_aggregate {
                    self.consume(Token::RParen)?;
                    // Parse optional alias for aggregates
                    let alias = self.parse_optional_alias()?;
                    match select_col {
                        SelectColumn::Aggregate(func, _) => SelectColumn::Aggregate(func, alias),
                        _ => select_col,
                    }
                } else {
                    select_col
                };

                columns.push(select_col);

                if !self.match_token(Token::Comma) {
                    break;
                }
            }
        }

        Ok(columns)
    }

    fn parse_order_by_list(&mut self) -> Result<Vec<OrderBy>> {
        let mut order_by = Vec::new();
        loop {
            let column = self.consume_identifier()?;
            let descending = if self.match_token(Token::Desc) {
                true
            } else {
                self.match_token(Token::Asc); // ASC is default, consume if present
                false
            };
            order_by.push(OrderBy {
                column,
                descending,
            });
            if !self.match_token(Token::Comma) {
                break;
            }
        }
        Ok(order_by)
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
        self.consume(Token::Create)?;
        match &self.current {
            Token::Table => self.parse_create_table(),
            Token::View => self.parse_create_view(),
            Token::Index => self.parse_create_index(false),
            Token::Unique => {
                self.consume(Token::Unique)?;
                self.parse_create_index(true)
            }
            _ => Err(ParseError::UnexpectedToken(format!("{:?}", self.current))),
        }
    }

    fn parse_create_view(&mut self) -> Result<Statement> {
        self.consume(Token::View)?;
        let name = self.consume_identifier()?;

        // Optional column list: CREATE VIEW v (col1, col2) AS ...
        let columns = if self.match_token(Token::LParen) {
            let cols = self.parse_column_list()?;
            self.consume(Token::RParen)?;
            Some(cols)
        } else {
            None
        };

        self.consume(Token::As)?;

        // Parse the view definition query
        let query = self.parse_select_stmt()?;

        Ok(Statement::CreateView(CreateViewStmt {
            name,
            columns,
            query,
        }))
    }

    fn parse_create_table(&mut self) -> Result<Statement> {
        self.consume(Token::Table)?;
        let table = self.consume_identifier()?;
        self.consume(Token::LParen)?;

        let mut columns = Vec::new();
        let mut foreign_keys = Vec::new();
        
        loop {
            // Check for table-level constraint (FOREIGN KEY)
            if self.match_token(Token::Foreign) {
                self.consume(Token::Key)?;
                let fk = self.parse_table_foreign_key()?;
                foreign_keys.push(fk);
            } else {
                // Column definition
                let name = self.consume_identifier()?;
                let data_type = self.parse_data_type()?;
                
                let mut primary_key = false;
                let mut foreign_key = None;
                
                // Parse column constraints
                loop {
                    if self.match_token(Token::Primary) {
                        self.consume(Token::Key)?;
                        primary_key = true;
                    } else if self.match_token(Token::References) {
                        foreign_key = Some(self.parse_column_foreign_key()?);
                    } else if self.match_token(Token::Not) {
                        self.consume(Token::Null)?;
                        // nullable = false; // TODO: track nullable
                    } else if self.match_token(Token::Null) {
                        // nullable = true; // default
                    } else if self.match_token(Token::Unique) {
                        // TODO: track unique constraint
                    } else {
                        break;
                    }
                }

                columns.push(ColumnDef {
                    name,
                    data_type,
                    nullable: true,
                    primary_key,
                    foreign_key,
                });
            }

            if !self.match_token(Token::Comma) {
                break;
            }
        }

        self.consume(Token::RParen)?;

        Ok(Statement::CreateTable(CreateTableStmt { table, columns, foreign_keys }))
    }

    /// Parse column-level foreign key: REFERENCES table(column) [ON DELETE action] [ON UPDATE action]
    fn parse_column_foreign_key(&mut self) -> Result<ColumnForeignKey> {
        let ref_table = self.consume_identifier()?;
        
        // Optional column specification
        let ref_column = if self.match_token(Token::LParen) {
            let col = self.consume_identifier()?;
            self.consume(Token::RParen)?;
            col
        } else {
            // Default to "id" or "rowid" - will resolve later
            "rowid".to_string()
        };

        let mut on_delete = ForeignKeyAction::default();
        let mut on_update = ForeignKeyAction::default();

        // Parse ON DELETE / ON UPDATE clauses
        loop {
            if self.match_token(Token::On) {
                if self.match_token(Token::Delete) {
                    on_delete = self.parse_foreign_key_action()?;
                } else if self.match_token(Token::Update) {
                    on_update = self.parse_foreign_key_action()?;
                } else {
                    return Err(ParseError::UnexpectedToken(
                        "Expected DELETE or UPDATE".to_string()
                    ));
                }
            } else {
                break;
            }
        }

        Ok(ColumnForeignKey {
            ref_table,
            ref_column,
            on_delete,
            on_update,
        })
    }

    /// Parse table-level foreign key: FOREIGN KEY (col1, col2) REFERENCES table(col1, col2)
    fn parse_table_foreign_key(&mut self) -> Result<ForeignKeyDef> {
        self.consume(Token::LParen)?;
        let mut columns = Vec::new();
        loop {
            columns.push(self.consume_identifier()?);
            if !self.match_token(Token::Comma) {
                break;
            }
        }
        self.consume(Token::RParen)?;

        self.consume(Token::References)?;
        let ref_table = self.consume_identifier()?;

        // Parse referenced columns
        self.consume(Token::LParen)?;
        let mut ref_columns = Vec::new();
        loop {
            ref_columns.push(self.consume_identifier()?);
            if !self.match_token(Token::Comma) {
                break;
            }
        }
        self.consume(Token::RParen)?;

        let mut on_delete = ForeignKeyAction::default();
        let mut on_update = ForeignKeyAction::default();

        loop {
            if self.match_token(Token::On) {
                if self.match_token(Token::Delete) {
                    on_delete = self.parse_foreign_key_action()?;
                } else if self.match_token(Token::Update) {
                    on_update = self.parse_foreign_key_action()?;
                } else {
                    return Err(ParseError::UnexpectedToken(
                        "Expected DELETE or UPDATE".to_string()
                    ));
                }
            } else {
                break;
            }
        }

        Ok(ForeignKeyDef {
            columns,
            ref_table,
            ref_columns,
            on_delete,
            on_update,
        })
    }

    /// Parse foreign key action: CASCADE, SET NULL, SET DEFAULT, RESTRICT, NO ACTION
    fn parse_foreign_key_action(&mut self) -> Result<ForeignKeyAction> {
        if self.match_token(Token::Cascade) {
            Ok(ForeignKeyAction::Cascade)
        } else if self.match_token(Token::Set) {
            if self.match_token(Token::Null) {
                Ok(ForeignKeyAction::SetNull)
            } else if self.match_token(Token::Default) {
                Ok(ForeignKeyAction::SetDefault)
            } else {
                Err(ParseError::UnexpectedToken("Expected NULL or DEFAULT".to_string()))
            }
        } else if self.match_token(Token::Restrict) {
            Ok(ForeignKeyAction::Restrict)
        } else if self.match_token(Token::No) {
            self.consume(Token::Action)?;
            Ok(ForeignKeyAction::NoAction)
        } else {
            Err(ParseError::UnexpectedToken(
                "Expected CASCADE, SET NULL, SET DEFAULT, RESTRICT, or NO ACTION".to_string()
            ))
        }
    }

    fn parse_create_index(&mut self, unique: bool) -> Result<Statement> {
        self.consume(Token::Index)?;
        let index_name = self.consume_identifier()?;
        self.consume(Token::On)?;
        let table = self.consume_identifier()?;
        self.consume(Token::LParen)?;
        let column = self.consume_identifier()?;
        self.consume(Token::RParen)?;

        let mut index_type = IndexType::BTree;
        if self.match_token(Token::Using) {
            let type_str = self.consume_identifier()?;
            if type_str.to_uppercase() == "HNSW" {
                index_type = IndexType::HNSW;
            }
        }

        Ok(Statement::CreateIndex(CreateIndexStmt {
            index_name,
            table,
            column,
            unique,
            index_type,
        }))
    }

    fn parse_drop(&mut self) -> Result<Statement> {
        self.advance();
        match &self.current {
            Token::Table => {
                self.advance();
                let table = self.consume_identifier()?;
                Ok(Statement::DropTable(DropTableStmt {
                    table,
                    if_exists: false,
                }))
            }
            Token::View => {
                self.advance();
                // Check for IF EXISTS
                let if_exists = if self.match_token(Token::If) {
                    self.consume(Token::Exists)?;
                    true
                } else {
                    false
                };
                let name = self.consume_identifier()?;
                Ok(Statement::DropView(DropViewStmt {
                    name,
                    if_exists,
                }))
            }
            _ => Err(ParseError::UnexpectedToken(format!("{:?}", self.current))),
        }
    }

    fn parse_alter(&mut self) -> Result<Statement> {
        self.advance();
        self.consume(Token::Table)?;
        let table = self.consume_identifier()?;

        if self.match_token(Token::Add) {
            // ALTER TABLE ... ADD COLUMN ...
            self.match_token(Token::Column); // Optional COLUMN keyword
            let column = self.parse_column_def()?;
            Ok(Statement::AlterTable(AlterTableStmt::AddColumn { table, column }))
        } else if self.match_token(Token::Drop) {
            // ALTER TABLE ... DROP COLUMN ...
            self.match_token(Token::Column); // Optional COLUMN keyword
            let column = self.consume_identifier()?;
            Ok(Statement::AlterTable(AlterTableStmt::DropColumn { table, column }))
        } else if self.match_token(Token::Rename) {
            if self.match_token(Token::To) {
                // ALTER TABLE ... RENAME TO new_name
                let new_name = self.consume_identifier()?;
                Ok(Statement::AlterTable(AlterTableStmt::RenameTable { table, new_name }))
            } else if self.match_token(Token::Column) {
                // ALTER TABLE ... RENAME COLUMN old_name TO new_name
                let old_name = self.consume_identifier()?;
                self.consume(Token::To)?;
                let new_name = self.consume_identifier()?;
                Ok(Statement::AlterTable(AlterTableStmt::RenameColumn { table, old_name, new_name }))
            } else {
                Err(ParseError::UnexpectedToken(
                    "Expected TO or COLUMN after RENAME".to_string()
                ))
            }
        } else {
            Err(ParseError::UnexpectedToken(
                "Expected ADD, DROP, or RENAME".to_string()
            ))
        }
    }

    fn parse_column_def(&mut self) -> Result<ColumnDef> {
        let name = self.consume_identifier()?;
        let data_type = self.parse_data_type()?;
        
        let mut primary_key = false;
        let mut foreign_key = None;
        
        // Parse column constraints
        loop {
            if self.match_token(Token::Primary) {
                self.consume(Token::Key)?;
                primary_key = true;
            } else if self.match_token(Token::References) {
                foreign_key = Some(self.parse_column_foreign_key()?);
            } else if self.match_token(Token::Not) {
                self.consume(Token::Null)?;
            } else if self.match_token(Token::Null) {
                // nullable = true;
            } else if self.match_token(Token::Unique) {
                // TODO: track unique
            } else {
                break;
            }
        }

        Ok(ColumnDef {
            name,
            data_type,
            nullable: true,
            primary_key,
            foreign_key,
        })
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
        let mut left = self.parse_in()?;

        loop {
            let op = match &self.current {
                Token::Less => BinaryOp::Less,
                Token::Greater => BinaryOp::Greater,
                Token::LessEqual => BinaryOp::LessEqual,
                Token::GreaterEqual => BinaryOp::GreaterEqual,
                _ => break,
            };
            self.advance();
            let right = self.parse_in()?;
            left = Expression::Binary {
                left: Box::new(left),
                op,
                right: Box::new(right),
            };
        }

        Ok(left)
    }

    /// Parse IN expression: expr IN (subquery) or expr IN (val1, val2, ...)
    fn parse_in(&mut self) -> Result<Expression> {
        let left = self.parse_term()?;

        if self.match_token(Token::In) {
            self.consume(Token::LParen)?;
            
            // Check if it's a subquery
            if self.current == Token::Select {
                let subquery = self.parse_select_stmt()?;
                self.consume(Token::RParen)?;
                Ok(Expression::Subquery(SubqueryExpr::In {
                    expr: Box::new(left),
                    subquery: Box::new(subquery),
                }))
            } else {
                // Regular IN with value list - convert to OR chain for now
                let mut values = vec![self.parse_expression()?];
                while self.match_token(Token::Comma) {
                    values.push(self.parse_expression()?);
                }
                self.consume(Token::RParen)?;
                
                // Convert to OR chain: a IN (1,2,3) -> a=1 OR a=2 OR a=3
                let mut result = Expression::Binary {
                    left: Box::new(left.clone()),
                    op: BinaryOp::Equal,
                    right: Box::new(values.remove(0)),
                };
                for val in values {
                    result = Expression::Binary {
                        left: Box::new(result),
                        op: BinaryOp::Or,
                        right: Box::new(Expression::Binary {
                            left: Box::new(left.clone()),
                            op: BinaryOp::Equal,
                            right: Box::new(val),
                        }),
                    };
                }
                Ok(result)
            }
        } else {
            Ok(left)
        }
    }

    fn parse_term(&mut self) -> Result<Expression> {
        let mut left = self.parse_factor()?;

        loop {
            let op = match &self.current {
                Token::Plus => BinaryOp::Add,
                Token::Minus => BinaryOp::Sub,
                _ => break,
            };
            self.advance();
            let right = self.parse_factor()?;
            left = Expression::Binary {
                left: Box::new(left),
                op,
                right: Box::new(right),
            };
        }

        Ok(left)
    }

    fn parse_factor(&mut self) -> Result<Expression> {
        let mut left = self.parse_primary()?;

        loop {
            let op = match &self.current {
                Token::Star => BinaryOp::Mul,
                Token::Slash => BinaryOp::Div,
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
            Token::FloatLiteral(f) => {
                let f = *f;
                self.advance();
                Ok(Expression::Float(f))
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
            Token::Not => {
                // Check for NOT EXISTS
                self.advance();
                if self.match_token(Token::Exists) {
                    self.consume(Token::LParen)?;
                    let subquery = self.parse_select_stmt()?;
                    self.consume(Token::RParen)?;
                    Ok(Expression::Subquery(SubqueryExpr::NotExists(Box::new(subquery))))
                } else {
                    Err(ParseError::UnexpectedToken("Expected EXISTS after NOT".to_string()))
                }
            }
            Token::Exists => {
                self.advance();
                self.consume(Token::LParen)?;
                let subquery = self.parse_select_stmt()?;
                self.consume(Token::RParen)?;
                Ok(Expression::Subquery(SubqueryExpr::Exists(Box::new(subquery))))
            }
            Token::QuestionMark => {
                // 占位符 `?` - 使用位置索引（从 1 开始）
                self.advance();
                // 这里简化处理，实际应该跟踪当前语句中的占位符位置
                Ok(Expression::Placeholder(1))
            }
            Token::Identifier(name) => {
                let name = name.clone();
                self.advance();
                if self.match_token(Token::LParen) {
                    let mut args = Vec::new();
                    if self.current != Token::RParen {
                        args.push(self.parse_expression()?);
                        while self.match_token(Token::Comma) {
                            args.push(self.parse_expression()?);
                        }
                    }
                    self.consume(Token::RParen)?;
                    Ok(Expression::FunctionCall { name, args })
                } else {
                    Ok(Expression::Column(name))
                }
            }
            Token::LParen => {
                self.advance();
                // Check for subquery (SELECT ...)
                if self.current == Token::Select {
                    let subquery = self.parse_select_stmt()?;
                    self.consume(Token::RParen)?;
                    Ok(Expression::Subquery(SubqueryExpr::Scalar(Box::new(subquery))))
                } else {
                    let expr = self.parse_expression()?;
                    self.consume(Token::RParen)?;
                    Ok(expr)
                }
            }
            Token::LBracket => {
                self.advance();
                let mut elements = Vec::new();
                if self.current != Token::RBracket {
                    elements.push(self.parse_expression()?);
                    while self.match_token(Token::Comma) {
                        elements.push(self.parse_expression()?);
                    }
                }
                self.consume(Token::RBracket)?;
                Ok(Expression::Vector(elements))
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
            Token::Blob => {
                self.advance();
                Ok(DataType::Blob)
            }
            Token::Vector => {
                self.advance();
                let mut dim = 0;
                if self.match_token(Token::LParen) {
                    if let Token::NumberLiteral(n) = self.current {
                        dim = n as u32;
                        self.advance();
                    }
                    self.consume(Token::RParen)?;
                }
                Ok(DataType::Vector(dim))
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

    /// Parse optional AS alias
    fn parse_optional_alias(&mut self) -> Result<Option<String>> {
        if self.match_token(Token::As) {
            Ok(Some(self.consume_identifier()?))
        } else {
            Ok(None)
        }
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
    fn test_parse_select_with_where() {
        let mut parser = Parser::new("SELECT * FROM users WHERE id = 1").unwrap();
        let stmt = parser.parse().unwrap();
        assert!(matches!(stmt, Statement::Select(_)));
    }

    #[test]
    fn test_parse_select_with_order_by() {
        let mut parser = Parser::new("SELECT * FROM users ORDER BY name DESC").unwrap();
        let stmt = parser.parse().unwrap();
        assert!(matches!(stmt, Statement::Select(_)));
    }

    #[test]
    fn test_parse_select_with_limit() {
        let mut parser = Parser::new("SELECT * FROM users LIMIT 10").unwrap();
        let stmt = parser.parse().unwrap();
        assert!(matches!(stmt, Statement::Select(_)));
    }

    #[test]
    fn test_parse_select_with_join() {
        let mut parser = Parser::new("SELECT * FROM users JOIN orders ON users.id = orders.user_id").unwrap();
        let stmt = parser.parse().unwrap();
        assert!(matches!(stmt, Statement::Select(_)));
    }

    #[test]
    fn test_parse_select_with_group_by() {
        let mut parser = Parser::new("SELECT COUNT(*) FROM users GROUP BY age").unwrap();
        let stmt = parser.parse().unwrap();
        assert!(matches!(stmt, Statement::Select(_)));
    }

    #[test]
    fn test_parse_select_columns() {
        let mut parser = Parser::new("SELECT id, name, age FROM users").unwrap();
        let stmt = parser.parse().unwrap();
        match stmt {
            Statement::Select(s) => {
                assert_eq!(s.columns.len(), 3);
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_insert() {
        let mut parser = Parser::new("INSERT INTO users VALUES (1, 'Alice')").unwrap();
        let stmt = parser.parse().unwrap();
        assert!(matches!(stmt, Statement::Insert(_)));
    }

    #[test]
    fn test_parse_insert_with_columns() {
        let mut parser = Parser::new("INSERT INTO users (id, name) VALUES (1, 'Alice')").unwrap();
        let stmt = parser.parse().unwrap();
        match stmt {
            Statement::Insert(i) => {
                assert!(i.columns.is_some());
                assert_eq!(i.columns.unwrap().len(), 2);
            }
            _ => panic!("Expected Insert"),
        }
    }

    #[test]
    fn test_parse_insert_multiple_rows() {
        let mut parser = Parser::new("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')").unwrap();
        let stmt = parser.parse().unwrap();
        match stmt {
            Statement::Insert(i) => {
                assert_eq!(i.values.len(), 2);
            }
            _ => panic!("Expected Insert"),
        }
    }

    #[test]
    fn test_parse_update() {
        let mut parser = Parser::new("UPDATE users SET name = 'Bob' WHERE id = 1").unwrap();
        let stmt = parser.parse().unwrap();
        assert!(matches!(stmt, Statement::Update(_)));
    }

    #[test]
    fn test_parse_update_multiple_columns() {
        let mut parser = Parser::new("UPDATE users SET name = 'Bob', age = 30 WHERE id = 1").unwrap();
        let stmt = parser.parse().unwrap();
        match stmt {
            Statement::Update(u) => {
                assert_eq!(u.set_clauses.len(), 2);
            }
            _ => panic!("Expected Update"),
        }
    }

    #[test]
    fn test_parse_delete() {
        let mut parser = Parser::new("DELETE FROM users WHERE id = 1").unwrap();
        let stmt = parser.parse().unwrap();
        assert!(matches!(stmt, Statement::Delete(_)));
    }

    #[test]
    fn test_parse_delete_without_where() {
        let mut parser = Parser::new("DELETE FROM users").unwrap();
        let stmt = parser.parse().unwrap();
        assert!(matches!(stmt, Statement::Delete(_)));
    }

    #[test]
    fn test_parse_create_table() {
        let mut parser = Parser::new("CREATE TABLE users (id INTEGER, name TEXT)").unwrap();
        let stmt = parser.parse().unwrap();
        assert!(matches!(stmt, Statement::CreateTable(_)));
    }

    #[test]
    fn test_parse_create_table_with_primary_key() {
        let mut parser = Parser::new("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)").unwrap();
        let stmt = parser.parse().unwrap();
        match stmt {
            Statement::CreateTable(ct) => {
                assert!(ct.columns[0].primary_key);
            }
            _ => panic!("Expected CreateTable"),
        }
    }

    #[test]
    fn test_parse_create_table_with_foreign_key() {
        let mut parser = Parser::new("CREATE TABLE orders (id INTEGER, user_id INTEGER REFERENCES users(id))").unwrap();
        let stmt = parser.parse().unwrap();
        assert!(matches!(stmt, Statement::CreateTable(_)));
    }

    #[test]
    fn test_parse_create_index() {
        let mut parser = Parser::new("CREATE INDEX idx_name ON users(name)").unwrap();
        let stmt = parser.parse().unwrap();
        assert!(matches!(stmt, Statement::CreateIndex(_)));
    }

    #[test]
    fn test_parse_create_unique_index() {
        let mut parser = Parser::new("CREATE UNIQUE INDEX idx_name ON users(name)").unwrap();
        let stmt = parser.parse().unwrap();
        match stmt {
            Statement::CreateIndex(ci) => {
                assert!(ci.unique);
            }
            _ => panic!("Expected CreateIndex"),
        }
    }

    #[test]
    fn test_parse_drop_table() {
        let mut parser = Parser::new("DROP TABLE users").unwrap();
        let stmt = parser.parse().unwrap();
        assert!(matches!(stmt, Statement::DropTable(_)));
    }

    #[test]
    fn test_parse_alter_table_add_column() {
        let mut parser = Parser::new("ALTER TABLE users ADD COLUMN email TEXT").unwrap();
        let stmt = parser.parse().unwrap();
        assert!(matches!(stmt, Statement::AlterTable(_)));
    }

    #[test]
    fn test_parse_alter_table_drop_column() {
        let mut parser = Parser::new("ALTER TABLE users DROP COLUMN email").unwrap();
        let stmt = parser.parse().unwrap();
        assert!(matches!(stmt, Statement::AlterTable(_)));
    }

    #[test]
    fn test_parse_alter_table_rename() {
        let mut parser = Parser::new("ALTER TABLE users RENAME TO customers").unwrap();
        let stmt = parser.parse().unwrap();
        assert!(matches!(stmt, Statement::AlterTable(_)));
    }

    #[test]
    fn test_parse_alter_table_rename_column() {
        let mut parser = Parser::new("ALTER TABLE users RENAME COLUMN name TO full_name").unwrap();
        let stmt = parser.parse().unwrap();
        assert!(matches!(stmt, Statement::AlterTable(_)));
    }

    #[test]
    fn test_parse_begin() {
        let mut parser = Parser::new("BEGIN").unwrap();
        let stmt = parser.parse().unwrap();
        assert!(matches!(stmt, Statement::BeginTransaction));
    }

    #[test]
    fn test_parse_commit() {
        let mut parser = Parser::new("COMMIT").unwrap();
        let stmt = parser.parse().unwrap();
        assert!(matches!(stmt, Statement::Commit));
    }

    #[test]
    fn test_parse_rollback() {
        let mut parser = Parser::new("ROLLBACK").unwrap();
        let stmt = parser.parse().unwrap();
        assert!(matches!(stmt, Statement::Rollback));
    }

    #[test]
    fn test_parse_invalid_syntax() {
        let mut parser = Parser::new("SELECT * FROM").unwrap();
        let result = parser.parse();
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_empty() {
        let mut parser = Parser::new("").unwrap();
        let result = parser.parse();
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_scalar_subquery() {
        let mut parser = Parser::new("SELECT (SELECT COUNT(*) FROM users) AS total FROM orders").unwrap();
        let stmt = parser.parse().unwrap();
        match stmt {
            Statement::Select(s) => {
                assert_eq!(s.columns.len(), 1);
                match &s.columns[0] {
                    SelectColumn::Expression(expr, _) => {
                        assert!(matches!(expr, Expression::Subquery(SubqueryExpr::Scalar(_))));
                    }
                    _ => panic!("Expected subquery expression"),
                }
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_in_subquery() {
        let mut parser = Parser::new("SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)").unwrap();
        let stmt = parser.parse().unwrap();
        match stmt {
            Statement::Select(s) => {
                assert!(s.where_clause.is_some());
                match s.where_clause.unwrap() {
                    Expression::Subquery(SubqueryExpr::In { .. }) => {}
                    _ => panic!("Expected IN subquery"),
                }
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_exists_subquery() {
        let mut parser = Parser::new("SELECT * FROM users WHERE EXISTS (SELECT 1 FROM orders)").unwrap();
        let stmt = parser.parse().unwrap();
        match stmt {
            Statement::Select(s) => {
                assert!(s.where_clause.is_some());
                match s.where_clause.unwrap() {
                    Expression::Subquery(SubqueryExpr::Exists(_)) => {}
                    _ => panic!("Expected EXISTS subquery"),
                }
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_not_exists_subquery() {
        let mut parser = Parser::new("SELECT * FROM users WHERE NOT EXISTS (SELECT 1 FROM orders)").unwrap();
        let stmt = parser.parse().unwrap();
        match stmt {
            Statement::Select(s) => {
                assert!(s.where_clause.is_some());
                match s.where_clause.unwrap() {
                    Expression::Subquery(SubqueryExpr::NotExists(_)) => {}
                    _ => panic!("Expected NOT EXISTS subquery"),
                }
            }
            _ => panic!("Expected Select"),
        }
    }
}
