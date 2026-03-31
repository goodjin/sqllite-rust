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
            Token::Select | Token::With => self.parse_select_statement(),
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

    fn parse_select_statement(&mut self) -> Result<Statement> {
        let stmt = self.parse_select_stmt()?;
        Ok(Statement::Select(stmt))
    }

    fn parse_select(&mut self) -> Result<Statement> {
        let stmt = self.parse_select_stmt()?;
        Ok(Statement::Select(stmt))
    }

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
                Some(JoinType::Inner)
            } else if self.match_token(Token::Inner) {
                self.consume(Token::Join)?;
                Some(JoinType::Inner)
            } else if self.match_token(Token::Left) {
                self.consume(Token::Join)?;
                Some(JoinType::Left)
            } else {
                None
            };

            if let Some(join_type) = join_type {
                let join_table = self.consume_identifier()?;
                self.consume(Token::On)?;
                let on_condition = self.parse_expression()?;
                joins.push(Join {
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
            
            // Parse the CTE query
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
                // Check for window functions (P5-4)
                let select_col = if let Some(window_func) = self.try_parse_window_function()? {
                    SelectColumn::WindowFunc(window_func, None)
                } else {
                    // Check for aggregate functions
                    match &self.current {
                        Token::Count => {
                            self.advance();
                            self.consume(Token::LParen)?;
                            let agg = if self.match_token(Token::Star) {
                                AggregateFunc::CountStar
                            } else {
                                let expr = self.parse_expression()?;
                                AggregateFunc::Count(expr)
                            };
                            self.consume(Token::RParen)?;
                            let alias = self.parse_optional_alias()?;
                            SelectColumn::Aggregate(agg, alias)
                        }
                        Token::Sum => {
                            self.advance();
                            self.consume(Token::LParen)?;
                            let expr = self.parse_expression()?;
                            self.consume(Token::RParen)?;
                            let alias = self.parse_optional_alias()?;
                            SelectColumn::Aggregate(AggregateFunc::Sum(expr), alias)
                        }
                        Token::Avg => {
                            self.advance();
                            self.consume(Token::LParen)?;
                            let expr = self.parse_expression()?;
                            self.consume(Token::RParen)?;
                            let alias = self.parse_optional_alias()?;
                            SelectColumn::Aggregate(AggregateFunc::Avg(expr), alias)
                        }
                        Token::Min => {
                            self.advance();
                            self.consume(Token::LParen)?;
                            let expr = self.parse_expression()?;
                            self.consume(Token::RParen)?;
                            let alias = self.parse_optional_alias()?;
                            SelectColumn::Aggregate(AggregateFunc::Min(expr), alias)
                        }
                        Token::Max => {
                            self.advance();
                            self.consume(Token::LParen)?;
                            let expr = self.parse_expression()?;
                            self.consume(Token::RParen)?;
                            let alias = self.parse_optional_alias()?;
                            SelectColumn::Aggregate(AggregateFunc::Max(expr), alias)
                        }
                        _ => {
                            let expr = self.parse_expression()?;
                            let alias = self.parse_optional_alias()?;
                            SelectColumn::Expression(expr, alias)
                        }
                    }
                };

                columns.push(select_col);

                if !self.match_token(Token::Comma) {
                    break;
                }
            }
        }

        Ok(columns)
    }

    /// P5-4: Try to parse a window function
    fn try_parse_window_function(&mut self) -> Result<Option<WindowFunc>> {
        let func_type = match &self.current {
            Token::RowNumber => Some("ROW_NUMBER"),
            Token::Rank => Some("RANK"),
            Token::DenseRank => Some("DENSE_RANK"),
            Token::Lead => Some("LEAD"),
            Token::Lag => Some("LAG"),
            Token::FirstValue => Some("FIRST_VALUE"),
            Token::LastValue => Some("LAST_VALUE"),
            Token::NthValue => Some("NTH_VALUE"),
            _ => None,
        };

        if func_type.is_none() {
            return Ok(None);
        }

        let func_name = func_type.unwrap();
        self.advance();
        self.consume(Token::LParen)?;

        let window_func = match func_name {
            "ROW_NUMBER" => {
                self.consume(Token::RParen)?;
                let over = self.parse_window_spec()?;
                WindowFunc::RowNumber { over }
            }
            "RANK" => {
                self.consume(Token::RParen)?;
                let over = self.parse_window_spec()?;
                WindowFunc::Rank { over }
            }
            "DENSE_RANK" => {
                self.consume(Token::RParen)?;
                let over = self.parse_window_spec()?;
                WindowFunc::DenseRank { over }
            }
            "LEAD" => {
                let expr = Box::new(self.parse_expression()?);
                let offset = if self.match_token(Token::Comma) {
                    Some(Box::new(self.parse_expression()?))
                } else {
                    None
                };
                let default = if self.match_token(Token::Comma) {
                    Some(Box::new(self.parse_expression()?))
                } else {
                    None
                };
                self.consume(Token::RParen)?;
                let over = self.parse_window_spec()?;
                WindowFunc::Lead { expr, offset, default, over }
            }
            "LAG" => {
                let expr = Box::new(self.parse_expression()?);
                let offset = if self.match_token(Token::Comma) {
                    Some(Box::new(self.parse_expression()?))
                } else {
                    None
                };
                let default = if self.match_token(Token::Comma) {
                    Some(Box::new(self.parse_expression()?))
                } else {
                    None
                };
                self.consume(Token::RParen)?;
                let over = self.parse_window_spec()?;
                WindowFunc::Lag { expr, offset, default, over }
            }
            "FIRST_VALUE" => {
                let expr = Box::new(self.parse_expression()?);
                self.consume(Token::RParen)?;
                let over = self.parse_window_spec()?;
                WindowFunc::FirstValue { expr, over }
            }
            "LAST_VALUE" => {
                let expr = Box::new(self.parse_expression()?);
                self.consume(Token::RParen)?;
                let over = self.parse_window_spec()?;
                WindowFunc::LastValue { expr, over }
            }
            "NTH_VALUE" => {
                let expr = Box::new(self.parse_expression()?);
                self.consume(Token::Comma)?;
                let n = Box::new(self.parse_expression()?);
                self.consume(Token::RParen)?;
                let over = self.parse_window_spec()?;
                WindowFunc::NthValue { expr, n, over }
            }
            _ => return Ok(None),
        };

        Ok(Some(window_func))
    }

    /// P5-4: Parse window specification
    fn parse_window_spec(&mut self) -> Result<WindowSpec> {
        self.consume(Token::Over)?;
        self.consume(Token::LParen)?;

        let mut spec = WindowSpec::default();

        // Parse PARTITION BY
        if self.match_token(Token::Partition) {
            self.consume(Token::By)?;
            loop {
                spec.partition_by.push(self.parse_expression()?);
                if !self.match_token(Token::Comma) {
                    break;
                }
            }
        }

        // Parse ORDER BY
        if self.match_token(Token::Order) {
            self.consume(Token::By)?;
            spec.order_by = self.parse_order_by_list()?;
        }

        // Parse frame specification (simplified)
        if self.match_token(Token::Rows) {
            spec.frame = Some(self.parse_window_frame()?);
        } else if self.match_token(Token::Range) {
            spec.frame = Some(self.parse_window_frame()?);
        }

        self.consume(Token::RParen)?;
        Ok(spec)
    }

    /// P5-4: Parse window frame
    fn parse_window_frame(&mut self) -> Result<WindowFrame> {
        let frame_type = if matches!(self.current, Token::Rows) {
            self.advance();
            "ROWS"
        } else if matches!(self.current, Token::Range) {
            self.advance();
            "RANGE"
        } else {
            "ROWS"
        };

        self.consume(Token::Between)?;
        let start = self.parse_frame_bound()?;
        self.consume(Token::And)?;
        let end = self.parse_frame_bound()?;

        if frame_type == "RANGE" {
            Ok(WindowFrame::Range(start, end))
        } else {
            Ok(WindowFrame::Rows(start, end))
        }
    }

    /// P5-4: Parse frame bound
    fn parse_frame_bound(&mut self) -> Result<WindowFrameBound> {
        if self.match_token(Token::Unbounded) {
            if self.match_token(Token::Preceding) {
                Ok(WindowFrameBound::UnboundedPreceding)
            } else if self.match_token(Token::Following) {
                Ok(WindowFrameBound::UnboundedFollowing)
            } else {
                Err(ParseError::UnexpectedToken("Expected PRECEDING or FOLLOWING".to_string()))
            }
        } else if self.match_token(Token::Current) {
            self.consume(Token::Row)?;
            Ok(WindowFrameBound::CurrentRow)
        } else if let Token::NumberLiteral(n) = &self.current {
            let n = *n;
            self.advance();
            if self.match_token(Token::Preceding) {
                Ok(WindowFrameBound::Preceding(n))
            } else if self.match_token(Token::Following) {
                Ok(WindowFrameBound::Following(n))
            } else {
                Err(ParseError::UnexpectedToken("Expected PRECEDING or FOLLOWING".to_string()))
            }
        } else {
            Err(ParseError::UnexpectedToken("Expected frame bound".to_string()))
        }
    }

    fn parse_order_by_list(&mut self) -> Result<Vec<OrderBy>> {
        let mut order_by = Vec::new();
        loop {
            let column = self.consume_identifier()?;
            let descending = if self.match_token(Token::Desc) {
                true
            } else {
                self.match_token(Token::Asc);
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
        // Parse optional WITH clause for CTE support (P5-5)
        let ctes = if self.current == Token::With {
            self.advance();
            self.parse_cte_list()?
        } else {
            Vec::new()
        };

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
            ctes,
        }))
    }

    fn parse_update(&mut self) -> Result<Statement> {
        // Parse optional WITH clause for CTE support (P5-5)
        let ctes = if self.current == Token::With {
            self.advance();
            self.parse_cte_list()?
        } else {
            Vec::new()
        };

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
            ctes,
        }))
    }

    fn parse_delete(&mut self) -> Result<Statement> {
        // Parse optional WITH clause for CTE support (P5-5)
        let ctes = if self.current == Token::With {
            self.advance();
            self.parse_cte_list()?
        } else {
            Vec::new()
        };

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
            ctes,
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
            Token::Trigger => self.parse_create_trigger(),
            Token::Virtual => self.parse_create_virtual_table(),
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

        // P5-3: WITH CHECK OPTION
        let with_check_option = self.match_token(Token::With);
        if with_check_option {
            self.match_token(Token::Check);
            self.match_token(Token::Option);
        }

        Ok(Statement::CreateView(CreateViewStmt {
            name,
            columns,
            query,
            with_check_option,
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
                let mut default_value = None;
                let mut is_virtual = false;
                let mut generated_always = None;
                
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
                    } else if self.match_token(Token::Default) {
                        default_value = Some(self.parse_expression()?);
                    } else if self.match_token(Token::Generated) {
                        // GENERATED ALWAYS AS (expr)
                        self.match_token(Token::Always);
                        self.consume(Token::As)?;
                        self.consume(Token::LParen)?;
                        generated_always = Some(self.parse_expression()?);
                        self.consume(Token::RParen)?;
                        is_virtual = true;
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
                    default_value,
                    is_virtual,
                    generated_always,
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

    /// P5-2: Parse CREATE TRIGGER
    fn parse_create_trigger(&mut self) -> Result<Statement> {
        self.consume(Token::Trigger)?;
        let name = self.consume_identifier()?;

        // Parse timing: BEFORE, AFTER, INSTEAD OF
        let timing = if self.match_token(Token::Before) {
            TriggerTiming::Before
        } else if self.match_token(Token::After) {
            TriggerTiming::After
        } else if self.match_token(Token::Instead) {
            self.consume(Token::Of)?;
            TriggerTiming::InsteadOf
        } else {
            return Err(ParseError::UnexpectedToken(
                "Expected BEFORE, AFTER, or INSTEAD OF".to_string()
            ));
        };

        // Parse event: INSERT, DELETE, UPDATE
        let event = if self.match_token(Token::Insert) {
            TriggerEvent::Insert
        } else if self.match_token(Token::Delete) {
            TriggerEvent::Delete
        } else if self.match_token(Token::Update) {
            let columns = if self.match_token(Token::Of) {
                let cols = self.parse_column_list()?;
                Some(cols)
            } else {
                None
            };
            TriggerEvent::Update { columns }
        } else {
            return Err(ParseError::UnexpectedToken(
                "Expected INSERT, DELETE, or UPDATE".to_string()
            ));
        };

        self.consume(Token::On)?;
        let table = self.consume_identifier()?;

        // Parse FOR EACH ROW (optional, default is FOR EACH STATEMENT)
        let for_each_row = if self.match_token(Token::For) {
            self.match_token(Token::Each);
            self.match_token(Token::Row);
            true
        } else {
            false
        };

        // Parse WHEN clause (optional)
        let when_clause = if self.match_token(Token::When) {
            Some(self.parse_expression()?)
        } else {
            None
        };

        // Parse trigger body (BEGIN ... END block or single statement)
        let body = if self.match_token(Token::Begin) {
            // Parse multiple statements until END
            let mut stmts = Vec::new();
            while !matches!(self.current, Token::End) {
                stmts.push(self.parse_trigger_statement()?);
                self.match_token(Token::Semicolon);
            }
            self.consume(Token::End)?;
            stmts
        } else {
            // Single statement
            vec![self.parse_trigger_statement()?]
        };

        Ok(Statement::CreateTrigger(CreateTriggerStmt {
            name,
            timing,
            event,
            table,
            for_each_row,
            when_clause,
            body,
        }))
    }

    /// P5-2: Parse trigger body statement
    fn parse_trigger_statement(&mut self) -> Result<TriggerStatement> {
        match &self.current {
            Token::Insert => {
                // Parse INSERT but return as TriggerStatement
                let insert = match self.parse_insert()? {
                    Statement::Insert(i) => i,
                    _ => return Err(ParseError::UnexpectedToken("Expected INSERT".to_string())),
                };
                Ok(TriggerStatement::Insert(insert))
            }
            Token::Update => {
                let update = match self.parse_update()? {
                    Statement::Update(u) => u,
                    _ => return Err(ParseError::UnexpectedToken("Expected UPDATE".to_string())),
                };
                Ok(TriggerStatement::Update(update))
            }
            Token::Delete => {
                let delete = match self.parse_delete()? {
                    Statement::Delete(d) => d,
                    _ => return Err(ParseError::UnexpectedToken("Expected DELETE".to_string())),
                };
                Ok(TriggerStatement::Delete(delete))
            }
            Token::Select => {
                let select = self.parse_select_stmt()?;
                Ok(TriggerStatement::Select(select))
            }
            _ => Err(ParseError::UnexpectedToken(
                format!("Unexpected token in trigger body: {:?}", self.current)
            )),
        }
    }

    /// P5-6/P5-7: Parse CREATE VIRTUAL TABLE
    fn parse_create_virtual_table(&mut self) -> Result<Statement> {
        self.consume(Token::Table)?;
        let name = self.consume_identifier()?;
        self.consume(Token::Using)?;

        // Parse module name: FTS5 or RTREE
        let module = match &self.current {
            Token::Fts5 => {
                self.advance();
                // Parse column list
                self.consume(Token::LParen)?;
                let columns = self.parse_column_list()?;
                self.consume(Token::RParen)?;
                VirtualTableModule::Fts5(columns)
            }
            Token::Rtree => {
                self.advance();
                self.consume(Token::LParen)?;
                let id_column = self.consume_identifier()?;
                self.consume(Token::Comma)?;
                let min_x = self.consume_identifier()?;
                self.consume(Token::Comma)?;
                let max_x = self.consume_identifier()?;
                self.consume(Token::Comma)?;
                let min_y = self.consume_identifier()?;
                self.consume(Token::Comma)?;
                let max_y = self.consume_identifier()?;
                self.consume(Token::RParen)?;
                VirtualTableModule::Rtree {
                    id_column,
                    min_x, max_x,
                    min_y, max_y,
                }
            }
            Token::Identifier(module_name) => {
                let name = module_name.clone();
                self.advance();
                self.consume(Token::LParen)?;
                let args = self.parse_column_list()?;
                self.consume(Token::RParen)?;
                if name.to_uppercase() == "FTS5" {
                    VirtualTableModule::Fts5(args)
                } else if name.to_uppercase() == "RTREE" {
                    // Simplified parsing for RTREE
                    VirtualTableModule::Rtree {
                        id_column: args.get(0).cloned().unwrap_or_default(),
                        min_x: args.get(1).cloned().unwrap_or_default(),
                        max_x: args.get(2).cloned().unwrap_or_default(),
                        min_y: args.get(3).cloned().unwrap_or_default(),
                        max_y: args.get(4).cloned().unwrap_or_default(),
                    }
                } else {
                    return Err(ParseError::UnexpectedToken(
                        format!("Unknown virtual table module: {}", name)
                    ));
                }
            }
            _ => return Err(ParseError::UnexpectedToken(
                "Expected virtual table module (FTS5, RTREE)".to_string()
            )),
        };

        Ok(Statement::CreateVirtualTable(CreateVirtualTableStmt {
            name,
            module,
        }))
    }

    fn parse_drop(&mut self) -> Result<Statement> {
        self.advance();
        match &self.current {
            Token::Table => {
                self.advance();
                // Check for IF EXISTS
                let if_exists = if self.match_token(Token::If) {
                    self.consume(Token::Exists)?;
                    true
                } else {
                    false
                };
                let table = self.consume_identifier()?;
                Ok(Statement::DropTable(DropTableStmt {
                    table,
                    if_exists,
                }))
            }
            Token::View => {
                self.advance();
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
            Token::Trigger => {
                self.advance();
                let if_exists = if self.match_token(Token::If) {
                    self.consume(Token::Exists)?;
                    true
                } else {
                    false
                };
                let name = self.consume_identifier()?;
                Ok(Statement::DropTrigger(DropTriggerStmt {
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
            self.match_token(Token::Column);
            let column = self.parse_column_def()?;
            Ok(Statement::AlterTable(AlterTableStmt::AddColumn { table, column }))
        } else if self.match_token(Token::Drop) {
            self.match_token(Token::Column);
            let column = self.consume_identifier()?;
            Ok(Statement::AlterTable(AlterTableStmt::DropColumn { table, column }))
        } else if self.match_token(Token::Rename) {
            if self.match_token(Token::To) {
                let new_name = self.consume_identifier()?;
                Ok(Statement::AlterTable(AlterTableStmt::RenameTable { table, new_name }))
            } else if self.match_token(Token::Column) {
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
        let mut default_value = None;
        let mut is_virtual = false;
        let mut generated_always = None;
        
        loop {
            if self.match_token(Token::Primary) {
                self.consume(Token::Key)?;
                primary_key = true;
            } else if self.match_token(Token::References) {
                foreign_key = Some(self.parse_column_foreign_key()?);
            } else if self.match_token(Token::Not) {
                self.consume(Token::Null)?;
            } else if self.match_token(Token::Null) {
            } else if self.match_token(Token::Unique) {
            } else if self.match_token(Token::Default) {
                default_value = Some(self.parse_expression()?);
            } else if self.match_token(Token::Generated) {
                self.match_token(Token::Always);
                self.consume(Token::As)?;
                self.consume(Token::LParen)?;
                generated_always = Some(self.parse_expression()?);
                self.consume(Token::RParen)?;
                is_virtual = true;
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
            default_value,
            is_virtual,
            generated_always,
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

    fn parse_in(&mut self) -> Result<Expression> {
        let left = self.parse_term()?;

        if self.match_token(Token::In) {
            self.consume(Token::LParen)?;
            
            if self.current == Token::Select {
                let subquery = self.parse_select_stmt()?;
                self.consume(Token::RParen)?;
                Ok(Expression::Subquery(SubqueryExpr::In {
                    expr: Box::new(left),
                    subquery: Box::new(subquery),
                }))
            } else {
                let mut values = vec![self.parse_expression()?];
                while self.match_token(Token::Comma) {
                    values.push(self.parse_expression()?);
                }
                self.consume(Token::RParen)?;
                
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
                self.advance();
                Ok(Expression::Placeholder(1))
            }
            // P5-2: NEW and OLD references in triggers
            Token::New => {
                self.advance();
                self.consume(Token::Dot)?;
                let column = self.consume_identifier()?;
                Ok(Expression::TriggerReference { is_new: true, column })
            }
            Token::Old => {
                self.advance();
                self.consume(Token::Dot)?;
                let column = self.consume_identifier()?;
                Ok(Expression::TriggerReference { is_new: false, column })
            }
            Token::Identifier(name) => {
                let name = name.clone();
                self.advance();
                if self.match_token(Token::LParen) {
                    // Function call
                    let mut args = Vec::new();
                    if self.current != Token::RParen {
                        args.push(self.parse_expression()?);
                        while self.match_token(Token::Comma) {
                            args.push(self.parse_expression()?);
                        }
                    }
                    self.consume(Token::RParen)?;
                    
                    // Check for JSON functions (P5-8)
                    let upper_name = name.to_uppercase();
                    if upper_name.starts_with("JSON_") {
                        let func_type = match upper_name.as_str() {
                            "JSON" => JsonFunctionType::Json,
                            "JSON_ARRAY" => JsonFunctionType::JsonArray,
                            "JSON_OBJECT" => JsonFunctionType::JsonObject,
                            "JSON_EXTRACT" => JsonFunctionType::JsonExtract,
                            "JSON_TYPE" => JsonFunctionType::JsonType,
                            "JSON_VALID" => JsonFunctionType::JsonValid,
                            _ => {
                                return Ok(Expression::FunctionCall { name, args });
                            }
                        };
                        return Ok(Expression::JsonFunction { func: func_type, args });
                    }
                    
                    Ok(Expression::FunctionCall { name, args })
                } else {
                    Ok(Expression::Column(name))
                }
            }
            Token::LParen => {
                self.advance();
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
            Token::Json => {
                self.advance();
                Ok(DataType::Json)
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

    // P5-2: Trigger tests
    #[test]
    fn test_parse_create_trigger() {
        let sql = "CREATE TRIGGER update_timestamp AFTER UPDATE ON users BEGIN UPDATE users SET updated_at = 'now' WHERE id = NEW.id; END";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();
        assert!(matches!(stmt, Statement::CreateTrigger(_)));
    }

    #[test]
    fn test_parse_drop_trigger() {
        let mut parser = Parser::new("DROP TRIGGER IF EXISTS update_timestamp").unwrap();
        let stmt = parser.parse().unwrap();
        assert!(matches!(stmt, Statement::DropTrigger(_)));
    }

    // P5-4: Window function tests
    #[test]
    fn test_parse_window_function_row_number() {
        let mut parser = Parser::new("SELECT ROW_NUMBER() OVER (ORDER BY salary DESC) AS rank FROM employees").unwrap();
        let stmt = parser.parse().unwrap();
        match stmt {
            Statement::Select(s) => {
                assert!(matches!(s.columns[0], SelectColumn::WindowFunc(_, _)));
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_window_function_partition() {
        let mut parser = Parser::new("SELECT RANK() OVER (PARTITION BY dept ORDER BY salary) FROM employees").unwrap();
        let stmt = parser.parse().unwrap();
        assert!(matches!(stmt, Statement::Select(_)));
    }

    // P5-6: Virtual table tests
    #[test]
    fn test_parse_create_virtual_table_fts5() {
        let mut parser = Parser::new("CREATE VIRTUAL TABLE docs USING FTS5(title, content)").unwrap();
        let stmt = parser.parse().unwrap();
        assert!(matches!(stmt, Statement::CreateVirtualTable(_)));
    }

    #[test]
    fn test_parse_create_virtual_table_rtree() {
        let mut parser = Parser::new("CREATE VIRTUAL TABLE places USING RTREE(id, minX, maxX, minY, maxY)").unwrap();
        let stmt = parser.parse().unwrap();
        assert!(matches!(stmt, Statement::CreateVirtualTable(_)));
    }

    // P5-8: JSON function tests
    #[test]
    fn test_parse_json_extract() {
        let mut parser = Parser::new("SELECT JSON_EXTRACT(data, '$.name') FROM users").unwrap();
        let stmt = parser.parse().unwrap();
        assert!(matches!(stmt, Statement::Select(_)));
    }
}
