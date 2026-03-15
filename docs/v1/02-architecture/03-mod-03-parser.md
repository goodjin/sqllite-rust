# MOD-03: SQL 解析器模块 (SQL Parser)

## 文档信息
- **项目名称**: sqllite-rust
- **文档编号**: MOD-03
- **版本**: v1.0
- **更新日期**: 2026-03-14
- **对应PRD**: FR-005, FR-011

---

## 目录

1. [系统定位](#系统定位)
2. [对应PRD](#对应prd)
3. [全局架构位置](#全局架构位置)
4. [依赖关系](#依赖关系)
5. [数据流](#数据流)
6. [核心设计](#核心设计)
7. [接口定义](#接口定义)
8. [数据结构](#数据结构)
9. [状态机设计](#状态机设计)
10. [边界条件](#边界条件)
11. [非功能需求](#非功能需求)
12. [实现文件](#实现文件)
13. [验收标准](#验收标准)
14. [覆盖映射](#覆盖映射)

---

## 系统定位

### 在整体架构中的位置

**所属层次**: L1-接口层

**架构定位图**:
```
┌─────────────────────────────────────────────────────┐
│              用户输入 (SQL String)                   │
└───────────────────────┬─────────────────────────────┘
                        │ ▼
┌─────────────────────────────────────────────────────┐
│         ★ MOD-03: SQL 解析器 (Parser) ★              │
│         Tokenizer → Parser → AST                     │
└───────────────────────┬─────────────────────────────┘
                        │ AST
                        ▼
┌─────────────────────────────────────────────────────┐
│              L2: 执行层 (Execution Layer)            │
│              Virtual Machine                         │
└─────────────────────────────────────────────────────┘
```

### 核心职责

- **词法分析 (Tokenizer)**: 将 SQL 字符串分割为 Token 序列
- **语法分析 (Parser)**: 将 Token 序列解析为抽象语法树 (AST)
- **语法验证**: 验证 SQL 语法是否符合支持的子集

### 边界说明

- **负责**:
  - SQL 词法分析
  - SQL 语法分析
  - AST 生成
  - 基础语法错误报告

- **不负责**:
  - 语义验证（如表是否存在，由 VM 负责）
  - 查询优化（由 Optimizer 负责）
  - 类型检查（由 VM 负责）

---

## 对应PRD

| PRD章节 | 编号 | 内容 |
|---------|-----|------|
| 功能需求 | FR-005 | SQL 解析器 |
| 功能需求 | FR-011 | WHERE 子句 |
| 功能需求 | FR-012 | CREATE TABLE |
| 功能需求 | FR-013 | DROP TABLE |
| 用户故事 | US-002 | 创建表 |
| 用户故事 | US-003~006 | 数据操作 |

---

## 全局架构位置

```
┌─────────────────────────────────────────────────────────────────┐
│                        L1: 接口层                                │
│  ┌───────────────────────────────────────────────────────────┐ │
│  │              ★ MOD-03 Parser ★                             │ │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐ │ │
│  │  │  Tokenizer  │→ │   Parser    │→ │        AST          │ │ │
│  │  │             │  │             │  │                     │ │ │
│  │  └─────────────┘  └─────────────┘  └─────────────────────┘ │ │
│  └───────────────────────────────────────────────────────────┘ │
└──────────────────────────┬──────────────────────────────────────┘
                           │ AST
                           ▼
┌──────────────────────────┬──────────────────────────────────────┐
│                        L2: 执行层                                │
│  ┌───────────────────────▼───────────────────────────────────┐ │
│  │              Virtual Machine (MOD-04)                      │ │
│  └───────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
```

---

## 依赖关系

### 上游依赖（本模块调用的模块）

无（本模块为最上层）

### 下游依赖（调用本模块的模块）

| 模块名称 | 模块编号 | 被调用场景 | 调用方式 |
|---------|---------|-----------|---------|
| Virtual Machine | MOD-04 | 执行解析后的 AST | 直接调用 |

---

## 数据流

### 输入数据流

| 数据项 | 来源 | 格式 | 说明 |
|-------|------|------|------|
| SQL 字符串 | 用户输入 | String | 要执行的 SQL |

### 输出数据流

| 数据项 | 目标 | 格式 | 说明 |
|-------|------|------|------|
| AST | VM | Statement | 解析后的语法树 |
| 错误信息 | 用户 | ParseError | 语法错误描述 |

---

## 核心设计

### 设计目标

| 目标 | 描述 | 度量标准 |
|-----|------|---------|
| 解析速度 | 快速解析 | 简单 SQL < 1ms |
| 错误信息 | 友好的错误提示 | 包含位置信息 |
| 可扩展性 | 易于添加新语法 | 模块化设计 |

### 核心组件

#### 1. Token 类型

```rust
/// SQL Token 类型
#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    // 关键字
    Select, Insert, Update, Delete,
    Create, Drop, Table, Index,
    From, Where, Set, Values,
    And, Or, Not, Null, True, False,
    Begin, Commit, Rollback, Transaction,
    Primary, Key,

    // 数据类型
    Integer, Text, Real, Blob,

    // 标识符和字面量
    Identifier(String),      // 表名、列名
    StringLiteral(String),   // 字符串 '...'
    NumberLiteral(i64),      // 整数
    FloatLiteral(f64),       // 浮点数

    // 运算符
    Equal,          // =
    NotEqual,       // <>, !=
    Less,           // <
    Greater,        // >
    LessEqual,      // <=
    GreaterEqual,   // >=
    Plus,           // +
    Minus,          // -
    Star,           // *
    Slash,          // /

    // 标点符号
    Semicolon,      // ;
    Comma,          // ,
    LParen,         // (
    RParen,         // )

    // 特殊
    Eof,            // 文件结束
    Invalid(String), // 无效字符
}
```

#### 2. AST 节点

```rust
/// SQL 语句
#[derive(Debug, Clone)]
pub enum Statement {
    Select(SelectStmt),
    Insert(InsertStmt),
    Update(UpdateStmt),
    Delete(DeleteStmt),
    CreateTable(CreateTableStmt),
    DropTable(DropTableStmt),
    CreateIndex(CreateIndexStmt),
    BeginTransaction,
    Commit,
    Rollback,
}

/// SELECT 语句
#[derive(Debug, Clone)]
pub struct SelectStmt {
    pub columns: Vec<SelectColumn>,
    pub from: String,
    pub where_clause: Option<Expression>,
}

#[derive(Debug, Clone)]
pub enum SelectColumn {
    All,                    // *
    Column(String),         // col
    Aliased(String, String), // col AS alias
}

/// INSERT 语句
#[derive(Debug, Clone)]
pub struct InsertStmt {
    pub table: String,
    pub columns: Option<Vec<String>>,
    pub values: Vec<Vec<Expression>>,
}

/// UPDATE 语句
#[derive(Debug, Clone)]
pub struct UpdateStmt {
    pub table: String,
    pub set_clauses: Vec<SetClause>,
    pub where_clause: Option<Expression>,
}

#[derive(Debug, Clone)]
pub struct SetClause {
    pub column: String,
    pub value: Expression,
}

/// DELETE 语句
#[derive(Debug, Clone)]
pub struct DeleteStmt {
    pub table: String,
    pub where_clause: Option<Expression>,
}

/// CREATE TABLE 语句
#[derive(Debug, Clone)]
pub struct CreateTableStmt {
    pub table: String,
    pub columns: Vec<ColumnDef>,
}

#[derive(Debug, Clone)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub primary_key: bool,
}

#[derive(Debug, Clone)]
pub enum DataType {
    Integer,
    Text,
    Real,
    Blob,
}

/// DROP TABLE 语句
#[derive(Debug, Clone)]
pub struct DropTableStmt {
    pub table: String,
    pub if_exists: bool,
}

/// CREATE INDEX 语句
#[derive(Debug, Clone)]
pub struct CreateIndexStmt {
    pub index_name: String,
    pub table: String,
    pub column: String,
    pub unique: bool,
}

/// 表达式
#[derive(Debug, Clone)]
pub enum Expression {
    // 字面量
    Integer(i64),
    String(String),
    Float(f64),
    Boolean(bool),
    Null,

    // 列引用
    Column(String),

    // 二元运算
    Binary {
        left: Box<Expression>,
        op: BinaryOp,
        right: Box<Expression>,
    },

    // 一元运算
    Unary {
        op: UnaryOp,
        expr: Box<Expression>,
    },
}

#[derive(Debug, Clone)]
pub enum BinaryOp {
    Equal, NotEqual, Less, Greater, LessEqual, GreaterEqual,
    And, Or,
    Add, Sub, Mul, Div,
}

#[derive(Debug, Clone)]
pub enum UnaryOp {
    Not, Minus,
}
```

#### 3. 词法分析器 (Tokenizer)

```rust
/// SQL 词法分析器
pub struct Tokenizer<'a> {
    input: &'a str,
    position: usize,
    line: usize,
    column: usize,
}

impl<'a> Tokenizer<'a> {
    pub fn new(input: &'a str) -> Self {
        Self {
            input,
            position: 0,
            line: 1,
            column: 1,
        }
    }

    /// 获取下一个 Token
    pub fn next_token(&mut self) -> Token {
        self.skip_whitespace();

        if self.is_at_end() {
            return Token::Eof;
        }

        let ch = self.peek();

        match ch {
            // 单字符运算符
            ';' => { self.advance(); Token::Semicolon }
            ',' => { self.advance(); Token::Comma }
            '(' => { self.advance(); Token::LParen }
            ')' => { self.advance(); Token::RParen }
            '*' => { self.advance(); Token::Star }
            '+' => { self.advance(); Token::Plus }
            '/' => { self.advance(); Token::Slash }

            // 可能多字符的运算符
            '=' => { self.advance(); Token::Equal }
            '<' => self.match_less(),
            '>' => self.match_greater(),
            '!' => self.match_bang(),
            '-' => self.match_minus(),

            // 字符串字面量
            '\'' => self.read_string(),

            // 数字
            '0'..='9' => self.read_number(),

            // 标识符或关键字
            'a'..='z' | 'A'..='Z' | '_' => self.read_identifier(),

            // 其他
            _ => {
                let ch = self.advance();
                Token::Invalid(format!("Unexpected character: {}", ch))
            }
        }
    }

    /// 读取标识符并检查是否为关键字
    fn read_identifier(&mut self) -> Token {
        let start = self.position;
        while self.peek().is_alphanumeric() || self.peek() == '_' {
            self.advance();
        }
        let text = &self.input[start..self.position];
        Self::keyword_or_identifier(text)
    }

    /// 检查是否为关键字
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
            "REAL" => Token::Real,
            "BLOB" => Token::Blob,
            _ => Token::Identifier(text.to_string()),
        }
    }

    // ... 其他辅助方法
}
```

#### 4. 语法分析器 (Parser)

```rust
/// SQL 语法分析器
pub struct Parser<'a> {
    tokenizer: Tokenizer<'a>,
    current: Token,
    peek: Token,
}

impl<'a> Parser<'a> {
    pub fn new(input: &'a str) -> Result<Self, ParseError> {
        let mut tokenizer = Tokenizer::new(input);
        let current = tokenizer.next_token();
        let peek = tokenizer.next_token();

        Ok(Self {
            tokenizer,
            current,
            peek,
        })
    }

    /// 解析 SQL 语句
    pub fn parse(&mut self) -> Result<Statement, ParseError> {
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
            _ => Err(ParseError::UnexpectedToken(self.current.clone())),
        }?;

        // 语句结束后应该是分号或 EOF
        if !matches!(self.current, Token::Semicolon | Token::Eof) {
            return Err(ParseError::ExpectedSemicolon);
        }

        Ok(stmt)
    }

    /// 解析 SELECT 语句
    fn parse_select(&mut self) -> Result<Statement, ParseError> {
        self.consume(Token::Select)?;

        // 解析列
        let columns = self.parse_select_columns()?;

        // FROM
        self.consume(Token::From)?;
        let table = self.consume_identifier()?;

        // 可选的 WHERE
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

    /// 解析 SELECT 列列表
    fn parse_select_columns(&mut self) -> Result<Vec<SelectColumn>, ParseError> {
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

    /// 解析 INSERT 语句
    fn parse_insert(&mut self) -> Result<Statement, ParseError> {
        self.consume(Token::Insert)?;
        self.consume(Token::Into)?;

        let table = self.consume_identifier()?;

        // 可选的列列表
        let columns = if self.match_token(Token::LParen) {
            let cols = self.parse_column_list()?;
            self.consume(Token::RParen)?;
            Some(cols)
        } else {
            None
        };

        // VALUES
        self.consume(Token::Values)?;
        let values = self.parse_values_list()?;

        Ok(Statement::Insert(InsertStmt {
            table,
            columns,
            values,
        }))
    }

    /// 解析表达式（用于 WHERE 子句）
    fn parse_expression(&mut self) -> Result<Expression, ParseError> {
        self.parse_or()
    }

    fn parse_or(&mut self) -> Result<Expression, ParseError> {
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

    fn parse_and(&mut self) -> Result<Expression, ParseError> {
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

    fn parse_equality(&mut self) -> Result<Expression, ParseError> {
        let mut left = self.parse_comparison()?;

        while self.match_tokens(&[Token::Equal, Token::NotEqual]) {
            let op = match self.previous() {
                Token::Equal => BinaryOp::Equal,
                Token::NotEqual => BinaryOp::NotEqual,
                _ => unreachable!(),
            };
            let right = self.parse_comparison()?;
            left = Expression::Binary {
                left: Box::new(left),
                op,
                right: Box::new(right),
            };
        }

        Ok(left)
    }

    fn parse_comparison(&mut self) -> Result<Expression, ParseError> {
        let mut left = self.parse_primary()?;

        while self.match_tokens(&[
            Token::Less, Token::Greater,
            Token::LessEqual, Token::GreaterEqual,
        ]) {
            let op = match self.previous() {
                Token::Less => BinaryOp::Less,
                Token::Greater => BinaryOp::Greater,
                Token::LessEqual => BinaryOp::LessEqual,
                Token::GreaterEqual => BinaryOp::GreaterEqual,
                _ => unreachable!(),
            };
            let right = self.parse_primary()?;
            left = Expression::Binary {
                left: Box::new(left),
                op,
                right: Box::new(right),
            };
        }

        Ok(left)
    }

    fn parse_primary(&mut self) -> Result<Expression, ParseError> {
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
            _ => Err(ParseError::UnexpectedToken(self.current.clone())),
        }
    }

    // 辅助方法...
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

    fn consume(&mut self, expected: Token) -> Result<(), ParseError> {
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

    fn consume_identifier(&mut self) -> Result<String, ParseError> {
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
```

---

## 接口定义

### 对外接口清单

| 接口编号 | 接口名称 | 方法 | 对应PRD |
|---------|---------|------|---------|
| API-007 | Parser::parse | fn parse(sql: &str) -> Result<Statement> | FR-005 |

### 接口详细定义

#### API-007: Parser::parse

**对应PRD**:
- 功能需求: FR-005
- 用户故事: US-002~006

**接口定义**:
```rust
/// 解析 SQL 字符串
///
/// # Arguments
/// * `sql` - SQL 字符串
///
/// # Returns
/// * `Ok(Statement)` - 解析成功，返回 AST
/// * `Err(ParseError)` - 解析失败，返回错误信息
pub fn parse(sql: &str) -> Result<Statement, ParseError>
```

**错误类型**:
```rust
#[derive(Debug)]
pub enum ParseError {
    UnexpectedToken(Token),
    ExpectedToken { expected: String, found: String },
    ExpectedIdentifier,
    ExpectedSemicolon,
    InvalidNumber(String),
    UnterminatedString,
    EmptyInput,
}
```

---

## 数据结构

### 核心实体

已在核心设计部分定义，见 [AST 节点](#2-ast-节点)。

---

## 状态机设计

本模块无复杂状态机。

---

## 边界条件

### BOUND-001: 空输入

**对应PRD**: FR-005

**处理**: 返回 EmptyInput 错误

### BOUND-002: 无效字符

**对应PRD**: FR-005

**处理**: 返回 UnexpectedToken 错误，包含位置信息

### BOUND-003: 未终止字符串

**对应PRD**: FR-005

**处理**: 返回 UnterminatedString 错误

### BOUND-004: 语法错误

**对应PRD**: FR-005

**处理**: 返回 ExpectedToken 错误，说明期望的 token 和实际找到的

---

## 非功能需求

### 性能要求

| 指标 | 要求 | 对应PRD |
|-----|------|---------|
| 解析速度 | 简单 SQL < 1ms | FR-005 |
| 内存使用 | 流式处理，不缓存大量 token | FR-005 |

### 可维护性

| 需求 | 描述 | 实现方案 |
|-----|------|---------|
| 可扩展 | 易于添加新语法 | 递归下降解析器，模块化设计 |
| 错误信息 | 友好的错误提示 | 包含行号、列号信息 |

---

## 实现文件

| 文件路径 | 职责 |
|---------|------|
| src/sql/mod.rs | 模块入口 |
| src/sql/token.rs | Token 定义 |
| src/sql/tokenizer.rs | 词法分析器 |
| src/sql/ast.rs | AST 定义 |
| src/sql/parser.rs | 语法分析器 |
| src/sql/error.rs | 错误类型 |

---

## 验收标准

| 标准 | 要求 | 验证方法 | 对应PRD |
|-----|------|---------|---------|
| 标准1 | 正确解析 SELECT | 单元测试：验证 AST 结构 | FR-005 |
| 标准2 | 正确解析 INSERT | 单元测试：验证 AST 结构 | FR-005 |
| 标准3 | 正确解析 UPDATE | 单元测试：验证 AST 结构 | FR-005 |
| 标准4 | 正确解析 DELETE | 单元测试：验证 AST 结构 | FR-005 |
| 标准5 | 正确解析 CREATE TABLE | 单元测试：验证 AST 结构 | FR-005 |
| 标准6 | 正确解析 WHERE 子句 | 单元测试：验证表达式结构 | FR-011 |
| 标准7 | 正确报告语法错误 | 单元测试：验证错误信息 | FR-005 |

---

## 覆盖映射

### PRD需求覆盖情况

| PRD类型 | PRD编号 | 架构元素 | 覆盖状态 |
|---------|---------|---------|---------|
| 功能需求 | FR-005 | Tokenizer, Parser, AST | ✅ |
| 功能需求 | FR-011 | Expression, BinaryOp | ✅ |
| 功能需求 | FR-012 | CreateTableStmt | ✅ |
| 功能需求 | FR-013 | DropTableStmt | ✅ |
| 用户故事 | US-002 | parse_create_table | ✅ |
| 用户故事 | US-003~006 | parse_insert/update/delete | ✅ |

---

## 变更历史

| 版本 | 日期 | 变更内容 | 作者 |
|-----|------|---------|------|
| 1.0 | 2026-03-14 | 初始版本 | Claude |
