# 开发计划 - MOD-03: SQL Parser (SQL 解析器)

## 文档信息
- **模块编号**: MOD-03
- **模块名称**: SQL Parser (SQL 解析器)
- **所属层次**: L1-接口层
- **对应架构**: docs/v1/02-architecture/03-mod-03-parser.md
- **优先级**: P0 (阶段 2)
- **预估工时**: 2天

---

## 1. 模块概述

### 1.1 模块职责
- SQL 词法分析（Tokenizer）
- SQL 语法分析（Parser）
- 生成抽象语法树（AST）
- 基础语法错误报告

### 1.2 对应PRD
| PRD编号 | 功能 | 用户故事 |
|---------|-----|---------|
| FR-005 | SQL 解析器 | US-002~006 |
| FR-011 | WHERE 子句 | US-004~006 |
| FR-012 | CREATE TABLE | US-002 |
| FR-013 | DROP TABLE | - |

### 1.3 架构定位
```
SQL String → Parser → AST → VM
```

---

## 2. 技术设计

### 2.1 目录结构
```
src/sql/
├── mod.rs           # 模块入口
├── token.rs         # Token 定义
├── tokenizer.rs     # 词法分析器
├── ast.rs           # AST 定义
├── parser.rs        # 语法分析器
└── error.rs         # 错误类型
```

### 2.2 依赖关系
| 依赖模块 | 依赖方式 | 用途 |
|---------|---------|------|
| 无 | - | 本模块为最上层 |

---

## 3. 接口清单

| 任务编号 | 接口编号 | 接口名称 | 复杂度 |
|---------|---------|---------|-------|
| T-04 | API-007 | Parser::parse | 高 |

---

## 4. 开发任务拆分

### 任务清单

| 任务编号 | 任务名称 | 涉及文件 | 代码行数 | 依赖 |
|---------|---------|---------|---------|------|
| T-01 | 错误类型定义 | 1 | ~30 | - |
| T-02 | Token 定义 | 1 | ~80 | T-01 |
| T-03 | Tokenizer 实现 | 2 | ~150 | T-02 |
| T-04 | AST 定义 | 1 | ~100 | T-01 |
| T-05 | Parser 实现（基础语句） | 2 | ~200 | T-03, T-04 |
| T-06 | WHERE 表达式解析 | 1 | ~120 | T-05 |
| T-07 | 单元测试 | 5 | ~200 | T-01~06 |

---

## 5. 详细任务定义

### T-01: 错误类型定义

**任务概述**: 定义 SQL 解析错误类型

**输出**:
- `src/sql/error.rs`

**实现要求**:
```rust
use thiserror::Error;

#[derive(Error, Debug, Clone, PartialEq)]
pub enum ParseError {
    #[error("Unexpected token: {0:?}")]
    UnexpectedToken(Token),

    #[error("Expected token {expected}, found {found}")]
    ExpectedToken { expected: String, found: String },

    #[error("Expected identifier")]
    ExpectedIdentifier,

    #[error("Expected semicolon")]
    ExpectedSemicolon,

    #[error("Invalid number: {0}")]
    InvalidNumber(String),

    #[error("Unterminated string")]
    UnterminatedString,

    #[error("Empty input")]
    EmptyInput,
}

pub type Result<T> = std::result::Result<T, ParseError>;
```

**预估工时**: 0.5小时

---

### T-02: Token 定义

**任务概述**: 定义 SQL Token 类型

**输出**:
- `src/sql/token.rs`

**实现要求**:
```rust
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
    Identifier(String),
    StringLiteral(String),
    NumberLiteral(i64),
    FloatLiteral(f64),

    // 运算符
    Equal, NotEqual, Less, Greater,
    LessEqual, GreaterEqual,
    Plus, Minus, Star, Slash,

    // 标点符号
    Semicolon, Comma, LParen, RParen,

    // 特殊
    Eof,
}
```

**预估工时**: 1小时

**依赖**: T-01

---

### T-03: Tokenizer 实现

**任务概述**: 实现 SQL 词法分析器

**输出**:
- `src/sql/tokenizer.rs`

**实现要求**:
```rust
pub struct Tokenizer<'a> {
    input: &'a str,
    position: usize,
    line: usize,
    column: usize,
}

impl<'a> Tokenizer<'a> {
    pub fn new(input: &'a str) -> Self {
        // 初始化
    }

    pub fn next_token(&mut self) -> Token {
        // 获取下一个 Token
    }

    fn skip_whitespace(&mut self) {
        // 跳过空白字符
    }

    fn read_identifier(&mut self) -> Token {
        // 读取标识符
    }

    fn read_number(&mut self) -> Token {
        // 读取数字
    }

    fn read_string(&mut self) -> Token {
        // 读取字符串
    }

    fn keyword_or_identifier(text: &str) -> Token {
        // 判断是关键字还是标识符
    }
}
```

**验收标准**:
- [ ] 正确识别所有关键字
- [ ] 正确识别标识符
- [ ] 正确识别数字和字符串
- [ ] 正确处理注释（可选）

**测试要求**:
- 测试用例: 8个（关键字、标识符、数字、字符串、运算符）

**预估工时**: 3小时

**依赖**: T-02

---

### T-04: AST 定义

**任务概述**: 定义抽象语法树

**输出**:
- `src/sql/ast.rs`

**实现要求**:
```rust
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

#[derive(Debug, Clone)]
pub struct SelectStmt {
    pub columns: Vec<SelectColumn>,
    pub from: String,
    pub where_clause: Option<Expression>,
}

#[derive(Debug, Clone)]
pub enum SelectColumn {
    All,
    Column(String),
}

#[derive(Debug, Clone)]
pub struct InsertStmt {
    pub table: String,
    pub columns: Option<Vec<String>>,
    pub values: Vec<Vec<Expression>>,
}

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

#[derive(Debug, Clone)]
pub struct DeleteStmt {
    pub table: String,
    pub where_clause: Option<Expression>,
}

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
}

#[derive(Debug, Clone)]
pub struct DropTableStmt {
    pub table: String,
    pub if_exists: bool,
}

#[derive(Debug, Clone)]
pub struct CreateIndexStmt {
    pub index_name: String,
    pub table: String,
    pub column: String,
    pub unique: bool,
}

#[derive(Debug, Clone)]
pub enum Expression {
    Integer(i64),
    String(String),
    Float(f64),
    Boolean(bool),
    Null,
    Column(String),
    Binary {
        left: Box<Expression>,
        op: BinaryOp,
        right: Box<Expression>,
    },
    Unary {
        op: UnaryOp,
        expr: Box<Expression>,
    },
}

#[derive(Debug, Clone)]
pub enum BinaryOp {
    Equal, NotEqual, Less, Greater,
    LessEqual, GreaterEqual,
    And, Or,
    Add, Sub, Mul, Div,
}

#[derive(Debug, Clone)]
pub enum UnaryOp {
    Not, Minus,
}
```

**预估工时**: 1.5小时

**依赖**: T-01

---

### T-05: Parser 实现（基础语句）

**任务概述**: 实现基础 SQL 语句的解析

**输出**:
- `src/sql/parser.rs`（基础部分）

**实现要求**:
```rust
pub struct Parser<'a> {
    tokenizer: Tokenizer<'a>,
    current: Token,
    peek: Token,
}

impl<'a> Parser<'a> {
    pub fn new(input: &'a str) -> Result<Self> {
        // 初始化
    }

    pub fn parse(&mut self) -> Result<Statement> {
        // 解析 SQL 语句
    }

    fn parse_select(&mut self) -> Result<Statement> {
        // 解析 SELECT
    }

    fn parse_insert(&mut self) -> Result<Statement> {
        // 解析 INSERT
    }

    fn parse_update(&mut self) -> Result<Statement> {
        // 解析 UPDATE
    }

    fn parse_delete(&mut self) -> Result<Statement> {
        // 解析 DELETE
    }

    fn parse_create_table(&mut self) -> Result<Statement> {
        // 解析 CREATE TABLE
    }

    fn parse_drop_table(&mut self) -> Result<Statement> {
        // 解析 DROP TABLE
    }

    fn parse_begin(&mut self) -> Result<Statement> {
        // 解析 BEGIN
    }

    fn parse_commit(&mut self) -> Result<Statement> {
        // 解析 COMMIT
    }

    fn parse_rollback(&mut self) -> Result<Statement> {
        // 解析 ROLLBACK
    }

    // 辅助方法
    fn advance(&mut self) { }
    fn match_token(&mut self, token: Token) -> bool { }
    fn consume(&mut self, expected: Token) -> Result<()> { }
    fn consume_identifier(&mut self) -> Result<String> { }
}
```

**验收标准**:
- [ ] 正确解析 SELECT
- [ ] 正确解析 INSERT
- [ ] 正确解析 UPDATE
- [ ] 正确解析 DELETE
- [ ] 正确解析 CREATE TABLE
- [ ] 正确解析 DROP TABLE
- [ ] 正确解析事务语句

**测试要求**:
- 测试用例: 10个（每种语句类型）

**预估工时**: 6小时

**依赖**: T-03, T-04

---

### T-06: WHERE 表达式解析

**任务概述**: 实现 WHERE 子句表达式解析

**输出**:
- `src/sql/parser.rs`（表达式部分）

**实现要求**:
```rust
impl<'a> Parser<'a> {
    fn parse_expression(&mut self) -> Result<Expression> {
        // 解析表达式（优先级：OR < AND < 比较 < 加减 < 乘除 < 一元）
        self.parse_or()
    }

    fn parse_or(&mut self) -> Result<Expression> {
        // 解析 OR 表达式
    }

    fn parse_and(&mut self) -> Result<Expression> {
        // 解析 AND 表达式
    }

    fn parse_equality(&mut self) -> Result<Expression> {
        // 解析等式表达式
    }

    fn parse_comparison(&mut self) -> Result<Expression> {
        // 解析比较表达式
    }

    fn parse_term(&mut self) -> Result<Expression> {
        // 解析加减
    }

    fn parse_factor(&mut self) -> Result<Expression> {
        // 解析乘除
    }

    fn parse_unary(&mut self) -> Result<Expression> {
        // 解析一元表达式
    }

    fn parse_primary(&mut self) -> Result<Expression> {
        // 解析基本表达式（字面量、列名、括号）
    }
}
```

**验收标准**:
- [ ] 正确解析比较运算符
- [ ] 正确解析 AND/OR
- [ ] 正确处理括号
- [ ] 正确处理优先级

**测试要求**:
- 测试用例: 6个（比较、逻辑、优先级、括号）

**预估工时**: 3小时

**依赖**: T-05

---

### T-07: 单元测试

**任务概述**: 编写完整的单元测试

**输出**:
- 各文件中的 `#[cfg(test)]` 模块

**测试清单**:
| 测试目标 | 测试文件 | 用例数 |
|---------|---------|-------|
| Tokenizer | tokenizer.rs | 8 |
| Parser 基础 | parser.rs | 10 |
| Parser 表达式 | parser.rs | 6 |

**预估工时**: 2小时

**依赖**: T-01~06

---

## 6. 验收清单

- [ ] 正确解析所有基础 SQL 语句
- [ ] 正确解析 WHERE 表达式
- [ ] 友好的错误信息
- [ ] 测试覆盖率 ≥ 80%

---

## 7. 覆盖映射

| 架构元素 | 架构编号 | 任务 | 覆盖状态 |
|---------|---------|------|---------|
| 错误类型 | - | T-01 | ✅ |
| Token | - | T-02 | ✅ |
| Tokenizer | - | T-03 | ✅ |
| AST | - | T-04 | ✅ |
| Parser | API-007 | T-05, T-06 | ✅ |

---

## 变更历史

| 版本 | 日期 | 变更内容 | 作者 |
|-----|------|---------|------|
| 1.0 | 2026-03-14 | 初始版本 | Claude |
