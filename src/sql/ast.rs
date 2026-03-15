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
}

#[derive(Debug, Clone)]
pub enum BinaryOp {
    Equal, NotEqual, Less, Greater,
    LessEqual, GreaterEqual,
    And, Or,
    Add, Sub, Mul, Div,
}
