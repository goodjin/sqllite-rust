#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    Select(SelectStmt),
    Insert(InsertStmt),
    Update(UpdateStmt),
    Delete(DeleteStmt),
    CreateTable(CreateTableStmt),
    DropTable(DropTableStmt),
    AlterTable(AlterTableStmt),
    CreateIndex(CreateIndexStmt),
    CreateView(CreateViewStmt),
    DropView(DropViewStmt),
    BeginTransaction,
    Commit,
    Rollback,
}

#[derive(Debug, Clone, PartialEq)]
pub enum AlterTableStmt {
    /// ALTER TABLE table_name ADD COLUMN column_def
    AddColumn {
        table: String,
        column: ColumnDef,
    },
    /// ALTER TABLE table_name DROP COLUMN column_name
    DropColumn {
        table: String,
        column: String,
    },
    /// ALTER TABLE table_name RENAME TO new_name
    RenameTable {
        table: String,
        new_name: String,
    },
    /// ALTER TABLE table_name RENAME COLUMN old_name TO new_name
    RenameColumn {
        table: String,
        old_name: String,
        new_name: String,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct SelectStmt {
    pub ctes: Vec<CommonTableExpr>,  // WITH 子句
    pub columns: Vec<SelectColumn>,
    pub from: String,
    pub joins: Vec<Join>,
    pub where_clause: Option<Expression>,
    pub group_by: Vec<String>,
    pub having: Option<Expression>,
    pub order_by: Vec<OrderBy>,
    pub limit: Option<i64>,
    pub offset: Option<i64>,
}

/// Common Table Expression (CTE) for WITH clause
#[derive(Debug, Clone, PartialEq)]
pub struct CommonTableExpr {
    pub name: String,
    pub columns: Option<Vec<String>>,
    pub query: SelectStmt,
    pub recursive: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateViewStmt {
    pub name: String,
    pub columns: Option<Vec<String>>, // 可选列名
    pub query: SelectStmt,            // 视图定义查询
}

#[derive(Debug, Clone, PartialEq)]
pub struct DropViewStmt {
    pub name: String,
    pub if_exists: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Join {
    pub table: String,
    pub join_type: JoinType,
    pub on_condition: Expression,
}

#[derive(Debug, Clone, PartialEq)]
pub enum JoinType {
    Inner,
    Left,
}

#[derive(Debug, Clone, PartialEq)]
pub struct OrderBy {
    pub column: String,
    pub descending: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SelectColumn {
    All,
    Column(String),
    Expression(Expression, Option<String>),
    Aggregate(AggregateFunc, Option<String>), // Added alias support
}

#[derive(Debug, Clone, PartialEq)]
pub enum AggregateFunc {
    CountStar,
    Count(Expression),
    Sum(Expression),
    Avg(Expression),
    Min(Expression),
    Max(Expression),
}

#[derive(Debug, Clone, PartialEq)]
pub struct InsertStmt {
    pub table: String,
    pub columns: Option<Vec<String>>,
    pub values: Vec<Vec<Expression>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct UpdateStmt {
    pub table: String,
    pub set_clauses: Vec<SetClause>,
    pub where_clause: Option<Expression>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SetClause {
    pub column: String,
    pub value: Expression,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DeleteStmt {
    pub table: String,
    pub where_clause: Option<Expression>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateTableStmt {
    pub table: String,
    pub columns: Vec<ColumnDef>,
    pub foreign_keys: Vec<ForeignKeyDef>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub primary_key: bool,
    pub foreign_key: Option<ColumnForeignKey>,
}

/// Column-level foreign key constraint
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnForeignKey {
    pub ref_table: String,
    pub ref_column: String,
    pub on_delete: ForeignKeyAction,
    pub on_update: ForeignKeyAction,
}

/// Table-level foreign key constraint (for multi-column FKs)
#[derive(Debug, Clone, PartialEq)]
pub struct ForeignKeyDef {
    pub columns: Vec<String>,
    pub ref_table: String,
    pub ref_columns: Vec<String>,
    pub on_delete: ForeignKeyAction,
    pub on_update: ForeignKeyAction,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ForeignKeyAction {
    NoAction,    // Default - same as Restrict
    Restrict,    // Prevent deletion/update if referenced
    Cascade,     // Cascade the deletion/update
    SetNull,     // Set referencing column to NULL
    SetDefault,  // Set referencing column to default value
}

impl Default for ForeignKeyAction {
    fn default() -> Self {
        ForeignKeyAction::NoAction
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum DataType {
    Integer,
    Text,
    Blob,
    Vector(u32),
}

#[derive(Debug, Clone, PartialEq)]
pub struct DropTableStmt {
    pub table: String,
    pub if_exists: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IndexType {
    BTree,
    HNSW,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateIndexStmt {
    pub index_name: String,
    pub table: String,
    pub column: String,
    pub unique: bool,
    pub index_type: IndexType,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Expression {
    Integer(i64),
    String(String),
    Float(f64),
    Boolean(bool),
    Null,
    Column(String),
    /// 参数占位符，如 `?` 或 `$1`
    Placeholder(usize),
    Binary {
        left: Box<Expression>,
        op: BinaryOp,
        right: Box<Expression>,
    },
    Vector(Vec<Expression>),
    FunctionCall {
        name: String,
        args: Vec<Expression>,
    },
    /// 子查询表达式
    Subquery(SubqueryExpr),
}

// Manual Hash implementation that handles f64
impl std::hash::Hash for Expression {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        use std::mem::discriminant;
        discriminant(self).hash(state);
        match self {
            Expression::Integer(v) => v.hash(state),
            Expression::String(v) => v.hash(state),
            Expression::Float(v) => v.to_bits().hash(state), // Convert f64 to bits for hashing
            Expression::Boolean(v) => v.hash(state),
            Expression::Null => {},
            Expression::Column(v) => v.hash(state),
            Expression::Placeholder(v) => v.hash(state),
            Expression::Binary { left, op, right } => {
                left.hash(state);
                op.hash(state);
                right.hash(state);
            }
            Expression::Vector(v) => v.hash(state),
            Expression::FunctionCall { name, args } => {
                name.hash(state);
                args.hash(state);
            }
            Expression::Subquery(v) => v.hash(state),
        }
    }
}

// Manual Eq implementation that handles f64
impl Eq for Expression {}

/// 子查询表达式类型
#[derive(Debug, Clone, PartialEq)]
pub enum SubqueryExpr {
    /// 标量子查询: (SELECT agg FROM t)
    Scalar(Box<SelectStmt>),
    /// IN 子查询: expr IN (SELECT ...)
    In {
        expr: Box<Expression>,
        subquery: Box<SelectStmt>,
    },
    /// EXISTS 子查询: EXISTS (SELECT ...)
    Exists(Box<SelectStmt>),
    /// NOT EXISTS 子查询: NOT EXISTS (SELECT ...)
    NotExists(Box<SelectStmt>),
}

// Manual Hash implementation for SubqueryExpr
// Uses a simple discriminant-based hash since the contents don't implement Hash
impl std::hash::Hash for SubqueryExpr {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        // Hash just the discriminant
        std::mem::discriminant(self).hash(state);
        // Note: We don't hash the contents because SelectStmt doesn't implement Hash
        // This is a simplification for the expr_cache usage
    }
}

// Manual Eq implementation for SubqueryExpr
impl Eq for SubqueryExpr {}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum BinaryOp {
    Equal, NotEqual, Less, Greater,
    LessEqual, GreaterEqual,
    And, Or,
    Add, Sub, Mul, Div,
}
