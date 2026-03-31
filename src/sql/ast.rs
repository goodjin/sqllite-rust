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
    CreateTrigger(CreateTriggerStmt),
    DropTrigger(DropTriggerStmt),
    CreateVirtualTable(CreateVirtualTableStmt),
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
    pub columns: Option<Vec<String>>,
    pub query: SelectStmt,
    pub with_check_option: bool,  // P5-3: WITH CHECK OPTION
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
    Aggregate(AggregateFunc, Option<String>),
    WindowFunc(WindowFunc, Option<String>),  // P5-4: Window functions
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

/// P5-4: Window function definitions
#[derive(Debug, Clone, PartialEq)]
pub enum WindowFunc {
    RowNumber {
        over: WindowSpec,
    },
    Rank {
        over: WindowSpec,
    },
    DenseRank {
        over: WindowSpec,
    },
    Lead {
        expr: Box<Expression>,
        offset: Option<Box<Expression>>,
        default: Option<Box<Expression>>,
        over: WindowSpec,
    },
    Lag {
        expr: Box<Expression>,
        offset: Option<Box<Expression>>,
        default: Option<Box<Expression>>,
        over: WindowSpec,
    },
    FirstValue {
        expr: Box<Expression>,
        over: WindowSpec,
    },
    LastValue {
        expr: Box<Expression>,
        over: WindowSpec,
    },
    NthValue {
        expr: Box<Expression>,
        n: Box<Expression>,
        over: WindowSpec,
    },
}

/// P5-4: Window specification (PARTITION BY, ORDER BY, frame)
#[derive(Debug, Clone, PartialEq, Default)]
pub struct WindowSpec {
    pub partition_by: Vec<Expression>,
    pub order_by: Vec<OrderBy>,
    pub frame: Option<WindowFrame>,
}

/// P5-4: Window frame specification
#[derive(Debug, Clone, PartialEq)]
pub enum WindowFrame {
    Rows(WindowFrameBound, WindowFrameBound),
    Range(WindowFrameBound, WindowFrameBound),
}

/// P5-4: Window frame bounds
#[derive(Debug, Clone, PartialEq)]
pub enum WindowFrameBound {
    UnboundedPreceding,
    Preceding(i64),
    CurrentRow,
    Following(i64),
    UnboundedFollowing,
}

#[derive(Debug, Clone, PartialEq)]
pub struct InsertStmt {
    pub table: String,
    pub columns: Option<Vec<String>>,
    pub values: Vec<Vec<Expression>>,
    pub ctes: Vec<CommonTableExpr>,  // P5-5: CTE in INSERT
}

#[derive(Debug, Clone, PartialEq)]
pub struct UpdateStmt {
    pub table: String,
    pub set_clauses: Vec<SetClause>,
    pub where_clause: Option<Expression>,
    pub ctes: Vec<CommonTableExpr>,  // P5-5: CTE in UPDATE
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
    pub ctes: Vec<CommonTableExpr>,  // P5-5: CTE in DELETE
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
    pub default_value: Option<Expression>,  // DEFAULT value
    pub is_virtual: bool,  // P5-8: Virtual column for JSON
    pub generated_always: Option<Expression>,  // GENERATED ALWAYS AS
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
    Json,  // P5-8: JSON data type
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

/// P5-2: CREATE TRIGGER statement
#[derive(Debug, Clone, PartialEq)]
pub struct CreateTriggerStmt {
    pub name: String,
    pub timing: TriggerTiming,  // BEFORE, AFTER, INSTEAD OF
    pub event: TriggerEvent,    // INSERT, UPDATE, DELETE
    pub table: String,
    pub for_each_row: bool,
    pub when_clause: Option<Expression>,
    pub body: Vec<TriggerStatement>,
}

/// P5-2: Trigger timing
#[derive(Debug, Clone, PartialEq)]
pub enum TriggerTiming {
    Before,
    After,
    InsteadOf,
}

/// P5-2: Trigger event
#[derive(Debug, Clone, PartialEq)]
pub enum TriggerEvent {
    Insert,
    Update { columns: Option<Vec<String>> },
    Delete,
}

/// P5-2: Statements allowed in trigger body
#[derive(Debug, Clone, PartialEq)]
pub enum TriggerStatement {
    Insert(InsertStmt),
    Update(UpdateStmt),
    Delete(DeleteStmt),
    Select(SelectStmt),  // For SELECT within trigger (rare but allowed)
}

/// P5-2: DROP TRIGGER statement
#[derive(Debug, Clone, PartialEq)]
pub struct DropTriggerStmt {
    pub name: String,
    pub if_exists: bool,
}

/// P5-6/P5-7: CREATE VIRTUAL TABLE statement
#[derive(Debug, Clone, PartialEq)]
pub struct CreateVirtualTableStmt {
    pub name: String,
    pub module: VirtualTableModule,
}

/// P5-6/P5-7: Virtual table modules
#[derive(Debug, Clone, PartialEq)]
pub enum VirtualTableModule {
    Fts5(Vec<String>),  // Column names
    Rtree { 
        id_column: String,
        min_x: String, max_x: String,
        min_y: String, max_y: String,
    },
}

/// P5-8: JSON path expression
#[derive(Debug, Clone, PartialEq)]
pub struct JsonPath {
    pub path: String,
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
    /// P5-8: JSON functions
    JsonFunction {
        func: JsonFunctionType,
        args: Vec<Expression>,
    },
    /// P5-8: JSON path access
    JsonExtract {
        expr: Box<Expression>,
        path: String,
    },
    /// P5-2: NEW/OLD references in triggers
    TriggerReference { is_new: bool, column: String },
    /// 子查询表达式
    Subquery(SubqueryExpr),
}

/// P5-8: JSON function types
#[derive(Debug, Clone, PartialEq)]
pub enum JsonFunctionType {
    Json,           // json()
    JsonArray,      // json_array()
    JsonObject,     // json_object()
    JsonExtract,    // json_extract()
    JsonType,       // json_type()
    JsonValid,      // json_valid()
}

// Manual Hash implementation that handles f64
impl std::hash::Hash for Expression {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        use std::mem::discriminant;
        discriminant(self).hash(state);
        match self {
            Expression::Integer(v) => v.hash(state),
            Expression::String(v) => v.hash(state),
            Expression::Float(v) => v.to_bits().hash(state),
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
            Expression::JsonFunction { func, args } => {
                std::mem::discriminant(func).hash(state);
                args.hash(state);
            }
            Expression::JsonExtract { expr, path } => {
                expr.hash(state);
                path.hash(state);
            }
            Expression::TriggerReference { is_new, column } => {
                is_new.hash(state);
                column.hash(state);
            }
            Expression::Subquery(v) => {
                std::mem::discriminant(v).hash(state);
            }
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
impl std::hash::Hash for SubqueryExpr {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        std::mem::discriminant(self).hash(state);
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
