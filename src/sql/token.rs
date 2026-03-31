#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    Select, Insert, Update, Delete,
    Create, Drop, Table, Index, View, Trigger,
    From, Where, Set, Values,
    Into, On,
    And, Or, Not, Null, True, False,
    Begin, Commit, Rollback, Transaction,
    Primary, Key,
    Integer, Text, Real, Blob, Vector,
    Limit, Offset, Order, By, Asc, Desc,
    Count, Sum, Avg, Min, Max,
    Join, Inner, Left,
    Group, Having,
    Using, Unique,
    // Subquery keywords
    Exists, In,
    // Foreign key tokens
    Foreign, References, Cascade, Restrict, Default,
    Action, No, Deferrable, Deferred, Immediate,
    // ALTER TABLE keywords
    Alter, Add, Column, Rename, To,
    // View keywords
    As, If,
    // CTE keywords
    With, Recursive,
    // Trigger keywords
    Before, After, Instead, Of, For, Each, Row, 
    When, Then, End, New, Old,
    // Window function keywords
    Over, Partition, Range, Rows, Between, Unbounded, Preceding, Following, Current, 
    RowNumber, Rank, DenseRank, Lead, Lag, FirstValue, LastValue, NthValue,
    // Virtual table keywords
    Virtual, Fts5, Rtree,
    // JSON keywords
    Json, JsonArray, JsonObject, JsonExtract, JsonType,
    // Match operator for FTS
    Match,
    // Check option
    Check, Option,
    // Generated columns
    Generated, Always,
    // Others
    Dot,  // For json path like '$.name'
    Identifier(String),
    StringLiteral(String),
    NumberLiteral(i64),
    FloatLiteral(f64),
    Equal, NotEqual, Less, Greater,
    LessEqual, GreaterEqual,
    Plus, Minus, Star, Slash,
    Semicolon, Comma, LParen, RParen, LBracket, RBracket,
    QuestionMark,  // 占位符 `?`
    Eof,
}
