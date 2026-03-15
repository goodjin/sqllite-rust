# 接口规约文档

## 文档信息
- **项目名称**: sqllite-rust
- **版本**: v1.0
- **对应PRD**: docs/v1/01-prd.md
- **更新日期**: 2026-03-14

---

## 接口清单

| 编号 | 接口名称 | 所属模块 | 对应用户故事 | 对应PRD |
|-----|---------|---------|------------|---------|
| API-001 | Database::open | MOD-01 | US-001 | FR-001 |
| API-002 | Database::execute | MOD-04 | US-002~006 | FR-006~013 |
| API-003 | BTree::insert | MOD-01 | US-003 | FR-003 |
| API-004 | BTree::search | MOD-01 | US-004 | FR-003 |
| API-005 | BTree::delete | MOD-01 | US-006 | FR-003 |
| API-006 | BTree::scan | MOD-01 | US-004 | FR-003 |
| API-007 | Parser::parse | MOD-03 | US-002~006 | FR-005 |
| API-008 | VM::execute_sql | MOD-04 | US-002~006 | FR-006~013 |
| API-009 | TransactionManager::begin | MOD-05 | US-007 | FR-014 |
| API-010 | TransactionManager::commit | MOD-05 | US-007 | FR-014 |
| API-011 | TransactionManager::rollback | MOD-05 | US-007 | FR-014 |
| API-012 | IndexManager::create_index | MOD-06 | US-008 | FR-016 |
| API-013 | IndexManager::search_index | MOD-06 | US-008 | FR-016 |

---

## 接口详细定义

### API-001: Database::open

**对应PRD**:
- 用户故事: US-001
- 验收标准: AC-001-01, AC-001-02

**所属模块**: MOD-01 (Storage)

**接口定义**:
```rust
pub fn open(path: &str) -> Result<Database, DatabaseError>
```

**功能描述**: 打开或创建数据库文件

**请求参数**:
| 参数名 | 类型 | 必填 | 来源 | 说明 |
|-------|------|------|------|------|
| path | &str | 是 | 用户输入 | 数据库文件路径 |

**响应格式**:
```rust
enum DatabaseError {
    IoError(std::io::Error),
    InvalidFormat,
    PermissionDenied,
}
```

**边界条件**:
| 编号 | 条件 | 处理方式 |
|-----|------|---------|
| BOUND-001 | 文件不存在 | 创建新数据库文件 |
| BOUND-002 | 文件已存在但格式无效 | 返回 InvalidFormat 错误 |
| BOUND-003 | 文件权限不足 | 返回 PermissionDenied 错误 |

---

### API-002: Database::execute

**对应PRD**:
- 用户故事: US-002~006
- 验收标准: AC-002-01~AC-006-03

**所属模块**: MOD-04 (VM)

**接口定义**:
```rust
pub fn execute(&self, sql: &str) -> Result<ResultSet, ExecuteError>
```

**功能描述**: 执行 SQL 语句

**请求参数**:
| 参数名 | 类型 | 必填 | 来源 | 说明 |
|-------|------|------|------|------|
| sql | &str | 是 | 用户输入 | SQL 语句 |

**响应格式**:
```rust
struct ResultSet {
    pub rows: Vec<Row>,
    pub columns: Vec<String>,
    pub affected_rows: usize,
}

struct Row {
    pub values: Vec<Value>,
}

enum ExecuteError {
    ParseError(ParseError),
    ExecutionError(VmError),
    TransactionError(TransactionError),
}
```

**边界条件**:
| 编号 | 条件 | 处理方式 |
|-----|------|---------|
| BOUND-004 | SQL 语法错误 | 返回 ParseError |
| BOUND-005 | 表不存在 | 返回 ExecutionError::TableNotFound |
| BOUND-006 | 列不存在 | 返回 ExecutionError::ColumnNotFound |

---

### API-003: BTree::insert

**对应PRD**:
- 用户故事: US-003
- 验收标准: AC-003-03

**所属模块**: MOD-01 (Storage)

**接口定义**:
```rust
pub fn insert(&mut self, key: Key, value: Value) -> Result<(), StorageError>
```

**功能描述**: 向 B+ Tree 插入键值对

**请求参数**:
| 参数名 | 类型 | 必填 | 来源 | 说明 |
|-------|------|------|------|------|
| key | Key | 是 | VM | 主键值 |
| value | Value | 是 | VM | 序列化后的记录 |

**响应格式**:
```rust
enum StorageError {
    DuplicateKey,
    PageFull,
    IoError(std::io::Error),
}
```

**边界条件**:
| 编号 | 条件 | 处理方式 |
|-----|------|---------|
| BOUND-007 | 键已存在 | 返回 DuplicateKey 错误 |
| BOUND-008 | 页面已满 | 触发节点分裂 |

---

### API-004: BTree::search

**对应PRD**:
- 用户故事: US-004
- 验收标准: AC-004-01~AC-004-04

**所属模块**: MOD-01 (Storage)

**接口定义**:
```rust
pub fn search(&self, key: &Key) -> Option<Value>
```

**功能描述**: 在 B+ Tree 中搜索键

**请求参数**:
| 参数名 | 类型 | 必填 | 来源 | 说明 |
|-------|------|------|------|------|
| key | &Key | 是 | VM | 要搜索的键 |

**响应格式**:
```rust
Option<Value>  // Some(Value) 表示找到，None 表示未找到
```

---

### API-005: BTree::delete

**对应PRD**:
- 用户故事: US-006
- 验收标准: AC-006-01~AC-006-03

**所属模块**: MOD-01 (Storage)

**接口定义**:
```rust
pub fn delete(&mut self, key: &Key) -> Result<(), StorageError>
```

**功能描述**: 从 B+ Tree 删除键值对

**请求参数**:
| 参数名 | 类型 | 必填 | 来源 | 说明 |
|-------|------|------|------|------|
| key | &Key | 是 | VM | 要删除的键 |

**响应格式**:
```rust
enum StorageError {
    KeyNotFound,
    IoError(std::io::Error),
}
```

**边界条件**:
| 编号 | 条件 | 处理方式 |
|-----|------|---------|
| BOUND-009 | 键不存在 | 返回 KeyNotFound 错误 |
| BOUND-010 | 节点填充率过低 | 触发节点合并或重分配 |

---

### API-006: BTree::scan

**对应PRD**:
- 用户故事: US-004
- 验收标准: AC-004-04

**所属模块**: MOD-01 (Storage)

**接口定义**:
```rust
pub fn scan(&self) -> Cursor
```

**功能描述**: 创建 B+ Tree 游标，用于全表扫描

**响应格式**:
```rust
struct Cursor {
    // 内部实现
}

impl Iterator for Cursor {
    type Item = (Key, Value);
    // ...
}
```

---

### API-007: Parser::parse

**对应PRD**:
- 用户故事: US-002~006
- 验收标准: AC-002-01~AC-006-03

**所属模块**: MOD-03 (Parser)

**接口定义**:
```rust
pub fn parse(sql: &str) -> Result<Statement, ParseError>
```

**功能描述**: 解析 SQL 字符串为 AST

**请求参数**:
| 参数名 | 类型 | 必填 | 来源 | 说明 |
|-------|------|------|------|------|
| sql | &str | 是 | 用户输入 | SQL 字符串 |

**响应格式**:
```rust
enum Statement {
    Select(SelectStmt),
    Insert(InsertStmt),
    Update(UpdateStmt),
    Delete(DeleteStmt),
    CreateTable(CreateTableStmt),
    DropTable(DropTableStmt),
    // ...
}

enum ParseError {
    UnexpectedToken(Token),
    ExpectedToken { expected: String, found: String },
    ExpectedIdentifier,
    ExpectedSemicolon,
}
```

---

### API-008: VM::execute_sql

**对应PRD**:
- 用户故事: US-002~006
- 验收标准: AC-002-01~AC-006-03

**所属模块**: MOD-04 (VM)

**接口定义**:
```rust
pub fn execute_sql(&mut self, sql: &str) -> Result<ResultSet, VmError>
```

**功能描述**: 执行 SQL 语句（完整流程：解析→优化→生成字节码→执行）

**请求参数**:
| 参数名 | 类型 | 必填 | 来源 | 说明 |
|-------|------|------|------|------|
| sql | &str | 是 | 用户输入 | SQL 语句 |

**响应格式**:
```rust
struct ResultSet {
    pub rows: Vec<Row>,
    pub columns: Vec<String>,
    pub affected_rows: usize,
}

enum VmError {
    ParseError(ParseError),
    ExecutionError(String),
    StorageError(StorageError),
}
```

---

### API-009: TransactionManager::begin

**对应PRD**:
- 用户故事: US-007
- 验收标准: AC-007-01

**所属模块**: MOD-05 (Transaction)

**接口定义**:
```rust
pub fn begin(&mut self) -> Result<(), TransactionError>
```

**功能描述**: 开始事务

**响应格式**:
```rust
enum TransactionError {
    TransactionAlreadyActive,
    IoError(std::io::Error),
}
```

**边界条件**:
| 编号 | 条件 | 处理方式 |
|-----|------|---------|
| BOUND-011 | 已有活跃事务 | 返回 TransactionAlreadyActive 错误 |

---

### API-010: TransactionManager::commit

**对应PRD**:
- 用户故事: US-007
- 验收标准: AC-007-02

**所属模块**: MOD-05 (Transaction)

**接口定义**:
```rust
pub fn commit(&mut self) -> Result<(), TransactionError>
```

**功能描述**: 提交事务

**响应格式**:
```rust
enum TransactionError {
    NoActiveTransaction,
    IoError(std::io::Error),
}
```

**边界条件**:
| 编号 | 条件 | 处理方式 |
|-----|------|---------|
| BOUND-012 | 无活跃事务 | 返回 NoActiveTransaction 错误 |

---

### API-011: TransactionManager::rollback

**对应PRD**:
- 用户故事: US-007
- 验收标准: AC-007-03

**所属模块**: MOD-05 (Transaction)

**接口定义**:
```rust
pub fn rollback(&mut self) -> Result<(), TransactionError>
```

**功能描述**: 回滚事务

**响应格式**:
```rust
enum TransactionError {
    NoActiveTransaction,
    IoError(std::io::Error),
}
```

**边界条件**:
| 编号 | 条件 | 处理方式 |
|-----|------|---------|
| BOUND-013 | 无活跃事务 | 返回 NoActiveTransaction 错误 |

---

### API-012: IndexManager::create_index

**对应PRD**:
- 用户故事: US-008
- 验收标准: AC-008-01

**所属模块**: MOD-06 (Index)

**接口定义**:
```rust
pub fn create_index(
    &mut self,
    index_name: &str,
    table: &str,
    column: &str,
    unique: bool,
) -> Result<(), IndexError>
```

**功能描述**: 创建索引

**请求参数**:
| 参数名 | 类型 | 必填 | 来源 | 说明 |
|-------|------|------|------|------|
| index_name | &str | 是 | SQL | 索引名称 |
| table | &str | 是 | SQL | 表名 |
| column | &str | 是 | SQL | 列名 |
| unique | bool | 是 | SQL | 是否唯一索引 |

**响应格式**:
```rust
enum IndexError {
    TableNotFound(String),
    ColumnNotFound(String),
    DuplicateKey(String),
    IndexNotFound(String),
}
```

---

### API-013: IndexManager::search_index

**对应PRD**:
- 用户故事: US-008
- 验收标准: AC-008-02

**所属模块**: MOD-06 (Index)

**接口定义**:
```rust
pub fn search_index(
    &self,
    index_name: &str,
    value: &Value,
) -> Result<Vec<RowId>, IndexError>
```

**功能描述**: 使用索引查找

**请求参数**:
| 参数名 | 类型 | 必填 | 来源 | 说明 |
|-------|------|------|------|------|
| index_name | &str | 是 | VM | 索引名称 |
| value | &Value | 是 | VM | 查找值 |

**响应格式**:
```rust
Vec<RowId>  // 匹配的行 ID 列表
```

---

## 错误码汇总

| 错误码 | 错误类型 | 场景 | 恢复方案 |
|-------|---------|------|---------|
| InvalidFormat | DatabaseError | 数据库文件格式无效 | 删除文件重新创建 |
| PermissionDenied | DatabaseError | 文件权限不足 | 修改文件权限 |
| TableNotFound | ExecutionError/IndexError | 表不存在 | 检查表名拼写 |
| ColumnNotFound | ExecutionError/IndexError | 列不存在 | 检查列名拼写 |
| DuplicateKey | StorageError/IndexError | 键已存在 | 使用不同的键值 |
| KeyNotFound | StorageError | 键不存在 | 检查键值 |
| TransactionAlreadyActive | TransactionError | 已有活跃事务 | 先提交或回滚当前事务 |
| NoActiveTransaction | TransactionError | 无活跃事务 | 先开始事务 |
| IndexNotFound | IndexError | 索引不存在 | 检查索引名 |

---

## 变更历史

| 版本 | 日期 | 变更内容 | 作者 |
|-----|------|---------|------|
| 1.0 | 2026-03-14 | 初始版本 | Claude |
