# 边界条件规约文档

## 文档信息
- **项目名称**: sqllite-rust
- **版本**: v1.0
- **对应PRD**: docs/v1/01-prd.md
- **更新日期**: 2026-03-14

---

## 边界条件清单

| 编号 | 边界条件名称 | 类型 | 对应PRD | 对应接口 |
|-----|------------|------|---------|---------|
| BOUND-001 | 创建新数据库文件 | 文件操作 | AC-001-01 | API-001 |
| BOUND-002 | 打开已存在的数据库文件 | 文件操作 | AC-001-02 | API-001 |
| BOUND-003 | 数据库文件格式版本验证 | 文件操作 | AC-001-03 | API-001 |
| BOUND-004 | 表已存在 | 数据约束 | AC-002-01 | API-002 |
| BOUND-005 | 数据类型不支持 | 数据约束 | AC-002-02 | API-002 |
| BOUND-006 | 表不存在 | 数据约束 | AC-002-03 | API-002 |
| BOUND-007 | 插入值数量不匹配 | 数据约束 | AC-003-01 | API-002 |
| BOUND-008 | 类型不匹配 | 数据约束 | AC-003-02 | API-002 |
| BOUND-009 | 主键重复 | 数据约束 | AC-003-03 | API-003 |
| BOUND-010 | 列不存在 | 数据约束 | AC-004-02 | API-002 |
| BOUND-011 | WHERE 条件语法错误 | 语法约束 | AC-004-03 | API-007 |
| BOUND-012 | 空结果集 | 数据约束 | AC-004-04 | API-004 |
| BOUND-013 | 更新行数为零 | 数据约束 | AC-005-03 | API-002 |
| BOUND-014 | 删除行数为零 | 数据约束 | AC-006-03 | API-002 |
| BOUND-015 | 嵌套事务 | 事务约束 | AC-007-01 | API-009 |
| BOUND-016 | 无事务提交/回滚 | 事务约束 | AC-007-02/03 | API-010/011 |
| BOUND-017 | 事务失败自动回滚 | 事务约束 | AC-007-04 | API-009~011 |
| BOUND-018 | 索引列不存在 | 数据约束 | AC-008-01 | API-012 |
| BOUND-019 | 唯一索引冲突 | 数据约束 | AC-008-03 | API-012 |

---

## 边界条件详细定义

### BOUND-001: 创建新数据库文件

**对应PRD**: AC-001-01

**所属接口**: API-001 (Database::open)

**边界类型**: 文件操作

**触发条件**:
- 指定的数据库文件路径不存在

**处理流程**:
1. 创建新文件
2. 初始化第 1 页（文件头页）
3. 写入 DatabaseHeader（默认配置）
4. 初始化 sqlite_master 表
5. 关闭文件（后续操作重新打开）

**验证规则**:
- 文件路径有效（父目录存在且有写权限）
- 页面大小为 512 的倍数（512-32768）

**错误处理**:
| 错误码 | 场景 | 处理方式 |
|-------|------|---------|
| IoError | 磁盘空间不足 | 返回错误，删除临时文件 |
| PermissionDenied | 无写权限 | 返回错误 |

---

### BOUND-002: 打开已存在的数据库文件

**对应PRD**: AC-001-02

**所属接口**: API-001 (Database::open)

**边界类型**: 文件操作

**触发条件**:
- 指定的数据库文件路径已存在

**处理流程**:
1. 打开文件（读写模式）
2. 读取并验证文件头魔数
3. 读取页面大小等元数据
4. 验证文件格式版本
5. 初始化页面缓存

**验证规则**:
- 魔数必须为 "SQLite format 3\0"
- 页面大小必须为 512 的倍数
- 文件格式版本必须兼容

**错误处理**:
| 错误码 | 场景 | 处理方式 |
|-------|------|---------|
| InvalidFormat | 魔数不匹配 | 返回错误 |
| InvalidFormat | 页面大小无效 | 返回错误 |
| PermissionDenied | 无读写权限 | 返回错误 |

---

### BOUND-003: 数据库文件格式版本验证

**对应PRD**: AC-001-03

**所属接口**: API-001 (Database::open)

**边界类型**: 文件操作

**验证规则**:
```rust
// 支持的文件格式版本
const SUPPORTED_WRITE_VERSION: u8 = 1;
const SUPPORTED_READ_VERSION: u8 = 1;

fn validate_format(header: &DatabaseHeader) -> Result<(), DatabaseError> {
    if header.file_format_write > SUPPORTED_WRITE_VERSION {
        return Err(DatabaseError::UnsupportedWriteVersion);
    }
    if header.file_format_read > SUPPORTED_READ_VERSION {
        return Err(DatabaseError::UnsupportedReadVersion);
    }
    Ok(())
}
```

---

### BOUND-004: 表已存在

**对应PRD**: AC-002-01

**所属接口**: API-002 (Database::execute - CREATE TABLE)

**边界类型**: 数据约束

**触发条件**:
- CREATE TABLE 指定的表名已存在

**处理流程**:
1. 检查 sqlite_master 中是否已存在同名表
2. 如果存在，返回错误

**错误处理**:
| 错误码 | 场景 | 处理方式 |
|-------|------|---------|
| TableAlreadyExists | 表已存在 | 返回错误，提示用户 |

---

### BOUND-005: 数据类型不支持

**对应PRD**: AC-002-02

**所属接口**: API-002 (Database::execute - CREATE TABLE)

**边界类型**: 数据约束

**支持的数据类型**:
| 类型 | 说明 | 存储大小 |
|-----|------|---------|
| INTEGER | 8 字节有符号整数 | 8 bytes |
| TEXT | 变长字符串 | 2 bytes length + data |
| REAL | 8 字节浮点数 | 8 bytes |
| BLOB | 二进制数据 | 2 bytes length + data |

**不支持的数据类型**:
- VARCHAR (使用 TEXT 代替)
- INT (使用 INTEGER 代替)
- FLOAT (使用 REAL 代替)
- DECIMAL (暂不支持)
- DATE/TIME (暂不支持)

**错误处理**:
| 错误码 | 场景 | 处理方式 |
|-------|------|---------|
| UnsupportedDataType | 不支持的类型 | 返回错误 |

---

### BOUND-006: 表不存在

**对应PRD**: AC-002-03

**所属接口**: API-002 (Database::execute)

**边界类型**: 数据约束

**触发条件**:
- 操作（SELECT/INSERT/UPDATE/DELETE）指定的表不存在

**处理流程**:
1. 从 sqlite_master 查询表元数据
2. 如果未找到，返回错误

**错误处理**:
| 错误码 | 场景 | 处理方式 |
|-------|------|---------|
| TableNotFound | 表不存在 | 返回错误 |

---

### BOUND-007: 插入值数量不匹配

**对应PRD**: AC-003-01

**所属接口**: API-002 (Database::execute - INSERT)

**边界类型**: 数据约束

**触发条件**:
- INSERT 语句中值的数量与列数不匹配

**示例**:
```sql
-- 错误：值数量不匹配
INSERT INTO users (id, name) VALUES (1);  -- 缺少 name 值
INSERT INTO users VALUES (1, 'Alice', 25); -- 列数不匹配（假设只有 id, name 两列）
```

**处理流程**:
1. 解析 INSERT 语句
2. 获取目标表的列数
3. 验证值的数量与列数匹配
4. 如果不匹配，返回错误

**错误处理**:
| 错误码 | 场景 | 处理方式 |
|-------|------|---------|
| ValueCountMismatch | 值数量不匹配 | 返回错误 |

---

### BOUND-008: 类型不匹配

**对应PRD**: AC-003-02

**所属接口**: API-002 (Database::execute - INSERT/UPDATE)

**边界类型**: 数据约束

**触发条件**:
- 插入或更新的值类型与列定义不匹配

**类型转换规则**:
| 目标类型 | 源类型 | 转换结果 |
|---------|-------|---------|
| INTEGER | INTEGER | 直接存储 |
| INTEGER | TEXT | 尝试解析为整数，失败则错误 |
| INTEGER | REAL | 截断为整数 |
| INTEGER | NULL | 存储 NULL |
| TEXT | TEXT | 直接存储 |
| TEXT | INTEGER | 转换为字符串 |
| TEXT | REAL | 转换为字符串 |
| TEXT | NULL | 存储 NULL |

**错误处理**:
| 错误码 | 场景 | 处理方式 |
|-------|------|---------|
| TypeMismatch | 无法转换 | 返回错误 |

---

### BOUND-009: 主键重复

**对应PRD**: AC-003-03

**所属接口**: API-003 (BTree::insert)

**边界类型**: 数据约束

**触发条件**:
- 插入的记录主键（row_id）已存在

**处理流程**:
1. 在 B+ Tree 中搜索要插入的键
2. 如果键已存在，返回错误

**错误处理**:
| 错误码 | 场景 | 处理方式 |
|-------|------|---------|
| DuplicateKey | 主键重复 | 返回错误 |

---

### BOUND-010: 列不存在

**对应PRD**: AC-004-02

**所属接口**: API-002 (Database::execute - SELECT/INSERT/UPDATE)

**边界类型**: 数据约束

**触发条件**:
- 引用了表中不存在的列

**示例**:
```sql
-- 错误：列不存在
SELECT nonexistent_column FROM users;
UPDATE users SET nonexistent_column = 'value';
```

**处理流程**:
1. 获取表结构
2. 验证所有引用的列存在
3. 如果有列不存在，返回错误

**错误处理**:
| 错误码 | 场景 | 处理方式 |
|-------|------|---------|
| ColumnNotFound | 列不存在 | 返回错误 |

---

### BOUND-011: WHERE 条件语法错误

**对应PRD**: AC-004-03

**所属接口**: API-007 (Parser::parse)

**边界类型**: 语法约束

**触发条件**:
- WHERE 子句语法不正确

**示例**:
```sql
-- 错误：语法错误
SELECT * FROM users WHERE;  -- 缺少条件
SELECT * FROM users WHERE id = ;  -- 缺少值
SELECT * FROM users WHERE id 1;  -- 缺少运算符
```

**处理流程**:
1. 解析 WHERE 子句
2. 如果遇到语法错误，返回 ParseError

**错误处理**:
| 错误码 | 场景 | 处理方式 |
|-------|------|---------|
| ParseError | 语法错误 | 返回错误，包含位置信息 |

---

### BOUND-012: 空结果集

**对应PRD**: AC-004-04

**所属接口**: API-004 (BTree::search)

**边界类型**: 数据约束

**触发条件**:
- 查询条件不匹配任何记录

**处理流程**:
1. 正常执行查询
2. 如果没有匹配的记录，返回空结果集
3. 这不是错误，是正常结果

**响应**:
```rust
ResultSet {
    rows: vec![],  // 空数组
    columns: vec!["id", "name"],  // 列名仍然存在
    affected_rows: 0,
}
```

---

### BOUND-013: 更新行数为零

**对应PRD**: AC-005-03

**所属接口**: API-002 (Database::execute - UPDATE)

**边界类型**: 数据约束

**触发条件**:
- UPDATE 语句的 WHERE 条件不匹配任何记录

**处理流程**:
1. 正常执行 UPDATE
2. 返回 affected_rows = 0
3. 这不是错误，是正常结果

---

### BOUND-014: 删除行数为零

**对应PRD**: AC-006-03

**所属接口**: API-002 (Database::execute - DELETE)

**边界类型**: 数据约束

**触发条件**:
- DELETE 语句的 WHERE 条件不匹配任何记录

**处理流程**:
1. 正常执行 DELETE
2. 返回 affected_rows = 0
3. 这不是错误，是正常结果

---

### BOUND-015: 嵌套事务

**对应PRD**: AC-007-01

**所属接口**: API-009 (TransactionManager::begin)

**边界类型**: 事务约束

**触发条件**:
- 在已有活跃事务时调用 BEGIN

**处理流程**:
1. 检查当前事务状态
2. 如果状态为 Active，返回错误
3. 暂不支持嵌套事务

**错误处理**:
| 错误码 | 场景 | 处理方式 |
|-------|------|---------|
| TransactionAlreadyActive | 已有活跃事务 | 返回错误 |

---

### BOUND-016: 无事务提交/回滚

**对应PRD**: AC-007-02, AC-007-03

**所属接口**: API-010, API-011

**边界类型**: 事务约束

**触发条件**:
- 在无活跃事务时调用 COMMIT 或 ROLLBACK

**处理流程**:
1. 检查当前事务状态
2. 如果状态为 AutoCommit，返回错误

**错误处理**:
| 错误码 | 场景 | 处理方式 |
|-------|------|---------|
| NoActiveTransaction | 无活跃事务 | 返回错误 |

---

### BOUND-017: 事务失败自动回滚

**对应PRD**: AC-007-04

**所属接口**: API-009~011

**边界类型**: 事务约束

**触发条件**:
- 事务执行过程中发生错误

**处理流程**:
1. 捕获执行错误
2. 自动调用 ROLLBACK
3. 丢弃所有脏页
4. 返回原始错误

**示例**:
```rust
fn execute_in_transaction(&mut self,
    f: impl FnOnce() -> Result<(), Error>
) -> Result<(), Error> {
    self.begin()?;
    match f() {
        Ok(()) => {
            self.commit()?;
            Ok(())
        }
        Err(e) => {
            self.rollback()?;  // 自动回滚
            Err(e)
        }
    }
}
```

---

### BOUND-018: 索引列不存在

**对应PRD**: AC-008-01

**所属接口**: API-012 (IndexManager::create_index)

**边界类型**: 数据约束

**触发条件**:
- CREATE INDEX 指定的列不存在

**处理流程**:
1. 获取表结构
2. 验证列存在
3. 如果不存在，返回错误

**错误处理**:
| 错误码 | 场景 | 处理方式 |
|-------|------|---------|
| ColumnNotFound | 列不存在 | 返回错误 |

---

### BOUND-019: 唯一索引冲突

**对应PRD**: AC-008-03

**所属接口**: API-012 (IndexManager::create_index)

**边界类型**: 数据约束

**触发条件**:
1. 创建唯一索引时表中已有重复值
2. 向唯一索引插入重复值

**处理流程**:
1. 检查值是否已存在
2. 如果存在，返回错误

**错误处理**:
| 错误码 | 场景 | 处理方式 |
|-------|------|---------|
| DuplicateKey | 重复值 | 返回错误 |

---

## 变更历史

| 版本 | 日期 | 变更内容 | 作者 |
|-----|------|---------|------|
| 1.0 | 2026-03-14 | 初始版本 | Claude |
