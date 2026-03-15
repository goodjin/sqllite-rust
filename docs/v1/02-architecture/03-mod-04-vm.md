# MOD-04: 虚拟机执行引擎模块 (Virtual Machine)

## 文档信息
- **项目名称**: sqllite-rust
- **文档编号**: MOD-04
- **版本**: v1.0
- **更新日期**: 2026-03-14
- **对应PRD**: FR-006~013, FR-018

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

**所属层次**: L2-执行层

**架构定位图**:
```
┌─────────────────────────────────────────────────────┐
│              L1: 接口层 (Interface Layer)            │
│              SQL Parser                              │
└───────────────────────┬─────────────────────────────┘
                        │ AST
                        ▼
┌─────────────────────────────────────────────────────┐
│         ★ MOD-04: 虚拟机执行引擎 (VM) ★              │
│         Code Generator → VM → ResultSet              │
└───────────────────────┬─────────────────────────────┘
                        │ 存储操作
                        ▼
┌─────────────────────────────────────────────────────┐
│              L3: 事务层 (Transaction Layer)          │
│              Transaction Manager                     │
└─────────────────────────────────────────────────────┘
```

### 核心职责

- **代码生成**: 将 AST 转换为字节码指令序列
- **查询优化**: 基础的成本估算和执行计划选择
- **虚拟机执行**: 解释执行字节码，操作 B+ Tree
- **结果集管理**: 收集和返回查询结果

### 边界说明

- **负责**:
  - AST 到字节码的转换
  - 字节码执行
  - 游标管理
  - 表达式求值
  - 简单的查询优化（索引选择）

- **不负责**:
  - SQL 解析（由 Parser 负责）
  - 事务管理（由 Transaction Manager 负责）
  - B+ Tree 底层操作（由 Storage Engine 负责）
  - 页面管理（由 Pager 负责）

---

## 对应PRD

| PRD章节 | 编号 | 内容 |
|---------|-----|------|
| 功能需求 | FR-006 | 虚拟机执行引擎 |
| 功能需求 | FR-007 | SELECT 查询 |
| 功能需求 | FR-008 | INSERT 插入 |
| 功能需求 | FR-009 | UPDATE 更新 |
| 功能需求 | FR-010 | DELETE 删除 |
| 功能需求 | FR-011 | WHERE 子句 |
| 功能需求 | FR-012 | CREATE TABLE |
| 功能需求 | FR-013 | DROP TABLE |
| 功能需求 | FR-018 | 查询优化器 |
| 用户故事 | US-002~006 | 所有数据操作 |
| 业务流程 | Flow-001 | SQL 执行流程 |

---

## 全局架构位置

```
┌─────────────────────────────────────────────────────────────────┐
│                        L1: 接口层                                │
│  ┌───────────────────────────────────────────────────────────┐ │
│  │                    SQL Parser (MOD-03)                     │ │
│  └───────────────────────────┬───────────────────────────────┘ │
└──────────────────────────────┼──────────────────────────────────┘
                               │ AST
                               ▼
┌──────────────────────────────┬──────────────────────────────────┐
│                        L2: 执行层                                │
│  ┌───────────────────────────▼───────────────────────────────┐ │
│  │              ★ MOD-04 Virtual Machine ★                    │ │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐ │ │
│  │  │  Optimizer  │→ │  Code Gen   │→ │    VM Executor      │ │ │
│  │  │             │  │             │  │                     │ │ │
│  │  └─────────────┘  └─────────────┘  └─────────────────────┘ │ │
│  └───────────────────────────┬───────────────────────────────┘ │
└──────────────────────────────┼──────────────────────────────────┘
                               │ 存储操作
                               ▼
┌──────────────────────────────┬──────────────────────────────────┐
│                        L3: 事务层                                │
│  ┌───────────────────────────▼───────────────────────────────┐ │
│  │              Transaction Manager (MOD-05)                  │ │
│  └───────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
```

---

## 依赖关系

### 上游依赖（本模块调用的模块）

| 模块名称 | 模块编号 | 依赖原因 | 调用方式 |
|---------|---------|---------|---------|
| SQL Parser | MOD-03 | 获取 AST | 直接调用 |
| Transaction Manager | MOD-05 | 事务上下文 | TransactionLayer trait |
| Storage Engine | MOD-01 | B+ Tree 操作 | StorageLayer trait |

### 下游依赖（调用本模块的模块）

| 模块名称 | 模块编号 | 被调用场景 | 调用方式 |
|---------|---------|-----------|---------|
| Database | - | 执行 SQL | 直接调用 |

---

## 数据流

### 输入数据流

| 数据项 | 来源 | 格式 | 说明 |
|-------|------|------|------|
| AST | Parser | Statement | 解析后的 SQL |
| 事务上下文 | Transaction Manager | Transaction | 当前事务 |

### 输出数据流

| 数据项 | 目标 | 格式 | 说明 |
|-------|------|------|------|
| 结果集 | 用户 | ResultSet | 查询结果 |
| 影响行数 | 用户 | usize | DML 操作影响行数 |

---

## 核心设计

### 设计目标

| 目标 | 描述 | 度量标准 |
|-----|------|---------|
| 执行效率 | 高效执行字节码 | 简单查询 < 5ms |
| 可调试性 | 支持执行跟踪 | 可打印执行计划 |
| 可扩展性 | 易于添加新指令 | 模块化指令实现 |

### 核心组件

#### 1. 字节码指令集

```rust
/// 虚拟机操作码
#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(u8)]
pub enum OpCode {
    // 游标操作
    OpenRead = 0x01,      // 打开只读游标: P1=游标编号, P2=根页面
    OpenWrite = 0x02,     // 打开读写游标: P1=游标编号, P2=根页面
    Close = 0x03,         // 关闭游标: P1=游标编号

    // 导航操作
    Rewind = 0x10,        // 移动到第一条: P1=游标, P2=跳转地址(空表时)
    Next = 0x11,          // 移动到下一条: P1=游标, P2=跳转地址
    Prev = 0x12,          // 移动到上一条: P1=游标, P2=跳转地址
    Seek = 0x13,          // 定位到指定键: P1=游标, P2=键寄存器

    // 列操作
    Column = 0x20,        // 读取列值: P1=游标, P2=列索引, P3=目标寄存器
    RowId = 0x21,         // 读取行 ID: P1=游标, P2=目标寄存器

    // 数据操作
    Insert = 0x30,        // 插入记录: P1=游标, P2=键寄存器, P3=值寄存器
    Delete = 0x31,        // 删除记录: P1=游标
    Update = 0x32,        // 更新记录: P1=游标, P2=值寄存器

    // 索引操作
    OpenIndex = 0x40,     // 打开索引: P1=游标, P2=索引根页面
    IndexInsert = 0x41,   // 插入索引: P1=游标, P2=键寄存器, P3=行 ID 寄存器
    IndexSeek = 0x42,     // 索引查找: P1=游标, P2=键寄存器, P3=结果寄存器

    // 寄存器操作
    LoadNull = 0x50,      // 加载 NULL: P1=目标寄存器
    LoadInteger = 0x51,   // 加载整数: P1=值, P2=目标寄存器
    LoadString = 0x52,    // 加载字符串: P1=字符串常量索引, P2=目标寄存器
    Move = 0x53,          // 寄存器复制: P1=源, P2=目标

    // 比较操作
    Compare = 0x60,       // 比较: P1=左寄存器, P2=右寄存器, P3=结果寄存器
    Eq = 0x61,            // 等于: P1=左, P2=右, P3=跳转地址(假时跳转)
    Ne = 0x62,            // 不等于
    Lt = 0x63,            // 小于
    Gt = 0x64,            // 大于
    Le = 0x65,            // 小于等于
    Ge = 0x66,            // 大于等于

    // 逻辑操作
    And = 0x70,           // 逻辑与: P1=左, P2=右, P3=结果
    Or = 0x71,            // 逻辑或
    Not = 0x72,           // 逻辑非: P1=源, P2=结果

    // 算术操作
    Add = 0x80,           // 加法: P1=左, P2=右, P3=结果
    Sub = 0x81,           // 减法
    Mul = 0x82,           // 乘法
    Div = 0x83,           // 除法

    // 控制流
    Jump = 0x90,          // 无条件跳转: P1=目标地址
    JumpIfTrue = 0x91,    // 真时跳转: P1=条件寄存器, P2=目标地址
    JumpIfFalse = 0x92,   // 假时跳转: P1=条件寄存器, P2=目标地址

    // 结果操作
    ResultRow = 0xA0,     // 返回结果行: P1=起始寄存器, P2=列数
    Halt = 0xA1,          // 停止执行: P1=返回码

    // 事务操作
    Begin = 0xB0,         // 开始事务
    Commit = 0xB1,        // 提交事务
    Rollback = 0xB2,      // 回滚事务

    // 元数据操作
    CreateTable = 0xC0,   // 创建表: P1=根页面寄存器
    DropTable = 0xC1,     // 删除表
    CreateIndex = 0xC2,   // 创建索引
    DropIndex = 0xC3,     // 删除索引
}

/// 指令
#[derive(Debug, Clone)]
pub struct Instruction {
    pub opcode: OpCode,
    pub p1: i32,      // 参数 1
    pub p2: i32,      // 参数 2
    pub p3: i32,      // 参数 3
    pub comment: Option<String>,  // 注释（调试用）
}

impl Instruction {
    pub fn new(opcode: OpCode, p1: i32, p2: i32, p3: i32) -> Self {
        Self {
            opcode,
            p1,
            p2,
            p3,
            comment: None,
        }
    }

    pub fn with_comment(mut self, comment: impl Into<String>) -> Self {
        self.comment = Some(comment.into());
        self
    }
}
```

#### 2. 虚拟机结构

```rust
/// 虚拟机
pub struct VirtualMachine {
    /// 指令序列
    program: Vec<Instruction>,
    /// 程序计数器
    pc: usize,
    /// 寄存器数组
    registers: Vec<Value>,
    /// 游标数组
    cursors: Vec<Option<Cursor>>,
    /// 结果集
    result_set: Vec<Row>,
    /// 当前结果行寄存器
    result_columns: Vec<usize>,
    /// 存储层接口
    storage: Arc<dyn StorageLayer>,
    /// 事务管理器
    txn_manager: Arc<dyn TransactionLayer>,
}

/// 游标
pub struct Cursor {
    /// 游标类型
    cursor_type: CursorType,
    /// B+ Tree 迭代器
    btree_iter: Option<BTreeCursor>,
    /// 当前记录
    current_record: Option<Record>,
    /// 是否已关闭
    is_closed: bool,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CursorType {
    Read,   // 只读游标
    Write,  // 读写游标
}

/// 结果行
#[derive(Debug, Clone)]
pub struct Row {
    pub values: Vec<Value>,
}

/// 结果集
#[derive(Debug)]
pub struct ResultSet {
    pub rows: Vec<Row>,
    pub columns: Vec<String>,
}
```

#### 3. 代码生成器

```rust
/// 代码生成器
pub struct CodeGenerator {
    /// 生成的指令
    instructions: Vec<Instruction>,
    /// 下一个可用寄存器编号
    next_reg: usize,
    /// 字符串常量池
    string_pool: Vec<String>,
}

impl CodeGenerator {
    pub fn new() -> Self {
        Self {
            instructions: Vec::new(),
            next_reg: 1,  // 寄存器从 1 开始，0 保留
            string_pool: Vec::new(),
        }
    }

    /// 分配新寄存器
    pub fn alloc_register(&mut self) -> usize {
        let reg = self.next_reg;
        self.next_reg += 1;
        reg
    }

    /// 添加指令
    pub fn emit(&mut self, instr: Instruction) -> usize {
        let addr = self.instructions.len();
        self.instructions.push(instr);
        addr
    }

    /// 生成 SELECT 语句的字节码
    pub fn generate_select(&mut self, stmt: &SelectStmt) -> Result<Vec<Instruction>, VmError> {
        // 1. 打开表游标
        let cursor = 0;  // 使用游标 0
        let table_root_page = self.get_table_root_page(&stmt.from)?;

        self.emit(Instruction::new(
            OpCode::OpenRead,
            cursor,
            table_root_page as i32,
            0,
        ).with_comment(format!("Open table '{}' for reading", stmt.from)));

        // 2. 跳转到第一条记录
        let rewind_addr = self.instructions.len();
        self.emit(Instruction::new(
            OpCode::Rewind,
            cursor,
            0,  // 占位，稍后回填
            0,
        ));

        // 3. WHERE 条件处理
        if let Some(ref where_expr) = stmt.where_clause {
            let condition_reg = self.generate_expression(where_expr)?;
            self.emit(Instruction::new(
                OpCode::JumpIfFalse,
                condition_reg as i32,
                0,  // 占位，稍后回填
                0,
            ));
        }

        // 4. 读取列值到寄存器
        let mut result_regs = Vec::new();
        for col in &stmt.columns {
            match col {
                SelectColumn::All => {
                    // 读取所有列
                    let table_schema = self.get_table_schema(&stmt.from)?;
                    for (i, _) in table_schema.columns.iter().enumerate() {
                        let reg = self.alloc_register();
                        self.emit(Instruction::new(
                            OpCode::Column,
                            cursor,
                            i as i32,
                            reg as i32,
                        ));
                        result_regs.push(reg);
                    }
                }
                SelectColumn::Column(name) => {
                    let col_idx = self.get_column_index(&stmt.from, name)?;
                    let reg = self.alloc_register();
                    self.emit(Instruction::new(
                        OpCode::Column,
                        cursor,
                        col_idx as i32,
                        reg as i32,
                    ));
                    result_regs.push(reg);
                }
                _ => unimplemented!(),
            }
        }

        // 5. 返回结果行
        if !result_regs.is_empty() {
            self.emit(Instruction::new(
                OpCode::ResultRow,
                result_regs[0] as i32,
                result_regs.len() as i32,
            ));
        }

        // 6. Next 循环
        let next_addr = self.instructions.len();
        self.emit(Instruction::new(
            OpCode::Next,
            cursor,
            rewind_addr as i32,  // 跳回循环开始
            0,
        ));

        // 回填 Rewind 的跳转地址（空表时跳到这里）
        let halt_addr = self.instructions.len();
        self.instructions[rewind_addr].p2 = halt_addr as i32;

        // 回填 JumpIfFalse 的跳转地址
        // ...

        // 7. 关闭游标
        self.emit(Instruction::new(
            OpCode::Close,
            cursor,
            0,
            0,
        ));

        // 8. 结束
        self.emit(Instruction::new(
            OpCode::Halt,
            0,
            0,
            0,
        ));

        Ok(self.instructions.clone())
    }

    /// 生成表达式求值代码
    fn generate_expression(&mut self, expr: &Expression) -> Result<usize, VmError> {
        match expr {
            Expression::Integer(n) => {
                let reg = self.alloc_register();
                self.emit(Instruction::new(
                    OpCode::LoadInteger,
                    *n as i32,
                    reg as i32,
                    0,
                ));
                Ok(reg)
            }
            Expression::String(s) => {
                let pool_idx = self.add_string_constant(s.clone());
                let reg = self.alloc_register();
                self.emit(Instruction::new(
                    OpCode::LoadString,
                    pool_idx as i32,
                    reg as i32,
                    0,
                ));
                Ok(reg)
            }
            Expression::Null => {
                let reg = self.alloc_register();
                self.emit(Instruction::new(
                    OpCode::LoadNull,
                    reg as i32,
                    0,
                    0,
                ));
                Ok(reg)
            }
            Expression::Column(name) => {
                // 列引用在上下文中处理
                unimplemented!("Column reference in expression")
            }
            Expression::Binary { left, op, right } => {
                let left_reg = self.generate_expression(left)?;
                let right_reg = self.generate_expression(right)?;
                let result_reg = self.alloc_register();

                let opcode = match op {
                    BinaryOp::Equal => OpCode::Eq,
                    BinaryOp::NotEqual => OpCode::Ne,
                    BinaryOp::Less => OpCode::Lt,
                    BinaryOp::Greater => OpCode::Gt,
                    BinaryOp::LessEqual => OpCode::Le,
                    BinaryOp::GreaterEqual => OpCode::Ge,
                    BinaryOp::Add => OpCode::Add,
                    BinaryOp::Sub => OpCode::Sub,
                    BinaryOp::Mul => OpCode::Mul,
                    BinaryOp::Div => OpCode::Div,
                    BinaryOp::And => OpCode::And,
                    BinaryOp::Or => OpCode::Or,
                };

                self.emit(Instruction::new(
                    opcode,
                    left_reg as i32,
                    right_reg as i32,
                    result_reg as i32,
                ));

                Ok(result_reg)
            }
            _ => unimplemented!("Expression type not yet supported"),
        }
    }

    // 辅助方法...
    fn add_string_constant(&mut self, s: String) -> usize {
        let idx = self.string_pool.len();
        self.string_pool.push(s);
        idx
    }
}
```

#### 4. 虚拟机执行器

```rust
impl VirtualMachine {
    /// 执行字节码程序
    pub fn execute(&mut self, program: Vec<Instruction>) -> Result<ResultSet, VmError> {
        self.program = program;
        self.pc = 0;
        self.registers = vec![Value::Null; 256];  // 预分配寄存器
        self.result_set = Vec::new();

        loop {
            if self.pc >= self.program.len() {
                break;
            }

            let instr = &self.program[self.pc].clone();
            self.pc += 1;

            match instr.opcode {
                // 游标操作
                OpCode::OpenRead => {
                    let cursor_idx = instr.p1 as usize;
                    let root_page = instr.p2 as u32;

                    let cursor = Cursor {
                        cursor_type: CursorType::Read,
                        btree_iter: Some(self.storage.btree_scan_from(root_page)?),
                        current_record: None,
                        is_closed: false,
                    };

                    self.ensure_cursor_slot(cursor_idx);
                    self.cursors[cursor_idx] = Some(cursor);
                }

                OpCode::Close => {
                    let cursor_idx = instr.p1 as usize;
                    if let Some(ref mut cursor) = self.cursors[cursor_idx] {
                        cursor.is_closed = true;
                    }
                }

                // 导航操作
                OpCode::Rewind => {
                    let cursor_idx = instr.p1 as usize;
                    let jump_if_empty = instr.p2 as usize;

                    if let Some(ref mut cursor) = self.cursors[cursor_idx] {
                        if let Some(ref mut iter) = cursor.btree_iter {
                            if let Some((key, value)) = iter.next() {
                                cursor.current_record = Some(Record {
                                    row_id: key.as_row_id()?,
                                    values: vec![value],
                                });
                            } else {
                                // 空表，跳转
                                self.pc = jump_if_empty;
                            }
                        }
                    }
                }

                OpCode::Next => {
                    let cursor_idx = instr.p1 as usize;
                    let jump_addr = instr.p2 as usize;

                    if let Some(ref mut cursor) = self.cursors[cursor_idx] {
                        if let Some(ref mut iter) = cursor.btree_iter {
                            if let Some((key, value)) = iter.next() {
                                cursor.current_record = Some(Record {
                                    row_id: key.as_row_id()?,
                                    values: vec![value],
                                });
                                // 继续循环
                                self.pc = jump_addr;
                            }
                            // 否则结束循环（不跳转）
                        }
                    }
                }

                // 列操作
                OpCode::Column => {
                    let cursor_idx = instr.p1 as usize;
                    let col_idx = instr.p2 as usize;
                    let dest_reg = instr.p3 as usize;

                    if let Some(ref cursor) = self.cursors[cursor_idx] {
                        if let Some(ref record) = cursor.current_record {
                            if col_idx < record.values.len() {
                                self.registers[dest_reg] = record.values[col_idx].clone();
                            } else {
                                self.registers[dest_reg] = Value::Null;
                            }
                        }
                    }
                }

                // 寄存器操作
                OpCode::LoadNull => {
                    let reg = instr.p1 as usize;
                    self.registers[reg] = Value::Null;
                }

                OpCode::LoadInteger => {
                    let value = instr.p1 as i64;
                    let reg = instr.p2 as usize;
                    self.registers[reg] = Value::Integer(value);
                }

                // 比较操作
                OpCode::Eq | OpCode::Ne | OpCode::Lt | OpCode::Gt | OpCode::Le | OpCode::Ge => {
                    let left_reg = instr.p1 as usize;
                    let right_reg = instr.p2 as usize;
                    let result_reg = instr.p3 as usize;

                    let left = &self.registers[left_reg];
                    let right = &self.registers[right_reg];

                    let result = match instr.opcode {
                        OpCode::Eq => left == right,
                        OpCode::Ne => left != right,
                        OpCode::Lt => left < right,
                        OpCode::Gt => left > right,
                        OpCode::Le => left <= right,
                        OpCode::Ge => left >= right,
                        _ => unreachable!(),
                    };

                    self.registers[result_reg] = Value::Boolean(result);
                }

                // 控制流
                OpCode::Jump => {
                    self.pc = instr.p1 as usize;
                }

                OpCode::JumpIfFalse => {
                    let cond_reg = instr.p1 as usize;
                    let jump_addr = instr.p2 as usize;

                    if let Value::Boolean(false) | Value::Null = self.registers[cond_reg] {
                        self.pc = jump_addr;
                    }
                }

                // 结果操作
                OpCode::ResultRow => {
                    let start_reg = instr.p1 as usize;
                    let col_count = instr.p2 as usize;

                    let mut row = Row { values: Vec::new() };
                    for i in 0..col_count {
                        row.values.push(self.registers[start_reg + i].clone());
                    }
                    self.result_set.push(row);
                }

                // 结束
                OpCode::Halt => {
                    break;
                }

                _ => {
                    return Err(VmError::UnimplementedOpcode(instr.opcode));
                }
            }
        }

        Ok(ResultSet {
            rows: self.result_set.clone(),
            columns: Vec::new(),  // TODO: 设置列名
        })
    }

    fn ensure_cursor_slot(&mut self, idx: usize) {
        while self.cursors.len() <= idx {
            self.cursors.push(None);
        }
    }
}
```

---

## 接口定义

### 对外接口清单

| 接口编号 | 接口名称 | 方法 | 对应PRD |
|---------|---------|------|---------|
| API-009 | VM::execute_sql | fn execute_sql(sql: &str) -> Result<ResultSet> | FR-006~013 |
| API-010 | VM::execute_program | fn execute_program(program: Vec<Instruction>) -> Result<ResultSet> | FR-006 |

### 接口详细定义

#### API-009: VM::execute_sql

**对应PRD**:
- 用户故事: US-002~006
- 功能需求: FR-006~013

**接口定义**:
```rust
/// 执行 SQL 语句
///
/// # Arguments
/// * `sql` - SQL 字符串
///
/// # Returns
/// * `Ok(ResultSet)` - 执行成功，返回结果集
/// * `Err(VmError)` - 执行失败
pub fn execute_sql(&mut self, sql: &str) -> Result<ResultSet, VmError>
```

**执行流程**:
1. 调用 Parser 解析 SQL 为 AST
2. 调用 Optimizer 优化查询计划
3. 调用 CodeGenerator 生成字节码
4. 调用 execute_program 执行字节码
5. 返回结果集

#### API-010: VM::execute_program

**对应PRD**:
- 功能需求: FR-006

**接口定义**:
```rust
/// 直接执行字节码程序
///
/// # Arguments
/// * `program` - 字节码指令序列
///
/// # Returns
/// * `Ok(ResultSet)` - 执行成功
/// * `Err(VmError)` - 执行失败
pub fn execute_program(&mut self, program: Vec<Instruction>) -> Result<ResultSet, VmError>
```

---

## 数据结构

### 核心实体

已在核心设计部分定义，见 [字节码指令集](#1-字节码指令集) 和 [虚拟机结构](#2-虚拟机结构)。

---

## 状态机设计

### STATE-002: SQL 执行流程

**对应PRD**: Flow-001

**状态定义**:
| 状态 | 编码 | PRD描述 | 说明 |
|-----|------|---------|------|
| Idle | 0 | 空闲状态 | 初始状态 |
| Parsing | 1 | 解析 SQL | 调用 Parser |
| Compiling | 2 | 生成执行计划 | 代码生成 |
| Executing | 3 | 执行字节码 | VM 执行 |
| Returning | 4 | 返回结果 | 结果集准备 |
| Error | 5 | 错误状态 | 执行出错 |

**状态转换**:
| 编号 | 当前状态 | 触发事件 | 下一状态 | 条件 | 对应PRD |
|-----|---------|---------|---------|------|---------|
| T001 | Idle | 接收 SQL | Parsing | SQL 非空 | Flow-001 |
| T002 | Parsing | 解析成功 | Compiling | 语法正确 | Flow-001 |
| T003 | Parsing | 解析失败 | Error | 语法错误 | Flow-001 |
| T004 | Compiling | 编译成功 | Executing | 计划有效 | Flow-001 |
| T005 | Compiling | 编译失败 | Error | 语义错误 | Flow-001 |
| T006 | Executing | 执行完成 | Returning | 正常完成 | Flow-001 |
| T007 | Executing | 执行错误 | Error | 运行时错误 | Flow-001 |
| T008 | Returning | 结果返回 | Idle | - | Flow-001 |
| T009 | Error | 错误处理 | Idle | - | Flow-001 |

---

## 边界条件

### BOUND-001: 寄存器溢出

**对应PRD**: FR-006

**触发条件**:
- 使用的寄存器编号超过预分配数量

**处理方式**:
- 动态扩展寄存器数组

### BOUND-002: 游标溢出

**对应PRD**: FR-006

**触发条件**:
- 使用的游标编号超过预分配数量

**处理方式**:
- 动态扩展游标数组

### BOUND-003: 除零错误

**对应PRD**: FR-006

**触发条件**:
- 执行 Div 指令时除数为 0

**处理方式**:
- 返回 DivisionByZero 错误

### BOUND-004: 无效游标操作

**对应PRD**: FR-006

**触发条件**:
- 对已关闭的游标执行操作

**处理方式**:
- 返回 InvalidCursor 错误

---

## 非功能需求

### 性能要求

| 指标 | 要求 | 对应PRD |
|-----|------|---------|
| 简单查询 | < 5ms | FR-006 |
| 字节码执行 | 每指令 < 1μs | FR-006 |
| 结果集大小 | 支持百万级行 | FR-007 |

### 调试支持

| 需求 | 描述 | 实现方案 |
|-----|------|---------|
| 执行跟踪 | 打印每条执行的指令 | VM 配置选项 |
| 执行计划 | 显示生成的字节码 | CodeGenerator 输出 |
| 寄存器状态 | 打印寄存器值 | 调试模式 |

---

## 实现文件

| 文件路径 | 职责 |
|---------|------|
| src/vm/mod.rs | 模块入口，VM 结构 |
| src/vm/opcode.rs | 操作码定义 |
| src/vm/instruction.rs | 指令结构 |
| src/vm/codegen.rs | 代码生成器 |
| src/vm/executor.rs | 虚拟机执行器 |
| src/vm/cursor.rs | 游标实现 |
| src/vm/result.rs | 结果集定义 |
| src/vm/optimizer.rs | 查询优化器 |

---

## 验收标准

| 标准 | 要求 | 验证方法 | 对应PRD |
|-----|------|---------|---------|
| 标准1 | SELECT 正确执行 | 单元测试：验证结果集 | FR-007 |
| 标准2 | INSERT 正确执行 | 单元测试：验证插入数据 | FR-008 |
| 标准3 | UPDATE 正确执行 | 单元测试：验证更新数据 | FR-009 |
| 标准4 | DELETE 正确执行 | 单元测试：验证删除数据 | FR-010 |
| 标准5 | WHERE 条件正确 | 单元测试：验证过滤结果 | FR-011 |
| 标准6 | CREATE TABLE 正确 | 单元测试：验证表创建 | FR-012 |
| 标准7 | DROP TABLE 正确 | 单元测试：验证表删除 | FR-013 |
| 标准8 | 字节码正确生成 | 单元测试：验证指令序列 | FR-006 |

---

## 覆盖映射

### PRD需求覆盖情况

| PRD类型 | PRD编号 | 架构元素 | 覆盖状态 |
|---------|---------|---------|---------|
| 功能需求 | FR-006 | VirtualMachine, Instruction | ✅ |
| 功能需求 | FR-007~010 | CodeGenerator::generate_select/insert/update/delete | ✅ |
| 功能需求 | FR-011 | generate_expression, JumpIfFalse | ✅ |
| 功能需求 | FR-012~013 | CreateTable/DropTable 指令 | ✅ |
| 功能需求 | FR-018 | Optimizer | ✅ |
| 用户故事 | US-002~006 | execute_sql | ✅ |
| 业务流程 | Flow-001 | STATE-002 | ✅ |

---

## 变更历史

| 版本 | 日期 | 变更内容 | 作者 |
|-----|------|---------|------|
| 1.0 | 2026-03-14 | 初始版本 | Claude |
