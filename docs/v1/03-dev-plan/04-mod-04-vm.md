# 开发计划 - MOD-04: Virtual Machine (虚拟机执行引擎)

## 文档信息
- **模块编号**: MOD-04
- **模块名称**: Virtual Machine (虚拟机执行引擎)
- **所属层次**: L2-执行层
- **对应架构**: docs/v1/02-architecture/03-mod-04-vm.md
- **优先级**: P0 (阶段 2)
- **预估工时**: 4天

---

## 1. 模块概述

### 1.1 模块职责
- 将 AST 转换为字节码指令
- 解释执行字节码
- 管理游标和寄存器
- 返回查询结果

### 1.2 对应PRD
| PRD编号 | 功能 | 用户故事 |
|---------|-----|---------|
| FR-006 | 虚拟机执行引擎 | - |
| FR-007~010 | CRUD 操作 | US-002~006 |
| FR-018 | 查询优化器 | - |

### 1.3 架构定位
```
AST → Code Generator → Bytecode → VM → ResultSet
```

---

## 2. 技术设计

### 2.1 目录结构
```
src/vm/
├── mod.rs           # 模块入口，VM 结构
├── opcode.rs        # 操作码定义
├── instruction.rs   # 指令结构
├── codegen.rs       # 代码生成器
├── executor.rs      # 虚拟机执行器
├── cursor.rs        # 游标实现
└── result.rs        # 结果集定义
```

### 2.2 依赖关系
| 依赖模块 | 依赖方式 | 用途 |
|---------|---------|------|
| MOD-03 Parser | use crate::sql | SQL 解析 |
| MOD-01 Storage | use crate::storage | B+ Tree 操作 |
| MOD-02 Pager | use crate::pager | 页面管理 |

---

## 3. 接口清单

| 任务编号 | 接口编号 | 接口名称 | 复杂度 |
|---------|---------|---------|-------|
| T-05 | API-008 | VM::execute_sql | 高 |
| T-06 | API-010 | VM::execute_program | 中 |

---

## 4. 开发任务拆分

### 任务清单

| 任务编号 | 任务名称 | 涉及文件 | 代码行数 | 依赖 |
|---------|---------|---------|---------|------|
| T-01 | Opcode 定义 | 1 | ~60 | - |
| T-02 | Instruction 定义 | 1 | ~40 | T-01 |
| T-03 | 结果集定义 | 1 | ~50 | - |
| T-04 | 代码生成器（SELECT） | 2 | ~200 | T-02 |
| T-05 | 代码生成器（INSERT/UPDATE/DELETE） | 1 | ~150 | T-04 |
| T-06 | 虚拟机执行器 | 3 | ~250 | T-02, T-03 |
| T-07 | 游标实现 | 1 | ~80 | T-06 |
| T-08 | 单元测试 | 6 | ~250 | T-01~07 |

---

## 5. 详细任务定义

### T-01: Opcode 定义

**任务概述**: 定义虚拟机操作码

**输出**:
- `src/vm/opcode.rs`

**实现要求**:
```rust
#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(u8)]
pub enum OpCode {
    // 游标操作
    OpenRead = 0x01,
    OpenWrite = 0x02,
    Close = 0x03,

    // 导航操作
    Rewind = 0x10,
    Next = 0x11,
    Seek = 0x12,

    // 列操作
    Column = 0x20,
    RowId = 0x21,

    // 数据操作
    Insert = 0x30,
    Delete = 0x31,
    Update = 0x32,

    // 寄存器操作
    LoadNull = 0x40,
    LoadInteger = 0x41,
    LoadString = 0x42,
    Move = 0x43,

    // 比较操作
    Eq = 0x50,
    Ne = 0x51,
    Lt = 0x52,
    Gt = 0x53,
    Le = 0x54,
    Ge = 0x55,

    // 逻辑操作
    And = 0x60,
    Or = 0x61,
    Not = 0x62,

    // 控制流
    Jump = 0x70,
    JumpIfTrue = 0x71,
    JumpIfFalse = 0x72,

    // 结果操作
    ResultRow = 0x80,
    Halt = 0x81,
}
```

**预估工时**: 1小时

---

### T-02: Instruction 定义

**任务概述**: 定义指令结构

**输出**:
- `src/vm/instruction.rs`

**实现要求**:
```rust
use crate::vm::OpCode;

#[derive(Debug, Clone)]
pub struct Instruction {
    pub opcode: OpCode,
    pub p1: i32,
    pub p2: i32,
    pub p3: i32,
    pub comment: Option<String>,
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

**预估工时**: 0.5小时

**依赖**: T-01

---

### T-03: 结果集定义

**任务概述**: 定义查询结果集

**输出**:
- `src/vm/result.rs`

**实现要求**:
```rust
use crate::storage::Value;

#[derive(Debug, Clone)]
pub struct Row {
    pub values: Vec<Value>,
}

#[derive(Debug)]
pub struct ResultSet {
    pub rows: Vec<Row>,
    pub columns: Vec<String>,
    pub affected_rows: usize,
}

impl ResultSet {
    pub fn new() -> Self {
        Self {
            rows: Vec::new(),
            columns: Vec::new(),
            affected_rows: 0,
        }
    }

    pub fn empty() -> Self {
        Self::new()
    }
}
```

**预估工时**: 0.5小时

---

### T-04: 代码生成器（SELECT）

**任务概述**: 实现 SELECT 语句的字节码生成

**输出**:
- `src/vm/codegen.rs`（SELECT 部分）

**实现要求**:
```rust
use crate::sql::ast::*;
use crate::vm::{Instruction, OpCode};

pub struct CodeGenerator {
    instructions: Vec<Instruction>,
    next_reg: usize,
    string_pool: Vec<String>,
}

impl CodeGenerator {
    pub fn new() -> Self {
        Self {
            instructions: Vec::new(),
            next_reg: 1,
            string_pool: Vec::new(),
        }
    }

    pub fn generate(&mut self, stmt: &Statement) -> Result<Vec<Instruction>, CodegenError> {
        match stmt {
            Statement::Select(s) => self.generate_select(s),
            // ...
        }
    }

    fn generate_select(&mut self, stmt: &SelectStmt) -> Result<Vec<Instruction>, CodegenError> {
        // 1. OpenRead 打开表
        // 2. Rewind 移动到第一条
        // 3. WHERE 条件处理
        // 4. Column 读取列值
        // 5. ResultRow 返回结果
        // 6. Next 循环
        // 7. Close 关闭游标
        // 8. Halt 结束
    }

    fn generate_expression(&mut self, expr: &Expression) -> Result<usize, CodegenError> {
        // 生成表达式求值代码
    }

    fn alloc_register(&mut self) -> usize {
        let reg = self.next_reg;
        self.next_reg += 1;
        reg
    }

    fn emit(&mut self, instr: Instruction) -> usize {
        let addr = self.instructions.len();
        self.instructions.push(instr);
        addr
    }
}
```

**验收标准**:
- [ ] 正确生成 SELECT 字节码
- [ ] 正确处理 WHERE 条件
- [ ] 正确处理列选择

**测试要求**:
- 测试用例: 4个（简单 SELECT、带 WHERE、多列、*）

**预估工时**: 6小时

**依赖**: T-02

---

### T-05: 代码生成器（INSERT/UPDATE/DELETE）

**任务概述**: 实现 DML 语句的字节码生成

**输出**:
- `src/vm/codegen.rs`（DML 部分）

**实现要求**:
```rust
impl CodeGenerator {
    fn generate_insert(&mut self, stmt: &InsertStmt) -> Result<Vec<Instruction>, CodegenError> {
        // 1. OpenWrite 打开表
        // 2. 计算插入值
        // 3. Insert 插入记录
        // 4. Close 关闭游标
        // 5. Halt 结束
    }

    fn generate_update(&mut self, stmt: &UpdateStmt) -> Result<Vec<Instruction>, CodegenError> {
        // 1. OpenWrite 打开表
        // 2. Rewind 移动到第一条
        // 3. WHERE 条件处理
        // 4. 计算新值
        // 5. Update 更新记录
        // 6. Next 循环
        // 7. Close 关闭游标
        // 8. Halt 结束
    }

    fn generate_delete(&mut self, stmt: &DeleteStmt) -> Result<Vec<Instruction>, CodegenError> {
        // 1. OpenWrite 打开表
        // 2. Rewind 移动到第一条
        // 3. WHERE 条件处理
        // 4. Delete 删除记录
        // 5. Next 循环
        // 6. Close 关闭游标
        // 7. Halt 结束
    }
}
```

**验收标准**:
- [ ] 正确生成 INSERT 字节码
- [ ] 正确生成 UPDATE 字节码
- [ ] 正确生成 DELETE 字节码

**测试要求**:
- 测试用例: 6个（INSERT、UPDATE、DELETE 各 2 个）

**预估工时**: 4小时

**依赖**: T-04

---

### T-06: 虚拟机执行器

**任务概述**: 实现字节码解释执行

**输出**:
- `src/vm/executor.rs`
- `src/vm/mod.rs`

**实现要求**:
```rust
use crate::vm::{Instruction, OpCode, ResultSet, Row};
use crate::storage::{Storage, Value};

pub struct VirtualMachine {
    program: Vec<Instruction>,
    pc: usize,
    registers: Vec<Value>,
    cursors: Vec<Option<Cursor>>,
    result_set: ResultSet,
    storage: Arc<Mutex<Storage>>,
}

impl VirtualMachine {
    pub fn new(storage: Arc<Mutex<Storage>>) -> Self {
        Self {
            program: Vec::new(),
            pc: 0,
            registers: vec![Value::Null; 256],
            cursors: Vec::new(),
            result_set: ResultSet::new(),
            storage,
        }
    }

    pub fn execute(&mut self, program: Vec<Instruction>) -> Result<ResultSet, VmError> {
        self.program = program;
        self.pc = 0;
        self.result_set = ResultSet::new();

        loop {
            if self.pc >= self.program.len() {
                break;
            }

            let instr = self.program[self.pc].clone();
            self.pc += 1;

            match instr.opcode {
                OpCode::OpenRead => self.op_open_read(instr.p1 as usize, instr.p2 as u32)?,
                OpCode::OpenWrite => self.op_open_write(instr.p1 as usize, instr.p2 as u32)?,
                OpCode::Close => self.op_close(instr.p1 as usize)?,
                OpCode::Rewind => self.op_rewind(instr.p1 as usize, instr.p2 as usize)?,
                OpCode::Next => self.op_next(instr.p1 as usize, instr.p2 as usize)?,
                OpCode::Column => self.op_column(instr.p1 as usize, instr.p2 as usize, instr.p3 as usize)?,
                OpCode::Insert => self.op_insert(instr.p1 as usize, instr.p2 as usize, instr.p3 as usize)?,
                OpCode::Delete => self.op_delete(instr.p1 as usize)?,
                OpCode::LoadNull => self.op_load_null(instr.p1 as usize),
                OpCode::LoadInteger => self.op_load_integer(instr.p1 as i64, instr.p2 as usize),
                OpCode::Eq => self.op_eq(instr.p1 as usize, instr.p2 as usize, instr.p3 as usize)?,
                OpCode::Jump => self.op_jump(instr.p1 as usize),
                OpCode::JumpIfFalse => self.op_jump_if_false(instr.p1 as usize, instr.p2 as usize)?,
                OpCode::ResultRow => self.op_result_row(instr.p1 as usize, instr.p2 as usize),
                OpCode::Halt => break,
                _ => return Err(VmError::UnimplementedOpcode(instr.opcode)),
            }
        }

        Ok(self.result_set.clone())
    }

    // 操作码实现...
    fn op_open_read(&mut self, cursor_idx: usize, root_page: u32) -> Result<(), VmError> {
        // 实现
    }

    fn op_rewind(&mut self, cursor_idx: usize, jump_addr: usize) -> Result<(), VmError> {
        // 实现
    }

    // ... 其他操作码
}
```

**验收标准**:
- [ ] 正确执行所有操作码
- [ ] 正确处理寄存器
- [ ] 正确处理游标

**测试要求**:
- 测试用例: 8个（各操作码测试）

**预估工时**: 8小时

**依赖**: T-02, T-03

---

### T-07: 游标实现

**任务概述**: 实现 B+ Tree 游标

**输出**:
- `src/vm/cursor.rs`

**实现要求**:
```rust
use crate::storage::{Storage, Record};

pub struct Cursor {
    pub cursor_type: CursorType,
    pub current_record: Option<Record>,
    pub is_closed: bool,
    // 内部状态...
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CursorType {
    Read,
    Write,
}

impl Cursor {
    pub fn new(cursor_type: CursorType) -> Self {
        Self {
            cursor_type,
            current_record: None,
            is_closed: false,
        }
    }

    pub fn rewind(&mut self, storage: &mut Storage) -> Result<bool, CursorError> {
        // 移动到第一条记录
    }

    pub fn next(&mut self, storage: &mut Storage) -> Result<bool, CursorError> {
        // 移动到下一条记录
    }

    pub fn close(&mut self) {
        self.is_closed = true;
    }
}
```

**验收标准**:
- [ ] 游标遍历正确
- [ ] 支持读写模式

**测试要求**:
- 测试用例: 3个（遍历、关闭、读写）

**预估工时**: 2小时

**依赖**: T-06

---

### T-08: 单元测试

**任务概述**: 编写完整的单元测试

**输出**:
- 各文件中的 `#[cfg(test)]` 模块

**测试清单**:
| 测试目标 | 测试文件 | 用例数 |
|---------|---------|-------|
| CodeGenerator | codegen.rs | 10 |
| VM Executor | executor.rs | 8 |
| Cursor | cursor.rs | 3 |

**预估工时**: 4小时

**依赖**: T-01~07

---

## 6. 验收清单

- [ ] 正确生成字节码
- [ ] 正确执行字节码
- [ ] 正确处理 SELECT/INSERT/UPDATE/DELETE
- [ ] 测试覆盖率 ≥ 80%

---

## 7. 覆盖映射

| 架构元素 | 架构编号 | 任务 | 覆盖状态 |
|---------|---------|------|---------|
| OpCode | - | T-01 | ✅ |
| Instruction | - | T-02 | ✅ |
| ResultSet | - | T-03 | ✅ |
| CodeGenerator | - | T-04, T-05 | ✅ |
| VM Executor | API-008, API-010 | T-06 | ✅ |
| Cursor | - | T-07 | ✅ |

---

## 变更历史

| 版本 | 日期 | 变更内容 | 作者 |
|-----|------|---------|------|
| 1.0 | 2026-03-14 | 初始版本 | Claude |
