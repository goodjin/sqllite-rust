# 状态机规约文档

## 文档信息
- **项目名称**: sqllite-rust
- **版本**: v1.0
- **对应PRD**: docs/v1/01-prd.md
- **更新日期**: 2026-03-14

---

## 状态机清单

| 编号 | 状态机名称 | 实体 | 对应PRD流程 | 状态数 | 转换数 |
|-----|-----------|------|------------|-------|-------|
| STATE-001 | 数据库连接状态 | Database | - | 2 | 2 |
| STATE-002 | SQL 执行流程 | VM | Flow-001 | 6 | 10 |
| STATE-003 | 事务状态 | TransactionManager | Flow-002 | 4 | 6 |

---

## 状态机详细定义

### STATE-001: 数据库连接状态

**对应PRD**: -

**所属实体**: Database

**状态定义**:
| 状态 | 编码 | 描述 | 说明 |
|-----|------|------|------|
| Closed | 0 | 连接已关闭 | 初始状态 |
| Open | 1 | 连接已打开 | 可操作状态 |

**状态转换**:
| 编号 | 当前状态 | 触发事件 | 下一状态 | 条件 |
|-----|---------|---------|---------|------|
| T001 | Closed | open() | Open | 文件打开成功 |
| T002 | Open | close() | Closed | - |

**状态转换图**:
```
┌─────────┐    open()    ┌─────────┐
│ Closed  │ ───────────▶ │  Open   │
│         │ ◀─────────── │         │
└─────────┘    close()   └─────────┘
```

---

### STATE-002: SQL 执行流程

**对应PRD**: Flow-001

**所属实体**: VirtualMachine

**状态定义**:
| 状态 | 编码 | 描述 | 说明 |
|-----|------|------|------|
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

**状态转换图**:
```
                    接收 SQL
    ┌───────────────────────────────────────────┐
    │                                           │
    ▼                                           │
┌─────────┐    解析成功    ┌───────────┐        │
│ Parsing │ ─────────────▶ │ Compiling │        │
│         │ ◀───────────── │           │        │
└────┬────┘   解析失败     └─────┬─────┘        │
     │                          │              │
     │ 解析失败                  │ 编译成功      │
     ▼                          ▼              │
┌─────────┐              ┌───────────┐         │
│  Error  │              │ Executing │         │
│         │              │           │         │
└────┬────┘              └─────┬─────┘         │
     │                         │               │
     │ 执行错误                 │ 执行完成       │
     │                         ▼               │
     │                   ┌───────────┐         │
     │                   │ Returning │ ────────┘
     │                   │           │ 结果返回
     │                   └───────────┘
     │
     └───────────────────────────────▶ Idle
              错误处理完成
```

---

### STATE-003: 事务状态

**对应PRD**: Flow-002

**所属实体**: TransactionManager

**状态定义**:
| 状态 | 编码 | 描述 | 说明 |
|-----|------|------|------|
| AutoCommit | 0 | 自动提交模式 | 初始状态 |
| Active | 1 | 事务活跃 | BEGIN 后状态 |
| Committing | 2 | 提交中 | COMMIT 处理中 |
| RollingBack | 3 | 回滚中 | ROLLBACK 处理中 |

**状态转换**:
| 编号 | 当前状态 | 触发事件 | 下一状态 | 条件 | 对应PRD |
|-----|---------|---------|---------|------|---------|
| T001 | AutoCommit | BEGIN | Active | 无活跃事务 | Flow-002 |
| T002 | Active | COMMIT | Committing | - | Flow-002 |
| T003 | Committing | 完成 | AutoCommit | 刷盘成功 | Flow-002 |
| T004 | Active | ROLLBACK | RollingBack | - | Flow-002 |
| T005 | RollingBack | 完成 | AutoCommit | 丢弃脏页 | Flow-002 |
| T006 | Active | 错误 | RollingBack | 自动回滚 | Flow-002 |

**状态转换图**:
```
                    BEGIN
    ┌───────────────┐
    │               │
    ▼               │
┌────────────┐      │
│ AutoCommit │◄─────┘
└─────┬──────┘    COMMIT/ROLLBACK 完成
      │
      │ BEGIN
      ▼
┌──────────┐    COMMIT    ┌────────────┐    完成    ┌────────────┐
│  Active  │ ───────────▶ │ Committing │ ─────────▶ │ AutoCommit │
│          │              └────────────┘            └────────────┘
│          │
│          │ ROLLBACK
│          ▼
│    ┌─────────────┐    完成
│    │ RollingBack │ ─────────▶ AutoCommit
│    └─────────────┘
│
│ 错误
└──────────────────────────────▶ RollingBack
```

**状态约束**:
- 从 AutoCommit 只能转换到 Active（通过 BEGIN）
- 从 Active 可以转换到 Committing（COMMIT）、RollingBack（ROLLBACK 或错误）
- Committing 和 RollingBack 都是中间状态，完成后必须回到 AutoCommit
- 不允许嵌套事务（Active 状态下不能再 BEGIN）

---

## 变更历史

| 版本 | 日期 | 变更内容 | 作者 |
|-----|------|---------|------|
| 1.0 | 2026-03-14 | 初始版本 | Claude |
