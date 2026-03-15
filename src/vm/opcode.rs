#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(u8)]
pub enum OpCode {
    // Stack operations
    Push = 0x01,
    Pop = 0x02,
    Dup = 0x03,

    // Register operations
    Load = 0x10,
    Store = 0x11,

    // Arithmetic
    Add = 0x20,
    Sub = 0x21,
    Mul = 0x22,
    Div = 0x23,

    // Comparison
    Eq = 0x30,
    Ne = 0x31,
    Lt = 0x32,
    Gt = 0x33,
    Le = 0x34,
    Ge = 0x35,

    // Control flow
    Jump = 0x40,
    JumpIf = 0x41,
    JumpIfNot = 0x42,
    Halt = 0x43,

    // Storage operations
    OpenRead = 0x50,
    OpenWrite = 0x51,
    Close = 0x52,
    Next = 0x53,
    Prev = 0x54,
    Seek = 0x55,
    Insert = 0x56,
    Delete = 0x57,
    ResultRow = 0x58,

    // Transaction
    Begin = 0x60,
    Commit = 0x61,
    Rollback = 0x62,
}

impl OpCode {
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            0x01 => Some(OpCode::Push),
            0x02 => Some(OpCode::Pop),
            0x03 => Some(OpCode::Dup),
            0x10 => Some(OpCode::Load),
            0x11 => Some(OpCode::Store),
            0x20 => Some(OpCode::Add),
            0x21 => Some(OpCode::Sub),
            0x22 => Some(OpCode::Mul),
            0x23 => Some(OpCode::Div),
            0x30 => Some(OpCode::Eq),
            0x31 => Some(OpCode::Ne),
            0x32 => Some(OpCode::Lt),
            0x33 => Some(OpCode::Gt),
            0x34 => Some(OpCode::Le),
            0x35 => Some(OpCode::Ge),
            0x40 => Some(OpCode::Jump),
            0x41 => Some(OpCode::JumpIf),
            0x42 => Some(OpCode::JumpIfNot),
            0x43 => Some(OpCode::Halt),
            0x50 => Some(OpCode::OpenRead),
            0x51 => Some(OpCode::OpenWrite),
            0x52 => Some(OpCode::Close),
            0x53 => Some(OpCode::Next),
            0x54 => Some(OpCode::Prev),
            0x55 => Some(OpCode::Seek),
            0x56 => Some(OpCode::Insert),
            0x57 => Some(OpCode::Delete),
            0x58 => Some(OpCode::ResultRow),
            0x60 => Some(OpCode::Begin),
            0x61 => Some(OpCode::Commit),
            0x62 => Some(OpCode::Rollback),
            _ => None,
        }
    }
}
