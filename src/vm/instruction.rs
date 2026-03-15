use crate::vm::OpCode;

#[derive(Debug, Clone)]
pub struct Instruction {
    pub opcode: OpCode,
    pub p1: i32,
    pub p2: i32,
    pub p3: i32,
    pub p4: Option<String>,
}

impl Instruction {
    pub fn new(opcode: OpCode) -> Self {
        Self {
            opcode,
            p1: 0,
            p2: 0,
            p3: 0,
            p4: None,
        }
    }

    pub fn with_p1(mut self, p1: i32) -> Self {
        self.p1 = p1;
        self
    }

    pub fn with_p2(mut self, p2: i32) -> Self {
        self.p2 = p2;
        self
    }

    pub fn with_p3(mut self, p3: i32) -> Self {
        self.p3 = p3;
        self
    }

    pub fn with_p4(mut self, p4: String) -> Self {
        self.p4 = Some(p4);
        self
    }
}
