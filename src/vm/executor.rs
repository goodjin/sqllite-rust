use crate::storage::{Record, Value};
use crate::vm::{Cursor, Instruction, OpCode, Result, VMError};

pub struct Executor {
    pub stack: Vec<Value>,
    pub registers: Vec<Value>,
    pub pc: usize,
    pub cursor: Option<Cursor>,
}

impl Executor {
    pub fn new() -> Self {
        Self {
            stack: Vec::new(),
            registers: vec![Value::Null; 16],
            pc: 0,
            cursor: None,
        }
    }

    pub fn execute(&mut self, instructions: &[Instruction]) -> Result<Vec<Record>> {
        let mut results = Vec::new();
        self.pc = 0;

        while self.pc < instructions.len() {
            let instruction = &instructions[self.pc];
            self.pc += 1;

            match instruction.opcode {
                OpCode::Halt => break,

                OpCode::Push => {
                    self.stack.push(Value::Null);
                }

                OpCode::Pop => {
                    self.stack.pop().ok_or(VMError::StackUnderflow)?;
                }

                OpCode::Dup => {
                    let value = self.stack.last()
                        .ok_or(VMError::StackUnderflow)?
                        .clone();
                    self.stack.push(value);
                }

                OpCode::Load => {
                    let idx = instruction.p1 as usize;
                    if idx >= self.registers.len() {
                        return Err(VMError::RegisterOutOfBounds(instruction.p1 as u8));
                    }
                    self.stack.push(self.registers[idx].clone());
                }

                OpCode::Store => {
                    let idx = instruction.p1 as usize;
                    if idx >= self.registers.len() {
                        return Err(VMError::RegisterOutOfBounds(instruction.p1 as u8));
                    }
                    self.registers[idx] = self.stack.pop()
                        .ok_or(VMError::StackUnderflow)?;
                }

                OpCode::Add => self.execute_binary_op(|a, b| match (a, b) {
                    (Value::Integer(x), Value::Integer(y)) => Ok(Value::Integer(x + y)),
                    _ => Err(VMError::ExecutionError("Invalid operands for Add".to_string())),
                })?,

                OpCode::Sub => self.execute_binary_op(|a, b| match (a, b) {
                    (Value::Integer(x), Value::Integer(y)) => Ok(Value::Integer(x - y)),
                    _ => Err(VMError::ExecutionError("Invalid operands for Sub".to_string())),
                })?,

                OpCode::Mul => self.execute_binary_op(|a, b| match (a, b) {
                    (Value::Integer(x), Value::Integer(y)) => Ok(Value::Integer(x * y)),
                    _ => Err(VMError::ExecutionError("Invalid operands for Mul".to_string())),
                })?,

                OpCode::Div => self.execute_binary_op(|a, b| match (a, b) {
                    (Value::Integer(x), Value::Integer(y)) => {
                        if y == 0 {
                            Err(VMError::ExecutionError("Division by zero".to_string()))
                        } else {
                            Ok(Value::Integer(x / y))
                        }
                    }
                    _ => Err(VMError::ExecutionError("Invalid operands for Div".to_string())),
                })?,

                OpCode::Eq => self.execute_comparison(|a, b| a == b)?,
                OpCode::Ne => self.execute_comparison(|a, b| a != b)?,

                OpCode::OpenRead | OpCode::OpenWrite => {
                    if let Some(ref table_name) = instruction.p4 {
                        self.cursor = Some(Cursor::new(table_name.clone()));
                        if let Some(ref mut cursor) = self.cursor {
                            cursor.open();
                        }
                    }
                }

                OpCode::Close => {
                    if let Some(ref mut cursor) = self.cursor {
                        cursor.close();
                    }
                    self.cursor = None;
                }

                OpCode::Next => {
                    if let Some(ref mut cursor) = self.cursor {
                        cursor.next();
                    }
                }

                OpCode::ResultRow => {
                    let values = self.stack.clone();
                    results.push(Record::new(values));
                }

                OpCode::Jump => {
                    self.pc = instruction.p2 as usize;
                }

                OpCode::JumpIf => {
                    let condition = self.stack.pop()
                        .ok_or(VMError::StackUnderflow)?;
                    if matches!(condition, Value::Integer(1)) {
                        self.pc = instruction.p2 as usize;
                    }
                }

                OpCode::JumpIfNot => {
                    let condition = self.stack.pop()
                        .ok_or(VMError::StackUnderflow)?;
                    if !matches!(condition, Value::Integer(1)) {
                        self.pc = instruction.p2 as usize;
                    }
                }

                _ => {
                    return Err(VMError::InvalidOpcode(instruction.opcode as u8));
                }
            }
        }

        Ok(results)
    }

    fn execute_binary_op<F>(&mut self,
        op: F,
    ) -> Result<()>
    where
        F: FnOnce(Value, Value) -> Result<Value>,
    {
        let b = self.stack.pop().ok_or(VMError::StackUnderflow)?;
        let a = self.stack.pop().ok_or(VMError::StackUnderflow)?;
        let result = op(a, b)?;
        self.stack.push(result);
        Ok(())
    }

    fn execute_comparison<F>(&mut self,
        op: F,
    ) -> Result<()>
    where
        F: FnOnce(&Value, &Value) -> bool,
    {
        let b = self.stack.pop().ok_or(VMError::StackUnderflow)?;
        let a = self.stack.pop().ok_or(VMError::StackUnderflow)?;
        let result = if op(&a, &b) {
            Value::Integer(1)
        } else {
            Value::Integer(0)
        };
        self.stack.push(result);
        Ok(())
    }
}

impl Default for Executor {
    fn default() -> Self {
        Self::new()
    }
}
