use crate::sql::ast::*;
use crate::vm::{Instruction, OpCode};

pub struct CodeGen;

impl CodeGen {
    pub fn new() -> Self {
        Self
    }

    pub fn generate(&mut self, stmt: &Statement) -> Vec<Instruction> {
        match stmt {
            Statement::Select(select) => self.generate_select(select),
            Statement::Insert(insert) => self.generate_insert(insert),
            Statement::Update(update) => self.generate_update(update),
            Statement::Delete(delete) => self.generate_delete(delete),
            Statement::CreateTable(create) => self.generate_create_table(create),
            Statement::DropTable(drop) => self.generate_drop_table(drop),
            _ => vec![Instruction::new(OpCode::Halt)],
        }
    }

    fn generate_select(&mut self, select: &SelectStmt) -> Vec<Instruction> {
        let mut instructions = vec![];

        // Open cursor for reading
        instructions.push(
            Instruction::new(OpCode::OpenRead)
                .with_p4(select.from.clone())
        );

        // Loop through rows
        let loop_start = instructions.len();

        instructions.push(Instruction::new(OpCode::Next));
        instructions.push(
            Instruction::new(OpCode::JumpIfNot)
                .with_p2(-1) // Will be patched
        );

        // Output row
        instructions.push(
            Instruction::new(OpCode::ResultRow)
        );

        // Jump back to loop start
        instructions.push(
            Instruction::new(OpCode::Jump)
                .with_p2(loop_start as i32)
        );

        // Close cursor
        instructions.push(Instruction::new(OpCode::Close));
        instructions.push(Instruction::new(OpCode::Halt));

        instructions
    }

    fn generate_insert(&mut self, insert: &InsertStmt) -> Vec<Instruction> {
        let mut instructions = vec![];

        // Open cursor for writing
        instructions.push(
            Instruction::new(OpCode::OpenWrite)
                .with_p4(insert.table.clone())
        );

        // Push values onto stack
        for _value_list in &insert.values {
            for _value in _value_list {
                // Push each value
                instructions.push(Instruction::new(OpCode::Push));
            }
        }

        // Insert record
        instructions.push(Instruction::new(OpCode::Insert));

        // Close cursor
        instructions.push(Instruction::new(OpCode::Close));
        instructions.push(Instruction::new(OpCode::Halt));

        instructions
    }

    fn generate_update(&mut self, _update: &UpdateStmt) -> Vec<Instruction> {
        vec![Instruction::new(OpCode::Halt)]
    }

    fn generate_delete(&mut self, _delete: &DeleteStmt) -> Vec<Instruction> {
        vec![Instruction::new(OpCode::Halt)]
    }

    fn generate_create_table(
        &mut self,
        _create: &CreateTableStmt,
    ) -> Vec<Instruction> {
        vec![Instruction::new(OpCode::Halt)]
    }

    fn generate_drop_table(
        &mut self,
        _drop: &DropTableStmt,
    ) -> Vec<Instruction> {
        vec![Instruction::new(OpCode::Halt)]
    }
}
