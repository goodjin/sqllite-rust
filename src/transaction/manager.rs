use crate::transaction::{Result, TransactionError, Wal};

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum TransactionState {
    None,
    Active,
    Committed,
    RolledBack,
}

pub struct TransactionManager {
    wal: Wal,
    state: TransactionState,
    read_version: u64,
    write_version: u64,
}

impl TransactionManager {
    pub fn new(wal_path: &str) -> Result<Self> {
        let wal = Wal::open(wal_path)?;
        Ok(Self {
            wal,
            state: TransactionState::None,
            read_version: 0,
            write_version: 0,
        })
    }

    pub fn begin(&mut self) -> Result<()> {
        if self.state == TransactionState::Active {
            return Err(TransactionError::AlreadyActive);
        }

        self.state = TransactionState::Active;
        self.read_version = self.write_version;

        Ok(())
    }

    pub fn commit(&mut self) -> Result<()> {
        if self.state != TransactionState::Active {
            return Err(TransactionError::NotActive);
        }

        self.write_version += 1;
        self.state = TransactionState::Committed;

        // Clear WAL after successful commit
        self.wal.clear()?;

        Ok(())
    }

    pub fn rollback(&mut self) -> Result<()> {
        if self.state != TransactionState::Active {
            return Err(TransactionError::NotActive);
        }

        self.state = TransactionState::RolledBack;

        // Clear WAL entries for this transaction
        self.wal.clear()?;

        Ok(())
    }

    pub fn state(&self) -> TransactionState {
        self.state
    }

    pub fn is_active(&self) -> bool {
        self.state == TransactionState::Active
    }

    pub fn wal_mut(&mut self) -> &mut Wal {
        &mut self.wal
    }
}
