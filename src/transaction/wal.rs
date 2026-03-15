use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use crate::transaction::{Result, TransactionError};

#[derive(Debug, Clone)]
pub struct WalEntry {
    pub page_id: u32,
    pub data: Vec<u8>,
    pub checksum: u32,
}

pub struct Wal {
    file: File,
}

impl Wal {
    pub fn open(path: &str) -> Result<Self> {
        let path = Path::new(path);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;

        Ok(Self { file })
    }

    pub fn append(&mut self, page_id: u32, data: &[u8]) -> Result<()> {
        let checksum = self.calculate_checksum(data);
        let entry = WalEntry {
            page_id,
            data: data.to_vec(),
            checksum,
        };

        // Write entry header
        self.file.write_all(&entry.page_id.to_be_bytes())?;
        self.file.write_all(&(entry.data.len() as u32).to_be_bytes())?;
        self.file.write_all(&entry.checksum.to_be_bytes())?;

        // Write data
        self.file.write_all(&entry.data)?;
        self.file.sync_all()?;

        Ok(())
    }

    pub fn read_entries(&mut self) -> Result<Vec<WalEntry>> {
        let mut entries = Vec::new();
        self.file.seek(SeekFrom::Start(0))?;

        loop {
            let mut page_id_buf = [0u8; 4];
            if self.file.read_exact(&mut page_id_buf).is_err() {
                break;
            }
            let page_id = u32::from_be_bytes(page_id_buf);

            let mut len_buf = [0u8; 4];
            self.file.read_exact(&mut len_buf)?;
            let len = u32::from_be_bytes(len_buf) as usize;

            let mut checksum_buf = [0u8; 4];
            self.file.read_exact(&mut checksum_buf)?;
            let checksum = u32::from_be_bytes(checksum_buf);

            let mut data = vec![0u8; len];
            self.file.read_exact(&mut data)?;

            // Verify checksum
            if self.calculate_checksum(&data) != checksum {
                return Err(TransactionError::WalError("Checksum mismatch".to_string()));
            }

            entries.push(WalEntry {
                page_id,
                data,
                checksum,
            });
        }

        Ok(entries)
    }

    pub fn clear(&mut self) -> Result<()> {
        self.file.set_len(0)?;
        self.file.sync_all()?;
        Ok(())
    }

    fn calculate_checksum(&self, data: &[u8]) -> u32 {
        let mut checksum: u32 = 0;
        for chunk in data.chunks(4) {
            let mut word = [0u8; 4];
            word[..chunk.len()].copy_from_slice(chunk);
            checksum = checksum.wrapping_add(u32::from_be_bytes(word));
        }
        checksum
    }
}
