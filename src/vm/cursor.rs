
#[derive(Debug)]
pub struct Cursor {
    pub table_name: String,
    pub current_row: Option<u64>,
    pub is_open: bool,
}

impl Cursor {
    pub fn new(table_name: String) -> Self {
        Self {
            table_name,
            current_row: None,
            is_open: false,
        }
    }

    pub fn open(&mut self) {
        self.is_open = true;
        self.current_row = None;
    }

    pub fn close(&mut self) {
        self.is_open = false;
        self.current_row = None;
    }

    pub fn next(&mut self) {
        self.current_row = Some(self.current_row.map_or(0, |r| r + 1));
    }

    pub fn prev(&mut self) {
        if let Some(row) = self.current_row {
            if row > 0 {
                self.current_row = Some(row - 1);
            } else {
                self.current_row = None;
            }
        }
    }
}
