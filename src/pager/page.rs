pub const PAGE_SIZE: usize = 4096;

pub type PageId = u32;

#[derive(Debug, Clone)]
pub struct Page {
    pub id: PageId,
    pub data: [u8; PAGE_SIZE],
}

impl Page {
    pub fn new(id: PageId) -> Self {
        Self {
            id,
            data: [0; PAGE_SIZE],
        }
    }

    pub fn from_bytes(id: PageId, bytes: Vec<u8>) -> Self {
        let mut data = [0u8; PAGE_SIZE];
        let len = bytes.len().min(PAGE_SIZE);
        data[..len].copy_from_slice(&bytes[..len]);
        Self { id, data }
    }

    pub fn id(&self) -> PageId {
        self.id
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.data
    }

    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.data
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_page_creation() {
        let page = Page::new(1);
        assert_eq!(page.id, 1);
        assert_eq!(page.data.len(), PAGE_SIZE);
        assert!(page.data.iter().all(|&b| b == 0));
    }

    #[test]
    fn test_page_access() {
        let mut page = Page::new(1);
        page.as_mut_slice()[0] = 42;
        assert_eq!(page.as_slice()[0], 42);
    }
}
