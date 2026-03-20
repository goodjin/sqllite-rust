//! B-Tree Index Implementation
//!
//! This module implements a disk-based B-Tree index that stores data in database pages.

use crate::index::{IndexError, Result};
use crate::pager::{PageId, Pager};
use crate::pager::page::{Page, PAGE_SIZE};
use crate::storage::Value;

/// B-Tree node type
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum NodeType {
    Internal = 0,
    Leaf = 1,
}

/// B-Tree index manager
pub struct BTreeIndex {
    pub name: String,
    pub table_name: String,
    pub column_name: String,
    pub root_page: PageId,
    pub unique: bool,
}

impl BTreeIndex {
    pub fn new(
        name: String,
        table_name: String,
        column_name: String,
        root_page: PageId,
        unique: bool,
    ) -> Self {
        Self {
            name,
            table_name,
            column_name,
            root_page,
            unique,
        }
    }

    /// Insert a key-rowid pair
    pub fn insert(&mut self, pager: &mut Pager, key: &Value, rowid: u64) -> Result<()> {
        if self.root_page == 0 {
            // Create new root
            let new_root = pager.allocate_page()?;
            self.root_page = new_root;

            // Initialize as leaf node
            let mut page = pager.get_page(new_root)?;
            write_node_type(&mut page, NodeType::Leaf);
            write_key_count(&mut page, 0);
            write_rightmost_child(&mut page, 0);
            write_next_leaf(&mut page, 0);
            write_parent(&mut page, 0);
            pager.write_page(&page)?;
        }

        match self.insert_recursive(pager, self.root_page, key, rowid) {
            Ok(_) => Ok(()),
            Err(IndexError::PageFull) => {
                // Split root
                self.split_root(pager, key, rowid)
            }
            Err(e) => Err(e),
        }
    }

    fn insert_recursive(&mut self, pager: &mut Pager, page_id: PageId, key: &Value, rowid: u64) -> Result<()> {
        let mut page = pager.get_page(page_id)?;
        let node_type = read_node_type(&page);

        if node_type == NodeType::Leaf {
            let pos = find_position(&page, key);

            // Check if key already exists
            if pos < read_key_count(&page) {
                if let Some(existing_key) = read_key_at(&page, pos) {
                    if existing_key == *key {
                        // Add rowid to existing key (simplified)
                        return Ok(());
                    }
                }
            }

            // Try to insert
            match insert_entry(&mut page, pos, key, 0, rowid) {
                Ok(_) => {
                    pager.write_page(&page)?;
                    Ok(())
                }
                Err(e) => Err(e),
            }
        } else {
            // Internal node - find child
            let child_id = get_child_for_key(&page, key);

            if child_id == 0 {
                return Err(IndexError::InvalidKey("No child found".to_string()));
            }

            match self.insert_recursive(pager, child_id, key, rowid) {
                Ok(_) => Ok(()),
                Err(IndexError::PageFull) => {
                    self.split_child(pager, page_id, child_id, key, rowid)
                }
                Err(e) => Err(e),
            }
        }
    }

    fn split_root(&mut self, pager: &mut Pager, key: &Value, rowid: u64) -> Result<()> {
        let old_root = self.root_page;
        let new_root = pager.allocate_page()?;

        let mut page = pager.get_page(new_root)?;
        write_node_type(&mut page, NodeType::Internal);
        write_key_count(&mut page, 0);
        write_rightmost_child(&mut page, old_root);
        write_parent(&mut page, 0);
        pager.write_page(&page)?;

        self.root_page = new_root;
        self.split_child(pager, new_root, old_root, key, rowid)
    }

    fn split_child(&mut self, pager: &mut Pager, parent_id: PageId, child_id: PageId, key: &Value, rowid: u64) -> Result<()> {
        // Read all entries from child
        let child_page = pager.get_page(child_id)?;
        let entries = read_all_entries(&child_page);
        let is_leaf = read_node_type(&child_page) == NodeType::Leaf;
        let next_leaf = read_next_leaf(&child_page);

        if entries.is_empty() {
            return Err(IndexError::InvalidKey("Empty child".to_string()));
        }

        // Find median
        let mid = entries.len() / 2;
        let median_key = entries[mid].0.clone();

        // Create sibling
        let sibling_id = pager.allocate_page()?;
        let mut sibling_page = pager.get_page(sibling_id)?;
        write_node_type(&mut sibling_page, if is_leaf { NodeType::Leaf } else { NodeType::Internal });
        write_key_count(&mut sibling_page, 0);
        write_parent(&mut sibling_page, parent_id);

        // Move second half to sibling
        let mut sibling_pos = 0;
        for i in (mid + 1)..entries.len() {
            let (ref k, child, ref rowids) = entries[i];
            if let Some(&first_rowid) = rowids.first() {
                let _ = insert_entry(&mut sibling_page, sibling_pos, k, child, first_rowid);
                sibling_pos += 1;
            }
        }

        if is_leaf {
            write_next_leaf(&mut sibling_page, next_leaf);
        }

        pager.write_page(&sibling_page)?;

        // Keep first half in child
        let mut child_page = pager.get_page(child_id)?;
        clear_entries(&mut child_page);
        for i in 0..mid {
            let (ref k, child, ref rowids) = entries[i];
            if let Some(&first_rowid) = rowids.first() {
                let _ = insert_entry(&mut child_page, i, k, child, first_rowid);
            }
        }

        if is_leaf {
            write_next_leaf(&mut child_page, sibling_id);
        }

        pager.write_page(&child_page)?;

        // Insert median into parent
        let mut parent_page = pager.get_page(parent_id)?;
        let pos = find_position(&parent_page, &median_key);
        insert_entry(&mut parent_page, pos, &median_key, sibling_id, 0)?;
        pager.write_page(&parent_page)?;

        // Retry insert
        self.insert_recursive(pager, parent_id, key, rowid)
    }

    /// Search for a key
    pub fn search(&self, pager: &mut Pager, key: &Value) -> Result<Vec<u64>> {
        if self.root_page == 0 {
            return Ok(vec![]);
        }
        self.search_node(pager, self.root_page, key)
    }

    fn search_node(&self, pager: &mut Pager, page_id: PageId, key: &Value) -> Result<Vec<u64>> {
        let page = pager.get_page(page_id)?;

        if read_node_type(&page) == NodeType::Leaf {
            // Linear search
            let count = read_key_count(&page);
            for i in 0..count {
                if let Some((k, _, rowids)) = read_entry(&page, i) {
                    if k == *key {
                        return Ok(rowids);
                    }
                }
            }
            Ok(vec![])
        } else {
            let child_id = get_child_for_key(&page, key);

            if child_id == 0 {
                Ok(vec![])
            } else {
                self.search_node(pager, child_id, key)
            }
        }
    }

    /// Delete a key-rowid pair
    pub fn delete(&mut self, pager: &mut Pager, key: &Value, rowid: u64) -> Result<()> {
        if self.root_page == 0 {
            return Ok(());
        }
        self.delete_node(pager, self.root_page, key, rowid)
    }

    fn delete_node(&mut self, pager: &mut Pager, page_id: PageId, key: &Value, rowid: u64) -> Result<()> {
        let page = pager.get_page(page_id)?;
        let node_type = read_node_type(&page);
        let count = read_key_count(&page);

        if node_type == NodeType::Leaf {
            for i in 0..count {
                if let Some((k, _, _)) = read_entry(&page, i) {
                    if k == *key {
                        // Remove rowid (simplified - just return for now)
                        return Ok(());
                    }
                }
            }
        } else {
            let child_id = get_child_for_key(&page, key);

            if child_id != 0 {
                self.delete_node(pager, child_id, key, rowid)?;
            }
        }

        Ok(())
    }

    /// Range scan
    pub fn range_scan(&self, pager: &mut Pager, start: Option<&Value>, end: Option<&Value>) -> Result<Vec<u64>> {
        if self.root_page == 0 {
            return Ok(vec![]);
        }

        let mut result = Vec::new();
        let mut current = self.find_leftmost_leaf(pager)?;

        while current != 0 {
            let page = pager.get_page(current)?;
            let count = read_key_count(&page);

            for i in 0..count {
                if let Some((key, _, rowids)) = read_entry(&page, i) {
                    if let Some(s) = start {
                        if key < *s {
                            continue;
                        }
                    }
                    if let Some(e) = end {
                        if key >= *e {
                            return Ok(result);
                        }
                    }
                    result.extend(rowids);
                }
            }

            current = read_next_leaf(&page);
        }

        Ok(result)
    }

    fn find_leftmost_leaf(&self, pager: &mut Pager) -> Result<PageId> {
        let mut current = self.root_page;

        loop {
            let page = pager.get_page(current)?;

            if read_node_type(&page) == NodeType::Leaf {
                return Ok(current);
            }

            // Get leftmost child
            if let Some((_, child_id, _)) = read_entry(&page, 0) {
                if child_id != 0 {
                    current = child_id;
                } else {
                    current = read_rightmost_child(&page);
                }
            } else {
                current = read_rightmost_child(&page);
            }

            if current == 0 {
                return Ok(0);
            }
        }
    }
}

// Page layout constants
const OFFSET_NODE_TYPE: usize = 0;
const OFFSET_KEY_COUNT: usize = 1;
const OFFSET_RIGHTMOST_CHILD: usize = 4;
const OFFSET_NEXT_LEAF: usize = 4;
const OFFSET_PARENT: usize = 8;
const OFFSET_HEADER_END: usize = 12;

// Helper functions for reading/writing page data

fn read_node_type(page: &Page) -> NodeType {
    match page.data[OFFSET_NODE_TYPE] {
        0 => NodeType::Internal,
        _ => NodeType::Leaf,
    }
}

fn write_node_type(page: &mut Page, node_type: NodeType) {
    page.data[OFFSET_NODE_TYPE] = match node_type {
        NodeType::Internal => 0,
        NodeType::Leaf => 1,
    };
}

fn read_key_count(page: &Page) -> usize {
    u32::from_be_bytes([
        0,
        page.data[OFFSET_KEY_COUNT],
        page.data[OFFSET_KEY_COUNT + 1],
        page.data[OFFSET_KEY_COUNT + 2],
    ]) as usize
}

fn write_key_count(page: &mut Page, count: usize) {
    let bytes = (count as u32).to_be_bytes();
    page.data[OFFSET_KEY_COUNT] = bytes[1];
    page.data[OFFSET_KEY_COUNT + 1] = bytes[2];
    page.data[OFFSET_KEY_COUNT + 2] = bytes[3];
}

fn read_rightmost_child(page: &Page) -> PageId {
    u32::from_be_bytes([
        page.data[OFFSET_RIGHTMOST_CHILD],
        page.data[OFFSET_RIGHTMOST_CHILD + 1],
        page.data[OFFSET_RIGHTMOST_CHILD + 2],
        page.data[OFFSET_RIGHTMOST_CHILD + 3],
    ])
}

fn write_rightmost_child(page: &mut Page, page_id: PageId) {
    let bytes = page_id.to_be_bytes();
    page.data[OFFSET_RIGHTMOST_CHILD] = bytes[0];
    page.data[OFFSET_RIGHTMOST_CHILD + 1] = bytes[1];
    page.data[OFFSET_RIGHTMOST_CHILD + 2] = bytes[2];
    page.data[OFFSET_RIGHTMOST_CHILD + 3] = bytes[3];
}

fn read_next_leaf(page: &Page) -> PageId {
    u32::from_be_bytes([
        page.data[OFFSET_NEXT_LEAF],
        page.data[OFFSET_NEXT_LEAF + 1],
        page.data[OFFSET_NEXT_LEAF + 2],
        page.data[OFFSET_NEXT_LEAF + 3],
    ])
}

fn write_next_leaf(page: &mut Page, page_id: PageId) {
    let bytes = page_id.to_be_bytes();
    page.data[OFFSET_NEXT_LEAF] = bytes[0];
    page.data[OFFSET_NEXT_LEAF + 1] = bytes[1];
    page.data[OFFSET_NEXT_LEAF + 2] = bytes[2];
    page.data[OFFSET_NEXT_LEAF + 3] = bytes[3];
}

fn read_parent(page: &Page) -> PageId {
    u32::from_be_bytes([
        page.data[OFFSET_PARENT],
        page.data[OFFSET_PARENT + 1],
        page.data[OFFSET_PARENT + 2],
        page.data[OFFSET_PARENT + 3],
    ])
}

fn write_parent(page: &mut Page, page_id: PageId) {
    let bytes = page_id.to_be_bytes();
    page.data[OFFSET_PARENT] = bytes[0];
    page.data[OFFSET_PARENT + 1] = bytes[1];
    page.data[OFFSET_PARENT + 2] = bytes[2];
    page.data[OFFSET_PARENT + 3] = bytes[3];
}

fn serialize_value(value: &Value) -> Vec<u8> {
    match value {
        Value::Null => vec![0],
        Value::Integer(n) => {
            let mut bytes = vec![1];
            bytes.extend_from_slice(&n.to_be_bytes());
            bytes
        }
        Value::Real(r) => {
            let mut bytes = vec![2];
            bytes.extend_from_slice(&r.to_be_bytes());
            bytes
        }
        Value::Text(s) => {
            let mut bytes = vec![3];
            let s_bytes = s.as_bytes();
            bytes.extend_from_slice(&(s_bytes.len() as u32).to_be_bytes());
            bytes.extend_from_slice(s_bytes);
            bytes
        }
        Value::Blob(b) => {
            let mut bytes = vec![4];
            bytes.extend_from_slice(&(b.len() as u32).to_be_bytes());
            bytes.extend_from_slice(b);
            bytes
        }
        Value::Vector(v) => {
            let mut bytes = vec![5];
            bytes.extend_from_slice(&(v.len() as u32).to_be_bytes());
            for x in v {
                bytes.extend_from_slice(&x.to_be_bytes());
            }
            bytes
        }
    }
}

fn deserialize_value(data: &[u8], pos: &mut usize) -> Option<Value> {
    if *pos >= data.len() {
        return None;
    }
    let type_byte = data[*pos];
    *pos += 1;

    match type_byte {
        0 => Some(Value::Null),
        1 => {
            if *pos + 8 > data.len() {
                return None;
            }
            let n = i64::from_be_bytes([
                data[*pos], data[*pos + 1], data[*pos + 2], data[*pos + 3],
                data[*pos + 4], data[*pos + 5], data[*pos + 6], data[*pos + 7],
            ]);
            *pos += 8;
            Some(Value::Integer(n))
        }
        2 => {
            if *pos + 8 > data.len() {
                return None;
            }
            let r = f64::from_be_bytes([
                data[*pos], data[*pos + 1], data[*pos + 2], data[*pos + 3],
                data[*pos + 4], data[*pos + 5], data[*pos + 6], data[*pos + 7],
            ]);
            *pos += 8;
            Some(Value::Real(r))
        }
        3 => {
            if *pos + 4 > data.len() {
                return None;
            }
            let len = u32::from_be_bytes([data[*pos], data[*pos + 1], data[*pos + 2], data[*pos + 3]]) as usize;
            *pos += 4;
            if *pos + len > data.len() {
                return None;
            }
            let s = String::from_utf8_lossy(&data[*pos..*pos + len]).to_string();
            *pos += len;
            Some(Value::Text(s))
        }
        4 => {
            if *pos + 4 > data.len() {
                return None;
            }
            let len = u32::from_be_bytes([data[*pos], data[*pos + 1], data[*pos + 2], data[*pos + 3]]) as usize;
            *pos += 4;
            if *pos + len > data.len() {
                return None;
            }
            let b = data[*pos..*pos + len].to_vec();
            *pos += len;
            Some(Value::Blob(b))
        }
        5 => {
            if *pos + 4 > data.len() {
                return None;
            }
            let len = u32::from_be_bytes([data[*pos], data[*pos + 1], data[*pos + 2], data[*pos + 3]]) as usize;
            *pos += 4;
            if *pos + len * 4 > data.len() {
                return None;
            }
            let mut vector = Vec::with_capacity(len);
            for _ in 0..len {
                let x = f32::from_be_bytes([
                    data[*pos], data[*pos + 1], data[*pos + 2], data[*pos + 3],
                ]);
                vector.push(x);
                *pos += 4;
            }
            Some(Value::Vector(vector))
        }
        _ => None,
    }
}

fn get_entry_offset(page: &Page, index: usize) -> usize {
    let mut offset = OFFSET_HEADER_END;
    for _ in 0..index {
        offset = skip_entry(page, offset);
    }
    offset
}

fn skip_entry(page: &Page, offset: usize) -> usize {
    let mut pos = offset;
    if pos + 4 > PAGE_SIZE {
        return PAGE_SIZE;
    }
    let key_len = u32::from_be_bytes([
        page.data[pos], page.data[pos + 1], page.data[pos + 2], page.data[pos + 3],
    ]) as usize;
    pos += 4 + key_len;

    if read_node_type(page) == NodeType::Internal {
        pos += 4;
    }

    if pos + 4 > PAGE_SIZE {
        return PAGE_SIZE;
    }
    let rowid_count = u32::from_be_bytes([
        page.data[pos], page.data[pos + 1], page.data[pos + 2], page.data[pos + 3],
    ]) as usize;
    pos += 4 + rowid_count * 8;

    pos
}

fn read_entry(page: &Page, index: usize) -> Option<(Value, PageId, Vec<u64>)> {
    if index >= read_key_count(page) {
        return None;
    }

    let offset = get_entry_offset(page, index);
    let mut pos = offset;

    let key_len = u32::from_be_bytes([
        page.data[pos], page.data[pos + 1], page.data[pos + 2], page.data[pos + 3],
    ]) as usize;
    pos += 4;

    let key_data = &page.data[pos..pos + key_len];
    pos += key_len;
    let mut key_pos = 0;
    let key = deserialize_value(key_data, &mut key_pos)?;

    let child_page_id = if read_node_type(page) == NodeType::Internal {
        let id = u32::from_be_bytes([
            page.data[pos], page.data[pos + 1], page.data[pos + 2], page.data[pos + 3],
        ]);
        pos += 4;
        id
    } else {
        0
    };

    let rowid_count = u32::from_be_bytes([
        page.data[pos], page.data[pos + 1], page.data[pos + 2], page.data[pos + 3],
    ]) as usize;
    pos += 4;

    let mut rowids = Vec::with_capacity(rowid_count);
    for _ in 0..rowid_count {
        let rowid = u64::from_be_bytes([
            page.data[pos], page.data[pos + 1], page.data[pos + 2], page.data[pos + 3],
            page.data[pos + 4], page.data[pos + 5], page.data[pos + 6], page.data[pos + 7],
        ]);
        rowids.push(rowid);
        pos += 8;
    }

    Some((key, child_page_id, rowids))
}

fn read_key_at(page: &Page, index: usize) -> Option<Value> {
    read_entry(page, index).map(|(key, _, _)| key)
}

fn find_position(page: &Page, key: &Value) -> usize {
    let mut low = 0;
    let mut high = read_key_count(page);

    while low < high {
        let mid = (low + high) / 2;
        match read_key_at(page, mid) {
            Some(mid_key) => {
                if mid_key < *key {
                    low = mid + 1;
                } else {
                    high = mid;
                }
            }
            None => break,
        }
    }
    low
}

fn get_child_for_key(page: &Page, key: &Value) -> PageId {
    if read_node_type(page) != NodeType::Internal {
        return 0;
    }

    let pos = find_position(page, key);
    if pos < read_key_count(page) {
        read_entry(page, pos).map(|(_, child, _)| child).unwrap_or(0)
    } else {
        read_rightmost_child(page)
    }
}

fn insert_entry(page: &mut Page, pos: usize, key: &Value, child: PageId, rowid: u64) -> Result<()> {
    let count = read_key_count(page);
    if pos > count {
        return Err(IndexError::InvalidKey("Invalid position".to_string()));
    }

    let key_bytes = serialize_value(key);
    let mut entry_size = 4 + key_bytes.len() + 4 + 8;
    if read_node_type(page) == NodeType::Internal {
        entry_size += 4;
    }

    // Check space
    let used = if count == 0 {
        OFFSET_HEADER_END
    } else {
        get_entry_offset(page, count)
    };

    if used + entry_size > PAGE_SIZE {
        return Err(IndexError::PageFull);
    }

    // Shift existing entries
    if pos < count {
        let src_offset = get_entry_offset(page, pos);
        let dst_offset = src_offset + entry_size;
        let bytes_to_move = PAGE_SIZE - dst_offset;

        for i in (0..bytes_to_move).rev() {
            let src = src_offset + i;
            let dst = dst_offset + i;
            if src < PAGE_SIZE && dst < PAGE_SIZE {
                page.data[dst] = page.data[src];
            }
        }
    }

    // Write entry
    let offset = get_entry_offset(page, pos);
    let mut write_pos = offset;

    let key_len = key_bytes.len() as u32;
    page.data[write_pos..write_pos + 4].copy_from_slice(&key_len.to_be_bytes());
    write_pos += 4;

    page.data[write_pos..write_pos + key_bytes.len()].copy_from_slice(&key_bytes);
    write_pos += key_bytes.len();

    if read_node_type(page) == NodeType::Internal {
        page.data[write_pos..write_pos + 4].copy_from_slice(&child.to_be_bytes());
        write_pos += 4;
    }

    page.data[write_pos..write_pos + 4].copy_from_slice(&1u32.to_be_bytes());
    write_pos += 4;

    page.data[write_pos..write_pos + 8].copy_from_slice(&rowid.to_be_bytes());

    write_key_count(page, count + 1);

    Ok(())
}

fn clear_entries(page: &mut Page) {
    write_key_count(page, 0);
}

fn read_all_entries(page: &Page) -> Vec<(Value, PageId, Vec<u64>)> {
    let mut entries = Vec::new();
    let count = read_key_count(page);
    for i in 0..count {
        if let Some(entry) = read_entry(page, i) {
            entries.push(entry);
        }
    }
    entries
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn test_btree_insert_and_search() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap();
        let mut pager = Pager::open(path).unwrap();

        let mut index = BTreeIndex::new(
            "test_idx".to_string(),
            "users".to_string(),
            "name".to_string(),
            0,
            false,
        );

        index.insert(&mut pager, &Value::Text("Alice".to_string()), 1).unwrap();
        index.insert(&mut pager, &Value::Text("Bob".to_string()), 2).unwrap();
        index.insert(&mut pager, &Value::Text("Charlie".to_string()), 3).unwrap();

        let result = index.search(&mut pager, &Value::Text("Bob".to_string())).unwrap();
        assert_eq!(result, vec![2]);

        let result = index.search(&mut pager, &Value::Text("Alice".to_string())).unwrap();
        assert_eq!(result, vec![1]);

        let result = index.search(&mut pager, &Value::Text("David".to_string())).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_btree_range_scan() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap();
        let mut pager = Pager::open(path).unwrap();

        let mut index = BTreeIndex::new(
            "test_idx".to_string(),
            "users".to_string(),
            "salary".to_string(),
            0,
            false,
        );

        index.insert(&mut pager, &Value::Integer(3000), 1).unwrap();
        index.insert(&mut pager, &Value::Integer(4000), 2).unwrap();
        index.insert(&mut pager, &Value::Integer(5000), 3).unwrap();
        index.insert(&mut pager, &Value::Integer(6000), 4).unwrap();
        index.insert(&mut pager, &Value::Integer(7000), 5).unwrap();

        let result = index.range_scan(&mut pager, Some(&Value::Integer(4000)), Some(&Value::Integer(6000))).unwrap();
        assert_eq!(result.len(), 2);
        assert!(result.contains(&2));
        assert!(result.contains(&3));
    }

    #[test]
    fn test_page_operations() {
        let mut page = Page::new(1);

        write_node_type(&mut page, NodeType::Leaf);
        write_key_count(&mut page, 0);

        assert_eq!(read_node_type(&page), NodeType::Leaf);
        assert_eq!(read_key_count(&page), 0);

        insert_entry(&mut page, 0, &Value::Integer(10), 0, 100).unwrap();
        insert_entry(&mut page, 1, &Value::Integer(20), 0, 200).unwrap();

        assert_eq!(read_key_count(&page), 2);

        let (key, _, rowids) = read_entry(&page, 0).unwrap();
        assert_eq!(key, Value::Integer(10));
        assert_eq!(rowids, vec![100]);
    }
}
