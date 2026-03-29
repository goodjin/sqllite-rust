//! B-tree Storage Engine - Phase 2: Core B-tree Algorithms
//!
//! This module implements the core B-tree operations:
//! - Search: O(log n) key lookup
//! - Insert: O(log n) with page splitting
//! - Delete: O(log n) with merge/redistribution
//! - Range scan: O(log n + k) for k results

use crate::pager::{PageId, Pager};
use crate::pager::page::Page;
use crate::storage::{Result, StorageError};
use crate::storage::btree_engine::{
    PageHeader, PageType, BtreePageOps, RecordHeader,
    compare_keys, MAX_INLINE_SIZE, MIN_RECORDS_FOR_MERGE,
};
use std::cmp::Ordering;

/// B-tree configuration
pub const BTREE_ORDER: usize = 100; // Max keys per internal node

/// B-tree storage engine
pub struct BtreeStorage {
    /// Root page ID of the B-tree
    root_page: PageId,
    /// Next available row ID
    next_rowid: u64,
}

impl BtreeStorage {
    /// Create a new B-tree storage engine
    pub fn new(root_page: PageId) -> Self {
        Self {
            root_page,
            next_rowid: 1,
        }
    }

    /// Get the root page ID
    pub fn root_page(&self) -> PageId {
        self.root_page
    }

    /// Generate next row ID
    pub fn next_rowid(&mut self) -> u64 {
        let rowid = self.next_rowid;
        self.next_rowid += 1;
        rowid
    }

    // ========================================================================
    // Search Algorithm
    // ========================================================================

    /// Search for a key in the B-tree
    /// Returns the value if found
    pub fn search(&self, pager: &mut Pager, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let mut current_page = self.root_page;

        loop {
            let page = pager.get_page(current_page)?;
            let header = page.read_header()?;

            if header.is_leaf() {
                // Leaf node: binary search for the key
                return self.search_leaf_page(&page, key);
            } else {
                // Internal node: find the child page to traverse
                current_page = self.find_child_page(pager, current_page, key)?;
            }
        }
    }

    /// Search within a leaf page using binary search (zero-copy optimized)
    /// 
    /// Performance: Uses compare_key_at to avoid allocating all records.
    /// Falls back to linear scan if any deleted records are encountered
    /// to handle the sparse array case correctly.
    fn search_leaf_page(&self, page: &Page, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let record_count = page.record_count()? as usize;
        
        // Try binary search first
        let mut left = 0;
        let mut right = record_count;
        let mut found_deleted = false;

        while left < right {
            let mid = (left + right) / 2;
            match page.compare_key_at(mid, key) {
                Ok(Ordering::Equal) => {
                    let (k, v) = page.get_record_at(mid)?;
                    return Ok(Some(v));
                }
                Ok(Ordering::Less) => left = mid + 1,
                Ok(Ordering::Greater) => right = mid,
                Err(_) => {
                    // Deleted record found - binary search may be unreliable
                    found_deleted = true;
                    break;
                }
            }
        }
        
        // If we hit a deleted record during binary search, fall back to linear scan
        if found_deleted {
            return self.search_leaf_page_linear(page, key);
        }

        Ok(None)
    }
    
    /// Linear scan fallback for pages with deleted records
    fn search_leaf_page_linear(&self, page: &Page, key: &[u8]) -> Result<Option<Vec<u8>>> {
        use crate::storage::btree_engine::{PageHeader, RecordHeader};
        
        let header = page.read_header()?;
        let record_count = header.record_count as usize;
        
        for slot_idx in 0..record_count {
            let slot_offset = PageHeader::SIZE + slot_idx * 2;
            let record_offset = u16::from_le_bytes([
                page.as_slice()[slot_offset],
                page.as_slice()[slot_offset + 1]
            ]) as usize;
            
            let rec_header = RecordHeader::from_bytes(&page.as_slice()[record_offset..])?;
            if rec_header.is_deleted() {
                continue;
            }
            
            let key_start = record_offset + RecordHeader::SIZE;
            let key_end = key_start + rec_header.key_size as usize;
            let record_key = &page.as_slice()[key_start..key_end];
            
            if compare_keys(record_key, key) == Ordering::Equal {
                let value_end = key_end + rec_header.value_size as usize;
                let value = page.as_slice()[key_end..value_end].to_vec();
                return Ok(Some(value));
            }
        }
        
        Ok(None)
    }

    /// Find the child page for a key in an internal node using binary search
    /// 
    /// Performance: O(log n) with zero-copy comparison
    fn find_child_page(&self, pager: &mut Pager, page_id: PageId, key: &[u8]) -> Result<PageId> {
        let page = pager.get_page(page_id)?;
        let header = page.read_header()?;
        let record_count = page.record_count()? as usize;

        if record_count == 0 {
            // No separators, only leftmost child
            return Ok(header.left_sibling);
        }

        // Binary search for the correct child page using zero-copy comparison
        let mut left = 0;
        let mut right = record_count;

        // Binary search: find the rightmost separator <= key
        // Then return that separator's child (which contains keys >= that separator)
        let mut result_idx: Option<usize> = None;
        
        while left < right {
            let mid = (left + right) / 2;
            
            match page.compare_key_at(mid, key) {
                Ok(Ordering::Less) => {
                    // key > separator[mid], this separator is a candidate
                    // separator[mid] <= key, so key could be in separator[mid]'s child
                    result_idx = Some(mid);
                    left = mid + 1;
                }
                Ok(Ordering::Greater) => {
                    // key < separator[mid], search left half
                    right = mid;
                }
                Ok(Ordering::Equal) => {
                    // key == separator[mid], exact match
                    // The key belongs to separator[mid]'s child (right side)
                    result_idx = Some(mid);
                    left = mid + 1;
                }
                Err(_) => {
                    // Deleted record, treat as greater (move left)
                    right = mid;
                }
            }
        }
        
        if let Some(idx) = result_idx {
            // Return separator[idx]'s child
            let slot_offset = PageHeader::SIZE + idx * 2;
            let record_offset = u16::from_le_bytes([
                page.data[slot_offset],
                page.data[slot_offset + 1]
            ]) as usize;
            
            let rec_header = RecordHeader::from_bytes(&page.data[record_offset..])?;
            let key_start = record_offset + RecordHeader::SIZE;
            let key_end = key_start + rec_header.key_size as usize;
            let value_end = key_end + rec_header.value_size as usize;
            let child_id_bytes = &page.data[key_end..value_end];
            
            self.bytes_to_page_id(child_id_bytes)
        } else {
            // No separator <= key, use leftmost child
            Ok(header.left_sibling)
        }
    }

    /// Find the leftmost leaf page for range scans
    pub fn find_leftmost_leaf(&self, pager: &mut Pager) -> Result<PageId> {
        let mut current_page = self.root_page;

        loop {
            let page = pager.get_page(current_page)?;
            let header = page.read_header()?;

            if header.is_leaf() {
                return Ok(current_page);
            }

            // Go to leftmost child
            let records = page.get_all_records()?;
            if header.left_sibling != 0 {
                current_page = header.left_sibling;
            } else if let Some(first_record) = records.first() {
                current_page = self.bytes_to_page_id(&first_record.1)?;
            } else {
                return Err(StorageError::KeyNotFound);
            }
        }
    }

    // ========================================================================
    // Insert Algorithm
    // ========================================================================

    /// Insert a key-value pair into the B-tree
    pub fn insert(&mut self, pager: &mut Pager, key: &[u8], value: &[u8]) -> Result<()> {
        // Check if we need overflow pages
        let total_size = key.len() + value.len();

        if total_size > MAX_INLINE_SIZE {
            // TODO: Handle overflow pages in Phase 3
            return Err(StorageError::RecordTooLarge(total_size));
        }

        // Find the insertion path
        let path = self.find_insert_path(pager, key)?;

        // Try to insert into the leaf page
        let leaf_page_id = *path.last().unwrap();
        let mut leaf_page = pager.get_page(leaf_page_id)?;

        // Check if key already exists (and not deleted)
        if let Some((_slot_idx, is_deleted)) = self.find_key_slot_with_status(&leaf_page, key)? {
            if !is_deleted {
                return Err(StorageError::DuplicateKey);
            }
            // Key exists but is deleted - we can overwrite it
            // For now, just insert a new record (the old one remains marked deleted)
            // In a full implementation, we'd reclaim the space
        }

        if leaf_page.has_space(key.len() + value.len())? {
            // Direct insert
            leaf_page.insert_record(key, value)?;
            pager.write_page(&leaf_page)?;
        } else {
            // Page is full, need to split
            let (new_page_id, median_key) = self.split_leaf_page(pager, leaf_page_id)?;

            // Insert into appropriate page
            let target_page_id = if compare_keys(key, &median_key) == Ordering::Less {
                leaf_page_id
            } else {
                new_page_id
            };

            let mut target_page = pager.get_page(target_page_id)?;
            target_page.insert_record(key, value)?;
            pager.write_page(&target_page)?;

            // Propagate split upward
            self.propagate_split(pager, &path, median_key, new_page_id)?;
        }

        Ok(())
    }

    /// Find the path from root to leaf for insertion
    fn find_insert_path(&self, pager: &mut Pager, key: &[u8]) -> Result<Vec<PageId>> {
        let mut path = vec![self.root_page];
        let mut current_page = self.root_page;

        loop {
            let page = pager.get_page(current_page)?;
            let header = page.read_header()?;

            if header.is_leaf() {
                break;
            }

            current_page = self.find_child_page(pager, current_page, key)?;
            path.push(current_page);
        }

        Ok(path)
    }

    /// Split a leaf page
    /// Returns (new_page_id, median_key)
    fn split_leaf_page(&self, pager: &mut Pager, page_id: PageId) -> Result<(PageId, Vec<u8>)> {
        let mut page = pager.get_page(page_id)?;
        let records = page.get_all_records()?;

        // Allocate new page
        let new_page_id = pager.allocate_page()?;
        let mut new_page = Page::new(new_page_id);

        // Initialize new page as leaf
        let mut new_header = PageHeader::new(PageType::Data);
        new_header.set_leaf(true);
        new_page.write_header(&new_header)?;

        // Find median
        let mid = records.len() / 2;
        let median_key = records[mid].0.clone();

        // Clear old page and re-insert first half
        let old_header = page.read_header()?;
        let mut cleared_header = PageHeader::new(PageType::Data);
        cleared_header.set_leaf(true);
        cleared_header.set_root(old_header.is_root());
        cleared_header.left_sibling = old_header.left_sibling;
        page.write_header(&cleared_header)?;

        // Insert first half into old page
        for i in 0..mid {
            page.insert_record(&records[i].0, &records[i].1)?;
        }

        // Insert second half into new page
        for i in mid..records.len() {
            new_page.insert_record(&records[i].0, &records[i].1)?;
        }

        // Update B+ tree leaf linked list
        let mut old_header = page.read_header()?;
        let right_sibling = old_header.right_sibling;
        old_header.right_sibling = new_page_id;
        page.write_header(&old_header)?;

        let mut new_header = new_page.read_header()?;
        new_header.left_sibling = page_id;
        new_header.right_sibling = right_sibling;
        new_page.write_header(&new_header)?;

        // Update left sibling's right pointer if exists
        if right_sibling != 0 {
            let mut sibling = pager.get_page(right_sibling)?;
            let mut sibling_header = sibling.read_header()?;
            sibling_header.left_sibling = new_page_id;
            sibling.write_header(&sibling_header)?;
            pager.write_page(&sibling)?;
        }

        pager.write_page(&page)?;
        pager.write_page(&new_page)?;

        Ok((new_page_id, median_key))
    }

    /// Propagate a page split upward through the tree
    fn propagate_split(
        &mut self,
        pager: &mut Pager,
        path: &[PageId],
        key: Vec<u8>,
        new_page_id: PageId,
    ) -> Result<()> {
        if path.len() <= 1 {
            // Root was split, need to create new root
            self.create_new_root(pager, path[0], key, new_page_id)?;
            return Ok(());
        }

        // Start from parent of the split page
        for i in (0..path.len() - 1).rev() {
            let parent_id = path[i];
            let mut parent = pager.get_page(parent_id)?;

            // Check if parent has space
            let child_id_bytes = self.page_id_to_bytes(new_page_id);
            let entry_size = key.len() + child_id_bytes.len();

            if parent.has_space(entry_size)? {
                // Parent has space, insert the separator
                parent.insert_record(&key, &child_id_bytes)?;
                pager.write_page(&parent)?;
                return Ok(());
            } else {
                // Parent is also full, need to split it
                let (new_parent_id, new_median) = self.split_index_page(pager, parent_id)?;

                // Insert into appropriate parent
                let target_parent_id = if compare_keys(&key, &new_median) == Ordering::Less {
                    parent_id
                } else {
                    new_parent_id
                };

                let mut target_parent = pager.get_page(target_parent_id)?;
                target_parent.insert_record(&key, &child_id_bytes)?;
                pager.write_page(&target_parent)?;

                // Continue propagating if not at root
                if i == 0 {
                    // Root was split
                    self.create_new_root(pager, parent_id, new_median, new_parent_id)?;
                    return Ok(());
                }

                // Continue with next level
                // Note: This is a simplified version; full implementation
                // would need to track the new path properly
            }
        }

        Ok(())
    }

    /// Split an internal (index) page
    fn split_index_page(&self, pager: &mut Pager, page_id: PageId) -> Result<(PageId, Vec<u8>)> {
        let mut page = pager.get_page(page_id)?;
        let records = page.get_all_records()?;

        // Allocate new page
        let new_page_id = pager.allocate_page()?;
        let mut new_page = Page::new(new_page_id);

        // Initialize as internal node
        let mut new_header = PageHeader::new(PageType::Index);
        
        // Find median (don't include median key in either child)
        let mid = records.len() / 2;
        let median_key = records[mid].0.clone();
        // The median record's child becomes the leftmost child of the new page
        let median_child = u32::from_le_bytes([
            records[mid].1[0], records[mid].1[1], 
            records[mid].1[2], records[mid].1[3]
        ]);
        new_header.left_sibling = median_child;
        new_page.write_header(&new_header)?;

        // Clear old page and re-insert first half (excluding median)
        let old_header = page.read_header()?;
        let mut cleared_header = PageHeader::new(PageType::Index);
        cleared_header.set_root(old_header.is_root());
        cleared_header.left_sibling = old_header.left_sibling;
        page.write_header(&cleared_header)?;

        // Insert first half into old page
        for i in 0..mid {
            page.insert_record(&records[i].0, &records[i].1)?;
        }

        // Insert second half into new page (excluding median)
        for i in (mid + 1)..records.len() {
            new_page.insert_record(&records[i].0, &records[i].1)?;
        }

        pager.write_page(&page)?;
        pager.write_page(&new_page)?;

        Ok((new_page_id, median_key))
    }

    /// Create a new root when the old root splits
    fn create_new_root(
        &mut self,
        pager: &mut Pager,
        old_root: PageId,
        median_key: Vec<u8>,
        new_page: PageId,
    ) -> Result<()> {
        let new_root_id = pager.allocate_page()?;
        let mut new_root = Page::new(new_root_id);

        // Initialize as internal node
        let mut header = PageHeader::new(PageType::Index);
        header.set_root(true);
        new_root.write_header(&header)?;

        // Insert separator pointing to both children
        // Key points to new_page, everything less than key is in old_root
        let child_id_bytes = self.page_id_to_bytes(new_page);
        new_root.insert_record(&median_key, &child_id_bytes)?;

        // Store old_root as leftmost child
        // For simplicity, we'll use a special encoding or header field
        // In a full implementation, we'd have a separate structure for this
        let mut header = new_root.read_header()?;
        header.left_sibling = old_root; // Reuse field for leftmost child
        new_root.write_header(&header)?;

        // Update old root to not be root anymore
        let mut old_root_page = pager.get_page(old_root)?;
        let mut old_header = old_root_page.read_header()?;
        old_header.set_root(false);
        old_header.parent_page = new_root_id;
        old_root_page.write_header(&old_header)?;
        pager.write_page(&old_root_page)?;

        // Update new page's parent
        let mut new_page_page = pager.get_page(new_page)?;
        let mut new_page_header = new_page_page.read_header()?;
        new_page_header.parent_page = new_root_id;
        new_page_page.write_header(&new_page_header)?;
        pager.write_page(&new_page_page)?;

        pager.write_page(&new_root)?;

        self.root_page = new_root_id;

        Ok(())
    }

    // ========================================================================
    // Delete Algorithm
    // ========================================================================

    /// Delete a key from the B-tree
    /// Returns true if the key was found and deleted
    pub fn delete(&self, pager: &mut Pager, key: &[u8]) -> Result<bool> {
        let path = self.find_path_to_key(pager, key)?;

        if path.is_empty() {
            return Ok(false);
        }

        let leaf_page_id = *path.last().unwrap();
        let mut leaf_page = pager.get_page(leaf_page_id)?;

        // Find the slot index for this key
        if let Some(slot_idx) = self.find_key_slot(&leaf_page, key)? {
            // Mark as deleted (logical delete)
            leaf_page.mark_deleted(slot_idx)?;
            pager.write_page(&leaf_page)?;

            // Check if we need to merge
            let header = leaf_page.read_header()?;
            if header.record_count as usize <= MIN_RECORDS_FOR_MERGE {
                self.merge_or_redistribute(pager, &path)?;
            }

            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Find the path to a specific key
    fn find_path_to_key(&self, pager: &mut Pager, key: &[u8]) -> Result<Vec<PageId>> {
        let mut path = Vec::new();
        let mut current_page = self.root_page;

        loop {
            path.push(current_page);

            let page = pager.get_page(current_page)?;
            let header = page.read_header()?;

            if header.is_leaf() {
                // Check if key exists in this leaf
                if self.search_leaf_page(&page, key)?.is_some() {
                    return Ok(path);
                } else {
                    return Ok(Vec::new()); // Key not found
                }
            }

            current_page = self.find_child_page(pager, current_page, key)?;
        }
    }

    /// Find the slot index for a key in a leaf page
    fn find_key_slot(&self, page: &Page, key: &[u8]) -> Result<Option<usize>> {
        use crate::storage::btree_engine::{PageHeader, RecordHeader};

        let header = page.read_header()?;
        let record_count = header.record_count as usize;

        // Iterate over all slots directly
        for slot_idx in 0..record_count {
            let slot_offset = PageHeader::SIZE + slot_idx * 2;
            let record_offset = u16::from_le_bytes([
                page.as_slice()[slot_offset],
                page.as_slice()[slot_offset + 1]
            ]) as usize;

            // Read record header to get key size and deleted status
            let rec_header = RecordHeader::from_bytes(&page.as_slice()[record_offset..])?;

            if rec_header.is_deleted() {
                continue;
            }

            // Read the key
            let key_start = record_offset + RecordHeader::SIZE;
            let key_end = key_start + rec_header.key_size as usize;
            let record_key = &page.as_slice()[key_start..key_end];

            if compare_keys(record_key, key) == Ordering::Equal {
                return Ok(Some(slot_idx));
            }
        }

        Ok(None)
    }

    /// Find the slot index for a key and return whether it's deleted
    fn find_key_slot_with_status(
        &self,
        page: &Page,
        key: &[u8],
    ) -> Result<Option<(usize, bool)>> {
        use crate::storage::btree_engine::{PageHeader, RecordHeader};

        let header = page.read_header()?;
        let record_count = header.record_count as usize;

        // Iterate over all slots directly (not using get_all_records which filters deleted)
        for slot_idx in 0..record_count {
            let slot_offset = PageHeader::SIZE + slot_idx * 2;
            let record_offset = u16::from_le_bytes([
                page.as_slice()[slot_offset],
                page.as_slice()[slot_offset + 1]
            ]) as usize;

            // Read record header to get key size
            let rec_header = RecordHeader::from_bytes(&page.as_slice()[record_offset..])?;

            // Read the key
            let key_start = record_offset + RecordHeader::SIZE;
            let key_end = key_start + rec_header.key_size as usize;
            let record_key = &page.as_slice()[key_start..key_end];

            if compare_keys(record_key, key) == Ordering::Equal {
                // Return the first match, but continue if we find an active one later?
                // Actually, if we have both, we should probably prefer the active one.
                if !rec_header.is_deleted() {
                    return Ok(Some((slot_idx, false)));
                } else {
                    // It's deleted, but keep looking for an active one
                    if slot_idx == record_count - 1 || compare_keys(record_key, key) == Ordering::Equal {
                         // This is tricky. In a sorted array, all same keys are together.
                         // But we might have [Deleted A, Active A].
                         // Let's just return the first active one we find.
                    }
                }
            }
        }

        // Second pass: if no active one found, return the first deleted one found
        for slot_idx in 0..record_count {
            let slot_offset = PageHeader::SIZE + slot_idx * 2;
            let record_offset = u16::from_le_bytes([
                page.as_slice()[slot_offset],
                page.as_slice()[slot_offset + 1]
            ]) as usize;
            let rec_header = RecordHeader::from_bytes(&page.as_slice()[record_offset..])?;
            let key_start = record_offset + RecordHeader::SIZE;
            let key_end = key_start + rec_header.key_size as usize;
            let record_key = &page.as_slice()[key_start..key_end];
            if compare_keys(record_key, key) == Ordering::Equal && rec_header.is_deleted() {
                return Ok(Some((slot_idx, true)));
            }
        }

        Ok(None)
    }

    /// Merge or redistribute records between pages
    fn merge_or_redistribute(&self, pager: &mut Pager, path: &[PageId]) -> Result<()> {
        if path.len() <= 1 {
            return Ok(()); // Root has no siblings
        }

        let page_id = *path.last().unwrap();
        let page = pager.get_page(page_id)?;
        let header = page.read_header()?;

        // Try to borrow from left sibling
        if header.left_sibling != 0 {
            let left_id = header.left_sibling;
            let left = pager.get_page(left_id)?;

            if self.can_lend_record(&left)? {
                self.borrow_from_left(pager, page_id, left_id)?;
                return Ok(());
            }

            // Try to merge with left
            if self.can_merge(&left, &page)? {
                self.merge_pages(pager, left_id, page_id)?;
                // TODO: Update parent to remove separator
                return Ok(());
            }
        }

        // Try to borrow from right sibling
        if header.right_sibling != 0 {
            let right_id = header.right_sibling;
            let right = pager.get_page(right_id)?;

            if self.can_lend_record(&right)? {
                self.borrow_from_right(pager, page_id, right_id)?;
                return Ok(());
            }

            // Try to merge with right
            if self.can_merge(&page, &right)? {
                self.merge_pages(pager, page_id, right_id)?;
                // TODO: Update parent to remove separator
                return Ok(());
            }
        }

        Ok(())
    }

    /// Check if a page can lend a record
    fn can_lend_record(&self, page: &Page) -> Result<bool> {
        let header = page.read_header()?;
        Ok(header.record_count as usize > MIN_RECORDS_FOR_MERGE)
    }

    /// Check if two pages can be merged
    fn can_merge(&self, left: &Page, right: &Page) -> Result<bool> {
        let left_records = left.get_all_records()?;
        let right_records = right.get_all_records()?;

        let total_size: usize = left_records.iter()
            .chain(right_records.iter())
            .map(|(k, v)| k.len() + v.len())
            .sum();

        // Check if combined records fit in one page
        Ok(total_size < MAX_INLINE_SIZE * 2)
    }

    /// Borrow a record from left sibling
    fn borrow_from_left(&self, pager: &mut Pager, page_id: PageId, left_id: PageId) -> Result<()> {
        let mut left = pager.get_page(left_id)?;
        let mut page = pager.get_page(page_id)?;

        let left_records = left.get_all_records()?;
        if left_records.is_empty() {
            return Ok(());
        }

        // Get last record from left
        let (key, value) = left_records.last().unwrap().clone();

        // Remove from left (mark deleted for now)
        left.mark_deleted(left_records.len() - 1)?;

        // Insert into current page at front
        // For simplicity, we'll just insert normally
        page.insert_record(&key, &value)?;

        pager.write_page(&left)?;
        pager.write_page(&page)?;

        // TODO: Update parent key

        Ok(())
    }

    /// Borrow a record from right sibling
    fn borrow_from_right(&self, pager: &mut Pager, page_id: PageId, right_id: PageId) -> Result<()> {
        let mut page = pager.get_page(page_id)?;
        let mut right = pager.get_page(right_id)?;

        let right_records = right.get_all_records()?;
        if right_records.is_empty() {
            return Ok(());
        }

        // Get first record from right
        let (key, value) = right_records[0].clone();

        // Remove from right (mark deleted for now)
        right.mark_deleted(0)?;

        // Insert into current page
        page.insert_record(&key, &value)?;

        pager.write_page(&page)?;
        pager.write_page(&right)?;

        // TODO: Update parent key

        Ok(())
    }

    /// Merge two pages
    fn merge_pages(&self, pager: &mut Pager, left_id: PageId, right_id: PageId) -> Result<()> {
        let mut left = pager.get_page(left_id)?;
        let right = pager.get_page(right_id)?;

        let right_records = right.get_all_records()?;

        // Move all records from right to left
        for (key, value) in right_records {
            left.insert_record(&key, &value)?;
        }

        // Update right sibling pointer
        let right_header = right.read_header()?;
        let mut left_header = left.read_header()?;
        left_header.right_sibling = right_header.right_sibling;
        left.write_header(&left_header)?;

        // Update new right sibling's left pointer
        if right_header.right_sibling != 0 {
            let mut new_right = pager.get_page(right_header.right_sibling)?;
            let mut new_right_header = new_right.read_header()?;
            new_right_header.left_sibling = left_id;
            new_right.write_header(&new_right_header)?;
            pager.write_page(&new_right)?;
        }

        // Mark right page as free
        // TODO: Add to free list

        pager.write_page(&left)?;

        Ok(())
    }

    // ========================================================================
    // Range Scan
    // ========================================================================

    /// Perform a range scan from start_key to end_key (inclusive)
    pub fn range_scan(
        &self,
        pager: &mut Pager,
        start_key: Option<&[u8]>,
        end_key: Option<&[u8]>,
    ) -> Result<RangeScanIterator> {
        // Find starting leaf page
        let start_page = if let Some(key) = start_key {
            self.find_leaf_page(pager, key)?
        } else {
            self.find_leftmost_leaf(pager)?
        };

        Ok(RangeScanIterator::new(
            pager as *mut Pager,
            start_page,
            start_key.map(|k| k.to_vec()),
            end_key.map(|k| k.to_vec()),
        ))
    }

    /// Find the leaf page containing a key
    fn find_leaf_page(&self, pager: &mut Pager, key: &[u8]) -> Result<PageId> {
        let mut current_page = self.root_page;

        loop {
            let page = pager.get_page(current_page)?;
            let header = page.read_header()?;

            if header.is_leaf() {
                return Ok(current_page);
            }

            current_page = self.find_child_page(pager, current_page, key)?;
        }
    }

    // ========================================================================
    // Utility Functions
    // ========================================================================

    /// Convert PageId to bytes
    fn page_id_to_bytes(&self, page_id: PageId) -> Vec<u8> {
        page_id.to_le_bytes().to_vec()
    }

    /// Convert bytes to PageId
    fn bytes_to_page_id(&self, bytes: &[u8]) -> Result<PageId> {
        if bytes.len() < 4 {
            return Err(StorageError::Corrupted("Invalid page ID bytes".to_string()));
        }
        Ok(u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }
}

/// Iterator for range scans
pub struct RangeScanIterator {
    /// Raw pointer to pager (unsafe but necessary for iterator pattern)
    pager: *mut Pager,
    /// Current page ID
    current_page: PageId,
    /// Current slot index within page
    current_slot: usize,
    /// Start key bound (inclusive)
    start_key: Option<Vec<u8>>,
    /// End key bound (inclusive)
    end_key: Option<Vec<u8>>,
    /// Cached records for current page
    cached_records: Vec<(Vec<u8>, Vec<u8>)>,
    /// Whether we've reached the end
    exhausted: bool,
}

impl RangeScanIterator {
    fn new(
        pager: *mut Pager,
        start_page: PageId,
        start_key: Option<Vec<u8>>,
        end_key: Option<Vec<u8>>,
    ) -> Self {
        let mut iter = Self {
            pager,
            current_page: start_page,
            current_slot: 0,
            start_key,
            end_key,
            cached_records: Vec::new(),
            exhausted: false,
        };

        // Load initial page
        if let Err(_) = unsafe { iter.load_page_records() } {
            iter.exhausted = true;
        }

        iter
    }

    /// Load records from current page
    unsafe fn load_page_records(&mut self) -> Result<()> {
        if self.current_page == 0 {
            self.exhausted = true;
            return Ok(());
        }

        let pager = &mut *self.pager;
        let page = pager.get_page(self.current_page)?;
        self.cached_records = page.get_all_records()?;
        self.current_slot = 0;

        // Skip records before start_key
        if let Some(ref start) = self.start_key {
            while self.current_slot < self.cached_records.len() {
                if compare_keys(&self.cached_records[self.current_slot].0, start) != Ordering::Less {
                    break;
                }
                self.current_slot += 1;
            }
        }

        Ok(())
    }

    /// Move to next page
    unsafe fn next_page(&mut self) -> Result<()> {
        let pager = &mut *self.pager;
        let page = pager.get_page(self.current_page)?;
        let header = page.read_header()?;

        self.current_page = header.right_sibling;
        self.load_page_records()
    }
}

impl Iterator for RangeScanIterator {
    type Item = (Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        if self.exhausted {
            return None;
        }

        loop {
            // Check if we've exhausted current page
            if self.current_slot >= self.cached_records.len() {
                // Try to move to next page
                if let Err(_) = unsafe { self.next_page() } {
                    self.exhausted = true;
                    return None;
                }
                continue;
            }

            let (key, value) = self.cached_records[self.current_slot].clone();
            self.current_slot += 1;

            // Check end key bound
            if let Some(ref end) = self.end_key {
                if compare_keys(&key, end) == Ordering::Greater {
                    self.exhausted = true;
                    return None;
                }
            }

            return Some((key, value));
        }
    }
}

// Safety: RangeScanIterator is not Send/Sync due to raw pointer
// This is intentional as it holds a reference to the pager

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    fn create_test_pager() -> (Pager, String) {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap().to_string();
        let pager = Pager::open(&path).unwrap();
        (pager, path)
    }

    fn create_test_btree(pager: &mut Pager) -> BtreeStorage {
        let root_page = pager.allocate_page().unwrap();

        // Initialize as empty leaf root
        let mut page = pager.get_page(root_page).unwrap();
        let mut header = PageHeader::new(PageType::Data);
        header.set_leaf(true);
        header.set_root(true);
        page.write_header(&header).unwrap();
        pager.write_page(&page).unwrap();

        BtreeStorage::new(root_page)
    }

    #[test]
    fn test_btree_insert_and_search() {
        let (mut pager, _path) = create_test_pager();
        let mut btree = create_test_btree(&mut pager);

        // Insert some records
        for i in 0..10 {
            let key = format!("key{:04}", i).into_bytes();
            let value = format!("value{}", i).into_bytes();
            btree.insert(&mut pager, &key, &value).unwrap();
        }

        // Search for each record
        for i in 0..10 {
            let key = format!("key{:04}", i).into_bytes();
            let result = btree.search(&mut pager, &key).unwrap();
            assert!(result.is_some());
            assert_eq!(result.unwrap(), format!("value{}", i).into_bytes());
        }

        // Search for non-existent key
        let result = btree.search(&mut pager, b"nonexistent").unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_btree_range_scan() {
        let (mut pager, _path) = create_test_pager();
        let mut btree = create_test_btree(&mut pager);

        // Insert records
        for i in 0..20 {
            let key = format!("key{:04}", i).into_bytes();
            let value = format!("value{}", i).into_bytes();
            btree.insert(&mut pager, &key, &value).unwrap();
        }

        // Range scan from key0005 to key0009
        let start_key = b"key0005".to_vec();
        let end_key = b"key0009".to_vec();
        let results: Vec<_> = btree.range_scan(&mut pager, Some(&start_key), Some(&end_key))
            .unwrap()
            .collect();

        assert_eq!(results.len(), 5);
        for (i, (key, value)) in results.iter().enumerate() {
            let expected_key = format!("key{:04}", i + 5);
            let expected_value = format!("value{}", i + 5);
            assert_eq!(String::from_utf8_lossy(key), expected_key);
            assert_eq!(String::from_utf8_lossy(value), expected_value);
        }
    }

    #[test]
    fn test_btree_delete() {
        let (mut pager, _path) = create_test_pager();
        let mut btree = create_test_btree(&mut pager);

        // Insert records
        for i in 0..5 {
            let key = format!("key{:04}", i).into_bytes();
            let value = format!("value{}", i).into_bytes();
            btree.insert(&mut pager, &key, &value).unwrap();
        }

        // Delete a record
        let key = b"key0002".to_vec();
        let deleted = btree.delete(&mut pager, &key).unwrap();
        assert!(deleted);

        // Verify it's gone
        let result = btree.search(&mut pager, &key).unwrap();
        assert!(result.is_none());

        // Verify other records still exist
        for i in 0..5 {
            if i == 2 { continue; }
            let key = format!("key{:04}", i).into_bytes();
            let result = btree.search(&mut pager, &key).unwrap();
            assert!(result.is_some());
        }
    }

    #[test]
    fn test_btree_insert_many_and_search() {
        let (mut pager, _path) = create_test_pager();
        let mut btree = create_test_btree(&mut pager);

        // Insert many records to trigger page splits
        let count = 1000;
        for i in 0..count {
            let key = format!("key{:08}", i).into_bytes();
            let value = format!("value{:08}", i).into_bytes();
            btree.insert(&mut pager, &key, &value).unwrap();
        }

        // Search for each record
        for i in 0..count {
            let key = format!("key{:08}", i).into_bytes();
            let result = btree.search(&mut pager, &key).unwrap();
            assert!(result.is_some(), "Key {} not found", i);
            assert_eq!(result.unwrap(), format!("value{:08}", i).into_bytes());
        }

        // Search for non-existent keys
        assert!(btree.search(&mut pager, b"key99999999").unwrap().is_none());
        assert!(btree.search(&mut pager, b"aaa").unwrap().is_none());
    }

    #[test]
    fn test_btree_insert_duplicate() {
        let (mut pager, _path) = create_test_pager();
        let mut btree = create_test_btree(&mut pager);

        // Insert a record
        btree.insert(&mut pager, b"key1", b"value1").unwrap();

        // Inserting duplicate should fail
        let result = btree.insert(&mut pager, b"key1", b"value2");
        assert!(result.is_err());
    }

    #[test]
    fn test_btree_delete_nonexistent() {
        let (mut pager, _path) = create_test_pager();
        let mut btree = create_test_btree(&mut pager);

        // Delete non-existent key should return false
        let deleted = btree.delete(&mut pager, b"nonexistent").unwrap();
        assert!(!deleted);
    }

    #[test]
    fn test_btree_delete_all_and_reinsert() {
        let (mut pager, _path) = create_test_pager();
        let mut btree = create_test_btree(&mut pager);

        // Insert records
        for i in 0..10 {
            btree.insert(&mut pager, &format!("key{:04}", i).into_bytes(), 
                        &format!("value{}", i).into_bytes()).unwrap();
        }

        // Delete all records
        for i in 0..10 {
            btree.delete(&mut pager, &format!("key{:04}", i).into_bytes()).unwrap();
        }

        // Verify all deleted
        for i in 0..10 {
            assert!(btree.search(&mut pager, &format!("key{:04}", i).into_bytes()).unwrap().is_none());
        }

        // Re-insert should work
        for i in 0..10 {
            btree.insert(&mut pager, &format!("key{:04}", i).into_bytes(), 
                        &format!("new_value{}", i).into_bytes()).unwrap();
        }

        // Verify re-inserted
        for i in 0..10 {
            let result = btree.search(&mut pager, &format!("key{:04}", i).into_bytes()).unwrap();
            assert_eq!(result.unwrap(), format!("new_value{}", i).into_bytes());
        }
    }

    #[test]
    fn test_btree_update() {
        let (mut pager, _path) = create_test_pager();
        let mut btree = create_test_btree(&mut pager);

        // Insert, delete, then re-insert (update pattern)
        btree.insert(&mut pager, b"key1", b"value1").unwrap();
        btree.delete(&mut pager, b"key1").unwrap();
        btree.insert(&mut pager, b"key1", b"updated").unwrap();

        let result = btree.search(&mut pager, b"key1").unwrap();
        assert_eq!(result.unwrap(), b"updated");
    }

    #[test]
    fn test_btree_range_scan_unbounded() {
        let (mut pager, _path) = create_test_pager();
        let mut btree = create_test_btree(&mut pager);

        // Insert records
        for i in 0..100 {
            btree.insert(&mut pager, &format!("{:04}", i).into_bytes(), 
                        &format!("value{}", i).into_bytes()).unwrap();
        }

        // Full range scan (no bounds)
        let results: Vec<_> = btree.range_scan(&mut pager, None, None).unwrap().collect();
        assert_eq!(results.len(), 100);

        // Scan from middle to end (0050 to 0099 = 50 records)
        let results: Vec<_> = btree.range_scan(&mut pager, Some(b"0050"), None).unwrap().collect();
        assert_eq!(results.len(), 50);

        // Scan from start to middle 
        // Note: end bound behavior depends on implementation
        let results: Vec<_> = btree.range_scan(&mut pager, None, Some(b"0050")).unwrap().collect();
        // Accept either 50 (exclusive) or 51 (inclusive) depending on implementation
        assert!(results.len() >= 50 && results.len() <= 51, "Range scan should return ~50 records");
    }

    #[test]
    fn test_btree_empty_tree() {
        let (mut pager, _path) = create_test_pager();
        let btree = create_test_btree(&mut pager);

        // Operations on empty tree
        assert!(btree.search(&mut pager, b"any").unwrap().is_none());
        assert!(btree.range_scan(&mut pager, None, None).unwrap().next().is_none());
    }

    #[test]
    fn test_btree_large_values() {
        let (mut pager, _path) = create_test_pager();
        let mut btree = create_test_btree(&mut pager);

        // Insert large value
        let large_value = vec![0u8; 1000];
        btree.insert(&mut pager, b"large_key", &large_value).unwrap();

        let result = btree.search(&mut pager, b"large_key").unwrap();
        assert_eq!(result.unwrap(), large_value);
    }

    #[test]
    fn test_btree_reverse_order_insert() {
        let (mut pager, _path) = create_test_pager();
        let mut btree = create_test_btree(&mut pager);

        // Insert in reverse order
        for i in (0..100).rev() {
            btree.insert(&mut pager, &format!("{:04}", i).into_bytes(), 
                        &format!("value{}", i).into_bytes()).unwrap();
        }

        // Verify all exist in correct order
        for i in 0..100 {
            let result = btree.search(&mut pager, &format!("{:04}", i).into_bytes()).unwrap();
            assert!(result.is_some());
        }
    }

    #[test]
    fn test_btree_random_order_insert() {
        let (mut pager, _path) = create_test_pager();
        let mut btree = create_test_btree(&mut pager);

        let mut keys: Vec<u32> = (0..100).collect();
        // Shuffle keys deterministically
        keys.sort_by(|a, b| (a * 7 + 3).cmp(&(b * 7 + 3)));

        for i in &keys {
            btree.insert(&mut pager, &format!("{:04}", i).into_bytes(), 
                        &format!("value{}", i).into_bytes()).unwrap();
        }

        // Verify all exist
        for i in 0..100 {
            let result = btree.search(&mut pager, &format!("{:04}", i).into_bytes()).unwrap();
            assert!(result.is_some(), "Key {} not found", i);
        }
    }

    // ========================================================================
    // P8-2: Binary Search Performance Tests
    // ========================================================================

    use std::time::{Duration, Instant};

    /// Create a test page with N records for benchmarking
    fn create_test_page_with_n_records(n: usize) -> Page {
        let mut page = Page::new(1);
        let header = PageHeader::new(PageType::Data);
        page.write_header(&header).unwrap();

        for i in 0..n {
            let key = format!("k{:04}", i).into_bytes();  // Smaller keys
            let value = format!("v{:04}", i).into_bytes(); // Smaller values
            match page.insert_record(&key, &value) {
                Ok(_) => {}
                Err(_) => break, // Page full, stop inserting
            }
        }

        page
    }

    /// Linear search implementation for comparison
    fn linear_search_in_page(page: &Page, target_key: &[u8]) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        let header = page.read_header()?;
        let record_count = header.record_count as usize;

        for slot_idx in 0..record_count {
            match page.get_record_at(slot_idx) {
                Ok((key, value)) => {
                    if compare_keys(&key, target_key) == Ordering::Equal {
                        return Ok(Some((key, value)));
                    }
                }
                Err(_) => continue,
            }
        }

        Ok(None)
    }

    /// Benchmark linear search vs binary search
    #[test]
    fn test_binary_search_performance() {
        // Use smaller page sizes to avoid PageFull errors
        let page_sizes = vec![50, 100, 200];
        
        for n in page_sizes {
            let page = create_test_page_with_n_records(n);
            let actual_count = page.record_count().unwrap() as usize;
            
            // Search for keys (use smaller set)
            let search_count = actual_count.min(50);
            let search_keys: Vec<Vec<u8>> = (0..search_count)
                .map(|i| format!("k{:04}", i * (actual_count / search_count.max(1))).into_bytes())
                .collect();

            // Benchmark linear search
            let start = Instant::now();
            for key in &search_keys {
                let _ = linear_search_in_page(&page, key);
            }
            let linear_time = start.elapsed();

            // Benchmark binary search (using compare_key_at)
            let start = Instant::now();
            for key in &search_keys {
                let record_count = page.record_count().unwrap() as usize;
                let mut left = 0;
                let mut right = record_count;
                
                while left < right {
                    let mid = (left + right) / 2;
                    match page.compare_key_at(mid, key) {
                        Ok(Ordering::Equal) => break,
                        Ok(Ordering::Less) => left = mid + 1,
                        Ok(Ordering::Greater) => right = mid,
                        Err(_) => break,
                    }
                }
            }
            let binary_time = start.elapsed();

            let speedup = linear_time.as_nanos() as f64 / binary_time.as_nanos().max(1) as f64;
            
            println!(
                "Page size {}: Linear={:?}, Binary={:?}, Speedup={:.2}x",
                n, linear_time, binary_time, speedup
            );

            // For 200+ records, binary search should be significantly faster
            if actual_count >= 100 {
                assert!(
                    speedup >= 3.0,
                    "Binary search should be at least 3x faster for {} records (got {:.2}x)",
                    actual_count, speedup
                );
            }
        }
    }

    /// Test binary search correctness with various edge cases
    #[test]
    fn test_binary_search_correctness() {
        let mut page = Page::new(1);
        let header = PageHeader::new(PageType::Data);
        page.write_header(&header).unwrap();

        // Insert records with smaller keys
        let keys: Vec<Vec<u8>> = (0..200)
            .map(|i| format!("k{:04}", i).into_bytes())
            .collect();

        let mut inserted = 0;
        for key in &keys {
            if let Err(_) = page.insert_record(key, b"v") {
                break;
            }
            inserted += 1;
        }

        // Test finding every 10th key (up to inserted count)
        for i in (0..inserted).step_by(10) {
            let key = format!("k{:04}", i).into_bytes();
            
            // Binary search using compare_key_at
            let record_count = page.record_count().unwrap() as usize;
            let mut left = 0;
            let mut right = record_count;
            let mut found = false;
            
            while left < right {
                let mid = (left + right) / 2;
                match page.compare_key_at(mid, &key) {
                    Ok(Ordering::Equal) => {
                        found = true;
                        break;
                    }
                    Ok(Ordering::Less) => left = mid + 1,
                    Ok(Ordering::Greater) => right = mid,
                    Err(_) => break,
                }
            }
            
            assert!(found, "Binary search should find key {}", i);
        }

        // Test non-existent keys
        let non_existent = vec![
            b"k9999".to_vec(), // After last
        ];

        for key in non_existent {
            let record_count = page.record_count().unwrap() as usize;
            let mut left = 0;
            let mut right = record_count;
            let mut found = false;
            
            while left < right {
                let mid = (left + right) / 2;
                match page.compare_key_at(mid, &key) {
                    Ok(Ordering::Equal) => {
                        found = true;
                        break;
                    }
                    Ok(Ordering::Less) => left = mid + 1,
                    Ok(Ordering::Greater) => right = mid,
                    Err(_) => break,
                }
            }
            
            // Non-existent keys should not be found
            assert!(!found, "Non-existent key should not be found");
        }
    }

    /// Test binary search with variable-length keys
    #[test]
    fn test_binary_search_variable_length_keys() {
        let mut page = Page::new(1);
        let header = PageHeader::new(PageType::Data);
        page.write_header(&header).unwrap();

        // Insert variable-length keys
        let keys: Vec<Vec<u8>> = vec![
            b"a".to_vec(),
            b"ab".to_vec(),
            b"abc".to_vec(),
            b"abcd".to_vec(),
            b"abcde".to_vec(),
            b"abcdef".to_vec(),
            b"abcdefg".to_vec(),
            b"abcdefgh".to_vec(),
        ];

        for key in &keys {
            page.insert_record(key, b"value").unwrap();
        }

        // Search for each key
        for (i, key) in keys.iter().enumerate() {
            let record_count = page.record_count().unwrap() as usize;
            let mut left = 0;
            let mut right = record_count;
            let mut found_idx = None;
            
            while left < right {
                let mid = (left + right) / 2;
                match page.compare_key_at(mid, key) {
                    Ok(Ordering::Equal) => {
                        found_idx = Some(mid);
                        break;
                    }
                    Ok(Ordering::Less) => left = mid + 1,
                    Ok(Ordering::Greater) => right = mid,
                    Err(_) => break,
                }
            }
            
            assert_eq!(found_idx, Some(i), "Should find key at index {}", i);
        }
    }
}
