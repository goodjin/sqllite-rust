//! B-tree Storage Engine - Phase 4: Integration with Existing System
//!
//! This module integrates the B-tree storage engine with the existing
//! SQL execution layer, replacing the single-page heap storage.

use crate::sql::ast::{ColumnDef, DataType, ForeignKeyAction, SelectStmt};
use crate::storage::{Record, Result, StorageError, BPlusTreeIndex, Value};
use crate::storage::btree_engine::{PageHeader, PageType};
use crate::storage::btree_core::BtreeStorage;
use crate::storage::overflow::OverflowManager;
use crate::storage::foreign_key::{ForeignKeyConstraint, ForeignKeyChecker};
use crate::pager::{PageId, Pager};
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct HnswIndexMetadata {
    pub name: String,
    pub column: String,
    pub root_page: PageId,
    pub dimension: usize,
}

/// View metadata storage
#[derive(Debug, Clone)]
pub struct ViewMetadata {
    pub name: String,
    pub columns: Vec<String>,
    pub definition: String,  // SQL 原文
    pub parsed_query: SelectStmt,
}

impl ViewMetadata {
    /// Serialize view metadata
    pub fn serialize(&self) -> Vec<u8> {
        let mut data = Vec::new();
        
        // Name length + name
        let name_bytes = self.name.as_bytes();
        data.extend_from_slice(&(name_bytes.len() as u32).to_be_bytes());
        data.extend_from_slice(name_bytes);
        
        // Columns count + each column
        data.extend_from_slice(&(self.columns.len() as u32).to_be_bytes());
        for col in &self.columns {
            let col_bytes = col.as_bytes();
            data.extend_from_slice(&(col_bytes.len() as u32).to_be_bytes());
            data.extend_from_slice(col_bytes);
        }
        
        // Definition length + definition
        let def_bytes = self.definition.as_bytes();
        data.extend_from_slice(&(def_bytes.len() as u32).to_be_bytes());
        data.extend_from_slice(def_bytes);
        
        data
    }
    
    /// Deserialize view metadata (query needs to be reparsed)
    pub fn deserialize(data: &[u8], definition: &str, parsed_query: SelectStmt) -> Result<Self> {
        let mut pos = 0;
        
        // Read name
        let name_len = u32::from_be_bytes([data[pos], data[pos+1], data[pos+2], data[pos+3]]) as usize;
        pos += 4;
        let name = String::from_utf8_lossy(&data[pos..pos+name_len]).to_string();
        pos += name_len;
        
        // Read columns
        let col_count = u32::from_be_bytes([data[pos], data[pos+1], data[pos+2], data[pos+3]]) as usize;
        pos += 4;
        let mut columns = Vec::new();
        for _ in 0..col_count {
            let col_len = u32::from_be_bytes([data[pos], data[pos+1], data[pos+2], data[pos+3]]) as usize;
            pos += 4;
            let col = String::from_utf8_lossy(&data[pos..pos+col_len]).to_string();
            pos += col_len;
            columns.push(col);
        }
        
        Ok(Self {
            name,
            columns,
            definition: definition.to_string(),
            parsed_query,
        })
    }
}

/// Table metadata for B-tree storage
#[derive(Debug, Clone)]
pub struct BtreeTable {
    pub name: String,
    pub columns: Vec<ColumnDef>,
    pub root_page: PageId,
    pub next_rowid: u64,
    pub hnsw_indices: Vec<HnswIndexMetadata>,
    pub foreign_keys: Vec<ForeignKeyConstraint>,
}

impl BtreeTable {
    pub fn new(name: String, columns: Vec<ColumnDef>, root_page: PageId) -> Self {
        Self {
            name,
            columns,
            root_page,
            next_rowid: 1,
            hnsw_indices: Vec::new(),
            foreign_keys: Vec::new(),
        }
    }

    /// Get column index by name
    pub fn column_index(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|c| c.name == name)
    }

    /// Serialize table metadata
    pub fn serialize(&self) -> Vec<u8> {
        let mut data = Vec::new();

        // Table name length + name
        let name_bytes = self.name.as_bytes();
        data.extend_from_slice(&(name_bytes.len() as u32).to_be_bytes());
        data.extend_from_slice(name_bytes);

        // Root page ID
        data.extend_from_slice(&self.root_page.to_be_bytes());

        // Next row ID
        data.extend_from_slice(&self.next_rowid.to_be_bytes());

        // Column count
        data.push(self.columns.len() as u8);

        // Each column info
        for col in &self.columns {
            // Column name length + name
            let col_name_bytes = col.name.as_bytes();
            data.extend_from_slice(&(col_name_bytes.len() as u32).to_be_bytes());
            data.extend_from_slice(col_name_bytes);

            // Data type
            match col.data_type {
                DataType::Integer => data.push(1),
                DataType::Text => data.push(2),
                DataType::Blob => data.push(4),
                DataType::Json => data.push(6),
                DataType::Vector(dim) => {
                    data.push(5);
                    data.extend_from_slice(&dim.to_be_bytes());
                }
            }
        }

        // Vector index count
        data.push(self.hnsw_indices.len() as u8);
        for idx in &self.hnsw_indices {
            let name_bytes = idx.name.as_bytes();
            data.extend_from_slice(&(name_bytes.len() as u32).to_be_bytes());
            data.extend_from_slice(name_bytes);
            
            let col_bytes = idx.column.as_bytes();
            data.extend_from_slice(&(col_bytes.len() as u32).to_be_bytes());
            data.extend_from_slice(col_bytes);

            data.extend_from_slice(&idx.root_page.to_be_bytes());
            data.extend_from_slice(&(idx.dimension as u32).to_be_bytes());
        }

        data
    }

    /// Deserialize table metadata
    pub fn deserialize(data: &[u8]) -> Result<Self> {
        let mut pos = 0;

        // Read table name
        let name_len = u32::from_be_bytes([data[pos], data[pos+1], data[pos+2], data[pos+3]]) as usize;
        pos += 4;
        let name = String::from_utf8_lossy(&data[pos..pos+name_len]).to_string();
        pos += name_len;

        // Read root page ID
        let root_page = u32::from_be_bytes([data[pos], data[pos+1], data[pos+2], data[pos+3]]);
        pos += 4;

        // Read next row ID
        let next_rowid = u64::from_be_bytes([
            data[pos], data[pos+1], data[pos+2], data[pos+3],
            data[pos+4], data[pos+5], data[pos+6], data[pos+7]
        ]);
        pos += 8;

        // Read column count
        let col_count = data[pos] as usize;
        pos += 1;

        // Read each column
        let mut columns = Vec::new();
        for _ in 0..col_count {
            let col_name_len = u32::from_be_bytes([data[pos], data[pos+1], data[pos+2], data[pos+3]]) as usize;
            pos += 4;
            let col_name = String::from_utf8_lossy(&data[pos..pos+col_name_len]).to_string();
            pos += col_name_len;

            let data_type = match data[pos] {
                1 => { pos += 1; DataType::Integer },
                2 => { pos += 1; DataType::Text },
                4 => { pos += 1; DataType::Blob },
                5 => {
                    pos += 1;
                    let dim = u32::from_be_bytes([data[pos], data[pos+1], data[pos+2], data[pos+3]]);
                    pos += 4;
                    DataType::Vector(dim)
                }
                _ => { pos += 1; DataType::Text },
            };

            columns.push(ColumnDef {
                name: col_name,
                data_type,
                nullable: true,
                primary_key: false,
                foreign_key: None,
                default_value: None,
                is_virtual: false,
                generated_always: None,

            });
        }

        // Read HNSW index count
        let mut hnsw_indices = Vec::new();
        if pos < data.len() {
            let hnsw_count = data[pos] as usize;
            pos += 1;

            for _ in 0..hnsw_count {
                // Name
                let name_len = u32::from_be_bytes([data[pos], data[pos+1], data[pos+2], data[pos+3]]) as usize;
                pos += 4;
                let name = String::from_utf8_lossy(&data[pos..pos+name_len]).to_string();
                pos += name_len;

                // Column
                let col_len = u32::from_be_bytes([data[pos], data[pos+1], data[pos+2], data[pos+3]]) as usize;
                pos += 4;
                let column = String::from_utf8_lossy(&data[pos..pos+col_len]).to_string();
                pos += col_len;

                // Root Page
                let root_page = u32::from_be_bytes([data[pos], data[pos+1], data[pos+2], data[pos+3]]);
                pos += 4;

                // Dimension
                let dimension = u32::from_be_bytes([data[pos], data[pos+1], data[pos+2], data[pos+3]]) as usize;
                pos += 4;

                hnsw_indices.push(HnswIndexMetadata { name, column, root_page, dimension });
            }
        }

        Ok(Self {
            name,
            columns,
            root_page,
            next_rowid,
            hnsw_indices,
            foreign_keys: Vec::new(),
        })
    }
}

/// B-tree based database - production-ready storage engine
pub struct BtreeDatabase {
    pager: Pager,
    tables: HashMap<String, BtreeTable>,
    views: HashMap<String, ViewMetadata>,
    btree_storages: HashMap<String, BtreeStorage>,
    indexes: HashMap<String, BPlusTreeIndex>,
    hnsw_indexes: HashMap<String, crate::index::HnswIndex>,
    schema_page: PageId,
    view_schema_page: PageId, // Page for storing view metadata
    _overflow_mgr: OverflowManager,
}

impl BtreeDatabase {
    /// Open or create a B-tree database
    pub fn open(path: &str) -> Result<Self> {
        let mut pager = Pager::open(path)?;
        let schema_page = 1; // Page 1 stores schema

        let mut tables = HashMap::new();
        let mut btree_storages = HashMap::new();
        let mut hnsw_indexes = HashMap::new();

        // Try to load existing tables
        if pager.header().database_size > schema_page {
            if let Ok(page) = pager.get_page(schema_page) {
                let table_count = page.as_slice()[0] as usize;
                let mut pos = 1;

                for _ in 0..table_count {
                    let table_data_len = u32::from_be_bytes([
                        page.as_slice()[pos], page.as_slice()[pos+1],
                        page.as_slice()[pos+2], page.as_slice()[pos+3]
                    ]) as usize;
                    pos += 4;

                    let table_data = &page.as_slice()[pos..pos+table_data_len];
                    if let Ok(table) = BtreeTable::deserialize(table_data) {
                        let btree = BtreeStorage::new(table.root_page);
                        btree_storages.insert(table.name.clone(), btree);
                        
                        // Load HNSW indices
                        for idx_meta in &table.hnsw_indices {
                            let hnsw = crate::index::HnswIndex::new(
                                idx_meta.name.clone(),
                                table.name.clone(),
                                idx_meta.column.clone(),
                                idx_meta.root_page,
                                idx_meta.dimension,
                            );
                            hnsw_indexes.insert(idx_meta.name.clone(), hnsw);
                        }

                        tables.insert(table.name.clone(), table);
                    }
                    pos += table_data_len;
                }
            }
        }

        // Note: Views are currently stored in memory only and need to be recreated after restart
        let views = HashMap::new();
        let view_schema_page = 2; // Page 2 reserved for view schema

        Ok(Self {
            pager,
            tables,
            views,
            btree_storages,
            indexes: HashMap::new(),
            hnsw_indexes,
            schema_page,
            view_schema_page,
            _overflow_mgr: OverflowManager::new(),
        })
    }

    /// Create a new view
    pub fn create_view(&mut self, view: ViewMetadata) -> Result<()> {
        if self.views.contains_key(&view.name) {
            return Err(StorageError::DuplicateKey);
        }
        
        // Also check that it doesn't conflict with a table name
        if self.tables.contains_key(&view.name) {
            return Err(StorageError::Other(
                format!("'{}' is already a table name", view.name)
            ));
        }
        
        self.views.insert(view.name.clone(), view);
        
        // Note: View persistence to disk is not implemented yet
        // In a full implementation, we would save to view_schema_page
        
        Ok(())
    }

    /// Get view metadata
    pub fn get_view(&self, name: &str) -> Option<&ViewMetadata> {
        self.views.get(name)
    }

    /// Drop a view
    pub fn drop_view(&mut self, name: &str) -> Result<()> {
        if self.views.remove(name).is_none() {
            return Err(StorageError::KeyNotFound);
        }
        Ok(())
    }

    /// List all views
    pub fn list_views(&self) -> Vec<&String> {
        self.views.keys().collect()
    }

    /// Create a new table
    pub fn create_table(&mut self, name: String, columns: Vec<ColumnDef>) -> Result<()> {
        self.create_table_with_fk(name, columns, Vec::new())
    }

    /// Create table with foreign key constraints
    pub fn create_table_with_fk(
        &mut self,
        name: String,
        columns: Vec<ColumnDef>,
        table_fks: Vec<crate::sql::ast::ForeignKeyDef>
    ) -> Result<()> {
        if self.tables.contains_key(&name) {
            return Err(StorageError::DuplicateKey);
        }

        // Allocate root page for the table (skip page 1 which stores schema)
        let root_page = self.pager.allocate_page()?;
        let root_page = if root_page == 1 {
            self.pager.allocate_page()?
        } else {
            root_page
        };

        // Initialize root page as empty leaf
        let mut page = self.pager.get_page(root_page)?;
        let mut header = PageHeader::new(PageType::Data);
        header.set_leaf(true);
        header.set_root(true);

        // Write header using the trait method
        use crate::storage::btree_engine::BtreePageOps;
        page.write_header(&header)?;
        self.pager.write_page(&page)?;

        // Create table metadata
        let mut table = BtreeTable::new(name.clone(), columns, root_page);
        
        // Convert AST foreign key definitions to storage foreign key constraints
        for fk_def in table_fks {
            table.foreign_keys.push(ForeignKeyConstraint {
                name: None,
                columns: fk_def.columns,
                ref_table: fk_def.ref_table,
                ref_columns: fk_def.ref_columns,
                on_delete: fk_def.on_delete,
                on_update: fk_def.on_update,
                deferrable: false,
                initially_deferred: false,
            });
        }

        // Create B-tree storage
        let btree = BtreeStorage::new(root_page);

        self.tables.insert(name.clone(), table);
        self.btree_storages.insert(name, btree);

        // Persist schema
        self.save_schema()?;

        Ok(())
    }

    /// Drop a table
    pub fn drop_table(&mut self, name: &str) -> Result<()> {
        if self.tables.remove(name).is_none() {
            return Err(StorageError::KeyNotFound);
        }
        self.btree_storages.remove(name);
        self.save_schema()?;
        Ok(())
    }

    /// ALTER TABLE ADD COLUMN
    pub fn alter_table_add_column(&mut self, table_name: &str, column: ColumnDef) -> Result<()> {
        let table = self.get_table_mut(table_name)
            .ok_or(StorageError::KeyNotFound)?;
        
        // Check if column already exists
        if table.column_index(&column.name).is_some() {
            return Err(StorageError::Other(
                format!("Column '{}' already exists in table '{}'", column.name, table_name)
            ));
        }
        
        // Add the column
        table.columns.push(column);
        
        // Save schema changes
        self.save_schema()?;
        
        Ok(())
    }

    /// ALTER TABLE DROP COLUMN
    pub fn alter_table_drop_column(&mut self, table_name: &str, column_name: &str) -> Result<()> {
        let table = self.get_table_mut(table_name)
            .ok_or(StorageError::KeyNotFound)?;
        
        // Find column index
        let col_idx = table.column_index(column_name)
            .ok_or_else(|| StorageError::Other(
                format!("Column '{}' does not exist in table '{}'", column_name, table_name)
            ))?;
        
        // Remove the column
        table.columns.remove(col_idx);
        
        // Save schema changes
        self.save_schema()?;
        
        Ok(())
    }

    /// ALTER TABLE RENAME TO
    pub fn alter_table_rename(&mut self, old_name: &str, new_name: &str) -> Result<()> {
        // Check if new name already exists
        if self.tables.contains_key(new_name) {
            return Err(StorageError::Other(
                format!("Table '{}' already exists", new_name)
            ));
        }
        
        // Remove table with old name
        let mut table = self.tables.remove(old_name)
            .ok_or(StorageError::KeyNotFound)?;
        let btree = self.btree_storages.remove(old_name)
            .ok_or(StorageError::KeyNotFound)?;
        
        // Update name
        table.name = new_name.to_string();
        
        // Insert with new name
        self.tables.insert(new_name.to_string(), table);
        self.btree_storages.insert(new_name.to_string(), btree);
        
        // Save schema changes
        self.save_schema()?;
        
        Ok(())
    }

    /// ALTER TABLE RENAME COLUMN
    pub fn alter_table_rename_column(&mut self, table_name: &str, old_name: &str, new_name: &str) -> Result<()> {
        let table = self.get_table_mut(table_name)
            .ok_or(StorageError::KeyNotFound)?;
        
        // Check if old column exists
        let col_idx = table.column_index(old_name)
            .ok_or_else(|| StorageError::Other(
                format!("Column '{}' does not exist in table '{}'", old_name, table_name)
            ))?;
        
        // Check if new name already exists
        if table.column_index(new_name).is_some() {
            return Err(StorageError::Other(
                format!("Column '{}' already exists in table '{}'", new_name, table_name)
            ));
        }
        
        // Rename the column
        table.columns[col_idx].name = new_name.to_string();
        
        // Save schema changes
        self.save_schema()?;
        
        Ok(())
    }

    /// Get table metadata
    pub fn get_table(&self, name: &str) -> Option<&BtreeTable> {
        self.tables.get(name)
    }

    /// Get mutable table metadata
    pub fn get_table_mut(&mut self, name: &str) -> Option<&mut BtreeTable> {
        self.tables.get_mut(name)
    }

    /// List all tables
    pub fn list_tables(&self) -> Vec<&String> {
        self.tables.keys().collect()
    }

    /// Create an index
    pub fn create_index(&mut self, index_name: String, table_name: String, column_name: String) -> Result<()> {
        if !self.tables.contains_key(&table_name) {
            return Err(StorageError::KeyNotFound);
        }

        let index = BPlusTreeIndex::new(
            index_name.clone(),
            table_name.clone(),
            column_name.clone(),
        );

        self.indexes.insert(index_name, index);
        Ok(())
    }

    /// Create an HNSW index
    pub fn create_hnsw_index(
        &mut self,
        index_name: String,
        table_name: String,
        column_name: String,
        dimension: usize,
    ) -> Result<()> {
        if !self.tables.contains_key(&table_name) {
            return Err(StorageError::KeyNotFound);
        }

        // Allocate a root page for the HNSW index
        let root_page = self.pager.allocate_page()?;
        
        let mut index = crate::index::HnswIndex::new(
            index_name.clone(),
            table_name.clone(),
            column_name.clone(),
            root_page,
            dimension,
        );
        index.init(&mut self.pager).map_err(|e| StorageError::Other(format!("HNSW init error: {:?}", e)))?;
        
        // Populate index from existing data
        let table = self.tables.get(&table_name).unwrap();
        let btree = self.btree_storages.get(&table_name).ok_or(StorageError::KeyNotFound)?;
        let col_idx = table.column_index(&column_name).ok_or(StorageError::KeyNotFound)?;
        
        let mut curr_rowid = 1u64;
        while curr_rowid < table.next_rowid {
            let key = curr_rowid.to_be_bytes().to_vec();
            if let Ok(Some(value)) = btree.search(&mut self.pager, &key) {
                let record = Record::deserialize(&value)?;
                if let Some(Value::Vector(vec)) = record.values.get(col_idx) {
                    index.insert(&mut self.pager, vec, curr_rowid).map_err(|e| StorageError::Other(format!("HNSW insert error: {:?}", e)))?;
                }
            }
            curr_rowid += 1;
        }

        let table_mut = self.tables.get_mut(&table_name).unwrap();
        table_mut.hnsw_indices.push(HnswIndexMetadata {
            name: index_name.clone(),
            column: column_name,
            root_page,
            dimension,
        });
        
        self.hnsw_indexes.insert(index_name, index);
        self.save_schema()?;
        Ok(())
    }

    /// Search vector nearest neighbors using HNSW index
    pub fn vector_search(
        &mut self,
        index_name: &str,
        query: &[f32],
        k: usize,
    ) -> Result<Vec<(Record, f32)>> {
        let (table_name, _col_names) = {
            let index = self.hnsw_indexes.get(index_name).ok_or(StorageError::KeyNotFound)?;
            let table = self.tables.get(&index.table_name).ok_or(StorageError::KeyNotFound)?;
            (index.table_name.clone(), table.columns.clone())
        };

        let results = {
            let index = self.hnsw_indexes.get_mut(index_name).ok_or(StorageError::KeyNotFound)?;
            index.search(&mut self.pager, query, k).map_err(|e| StorageError::Other(format!("HNSW search error: {:?}", e)))?
        };

        let mut final_results = Vec::new();
        for (rowid, dist) in results {
            if let Ok(record) = self.get_record(&table_name, rowid) {
                final_results.push((record, dist));
            }
        }
        
        Ok(final_results)
    }

    /// Get an index
    pub fn get_index(&self, name: &str) -> Option<&BPlusTreeIndex> {
        self.indexes.get(name)
    }

    /// Get an index (mutable)
    pub fn get_index_mut(&mut self, name: &str) -> Option<&mut BPlusTreeIndex> {
        self.indexes.get_mut(name)
    }

    /// List all indexes for a table
    pub fn get_table_indexes(&self, table_name: &str) -> Vec<&BPlusTreeIndex> {
        self.indexes
            .values()
            .filter(|idx| idx.table == table_name)
            .collect()
    }

    /// Get records by index lookup (point query)
    pub fn get_records_by_index(&mut self, table_name: &str, index_name: &str, key: &Value) -> Result<Vec<Record>> {
        // Clone rowids to avoid borrow checker issues
        let rowids: Vec<u64> = {
            let index = self.get_index(index_name)
                .ok_or(StorageError::KeyNotFound)?;
            index.lookup(key)
                .map(|v| v.clone())
                .unwrap_or_default()
        };

        let mut records = Vec::new();
        for rowid in rowids {
            if let Ok(record) = self.get_record(table_name, rowid) {
                records.push(record);
            }
        }
        Ok(records)
    }

    /// Get records by covering index lookup (no table lookup needed)
    /// For now, this is a placeholder that returns the same as regular index scan
    /// In a full implementation, this would construct records from index data only
    pub fn get_records_by_index_covering(&mut self, table_name: &str, index_name: &str, key: &Value) -> Result<Vec<Record>> {
        // In a full implementation, we would:
        // 1. Get the index entry which contains the indexed column value
        // 2. Construct a partial record with just that column
        // 3. Return without accessing the main table
        
        // For now, delegate to regular index scan (still benefits from being marked as covering)
        self.get_records_by_index(table_name, index_name, key)
    }

    /// Get records by index range scan
    pub fn get_records_by_index_range(
        &mut self,
        table_name: &str,
        index_name: &str,
        start: Option<&Value>,
        end: Option<&Value>,
    ) -> Result<Vec<Record>> {
        // Clone rowids to avoid borrow checker issues
        let rowids: Vec<u64> = {
            let index = self.get_index(index_name)
                .ok_or(StorageError::KeyNotFound)?;

            // Get rowids from index
            if let (Some(start), Some(end)) = (start, end) {
                index.range_scan(start, end)
            } else if let Some(start) = start {
                // Range from start to max
                let end = Value::Text("\u{10FFFF}".to_string()); // Max unicode char
                index.range_scan(start, &end)
            } else if let Some(end) = end {
                // Range from min to end
                let start = Value::Null;
                index.range_scan(&start, end)
            } else {
                // Full scan through index
                let start = Value::Null;
                let end = Value::Text("\u{10FFFF}".to_string());
                index.range_scan(&start, &end)
            }
        };

        let mut records = Vec::new();
        for rowid in rowids {
            if let Ok(record) = self.get_record(table_name, rowid) {
                records.push(record);
            }
        }
        Ok(records)
    }

    /// Get records by covering index range scan (no table lookup needed)
    pub fn get_records_by_index_range_covering(
        &mut self,
        table_name: &str,
        index_name: &str,
        start: Option<&Value>,
        end: Option<&Value>,
    ) -> Result<Vec<Record>> {
        // For now, delegate to regular index range scan
        self.get_records_by_index_range(table_name, index_name, start, end)
    }

    /// Insert a record into a table
    pub fn insert(&mut self, table_name: &str, record: Record) -> Result<u64> {
        // Check foreign key constraints before inserting
        self.check_foreign_keys_on_insert(table_name, &record)?;

        let table = self.get_table_mut(table_name)
            .ok_or(StorageError::KeyNotFound)?;

        let rowid = table.next_rowid;
        table.next_rowid += 1;

        // Serialize record
        let value = record.serialize();

        // Create key from rowid
        let key = rowid.to_be_bytes().to_vec();

        // Get B-tree storage
        let btree = self.btree_storages.get_mut(table_name)
            .ok_or(StorageError::KeyNotFound)?;

        // Insert into B-tree
        btree.insert(&mut self.pager, &key, &value)?;

        // Update indexes
        self.update_indexes_on_insert(table_name, rowid, &record)?;

        // Save schema (to persist next_rowid)
        self.save_schema()?;

        Ok(rowid)
    }

    /// Check foreign key constraints on insert
    fn check_foreign_keys_on_insert(&mut self, table_name: &str, record: &Record) -> Result<()> {
        // Collect column-level foreign keys first
        let column_fks: Vec<(String, String, Value)> = {
            let table = self.get_table(table_name)
                .ok_or(StorageError::KeyNotFound)?;
            let mut fks = Vec::new();
            for (col_idx, col) in table.columns.iter().enumerate() {
                if let Some(fk) = &col.foreign_key {
                    if col_idx < record.values.len() {
                        let fk_value = &record.values[col_idx];
                        if *fk_value != Value::Null {
                            fks.push((fk.ref_table.clone(), fk.ref_column.clone(), fk_value.clone()));
                        }
                    }
                }
            }
            fks
        };
        
        // Check column-level foreign keys
        for (ref_table, ref_column, value) in column_fks {
            self.check_foreign_key_value(&ref_table, &ref_column, &value)?;
        }

        // Collect and check table-level foreign keys
        let table_fks: Vec<ForeignKeyConstraint> = {
            let table = self.get_table(table_name)
                .ok_or(StorageError::KeyNotFound)?;
            table.foreign_keys.clone()
        };
        
        for fk in &table_fks {
            self.check_table_foreign_key(fk, record)?;
        }

        Ok(())
    }

    /// Check if a single foreign key value references an existing record
    fn check_foreign_key_value(&mut self, ref_table: &str, ref_column: &str, value: &Value) -> Result<()> {
        // Check if referenced table exists
        let ref_table_meta = self.get_table(ref_table)
            .ok_or_else(|| StorageError::ForeignKeyViolation {
                table: ref_table.to_string(),
                detail: format!("Referenced table '{}' does not exist", ref_table),
            })?;

        // Find the column index in referenced table
        let ref_col_idx = ref_table_meta.column_index(ref_column)
            .unwrap_or(0); // Default to first column if not found

        // Search for record with matching value
        let records = self.select_all(ref_table)?;
        for record in records {
            if ref_col_idx < record.values.len() {
                if &record.values[ref_col_idx] == value {
                    return Ok(()); // Found matching record
                }
            }
        }

        Err(StorageError::ForeignKeyViolation {
            table: ref_table.to_string(),
            detail: format!(
                "No matching record in '{}' where {} = {:?}",
                ref_table, ref_column, value
            ),
        })
    }

    /// Check a table-level foreign key constraint
    fn check_table_foreign_key(&mut self, fk: &ForeignKeyConstraint, record: &Record) -> Result<()> {
        // Build composite key from record values and collect column indices
        let mut key_values = Vec::new();
        let ref_col_indices: Vec<usize> = {
            let table = self.get_table(&fk.ref_table)
                .ok_or(StorageError::KeyNotFound)?;
            
            let mut indices = Vec::new();
            for col_name in &fk.columns {
                let col_idx = table.column_index(col_name)
                    .ok_or(StorageError::KeyNotFound)?;
                
                if col_idx >= record.values.len() {
                    return Ok(()); // Partial key, skip check
                }
                
                let value = &record.values[col_idx];
                if *value == Value::Null {
                    // If any part of composite key is NULL, constraint is satisfied
                    return Ok(());
                }
                key_values.push(value.clone());
                
                // Store the corresponding reference column index
                if let Some(ref_col_idx) = table.column_index(&fk.ref_columns[indices.len()]) {
                    indices.push(ref_col_idx);
                } else {
                    indices.push(indices.len());
                }
            }
            indices
        };

        // Check if referenced table exists
        let _ = self.get_table(&fk.ref_table)
            .ok_or_else(|| StorageError::ForeignKeyViolation {
                table: fk.ref_table.clone(),
                detail: format!("Referenced table '{}' does not exist", fk.ref_table),
            })?;

        let records = self.select_all(&fk.ref_table)?;
        for rec in records {
            let mut all_match = true;
            for (i, ref_col_idx) in ref_col_indices.iter().enumerate() {
                if *ref_col_idx >= rec.values.len() || rec.values[*ref_col_idx] != key_values[i] {
                    all_match = false;
                    break;
                }
            }
            if all_match {
                return Ok(());
            }
        }

        Err(StorageError::ForeignKeyViolation {
            table: fk.ref_table.clone(),
            detail: format!(
                "No matching record in '{}' for key {:?}",
                fk.ref_table, key_values
            ),
        })
    }

    /// Update indexes on insert
    fn update_indexes_on_insert(
        &mut self,
        table_name: &str,
        rowid: u64,
        record: &Record,
    ) -> Result<()> {
        // Collect indexes to update
        let indexes_to_update: Vec<(String, Value)> = {
            let table = self.get_table(table_name)
                .ok_or(StorageError::KeyNotFound)?;

            self.get_table_indexes(table_name)
                .iter()
                .filter_map(|index| {
                    table.column_index(&index.column)
                        .filter(|&col_idx| col_idx < record.values.len())
                        .map(|col_idx| (index.name.clone(), record.values[col_idx].clone()))
                })
                .collect()
        };

        // Update indexes
        // Update B-trees (simplified memory-based ones)
        for (index_name, key) in indexes_to_update {
            if let Some(index) = self.indexes.get_mut(&index_name) {
                index.insert(key, rowid)?;
            }
        }

        // Update HNSW indices (disk-based)
        let table = self.tables.get(table_name).ok_or(StorageError::KeyNotFound)?;
        let hnsw_metas = table.hnsw_indices.clone();
        for idx_meta in hnsw_metas {
            if let Some(col_idx) = table.column_index(&idx_meta.column) {
                if let Some(val) = record.values.get(col_idx) {
                    if let Value::Vector(vec) = val {
                        if let Some(hnsw) = self.hnsw_indexes.get_mut(&idx_meta.name) {
                            hnsw.insert(&mut self.pager, vec, rowid).map_err(|e| StorageError::Other(format!("HNSW update error: {:?}", e)))?;
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Select all records from a table
    pub fn select_all(&mut self, table_name: &str) -> Result<Vec<Record>> {
        self.select_all_with_filter(table_name, None::<&dyn Fn(&Record) -> bool>)
    }

    /// Select all records from a table with optional filter
    /// 
    /// The filter is applied at the storage layer to reduce data transfer
    pub fn select_all_with_filter<F>(
        &mut self, 
        table_name: &str, 
        filter: Option<F>
    ) -> Result<Vec<Record>> 
    where
        F: Fn(&Record) -> bool,
    {
        let _table = self.get_table(table_name)
            .ok_or(StorageError::KeyNotFound)?;

        let btree = self.btree_storages.get(table_name)
            .ok_or(StorageError::KeyNotFound)?;

        // Range scan from beginning to end with optional filtering
        let records: Vec<_> = if let Some(ref filter_fn) = filter {
            btree.range_scan(&mut self.pager, None, None)?
                .filter_map(|(_key, value)| {
                    Record::deserialize(&value).ok().filter(filter_fn)
                })
                .collect()
        } else {
            btree.range_scan(&mut self.pager, None, None)?
                .filter_map(|(_key, value)| {
                    Record::deserialize(&value).ok()
                })
                .collect()
        };

        Ok(records)
    }

    /// Select all records with rowid
    pub fn select_all_with_rowid(&mut self, table_name: &str) -> Result<Vec<(u64, Record)>> {
        let _table = self.get_table(table_name)
            .ok_or(StorageError::KeyNotFound)?;

        let btree = self.btree_storages.get(table_name)
            .ok_or(StorageError::KeyNotFound)?;

        let results: Vec<_> = btree.range_scan(&mut self.pager, None, None)?
            .filter_map(|(key, value)| {
                let rowid = u64::from_be_bytes([
                    key.get(0).copied().unwrap_or(0),
                    key.get(1).copied().unwrap_or(0),
                    key.get(2).copied().unwrap_or(0),
                    key.get(3).copied().unwrap_or(0),
                    key.get(4).copied().unwrap_or(0),
                    key.get(5).copied().unwrap_or(0),
                    key.get(6).copied().unwrap_or(0),
                    key.get(7).copied().unwrap_or(0),
                ]);
                Record::deserialize(&value).ok().map(|r| (rowid, r))
            })
            .collect();

        Ok(results)
    }

    /// Get a single record by rowid
    pub fn get_record(&mut self, table_name: &str, rowid: u64) -> Result<Record> {
        let _table = self.get_table(table_name)
            .ok_or(StorageError::KeyNotFound)?;

        let btree = self.btree_storages.get(table_name)
            .ok_or(StorageError::KeyNotFound)?;

        let key = rowid.to_be_bytes();
        let value = btree.search(&mut self.pager, &key)?
            .ok_or(StorageError::KeyNotFound)?;
        Record::deserialize(&value)
    }

    /// Delete a record by rowid
    pub fn delete(&mut self, table_name: &str, rowid: u64) -> Result<()> {
        // First get the record being deleted (for cascade operations)
        let record = self.get_record(table_name, rowid)?;

        // Handle foreign key actions (ON DELETE CASCADE, SET NULL, etc.)
        self.handle_on_delete_cascade(table_name, &record)?;

        let _table = self.get_table(table_name)
            .ok_or(StorageError::KeyNotFound)?;

        let btree = self.btree_storages.get(table_name)
            .ok_or(StorageError::KeyNotFound)?;

        let key = rowid.to_be_bytes();
        let deleted = btree.delete(&mut self.pager, &key)?;

        if !deleted {
            return Err(StorageError::KeyNotFound);
        }

        Ok(())
    }

    /// Handle ON DELETE CASCADE, SET NULL, etc.
    fn handle_on_delete_cascade(&mut self, parent_table: &str, parent_record: &Record) -> Result<()> {
        // Find all tables that reference this table
        let referencing_tables: Vec<(String, Vec<(usize, ForeignKeyAction)>)> = {
            let mut refs = Vec::new();
            
            for (table_name, table) in &self.tables {
                let mut fk_columns = Vec::new();
                
                // Check column-level foreign keys
                for (col_idx, col) in table.columns.iter().enumerate() {
                    if let Some(fk) = &col.foreign_key {
                        if fk.ref_table == parent_table {
                            fk_columns.push((col_idx, fk.on_delete));
                        }
                    }
                }
                
                if !fk_columns.is_empty() {
                    refs.push((table_name.clone(), fk_columns));
                }
            }
            
            refs
        };

        // For each referencing table, find and handle dependent records
        for (child_table_name, fk_columns) in referencing_tables {
            let child_records = self.select_all_with_rowid(&child_table_name)?;
            
            for (child_rowid, child_record) in child_records {
                for (fk_col_idx, on_delete_action) in &fk_columns {
                    if *fk_col_idx >= child_record.values.len() {
                        continue;
                    }
                    
                    let fk_value = &child_record.values[*fk_col_idx];
                    
                    // Check if this child record references the parent being deleted
                    // For simplicity, assume single-column integer primary key
                    if self.values_match(fk_value, parent_record) {
                        match on_delete_action {
                            ForeignKeyAction::Cascade => {
                                // Delete the child record
                                self.delete(&child_table_name, child_rowid)?;
                            }
                            ForeignKeyAction::SetNull => {
                                // Update child record, set FK to NULL
                                let mut updated_record = child_record.clone();
                                updated_record.values[*fk_col_idx] = Value::Null;
                                self.update(&child_table_name, child_rowid, updated_record)?;
                            }
                            ForeignKeyAction::Restrict | ForeignKeyAction::NoAction => {
                                // Prevent deletion
                                return Err(StorageError::ForeignKeyViolation {
                                    table: child_table_name.clone(),
                                    detail: format!(
                                        "Cannot delete parent row from '{}' - child rows exist in '{}'",
                                        parent_table, child_table_name
                                    ),
                                });
                            }
                            _ => {} // SetDefault not implemented yet
                        }
                        break; // Processed this child record, move to next
                    }
                }
            }
        }

        Ok(())
    }

    /// Check if a foreign key value matches a parent record
    fn values_match(&self, fk_value: &Value, parent_record: &Record) -> bool {
        // Simple case: compare with first column (assuming PK is first column)
        if !parent_record.values.is_empty() {
            return &parent_record.values[0] == fk_value;
        }
        false
    }

    /// Update a record by rowid
    pub fn update(&mut self, table_name: &str, rowid: u64, record: Record) -> Result<()> {
        let _table = self.get_table(table_name)
            .ok_or(StorageError::KeyNotFound)?;

        let btree = self.btree_storages.get_mut(table_name)
            .ok_or(StorageError::KeyNotFound)?;

        let key = rowid.to_be_bytes();
        let value = record.serialize();

        // Check if record exists
        if btree.search(&mut self.pager, &key)?.is_none() {
            return Err(StorageError::KeyNotFound);
        }

        // For simplicity, we use delete + insert with a special flag
        // to allow overwriting the same key
        btree.delete(&mut self.pager, &key)?;
        btree.insert(&mut self.pager, &key, &value)?;

        self.save_schema()?;

        Ok(())
    }

    /// Flush changes to disk
    pub fn flush(&mut self) -> Result<()> {
        self.pager.flush()?;
        Ok(())
    }

    /// Save schema to page
    fn save_schema(&mut self) -> Result<()> {
        let mut page = self.pager.get_page(self.schema_page)?;

        // Table count
        let table_count = self.tables.len() as u8;
        page.as_mut_slice()[0] = table_count;

        let mut pos = 1;
        for table in self.tables.values() {
            let table_data = table.serialize();
            let len = table_data.len();

            // Write length
            page.as_mut_slice()[pos..pos+4].copy_from_slice(&(len as u32).to_be_bytes());
            pos += 4;

            // Write data
            page.as_mut_slice()[pos..pos+len].copy_from_slice(&table_data);
            pos += len;
        }

        self.pager.write_page(&page)?;
        self.pager.flush()?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn test_btree_database_create_table() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap();

        let mut db = BtreeDatabase::open(path).unwrap();

        let columns = vec![
            ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                primary_key: true,
                foreign_key: None,
                default_value: None,
                is_virtual: false,
                generated_always: None,
            },
            ColumnDef {
                name: "name".to_string(),
                data_type: DataType::Text,
                nullable: true,
                primary_key: false,
                foreign_key: None,
                default_value: None,
                is_virtual: false,
                generated_always: None,
            },
        ];

        db.create_table("users".to_string(), columns).unwrap();

        assert!(db.get_table("users").is_some());
        assert_eq!(db.list_tables().len(), 1);
    }

    #[test]
    fn test_btree_database_insert_and_select() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap();

        let mut db = BtreeDatabase::open(path).unwrap();

        // Create table
        let columns = vec![
            ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                primary_key: true,
                foreign_key: None,
                default_value: None,
                is_virtual: false,
                generated_always: None,
            },
            ColumnDef {
                name: "name".to_string(),
                data_type: DataType::Text,
                nullable: true,
                primary_key: false,
                foreign_key: None,
                default_value: None,
                is_virtual: false,
                generated_always: None,
            },
        ];
        db.create_table("users".to_string(), columns).unwrap();

        // Insert records
        for i in 0..20 {
            let record = Record::new(vec![
                Value::Integer(i as i64),
                Value::Text(format!("User{}", i)),
            ]);
            let rowid = db.insert("users", record).unwrap();
            assert_eq!(rowid, i as u64 + 1);
        }

        // Select all
        let records = db.select_all("users").unwrap();
        assert_eq!(records.len(), 20);

        // Verify data
        for (i, record) in records.iter().enumerate() {
            assert_eq!(record.values[0], Value::Integer(i as i64));
            assert_eq!(record.values[1], Value::Text(format!("User{}", i)));
        }
    }

    #[test]
    fn test_btree_database_get_record() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap();

        let mut db = BtreeDatabase::open(path).unwrap();

        let columns = vec![
            ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                primary_key: true,
                foreign_key: None,
                default_value: None,
                is_virtual: false,
                generated_always: None,
            },
            ColumnDef {
                name: "name".to_string(),
                data_type: DataType::Text,
                nullable: true,
                primary_key: false,
                foreign_key: None,
                default_value: None,
                is_virtual: false,
                generated_always: None,
            },
        ];
        db.create_table("users".to_string(), columns).unwrap();

        // Insert
        let record = Record::new(vec![
            Value::Integer(42),
            Value::Text("Alice".to_string()),
        ]);
        let rowid = db.insert("users", record.clone()).unwrap();

        // Get
        let retrieved = db.get_record("users", rowid).unwrap();
        assert_eq!(retrieved.values[0], Value::Integer(42));
        assert_eq!(retrieved.values[1], Value::Text("Alice".to_string()));
    }

    #[test]
    fn test_btree_database_delete() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap();

        let mut db = BtreeDatabase::open(path).unwrap();

        let columns = vec![
            ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                primary_key: true,
                foreign_key: None,
                default_value: None,
                is_virtual: false,
                generated_always: None,
            },
            ColumnDef {
                name: "name".to_string(),
                data_type: DataType::Text,
                nullable: true,
                primary_key: false,
                foreign_key: None,
                default_value: None,
                is_virtual: false,
                generated_always: None,
            },
        ];
        db.create_table("users".to_string(), columns).unwrap();

        // Insert
        let record = Record::new(vec![
            Value::Integer(1),
            Value::Text("Alice".to_string()),
        ]);
        let rowid = db.insert("users", record).unwrap();

        // Delete
        db.delete("users", rowid).unwrap();

        // Verify deleted
        assert!(db.get_record("users", rowid).is_err());
    }

    #[test]
    fn test_btree_database_delete_all() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap();

        let mut db = BtreeDatabase::open(path).unwrap();

        let columns = vec![
            ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                primary_key: true,
                foreign_key: None,
                default_value: None,
                is_virtual: false,
                generated_always: None,
            },
            ColumnDef {
                name: "name".to_string(),
                data_type: DataType::Text,
                nullable: true,
                primary_key: false,
                foreign_key: None,
                default_value: None,
                is_virtual: false,
                generated_always: None,
            },
        ];
        db.create_table("users".to_string(), columns).unwrap();

        // Insert 3 records
        for i in 0..3 {
            let record = Record::new(vec![
                Value::Integer(i as i64),
                Value::Text(format!("User{}", i)),
            ]);
            db.insert("users", record).unwrap();
        }

        // Verify 3 records
        let records = db.select_all("users").unwrap();
        assert_eq!(records.len(), 3);

        // Delete all
        for i in 1..=3 {
            db.delete("users", i).unwrap();
        }

        // Verify 0 records
        let records = db.select_all("users").unwrap();
        assert_eq!(records.len(), 0);
    }

    #[test]
    fn test_foreign_key_column_level() {
        use crate::sql::ast::{ColumnForeignKey, ForeignKeyAction};
        
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap();

        let mut db = BtreeDatabase::open(path).unwrap();

        // Create parent table
        let parent_columns = vec![
            ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                primary_key: true,
                foreign_key: None,
                default_value: None,
                is_virtual: false,
                generated_always: None,
            },
        ];
        db.create_table("users".to_string(), parent_columns).unwrap();

        // Create child table with foreign key
        let child_columns = vec![
            ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                primary_key: true,
                foreign_key: None,
                default_value: None,
                is_virtual: false,
                generated_always: None,
            },
            ColumnDef {
                name: "user_id".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                primary_key: false,
                foreign_key: Some(ColumnForeignKey {
                    ref_table: "users".to_string(),
                    ref_column: "id".to_string(),
                    on_delete: ForeignKeyAction::Restrict,
                    on_update: ForeignKeyAction::NoAction,
                }),
                default_value: None,
                is_virtual: false,
                generated_always: None,
            },
        ];
        db.create_table("orders".to_string(), child_columns).unwrap();

        // Insert parent record
        let user = Record::new(vec![Value::Integer(1)]);
        db.insert("users", user).unwrap();

        // Insert child record with valid FK - should succeed
        let order = Record::new(vec![Value::Integer(1), Value::Integer(1)]);
        db.insert("orders", order).unwrap();

        // Insert child record with NULL FK - should succeed
        let order2 = Record::new(vec![Value::Integer(2), Value::Null]);
        db.insert("orders", order2).unwrap();

        // Insert child record with invalid FK - should fail
        let invalid_order = Record::new(vec![Value::Integer(3), Value::Integer(999)]);
        let result = db.insert("orders", invalid_order);
        assert!(result.is_err(), "Should fail with foreign key violation");
    }

    #[test]
    fn test_foreign_key_on_delete_cascade() {
        use crate::sql::ast::{ColumnForeignKey, ForeignKeyAction};
        
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap();

        let mut db = BtreeDatabase::open(path).unwrap();

        // Create parent table
        let parent_columns = vec![
            ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                primary_key: true,
                foreign_key: None,
                default_value: None,
                is_virtual: false,
                generated_always: None,
            },
        ];
        db.create_table("users".to_string(), parent_columns).unwrap();

        // Create child table with CASCADE delete
        let child_columns = vec![
            ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                primary_key: true,
                foreign_key: None,
                default_value: None,
                is_virtual: false,
                generated_always: None,
            },
            ColumnDef {
                name: "user_id".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                primary_key: false,
                foreign_key: Some(ColumnForeignKey {
                    ref_table: "users".to_string(),
                    ref_column: "id".to_string(),
                    on_delete: ForeignKeyAction::Cascade,
                    on_update: ForeignKeyAction::NoAction,
                }),
                default_value: None,
                is_virtual: false,
                generated_always: None,
            },
        ];
        db.create_table("orders".to_string(), child_columns).unwrap();

        // Insert parent and child records
        db.insert("users", Record::new(vec![Value::Integer(1)])).unwrap();
        db.insert("users", Record::new(vec![Value::Integer(2)])).unwrap();
        db.insert("orders", Record::new(vec![Value::Integer(1), Value::Integer(1)])).unwrap();
        db.insert("orders", Record::new(vec![Value::Integer(2), Value::Integer(1)])).unwrap();
        db.insert("orders", Record::new(vec![Value::Integer(3), Value::Integer(2)])).unwrap();

        // Verify initial counts
        assert_eq!(db.select_all("users").unwrap().len(), 2);
        assert_eq!(db.select_all("orders").unwrap().len(), 3);

        // Delete parent record - should cascade delete children with user_id=1
        db.delete("users", 1).unwrap();

        // Verify cascade worked
        assert_eq!(db.select_all("users").unwrap().len(), 1);
        assert_eq!(db.select_all("orders").unwrap().len(), 1); // Only order with user_id=2 remains
    }

    #[test]
    fn test_foreign_key_on_delete_set_null() {
        use crate::sql::ast::{ColumnForeignKey, ForeignKeyAction};
        
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap();

        let mut db = BtreeDatabase::open(path).unwrap();

        // Create parent table
        let parent_columns = vec![
            ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                primary_key: true,
                foreign_key: None,
                default_value: None,
                is_virtual: false,
                generated_always: None,
            },
        ];
        db.create_table("users".to_string(), parent_columns).unwrap();

        // Create child table with SET NULL delete
        let child_columns = vec![
            ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                primary_key: true,
                foreign_key: None,
                default_value: None,
                is_virtual: false,
                generated_always: None,
            },
            ColumnDef {
                name: "user_id".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                primary_key: false,
                foreign_key: Some(ColumnForeignKey {
                    ref_table: "users".to_string(),
                    ref_column: "id".to_string(),
                    on_delete: ForeignKeyAction::SetNull,
                    on_update: ForeignKeyAction::NoAction,
                }),
                default_value: None,
                is_virtual: false,
                generated_always: None,
            },
        ];
        db.create_table("orders".to_string(), child_columns).unwrap();

        // Insert parent and child records
        db.insert("users", Record::new(vec![Value::Integer(1)])).unwrap();
        db.insert("orders", Record::new(vec![Value::Integer(1), Value::Integer(1)])).unwrap();
        db.insert("orders", Record::new(vec![Value::Integer(2), Value::Integer(1)])).unwrap();

        // Delete parent record - should set children FK to NULL
        db.delete("users", 1).unwrap();

        // Verify SET NULL worked
        let orders = db.select_all("orders").unwrap();
        assert_eq!(orders.len(), 2);
        assert_eq!(orders[0].values[1], Value::Null);
        assert_eq!(orders[1].values[1], Value::Null);
    }

    #[test]
    fn test_alter_table_add_column() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap();

        let mut db = BtreeDatabase::open(path).unwrap();

        // Create table
        let columns = vec![
            ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                primary_key: true,
                foreign_key: None,
                default_value: None,
                is_virtual: false,
                generated_always: None,
            },
        ];
        db.create_table("users".to_string(), columns).unwrap();

        // Add column
        let new_column = ColumnDef {
            name: "email".to_string(),
            data_type: DataType::Text,
            nullable: true,
            primary_key: false,
            foreign_key: None,
                default_value: None,
                is_virtual: false,
                generated_always: None,
        };
        db.alter_table_add_column("users", new_column).unwrap();

        // Verify column was added
        let table = db.get_table("users").unwrap();
        assert_eq!(table.columns.len(), 2);
        assert_eq!(table.columns[1].name, "email");
    }

    #[test]
    fn test_alter_table_drop_column() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap();

        let mut db = BtreeDatabase::open(path).unwrap();

        // Create table
        let columns = vec![
            ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                primary_key: true,
                foreign_key: None,
                default_value: None,
                is_virtual: false,
                generated_always: None,
            },
            ColumnDef {
                name: "email".to_string(),
                data_type: DataType::Text,
                nullable: true,
                primary_key: false,
                foreign_key: None,
                default_value: None,
                is_virtual: false,
                generated_always: None,
            },
        ];
        db.create_table("users".to_string(), columns).unwrap();

        // Drop column
        db.alter_table_drop_column("users", "email").unwrap();

        // Verify column was dropped
        let table = db.get_table("users").unwrap();
        assert_eq!(table.columns.len(), 1);
        assert!(table.column_index("email").is_none());
    }

    #[test]
    fn test_alter_table_rename() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap();

        let mut db = BtreeDatabase::open(path).unwrap();

        // Create table
        let columns = vec![
            ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                primary_key: true,
                foreign_key: None,
                default_value: None,
                is_virtual: false,
                generated_always: None,
            },
        ];
        db.create_table("users".to_string(), columns).unwrap();

        // Rename table
        db.alter_table_rename("users", "customers").unwrap();

        // Verify old table doesn't exist
        assert!(db.get_table("users").is_none());
        // Verify new table exists
        assert!(db.get_table("customers").is_some());
    }

    #[test]
    fn test_alter_table_rename_column() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap();

        let mut db = BtreeDatabase::open(path).unwrap();

        // Create table
        let columns = vec![
            ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                primary_key: true,
                foreign_key: None,
                default_value: None,
                is_virtual: false,
                generated_always: None,
            },
            ColumnDef {
                name: "email".to_string(),
                data_type: DataType::Text,
                nullable: true,
                primary_key: false,
                foreign_key: None,
                default_value: None,
                is_virtual: false,
                generated_always: None,
            },
        ];
        db.create_table("users".to_string(), columns).unwrap();

        // Rename column
        db.alter_table_rename_column("users", "email", "email_address").unwrap();

        // Verify old column name doesn't exist
        let table = db.get_table("users").unwrap();
        assert!(table.column_index("email").is_none());
        assert!(table.column_index("email_address").is_some());
    }
}
