//! P5-2: Trigger System Implementation
//!
//! Triggers are database callbacks that are automatically executed 
//! when specified database events occur.

use crate::sql::ast::*;
use crate::storage::{Value, Record};
use std::collections::HashMap;

pub mod error;
pub use error::{TriggerError, Result};

/// Trigger metadata stored in database
#[derive(Debug, Clone)]
pub struct TriggerMetadata {
    pub name: String,
    pub timing: TriggerTiming,
    pub event: TriggerEvent,
    pub table: String,
    pub for_each_row: bool,
    pub when_clause: Option<Expression>,
    pub body: Vec<TriggerStatement>,
    pub enabled: bool,
}

impl TriggerMetadata {
    /// Serialize trigger to bytes for storage
    pub fn serialize(&self) -> Vec<u8> {
        let mut data = Vec::new();
        
        // Name
        let name_bytes = self.name.as_bytes();
        data.extend_from_slice(&(name_bytes.len() as u32).to_be_bytes());
        data.extend_from_slice(name_bytes);
        
        // Table
        let table_bytes = self.table.as_bytes();
        data.extend_from_slice(&(table_bytes.len() as u32).to_be_bytes());
        data.extend_from_slice(table_bytes);
        
        // Timing (1=Before, 2=After, 3=InsteadOf)
        data.push(match self.timing {
            TriggerTiming::Before => 1,
            TriggerTiming::After => 2,
            TriggerTiming::InsteadOf => 3,
        });
        
        // Event (1=Insert, 2=Delete, 3=Update)
        data.push(match &self.event {
            TriggerEvent::Insert => 1,
            TriggerEvent::Delete => 2,
            TriggerEvent::Update { .. } => 3,
        });
        
        // For each row
        data.push(if self.for_each_row { 1 } else { 0 });
        
        // Enabled
        data.push(if self.enabled { 1 } else { 0 });
        
        data
    }
    
    /// Deserialize from bytes
    pub fn deserialize(data: &[u8], body: Vec<TriggerStatement>) -> Result<Self> {
        let mut pos = 0;
        
        let name_len = u32::from_be_bytes([data[pos], data[pos+1], data[pos+2], data[pos+3]]) as usize;
        pos += 4;
        let name = String::from_utf8_lossy(&data[pos..pos+name_len]).to_string();
        pos += name_len;
        
        let table_len = u32::from_be_bytes([data[pos], data[pos+1], data[pos+2], data[pos+3]]) as usize;
        pos += 4;
        let table = String::from_utf8_lossy(&data[pos..pos+table_len]).to_string();
        pos += table_len;
        
        let timing = match data[pos] {
            1 => TriggerTiming::Before,
            2 => TriggerTiming::After,
            3 => TriggerTiming::InsteadOf,
            _ => TriggerTiming::Before,
        };
        pos += 1;
        
        let event = match data[pos] {
            1 => TriggerEvent::Insert,
            2 => TriggerEvent::Delete,
            3 => TriggerEvent::Update { columns: None },
            _ => TriggerEvent::Insert,
        };
        pos += 1;
        
        let for_each_row = data[pos] == 1;
        pos += 1;
        
        let enabled = data[pos] == 1;
        
        Ok(Self {
            name,
            timing,
            event,
            table,
            for_each_row,
            when_clause: None,
            body,
            enabled,
        })
    }
}

/// Trigger execution context
#[derive(Debug)]
pub struct TriggerContext {
    /// OLD row values (for UPDATE/DELETE)
    pub old_row: Option<HashMap<String, Value>>,
    /// NEW row values (for INSERT/UPDATE)
    pub new_row: Option<HashMap<String, Value>>,
    /// Trigger name
    pub trigger_name: String,
    /// Table name
    pub table_name: String,
    /// Operation type
    pub operation: String,
}

impl TriggerContext {
    pub fn new(trigger_name: String, table_name: String, operation: String) -> Self {
        Self {
            old_row: None,
            new_row: None,
            trigger_name,
            table_name,
            operation,
        }
    }
    
    /// Set OLD row values
    pub fn set_old_row(&mut self, record: &Record, columns: &[String]) {
        let mut row = HashMap::new();
        for (i, col) in columns.iter().enumerate() {
            if let Some(val) = record.values.get(i) {
                row.insert(col.clone(), val.clone());
            }
        }
        self.old_row = Some(row);
    }
    
    /// Set NEW row values
    pub fn set_new_row(&mut self, record: &Record, columns: &[String]) {
        let mut row = HashMap::new();
        for (i, col) in columns.iter().enumerate() {
            if let Some(val) = record.values.get(i) {
                row.insert(col.clone(), val.clone());
            }
        }
        self.new_row = Some(row);
    }
    
    /// Get value from OLD or NEW row
    pub fn get_value(&self, is_new: bool, column: &str) -> Option<Value> {
        if is_new {
            self.new_row.as_ref()?.get(column).cloned()
        } else {
            self.old_row.as_ref()?.get(column).cloned()
        }
    }
}

/// Trigger manager
pub struct TriggerManager {
    triggers: HashMap<String, TriggerMetadata>,
}

impl TriggerManager {
    pub fn new() -> Self {
        Self {
            triggers: HashMap::new(),
        }
    }
    
    /// Register a new trigger
    pub fn register(&mut self, trigger: TriggerMetadata) -> Result<()> {
        if self.triggers.contains_key(&trigger.name) {
            return Err(TriggerError::AlreadyExists(trigger.name));
        }
        self.triggers.insert(trigger.name.clone(), trigger);
        Ok(())
    }
    
    /// Drop a trigger
    pub fn drop_trigger(&mut self, name: &str, if_exists: bool) -> Result<()> {
        if !self.triggers.contains_key(name) {
            if if_exists {
                return Ok(());
            }
            return Err(TriggerError::NotFound(name.to_string()));
        }
        self.triggers.remove(name);
        Ok(())
    }
    
    /// Find triggers for a table and event
    pub fn find_triggers(
        &self,
        table: &str,
        timing: TriggerTiming,
        event: &TriggerEvent,
    ) -> Vec<&TriggerMetadata> {
        self.triggers
            .values()
            .filter(|t| {
                t.table == table 
                    && t.timing == timing 
                    && t.enabled
                    && Self::event_matches(&t.event, event)
            })
            .collect()
    }
    
    fn event_matches(a: &TriggerEvent, b: &TriggerEvent) -> bool {
        match (a, b) {
            (TriggerEvent::Insert, TriggerEvent::Insert) => true,
            (TriggerEvent::Delete, TriggerEvent::Delete) => true,
            (TriggerEvent::Update { columns: a_cols }, TriggerEvent::Update { columns: b_cols }) => {
                match (a_cols, b_cols) {
                    (None, _) => true,
                    (Some(a), Some(b)) => a.iter().any(|col| b.contains(col)),
                    (Some(_), None) => true,
                }
            }
            _ => false,
        }
    }
    
    /// Enable a trigger
    pub fn enable_trigger(&mut self, name: &str) -> Result<()> {
        if let Some(trigger) = self.triggers.get_mut(name) {
            trigger.enabled = true;
            Ok(())
        } else {
            Err(TriggerError::NotFound(name.to_string()))
        }
    }
    
    /// Disable a trigger
    pub fn disable_trigger(&mut self, name: &str) -> Result<()> {
        if let Some(trigger) = self.triggers.get_mut(name) {
            trigger.enabled = false;
            Ok(())
        } else {
            Err(TriggerError::NotFound(name.to_string()))
        }
    }
    
    /// List all triggers for a table
    pub fn list_triggers(&self, table: Option<&str>) -> Vec<&TriggerMetadata> {
        match table {
            Some(t) => self.triggers.values().filter(|tr| tr.table == t).collect(),
            None => self.triggers.values().collect(),
        }
    }
    
    /// Get trigger by name
    pub fn get_trigger(&self, name: &str) -> Option<&TriggerMetadata> {
        self.triggers.get(name)
    }
}

impl Default for TriggerManager {
    fn default() -> Self {
        Self::new()
    }
}
