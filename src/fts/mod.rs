//! P5-6: Full Text Search (FTS5) Implementation
//!
//! FTS5 is a SQLite virtual table module that provides full-text search functionality.

use std::collections::{HashMap, HashSet};
use crate::storage::Value;

pub mod error;
pub use error::{FtsError, Result};

/// Tokenizer for breaking text into terms
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum TokenizerType {
    /// Simple whitespace tokenizer
    Simple,
    /// Unicode-aware tokenizer (simplified)
    Unicode61,
}

impl Default for TokenizerType {
    fn default() -> Self {
        TokenizerType::Simple
    }
}

/// Tokenize text into terms
pub fn tokenize(text: &str, _tokenizer: TokenizerType) -> Vec<String> {
    // Simplified tokenization: split on whitespace and punctuation
    text.to_lowercase()
        .split_whitespace()
        .map(|s| s.trim_matches(|c: char| !c.is_alphanumeric()).to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

/// FTS5 index entry
#[derive(Debug, Clone)]
pub struct FtsIndexEntry {
    pub term: String,
    pub doc_id: u64,
    pub column: usize,
    pub position: usize,
}

/// FTS5 virtual table
#[derive(Debug)]
pub struct Fts5Table {
    pub name: String,
    pub columns: Vec<String>,
    pub tokenizer: TokenizerType,
    /// Inverted index: term -> [(doc_id, column, position)]
    inverted_index: HashMap<String, Vec<(u64, usize, usize)>>,
    /// Document content: doc_id -> [column_values]
    documents: HashMap<u64, Vec<String>>,
    /// Next document ID
    next_doc_id: u64,
}

impl Fts5Table {
    /// Create a new FTS5 table
    pub fn new(name: String, columns: Vec<String>) -> Self {
        Self {
            name,
            columns,
            tokenizer: TokenizerType::Simple,
            inverted_index: HashMap::new(),
            documents: HashMap::new(),
            next_doc_id: 1,
        }
    }
    
    /// Insert a document into the FTS index
    pub fn insert(&mut self, values: &[String]) -> Result<u64> {
        let doc_id = self.next_doc_id;
        self.next_doc_id += 1;
        
        // Store document content
        self.documents.insert(doc_id, values.to_vec());
        
        // Build inverted index
        for (col_idx, value) in values.iter().enumerate() {
            let terms = tokenize(value, self.tokenizer);
            for (pos, term) in terms.iter().enumerate() {
                self.inverted_index
                    .entry(term.clone())
                    .or_default()
                    .push((doc_id, col_idx, pos));
            }
        }
        
        Ok(doc_id)
    }
    
    /// Update a document in the FTS index
    pub fn update(&mut self, doc_id: u64, values: &[String]) -> Result<()> {
        // Remove old entries
        self.delete(doc_id)?;
        
        // Re-insert with same doc_id
        self.documents.insert(doc_id, values.to_vec());
        
        for (col_idx, value) in values.iter().enumerate() {
            let terms = tokenize(value, self.tokenizer);
            for (pos, term) in terms.iter().enumerate() {
                self.inverted_index
                    .entry(term.clone())
                    .or_default()
                    .push((doc_id, col_idx, pos));
            }
        }
        
        Ok(())
    }
    
    /// Delete a document from the FTS index
    pub fn delete(&mut self, doc_id: u64) -> Result<()> {
        // Remove from documents
        self.documents.remove(&doc_id);
        
        // Remove from inverted index
        for entries in self.inverted_index.values_mut() {
            entries.retain(|(d, _, _)| *d != doc_id);
        }
        
        // Clean up empty entries
        self.inverted_index.retain(|_, entries| !entries.is_empty());
        
        Ok(())
    }
    
    /// Search for documents matching query
    pub fn search(&self, query: &str) -> Result<Vec<(u64, f64)>> {
        let query_terms = tokenize(query, self.tokenizer);
        
        if query_terms.is_empty() {
            return Ok(Vec::new());
        }
        
        // Score documents by term frequency
        let mut doc_scores: HashMap<u64, f64> = HashMap::new();
        
        for term in &query_terms {
            if let Some(entries) = self.inverted_index.get(term) {
                for (doc_id, _, _) in entries {
                    *doc_scores.entry(*doc_id).or_insert(0.0) += 1.0;
                }
            }
        }
        
        // Convert to sorted vector
        let mut results: Vec<(u64, f64)> = doc_scores.into_iter().collect();
        results.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        
        Ok(results)
    }
    
    /// Match documents using MATCH operator
    pub fn match_query(&self, query: &str, column: Option<usize>) -> Result<Vec<u64>> {
        let query_terms = tokenize(query, self.tokenizer);
        
        if query_terms.is_empty() {
            return Ok(Vec::new());
        }
        
        let mut matching_docs: HashSet<u64> = HashSet::new();
        let mut first_term = true;
        
        for term in &query_terms {
            if let Some(entries) = self.inverted_index.get(term) {
                let term_docs: HashSet<u64> = entries
                    .iter()
                    .filter(|(_, col, _)| column.map(|c| c == *col).unwrap_or(true))
                    .map(|(doc_id, _, _)| *doc_id)
                    .collect();
                
                if first_term {
                    matching_docs = term_docs;
                    first_term = false;
                } else {
                    // AND semantics: keep only docs that have all terms
                    matching_docs = matching_docs.intersection(&term_docs).copied().collect();
                }
            } else {
                // Term not found, no matches possible
                return Ok(Vec::new());
            }
        }
        
        let mut result: Vec<u64> = matching_docs.into_iter().collect();
        result.sort();
        Ok(result)
    }
    
    /// Get document content
    pub fn get_document(&self, doc_id: u64) -> Option<&Vec<String>> {
        self.documents.get(&doc_id)
    }
    
    /// Get document count
    pub fn doc_count(&self) -> usize {
        self.documents.len()
    }
    
    /// Get term count
    pub fn term_count(&self) -> usize {
        self.inverted_index.len()
    }
}

/// Query parser for FTS MATCH expressions
pub struct FtsQueryParser;

impl FtsQueryParser {
    /// Parse a MATCH query string
    /// Supports: term1 term2 (AND), term1 OR term2, "phrase", column:term
    pub fn parse(query: &str) -> FtsQuery {
        let mut terms = Vec::new();
        let mut current = String::new();
        let mut in_quotes = false;
        
        for ch in query.chars() {
            match ch {
                '"' => {
                    if in_quotes {
                        if !current.is_empty() {
                            terms.push(FtsQueryTerm::Phrase(current.clone()));
                            current.clear();
                        }
                        in_quotes = false;
                    } else {
                        in_quotes = true;
                    }
                }
                ' ' if !in_quotes => {
                    if !current.is_empty() {
                        terms.push(Self::parse_term(&current));
                        current.clear();
                    }
                }
                _ => current.push(ch),
            }
        }
        
        if !current.is_empty() {
            terms.push(Self::parse_term(&current));
        }
        
        FtsQuery { terms }
    }
    
    fn parse_term(term: &str) -> FtsQueryTerm {
        if let Some(pos) = term.find(':') {
            let (col, val) = term.split_at(pos);
            FtsQueryTerm::Column(col.to_string(), val[1..].to_string())
        } else if term.to_uppercase() == "OR" {
            FtsQueryTerm::Or
        } else if term.to_uppercase() == "AND" {
            FtsQueryTerm::And
        } else if term.starts_with('-') {
            FtsQueryTerm::Not(term[1..].to_string())
        } else {
            FtsQueryTerm::Word(term.to_string())
        }
    }
}

/// FTS query
#[derive(Debug)]
pub struct FtsQuery {
    pub terms: Vec<FtsQueryTerm>,
}

/// FTS query term
#[derive(Debug)]
pub enum FtsQueryTerm {
    Word(String),
    Phrase(String),
    Column(String, String),
    And,
    Or,
    Not(String),
}

/// Highlight matches in text
pub fn highlight_matches(text: &str, query: &str) -> String {
    let terms = tokenize(query, TokenizerType::Simple);
    let mut result = text.to_string();
    
    for term in terms {
        // Simple case-insensitive replacement
        let lower_result = result.to_lowercase();
        let lower_term = term.to_lowercase();
        
        if let Some(pos) = lower_result.find(&lower_term) {
            let matched = &result[pos..pos + term.len()];
            result.replace_range(pos..pos + term.len(), &format!("**{}**", matched));
        }
    }
    
    result
}
