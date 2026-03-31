//! P5-7: R-Tree Spatial Index Implementation
//!
//! R-Tree is a spatial index for efficient range and nearest-neighbor queries.

use std::collections::HashMap;

pub mod error;
pub use error::{RtreeError, Result};

/// 2D Bounding box
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct BoundingBox {
    pub min_x: f64,
    pub max_x: f64,
    pub min_y: f64,
    pub max_y: f64,
}

impl BoundingBox {
    /// Create a new bounding box
    pub fn new(min_x: f64, max_x: f64, min_y: f64, max_y: f64) -> Self {
        Self { min_x, max_x, min_y, max_y }
    }
    
    /// Check if this box intersects with another
    pub fn intersects(&self, other: &BoundingBox) -> bool {
        self.min_x <= other.max_x && self.max_x >= other.min_x &&
        self.min_y <= other.max_y && self.max_y >= other.min_y
    }
    
    /// Check if this box contains a point
    pub fn contains_point(&self, x: f64, y: f64) -> bool {
        x >= self.min_x && x <= self.max_x && y >= self.min_y && y <= self.max_y
    }
    
    /// Check if this box completely contains another box
    pub fn contains(&self, other: &BoundingBox) -> bool {
        self.min_x <= other.min_x && self.max_x >= other.max_x &&
        self.min_y <= other.min_y && self.max_y >= other.max_y
    }
    
    /// Calculate area
    pub fn area(&self) -> f64 {
        (self.max_x - self.min_x) * (self.max_y - self.min_y)
    }
    
    /// Calculate expanded area if we add another box
    pub fn expanded_area(&self, other: &BoundingBox) -> f64 {
        let min_x = self.min_x.min(other.min_x);
        let max_x = self.max_x.max(other.max_x);
        let min_y = self.min_y.min(other.min_y);
        let max_y = self.max_y.max(other.max_y);
        (max_x - min_x) * (max_y - min_y)
    }
    
    /// Create a box that contains both boxes
    pub fn combine(&self, other: &BoundingBox) -> BoundingBox {
        BoundingBox {
            min_x: self.min_x.min(other.min_x),
            max_x: self.max_x.max(other.max_x),
            min_y: self.min_y.min(other.min_y),
            max_y: self.max_y.max(other.max_y),
        }
    }
    
    /// Calculate center point
    pub fn center(&self) -> (f64, f64) {
        ((self.min_x + self.max_x) / 2.0, (self.min_y + self.max_y) / 2.0)
    }
    
    /// Calculate distance from a point to the nearest edge
    pub fn distance_to_point(&self, x: f64, y: f64) -> f64 {
        let dx = if x < self.min_x {
            self.min_x - x
        } else if x > self.max_x {
            x - self.max_x
        } else {
            0.0
        };
        
        let dy = if y < self.min_y {
            self.min_y - y
        } else if y > self.max_y {
            y - self.max_y
        } else {
            0.0
        };
        
        (dx * dx + dy * dy).sqrt()
    }
}

/// R-Tree node
#[derive(Debug)]
#[derive(Clone)]
enum RtreeNode {
    /// Internal node: (bbox, child_id)
    Internal {
        entries: Vec<(BoundingBox, u64)>,
    },
    /// Leaf node: (bbox, object_id)
    Leaf {
        entries: Vec<(BoundingBox, u64)>,
    },
}

/// R-Tree index
#[derive(Debug)]
pub struct RtreeIndex {
    name: String,
    root_id: u64,
    nodes: HashMap<u64, RtreeNode>,
    next_node_id: u64,
    max_entries: usize,  // Maximum entries per node
    min_entries: usize,  // Minimum entries per node (except root)
}

impl RtreeIndex {
    /// Create a new R-Tree index
    pub fn new(name: String) -> Self {
        let mut index = Self {
            name,
            root_id: 0,
            nodes: HashMap::new(),
            next_node_id: 1,
            max_entries: 8,
            min_entries: 2,
        };
        
        // Create root node as empty leaf
        index.nodes.insert(0, RtreeNode::Leaf { entries: Vec::new() });
        index
    }
    
    /// Insert an object with bounding box
    pub fn insert(&mut self, bbox: BoundingBox, object_id: u64) -> Result<()> {
        self.insert_recursive(self.root_id, bbox, object_id)?;
        Ok(())
    }
    
    fn insert_recursive(&mut self, node_id: u64, bbox: BoundingBox, object_id: u64) -> Result<Option<(BoundingBox, u64)>> {
        // Clone the node data first to avoid borrow issues
        let node_data = self.nodes.get(&node_id).cloned()
            .ok_or(RtreeError::InvalidNode(node_id))?;
        
        match node_data {
            RtreeNode::Leaf { mut entries } => {
                entries.push((bbox, object_id));
                
                // Check if we need to split
                if entries.len() > self.max_entries {
                    // Split the node
                    let (new_bbox, new_node_id) = self.split_leaf(node_id, entries)?;
                    Ok(Some((new_bbox, new_node_id)))
                } else {
                    self.nodes.insert(node_id, RtreeNode::Leaf { entries });
                    Ok(None)
                }
            }
            RtreeNode::Internal { entries } => {
                // Find best child to insert into (least area enlargement)
                let best_child = entries.iter()
                    .map(|(child_bbox, child_id)| {
                        let area_increase = child_bbox.expanded_area(&bbox) - child_bbox.area();
                        (area_increase, *child_id)
                    })
                    .min_by(|a, b| a.0.partial_cmp(&b.0).unwrap())
                    .map(|(_, child_id)| child_id)
                    .unwrap_or(entries[0].1);
                
                // Recursively insert
                if let Some((new_bbox, new_node_id)) = self.insert_recursive(best_child, bbox, object_id)? {
                    // Child was split, add new entry
                    let mut new_entries = entries.clone();
                    new_entries.push((new_bbox, new_node_id));
                    
                    if new_entries.len() > self.max_entries {
                        let (split_bbox, split_node_id) = self.split_internal(node_id, new_entries)?;
                        Ok(Some((split_bbox, split_node_id)))
                    } else {
                        self.nodes.insert(node_id, RtreeNode::Internal { entries: new_entries });
                        Ok(None)
                    }
                } else {
                    Ok(None)
                }
            }
        }
    }
    
    fn split_leaf(&mut self, node_id: u64, entries: Vec<(BoundingBox, u64)>) -> Result<(BoundingBox, u64)> {
        // Linear split: sort by min_x and split in half
        let mut sorted = entries.clone();
        sorted.sort_by(|a, b| a.0.min_x.partial_cmp(&b.0.min_x).unwrap());
        
        let mid = sorted.len() / 2;
        let left_entries: Vec<_> = sorted[..mid].to_vec();
        let right_entries: Vec<_> = sorted[mid..].to_vec();
        
        // Calculate bounding boxes
        let left_bbox = Self::compute_bbox(&left_entries);
        let right_bbox = Self::compute_bbox(&right_entries);
        
        // Create new node
        let new_node_id = self.next_node_id;
        self.next_node_id += 1;
        
        self.nodes.insert(node_id, RtreeNode::Leaf { entries: left_entries });
        self.nodes.insert(new_node_id, RtreeNode::Leaf { entries: right_entries });
        
        Ok((right_bbox, new_node_id))
    }
    
    fn split_internal(&mut self, node_id: u64, entries: Vec<(BoundingBox, u64)>) -> Result<(BoundingBox, u64)> {
        let mut sorted = entries.clone();
        sorted.sort_by(|a, b| a.0.min_x.partial_cmp(&b.0.min_x).unwrap());
        
        let mid = sorted.len() / 2;
        let left_entries: Vec<_> = sorted[..mid].to_vec();
        let right_entries: Vec<_> = sorted[mid..].to_vec();
        
        let left_bbox = Self::compute_bbox(&left_entries);
        let right_bbox = Self::compute_bbox(&right_entries);
        
        let new_node_id = self.next_node_id;
        self.next_node_id += 1;
        
        self.nodes.insert(node_id, RtreeNode::Internal { entries: left_entries });
        self.nodes.insert(new_node_id, RtreeNode::Internal { entries: right_entries });
        
        Ok((right_bbox, new_node_id))
    }
    
    fn compute_bbox(entries: &[(BoundingBox, u64)]) -> BoundingBox {
        if entries.is_empty() {
            return BoundingBox::new(0.0, 0.0, 0.0, 0.0);
        }
        
        let mut min_x = entries[0].0.min_x;
        let mut max_x = entries[0].0.max_x;
        let mut min_y = entries[0].0.min_y;
        let mut max_y = entries[0].0.max_y;
        
        for (bbox, _) in entries.iter().skip(1) {
            min_x = min_x.min(bbox.min_x);
            max_x = max_x.max(bbox.max_x);
            min_y = min_y.min(bbox.min_y);
            max_y = max_y.max(bbox.max_y);
        }
        
        BoundingBox::new(min_x, max_x, min_y, max_y)
    }
    
    /// Search for objects intersecting a bounding box
    pub fn search_range(&self, bbox: BoundingBox) -> Vec<u64> {
        let mut results = Vec::new();
        self.search_range_recursive(self.root_id, bbox, &mut results);
        results
    }
    
    fn search_range_recursive(&self, node_id: u64, bbox: BoundingBox, results: &mut Vec<u64>) {
        if let Some(node) = self.nodes.get(&node_id) {
            match node {
                RtreeNode::Leaf { entries } => {
                    for (entry_bbox, object_id) in entries {
                        if entry_bbox.intersects(&bbox) {
                            results.push(*object_id);
                        }
                    }
                }
                RtreeNode::Internal { entries } => {
                    for (child_bbox, child_id) in entries {
                        if child_bbox.intersects(&bbox) {
                            self.search_range_recursive(*child_id, bbox, results);
                        }
                    }
                }
            }
        }
    }
    
    /// Find k nearest neighbors to a point
    pub fn nearest_neighbors(&self, x: f64, y: f64, k: usize) -> Vec<(u64, f64)> {
        let mut candidates: Vec<(u64, f64)> = Vec::new();
        self.nn_recursive(self.root_id, x, y, &mut candidates);
        
        // Sort by distance and take k
        candidates.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        candidates.truncate(k);
        candidates
    }
    
    fn nn_recursive(&self, node_id: u64, x: f64, y: f64, candidates: &mut Vec<(u64, f64)>) {
        if let Some(node) = self.nodes.get(&node_id) {
            match node {
                RtreeNode::Leaf { entries } => {
                    for (entry_bbox, object_id) in entries {
                        let dist = entry_bbox.distance_to_point(x, y);
                        candidates.push((*object_id, dist));
                    }
                }
                RtreeNode::Internal { entries } => {
                    // Sort children by distance to minimize search
                    let mut children: Vec<_> = entries.iter()
                        .map(|(bbox, id)| (bbox.distance_to_point(x, y), *id))
                        .collect();
                    children.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
                    
                    for (_, child_id) in children {
                        self.nn_recursive(child_id, x, y, candidates);
                    }
                }
            }
        }
    }
    
    /// Delete an object from the index
    pub fn delete(&mut self, object_id: u64) -> Result<()> {
        let deleted = self.delete_recursive(self.root_id, object_id)?;
        if deleted {
            Ok(())
        } else {
            Err(RtreeError::ObjectNotFound(object_id))
        }
    }
    
    fn delete_recursive(&mut self, node_id: u64, object_id: u64) -> Result<bool> {
        if let Some(node) = self.nodes.get_mut(&node_id) {
            match node {
                RtreeNode::Leaf { entries } => {
                    let original_len = entries.len();
                    entries.retain(|(_, id)| *id != object_id);
                    Ok(entries.len() < original_len)
                }
                RtreeNode::Internal { entries } => {
                    for (_, child_id) in entries.clone() {
                        if self.delete_recursive(child_id, object_id)? {
                            return Ok(true);
                        }
                    }
                    Ok(false)
                }
            }
        } else {
            Ok(false)
        }
    }
    
    /// Get statistics about the index
    pub fn stats(&self) -> RtreeStats {
        let mut stats = RtreeStats {
            node_count: self.nodes.len(),
            object_count: 0,
            height: 0,
        };
        
        // Count objects in leaf nodes
        for node in self.nodes.values() {
            if let RtreeNode::Leaf { entries } = node {
                stats.object_count += entries.len();
            }
        }
        
        stats
    }
}

/// R-Tree statistics
#[derive(Debug)]
pub struct RtreeStats {
    pub node_count: usize,
    pub object_count: usize,
    pub height: usize,
}
