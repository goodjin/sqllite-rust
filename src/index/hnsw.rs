use rand::Rng;
// use std::collections::BinaryHeap;
use std::cmp::Ordering;
use super::{IndexError, Result};
use crate::pager::{PageId, Pager};
use crate::pager::page::PAGE_SIZE;

const NODE_SLOT_SIZE: usize = 2048;
const NODES_PER_PAGE: usize = PAGE_SIZE / NODE_SLOT_SIZE;

/// HNSW parameters
#[derive(Debug, Clone, Copy)]
pub struct HnswConfig {
    pub m: usize,               // Max number of outgoing connections per node
    pub m_max: usize,           // Max connections for layers > 0
    pub m_max0: usize,          // Max connections for layer 0
    pub ef_construction: usize, // Size of the dynamic candidate list for construction
    pub ef_search: usize,       // Size of the dynamic candidate list for search
    pub ml: f32,                // Level generation parameter
}

impl Default for HnswConfig {
    fn default() -> Self {
        Self {
            m: 16,
            m_max: 16,
            m_max0: 32,
            ef_construction: 100,
            ef_search: 50,
            ml: 1.0 / (16.0f32.ln()), // 1/ln(M)
        }
    }
}

/// Helper for priority queue
#[derive(Debug)]
struct NodeDist {
    id: u32,
    dist: f32,
}

impl PartialEq for NodeDist {
    fn eq(&self, other: &Self) -> bool {
        self.dist == other.dist
    }
}

impl Eq for NodeDist {}

impl PartialOrd for NodeDist {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for NodeDist {
    fn cmp(&self, other: &Self) -> Ordering {
        // Min-heap for search candidates, Max-heap for results
        self.dist.partial_cmp(&other.dist).unwrap_or(Ordering::Equal)
    }
}

/// A node in the HNSW graph
#[derive(Debug, Clone)]
pub struct HnswNode {
    pub id: u32,                  // Internal ID (index in the HNSW)
    pub rowid: u64,               // Pointer to the table record
    pub vector: Vec<f32>,
    pub level: usize,
    pub neighbors: Vec<Vec<u32>>, // neighbors[layer] = [node_ids...]
}

impl HnswNode {
    /// Serialize node to bytes
    pub fn to_bytes(&self, _dimension: usize, m_max0: usize) -> Vec<u8> {
        let mut bytes = vec![0u8; NODE_SLOT_SIZE];
        let mut offset = 0;

        // ID
        bytes[offset..offset+4].copy_from_slice(&self.id.to_le_bytes());
        offset += 4;

        // RowId
        bytes[offset..offset+8].copy_from_slice(&self.rowid.to_le_bytes());
        offset += 8;

        // Vector
        for &f in &self.vector {
            bytes[offset..offset+4].copy_from_slice(&f.to_le_bytes());
            offset += 4;
        }

        // Level
        bytes[offset] = self.level as u8;
        offset += 1;

        // Neighbors
        for layer in 0..=self.level {
            if let Some(layer_neighbors) = self.neighbors.get(layer) {
                for i in 0..m_max0 {
                    let neighbor_id = layer_neighbors.get(i).cloned().unwrap_or(0);
                    bytes[offset..offset+4].copy_from_slice(&neighbor_id.to_le_bytes());
                    offset += 4;
                }
            } else {
                offset += m_max0 * 4;
            }
        }

        bytes
    }

    /// Deserialize node from bytes
    pub fn from_bytes(bytes: &[u8], dimension: usize, m_max0: usize) -> Self {
        let mut offset = 0;

        // ID
        let id = u32::from_le_bytes(bytes[offset..offset+4].try_into().unwrap());
        offset += 4;

        // RowId
        let rowid = u64::from_le_bytes(bytes[offset..offset+8].try_into().unwrap());
        offset += 8;

        // Vector
        let mut vector = Vec::with_capacity(dimension);
        for _ in 0..dimension {
            vector.push(f32::from_le_bytes(bytes[offset..offset+4].try_into().unwrap()));
            offset += 4;
        }

        // Level
        let level = bytes[offset] as usize;
        offset += 1;

        // Neighbors
        let mut neighbors = Vec::with_capacity(level + 1);
        for _ in 0..=level {
            let mut layer_neighbors = Vec::new();
            for _ in 0..m_max0 {
                let neighbor_id = u32::from_le_bytes(bytes[offset..offset+4].try_into().unwrap());
                if neighbor_id != 0 {
                    layer_neighbors.push(neighbor_id);
                }
                offset += 4;
            }
            neighbors.push(layer_neighbors);
        }

        Self { id, rowid, vector, level, neighbors }
    }
}

/// HNSW index manager
pub struct HnswIndex {
    pub name: String,
    pub table_name: String,
    pub column_name: String,
    pub root_page: PageId, // Page 0 is the metadata page
    pub config: HnswConfig,
    pub dimension: usize,
}

impl HnswIndex {
    pub fn new(
        name: String,
        table_name: String,
        column_name: String,
        root_page: PageId,
        dimension: usize,
    ) -> Self {
        Self {
            name,
            table_name,
            column_name,
            root_page,
            config: HnswConfig::default(),
            dimension,
        }
    }

    /// Initialize the HNSW index metadata page
    pub fn init(&mut self, pager: &mut Pager) -> Result<()> {
        let mut page = pager.get_page(self.root_page)?;
        let data = page.as_mut_slice();
        
        // Header: "HNSW" (4 bytes)
        data[0..4].copy_from_slice(b"HNSW");
        // Version: 1 (2 bytes)
        data[4..6].copy_from_slice(&1u16.to_le_bytes());
        // Dimension (4 bytes)
        data[6..10].copy_from_slice(&(self.dimension as u32).to_le_bytes());
        // Entry Point ID (4 bytes)
        data[10..14].copy_from_slice(&0u32.to_le_bytes());
        // Max Level (1 byte)
        data[14] = 0;
        // Node Count (4 bytes)
        data[15..19].copy_from_slice(&0u32.to_le_bytes());

        pager.write_page(&page)?;
        Ok(())
    }

    fn get_metadata(&self, pager: &mut Pager) -> Result<(u32, usize, u32)> {
        let page = pager.get_page(self.root_page)?;
        let data = page.as_slice();
        let ep = u32::from_le_bytes(data[10..14].try_into().unwrap());
        let max_level = data[14] as usize;
        let count = u32::from_le_bytes(data[15..19].try_into().unwrap());
        Ok((ep, max_level, count))
    }

    fn save_metadata(&self, pager: &mut Pager, ep: u32, max_level: usize, count: u32) -> Result<()> {
        let mut page = pager.get_page(self.root_page)?;
        let data = page.as_mut_slice();
        data[10..14].copy_from_slice(&ep.to_le_bytes());
        data[14] = max_level as u8;
        data[15..19].copy_from_slice(&count.to_le_bytes());
        pager.write_page(&page)?;
        Ok(())
    }

    /// Get internal node by ID
    fn get_node(&self, pager: &mut Pager, node_id: u32) -> Result<HnswNode> {
        if node_id == 0 {
            return Err(IndexError::InvalidKey("Node ID 0 is invalid".to_string()));
        }

        let zero_based_id = node_id - 1;
        let page_index = (zero_based_id as usize / NODES_PER_PAGE) + 1;
        let slot_index = zero_based_id as usize % NODES_PER_PAGE;
        let page_id = self.root_page + page_index as u32;

        let page = pager.get_page(page_id)?;
        let offset = slot_index * NODE_SLOT_SIZE;
        let node_bytes = &page.as_slice()[offset..offset + NODE_SLOT_SIZE];

        Ok(HnswNode::from_bytes(node_bytes, self.dimension, self.config.m_max0))
    }

    /// Write internal node by ID
    fn save_node(&self, pager: &mut Pager, node: &HnswNode) -> Result<()> {
        let zero_based_id = node.id - 1;
        let page_index = (zero_based_id as usize / NODES_PER_PAGE) + 1;
        let slot_index = zero_based_id as usize % NODES_PER_PAGE;
        let offset = slot_index * NODE_SLOT_SIZE;
        let page_id = self.root_page + page_index as u32;

        // We might need to allocate pages if this is a new node ID
        // Pager keeps track of database size in its header
        while page_id >= pager.header().database_size {
            let _ = pager.allocate_page()?;
        }

        let mut page = pager.get_page(page_id)?;
        let offset = slot_index * NODE_SLOT_SIZE;
        let node_bytes = node.to_bytes(self.dimension, self.config.m_max0);
        page.as_mut_slice()[offset..offset + NODE_SLOT_SIZE].copy_from_slice(&node_bytes);

        pager.write_page(&page)?;
        Ok(())
    }

    fn random_level(&self) -> usize {
        let mut rng = rand::thread_rng();
        let r: f32 = rng.gen();
        if r == 0.0 { return self.config.m_max; }
        let level = - (r.ln() * self.config.ml) as usize;
        level.min(self.config.m_max)
    }

    /// Search layer for the nearest node
    fn search_layer(&self, pager: &mut Pager, query: &[f32], entry_point: u32, layer: usize) -> Result<u32> {
        let mut curr_node_id = entry_point;
        let mut curr_node = self.get_node(pager, curr_node_id)?;
        let mut curr_dist = l2_distance(query, &curr_node.vector);
        let mut changed = true;

        while changed {
            changed = false;
            let current_layer_neighbors = curr_node.neighbors[layer].clone();
            for neighbor_id in current_layer_neighbors {
                let neighbor = self.get_node(pager, neighbor_id)?;
                let dist = l2_distance(query, &neighbor.vector);
                if dist < curr_dist {
                    curr_dist = dist;
                    curr_node_id = neighbor_id;
                    curr_node = neighbor;
                    changed = true;
                }
            }
        }
        Ok(curr_node_id)
    }

    /// Insert a vector into the HNSW index
    pub fn insert(&mut self, pager: &mut Pager, vector: &[f32], rowid: u64) -> Result<()> {
        let (mut ep, mut max_level, count) = self.get_metadata(pager)?;
        let node_id = count + 1;
        let new_level = self.random_level();

        let mut new_node = HnswNode {
            id: node_id,
            rowid,
            vector: vector.to_vec(),
            level: new_level,
            neighbors: vec![Vec::new(); new_level + 1],
        };

        if ep == 0 {
            // First node
            self.save_node(pager, &new_node)?;
            self.save_metadata(pager, node_id, new_level, node_id)?;
            return Ok(());
        }

        // Greedy search from top to new_level + 1
        let mut curr_ep = ep;
        for layer in (new_level + 1..=max_level).rev() {
            curr_ep = self.search_layer(pager, vector, curr_ep, layer)?;
        }

        // From new_level down to 0, connect neighbors
        for layer in (0..=new_level.min(max_level)).rev() {
            // simplified: connect to the greedy winner for now
            // proper HNSW would find ef_construction candidates
            let neighbor_id = self.search_layer(pager, vector, curr_ep, layer)?;
            let mut neighbor = self.get_node(pager, neighbor_id)?;
            
            // Bi-directional link
            new_node.neighbors[layer].push(neighbor_id);
            neighbor.neighbors[layer].push(node_id);
            
            self.save_node(pager, &neighbor)?;
            curr_ep = neighbor_id;
        }

        self.save_node(pager, &new_node)?;
        
        // Update metadata if needed
        if new_level > max_level {
            ep = node_id;
            max_level = new_level;
        }
        self.save_metadata(pager, ep, max_level, node_id)?;

        Ok(())
    }

    /// Search for nearest neighbors
    pub fn search(&self, pager: &mut Pager, query: &[f32], _k: usize) -> Result<Vec<(u64, f32)>> {
        let (ep, max_level, _) = self.get_metadata(pager)?;
        if ep == 0 { return Ok(Vec::new()); }

        let mut curr_ep = ep;
        for layer in (1..=max_level).rev() {
            curr_ep = self.search_layer(pager, query, curr_ep, layer)?;
        }

        // Layer 0 search
        // Simplified: return best from greedy search
        // proper HNSW would maintain a candidate list of size ef_search
        let best_id = self.search_layer(pager, query, curr_ep, 0)?;
        let node = self.get_node(pager, best_id)?;
        let dist = l2_distance(query, &node.vector);
        
        Ok(vec![(node.rowid, dist)])
    }
}

fn l2_distance(v1: &[f32], v2: &[f32]) -> f32 {
    v1.iter()
        .zip(v2.iter())
        .map(|(x, y)| (x - y).powi(2))
        .sum::<f32>()
        .sqrt()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pager::Pager;
    use tempfile::NamedTempFile;

    #[test]
    fn test_hnsw_basic() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap();
        let mut pager = Pager::open(path).expect("Failed to open pager");
        
        let mut index = HnswIndex::new(
            "idx_vec".to_string(),
            "embeddings".to_string(),
            "vec".to_string(),
            0,
            3,
        );
        
        index.init(&mut pager).expect("Failed to init index");
        
        // Insert vectors
        index.insert(&mut pager, &[1.0, 2.0, 3.0], 1).expect("Failed to insert node 1");
        index.insert(&mut pager, &[4.0, 5.0, 6.0], 2).expect("Failed to insert node 2");
        index.insert(&mut pager, &[1.1, 2.1, 3.1], 3).expect("Failed to insert node 3");
        
        // Search
        let results = index.search(&mut pager, &[1.01, 2.01, 3.01], 1).expect("Failed to search");
        assert!(!results.is_empty());
        let rowid = results[0].0;
        assert!(rowid == 1 || rowid == 3);
    }
}
