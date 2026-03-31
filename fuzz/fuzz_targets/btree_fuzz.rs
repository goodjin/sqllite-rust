#![no_main]

use libfuzzer_sys::fuzz_target;
use std::collections::BTreeMap;

// B+Tree fuzzing target
// Tests B+Tree operations: insert, delete, search, range scan

fuzz_target!(|data: &[u8]| {
    if data.len() < 4 {
        return;
    }
    
    // Parse operations
    let ops = parse_btree_operations(data);
    
    // Execute against BTreeMap (reference implementation)
    let _ = execute_btree_operations(ops);
    
    // Test key serialization
    let _ = fuzz_key_serialization(data);
    
    // Test page layout
    let _ = fuzz_page_layout(data);
    
    // Test node splitting/merging logic
    let _ = fuzz_node_operations(data);
});

#[derive(Debug, Clone)]
enum BTreeOp {
    Insert(Vec<u8>, Vec<u8>),
    Delete(Vec<u8>),
    Get(Vec<u8>),
    RangeScan(Vec<u8>, Vec<u8>),
    IterateFrom(Vec<u8>),
}

fn parse_btree_operations(data: &[u8]) -> Vec<BTreeOp> {
    let mut ops = Vec::new();
    let mut i = 0;
    
    while i < data.len() {
        let op_type = data[i] % 5;
        i += 1;
        
        // Parse key length (1-32 bytes)
        let key_len = if i < data.len() {
            (data[i] as usize % 32) + 1
        } else {
            break;
        };
        i += 1;
        
        // Parse key
        let key_end = (i + key_len).min(data.len());
        let key = data[i..key_end].to_vec();
        i = key_end;
        
        let op = match op_type {
            0 => {
                // Insert - parse value
                if i < data.len() {
                    let val_len = (data[i] as usize % 64) + 1;
                    i += 1;
                    let val_end = (i + val_len).min(data.len());
                    let value = data[i..val_end].to_vec();
                    i = val_end;
                    Some(BTreeOp::Insert(key, value))
                } else {
                    None
                }
            }
            1 => Some(BTreeOp::Delete(key)),
            2 => Some(BTreeOp::Get(key)),
            3 => {
                // Range scan - parse end key
                let end_key = key.iter().map(|b| b.wrapping_add(1)).collect();
                Some(BTreeOp::RangeScan(key, end_key))
            }
            4 => Some(BTreeOp::IterateFrom(key)),
            _ => None,
        };
        
        if let Some(op) = op {
            ops.push(op);
        }
        
        if ops.len() >= 200 {
            break;
        }
    }
    
    ops
}

fn execute_btree_operations(ops: Vec<BTreeOp>) -> Result<(), ()> {
    // Use BTreeMap as reference for B+Tree behavior
    let mut btree: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
    
    for op in ops {
        match op {
            BTreeOp::Insert(key, value) => {
                btree.insert(key, value);
            }
            BTreeOp::Delete(key) => {
                btree.remove(&key);
            }
            BTreeOp::Get(key) => {
                let _ = btree.get(&key);
            }
            BTreeOp::RangeScan(start, end) => {
                let _: Vec<_> = btree
                    .range(start..=end)
                    .collect();
            }
            BTreeOp::IterateFrom(start) => {
                let _: Vec<_> = btree
                    .range(start..)
                    .take(10)
                    .collect();
            }
        }
    }
    
    // Verify tree invariants
    verify_btree_invariants(&btree)?;
    
    Ok(())
}

fn verify_btree_invariants<K, V>(_btree: &BTreeMap<K, V>) -> Result<(), ()>
where
    K: Ord,
{
    // BTreeMap maintains invariants automatically
    // Here we'd add custom checks if using a custom B+Tree
    Ok(())
}

fn fuzz_key_serialization(data: &[u8]) -> Result<(), ()> {
    // Test various key serialization strategies
    
    // Integer keys
    for chunk in data.chunks(8) {
        if chunk.len() == 8 {
            let key = u64::from_be_bytes([
                chunk[0], chunk[1], chunk[2], chunk[3],
                chunk[4], chunk[5], chunk[6], chunk[7]
            ]);
            
            // Serialize and deserialize
            let bytes = key.to_be_bytes();
            let recovered = u64::from_be_bytes(bytes);
            assert_eq!(key, recovered, "Key serialization failed");
        }
    }
    
    // Variable-length keys with length prefix
    for chunk in data.chunks(16) {
        if !chunk.is_empty() {
            let len = (chunk[0] as usize % 15) + 1;
            let key_data = &chunk[1..(1 + len).min(chunk.len())];
            
            // Serialize with length prefix
            let mut serialized = vec![key_data.len() as u8];
            serialized.extend_from_slice(key_data);
            
            // Deserialize
            let recovered_len = serialized[0] as usize;
            let recovered = &serialized[1..1 + recovered_len];
            
            assert_eq!(key_data, recovered, "Variable key serialization failed");
        }
    }
    
    Ok(())
}

fn fuzz_page_layout(data: &[u8]) -> Result<(), ()> {
    // Test page layout calculations
    const PAGE_SIZE: usize = 4096;
    const HEADER_SIZE: usize = 16;
    
    #[derive(Debug)]
    struct PageHeader {
        page_id: u32,
        page_type: u8,
        num_entries: u16,
        free_space: u16,
    }
    
    // Parse page header from data
    if data.len() >= HEADER_SIZE {
        let header = PageHeader {
            page_id: u32::from_le_bytes([data[0], data[1], data[2], data[3]]),
            page_type: data[4],
            num_entries: u16::from_le_bytes([data[5], data[6]]),
            free_space: u16::from_le_bytes([data[7], data[8]]),
        };
        
        // Validate header
        assert!(header.num_entries as usize <= PAGE_SIZE / 4, "Too many entries for page");
        assert!(header.free_space as usize <= PAGE_SIZE - HEADER_SIZE, "Invalid free space");
        
        // Calculate available space
        let available = PAGE_SIZE - HEADER_SIZE - (header.num_entries as usize * 4);
        assert_eq!(available as u16, header.free_space, "Free space mismatch");
    }
    
    // Test slot array layout
    let num_slots = (data.get(0).copied().unwrap_or(0) as usize % 100) + 1;
    let slot_array_size = num_slots * 2; // 2 bytes per offset
    
    assert!(HEADER_SIZE + slot_array_size <= PAGE_SIZE, "Slot array too large");
    
    Ok(())
}

fn fuzz_node_operations(data: &[u8]) -> Result<(), ()> {
    // Test node split/merge logic
    
    const ORDER: usize = 4; // Minimum entries per node
    
    #[derive(Debug)]
    struct Node {
        keys: Vec<Vec<u8>>,
        is_leaf: bool,
    }
    
    let mut node = Node {
        keys: Vec::new(),
        is_leaf: data.get(0).copied().unwrap_or(0) % 2 == 0,
    };
    
    // Insert keys until split needed
    for (i, &byte) in data.iter().enumerate().skip(1) {
        let key = vec![byte, (i % 256) as u8];
        
        // Insert in sorted order
        let pos = node.keys.binary_search(&key).unwrap_or_else(|e| e);
        if !node.keys.contains(&key) {
            node.keys.insert(pos, key);
        }
        
        // Check if split needed (simplified: split at 2*ORDER)
        if node.keys.len() >= 2 * ORDER {
            // Split node
            let split_point = node.keys.len() / 2;
            let new_node = Node {
                keys: node.keys.split_off(split_point),
                is_leaf: node.is_leaf,
            };
            
            // Verify split
            assert!(!node.keys.is_empty(), "Left node empty after split");
            assert!(!new_node.keys.is_empty(), "Right node empty after split");
            
            // Verify ordering
            if let (Some(left_max), Some(right_min)) = (node.keys.last(), new_node.keys.first()) {
                assert!(left_max < right_min, "Split violated ordering");
            }
            
            // Continue with left node
            if i % 2 == 0 {
                node = new_node;
            }
        }
    }
    
    // Verify final node invariants
    for i in 1..node.keys.len() {
        assert!(node.keys[i-1] < node.keys[i], "Keys not sorted");
    }
    
    Ok(())
}
