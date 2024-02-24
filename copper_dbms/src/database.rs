use crate::lsm_tree::LSMTree;
use bloomfilter::Bloom;
use std::path::Path;

// Constants:
const BLOOM_FILTER_ITEM_SIZE: usize = 100;
const BLOOM_FILTER_FALSE_POSITIVE_RATE: f64 = 0.1;

pub struct Database {
    pub lsm_tree: LSMTree,
    pub bloom: Bloom<u8>,
}

impl Database {
    // Create a new database.
    pub fn new() -> Database {
        Database { lsm_tree: LSMTree::new(), bloom: Bloom::new_for_fp_rate(BLOOM_FILTER_ITEM_SIZE, BLOOM_FILTER_FALSE_POSITIVE_RATE) }
    }

    // Load database from disk.
    pub fn load(filepath: &Path) -> Database {
        todo!();
    }

    // Save database to disk.
    pub fn save(filepath: &Path) -> Database {
        todo!();
    }

    // Insert a key-value pair in the LSM-Tree.
    pub fn insert(&mut self, key: Vec<u8>, value: Vec<u8>) {
        self.lsm_tree.insert(key, value);
    }

    // Try to get the value associated to the key if it exists.
    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.lsm_tree.get(key)
    }

    // Delete a key-value pair from the LSM-Tree.
    pub fn delete(&mut self, key: &[u8]) {
        self.lsm_tree.delete(key);
    }
}
