use std::collections::BTreeMap;

pub struct Database {
    pub lsm_tree: LSMTree,
    pub bloom_filter: BloomFilter,
    pub write_ahead_log: WriteAheadLog,
}

impl Database {
    // Create a new database.
    pub fn new() -> Database;

    // Inserts a pair in the database.
    pub fn insert(&mut self, key: Vec<u8>, value: Vec<u8>);

    // Get a pair from the database.
    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>>;

    // Delete a pair from the database.
    pub fn delete(&mut self, key: &[u8]);

    // That is it for now...
}
