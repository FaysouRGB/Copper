use std::collections::BTreeMap;

pub struct LSMTree {
    pub memtable: Memtable,
    pub levels: Vec<Level>,
}

pub struct Level {
    pub sstables: Vec<SSTable>,
}

pub struct SSTable {
    pub data: BTreeMap<Vec<u8>, Vec<u8>>,
}

impl LSMTree {
    pub fn new() -> LSMTree;

    pub fn insert(&mut self, key: Vec<u8>, value: Vec<u8>);

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>>;

    pub fn delete(&mut self, key: &[u8]);
}

impl Level {
    pub fn new() -> Level;
}

impl SSTable {
    pub fn new() -> SSTable;
}
