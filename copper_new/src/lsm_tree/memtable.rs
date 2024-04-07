use super::entry::Entry;
use bloomfilter::Bloom;
use std::{collections::BTreeMap, fmt::Debug};

pub struct Memtable {
    entries: BTreeMap<Vec<u8>, Entry>,
    size: usize,
    max_size: usize,
    bloom_filter: Bloom<Vec<u8>>,
}

impl Default for Memtable {
    fn default() -> Self {
        Self::new()
    }
}

impl Memtable {
    pub fn new() -> Self {
        Self { entries: BTreeMap::new(), size: 0, max_size: 32, bloom_filter: Bloom::new_for_fp_rate(1000, 0.01) }
    }

    pub fn get_all_entries(&self) -> Vec<Entry> {
        self.entries.iter().map(|(key, entry)| Entry::new(key, entry.get_value(), entry.is_deleted())).collect()
    }

    pub fn insert(&mut self, key: &[u8], value: &[u8], deleted: bool) -> bool {
        self.bloom_filter.set(&key.to_vec());

        let entry = Entry::new(key, value, deleted);
        let previous_entry = self.entries.insert(key.to_vec(), Entry::new(entry.get_key(), entry.get_value(), entry.is_deleted()));
        match previous_entry {
            Some(previous_entry) => {
                self.size = self.size + entry.get_size() - previous_entry.get_size();
                true
            }
            None => {
                self.size += entry.get_size();
                false
            }
        }
    }

    pub fn get(&self, key: &[u8]) -> Option<Entry> {
        if !self.bloom_filter.check(&key.to_vec()) {
            return None;
        }

        let entry = self.entries.get(key);
        entry.map(|entry| Entry::new(entry.get_key(), entry.get_value(), entry.is_deleted()))
    }

    pub fn get_range(&self, start: &[u8], end: &[u8]) -> Vec<Entry> {
        let start_key = start.to_vec();
        let end_key = end.to_vec();

        self.entries.range(start_key..=end_key).filter_map(|(key, entry)| if entry.is_deleted() { None } else { Some(Entry::new(key, entry.get_value(), entry.is_deleted())) }).collect()
    }

    pub fn is_full(&self) -> bool {
        self.size >= self.max_size
    }

    pub fn get_size(&self) -> usize {
        self.size
    }

    pub fn get_entries(&self) -> &BTreeMap<Vec<u8>, Entry> {
        &self.entries
    }

    pub fn clear(&mut self) {
        self.entries.clear();
        self.size = 0;
        self.bloom_filter.clear();
    }
}

impl Debug for Memtable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Memtable").field("size", &self.size).field("max_size", &self.max_size).field("entries", &self.entries).finish()
    }
}
