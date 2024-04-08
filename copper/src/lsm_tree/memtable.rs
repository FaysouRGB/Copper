use super::entry::Entry;
use bloomfilter::Bloom;
use std::{collections::BTreeMap, fmt::Debug};

/// `Memtable` struct represents an in-memory table in a database.
/// It has a `BTreeMap` of entries, a size, a maximum size, and a bloom filter.
pub struct Memtable {
    entries: BTreeMap<Vec<u8>, Entry>,
    size: usize,
    max_size: usize,
    bloom_filter: Bloom<Vec<u8>>,
}

impl Default for Memtable {
    /// Creates a new `Memtable` with default values.
    fn default() -> Self {
        Self::new()
    }
}

impl Memtable {
    /// Creates a new `Memtable` with an empty `BTreeMap` of entries, a size of 0, a maximum size of 32, and a new bloom filter.
    pub fn new() -> Self {
        Self { entries: BTreeMap::new(), size: 0, max_size: 32, bloom_filter: Bloom::new_for_fp_rate(1000, 0.01) }
    }

    /// Returns all entries in the `Memtable`.
    pub fn get_all_entries(&self) -> Vec<Entry> {
        self.entries.iter().map(|(key, entry)| Entry::new(key, entry.get_value(), entry.is_deleted())).collect()
    }

    /// Inserts a new entry into the `Memtable`.
    ///
    /// # Arguments
    ///
    /// * `key` - A byte slice that holds the key of the entry.
    /// * `value` - A byte slice that holds the value of the entry.
    /// * `deleted` - A boolean indicating whether the entry is deleted.
    ///
    /// Returns `true` if an entry with the same key already exists and is replaced, `false` otherwise.
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

    /// Returns the entry with the given key if it exists and the bloom filter indicates that it might be in the `Memtable`.
    ///
    /// # Arguments
    ///
    /// * `key` - A byte slice that holds the key of the entry.
    pub fn get(&self, key: &[u8]) -> Option<Entry> {
        if !self.bloom_filter.check(&key.to_vec()) {
            return None;
        }

        let entry = self.entries.get(key);
        entry.map(|entry| Entry::new(entry.get_key(), entry.get_value(), entry.is_deleted()))
    }

    /// Returns a range of entries in the `Memtable` from the start key to the end key, inclusive.
    ///
    /// # Arguments
    ///
    /// * `start` - A byte slice that holds the start key of the range.
    /// * `end` - A byte slice that holds the end key of the range.
    ///
    /// The function does not return deleted entries.
    pub fn get_range(&self, start: &[u8], end: &[u8]) -> Vec<Entry> {
        let start_key = start.to_vec();
        let end_key = end.to_vec();

        self.entries.range(start_key..=end_key).filter_map(|(key, entry)| if entry.is_deleted() { None } else { Some(Entry::new(key, entry.get_value(), entry.is_deleted())) }).collect()
    }

    /// Returns whether the `Memtable` is full, i.e., its size is greater than or equal to its maximum size.
    pub fn is_full(&self) -> bool {
        self.size >= self.max_size
    }

    /// Returns the size of the `Memtable`.
    pub fn get_size(&self) -> usize {
        self.size
    }

    /// Returns a reference to the `BTreeMap` of entries in the `Memtable`.
    pub fn get_entries(&self) -> &BTreeMap<Vec<u8>, Entry> {
        &self.entries
    }

    /// Clears the `Memtable`, removing all entries, setting the size to 0, and clearing the bloom filter.
    pub fn clear(&mut self) {
        self.entries.clear();
        self.size = 0;
        self.bloom_filter.clear();
    }
}

impl Debug for Memtable {
    /// Formats the `Memtable` for printing.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Memtable").field("size", &self.size).field("max_size", &self.max_size).field("entries", &self.entries).finish()
    }
}
