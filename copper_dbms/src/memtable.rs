use std::collections::BTreeMap;

use bloomfilter::Bloom;

use crate::entry::Entry;

/// Represents an in-memory table of database entries.
///
/// A `Memtable` consists of a map of entries and the total size of all entries.
///
/// # Fields
///
/// * `entries` - A `HashMap` where the key is a `Vec<u8>` and the value is an `Entry`.
/// * `size` - The total size of all entries in the `Memtable`.
pub struct Memtable {
    entries: BTreeMap<Vec<u8>, Entry>,
    size: usize,
    bloom_filter: Bloom<Vec<u8>>,
}

impl Memtable {
    pub fn new() -> Self {
        Self { entries: BTreeMap::new(), size: 0, bloom_filter: Bloom::new_for_fp_rate(1000, 0.01) }
    }

    /// Inserts an `Entry` into the `Memtable`.
    ///
    /// If an entry with the same key already exists in the `Memtable`, it is replaced.
    ///
    /// # Arguments
    ///
    /// * `entry` - The `Entry` to be inserted.
    ///
    /// # Returns
    ///
    /// `true` if an entry with the same key was already present, `false` otherwise.
    pub fn insert(&mut self, key: &[u8], value: &[u8], deleted: bool) -> bool {
        // Update the bloom filter on add
        if !deleted {
            self.bloom_filter.set(&key.to_vec());
        }

        // Create the entry
        let entry = Entry::new(key, value, deleted);

        // Insert it into the memtable and get the potential previous value
        let previous_entry = self.entries.insert(key.to_vec(), entry);

        // Update the size
        match previous_entry {
            Some(previous_entry) => {
                if previous_entry.get_size() > entry.get_size() {
                    self.size -= previous_entry.get_size() - entry.get_size();
                } else {
                    self.size += entry.get_size() - previous_entry.get_size();
                }
                true
            }
            None => {
                self.size += entry.get_size();
                false
            }
        }
    }

    /// Returns a reference to the `Entry` for a given key, if it exists and is not marked as deleted.
    ///
    /// # Arguments
    ///
    /// * `key` - A byte slice that holds the key.
    ///
    /// # Returns
    ///
    /// An `Option` which is `Some` if an entry with the given key exists and is not marked as deleted, and `None` otherwise.
    pub fn get(&self, key: &[u8]) -> Option<&Entry> {
        // Check the bloom filter
        if !self.bloom_filter.check(&key.to_vec()) {
            return None;
        }

        // Get the entry
        let entry = self.entries.get(key);
        match entry {
            Some(entry) => {
                if entry.deleted {
                    None
                } else {
                    Some(entry)
                }
            }
            None => None,
        }
    }

    /// Returns the total size of all entries in the `Memtable`.
    ///
    /// # Returns
    ///
    /// The total size of all entries in the `Memtable`.
    pub fn get_size(&self) -> usize {
        self.size
    }
}
