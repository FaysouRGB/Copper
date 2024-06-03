use std::fmt::Debug;

/// `Entry` struct represents an entry in a database.
/// It has a `key`, a `value`, and a `deleted` flag.
pub struct Entry {
    key: Vec<u8>,
    value: Vec<u8>,
    deleted: bool,
}

impl Entry {
    /// Creates a new `Entry` with the given `key`, `value`, and `deleted` flag.
    ///
    /// # Arguments
    ///
    /// * `key` - A byte slice that holds the key of the entry.
    /// * `value` - A byte slice that holds the value of the entry.
    /// * `deleted` - A boolean indicating whether the entry is deleted.
    pub fn new(key: &[u8], value: &[u8], deleted: bool) -> Self {
        Self { key: key.to_vec(), value: value.to_vec(), deleted }
    }

    /// Returns the key of the `Entry`.
    pub fn get_key(&self) -> &[u8] {
        &self.key
    }

    /// Returns the value of the `Entry`.
    pub fn get_value(&self) -> &[u8] {
        &self.value
    }

    /// Returns the size of the `Entry`, which is the sum of the lengths of the key and value plus 1.
    pub fn get_size(&self) -> usize {
        self.key.len() + self.value.len() + 1
    }

    /// Returns whether the `Entry` is deleted.
    pub fn is_deleted(&self) -> bool {
        self.deleted
    }
}

impl Debug for Entry {
    /// Formats the `Entry` for printing.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Entry(key: {:?}, value: {:?}, deleted: {:?})", String::from_utf8_lossy(&self.key), String::from_utf8_lossy(&self.value), self.deleted)
    }
}
