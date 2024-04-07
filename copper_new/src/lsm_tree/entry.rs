use std::fmt::Debug;

pub struct Entry {
    key: Vec<u8>,
    value: Vec<u8>,
    deleted: bool,
}

impl Entry {
    pub fn new(key: &[u8], value: &[u8], deleted: bool) -> Self {
        Self { key: key.to_vec(), value: value.to_vec(), deleted }
    }

    pub fn get_key(&self) -> &[u8] {
        &self.key
    }

    pub fn get_value(&self) -> &[u8] {
        &self.value
    }

    pub fn get_size(&self) -> usize {
        self.key.len() + self.value.len() + 1
    }

    pub fn is_deleted(&self) -> bool {
        self.deleted
    }
}

impl Debug for Entry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Entry(key: {:?}, value: {:?}, deleted: {:?})", String::from_utf8_lossy(&self.key), String::from_utf8_lossy(&self.value), self.deleted)
    }
}
