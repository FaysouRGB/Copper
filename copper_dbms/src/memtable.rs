use std::{collections::BTreeMap, fmt};

use crate::settings;

#[derive(Default)]
pub struct Memtable {
    pub records: BTreeMap<Vec<u8>, MemtableEntry>,
    pub size: usize,
    pub max_size: usize,
}

pub struct MemtableEntry {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub deleted: bool,
}

impl MemtableEntry {
    // Create a new memtable entry.
    pub fn new(key: &[u8], value: &[u8], deleted: bool) -> MemtableEntry {
        MemtableEntry { key: key.to_vec(), value: value.to_vec(), deleted }
    }

    // Get the size of the memtable entry.
    pub fn get_size(&self) -> usize {
        self.key.len() + self.value.len() + 1
    }
}

impl Memtable {
    // Create a new memtable.
    pub fn new() -> Memtable {
        Memtable { records: BTreeMap::new(), size: 0, max_size: settings::MEMTABLE_MAX_SIZE }
    }

    // Insert an entry in the memtable, if already existing, then it is updated.
    pub fn insert(&mut self, key: &[u8], value: &[u8], deleted: bool) {
        let entry = MemtableEntry::new(key, value, deleted);
        let entry_size = entry.get_size();
        let old_entry = self.records.insert(key.to_vec(), entry);

        match old_entry {
            Some(old_entry) => {
                if old_entry.get_size() > entry_size {
                    self.size -= old_entry.get_size() - entry_size;
                } else {
                    self.size += entry_size - old_entry.get_size();
                }
            }
            None => self.size += entry_size,
        }
    }

    // Get the value associated with the key, if it does not exists, return None.
    pub fn get(&self, key: &[u8]) -> Option<(bool, Vec<u8>)> {
        let entry = self.records.get(key);
        match entry {
            Some(entry) => {
                if entry.deleted {
                    Some((true, Vec::new()))
                } else {
                    Some((false, entry.value.to_vec()))
                }
            }
            None => None,
        }
    }
}

impl fmt::Display for Memtable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "Memtable - {}B / {}B ({}%) {{", self.size, self.max_size, self.size / self.max_size * 100)?;
        for (i, entry) in self.records.iter().enumerate() {
            let key_string: String = entry.1.key.iter().map(|x| char::from(*x)).collect();
            let value_string: String = entry.1.value.iter().map(|x| char::from(*x)).collect();
            writeln!(f, "\t{}: {:?} -> {:?} ({})", i, key_string, value_string, if !entry.1.deleted { "Alive" } else { "Dead" })?;
        }

        writeln!(f, "}}")?;
        Ok(())
    }
}
