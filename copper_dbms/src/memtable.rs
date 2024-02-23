use std::{borrow::Borrow, f32::consts::E, fmt};

pub struct Memtable {
    pub records: Vec<MemtableEntry>,
    pub size: usize,
    pub max_size: usize,
}

pub struct MemtableEntry {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub deleted: bool,
}

impl MemtableEntry {
    pub fn get_size(&self) -> usize {
        self.key.len() + self.value.len() + 1
    }
}

impl Memtable {
    pub fn new() -> Memtable {
        Memtable { records: Vec::new(), size: 0, max_size: 1024 * 1024 * 1024 }
    }

    pub fn set(&mut self, key: &[u8], value: &[u8]) {
        // First let's convert the key slice to a vector
        let key = key.to_vec();

        // Then let's use binary search to see if it is in the records
        // If it is, we update the value
        // If not, we insert it to the good place
        match self.records.binary_search_by(|entry| entry.key.cmp(&key)) {
            Ok(index) => {
                let entry = MemtableEntry { key, value: value.to_vec(), deleted: false };

                // Update the size of the memtable
                if entry.get_size() > self.records[index].get_size() {
                    self.size += entry.get_size() - self.records[index].get_size();
                } else {
                    self.size -= self.records[index].get_size() - entry.get_size();
                }

                self.records[index] = entry;
            }
            Err(index) => {
                let entry = MemtableEntry { key, value: value.to_vec(), deleted: false };
                self.size += entry.get_size();
                self.records.insert(index, entry);
            }
        }
    }

    pub fn get(&self, key: &[u8]) -> Option<&MemtableEntry> {
        let key = key.to_vec();
        match self.records.binary_search_by(|entry| entry.key.cmp(&key)) {
            Ok(index) => Some(&self.records[index]),
            Err(_) => None,
        }
    }

    pub fn delete(&mut self, key: &[u8]) {
        let key = key.to_vec();
        if let Ok(index) = self.records.binary_search_by(|entry| entry.key.cmp(&key)) {
            self.records[index].deleted = true;
        }
    }
}

impl fmt::Display for Memtable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "Memtable - {}B / {}B ({}%) {{", self.size, self.max_size, self.size / self.max_size * 100)?;
        for (i, entry) in self.records.iter().enumerate() {
            let key_string: String = entry.key.iter().map(|x| char::from(*x)).collect();
            let value_string: String = entry.value.iter().map(|x| char::from(*x)).collect();
            writeln!(f, "\t{}: {:?} -> {:?} ({})", i, key_string, value_string, if !entry.deleted { "Alive" } else { "Dead" })?;
        }

        writeln!(f, "}}")?;
        Ok(())
    }
}
