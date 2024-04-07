use bloomfilter::Bloom;
use std::{
    collections::BTreeMap,
    fmt::Debug,
    io::{Read, Write},
};

use super::{entry::Entry, memtable::Memtable};

pub struct SSTable {
    data: BTreeMap<Vec<u8>, Entry>,
    bloom_filter: Bloom<Vec<u8>>,
}

impl SSTable {
    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        // Check the Bloom filter first
        if !self.bloom_filter.check(&key.to_vec()) {
            return None;
        }

        // Look up the key in the data map
        self.data.get(key).map(|entry| entry.get_value().to_vec())
    }

    pub fn from_memtable(memtable: &Memtable) -> Self {
        let mut bloom_filter = Bloom::new_for_fp_rate(1000, 0.01);
        let mut data = BTreeMap::new();
        for key in memtable.get_entries().keys() {
            let entry = memtable.get(key).unwrap();
            data.insert(key.to_vec(), entry);
            bloom_filter.set(key);
        }

        Self { data, bloom_filter }
    }

    pub fn save_to_disk(&self, path: &str) -> Result<(), std::io::Error> {
        let file = std::fs::File::create(path)?;
        let mut writer = std::io::BufWriter::new(file);

        // Write the number of entries in the SSTable
        writer.write_all(&(self.data.len() as u64).to_be_bytes())?;

        // Write each key-value pair to the file
        for (key, entry) in &self.data {
            writer.write_all(&(key.len() as u64).to_be_bytes())?;
            writer.write_all(key)?;
            writer.write_all(&(entry.get_value().len() as u64).to_be_bytes())?;
            writer.write_all(entry.get_value())?;
            // Write the tombstone
            writer.write_all(&[if entry.is_deleted() { 1 } else { 0 }])?;
        }

        Ok(())
    }

    pub fn get_all_entries(&self) -> Vec<Entry> {
        self.data.iter().map(|(key, entry)| Entry::new(key, entry.get_value(), entry.is_deleted())).collect()
    }

    pub fn load_from_disk(path: &str) -> Result<Self, std::io::Error> {
        let file = std::fs::File::open(path)?;
        let mut reader = std::io::BufReader::new(file);

        // Read the number of entries in the SSTable
        let mut num_entries = [0; 8];
        reader.read_exact(&mut num_entries)?;
        let num_entries = u64::from_be_bytes(num_entries) as usize;

        // Read each key-value pair from the file
        let mut data = BTreeMap::new();
        for _ in 0..num_entries {
            let mut key_len = [0; 8];
            reader.read_exact(&mut key_len)?;
            let key_len = u64::from_be_bytes(key_len) as usize;

            let mut key = vec![0; key_len];
            reader.read_exact(&mut key)?;

            let mut value_len = [0; 8];
            reader.read_exact(&mut value_len)?;
            let value_len = u64::from_be_bytes(value_len) as usize;

            let mut value = vec![0; value_len];
            reader.read_exact(&mut value)?;

            // Read the tombstone
            let mut tombstone = [0];
            reader.read_exact(&mut tombstone)?;

            let entry = Entry::new(&key, &value, tombstone[0] == 1);
            data.insert(key, entry);
        }

        let mut bloom_filter = Bloom::new_for_fp_rate(1000, 0.01);
        for key in data.keys() {
            bloom_filter.set(key);
        }

        Ok(Self { data, bloom_filter })
    }

    pub fn get_size(&self) -> usize {
        let mut sum = 0;
        for (key, entry) in &self.data {
            sum += key.len() + entry.get_value().len() + 1;
        }
        sum
    }

    pub fn compact(level: &Vec<Self>) -> Self {
        let mut data = BTreeMap::new();
        let mut bloom_filter = Bloom::new_for_fp_rate(1000, 0.01);

        for sstable in level {
            for (key, entry) in &sstable.data {
                data.insert(key.to_vec(), Entry::new(key, entry.get_value(), entry.is_deleted()));
                bloom_filter.set(key);
            }
        }

        Self { data, bloom_filter }
    }

    pub fn get_range(&self, start: &[u8], end: &[u8]) -> Vec<Entry> {
        let start_key = start.to_vec();
        let end_key = end.to_vec();

        self.data.range(start_key..=end_key).filter_map(|(key, entry)| if entry.is_deleted() { None } else { Some(Entry::new(key, entry.get_value(), entry.is_deleted())) }).collect()
    }

    pub fn clear(&mut self) {
        self.data.clear();
        self.bloom_filter.clear();
    }

    pub fn merge(sstables: &[SSTable]) -> SSTable {
        let mut data = BTreeMap::new();
        let mut bloom_filter = Bloom::new_for_fp_rate(1000, 0.01);

        for sstable in sstables {
            for (key, entry) in &sstable.data {
                data.insert(key.to_vec(), Entry::new(key, entry.get_value(), entry.is_deleted()));
                bloom_filter.set(key);
            }
        }

        Self { data, bloom_filter }
    }
}

impl Debug for SSTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SSTable(data: {:?})", self.data)
    }
}
