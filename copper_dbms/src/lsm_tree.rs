const MEMTABLE_FLUSH_THRESHOLD: usize = 1024 * 1024 * 100; // Example threshold for flushing memtable (100 MB)
const MEMTABLE_LOAD_THRESHOLD: usize = 1024 * 1024 * 10; // Example threshold for loading memtable (10 MB)

use crate::memtable::Memtable;
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::BufRead;
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::Path;

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
    pub fn new() -> LSMTree {
        LSMTree { memtable: Memtable::new(), levels: Vec::new() }
    }

    pub fn insert(&mut self, key: Vec<u8>, value: Vec<u8>) {
        self.memtable.set(&key, &value);
        // Check if memtable size exceeds a threshold, then flush to disk
        if self.memtable.size > MEMTABLE_FLUSH_THRESHOLD {
            self.flush_memtable();
        }
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        // Search memtable first
        if let Some(entry) = self.memtable.get(&key) {
            return Some(entry.value.clone());
        }
        // Search SSTables in levels
        for level in self.levels.iter().rev() {
            for sstable in level.sstables.iter().rev() {
                if let Some(value) = sstable.data.get(key) {
                    return Some(value.clone());
                }
            }
        }
        None
    }

    pub fn delete(&mut self, key: &[u8]) {
        self.memtable.delete(key);
        if self.memtable.size > MEMTABLE_FLUSH_THRESHOLD {
            self.flush_memtable();
        }
    }

    pub fn flush_memtable(&mut self) {
        // Create a new SSTable from memtable data
        let new_sstable = SSTable { data: self.memtable.records.iter().map(|entry| (entry.key.clone(), entry.value.clone())).collect() };
        // Add the new SSTable to the first level
        if let Some(first_level) = self.levels.first_mut() {
            first_level.sstables.push(new_sstable);
        } else {
            let mut level = Level { sstables: Vec::new() };
            level.sstables.push(new_sstable);
            self.levels.push(level);
        }
        // Clear the memtable
        self.memtable = Memtable::new();

        // clear the wal
    }

    pub fn load_on_disk(&mut self) -> io::Result<()> {
        // Directory containing SSTable files
        let path = Path::new("./sstables");

        // Iterate over SSTable files
        for entry in fs::read_dir(path)? {
            let entry = entry?;
            let file_path = entry.path();

            // Open file for reading
            let file = File::open(&file_path)?;
            let mut reader = BufReader::new(file);

            // Read key-value pairs from file
            let mut data = BTreeMap::new();
            loop {
                let mut key = Vec::new();
                let mut value = Vec::new();

                // Attempt to read key
                match reader.read_until(b'\n', &mut key) {
                    Ok(0) => break, // Reached EOF
                    Ok(_) => {}     // Continue reading
                    Err(_) => return Err(io::Error::new(io::ErrorKind::Other, "Failed to read key")),
                }

                // Attempt to read value
                match reader.read_until(b'\n', &mut value) {
                    Ok(0) => break, // Reached EOF
                    Ok(_) => {}     // Continue reading
                    Err(_) => return Err(io::Error::new(io::ErrorKind::Other, "Failed to read value")),
                }

                // Remove trailing newline characters
                key.pop();
                value.pop();

                // Insert key-value pair into data map
                data.insert(key, value);
            }

            // Determine level and create SSTable
            let file_name = entry.file_name().into_string().unwrap();
            let parts: Vec<&str> = file_name.split("_").collect();
            if parts.len() == 2 {
                if let (Some(level_index_str), Some(_), Some(sstable_index_str)) = (parts.get(0), parts.get(1)) {
                    if let (Ok(level_index), Ok(sstable_index)) = (level_index_str.parse::<usize>(), sstable_index_str.parse::<usize>()) {
                        // Add SSTable to the corresponding level
                        if level_index < self.levels.len() {
                            let sstable = SSTable { data };
                            self.levels[level_index].sstables.push(sstable);
                        }
                    }
                }
            }
        }

        Ok(())
    }

    pub fn compact_actual_level(&mut self) {
        // For simplicity, let's just merge all SSTables into a single one
        if let Some(actual_level) = self.levels.last_mut() {
            let mut merged_sstable = SSTable::new();
            for sstable in actual_level.sstables.iter_mut().rev() {
                merged_sstable.data.extend(sstable.data.drain());
            }

            actual_level.sstables.clear();
            actual_level.sstables.push(merged_sstable);
        }
    }

    pub fn save_on_disk(&mut self) -> io::Result<()> {
        // Directory to store SSTables
        let path = Path::new("./sstables");
        fs::create_dir_all(path)?;

        // Save each SSTable to disk
        for (level_index, level) in self.levels.iter_mut().enumerate() {
            for (sstable_index, sstable) in level.sstables.iter_mut().enumerate() {
                let filename = format!("level{}_sstable{}.dat", level_index, sstable_index);
                let filepath = path.join(filename);

                let file = File::create(filepath)?;
                let mut writer = BufWriter::new(file);

                // Write each key-value pair to the file
                for (key, value) in &sstable.data {
                    writer.write_all(key)?;
                    writer.write_all(b"\n")?; // Newline separator between key and value
                    writer.write_all(value)?;
                    writer.write_all(b"\n")?; // Newline separator between value and next key
                }
            }
        }

        Ok(())
    }
}

impl Level {
    pub fn new() -> Level {
        Level { sstables: Vec::new() }
    }
}

impl SSTable {
    pub fn new() -> SSTable {
        SSTable { data: BTreeMap::new() }
    }
}
