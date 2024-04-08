use super::{
    column::{Column, DataType},
    entry::Entry,
    memtable::Memtable,
    sstable::SSTable,
    wal,
};
use std::{
    collections::HashMap,
    fmt::{Debug, Formatter},
    fs,
    io::Error,
};

use crate::lsm_tree::log::write_log;
use crate::lsm_tree::log::Operation;

/// `LsmTree` struct represents a Log-Structured Merge Tree in a database.
/// It has a path, a `Memtable`, a vector of `Column`s, and a vector of vectors of `SSTable`s.
pub struct LsmTree {
    path: String,
    memtable: Memtable,
    columns: Vec<Column>,
    levels: Vec<Vec<SSTable>>,
}

/// `Value` enum represents a value in a database entry.
/// It can be an integer, a boolean, or a string.
pub enum Value {
    Int(i32),
    Bool(bool),
    Text(String),
    // Add more types as needed
}

impl LsmTree {
    /// Creates a new `LsmTree` with the given path and columns.
    ///
    /// # Arguments
    ///
    /// * `path` - A string that specifies the path of the database.
    /// * `columns` - A vector of `Column`s that specifies the columns of the database.
    ///
    /// The function creates the directories for the database and the SSTables,
    /// clears the Write-Ahead Log (WAL) file and the config file,
    /// and creates a new config file with the column names and data types.
    ///
    /// The function panics if no columns are provided.
    pub fn new(path: String, columns: Vec<Column>) -> Self {
        // Create the directory for the database
        let _ = std::fs::create_dir_all(&path);

        // Create the directory for the SSTables
        let _ = std::fs::create_dir_all(format!("{}/ssts", path));
        // Clear the WAL file
        let _ = wal::clear_wal(path.as_ref());
        // Clear the config file
        let _ = std::fs::remove_file(format!("{}/config.txt", path));

        // Create a config file with the column names and data types
        let config_path = format!("{}/config.txt", path);
        let mut config = String::new();
        if columns.is_empty() {
            panic!("No columns provided");
        }
        for column in &columns {
            config.push_str(&format!("{}|{}\n", column.get_name(), column.get_data_type().get_char()));
        }
        let _ = std::fs::write(config_path, config);

        // Create the wal
        let _ = fs::File::create(format!("{}/wal.txt", path));
        write_log(path.as_ref(), Operation::Creation);
        Self { path, memtable: Memtable::new(), columns, levels: Vec::new() }
    }

    /// Load an LSM Tree from a given path.
    /// Reads the configuration file to get column names and data types,
    /// loads the memtable from the Write-Ahead Log (WAL) if it exists,
    /// and loads the SSTables from disk.
    pub fn load(path: String) -> Result<Self, Error> {
        // Read the config file to get the column names and data types
        let config_path = format!("{}/config.txt", path);
        let config = std::fs::read_to_string(config_path)?;
        let columns: Vec<Column> = config
            .lines()
            .map(|line| {
                let parts: Vec<&str> = line.split('|').collect();
                Column::new(parts[0], DataType::from_char(parts[1].chars().next().unwrap()))
            })
            .collect();

        // If there is already a WAL file, read the memtable from it
        let memtable = match wal::get_memtable_from_wal(path.as_ref(), &columns) {
            Ok(memtable) => memtable,
            Err(wal_error) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    match wal_error {
                        wal::WalError::Io(io_error) => io_error.to_string(),
                        wal::WalError::MalformedEntry => "Malformed WAL entry".to_string(),
                    },
                ))
            }
        };

        // Load the SSTables from disk, each sstable file is labeled sst_0_0, sst_0_1, sst_1_0, etc.
        let mut levels = Vec::new();
        // Load all levels
        let mut i = 0;
        loop {
            let mut level = Vec::new();
            let mut j = 0;
            loop {
                let sst_path = format!("{}/ssts/sst_{}_{}.txt", path, i, j);
                if !std::path::Path::new(&sst_path).exists() {
                    break;
                }

                let sstable = SSTable::load_from_disk(&sst_path)?;
                level.push(sstable);
                j += 1;
            }

            // If no SSTables were loaded for this level, we're done
            if level.is_empty() {
                break;
            }

            levels.push(level);
            i += 1;
        }
        write_log(path.as_ref(), Operation::Load);
        Ok(Self { path, memtable, columns, levels })
    }

    /// Insert a key-value pair into the LSM Tree.
    /// Checks if the values respect the columns, then inserts the key-value pair into the memtable.
    /// If the memtable is full, it flushes it to an SSTable.
    pub fn insert(&mut self, key: &[u8], values: &[Vec<u8>]) -> Result<(), Error> {
        // Check that the values respects the columns
        if values.len() != self.columns.len() {
            return Err(Error::new(std::io::ErrorKind::InvalidInput, "Invalid number of values"));
        }

        // Flatten the values into a single byte vector with separators
        let mut value = Vec::new();
        for i in 0..values.len() {
            value.extend_from_slice(&values[i]);
            if i < values.len() - 1 {
                value.push(b'|');
            }
        }

        // Insert the key-value pair into the memtable
        self.memtable.insert(key, &value, false);
        let _ = wal::write_to_wal(self.path.as_ref(), &Entry::new(key, &value, false));

        // If the memtable is full, flush it to an SSTable
        if self.memtable.is_full() {
            self.flush()?;
        }

        write_log(self.path.as_ref(), Operation::Insertion(key.to_vec(), value));

        Ok(())
    }

    /// Retrieve a value associated with a given key from the LSM Tree.
    /// First checks the memtable, then each level of SSTables.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        write_log(self.path.as_ref(), Operation::Get(key.to_vec()));
        // Check the memtable first
        if let Some(entry) = self.memtable.get(key) {
            if entry.is_deleted() {
                return Ok(None);
            }

            return Ok(Some(entry.get_value().to_vec()));
        }

        // If the key is not in the memtable, check each level of SSTables
        for level in &self.levels {
            for sstable in level {
                if let Some(value) = sstable.get(key) {
                    return Ok(Some(value));
                }
            }
        }

        // If the key is not found, return None
        Ok(None)
    }

    /// Delete a key-value pair from the LSM Tree.
    /// Inserts a tombstone value into the memtable.
    /// If the memtable is full, it flushes it to an SSTable.
    pub fn delete(&mut self, key: &[u8]) -> Result<bool, Error> {
        // Insert a tombstone value into the memtable
        let result = self.memtable.insert(key, &[], true);
        let mut value = Vec::new();
        value.resize(self.columns.len() - 1, b'|');
        let _ = wal::write_to_wal(self.path.as_ref(), &Entry::new(key, &value, true));

        // If the memtable is full, flush it to an SSTable
        if self.memtable.is_full() {
            self.flush()?;
        }

        write_log(self.path.as_ref(), Operation::Deletion(key.to_vec()));

        Ok(result)
    }

    /// Flush the memtable to an SSTable and clear the memtable.
    /// Creates a new SSTable and writes the contents of the memtable to it.
    /// Adds the new SSTable to the first level.
    /// Clears the memtable and the sst folder.
    /// Writes all levels to disk.
    /// Compacts the levels.
    pub fn flush(&mut self) -> Result<(), Error> {
        // Create a new SSTable and write the contents of the memtable to it
        let sstable = SSTable::from_memtable(&self.memtable);
        wal::clear_wal(self.path.as_ref()).expect("Cannot clear wal");

        // Add the new SSTable to the first level
        if self.levels.is_empty() {
            self.levels.push(Vec::new());
        }
        self.levels[0].insert(0, sstable);

        // Clear the memtable
        self.memtable.clear();

        // Clear the sst folder
        let _ = std::fs::remove_dir_all(format!("{}/ssts", self.path));

        // Create the sst table folder
        let _ = std::fs::create_dir_all(format!("{}/ssts", self.path));

        // Write all levels
        for (i, level) in self.levels.iter().enumerate() {
            for (j, sstable) in level.iter().enumerate() {
                let sst_path = format!("{}/ssts/sst_{}_{}.txt", self.path, i, j);
                sstable.save_to_disk(&sst_path)?;
            }
        }

        write_log(self.path.as_ref(), Operation::Flush);

        // compact the levels
        self.compact()?;

        Ok(())
    }

    /// Compact the levels of the LSM Tree.
    /// For each level, if there are more than 2 SSTables, merges them,
    /// pushes them to the next level and removes them from the current level.
    pub fn compact(&mut self) -> Result<(), Error> {
        // For each level, check if there is more than 2 ss tables
        // If so, merge them, push them to the next level and remove them from the current level
        // Do this for each level

        for i in 0..self.levels.len() {
            // If there are more than 2 SSTables in this level
            if self.levels[i].len() > 2 {
                // Merge the SSTables
                let merged = SSTable::merge(&self.levels[i]);

                // If there's a next level, push the merged SSTable to it
                if i + 1 < self.levels.len() {
                    self.levels[i + 1].push(merged);
                } else {
                    // Otherwise, create a new level and push the merged SSTable to it
                    self.levels.push(vec![merged]);
                }

                // Remove the SSTables from the current level
                self.levels[i].clear();
            }
        }

        write_log(self.path.as_ref(), Operation::Compact);

        Ok(())
    }

    /// Get a range of entries from the LSM Tree.
    /// Gets all entries from the memtable and the SSTables that satisfy a given predicate.
    /// Only keeps non-deleted entries.
    pub fn get_range<F>(&self, predicate: F) -> Result<Vec<Vec<u8>>, Error>
    where
        F: Fn(&Entry) -> bool,
    {
        let mut entries = Vec::new();

        // Get all entries from the memtable
        for entry in self.memtable.get_all_entries() {
            if predicate(&entry) {
                entries.push(entry);
            }
        }

        // Get all entries from the SSTables
        for level in self.levels.iter().rev() {
            for sstable in level.iter() {
                for entry in sstable.get_all_entries() {
                    if predicate(&entry) {
                        entries.push(entry);
                    }
                }
            }
        }

        // Now only keep non deleted entries
        // To do this, we  go through the entries and check if the last byte is 0 or 1, if it is 1, we remove it from the entries and will not add the other entries with the same key
        let mut result = Vec::new();
        let mut not_to_add = Vec::new();
        for entry in entries {
            if entry.is_deleted() {
                not_to_add.push(entry.get_key().to_vec());
            } else if !not_to_add.contains(&entry.get_key().to_vec()) {
                result.push(entry.get_value().to_vec());
            }
        }

        write_log(self.path.as_ref(), Operation::GetRange);

        Ok(result)
    }

    /// Get the total size of the LSM Tree.
    /// Adds the size of the memtable and the size of each SSTable in each level.
    pub fn size(&self) -> usize {
        let mut total_size = 0;

        // Add the size of the memtable
        total_size += self.memtable.get_size();

        // Add the size of each SSTable in each level
        for level in &self.levels {
            for sstable in level {
                total_size += sstable.get_size();
            }
        }

        total_size
    }

    /// Clear the LSM Tree.
    /// Clears the memtable and each SSTable in each level.
    pub fn clear(&mut self) -> Result<(), Error> {
        // Clear the memtable
        self.memtable.clear();

        // Clear each SSTable in each level
        for level in &mut self.levels {
            for sstable in level {
                sstable.clear();
            }
        }

        write_log(self.path.as_ref(), Operation::Clear);

        Ok(())
    }

    /// Decode a byte slice into a HashMap.
    /// Splits the byte slice into values and decodes each value according to its data type.
    pub fn decode(&self, data: &[u8]) -> HashMap<String, Value> {
        let mut map = HashMap::new();
        let values: Vec<&[u8]> = data.split(|b| *b == b'|').collect();
        for (i, column) in self.columns.iter().enumerate() {
            let value = match column.get_data_type() {
                DataType::Int => Value::Int(i32::from_ne_bytes(values[i].try_into().unwrap())),
                DataType::Bool => Value::Bool(values[i][0] == b'\x01'),
                DataType::Text => Value::Text(String::from_utf8(values[i].to_vec()).unwrap()),
            };

            map.insert(column.get_name().to_string(), value);
        }

        write_log(self.path.as_ref(), Operation::Decode(data.to_vec()));

        map
    }
}

impl Debug for LsmTree {
    /// Formats the `LsmTree` for printing.
    /// Includes the path, memtable, columns, and levels in the output.
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LsmTree").field("path", &self.path).field("memtable", &self.memtable).field("columns", &self.columns).field("levels", &self.levels).finish()
    }
}

impl Debug for Value {
    /// Formats the `Value` for printing.
    /// Matches on the `Value` variant and writes the corresponding value to the formatter.
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Int(value) => write!(f, "{}", value),
            Value::Bool(value) => write!(f, "{}", value),
            Value::Text(value) => write!(f, "{}", value),
        }
    }
}
