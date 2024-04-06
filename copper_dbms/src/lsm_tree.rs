use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

use bloomfilter::Bloom;

use crate::{entry::Entry, memtable::Memtable};

mod old {
    use crate::memtable::Memtable;
    use crate::{settings, wal};
    use std::collections::BTreeMap;
    use std::fs::{self, File};
    use std::io::BufRead;
    use std::io::{self, BufReader, BufWriter, Write};
    use std::path::Path;

    pub struct LSMTree {
        pub name: String,
        pub path: String,
        pub memtable: Memtable,
        pub levels: Vec<Level>,
    }

    #[derive(Default)]
    pub struct Level {
        pub sstables: Vec<SSTable>,
    }

    #[derive(Default)]
    pub struct SSTable {
        pub data: BTreeMap<Vec<u8>, SSTableEntry>,
    }

    #[derive(Clone)]
    pub struct SSTableEntry {
        pub key: Vec<u8>,
        pub value: Vec<u8>,
        pub deleted: bool,
    }

    impl LSMTree {
        pub fn new(directory: &Path, name: &str) -> Result<LSMTree, io::Error> {
            // Check if the database directory exists.
            if !directory.exists() {
                return Err(io::Error::new(io::ErrorKind::NotFound, "Database directory does not exists."));
            }

            // Create LSM-Tree directory and WAL.
            let mut path = directory.join(name);
            let lsm_path = path.clone().to_str().unwrap().to_string();
            fs::create_dir_all(&path)?;
            path.push("wal.dat");
            File::create(path)?;

            // Create a new LSM-Tree.
            Ok(LSMTree { name: name.to_string(), memtable: Memtable::new(), levels: Vec::new(), path: lsm_path })
        }

        pub fn load(directory: &Path) -> Result<LSMTree, io::Error> {
            if !directory.exists() {
                return Err(io::Error::new(io::ErrorKind::NotFound, "LSM-Tree directory does not exists."));
            }

            // Create the WAL if does not exists.
            let mut path = directory.to_path_buf();
            path.push("wal.dat");
            if !path.exists() || path.is_dir() {
                File::create(&path)?;
            }

            // Create the LSM-Tree.
            let mut lsm_tree = LSMTree { name: directory.file_name().unwrap().to_str().unwrap().to_string(), memtable: wal::get_memtable_from_wal(path.as_path())?, levels: Vec::new(), path: directory.to_str().unwrap().to_string() };

            // Load the SSTables.
            let sstable_path = format!("{}/{}", lsm_tree.path, "sstables/");
            let sstable_path = Path::new(&sstable_path);
            if !sstable_path.exists() || sstable_path.is_file() {
                return Ok(lsm_tree);
            }

            let entries = sstable_path.read_dir()?.filter(|e| e.is_ok());
            for entry in entries {
                let entry = entry?;
                let level_path = entry.path();
                if level_path.is_dir() {
                    let level_path = level_path.file_name().unwrap().to_str().unwrap();
                    if level_path.starts_with("level") {
                        let level_index: usize = level_path.strip_prefix("level_").unwrap().parse().unwrap();
                        while lsm_tree.levels.len() <= level_index {
                            lsm_tree.levels.push(Level::default());
                        }

                        // get all files (sstables) in the level directory, and sort them by name.
                        let mut sst_entries = entry.path().read_dir()?.filter(|e| e.is_ok()).collect::<Vec<_>>();
                        sst_entries.sort_by(|a, b| a.as_ref().unwrap().path().file_name().cmp(&b.as_ref().unwrap().path().file_name()));
                        for file_entry in sst_entries {
                            let file_entry = file_entry.unwrap();
                            let file_path = file_entry.path();
                            if file_path.is_file() {
                                let file = File::open(file_path)?;
                                let reader = BufReader::new(file);

                                let mut sstable = SSTable::default();
                                let mut lines = reader.lines();

                                while let Some(Ok(key)) = lines.next() {
                                    if let Some(Ok(value)) = lines.next() {
                                        if let Some(Ok(deleted)) = lines.next() {
                                            let entry = SSTableEntry { key: key.into_bytes(), value: value.into_bytes(), deleted: deleted == "1" };
                                            sstable.data.insert(entry.key.clone(), entry);
                                        }
                                    }
                                }

                                lsm_tree.levels[level_index].sstables.push(sstable);
                            }
                        }
                    }
                }
            }

            // Return the LSM-Tree.
            Ok(lsm_tree)
        }

        pub fn save(&mut self) -> Result<(), io::Error> {
            let path = Path::new(&self.path);
            fs::create_dir_all(path)?;

            //let wal_path = format!("{}/{}", self.path, "wal.dat");
            // wal::save(&wal_path, &self.memtable)?;

            // Delete old save.
            let binding = format!("{}/{}", self.path, "sstables/");
            let sstable_path = Path::new(&binding);
            let _ = fs::remove_dir_all(sstable_path);

            // Write new save.
            fs::create_dir_all(sstable_path)?;
            for (i, level) in self.levels.iter().enumerate() {
                fs::create_dir_all(sstable_path.join(format!("level_{}", i)))?;
                for (j, sstable) in level.sstables.iter().enumerate() {
                    let filename = format!("sstable{}.dat", j);
                    let filepath = sstable_path.join(format!("level_{}/", i)).join(filename);
                    let file = File::create(filepath)?;
                    let mut writer = BufWriter::new(file);

                    for (key, entry) in &sstable.data {
                        writer.write_all(key)?;
                        writer.write_all(b"\n")?;
                        writer.write_all(&entry.value)?;
                        writer.write_all(b"\n")?;
                        writer.write_all(if entry.deleted { b"1" } else { b"0" })?;
                        writer.write_all(b"\n")?;
                    }
                }
            }

            Ok(())
        }

        pub fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<(), io::Error> {
            // Write to WAL.
            //let wal_path = format!("{}/{}", self.path, "wal.dat");
            //wal::write_to_wal(Path::new(&wal_path), &Entry { key: key.to_vec(), value: value.to_vec(), deleted: false })?;

            // Insert into the memtable.
            self.memtable.insert(key, value, false);

            // Flush the memtable if it is full.
            if self.memtable.size > self.memtable.max_size {
                self.flush_memtable()?;
            }

            Ok(())
        }

        pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, io::Error> {
            if let Some(entry) = self.memtable.get(key) {
                return if entry.0 { Ok(None) } else { Ok(Some(entry.1)) };
            }

            for level in self.levels.iter().rev() {
                for sstable in level.sstables.iter().rev() {
                    if let Some(entry) = sstable.data.get(key) {
                        return if entry.deleted { Ok(None) } else { Ok(Some(entry.value.to_vec())) };
                    }
                }
            }

            Ok(None)
        }

        pub fn delete(&mut self, key: &[u8]) -> Result<(), io::Error> {
            // Write to WAL.
            //let wal_path = format!("{}/{}", self.path, "wal.dat");
            //wal::write_to_wal(Path::new(&wal_path), &Entry { key: key.to_vec(), value: b"".to_vec(), deleted: true })?;

            // Insert into the memtable.
            self.memtable.insert(key, b"0", true);

            // Flush the memtable if it is full.
            if self.memtable.size > self.memtable.max_size {
                self.flush_memtable()?;
            }

            Ok(())
        }

        pub fn flush_memtable(&mut self) -> Result<(), io::Error> {
            let new_sstable = SSTable { data: self.memtable.records.iter().map(|(key, entry)| (key.clone(), SSTableEntry { key: entry.key.clone(), value: entry.value.clone(), deleted: entry.deleted })).collect() };

            if let Some(first_level) = self.levels.first_mut() {
                first_level.sstables.push(new_sstable);
            } else {
                let mut level = Level { sstables: Vec::new() };
                level.sstables.push(new_sstable);
                self.levels.push(level);
            }

            self.memtable = Memtable::new();
            let wal_path = format!("{}/{}", self.path, "wal.dat");
            wal::clear_wal(Path::new(&wal_path))?;
            // wal::load(Path::new(&wal_path))?;
            self.compact_tree();

            Ok(())
        }

        pub fn compact_tree(&mut self) {
            for i in 0..self.levels.len() {
                if self.levels[i].sstables.len() >= settings::MAX_SSTS_PER_LEVEL {
                    // Create a new SSTable
                    let mut new_sstable = SSTable::default();
                    // For each sstable in the level.
                    for sstable in self.levels[i].sstables.iter() {
                        // Add all key-value pairs.
                        for (key, entry) in &sstable.data {
                            new_sstable.data.insert(key.clone(), entry.clone());
                        }
                    }

                    // Add the new SSTable to the next level.
                    if i + 1 < self.levels.len() {
                        self.levels[i + 1].sstables.push(new_sstable);
                    } else {
                        let mut level = Level::default();
                        level.sstables.push(new_sstable);
                        self.levels.push(level);
                    }

                    // Remove the SSTables from the current level.
                    self.levels[i].sstables.clear();
                }
            }
        }

        pub fn drop(&self) -> Result<(), io::Error> {
            let path = Path::new(&self.path);
            fs::remove_dir_all(path)?;
            Ok(())
        }
    }
}

pub struct Database {
    pub path: PathBuf,
    pub tables: Vec<Table>,
}

pub struct Table {
    pub name: String,
    pub memtable: Memtable,
    pub columns: Vec<Column>,
    pub levels: Vec<Vec<SSTable>>,
}

pub struct Column {
    pub name: String,
    pub data_type: DataType,
}

pub enum DataType {
    Int,
    String,
    Bool,
}

impl Table {
    pub fn new(database_path: &Path, name: String, columns: Vec<Column>) -> Result<Self, std::io::Error> {
        // Create the table
        let mut table = Table { name, memtable: Memtable::new(), columns, levels: vec![] };

        Ok(table)
    }
}

pub struct SSTable {
    entries: BTreeMap<Vec<u8>, Entry>,
    bloom_filter: Bloom<Vec<u8>>,
}
