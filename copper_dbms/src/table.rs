use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::{self, BufRead, Write},
    path::{Path, PathBuf},
};

use bloomfilter::Bloom;

use crate::{entry::Entry, memtable::Memtable, wal};

pub struct Table {
    pub path: PathBuf,
    pub memtable: Memtable,
    pub columns: Vec<Column>,
    pub levels: Vec<Vec<SSTable>>,
}

pub struct SSTable {
    path: PathBuf,
    entries: BTreeMap<Vec<u8>, Entry>,
    bloom_filter: Bloom<Vec<u8>>,
}

pub struct Column {
    pub name: String,
    pub data_type: DataType,
}

pub enum DataType {
    Int = 0,
    String = 1,
    Bool = 2,
}

impl DataType {
    pub fn get_display(&self) -> char {
        match self {
            DataType::Int => 'i',
            DataType::String => 's',
            DataType::Bool => 'b',
        }
    }

    pub fn from_display(c: char) -> DataType {
        match c {
            'i' => DataType::Int,
            's' => DataType::String,
            'b' => DataType::Bool,
        }
    }
}

impl Table {
    pub fn new(database_path: &Path, name: String, columns: Vec<Column>) -> Result<Self, std::io::Error> {
        // Get the table path
        let table_path = database_path.join(name);

        // Create the table
        let mut table = Table { path: table_path, memtable: Memtable::new(), columns, levels: vec![] };

        // Create the table folder
        fs::create_dir(table_path)?;

        // Create the config file
        let config_path = table_path.join("config.txt");
        let mut file = OpenOptions::new().create(true).append(false).open(config_path)?;

        for column in &table.columns {
            writeln!(&mut file, "{}", String::from_utf8_lossy(column.name.as_bytes()))?;
            writeln!(&mut file, "{}", column.data_type.get_display())?;
        }

        Ok(table)
    }

    pub fn load_table(database_path: &Path, name: String) -> Result<Table, std::io::Error> {
        // Construct the table path
        let table_path = database_path.join(&name);

        // Load the memtable from the WAL
        let memtable = wal::get_memtable_from_wal(&table_path)?;

        // Load the columns from the config file
        let columns = Self::load_columns(&table_path)?;

        // Load SSTables from each level folder
        let levels = Self::load_sstables(&table_path)?;

        // Create the table
        let table = Table { path: table_path, memtable, columns, levels };

        Ok(table)
    }

    // Function to load columns from the config file
    fn load_columns(table_path: &Path) -> Result<Vec<Column>, std::io::Error> {
        let config_path = table_path.join("config.txt");
        let config_file = File::open(&config_path)?;
        let mut columns = Vec::new();

        let reader = io::BufReader::new(config_file);
        let mut lines = reader.lines();
        while let Some(name) = lines.next() {
            if let Some(data_type) = lines.next() {
                let data_type_char = data_type?.chars().next().unwrap_or('i');
                let data_type = DataType::from_display(data_type_char);
                columns.push(Column { name: name?, data_type });
            }
        }

        Ok(columns)
    }

    // Function to load SSTables from each level folder
    fn load_sstables(table_path: &Path) -> Result<Vec<Vec<SSTable>>, std::io::Error> {
        let mut levels = Vec::new();
        let level_folders = fs::read_dir(&table_path)?;

        // Sort level folders by their path
        let mut level_folders: Vec<_> = level_folders.filter_map(Result::ok).filter(|entry| entry.file_type().ok().map_or(false, |t| t.is_dir())).collect();
        level_folders.sort_by(|a, b| a.path().cmp(&b.path()));

        for level_folder in level_folders {
            let mut sstables = Vec::new();
            let sstable_files = fs::read_dir(level_folder.path())?;

            // Sort SSTable files by their path
            let mut sstable_files: Vec<_> = sstable_files.filter_map(Result::ok).filter(|entry| entry.file_type().ok().map_or(false, |t| t.is_file())).collect();
            sstable_files.sort_by(|a, b| a.path().cmp(&b.path()));

            for sstable_file in sstable_files {
                let sstable = SSTable::load_from_file(&sstable_file.path())?;
                sstables.push(sstable);
            }

            levels.push(sstables);
        }

        Ok(levels)
    }

    pub fn insert() {}
    pub fn get() {}
    pub fn flush_memtable() {}
    pub fn compact_tree() {}
}

impl SSTable {
    // Load SSTable from a file
    fn load_from_file(path: &Path) -> Result<Self, std::io::Error> {
        let mut entries = BTreeMap::new();

        let file = File::open(path)?;
        let reader = io::BufReader::new(file);
        let mut lines = reader.lines();
        while let Some(key) = lines.next() {
            if let Some(value) = lines.next() {
                if let Some(deleted_flag) = lines.next() {
                    let key = key?.trim().as_bytes().to_vec();
                    let value = value?.trim().as_bytes().to_vec();
                    let deleted = deleted_flag?.trim().parse::<u8>().unwrap() != 0;
                    entries.insert(key, Entry::new(&key, &value, deleted));
                } else {
                    return Err(io::Error::new(io::ErrorKind::InvalidData, "Missing deleted flag"));
                }
            } else {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "Missing value"));
            }
        }

        let mut bloom_filter = Bloom::new_for_fp_rate(1000, 0.01);
        for key in entries.keys() {
            bloom_filter.set(key);
        }

        Ok(SSTable { path: path.to_path_buf(), entries, bloom_filter })
    }
}
