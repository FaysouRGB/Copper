use std::{
    fs::{self, File},
    path::{Path, PathBuf},
};

use crate::{lsm_tree::SSTable, memtable::Memtable};

pub struct Table {
    pub path: PathBuf,
    pub memtable: Memtable,
    pub columns: Vec<Column>,
    pub levels: Vec<Vec<SSTable>>,
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

        File::create(config_path)?;

        Ok(table)
    }
}
