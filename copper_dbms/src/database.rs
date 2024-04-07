use crate::errors;
use crate::settings;
use crate::table::Table;
use std::collections::HashMap;
use std::io;
use std::{fs, path::Path};

#[derive(Default)]
pub struct Database {
    pub name: String,
    pub tables: HashMap<String, Table>,
}

impl Database {
    // Create a new database.
    pub fn new(name: &str) -> Result<Database, io::Error> {
        // Create a Path object from the database name.
        let path = format!("{}/{}", settings::DATABASE_PATH, name);
        let path = Path::new(&path);

        // Check if the database already exists.
        if path.exists() && path.is_dir() {
            return Err(errors::invalid_input("A database with the same name already exists."));
        }

        // Create the database directory.
        fs::create_dir_all(path)?;

        // Return the new database.
        Ok(Database { name: name.to_string(), tables: HashMap::new() })
    }

    // Load database from disk.
    pub fn load(name: &str) -> Result<Database, io::Error> {
        let path = format!("{}/{}", settings::DATABASE_PATH, name);
        let path = Path::new(&path);
        if !path.exists() || !path.is_dir() {
            return Err(errors::invalid_input("Path to database does not exist or is not a directory."));
        }

        // Get the database name.
        let name = path.file_name().and_then(|n| n.to_str()).map(|n| n.to_string()).ok_or_else(|| errors::invalid_data("Failed to convert database name to string."))?;

        // Create the database object.
        let mut database = Database { name: name.to_string(), ..Default::default() };

        // Load the tables from disk.
        let entries = path.read_dir()?.filter(|e| e.is_ok());
        for entry in entries {
            let path = entry?.path();
            if path.is_file() {
                continue;
            }

            // let name = path.file_name().and_then(|n| n.to_str()).map(|n| n.to_string()).ok_or_else(|| errors::invalid_data("Failed to convert table name to string."))?;
            database.tables.insert(path.file_name().unwrap().to_str().unwrap().to_string(), LSMTree::load(&path)?);
        }

        // Return the database.
        Ok(database)
    }

    pub fn get_database_path(&self) -> String {
        format!("{}/{}", settings::DATABASE_PATH, self.name)
    }

    // Save database to disk.
    pub fn save(&mut self) -> Result<(), io::Error> {
        // Ensure that the database directory exists.
        let path = format!("{}/{}", settings::DATABASE_PATH, self.name);
        let path = Path::new(&path);
        fs::create_dir_all(path)?;

        // Save the tables to disk.
        for table in self.tables.values_mut() {
            table.save()?;
        }

        Ok(())
    }

    // Insert a key-value pair in a LSM-Tree.
    pub fn insert_into_table(&mut self, table_name: &str, key: &[u8], value: &[u8]) -> Result<(), io::Error> {
        self.tables.get_mut(table_name).ok_or_else(|| errors::not_found("The requested table was not found."))?.insert(key, value)
    }

    // Try to get the value associated to the key if it exists in a LSM-Tree.
    pub fn get_from_table(&self, table_name: &str, key: &[u8]) -> Result<Option<Vec<u8>>, io::Error> {
        self.tables.get(table_name).ok_or_else(|| errors::not_found("The requested table was not found."))?.get(key)
    }

    // Delete a key-value pair from a LSM-Tree.
    pub fn delete_from_table(&mut self, table_name: &str, key: &[u8]) -> Result<(), io::Error> {
        self.tables.get_mut(table_name).ok_or_else(|| errors::not_found("The requested table was not found."))?.delete(key)
    }

    pub fn create_table(&mut self, name: &str) -> Result<(), io::Error> {
        if self.tables.contains_key(name) {
            return Err(errors::invalid_input("A table with the same name already exists."));
        }

        let binding = self.get_database_path();
        let path = Path::new(&binding);
        let table = LSMTree::new(path, name)?;
        self.tables.insert(name.to_string(), table);

        Ok(())
    }

    pub fn drop_table(&mut self, table: &str) -> Result<(), io::Error> {
        if self.tables.contains_key(table) {
            let drop = self.tables.remove(table);
            drop.unwrap().drop()?;
            Ok(())
        } else {
            Err(errors::not_found("The requested table was not found."))
        }
    }

    // Delete the database from disk. Don't use it afterwards.
    pub fn drop(&mut self) -> Result<(), io::Error> {
        let path = self.get_database_path();
        let path = Path::new(&path);
        fs::remove_dir_all(path)?;

        Ok(())
    }

    pub fn flush_all(&mut self) -> Result<(), io::Error> {
        for table in self.tables.values_mut() {
            table.flush_memtable()?;
        }

        Ok(())
    }
}
