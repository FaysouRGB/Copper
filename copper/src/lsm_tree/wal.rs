use std::{
    fs::OpenOptions,
    io::{BufRead, BufReader, Write},
    path::Path,
};

use super::{column::Column, entry::Entry, memtable::Memtable};

#[derive(Debug)]
pub enum WalError {
    Io(std::io::Error),
    MalformedEntry,
}

impl From<std::io::Error> for WalError {
    fn from(err: std::io::Error) -> WalError {
        WalError::Io(err)
    }
}

/// Writes an entry to the Write-Ahead Log (WAL).
///
/// # Arguments
///
/// * `table_path` - A reference to the path of the table.
/// * `entry` - A reference to the entry to be written.
///
/// The function opens the WAL file in append mode,
/// writes the key and value of the entry to the file,
/// writes a byte that indicates whether the entry is deleted,
/// and writes a newline character.
pub fn write_to_wal(table_path: &Path, entry: &Entry) -> Result<(), WalError> {
    let wal_path = table_path.join("wal.txt");
    let mut file = OpenOptions::new().create(true).append(true).open(wal_path)?;

    // Write the entry to the WAL file

    file.write_all(format!("{}|{}|", String::from_utf8_lossy(entry.get_key()), String::from_utf8_lossy(entry.get_value())).as_bytes())?;
    file.write_all(if entry.is_deleted() { &[b'\x01'] } else { &[b'\x00'] })?;
    file.write_all(b"\n")?;
    Ok(())
}

/// Clears the Write-Ahead Log (WAL).
///
/// # Arguments
///
/// * `table_path` - A reference to the path of the table.
///
/// The function opens the WAL file in write mode and truncates it.
pub fn clear_wal(table_path: &Path) -> Result<(), WalError> {
    let wal_path = table_path.join("wal.txt");
    // Clear the file
    OpenOptions::new().write(true).truncate(true).open(wal_path)?;

    Ok(())
}

/// Gets a memtable from the Write-Ahead Log (WAL).
///
/// # Arguments
///
/// * `table_path` - A reference to the path of the table.
/// * `columns` - A slice of columns.
///
/// The function opens the WAL file in read mode,
/// reads each line of the file,
/// splits each line into parts,
/// and adds each part to the memtable.
pub fn get_memtable_from_wal(table_path: &Path, columns: &[Column]) -> Result<Memtable, WalError> {
    let wal_path = table_path.join("wal.txt");
    // Open wal file
    let file = OpenOptions::new().read(true).open(wal_path)?;

    let mut memtable = Memtable::new();
    let reader = BufReader::new(file);

    for line in reader.lines() {
        let line = line?;
        let parts: Vec<&str> = line.split('|').collect();
        if parts.len() != columns.len() + 2 {
            return Err(WalError::MalformedEntry);
        }

        let key = parts[0].as_bytes().to_vec();
        // Parse the values using the columns
        let mut value = Vec::new();
        for i in 0..columns.len() {
            let value_part = parts[i + 1].as_bytes().to_vec();
            value.extend(value_part);
            if i < columns.len() - 1 {
                value.push(b'|');
            }
        }

        // Parse the last part as a boolean
        let deleted = parts[columns.len() + 1] == "\x01";

        memtable.insert(&key, &value, deleted);
    }

    Ok(memtable)
}
