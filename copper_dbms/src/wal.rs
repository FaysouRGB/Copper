use crate::{entry::Entry, memtable::Memtable};
use std::{
    fs::OpenOptions,
    io::{self, BufRead, Write},
    path::Path,
};

/// Writes an entry to the Write-Ahead Log (WAL).
///
/// This function takes a reference to a `Path` for the table and a reference to an `Entry`.
/// It appends the entry to the WAL, which is located at "wal.txt" within the table directory.
/// Each entry is written as three lines in the WAL:
/// - The first line is the key.
/// - The second line is the value.
/// - The third line is a boolean indicating whether the entry has been deleted.
///
/// # Arguments
///
/// * `table_path` - A reference to a `Path` that points to the table directory.
/// * `entry` - A reference to the `Entry` to be written to the WAL.
///
/// # Errors
///
/// This function will return an `Err` if the WAL file cannot be opened or if there is an error
/// writing to the file.
///
/// # Examples
///
/// ```
/// use std::path::Path;
/// use copper_dbms::entry::Entry;
/// use copper_dbms::wal::write_to_wal;
///
/// let table_path = Path::new("/path/to/table");
/// let entry = Entry { key: vec![1, 2, 3], value: vec![4, 5, 6], deleted: false };
///
/// write_to_wal(&table_path, &entry);
/// ```
pub fn write_to_wal(table_path: &Path, entry: &Entry) -> Result<(), std::io::Error> {
    // Get the wal path
    let wal_path = table_path.join("wal.txt");

    // Ensure the wal file exists and open the file
    let mut file = OpenOptions::new().create(true).append(true).open(wal_path)?;

    // Write the entry to the file
    writeln!(&mut file, "{}", String::from_utf8_lossy(&entry.key))?;
    writeln!(&mut file, "{}", String::from_utf8_lossy(&entry.value))?;
    writeln!(&mut file, "{}", entry.deleted)?;

    Ok(())
}

/// Clears the Write-Ahead Log (WAL) associated with the specified table.
///
/// This function takes a reference to a `Path` for the table. It opens the WAL,
/// which is located at "wal.txt" within the table directory, and clears its contents.
///
/// # Arguments
///
/// * `table_path` - A reference to a `Path` that points to the table directory.
///
/// # Errors
///
/// This function will return an `Err` if the WAL file cannot be opened or if there is an error
/// setting the length of the file.
///
/// # Examples
///
/// ```
/// use std::path::Path;
/// use copper_dbms::wal::clear_wal;
///
/// let table_path = Path::new("/path/to/table");
///
/// clear_wal(&table_path);
/// ```
pub fn clear_wal(table_path: &Path) -> Result<(), std::io::Error> {
    // Get the wal path
    let wal_path = table_path.join("wal.txt");

    // Ensure the wal file exists and open the file
    let file = OpenOptions::new().create(true).append(false).open(wal_path)?;

    // Clear the wal
    file.set_len(0)?;

    Ok(())
}

/// Reads the Write-Ahead Log (WAL) and returns a `Memtable` representing its current state.
///
/// This function takes a reference to a `Path` for the table. It opens the WAL,
/// which is located at "wal.txt" within the table directory, and reads its contents.
/// Each entry in the WAL is represented as three lines:
/// - The first line is the key.
/// - The second line is the value.
/// - The third line is a boolean indicating whether the entry has been deleted.
/// These entries are parsed and inserted into a `Memtable`, which is then returned.
///
/// # Arguments
///
/// * `table_path` - A reference to a `Path` that points to the table directory.
///
/// # Errors
///
/// This function will return an `Err` if the WAL file cannot be opened or if there is an error
/// reading from the file.
///
/// # Examples
///
/// ```
/// use std::path::Path;
/// use copper_dbms::wal::get_memtable_from_wal;
///
/// let table_path = Path::new("/path/to/table");
///
/// let memtable = get_memtable_from_wal(&table_path);
/// ```
pub fn get_memtable_from_wal(table_path: &Path) -> Result<Memtable, std::io::Error> {
    // Get the wal path
    let wal_path = table_path.join("wal.txt");

    // Ensure the wal file exists and open the file
    let file = OpenOptions::new().create(true).open(wal_path)?;

    // Create the memtable
    let mut memtable = Memtable::new();

    // Buffer for entry lines
    let mut entry_lines = Vec::with_capacity(3);

    // Iterate over each lines
    for line in io::BufReader::new(file).lines() {
        // Get the line
        let line = line?;

        // Add the line to the buffer
        entry_lines.push(line);

        // If we have 3 lines, parse them into an entry
        if entry_lines.len() == 3 {
            // Get the fields
            let key = entry_lines[0].clone();
            let value = entry_lines[1].clone();
            let deleted = entry_lines[2].trim().parse::<u8>().unwrap() != 0;

            // Insert into the memtable
            memtable.insert(key.as_bytes(), value.as_bytes(), deleted);

            // Clear the buffer
            entry_lines.clear();
        }
    }

    Ok(memtable)
}
