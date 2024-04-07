use chrono::prelude::*;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;

/// `Entry` struct represents an entry in a database.
/// It has a `key`, a `value`, and a `deleted` flag.
pub enum Operation {
    Deletion(Vec<u8>),
    Insertion(Vec<u8>, Vec<u8>),
    Flush,
    Creation,
    Load,
    Get(Vec<u8>),
    GetRange,
    Compact,
    Clear,
    Decode(Vec<u8>),
}

/// Writes a log of an operation to a file.
///
/// # Arguments
///
/// * `path` - A reference to a `Path` that specifies the directory where the log file is located.
/// * `operation` - An `Operation` that specifies the operation to be logged.
///
/// This function creates a file named "log.txt" in the specified directory if it does not exist,
/// and appends a log of the operation to the file.
/// The log includes the date and time of the operation and the details of the operation.
pub fn write_log(path: &Path, operation: Operation) {
    // Create the path
    let path = path.join("log.txt");

    // Open a file, if it already exists, append
    let mut file = OpenOptions::new().create(true).append(true).open(path).unwrap();

    // Write the date and time of the operation
    let now = Local::now();
    file.write_all(b"[").unwrap();
    let date_time = now.format("%Y-%m-%d %H:%M:%S").to_string();
    file.write_all(date_time.as_bytes()).unwrap();

    // Write the operation
    match operation {
        Operation::Deletion(key) => {
            file.write_all(b"] Deletion : ").unwrap();
            file.write_all(&key).unwrap();
        }
        Operation::Insertion(key, value) => {
            file.write_all(b"] Insertion : ").unwrap();
            file.write_all(&key).unwrap();
            file.write_all(b" : ").unwrap();
            file.write_all(&value).unwrap();
        }
        Operation::Flush => {
            file.write_all(b"] Memtable flushed").unwrap();
        }
        Operation::Creation => {
            file.write_all(b"] Table created").unwrap();
        }
        Operation::Load => {
            file.write_all(b"] Table loaded").unwrap();
        }
        Operation::Get(key) => {
            file.write_all(b"] Get : ").unwrap();
            file.write_all(&key).unwrap();
        }
        Operation::Compact => {
            file.write_all(b"] Levels compacted").unwrap();
        }
        Operation::GetRange => {
            file.write_all(b"] Get range with predicate").unwrap();
        }
        Operation::Clear => {
            file.write_all(b"] Table cleared").unwrap();
        }
        Operation::Decode(key) => {
            file.write_all(b"] Decoding : ").unwrap();
            file.write_all(&key).unwrap();
        }
    }

    file.write_all(b"\n").unwrap();
}
