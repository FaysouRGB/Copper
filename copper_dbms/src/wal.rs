use crate::memtable::Memtable;
use std::fs::{File, OpenOptions};
use std::io::{self, BufRead, Write};
use std::path::Path;

pub fn load(wal_path: &Path) -> Result<Memtable, std::io::Error> {
    let mut memtable = Memtable::new();
    if !wal_path.exists() || !wal_path.is_file() {
        File::create(wal_path)?;
    }

    let file = File::open(wal_path)?;
    let reader = io::BufReader::new(file);
    let mut lines = reader.lines();
    while let Some(line) = lines.next() {
        let line = line?;
        let key = line.as_bytes();

        let line = lines.next().unwrap()?;
        let value = line.as_bytes();

        let line = lines.next().unwrap()?;
        let deleted = line;

        memtable.insert(key, value, deleted == "1");
    }

    Ok(memtable)
}

pub fn insert(wal_path: &Path, key: &[u8], value: &[u8], deleted: bool) -> Result<(), io::Error> {
    let mut file = OpenOptions::new().append(true).open(wal_path)?;

    file.write_all(key)?;
    file.write_all(b"\n")?;

    file.write_all(value)?;
    file.write_all(b"\n")?;

    let deleted = if deleted { b"1" } else { b"0" };
    file.write_all(&deleted[..])?;
    file.write_all(b"\n")?;

    Ok(())
}

pub fn save(wal_path: &str, memtable: &Memtable) -> Result<(), io::Error> {
    let mut file = File::create(wal_path)?;

    for (key, value) in memtable.records.iter() {
        file.write_all(key)?;
        file.write_all(b"\n")?;

        file.write_all(&value.value)?;
        file.write_all(b"\n")?;

        let deleted = if value.deleted { b"1" } else { b"0" };
        file.write_all(&deleted[..])?;
        file.write_all(b"\n")?;
    }

    Ok(())
}

pub fn delete(wal_path: &Path) -> Result<(), io::Error> {
    std::fs::remove_file(wal_path)
}
