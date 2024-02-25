use crate::database::Database;
use std::io;

pub fn parse(input: &str, database: &mut Database) -> Result<(), io::Error> {
    // Insert
    if input.starts_with("insert") {
        let arguments = input.split_whitespace().collect::<Vec<&str>>();
        if arguments.len() != 5 {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "Invalid number of arguments."));
        }

        if arguments[1] != "into" {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "Invalid command."));
        }

        let result = database.insert_into_table(arguments[2], arguments[3].as_bytes(), arguments[4].as_bytes());
        if result.is_err() {
            return Err(result.err().unwrap());
        }

        println!("Row '{}':'{}' has been inserted into table '{}'.", arguments[3], arguments[4], arguments[2]);
        return Ok(());
    }
    // Select
    if input.starts_with("select") {
        let arguments = input.split_whitespace().collect::<Vec<&str>>();
        if arguments.len() != 4 {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "Invalid number of arguments."));
        }

        let result = database.get_from_table(arguments[3], arguments[1].as_bytes());
        if result.is_err() {
            return Err(result.err().unwrap());
        }

        let result = result.unwrap();
        if let Some(result) = result {
            println!("Row '{}':'{}' has been selected from table '{}'.", arguments[1], String::from_utf8_lossy(&result), arguments[3]);
        } else {
            println!("Row '{}' does not exist in table '{}'.", arguments[1], arguments[3]);
        }

        return Ok(());
    }
    // Delete
    if input.starts_with("delete") {
        let arguments = input.split_whitespace().collect::<Vec<&str>>();
        if arguments.len() != 4 {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "Invalid number of arguments."));
        }

        if arguments[2] != "from" {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "Invalid command."));
        }

        let table_name = arguments[3];
        let key = arguments[1];

        let result = database.delete_from_table(table_name, key.as_bytes());
        if result.is_err() {
            return Err(result.err().unwrap());
        }

        println!("Key '{}' has been deleted from table '{}'.", arguments[1], table_name);
        return Ok(());
    }

    Err(io::Error::new(io::ErrorKind::InvalidInput, "Invalid command."))
}
