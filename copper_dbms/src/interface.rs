use crate::{database::Database, settings::DATABASE_PATH, sql_parser};
use std::{fs, io, path::PathBuf};

// Create a database based on a name and return it.
pub fn create_database(name: &str) -> Result<Database, io::Error> {
    Database::new(name)
}

// Load a database from disk based on a name and return it.
pub fn load_database(name: &str) -> Result<Database, io::Error> {
    Database::load(name)
}

// Drop a database from disk (delete it).
pub fn drop_database(database: &mut Database) -> Result<(), io::Error> {
    database.drop()
}

// Return a list of databases in the database directory.
pub fn get_databases() -> Result<Vec<PathBuf>, io::Error> {
    fs::create_dir_all(DATABASE_PATH)?;
    let mut entries = fs::read_dir(DATABASE_PATH)?.map(|res| res.map(|e| e.path())).collect::<Result<Vec<_>, io::Error>>()?;
    entries.sort();
    Ok(entries)
}

pub fn interface_loop() {
    // Get the database the user wants to use.
    println!("Welcome to COPPER DBMS.");
    println!("-------------------");

    let mut database: Database;

    loop {
        println!("Please choose a database or create a new one with: new <name>, delete with 'delete <index>', to quit type 'exit':");

        // Print available databases.
        let entries = get_databases();
        if entries.is_err() {
            println!("Failed to read database entries.");
            continue;
        }

        // Print entries.
        let entries = entries.unwrap();
        for (i, entry) in entries.iter().enumerate() {
            let entry = entry.file_name().and_then(|n| n.to_str()).unwrap();
            println!("[{}] {}", i, entry);
        }

        // Wait input.
        let mut input = String::new();
        io::stdin().read_line(&mut input).expect("Failed to read line.");

        // New
        if input.starts_with("new") {
            let arguments = input.split_whitespace().collect::<Vec<&str>>();
            if arguments.len() != 2 {
                println!("Invalid number of arguments.");
                continue;
            }

            let result = create_database(arguments[1]);
            if result.is_err() {
                println!("Failed to create database: {}", result.err().unwrap());
                continue;
            }

            database = result.unwrap();
            println!("Database '{}' has been created.", database.name);
            break;
        }

        // Delete
        if input.starts_with("delete") {
            let arguments = input.split_whitespace().collect::<Vec<&str>>();
            if arguments.len() != 2 {
                println!("Invalid number of arguments.");
                continue;
            }

            let result = arguments[1].parse::<usize>();
            if result.is_err() {
                println!("Failed to parse index.");
                continue;
            }

            let i = result.unwrap();
            let result = entries[i].file_name().and_then(|n| n.to_str());
            if result.is_none() {
                println!("Failed to get database name.");
                continue;
            }

            let result = load_database(result.unwrap());
            if result.is_ok() {
                database = result.unwrap();
                if database.drop().is_err() {
                    println!("Failed to delete database.");
                    continue;
                }

                println!("Database '{}' has been deleted.", database.name);
            } else {
                println!("Failed to load database: {}", result.err().unwrap());
            }
        }
        // Exit
        else if input.starts_with("exit") {
            println!("Goodbye!");
            return;
        }
        // Select
        else {
            let arguments = input.split_whitespace().collect::<Vec<&str>>();
            if arguments.len() != 1 {
                println!("Invalid number of arguments.");
                continue;
            }

            let result = arguments[0].parse::<usize>();
            if result.is_err() {
                println!("Failed to parse index.");
                continue;
            }

            let i = result.unwrap();
            let result = entries[i].file_name().and_then(|n| n.to_str());
            if result.is_none() {
                println!("Failed to get database name.");
                continue;
            }

            let result = load_database(result.unwrap());
            if result.is_ok() {
                database = result.unwrap();
                break;
            }

            println!("Failed to load database: {}", result.err().unwrap());
        }
    }

    // A database is now selected.
    println!("-------------------");
    println!("Database '{}' has been selected.", database.name);
    println!("Type 'exit' to exit the program. Type 'help' for a list of commands.");

    // Wait input.
    let mut input = String::new();

    // Execution loop
    loop {
        println!("-------------------");
        input.clear();
        io::stdin().read_line(&mut input).expect("Failed to read line.");
        // Exit
        if input.starts_with("exit") {
            break;
        }
        // Help
        else if input.starts_with("help") {
            help();
        }
        // Create
        else if input.starts_with("create") {
            let arguments = input.split_whitespace().collect::<Vec<&str>>();
            if arguments.len() != 2 {
                println!("Invalid number of arguments.");
                continue;
            }

            let table_name = arguments[1];

            // If the table already exists, do nothing.
            if database.tables.contains_key(table_name) {
                println!("Table '{}' already exists.", table_name);
                continue;
            }

            // Create the table.
            let table = database.create_table(table_name);
            if table.is_err() {
                println!("Failed to create table: {}", table.err().unwrap());
                continue;
            }

            // Print success message.
            println!("Table '{}' has been created.", table_name);
        }
        // List of tables
        else if input.starts_with("tables") {
            println!("Tables in database '{}':", database.name);
            for table in database.tables.keys() {
                println!("{}", table);
            }
        }
        // Drop
        else if input.starts_with("drop") {
            let arguments = input.split_whitespace().collect::<Vec<&str>>();
            if arguments.len() != 2 {
                println!("Invalid number of arguments.");
                continue;
            }

            let table_name = arguments[1];
            println!("Dropping table '{}'...", table_name);
            if database.drop_table(table_name).is_err() {
                println!("Failed to drop table '{}'.", table_name);
            } else {
                println!("Table '{}' has been dropped.", table_name);
            }
        }
        // Clear the screen
        else if input.starts_with("clear") {
            print!("{esc}[2J{esc}[1;1H", esc = 27 as char);
        }
        // SQL
        else if input.starts_with("insert") || input.starts_with("select") || input.starts_with("delete") {
            let result = sql_parser::parse(&input, &mut database);
            if result.is_err() {
                println!("Failed to execute SQL command: {}", result.err().unwrap());
            }
        }
        // Save
        else if input.starts_with("save") {
            println!("Saving database '{}' to disk...", database.name);
            if let Err(e) = database.save() {
                println!("Failed to save database to disk: {}", e);
            } else {
                println!("Database '{}' has been saved to disk.", database.name);
            }
        }
        // Flush
        else if input.starts_with("flush") {
            println!("Flushing memtables to disk...");
            if let Err(e) = database.flush_all() {
                println!("Failed to flush memtable to disk: {}", e);
            } else {
                println!("Memtables have been flushed to disk.");
            }
        }
        // Unknown
        else {
            println!("Unknown command.")
        }
    }

    println!("Saving database '{}' to disk...", database.name);
    if let Err(e) = database.save() {
        println!("Failed to save database to disk: {}", e);
    }

    println!("Goodbye!");
}

pub fn help() {
    println!("Available commands:");

    println!("tables - List of all tables.");
    println!("create <name> - Create a table.");
    println!("drop <name> - Drop a table.");
    println!("save - Save the database to disk.");
    println!("flush - Flush all memtables to disk.");

    println!("insert into <table> <key> <value>: Insert a new record into a table.");
    println!("select <key> from <table>: Select a record from a table.");
    println!("delete <key> from <table>: Delete a record from a table.");

    println!("clear - Clear the screen.");
    println!("exit - Exit the program.");
    println!("help - Print this help message.");
}
