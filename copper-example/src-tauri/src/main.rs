// Prevents additional console window on Windows in release, DO NOT REMOVE!!
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use std::fs;

use lsm_tree::{
    column::Column,
    tree::{self, LsmTree, Value},
};
use prettytable::{Cell, Row, Table};
use serde::{Deserialize, Serialize};

mod lsm_tree;

// Learn more about Tauri commands at https://tauri.app/v1/guides/features/command
#[tauri::command]
fn greet(name: &str) -> String {
    format!("Hello, {}! You've been greeted from Rust!", name)
}

#[tauri::command]
fn get_shops() -> Vec<String> {
    let _ = std::fs::create_dir_all("shops");
    let mut result = vec![];
    for shop in std::fs::read_dir("shops").unwrap() {
        let shop = shop.unwrap();
        result.push(shop.file_name().into_string().unwrap());
    }

    result
}

#[tauri::command]
fn delete_shop(name: String) {
    let _ = std::fs::remove_dir_all(format!("shops/{}", name));
}

#[tauri::command]
fn create_shop(name: String) {
    let columns = vec![
        Column::new("Name", lsm_tree::column::DataType::Text),
        Column::new("Author", lsm_tree::column::DataType::Text),
        Column::new("Year", lsm_tree::column::DataType::Int),
        Column::new("Quantity", lsm_tree::column::DataType::Int),
    ];
    let shop_path = format!("shops/{}", name);

    let _ = tree::LsmTree::new(shop_path, columns);
}

#[tauri::command]
fn new_book(shop: String, name: String, author: String, year: String, quantity: String) {
    let mut lsm_tree = LsmTree::load(format!("shops/{}", shop)).unwrap();
    let key = name.as_bytes();
    let values = vec![
        name.as_bytes().to_vec(),
        author.as_bytes().to_vec(),
        year.parse::<i32>().unwrap().to_ne_bytes().to_vec(),
        quantity.parse::<i32>().unwrap().to_ne_bytes().to_vec(),
    ];
    let _ = lsm_tree.insert(&key, &values);
}

#[tauri::command]
fn add_book(shop: String, name: String) {
    let mut lsm_tree = LsmTree::load(format!("shops/{}", shop)).unwrap();
    let data = lsm_tree.get(&name.as_bytes().to_vec()).unwrap();
    let mut data = lsm_tree.decode(&data.unwrap());
    let a = data.get("Quantity");
    if let Value::Int(b) = a.unwrap() {
        data.insert("Quantity".to_string(), Value::Int(b + 1));
        let values = vec![
            name.as_bytes().to_vec(),
            data.get("Author").unwrap().get_text().as_bytes().to_vec(),
            data.get("Year").unwrap().get_int().to_ne_bytes().to_vec(),
            data.get("Quantity").unwrap().get_int().to_ne_bytes().to_vec(),
        ];

        let _ = lsm_tree.insert(&data.get("Name").unwrap().get_text().as_bytes().to_vec(), &values);
    }
}

#[tauri::command]
fn sell_book(shop: String, name: String) {
    let mut lsm_tree = LsmTree::load(format!("shops/{}", shop)).unwrap();
    let data = lsm_tree.get(&name.as_bytes().to_vec()).unwrap();
    let mut data = lsm_tree.decode(&data.unwrap());
    let a = data.get("Quantity");
    if let Value::Int(b) = a.unwrap() {
        data.insert("Quantity".to_string(), Value::Int(if b - 1 < 0 { 0 } else { b - 1 }));
        let values = vec![
            name.as_bytes().to_vec(),
            data.get("Author").unwrap().get_text().as_bytes().to_vec(),
            data.get("Year").unwrap().get_int().to_ne_bytes().to_vec(),
            data.get("Quantity").unwrap().get_int().to_ne_bytes().to_vec(),
        ];

        let _ = lsm_tree.insert(&data.get("Name").unwrap().get_text().as_bytes().to_vec(), &values);
    }
}

#[tauri::command]
fn remove_book(shop: String, name: String) {
    let mut lsm_tree = LsmTree::load(format!("shops/{}", shop)).unwrap();
    let _ = lsm_tree.delete(&name.as_bytes().to_vec());
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Book {
    pub name: String,
    pub author: String,
    pub year: String,
    pub quantity: String,
}

#[tauri::command]
fn fetch_book(shop: String) -> Vec<String> {
    let lsm_tree = LsmTree::load(format!("shops/{}", shop)).unwrap();
    let books = lsm_tree.get_range(|_| true).unwrap();
    let mut results = vec![];
    for book in books {
        let decode = lsm_tree.decode(&book);
        let name = decode.get("Name").unwrap().get_text();
        let author = decode.get("Author").unwrap().get_text();
        let year = decode.get("Year").unwrap().get_int();
        let in_stock = decode.get("Quantity").unwrap().get_int();

        let book = Book {
            name,
            author,
            year: year.to_string(),
            quantity: in_stock.to_string(),
        };

        results.push(serde_json::to_string(&book).unwrap())
    }

    results
}

#[tauri::command]
fn debug_print(shop: String) -> String {
    let lsm_tree = LsmTree::load(format!("shops/{}", shop)).unwrap();
    format!("{:#?}", lsm_tree)
}

#[tauri::command]
fn get_log(shop: String) -> String {
    let _ = LsmTree::load(format!("shops/{}", shop)).unwrap();
    let content = fs::read(format!("shops/{}/log.txt", shop)).unwrap();
    let text = String::from_utf8_lossy(&content).to_string();
    text
}

#[tauri::command]
fn execute_sql(shop: String, query: String) -> String {
    let mut shop = LsmTree::load(format!("shops/{}", shop)).unwrap();
    // Little sql parser with insert, select and delete. Since it is a simple parser, it is case insensitive and we only work on one table so no need for into
    if query.starts_with("insert") {
        let values: Vec<&str> = query.split_whitespace().skip(1).collect();
        if values.len() != 4 {
            return "Invalid insert query!".to_string();
        } else {
            let name = values[0];
            let author = values[1];
            let year: i32 = values[2].parse().unwrap();
            let quantity: i32 = values[3].parse().unwrap();
            let key = name.as_bytes();
            let values = vec![
                name.as_bytes().to_vec(),
                author.as_bytes().to_vec(),
                year.to_ne_bytes().to_vec(),
                quantity.to_ne_bytes().to_vec(),
            ];
            let _ = shop.insert(key, &values);
            return "Book added successfully!".to_string();
        }
    } else if query.starts_with("select") {
        // Check which rows are selected
        let rows: Vec<&str> = query.split_whitespace().skip(1).collect();
        if rows.is_empty() {
            return "Invalid select query!".to_string();
        } else {
            let mut table = Table::new();
            // Set the table column based on the selected column of the select query
            let mut header = vec![];
            for row in rows {
                match row {
                    "name" => {
                        header.push("Name");
                    }
                    "author" => {
                        header.push("Author");
                    }
                    "year" => {
                        header.push("Year");
                    }
                    "quantity" => {
                        header.push("Quantity");
                    }
                    _ => {
                        return "Invalid select query!".to_string();
                    }
                }
            }

            header.sort();
            // Add each cell of the header
            let mut row: Vec<Cell> = vec![];
            for column in &header {
                row.push(Cell::new(column));
            }
            table.add_row(Row::new(row));

            // Get all book with get_range returning always true for the predicate
            let books = shop.get_range(|_| true).unwrap();
            for book in books {
                let decode = shop.decode(&book);
                let mut row: Vec<Cell> = vec![];
                for column in &header {
                    let value = decode.get(&column.to_string()).unwrap();
                    match value {
                        Value::Text(value) => {
                            row.push(Cell::new(value));
                        }
                        Value::Int(value) => {
                            row.push(Cell::new(&value.to_string()));
                        }
                        Value::Bool(value) => {
                            row.push(Cell::new(&value.to_string()));
                        }
                    }
                }
                table.add_row(Row::new(row));
            }

            return table.to_string();
        }
    } else if query.starts_with("delete") {
        let name = query.split_whitespace().nth(1);
        if let Some(name) = name {
            let key = name.as_bytes();
            let result = shop.delete(key).unwrap();
            if result {
                return "Book removed successfully!".to_string();
            } else {
                return "Book not found!".to_string();
            }
        } else {
            return "Invalid delete query!".to_string();
        }
    } else if query.starts_with("update") {
        let values: Vec<&str> = query.split_whitespace().skip(1).collect();
        if values.len() != 4 {
            return "Invalid update query!".to_string();
        } else {
            let name = values[0];
            let author = values[1];
            let year: i32 = values[2].parse().unwrap();
            let quantity: i32 = values[3].parse().unwrap();
            let key = name.as_bytes();
            let values = vec![
                name.as_bytes().to_vec(),
                author.as_bytes().to_vec(),
                year.to_ne_bytes().to_vec(),
                quantity.to_ne_bytes().to_vec(),
            ];
            let _ = shop.insert(key, &values);
            return "Book updated successfully!".to_string();
        }
    } else {
        return "Invalid query!".to_string();
    }
}

fn main() {
    tauri::Builder::default()
        .invoke_handler(tauri::generate_handler![
            greet,
            get_shops,
            create_shop,
            delete_shop,
            add_book,
            sell_book,
            remove_book,
            fetch_book,
            new_book,
            debug_print,
            get_log,
            execute_sql
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
