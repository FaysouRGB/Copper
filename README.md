# Copper DBMS

Copper DBMS is a database management library written in Rust as a S4 project at EPITA. It includes also include a sql-like query language.

# Getting started

To create your first database, use the following code:

```rust
let columns = vec![Column::new("Name".to_string(), lsm_tree::column::DataType::Text), Column::new("Author".to_string(), lsm_tree::column::DataType::Text)];

let shop = tree::LsmTree::new("my_database", columns);
```

To add an entry, use:

```rust
let key = name.as_bytes();
let values = vec![name.as_bytes().to_vec(), author.as_bytes().to_vec(), year.to_ne_bytes().to_vec(), b"\x01".to_vec()];
let _ = shop.insert(key, &values);
```

And voila! You can visit the documentation by typing:

```shell
cargo doc
```

Or by taking a look at a library manager showcasing the different use-cases.

# Authors

This project has been done by:

- Fayçal Beghalia (faycal.beghalia@epita.fr)

- Salma Boubakkar (salma.boubakkar@epita.fr)

- Wilfrid (wilfrid.wangon-zekou@epita.fr)
