# Copper

Meet Copper, a fast and easy to use Rust database library,
along with a SQL-like query language.

# Getting started

## Creating a table

Let's begin by creating your first table:

```rust
use crate::lsm_tree::tree::LsmTree;
use crate::lsm_tree::column::Column;
use crate::lsm_tree::column::DataType;

// Define your table's columns
let columns = vec![
    Column::new("Name", DataType::Text),
    Column::new("Age", DataType::Int),
];

// Create the table
let table = LsmTree::new("my_table", columns);
```

## Adding an entry

Now, let's add an entry:

```rust
// Create a key
let key = "Jane".as_bytes();

// Create the row
let row = vec!["Jane".as_bytes(), 21_i32.as_ne_bytes()]

// Insert into the table; if the key was already present,
// it will be replaced, and insert() will return true
let was_previously_present = shop.insert(key, &values);
```

## Getting an entry

To retrieve an entry, follow these steps:

```rust
// Create a key
let key = "Jane".as_bytes();

// Retrieve the bytes associated with it
let bytes = table.get(key);


// Decode the value into multiple values based on the columns
let values = table.decode(bytes);
```

## Deleting an entry

To delete an entry, you could do it like this:

```rust
// Create a key
let key = "Jane".as_bytes();

// Delete it and check if the operation was performed
let was_deleted = table.delete(key).expect("An error happened while trying to delete");
```

## Next

You can access the documentation by typing:

```shell
cargo doc
```

Don't forget to check our [website](https://faysourgb.github.io/Copper/website/index.html) for more information.

# Authors

This project was authored by:

- Fayçal Beghalia (faycal.beghalia@epita.fr)

- Salma Boubakkar (salma.boubakkar@epita.fr)

- Wilfrid Wangon-Zekou (wilfird.wangon-zekou@epita.fr)
