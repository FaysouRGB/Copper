mod storage;

use crate::storage::memtable::Memtable;

fn main() {
    // Create the memtable
    let mut memtable = Memtable::new();

    println!("==== EMPTY MEMTABLE ====");

    // Print the memtable
    print!("{}", memtable);

    println!("==== INSERT 10 KEY-VALUE PAIRS ====");

    // Insert 10 key-value pairs
    for i in 0..10 {
        let mut key = String::from("key_");
        key.push_str(&i.to_string());
        let mut value = String::from("value_");
        value.push_str(&i.to_string());

        memtable.set(key.as_bytes(), value.as_bytes());
    }

    // Print the memtable
    print!("{}", memtable.size / memtable.max_size * 100);

    println!("==== UPDATE THE VALUE OF KEY_5 ====");

    // Delete the key-value pair with key_5
    memtable.set("key_5".as_bytes(), "new_value_5".as_bytes());

    // Print the memtable
    print!("{}", memtable);

    println!("==== GET THE KEY-VALUE PAIR WITH KEY_5 ====");

    // Get the value of key_5
    match memtable.get("key_5".as_bytes()) {
        Some(entry) => println!("Value of key_5: {}", String::from_utf8_lossy(&entry.value)),
        None => println!("Key_5 not found"),
    }

    println!("==== DELETE THE KEY 4 ====");

    // Delete the key-value pair with key_4
    memtable.delete("key_4".as_bytes());

    // Print the memtable
    print!("{}", memtable);
}
