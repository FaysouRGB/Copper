//! Copper DBMS
//!
//! This library is a rust implementation of a database library featuring a sql-like query language.
//! You can learn more about it on the project's website.

pub mod database;
pub mod entry;
pub mod errors;
pub mod interface;
pub mod lsm_tree;
pub mod memtable;
pub mod settings;
pub mod sql_parser;
pub mod table;
pub mod wal;

#[cfg(test)]
mod tests {
    use crate::database::Database;
    #[test]
    pub fn database_tests() {
        let mut database = Database::new("my_database").unwrap();
        let _ = database.create_table("table1");
        let _ = database.insert_into_table("table1", b"key1", b"value1");
        assert_eq!(database.get_from_table("table1", b"key1".as_ref()).unwrap(), Some(b"value1".to_vec()));
        let _ = database.delete_from_table("table1", b"key1".as_ref());
        assert_eq!(database.get_from_table("table1", b"key1".as_ref()).unwrap(), None);
    }

    #[test]
    pub fn lsm_tree_tests() {
        /*
        // Create a new LSM-Tree.
        let mut lsm_tree = LSMTree::new(100, "test_database");

        // Test insert.
        lsm_tree.insert(b"key1".to_vec(), b"value1".to_vec());
        assert_eq!(lsm_tree.get(b"key1".as_ref()), Some(b"value1".to_vec()));

        // Test overwrite.
        lsm_tree.insert(b"key1".to_vec(), b"value2".to_vec());
        assert_eq!(lsm_tree.get(b"key1".as_ref()), Some(b"value2".to_vec()));

        // Test delete.
        lsm_tree.delete(b"key1".as_ref());
        assert_eq!(lsm_tree.get(b"key1".as_ref()), None);

        lsm_tree.flush_memtable();

        assert!(!lsm_tree.levels.is_empty());
        assert!(lsm_tree.memtable.records.is_empty());
        */
    }
}
