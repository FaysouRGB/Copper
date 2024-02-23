mod lsm_tree;

#[cfg(test)]
mod tests {
    use crate::lsm_tree::LSMTree;
    use std::fs;

    #[test]
    fn test_insert_get_delete() {
        let mut lsm_tree = LSMTree::new();

        // Insert data
        lsm_tree.insert(b"key1".to_vec(), b"value1".to_vec());
        lsm_tree.insert(b"key2".to_vec(), b"value2".to_vec());

        // Get data
        assert_eq!(lsm_tree.get(b"key1"), Some(b"value1".to_vec()));
        assert_eq!(lsm_tree.get(b"key2"), Some(b"value2".to_vec()));

        // Delete data
        lsm_tree.delete(b"key1");
        assert_eq!(lsm_tree.get(b"key1"), None);
    }

    #[test]
    fn test_flush_memtable() {
        let mut lsm_tree = LSMTree::new();

        // Insert data
        lsm_tree.insert(b"key1".to_vec(), b"value1".to_vec());
        lsm_tree.insert(b"key2".to_vec(), b"value2".to_vec());

        // Flush memtable to disk
        assert!(lsm_tree.flush_memtable().is_ok());
    }

    #[test]
    fn test_load_on_disk() {
        let mut lsm_tree = LSMTree::new();

        // Load data from disk
        assert!(lsm_tree.load_on_disk().is_ok());

        // Verify loaded data
        assert_eq!(lsm_tree.get(b"key1"), Some(b"value1".to_vec()));
        assert_eq!(lsm_tree.get(b"key2"), Some(b"value2".to_vec()));
    }

    #[test]
    fn test_compact_actual_level() {
        let mut lsm_tree = LSMTree::new();

        // Insert data
        lsm_tree.insert(b"key1".to_vec(), b"value1".to_vec());
        lsm_tree.insert(b"key2".to_vec(), b"value2".to_vec());

        // Flush memtable to disk
        lsm_tree.flush_memtable().unwrap();

        // Compact actual level
        assert!(lsm_tree.compact_actual_level().is_ok());
    }

    #[test]
    fn test_save_on_disk() {
        let mut lsm_tree = LSMTree::new();

        // Insert data
        lsm_tree.insert(b"key1".to_vec(), b"value1".to_vec());
        lsm_tree.insert(b"key2".to_vec(), b"value2".to_vec());

        // Flush memtable to disk
        lsm_tree.flush_memtable().unwrap();

        // Compact actual level
        lsm_tree.compact_actual_level().unwrap();

        // Save data to disk
        assert!(lsm_tree.save_on_disk().is_ok());
    }

    #[cfg(test)]
    fn cleanup() {
        // Cleanup test files and directories
        let _ = fs::remove_dir_all("./sstables");
    }

    #[test]
    fn test_all() {
        test_insert_get_delete();
        test_flush_memtable();
        test_load_on_disk();
        test_compact_actual_level();
        test_save_on_disk();
        cleanup();
    }
}

