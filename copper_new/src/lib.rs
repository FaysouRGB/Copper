/// `lsm_tree` module
///
/// This module contains the implementation of a Log-Structured Merge-Tree (LSM Tree),
/// a data structure commonly used in storage systems for write-heavy workloads.
/// The LSM Tree provides efficient write and read operations by maintaining data
/// in different levels of sorted runs and periodically compacting these runs to
/// maintain performance. The specific details of the implementation, including the
/// data structures and algorithms used, can be found within the module.
pub mod lsm_tree;
