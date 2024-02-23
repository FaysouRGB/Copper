use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

pub struct BloomFilter {
    pub hash_functions: usize,
    pub bits: Vec<bool>,
    pub size: usize,
}

impl BloomFilter {
    // Returns a new BloomFilter.
    pub fn new(size: usize, hash_functions: usize) -> BloomFilter;

    // Modifies the bits of the vector to add the item.
    pub fn insert<T>(&mut self, item: &T)
    where
        T: Hash,
    {
    }

    // Check if the BloomFilter may contains the item.
    pub fn contains<T>(&self, item: &T) -> bool
    where
        T: Hash,
    {
    }
}
