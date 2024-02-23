mod memtable;
use crate::memtable::Memtable;

#[cfg(test)]
mod tests {
    use super::*;

    // Write me tests to see if my memtable implementation is working correctly, be sure to check edge cases.
    #[test]
    fn test_memtable() {
        let mut memtable = Memtable::new();
        let key = "key".as_bytes();
        let value = "value".as_bytes();
        memtable.set(key, value);
        assert_eq!(memtable.get(key).unwrap().value, value);
    }
}
