/// Represents a database entry.
///
/// An `Entry` consists of a key, a value, and a flag indicating whether the entry has been deleted.
///
/// # Fields
///
/// * `key` - A `Vec<u8>` representing the key of the entry.
/// * `value` - A `Vec<u8>` representing the value of the entry.
/// * `deleted` - A `bool` indicating whether the entry has been deleted. `true` if the entry has been deleted, `false` otherwise.
///
/// # Examples
///
/// ```
/// use your_crate::Entry;
///
/// let entry = Entry::new(&[1, 2, 3], &[4, 5, 6], false);
/// ```
pub struct Entry {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub deleted: bool,
}

impl Entry {
    /// Creates a new `Entry`.
    ///
    /// # Arguments
    ///
    /// * `key` - A byte slice that holds the key.
    /// * `value` - A byte slice that holds the value.
    /// * `deleted` - A boolean indicating whether the entry is deleted.
    ///
    /// # Returns
    ///
    /// A new `Entry`.
    ///
    /// # Examples
    ///
    /// ```
    /// use your_crate::Entry;
    ///
    /// let entry = Entry::new(&[1, 2, 3], &[4, 5, 6], false);
    /// ```
    pub fn new(key: &[u8], value: &[u8], deleted: bool) -> Self {
        Self { key: key.to_vec(), value: value.to_vec(), deleted }
    }

    /// Returns the size of the `Entry`.
    ///
    /// The size is calculated as the sum of the lengths of the key and value, plus 1 for the `deleted` field.
    ///
    /// # Returns
    ///
    /// The size of the `Entry`.
    ///
    /// # Examples
    ///
    /// ```
    /// use your_crate::Entry;
    ///
    /// let entry = Entry::new(&[1, 2, 3], &[4, 5, 6], false);
    /// let size = entry.get_size(); // size will be 7
    /// ```
    pub fn get_size(&self) -> usize {
        self.key.len() + self.value.len() + 1
    }
}
