use std::fmt::Debug;

/// `Column` struct represents a column in a database table.
/// It has a `name` and a `data_type`.
pub struct Column {
    name: String,
    data_type: DataType,
}

/// `DataType` enum represents the type of data that can be stored in a `Column`.
/// It can be an `Int`, `Text`, or `Bool`.
#[repr(u8)]
pub enum DataType {
    Int = 0,
    Text = 1,
    Bool = 2,
}

impl Column {
    /// Creates a new `Column` with the given `name` and `data_type`.
    pub fn new(name: &str, data_type: DataType) -> Self {
        Self { name: name.to_string(), data_type }
    }

    /// Returns the name of the `Column`.
    pub fn get_name(&self) -> &str {
        &self.name
    }

    /// Returns the data type of the `Column`.
    pub fn get_data_type(&self) -> &DataType {
        &self.data_type
    }
}

impl DataType {
    /// Returns a character representing the `DataType`.
    /// 'i' for `Int`, 't' for `Text`, 'b' for `Bool`.
    pub fn get_char(&self) -> char {
        match self {
            DataType::Int => 'i',
            DataType::Text => 't',
            DataType::Bool => 'b',
        }
    }

    /// Returns a `DataType` from a character.
    /// 'i' for `Int`, 't' for `Text`, 'b' for `Bool`.
    /// Panics if the character is not 'i', 't', or 'b'.
    pub fn from_char(c: char) -> Self {
        match c {
            'i' => DataType::Int,
            't' => DataType::Text,
            'b' => DataType::Bool,
            _ => panic!("Invalid data type character"),
        }
    }
}

impl Debug for Column {
    /// Formats the `Column` for printing.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} ({:?})", self.name, self.data_type)
    }
}

impl Debug for DataType {
    /// Formats the `DataType` for printing.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.get_char())
    }
}
