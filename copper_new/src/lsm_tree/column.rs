use std::fmt::Debug;

pub struct Column {
    name: String,
    data_type: DataType,
}

#[repr(u8)]
pub enum DataType {
    Int = 0,
    Text = 1,
    Bool = 2,
}

impl Column {
    pub fn new(name: String, data_type: DataType) -> Self {
        Self { name, data_type }
    }

    pub fn get_name(&self) -> &str {
        &self.name
    }

    pub fn get_data_type(&self) -> &DataType {
        &self.data_type
    }
}

impl DataType {
    pub fn get_char(&self) -> char {
        match self {
            DataType::Int => 'i',
            DataType::Text => 't',
            DataType::Bool => 'b',
        }
    }

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
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} ({:?})", self.name, self.data_type)
    }
}

impl Debug for DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.get_char())
    }
}
