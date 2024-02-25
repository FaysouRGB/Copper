use std::io;

pub fn invalid_input(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

pub fn invalid_data(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

pub fn not_found(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::NotFound, message)
}

pub fn other(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::Other, message)
}
