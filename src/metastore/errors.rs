use std::error::Error;
use std::fmt;

// Define the error type
#[derive(Debug)]
pub enum MetaError {
    KeyNotFound,
    KeyAlreadyExists,
    CollectionNotFound,
    BucketNotFound,
    NotMetaTree(String),
    TransactionError(String),
    PersistError(String),
    OtherDBError(String),
}

// Implement the std::error::Error trait
impl Error for MetaError {}

// Implement the Display trait for custom error messages
impl fmt::Display for MetaError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            MetaError::KeyNotFound => write!(f, "Key not found"),
            MetaError::KeyAlreadyExists => write!(f, "Key already exists"),
            MetaError::CollectionNotFound => write!(f, "Collection not found"),
            MetaError::BucketNotFound => write!(f, "Bucket not found"),
            MetaError::NotMetaTree(ref s) => write!(f, "Not a meta tree: {}", s),
            MetaError::TransactionError(ref s) => write!(f, "Transaction error: {}", s),
            MetaError::PersistError(ref s) => write!(f, "Persist error: {}", s),
            MetaError::OtherDBError(ref s) => write!(f, "Other DB error: {}", s),
        }
    }
}

use std::io;

impl From<MetaError> for io::Error {
    fn from(error: MetaError) -> Self {
        io::Error::new(io::ErrorKind::Other, error.to_string())
    }
}
