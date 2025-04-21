use std::fmt::Debug;
use std::str::FromStr;

use super::{object::Object, MetaError, Transaction};

/// `BaseMetaTree` defines the core operations for a metadata tree storage.
///
/// This trait provides the fundamental operations needed to interact with a key-value
/// storage system, including inserting, removing, and retrieving values.
#[allow(clippy::len_without_is_empty)]
pub trait BaseMetaTree: Send + Sync {
    /// Inserts a key-value pair into the tree.
    ///
    /// # Arguments
    /// * `key` - The key as a byte slice
    /// * `value` - The value as a vector of bytes
    ///
    /// # Returns
    /// * `Result<(), MetaError>` - Success or an error if the insertion fails
    fn insert(&self, key: &[u8], value: Vec<u8>) -> Result<(), MetaError>;

    /// Removes a key and its associated value from the tree.
    ///
    /// # Arguments
    /// * `key` - The key to remove as a byte slice
    ///
    /// # Returns
    /// * `Result<(), MetaError>` - Success or an error if the removal fails
    fn remove(&self, key: &[u8]) -> Result<(), MetaError>;

    /// Checks if a key exists in the tree.
    ///
    /// # Arguments
    /// * `key` - The key to check as a byte slice
    ///
    /// # Returns
    /// * `Result<bool, MetaError>` - True if the key exists, false otherwise, or an error
    fn contains_key(&self, key: &[u8]) -> Result<bool, MetaError>;

    /// Retrieves a value for the given key.
    ///
    /// # Arguments
    /// * `key` - The key to look up as a byte slice
    ///
    /// # Returns
    /// * `Result<Option<Vec<u8>>, MetaError>` - The value if found, None if the key doesn't exist, or an error
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, MetaError>;

    /// Returns the number of key-value pairs in the tree.
    ///
    /// # Returns
    /// * `usize` - The number of entries
    fn len(&self) -> usize;

    fn is_empty(&self) -> Result<bool, MetaError>;
}

/// Type alias for a boxed iterator over key-value pairs.
pub type KeyValuePairs = Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>), MetaError>> + Send>;

/// `MetaTreeExt` extends the `BaseMetaTree` with additional operations.
///
/// This trait provides more advanced functionality like iteration and filtering
/// on top of the basic tree operations.
pub trait MetaTreeExt: BaseMetaTree {
    /// Iterates over all key-value pairs in the tree.
    ///
    /// # Arguments
    /// * `start_after` - Optional key to start iteration after (exclusive)
    ///
    /// # Returns
    /// * `KeyValuePairs` - A boxed iterator over all key-value pairs
    fn iter_kv(&self, start_after: Option<Vec<u8>>) -> KeyValuePairs;

    /// Filters and iterates over a range of keys with optional filtering parameters.
    ///
    /// # Arguments
    /// * `start_after` - Optional string to start iteration after
    /// * `prefix` - Optional prefix to filter keys
    /// * `continuation_token` - Optional token for pagination
    ///
    /// # Returns
    /// * A boxed iterator yielding key-value pairs as (String, Object) tuples
    fn range_filter<'a>(
        &'a self,
        start_after: Option<String>,
        prefix: Option<String>,
        continuation_token: Option<String>,
    ) -> Box<(dyn Iterator<Item = (String, Object)> + 'a)>;
}

/// `Store` represents a storage backend for metadata trees.
///
/// This trait defines operations for managing multiple metadata trees,
/// including creating, opening, and deleting trees, as well as transaction support.
pub trait Store: Send + Sync + Debug + 'static {
    /// Opens a tree with the given name, creating it if it doesn't exist.
    ///
    /// # Arguments
    /// * `name` - The name of the tree to open
    ///
    /// # Returns
    /// * `Result<Box<dyn BaseMetaTree>, MetaError>` - A boxed trait object implementing BaseMetaTree or an error
    fn tree_open(&self, name: &str) -> Result<Box<dyn BaseMetaTree>, MetaError>;

    /// Opens a tree with extended functionality.
    ///
    /// # Arguments
    /// * `name` - The name of the tree to open
    ///
    /// # Returns
    /// * `Result<Box<dyn MetaTreeExt + Send + Sync>, MetaError>` - A boxed trait object implementing MetaTreeExt or an error
    fn tree_ext_open(&self, name: &str) -> Result<Box<dyn MetaTreeExt + Send + Sync>, MetaError>;

    /// Checks if a tree with the given name exists.
    ///
    /// # Arguments
    /// * `name` - The name of the tree to check
    ///
    /// # Returns
    /// * `Result<bool, MetaError>` - True if the tree exists, false otherwise, or an error
    fn tree_exists(&self, name: &str) -> Result<bool, MetaError>;

    /// Deletes the tree with the given name.
    ///
    /// # Arguments
    /// * `name` - The name of the tree to delete
    ///
    /// # Returns
    /// * `Result<(), MetaError>` - Success or an error if the deletion fails
    fn tree_delete(&self, name: &str) -> Result<(), MetaError>;

    /// Begins a new transaction.
    ///
    /// # Returns
    /// * `Transaction` - A new transaction object
    fn begin_transaction(&self) -> Transaction;

    /// Returns the total disk space used by the storage.
    ///
    /// # Returns
    /// * `u64` - The disk space usage in bytes
    fn disk_space(&self) -> u64;
}

/// `Durability` defines the durability guarantees for storage operations.
///
/// This enum represents different levels of durability that can be used
/// when configuring storage operations.
#[derive(Debug, Clone, Copy)]
pub enum Durability {
    /// Data is buffered in memory and will be written to disk later.
    /// This provides the highest performance but lowest durability.
    Buffer,

    /// Data is synchronized to disk with full metadata using fsync.
    /// This provides the highest durability but lowest performance.
    Fsync,

    /// Data is synchronized to disk without metadata using fdatasync.
    /// This provides a balance between durability and performance.
    Fdatasync,
}

impl FromStr for Durability {
    type Err = String;

    /// Converts a string to a `Durability` enum value.
    ///
    /// # Arguments
    /// * `s` - The string to parse
    ///
    /// # Returns
    /// * `Result<Self, Self::Err>` - The parsed durability level or an error
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "buffer" => Ok(Durability::Buffer),
            "fsync" => Ok(Durability::Fsync),
            "fdatasync" => Ok(Durability::Fdatasync),
            _ => Err(format!("Unknown durability option: {}", s)),
        }
    }
}
