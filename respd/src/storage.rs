use std::error::Error;
use std::fmt;
use std::path::PathBuf;

use anyhow::Result;
use serde::{Deserialize, Serialize};

use metastore::{BaseMetaTree, Durability, FjallStore, MetaError, MetaStore};

// Default tree name for key-value storage
// No longer using a default tree name as we'll use the namespace as the tree name

/// Storage implementation using metastore with inlined data
pub struct Storage {
    store: MetaStore,
}

impl Storage {
    /// Create a new MetaStorage instance
    pub fn new(data_dir: PathBuf, inlined_metadata_size: Option<usize>) -> Self {
        // Create the metastore with FjallStore backend
        let fjall_store =
            FjallStore::new(data_dir, inlined_metadata_size, Some(Durability::Fdatasync));

        let store = MetaStore::new(fjall_store, inlined_metadata_size);

        Self { store }
    }

    /// Get a namespace instance for a specific namespace name
    pub fn get_namespace(&self, name: &str) -> Result<Box<dyn BaseMetaTree>, StorageError> {
        if !self.store.bucket_exists(name)? {
            return Err(StorageError::NamespaceNotFound);
        }

        // TODO: get namespace meta

        self.store
            .get_tree(name)
            .map_err(|e| StorageError::MetaError(e.to_string()))
    }

    pub fn create_namespace(&self, name: &str) -> Result<Box<dyn BaseMetaTree>, StorageError> {
        if self.store.bucket_exists(name)? {
            return Err(StorageError::NamespaceNotFound);
        }
        let namespace_meta_raw = NamespaceMeta::new(name.to_string())
            .to_msgpack()
            .map_err(|e| MetaError::OtherDBError(e.to_string()))?;
        self.store.insert_bucket(name, namespace_meta_raw)?;
        self.get_namespace(name)
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct NamespaceMeta {
    name: String,
    password: Option<String>,
    max_size: Option<u64>,
    private: bool,
    read_only: bool,
    freeze: bool,
    key_mode: KeyMode,
}

#[derive(Debug, Serialize, Deserialize)]
enum KeyMode {
    UserKey,
    Sequential,
}

impl NamespaceMeta {
    /// Create a new NamespaceMeta with default values
    ///
    /// All fields are defaulted to false/None except for the name
    pub fn new(name: String) -> Self {
        Self {
            name,
            password: None,
            max_size: None,
            private: false,
            read_only: false,
            freeze: false,
            key_mode: KeyMode::UserKey,
        }
    }

    /// Encode the NamespaceMeta to MessagePack format
    pub fn to_msgpack(&self) -> Result<Vec<u8>> {
        rmp_serde::to_vec(self)
            .map_err(|e| anyhow::anyhow!("Failed to encode NamespaceMeta to MessagePack: {}", e))
    }

    /// Decode a NamespaceMeta from MessagePack format
    #[allow(dead_code)]
    pub fn from_msgpack(data: &[u8]) -> Result<Self> {
        rmp_serde::from_slice(data)
            .map_err(|e| anyhow::anyhow!("Failed to decode NamespaceMeta from MessagePack: {}", e))
    }
}

#[derive(Debug)]
pub enum StorageError {
    NamespaceNotFound,
    MetaError(String),
}

// Implement the std::error::Error trait
impl Error for StorageError {}

// Implement the Display trait for custom error messages
impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            StorageError::NamespaceNotFound => write!(f, "Namespace not found"),
            StorageError::MetaError(ref msg) => write!(f, "{}", msg),
        }
    }
}

// Implement conversion from MetaError to StorageError
impl From<MetaError> for StorageError {
    fn from(error: MetaError) -> Self {
        StorageError::MetaError(error.to_string())
    }
}
