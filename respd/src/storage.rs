use std::path::PathBuf;

use anyhow::Result;

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
    pub fn get_tree(&self, name: &str) -> Result<Box<dyn BaseMetaTree>, MetaError> {
        self.store.get_tree(name)
    }
}
