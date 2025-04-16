use anyhow::Result;
use metastore::{BaseMetaTree, Durability, FjallStore, MetaError, MetaStore};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;

// Default tree name for key-value storage
// No longer using a default tree name as we'll use the namespace as the tree name

/// Storage implementation using metastore with inlined data
pub struct MetaStorage {
    store: Arc<Mutex<MetaStore>>,
}

impl MetaStorage {
    /// Create a new MetaStorage instance
    pub fn new(data_dir: PathBuf, inlined_metadata_size: Option<usize>) -> Self {
        // Create the metastore with FjallStore backend
        let fjall_store =
            FjallStore::new(data_dir, inlined_metadata_size, Some(Durability::Fdatasync));

        let store = MetaStore::new(fjall_store, inlined_metadata_size);

        // The tree will be created automatically when we try to access it

        Self {
            store: Arc::new(Mutex::new(store)),
        }
    }

    /// Get a tree for a specific namespace
    pub async fn get_tree(&self, namespace: &str) -> Result<Box<dyn BaseMetaTree>, MetaError> {
        let store = self.store.lock().await;
        store.get_tree(namespace)
    }
}
