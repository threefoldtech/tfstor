use anyhow::Result;
use metastore::{MetaStore, MetaError, FjallStore, Durability};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;

// Default tree name for key-value storage
const DEFAULT_KV_TREE: &str = "_KV_STORE";

/// Storage implementation using metastore with inlined data
pub struct MetaStorage {
    store: Arc<Mutex<MetaStore>>,
}

impl MetaStorage {
    /// Create a new MetaStorage instance
    pub fn new(data_dir: PathBuf, inlined_metadata_size: Option<usize>) -> Self {
        // Create the metastore with FjallStore backend
        let fjall_store = FjallStore::new(
            data_dir,
            inlined_metadata_size,
            Some(Durability::Fdatasync),
        );
        
        let store = MetaStore::new(fjall_store, inlined_metadata_size);
        
        // The tree will be created automatically when we try to access it

        Self {
            store: Arc::new(Mutex::new(store)),
        }
    }

    /// Get a value by key
    pub async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, MetaError> {
        let store = self.store.lock().await;
        let tree = store.get_tree(DEFAULT_KV_TREE)?;
        tree.get(key.as_bytes())
    }

    /// Set a key-value pair
    pub async fn set(&self, key: &str, value: Vec<u8>) -> Result<(), MetaError> {
        let store = self.store.lock().await;
        let tree = store.get_tree(DEFAULT_KV_TREE)?;
        tree.insert(key.as_bytes(), value)
    }

    /// Delete a key-value pair
    pub async fn delete(&self, key: &str) -> Result<(), MetaError> {
        let store = self.store.lock().await;
        let tree = store.get_tree(DEFAULT_KV_TREE)?;
        tree.remove(key.as_bytes())
    }

    /// Check if a key exists
    pub async fn exists(&self, key: &str) -> Result<bool, MetaError> {
        let result = self.get(key).await?;
        Ok(result.is_some())
    }
}
