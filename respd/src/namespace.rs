use std::collections::HashMap;
use std::sync::RwLock;
use std::{convert::TryFrom, sync::Arc};

use anyhow::Result;
use bytes::Bytes;
use md5::{Digest, Md5};
use tracing::debug;

use crate::storage::{Storage, StorageError};
use metastore::{MetaError, MetaTreeExt, Object, ObjectData};

/// Represents a namespace with its associated tree
pub struct Namespace {
    /// The tree for this namespace
    pub tree: Arc<Box<dyn MetaTreeExt + Send + Sync>>,
}

/// A cache for namespace instances to allow sharing between clients
pub struct NamespaceCache {
    storage: Arc<Storage>,
    namespaces: RwLock<HashMap<String, Arc<Namespace>>>,
}

impl NamespaceCache {
    /// Create a new namespace cache
    pub fn new(storage: Arc<Storage>) -> Self {
        Self {
            storage,
            namespaces: RwLock::new(HashMap::new()),
        }
    }

    /// Get a namespace from the cache or create a new one if it doesn't exist
    pub fn get_or_create(&self, name: String) -> Result<Arc<Namespace>, StorageError> {
        // First, try to get from cache
        {
            let namespaces = self.namespaces.read().unwrap();
            if let Some(namespace) = namespaces.get(&name) {
                debug!("Using cached namespace: {}", name);
                return Ok(namespace.clone());
            }
        }

        // Not in cache, create a new one
        debug!("Creating new namespace: {}", name);
        let tree = self.storage.get_namespace(name.as_str())?;
        let namespace = Arc::new(Namespace {
            tree: Arc::new(tree),
        });

        // Store in cache
        {
            let mut namespaces = self.namespaces.write().unwrap();
            namespaces.insert(name, namespace.clone());
        }

        Ok(namespace)
    }

    /// Create a namespace if it doesn't exist and return it
    pub fn create_if_not_exists(&self, name: String) -> Result<Arc<Namespace>, StorageError> {
        match self.get_or_create(name.clone()) {
            Ok(namespace) => Ok(namespace),
            Err(_) => {
                // Try to create the namespace
                debug!("Creating new namespace that didn't exist: {}", name);
                let tree = self.storage.create_namespace(&name)?;
                let namespace = Arc::new(Namespace {
                    tree: Arc::new(tree),
                });

                // Store in cache
                {
                    let mut namespaces = self.namespaces.write().unwrap();
                    namespaces.insert(name, namespace.clone());
                }

                Ok(namespace)
            }
        }
    }
}

impl Namespace {
    // Create a new namespace with the given name and storage
    /*pub fn new(storage: Arc<Storage>, name: String) -> Result<Self, StorageError> {
        let tree = storage.get_namespace(name.as_str())?;
        Ok(Self {
            tree: Arc::new(tree),
        })
    }*/

    pub fn set(&self, key: &[u8], value: Bytes) -> Result<()> {
        let data = value.to_vec();
        let hash = Md5::digest(&data).into();
        let size = data.len() as u64;
        let obj_meta = Object::new(size, hash, ObjectData::Inline { data });
        (**self.tree).insert(key, obj_meta.to_vec())?;
        Ok(())
    }

    /// Get an Object from the tree for a given key
    fn get_object(&self, key: &[u8]) -> Result<Option<Object>, MetaError> {
        match (**self.tree).get(key)? {
            Some(data) => {
                let obj = Object::try_from(&*data).expect("Malformed object");
                Ok(Some(obj))
            }
            None => Ok(None),
        }
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>, MetaError> {
        let obj_meta = self.get_object(key)?;
        match obj_meta {
            Some(obj) => {
                if let Some(data) = obj.inlined() {
                    let bytes = bytes::Bytes::from(data.clone());
                    Ok(Some(bytes))
                } else {
                    Err(MetaError::OtherDBError("Object is not inline".to_string()))
                }
            }
            None => Ok(None),
        }
    }

    pub fn del(&self, key: &[u8]) -> Result<()> {
        (**self.tree).remove(key)?;
        Ok(())
    }

    pub fn exists(&self, key: &[u8]) -> Result<bool, MetaError> {
        (**self.tree).contains_key(key)
    }

    /// Get the length (size) of a key's value
    /// Returns None if the key doesn't exist
    pub fn length(&self, key: &[u8]) -> Result<Option<u64>, MetaError> {
        match self.get_object(key)? {
            Some(obj) => Ok(Some(obj.size())),
            None => Ok(None),
        }
    }

    /// Get the last-modified timestamp of a key
    /// Returns None if the key doesn't exist
    pub fn keytime(&self, key: &[u8]) -> Result<Option<i64>, MetaError> {
        match self.get_object(key)? {
            Some(obj) => {
                // Get the last modified time as Unix timestamp (seconds since epoch)
                let system_time = obj.last_modified();
                let timestamp = system_time
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs() as i64;
                Ok(Some(timestamp))
            }
            None => Ok(None),
        }
    }

    pub fn check(&self, key: &[u8]) -> Result<Option<bool>, MetaError> {
        let obj = self.get_object(key)?;
        match obj {
            Some(obj) => {
                if let Some(data) = obj.inlined() {
                    // check the hash
                    let hash: [u8; 16] = Md5::digest(data).into();
                    if hash != *obj.hash() {
                        Ok(Some(false))
                    } else {
                        Ok(Some(true))
                    }
                } else {
                    Err(MetaError::OtherDBError("Object is not inline".to_string()))
                }
            }
            None => Ok(None),
        }
    }

    pub fn num_keys(&self) -> usize {
        (**self.tree).len()
    }

    pub fn scan(
        &self,
        start_after: Option<Vec<u8>>,
        num_keys: u32,
    ) -> Result<Vec<Vec<u8>>, MetaError> {
        let mut keys = Vec::new();
        let mut count = 0;

        for result in (**self.tree).iter_kv(start_after) {
            match result {
                Ok((key, _)) => {
                    keys.push(key);
                    count += 1;
                    if count >= num_keys {
                        break;
                    }
                }
                Err(e) => return Err(e),
            }
        }

        Ok(keys)
    }
}
