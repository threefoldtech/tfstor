use std::{convert::TryFrom, sync::Arc};

use anyhow::Result;
use bytes::Bytes;
use md5::{Digest, Md5};

use crate::storage::Storage;
use metastore::{BaseMetaTree, MetaError, Object, ObjectData};

/// Represents a namespace with its associated tree
pub struct Namespace {
    /// The tree for this namespace
    pub tree: Box<dyn BaseMetaTree>,
}

impl Namespace {
    /// Create a new namespace with the given name and storage
    pub fn new(storage: Arc<Storage>, name: String) -> Result<Self, MetaError> {
        let tree = storage.get_tree(name.as_str())?;
        Ok(Self { tree })
    }

    pub fn set(&self, key: &[u8], value: Bytes) -> Result<()> {
        let data = value.to_vec();
        let hash = Md5::digest(&data).into();
        let size = data.len() as u64;
        let obj_meta = Object::new(size, hash, ObjectData::Inline { data });
        self.tree.insert(key, obj_meta.to_vec())?;
        Ok(())
    }

    /// Get an Object from the tree for a given key
    fn get_object(&self, key: &[u8]) -> Result<Option<Object>, MetaError> {
        match self.tree.get(key)? {
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
        self.tree.remove(key)?;
        Ok(())
    }

    pub fn exists(&self, key: &[u8]) -> Result<bool, MetaError> {
        self.tree.contains_key(key)
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
}
