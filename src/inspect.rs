use anyhow::Result;
use std::path::PathBuf;

use crate::cas::StorageEngine;
use crate::metastore::{FjallStore, FjallStoreNotx, MetaStore};

pub fn num_keys(meta_root: PathBuf, storage_engine: StorageEngine) -> Result<usize> {
    let meta_store: Box<dyn MetaStore> = match storage_engine {
        StorageEngine::Fjall => Box::new(FjallStore::new(meta_root, None, None)),
        StorageEngine::FjallNotx => Box::new(FjallStoreNotx::new(meta_root, None)),
    };

    let (bucket_keys, block_keys, path_keys) = meta_store.num_keys();
    Ok(bucket_keys + block_keys + path_keys)
}

pub fn disk_space(meta_root: PathBuf, storage_engine: StorageEngine) -> u64 {
    let meta_store: Box<dyn MetaStore> = match storage_engine {
        StorageEngine::Fjall => Box::new(FjallStore::new(meta_root, None, None)),
        StorageEngine::FjallNotx => Box::new(FjallStoreNotx::new(meta_root, None)),
    };

    meta_store.disk_space()
}
