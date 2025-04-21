use anyhow::Result;
use std::path::PathBuf;

use crate::cas::StorageEngine;
use metastore::{FjallStore, FjallStoreNotx, MetaStore};

pub fn num_keys(
    meta_root: PathBuf,
    storage_engine: StorageEngine,
    bucket_name: &str,
) -> Result<usize> {
    let meta_store = match storage_engine {
        StorageEngine::Fjall => {
            let store = FjallStore::new(meta_root, None, None);
            MetaStore::new(store, None)
        }
        StorageEngine::FjallNotx => {
            let store = FjallStoreNotx::new(meta_root, None);
            MetaStore::new(store, None)
        }
    };

    let bucket_keys = meta_store.num_keys(bucket_name)?;
    Ok(bucket_keys)
}

pub fn disk_space(meta_root: PathBuf, storage_engine: StorageEngine) -> u64 {
    let meta_store = match storage_engine {
        StorageEngine::Fjall => {
            let store = FjallStore::new(meta_root, None, None);
            MetaStore::new(store, None)
        }
        StorageEngine::FjallNotx => {
            let store = FjallStoreNotx::new(meta_root, None);
            MetaStore::new(store, None)
        }
    };

    meta_store.disk_space()
}
