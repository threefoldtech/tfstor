use std::convert::TryFrom;
use std::ops::Deref;
use std::path::PathBuf;
use std::sync::Arc;

use crate::{
    BaseMetaTree, KeyValuePairs, MetaError, MetaTreeExt, Object, Store, Transaction,
    TransactionBackend,
};

#[derive(Clone)]
pub struct FjallStoreNotx {
    keyspace: Arc<fjall::Keyspace>,
    inlined_metadata_size: usize,
}

impl std::fmt::Debug for FjallStoreNotx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FjallStoreNotx")
            .field("keyspace", &"<fjall::Keyspace>")
            .finish()
    }
}

impl FjallStoreNotx {
    pub fn new(path: PathBuf, inlined_metadata_size: Option<usize>) -> Self {
        tracing::debug!("Opening fjall store at {:?}", path);

        let keyspace = fjall::Config::new(path).open().unwrap();
        // setting very low will practically disable it by default
        let inlined_metadata_size = inlined_metadata_size.unwrap_or(1);

        Self {
            keyspace: Arc::new(keyspace),
            inlined_metadata_size,
        }
    }

    fn get_partition(&self, name: &str) -> Result<fjall::PartitionHandle, MetaError> {
        match self.keyspace.open_partition(name, Default::default()) {
            Ok(partition) => Ok(partition),
            Err(e) => Err(MetaError::OtherDBError(e.to_string())),
        }
    }

    pub fn get_inlined_metadata_size(&self) -> usize {
        self.inlined_metadata_size
    }
}

impl Store for FjallStoreNotx {
    fn tree_open(&self, name: &str) -> Result<Box<dyn BaseMetaTree>, MetaError> {
        let partition = self.get_partition(name)?;
        Ok(Box::new(FjallTreeNotx::new(Arc::new(partition))))
    }

    fn tree_ext_open(&self, name: &str) -> Result<Box<dyn MetaTreeExt + Send + Sync>, MetaError> {
        let partition = self.get_partition(name)?;
        Ok(Box::new(FjallTreeNotx::new(Arc::new(partition))))
    }

    fn tree_exists(&self, name: &str) -> Result<bool, MetaError> {
        let exists = self.keyspace.partition_exists(name);
        Ok(exists)
    }

    fn tree_delete(&self, name: &str) -> Result<(), MetaError> {
        let partition = self.get_partition(name)?;
        match self.keyspace.delete_partition(partition) {
            Ok(_) => Ok(()),
            Err(e) => Err(MetaError::OtherDBError(e.to_string())),
        }
    }

    fn begin_transaction(&self) -> Transaction {
        Transaction::new(Box::new(FjallNoTransaction::new(Arc::new(self.clone()))))
    }

    fn disk_space(&self) -> u64 {
        self.keyspace.disk_space()
    }
}

pub struct FjallNoTransaction {
    store: Arc<FjallStoreNotx>,

    inserted_keys: Vec<(String, Vec<u8>)>, // tupple of tree name and key
}

impl FjallNoTransaction {
    pub fn new(store: Arc<FjallStoreNotx>) -> Self {
        Self {
            store,
            inserted_keys: Vec::new(),
        }
    }
}

unsafe impl Send for FjallNoTransaction {}
unsafe impl Sync for FjallNoTransaction {}

impl TransactionBackend for FjallNoTransaction {
    fn commit(&mut self) -> Result<(), MetaError> {
        Ok(())
    }

    fn rollback(&mut self) {
        for (tree_name, key) in &self.inserted_keys {
            let partition = self.store.get_partition(tree_name).unwrap();
            let _ = partition.remove(key);
        }
    }

    fn get(&mut self, tree_name: &str, key: &[u8]) -> Result<Option<Vec<u8>>, MetaError> {
        let partition = self.store.get_partition(tree_name)?;
        match partition.get(key) {
            Ok(Some(data)) => Ok(Some(data.to_vec())),
            Ok(None) => Ok(None),
            Err(e) => Err(MetaError::OtherDBError(e.to_string())),
        }
    }

    fn insert(&mut self, tree_name: &str, key: &[u8], data: Vec<u8>) -> Result<(), MetaError> {
        let partition = self.store.get_partition(tree_name)?;
        match partition.insert(key, data) {
            Ok(_) => {
                self.inserted_keys
                    .push((tree_name.to_string(), key.to_vec()));
                Ok(())
            }
            Err(e) => Err(MetaError::InsertError(e.to_string())),
        }
    }
}

pub struct FjallTreeNotx {
    partition: Arc<fjall::PartitionHandle>,
}

impl FjallTreeNotx {
    pub fn new(partition: Arc<fjall::PartitionHandle>) -> Self {
        Self { partition }
    }

    fn get(&self, key: &[u8]) -> Result<Option<fjall::Slice>, MetaError> {
        match self.partition.get(key) {
            Ok(Some(v)) => Ok(Some(v)),
            Ok(None) => Ok(None),
            Err(e) => Err(MetaError::OtherDBError(e.to_string())),
        }
    }
}

impl BaseMetaTree for FjallTreeNotx {
    fn insert(&self, key: &[u8], value: Vec<u8>) -> Result<(), MetaError> {
        match self.partition.insert(key, value) {
            Ok(_) => Ok(()),
            Err(e) => Err(MetaError::OtherDBError(e.to_string())),
        }
    }

    fn remove(&self, key: &[u8]) -> Result<(), MetaError> {
        match self.partition.remove(key) {
            Ok(_) => Ok(()),
            Err(e) => Err(MetaError::OtherDBError(e.to_string())),
        }
    }

    fn contains_key(&self, key: &[u8]) -> Result<bool, MetaError> {
        match self.partition.contains_key(key) {
            Ok(v) => Ok(v),
            Err(_) => Err(MetaError::KeyNotFound),
        }
    }

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, MetaError> {
        match self.get(key) {
            Ok(Some(v)) => Ok(Some(v.to_vec())),
            Ok(None) => Ok(None),
            Err(e) => Err(e),
        }
    }

    fn len(&self) -> usize {
        self.partition.approximate_len()
    }

    fn is_empty(&self) -> Result<bool, MetaError> {
        self.partition
            .is_empty()
            .map_err(|e| MetaError::OtherDBError(e.to_string()))
    }
}

impl MetaTreeExt for FjallTreeNotx {
    fn iter_kv(&self, start_after: Option<Vec<u8>>) -> KeyValuePairs {
        let partition = self.partition.clone();
        let mut last_key = start_after;

        Box::new(std::iter::from_fn(move || {
            let range = match &last_key {
                Some(k) => {
                    let mut next = k.clone();
                    next.push(0);
                    next..
                }
                None => Vec::new()..,
            };

            partition
                .range::<Vec<u8>, _>(range)
                .next()
                .map(|res| match res {
                    Ok((k, v)) => {
                        last_key = Some(k.to_vec());
                        Ok((k.to_vec(), v.to_vec()))
                    }
                    Err(e) => {
                        tracing::error!("Error reading key: {}", e);
                        Err(MetaError::OtherDBError(e.to_string()))
                    }
                })
        }))
    }

    // rules:
    // 1. continuation_token and start_after exists: use the one with the highest lexicographical order
    //    -> call it: ctsa
    // 2. if prefix exists
    //    -> ctsa > the prefix && doesn't have prefix: return zero results
    //    -> ctsa < prefix: ignore it
    //    -> ctsa has the prefix: use it as start_after
    //          In kv store like fjall & Sled: we process it in the Rust code
    fn range_filter<'a>(
        &'a self,
        start_after: Option<String>,
        prefix: Option<String>,
        continuation_token: Option<String>,
    ) -> Box<(dyn Iterator<Item = (String, Object)> + 'a)> {
        // we only use one of token or start_after
        // we can ignore the other
        let mut ctsa = match (continuation_token, start_after) {
            (Some(token), Some(start)) => Some(std::cmp::max(token, start)),
            (Some(token), None) => Some(token),
            (None, start) => start,
        };

        let partition = self.partition.clone();

        // create the iterator based on the existence of the prefix and ctsa
        let base_iter: Box<
            dyn Iterator<Item = Result<(fjall::Slice, fjall::Slice), fjall::Error>>,
        > = match (prefix.as_ref(), ctsa.as_ref()) {
            // if ctsa is after prefix and doesn't have prefix, return empty iterator
            (Some(prefix), Some(ctsa)) if (ctsa > prefix && !ctsa.starts_with(prefix)) => {
                //Return empty iterator if ctsa is after prefix
                Box::new(std::iter::empty())
            }

            // if ctsa is before prefix, ignore ctsa
            (Some(prefix), Some(ctsa_local)) if ctsa_local < prefix => {
                // If ctsa is before prefix, ignore ctsa
                ctsa = None;
                Box::new(partition.prefix(prefix.as_bytes()))
            }

            // if prefix exists, with or without ctsa, use `prefix`
            (Some(prefix), _) => Box::new(partition.prefix(prefix.as_bytes())),

            // if ctsa exists, without prefix, use `range` from ctsa
            (None, Some(ctsa)) => {
                let mut next_key = ctsa.as_bytes().to_vec();
                next_key.push(0);
                Box::new(partition.range(next_key..))
            }
            (None, None) => Box::new(partition.range::<Vec<u8>, _>(..)),
        };

        // filter out errors
        let filtered = base_iter.filter_map(|res| res.ok());

        //  if both prefix and ctsa exists, skip keys before ctsa
        let skip_filtered = if prefix.is_some() && ctsa.is_some() {
            let ctsa_bytes = ctsa.unwrap().into_bytes();
            Box::new(
                filtered.skip_while(move |(raw_key, _)| raw_key.deref() <= ctsa_bytes.as_slice()),
            ) as Box<dyn Iterator<Item = _>>
        } else {
            Box::new(filtered)
        };

        // convert raw keys and values to strings and objects
        Box::new(skip_filtered.map(|(raw_key, raw_value)| {
            let key = unsafe { String::from_utf8_unchecked(raw_key.to_vec()) };
            let obj = Object::try_from(&*raw_value).unwrap();
            (key, obj)
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stores::test_utils;
    use tempfile::tempdir;

    impl test_utils::TestStore for FjallStoreNotx {
        fn tree_open(&self, name: &str) -> Result<Box<dyn BaseMetaTree>, MetaError> {
            <FjallStoreNotx as Store>::tree_open(self, name)
        }

        fn get_bucket_ext(
            &self,
            name: &str,
        ) -> Result<Box<dyn MetaTreeExt + Send + Sync>, MetaError> {
            <FjallStoreNotx as Store>::tree_ext_open(self, name)
        }
    }

    fn setup_store() -> (FjallStoreNotx, tempfile::TempDir) {
        let dir = tempdir().unwrap();
        let store = FjallStoreNotx::new(dir.path().to_path_buf(), Some(1));
        (store, dir)
    }

    #[test]
    fn test_get_bucket_keys() {
        let (store, _dir) = setup_store();
        test_utils::test_get_bucket_keys(&store);
    }

    #[test]
    fn test_range_filter() {
        let (store, _dir) = setup_store();
        test_utils::test_range_filter(&store);
    }
}
