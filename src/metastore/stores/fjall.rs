use std::convert::TryFrom;
use std::ops::Deref;
use std::path::PathBuf;
use std::sync::Arc;

use fjall;

use crate::metastore::{
    BaseMetaTree, Block, BlockID, BucketMeta, BucketTreeExt, Durability, MetaError, Object, Store,
    Transaction, BLOCKID_SIZE,
};

#[derive(Clone)]
pub struct FjallStore {
    keyspace: Arc<fjall::TxKeyspace>,
    inlined_metadata_size: usize,
    durability: fjall::PersistMode,
}

impl std::fmt::Debug for FjallStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FjallStore")
            .field("keyspace", &"<fjall::Keyspace>")
            .finish()
    }
}

const DEFAULT_INLINED_METADATA_SIZE: usize = 1; // setting very low will practically disable it by default

impl FjallStore {
    pub fn new(
        path: PathBuf,
        inlined_metadata_size: Option<usize>,
        durability: Option<Durability>,
    ) -> Self {
        tracing::debug!("Opening fjall store at {:?}", path);

        let tx_keyspace = fjall::Config::new(path).open_transactional().unwrap();
        let inlined_metadata_size = inlined_metadata_size.unwrap_or(DEFAULT_INLINED_METADATA_SIZE);

        let durability = durability.unwrap_or(Durability::Fdatasync);
        let durability = match durability {
            Durability::Buffer => fjall::PersistMode::Buffer,
            Durability::Fsync => fjall::PersistMode::SyncData,
            Durability::Fdatasync => fjall::PersistMode::SyncAll,
        };

        Self {
            keyspace: Arc::new(tx_keyspace),
            inlined_metadata_size,
            durability,
        }
    }

    fn get_partition(&self, name: &str) -> Result<fjall::TxPartitionHandle, MetaError> {
        match self.keyspace.open_partition(name, Default::default()) {
            Ok(partition) => Ok(partition),
            Err(e) => Err(MetaError::OtherDBError(e.to_string())),
        }
    }

    fn commit_persist(&self, tx: fjall::WriteTransaction) -> Result<(), MetaError> {
        tx.commit()
            .map_err(|e| MetaError::TransactionError(e.to_string()))?;

        self.keyspace
            .persist(self.durability)
            .map_err(|e| MetaError::PersistError(e.to_string()))?;
        Ok(())
    }

    pub fn get_inlined_metadata_size(&self) -> usize {
        self.inlined_metadata_size
    }
}

impl Store for FjallStore {
    fn tree_open(&self, name: &str) -> Result<Box<dyn BaseMetaTree>, MetaError> {
        let partition = self.get_partition(name)?;
        Ok(Box::new(FjallTree::new(
            self.keyspace.clone(),
            Arc::new(partition),
        )))
    }

    fn tree_ext_open(&self, name: &str) -> Result<Box<dyn BucketTreeExt + Send + Sync>, MetaError> {
        let partition = self.get_partition(name)?;
        Ok(Box::new(FjallTree::new(
            self.keyspace.clone(),
            Arc::new(partition),
        )))
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

    fn begin_transaction(
        &self,
        _block_tree_name: &str,
        _path_tree_name: &str,
    ) -> Box<dyn Transaction> {
        // Use unsafe to extend lifetime to 'static since the transaction
        // won't outlive the store
        let tx = unsafe {
            std::mem::transmute::<fjall::WriteTransaction<'_>, fjall::WriteTransaction<'static>>(
                self.keyspace.write_tx(),
            )
        };
        let block_partition = self.get_partition(_block_tree_name).unwrap();
        let path_partition = self.get_partition(_path_tree_name).unwrap();

        Box::new(FjallTransaction::new(
            tx,
            Arc::new(self.clone()),
            block_partition,
            path_partition,
        ))
    }

    fn num_keys(&self) -> (usize, usize, usize) {
        unimplemented!(); // fjall with transaction does not support this
    }

    fn disk_space(&self) -> u64 {
        self.keyspace.disk_space()
    }

    fn list_buckets(&self, bucket_partition_name: &str) -> Result<Vec<BucketMeta>, MetaError> {
        let bucket_partition = self.get_partition(bucket_partition_name)?;
        let read_tx = self.keyspace.read_tx();
        let buckets = read_tx
            .range::<Vec<u8>, _>(&bucket_partition, std::ops::RangeFull) // Specify type parameter for range
            .filter_map(|raw_value| {
                let value = match raw_value {
                    Err(_) => return None,
                    Ok((_, value)) => value,
                };
                // unwrap here is fine as it means the db is corrupt
                let bucket_meta = BucketMeta::try_from(&*value).expect("Corrupted bucket metadata");
                Some(bucket_meta)
            })
            .collect();
        Ok(buckets)
    }
}

pub struct FjallTransaction {
    tx: Option<fjall::WriteTransaction<'static>>,
    store: Arc<FjallStore>,
    block_partition: fjall::TransactionalPartitionHandle,
    path_partition: fjall::TransactionalPartitionHandle,
}

impl FjallTransaction {
    pub fn new(
        tx: fjall::WriteTransaction<'static>,
        store: Arc<FjallStore>,
        block_partition: fjall::TransactionalPartitionHandle,
        path_partition: fjall::TransactionalPartitionHandle,
    ) -> Self {
        Self {
            tx: Some(tx),
            store,
            block_partition,
            path_partition,
        }
    }
}

unsafe impl Send for FjallTransaction {}
unsafe impl Sync for FjallTransaction {}

impl Transaction for FjallTransaction {
    fn commit(mut self: Box<Self>) -> Result<(), MetaError> {
        if let Some(tx) = self.tx.take() {
            self.store.commit_persist(tx)
        } else {
            Err(MetaError::TransactionError(
                "Transaction already rolled back".to_string(),
            ))
        }
    }

    fn rollback(mut self: Box<Self>) {
        if let Some(tx) = self.tx.take() {
            tx.rollback();
        }
    }

    fn write_block(
        &mut self,
        block_hash: BlockID,
        data_len: usize,
        key_has_block: bool,
    ) -> Result<(bool, Block), MetaError> {
        let blocks = self.block_partition.clone();
        let paths = self.path_partition.clone();

        let tx = self.tx.as_mut().ok_or_else(|| {
            MetaError::TransactionError("Transaction already rolled back".to_string())
        })?;

        match tx.get(&blocks, block_hash) {
            Ok(Some(block_data)) => {
                let mut block =
                    Block::try_from(&*block_data).expect("Only valid blocks are stored");

                if !key_has_block {
                    block.increment_refcount();
                    tx.insert(&blocks, block_hash, block.to_vec());
                }
                Ok((false, block))
            }
            Ok(None) => {
                let mut idx = 0;
                for index in 1..BLOCKID_SIZE {
                    match tx.get(&paths, &block_hash[..index]) {
                        Ok(Some(_)) => continue,
                        Ok(None) => {
                            idx = index;
                            break;
                        }
                        Err(e) => return Err(MetaError::OtherDBError(e.to_string())),
                    }
                }

                tx.insert(&paths, &block_hash[..idx], block_hash);
                let block = Block::new(data_len, block_hash[..idx].to_vec());
                tx.insert(&blocks, block_hash, block.to_vec());
                Ok((true, block))
            }
            Err(e) => Err(MetaError::OtherDBError(e.to_string())),
        }
    }
}

pub struct FjallTree {
    keyspace: Arc<fjall::TxKeyspace>,
    partition: Arc<fjall::TxPartitionHandle>,
}

impl FjallTree {
    pub fn new(keyspace: Arc<fjall::TxKeyspace>, partition: Arc<fjall::TxPartitionHandle>) -> Self {
        Self {
            keyspace,
            partition,
        }
    }

    fn get(&self, key: &[u8]) -> Result<Option<fjall::Slice>, MetaError> {
        match self.partition.get(key) {
            Ok(Some(v)) => Ok(Some(v)),
            Ok(None) => Ok(None),
            Err(e) => Err(MetaError::OtherDBError(e.to_string())),
        }
    }
}

impl BaseMetaTree for FjallTree {
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

    #[cfg(test)]
    fn len(&self) -> Result<usize, MetaError> {
        let read_tx = self.keyspace.read_tx();
        let len = read_tx
            .len(&self.partition)
            .map_err(|e| MetaError::OtherDBError(e.to_string()))?;
        Ok(len)
    }
}

impl BucketTreeExt for FjallTree {
    fn get_bucket_keys(&self) -> Box<dyn Iterator<Item = Result<Vec<u8>, MetaError>> + Send> {
        let partition = self.partition.clone();
        let keyspace = self.keyspace.clone();
        let mut last_key: Option<Vec<u8>> = None;

        Box::new(std::iter::from_fn(move || {
            let read_tx = keyspace.read_tx();
            let range = match &last_key {
                Some(k) => {
                    let mut next = k.clone();
                    next.push(0);
                    next..
                }
                None => Vec::new()..,
            };

            read_tx
                .range::<Vec<u8>, _>(&partition, range)
                .next()
                .map(|res| match res {
                    Ok((k, _)) => {
                        last_key = Some(k.to_vec());
                        Ok(k.to_vec())
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
        let mut ctsa = match (continuation_token, start_after) {
            (Some(token), Some(start)) => Some(std::cmp::max(token, start)),
            (Some(token), None) => Some(token),
            (None, start) => start,
        };

        let read_tx = self.keyspace.read_tx();

        let base_iter: Box<
            dyn Iterator<Item = Result<(fjall::Slice, fjall::Slice), fjall::Error>>,
        > = match (prefix.as_ref(), ctsa.as_ref()) {
            (Some(prefix), Some(ctsa)) if (ctsa > prefix && !ctsa.starts_with(prefix)) => {
                //Return empty iterator if ctsa is after prefix
                Box::new(std::iter::empty())
            }
            (Some(prefix), Some(ctsa_local)) if ctsa_local < prefix => {
                // If ctsa is before prefix, ignore ctsa
                ctsa = None;
                Box::new(read_tx.prefix(&self.partition, prefix.as_bytes()))
            }
            (Some(prefix), _) => Box::new(read_tx.prefix(&self.partition, prefix.as_bytes())),
            (None, Some(ctsa)) => {
                let mut next_key = ctsa.as_bytes().to_vec();
                next_key.push(0);
                Box::new(read_tx.range(&self.partition, next_key..))
            }
            (None, None) => Box::new(read_tx.range::<Vec<u8>, _>(&self.partition, ..)),
        };

        let filtered = base_iter.filter_map(|res| res.ok());

        let skip_filtered = if prefix.is_some() && ctsa.is_some() {
            let ctsa_bytes = ctsa.unwrap().into_bytes();
            Box::new(
                filtered.skip_while(move |(raw_key, _)| raw_key.deref() <= ctsa_bytes.as_slice()),
            ) as Box<dyn Iterator<Item = _>>
        } else {
            Box::new(filtered)
        };

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
    use crate::metastore::stores::test_utils;
    use tempfile::tempdir;

    fn setup_store() -> (FjallStore, tempfile::TempDir) {
        let dir = tempdir().unwrap();
        let store = FjallStore::new(dir.path().to_path_buf(), Some(1), None);
        (store, dir)
    }

    impl test_utils::TestStore for FjallStore {
        fn tree_open(&self, name: &str) -> Result<Box<dyn BaseMetaTree>, MetaError> {
            <FjallStore as Store>::tree_open(self, name)
        }

        fn get_bucket_ext(
            &self,
            name: &str,
        ) -> Result<Box<dyn BucketTreeExt + Send + Sync>, MetaError> {
            <FjallStore as Store>::tree_ext_open(self, name)
        }
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
