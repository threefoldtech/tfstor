use std::convert::TryFrom;
use std::ops::Deref;
use std::path::PathBuf;
use std::sync::Arc;

use fjall;

use crate::metastore::{
    AllBucketsTree, BaseMetaTree, Block, BlockID, BlockTree, BucketMeta, BucketTreeExt, Durability,
    MetaError, MetaStore, Object, Transaction, BLOCKID_SIZE,
};

#[derive(Clone)]
pub struct FjallStore {
    keyspace: Arc<fjall::TxKeyspace>,
    bucket_partition: Arc<fjall::TxPartitionHandle>,
    block_partition: Arc<fjall::TxPartitionHandle>,
    path_partition: Arc<fjall::TxPartitionHandle>,
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
        tracing::info!("Opening fjall store at {:?}", path);
        const BUCKET_META_PARTITION: &str = "_BUCKETS";
        const BLOCK_PARTITION: &str = "_BLOCKS";
        const PATH_PARTITION: &str = "_PATHS";

        let tx_keyspace = fjall::Config::new(path).open_transactional().unwrap();
        let bucket_partition = tx_keyspace
            .open_partition(BUCKET_META_PARTITION, Default::default())
            .unwrap();
        let block_partition = tx_keyspace
            .open_partition(BLOCK_PARTITION, Default::default())
            .unwrap();
        let path_partition = tx_keyspace
            .open_partition(PATH_PARTITION, Default::default())
            .unwrap();
        let inlined_metadata_size = inlined_metadata_size.unwrap_or(DEFAULT_INLINED_METADATA_SIZE);

        let durability = durability.unwrap_or(Durability::Fdatasync);
        let durability = match durability {
            Durability::Buffer => fjall::PersistMode::Buffer,
            Durability::Fsync => fjall::PersistMode::SyncData,
            Durability::Fdatasync => fjall::PersistMode::SyncAll,
        };

        Self {
            keyspace: Arc::new(tx_keyspace),
            bucket_partition: Arc::new(bucket_partition),
            block_partition: Arc::new(block_partition),
            path_partition: Arc::new(path_partition),
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
}

impl MetaStore for FjallStore {
    fn max_inlined_data_length(&self) -> usize {
        if self.inlined_metadata_size < Object::minimum_inline_metadata_size() {
            return 0;
        }
        self.inlined_metadata_size - Object::minimum_inline_metadata_size()
    }

    fn get_bucket_ext(
        &self,
        name: &str,
    ) -> Result<Box<dyn BucketTreeExt + Send + Sync>, MetaError> {
        let bucket = self.get_partition(name)?;
        Ok(Box::new(FjallTree::new(
            self.keyspace.clone(),
            Arc::new(bucket),
        )))
    }

    fn get_allbuckets_tree(&self) -> Result<Box<dyn AllBucketsTree>, MetaError> {
        Ok(Box::new(FjallTree::new(
            self.keyspace.clone(),
            self.bucket_partition.clone(),
        )))
    }

    fn get_block_tree(&self) -> Result<Box<dyn BlockTree>, MetaError> {
        Ok(Box::new(FjallTree::new(
            self.keyspace.clone(),
            self.block_partition.clone(),
        )))
    }

    fn get_tree(&self, name: &str) -> Result<Box<dyn BaseMetaTree>, MetaError> {
        let partition = self.get_partition(name)?;
        Ok(Box::new(FjallTree::new(
            self.keyspace.clone(),
            Arc::new(partition),
        )))
    }

    fn get_path_tree(&self) -> Result<Box<dyn BaseMetaTree>, MetaError> {
        Ok(Box::new(FjallTree::new(
            self.keyspace.clone(),
            self.path_partition.clone(),
        )))
    }

    fn drop_bucket(&self, name: &str) -> Result<(), MetaError> {
        let partition = self.get_partition(name)?;
        match self.keyspace.delete_partition(partition) {
            Ok(_) => Ok(()),
            Err(e) => Err(MetaError::OtherDBError(e.to_string())),
        }
    }

    fn insert_bucket(&self, bucket_name: &str, raw_bucket: Vec<u8>) -> Result<(), MetaError> {
        let mut tx = self.keyspace.write_tx();
        tx.insert(&self.bucket_partition, bucket_name, raw_bucket);

        self.commit_persist(tx)?;

        match self.get_partition(bucket_name) {
            // get partition to create it
            Ok(_) => Ok(()),
            Err(e) => Err(e),
        }
    }

    fn bucket_exists(&self, bucket_name: &str) -> Result<bool, MetaError> {
        let exists = self.keyspace.partition_exists(bucket_name);
        Ok(exists)
    }

    /// Get a list of all buckets in the system.
    fn list_buckets(&self) -> Result<Vec<BucketMeta>, MetaError> {
        let read_tx = self.keyspace.read_tx();
        let buckets = read_tx
            .range::<Vec<u8>, _>(&self.bucket_partition, std::ops::RangeFull) // Specify type parameter for range
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

    fn insert_meta(&self, bucket_name: &str, key: &str, raw_obj: Vec<u8>) -> Result<(), MetaError> {
        let bucket = self.get_partition(bucket_name)?;
        let mut tx = self.keyspace.write_tx();
        tx.insert(&bucket, key, raw_obj);
        self.commit_persist(tx)
    }

    fn get_meta(&self, bucket_name: &str, key: &str) -> Result<Option<Object>, MetaError> {
        let bucket = self.get_partition(bucket_name)?;
        let read_tx = self.keyspace.read_tx();
        let raw_object = match read_tx.get(&bucket, key) {
            Ok(Some(o)) => o,
            Ok(None) => return Ok(None),
            Err(e) => return Err(MetaError::OtherDBError(e.to_string())),
        };

        let obj = Object::try_from(&*raw_object).expect("Malformed object");
        Ok(Some(obj))
    }

    fn delete_object(&self, bucket: &str, key: &str) -> Result<Vec<Block>, MetaError> {
        let bucket = self.get_partition(bucket)?;

        let raw_object = match bucket.get(key) {
            Ok(Some(o)) => o,
            Ok(None) => return Ok(vec![]),
            Err(e) => return Err(MetaError::OtherDBError(e.to_string())),
        };

        let obj = Object::try_from(&*raw_object).expect("Malformed object");
        let mut to_delete: Vec<Block> = Vec::with_capacity(obj.blocks().len());

        let mut tx = self.keyspace.write_tx();
        // delete the object in the database, we have it in memory to remove the
        // blocks as needed.
        tx.remove(&bucket, key);

        for block_id in obj.blocks() {
            match tx.get(&self.block_partition, block_id) {
                Err(e) => return Err(MetaError::OtherDBError(e.to_string())),
                Ok(None) => continue,
                Ok(Some(block_data)) => {
                    let mut block = Block::try_from(&*block_data).expect("corrupt block data");
                    // We are deleting the last reference to the block, delete the
                    // whole block.
                    // Importantly, we don't remove the path yet from the path map.
                    // Leaving this path dangling in the database ensures it is not
                    // filled in by another block, before we properly delete the
                    // path from disk.
                    if block.rc() == 1 {
                        tx.remove(&self.block_partition, block_id);
                        to_delete.push(block);
                    } else {
                        block.decrement_refcount();
                        tx.insert(&self.block_partition, block_id, block.to_vec());
                    }
                }
            }
        }
        self.commit_persist(tx)?;
        Ok(to_delete)
    }

    fn begin_transaction(&self) -> Box<dyn Transaction> {
        // Use unsafe to extend lifetime to 'static since the transaction
        // won't outlive the store
        let tx = unsafe {
            std::mem::transmute::<fjall::WriteTransaction<'_>, fjall::WriteTransaction<'static>>(
                self.keyspace.write_tx(),
            )
        };

        Box::new(FjallTransaction::new(tx, Arc::new(self.clone())))
    }
}

pub struct FjallTransaction {
    tx: Option<fjall::WriteTransaction<'static>>,
    store: Arc<FjallStore>,
}

impl FjallTransaction {
    pub fn new(tx: fjall::WriteTransaction<'static>, store: Arc<FjallStore>) -> Self {
        Self {
            tx: Some(tx),
            store,
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
        let blocks = self.store.block_partition.clone();
        let paths = self.store.path_partition.clone();

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
}

impl BlockTree for FjallTree {
    fn get_block(&self, key: &[u8]) -> Result<Option<Block>, MetaError> {
        let block_data = match self.get(key) {
            Ok(Some(b)) => b,
            Ok(None) => return Ok(None),
            Err(e) => return Err(MetaError::OtherDBError(e.to_string())),
        };

        let block = match Block::try_from(&*block_data) {
            Ok(b) => b,
            Err(e) => return Err(MetaError::OtherDBError(e.to_string())),
        };
        Ok(Some(block))
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
        fn insert_bucket(&self, bucket_name: &str, raw_bucket: Vec<u8>) -> Result<(), MetaError> {
            <FjallStore as MetaStore>::insert_bucket(self, bucket_name, raw_bucket)
        }

        fn bucket_exists(&self, bucket_name: &str) -> Result<bool, MetaError> {
            <FjallStore as MetaStore>::bucket_exists(self, bucket_name)
        }

        fn list_buckets(&self) -> Result<Vec<BucketMeta>, MetaError> {
            <FjallStore as MetaStore>::list_buckets(self)
        }

        fn insert_meta(
            &self,
            bucket_name: &str,
            key: &str,
            raw_obj: Vec<u8>,
        ) -> Result<(), MetaError> {
            <FjallStore as MetaStore>::insert_meta(self, bucket_name, key, raw_obj)
        }

        fn get_meta(&self, bucket_name: &str, key: &str) -> Result<Option<Object>, MetaError> {
            <FjallStore as MetaStore>::get_meta(self, bucket_name, key)
        }

        fn get_bucket_ext(
            &self,
            name: &str,
        ) -> Result<Box<dyn BucketTreeExt + Send + Sync>, MetaError> {
            <FjallStore as MetaStore>::get_bucket_ext(self, name)
        }
    }

    #[test]
    fn test_errors() {
        let (store, _dir) = setup_store();
        test_utils::test_errors(&store);
    }

    #[test]
    fn test_bucket_operations() {
        let (store, _dir) = setup_store();
        test_utils::test_bucket_operations(&store);
    }

    #[test]
    fn test_object_operations() {
        let (store, _dir) = setup_store();
        test_utils::test_object_operations(&store);
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
