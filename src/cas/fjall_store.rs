use std::any::Any;
use std::convert::TryFrom;
use std::ops::Deref;
use std::path::PathBuf;
use std::sync::Arc;

use fjall;

use super::{
    block::{Block, BlockID, BLOCKID_SIZE},
    bucket_meta::BucketMeta,
    meta_errors::MetaError,
    meta_store::{BaseMetaTree, MetaStore, MetaTree, MetaTreeExt},
    multipart::MultiPart,
    object::Object,
};

pub struct FjallStore {
    keyspace: Arc<fjall::TxKeyspace>,
    bucket_partition: Arc<fjall::TxPartitionHandle>,
    block_partition: Arc<fjall::TxPartitionHandle>,
    path_partition: Arc<fjall::TxPartitionHandle>,
    multipart_partition: Arc<fjall::TxPartitionHandle>,
}

impl std::fmt::Debug for FjallStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FjallStore")
            .field("keyspace", &"<fjall::Keyspace>")
            .finish()
    }
}

impl FjallStore {
    pub fn new(path: PathBuf) -> Self {
        eprintln!("Opening fjall store at {:?}", path);
        const BUCKET_META_PARTITION: &str = "_BUCKETS";
        const BLOCK_PARTITION: &str = "_BLOCKS";
        const PATH_PARTITION: &str = "_PATHS";
        const MULTIPART_PARTITION: &str = "_MULTIPART_PARTS";

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
        let multipart_partition = tx_keyspace
            .open_partition(MULTIPART_PARTITION, Default::default())
            .unwrap();
        Self {
            keyspace: Arc::new(tx_keyspace),
            bucket_partition: Arc::new(bucket_partition),
            block_partition: Arc::new(block_partition),
            path_partition: Arc::new(path_partition),
            multipart_partition: Arc::new(multipart_partition),
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
            .persist(fjall::PersistMode::SyncAll)
            .map_err(|e| MetaError::PersistError(e.to_string()))?;
        Ok(())
    }

    #[allow(dead_code)]
    pub fn bucket_delete(&self, bucket_name: &str) -> Result<(), MetaError> {
        // this fn is only for testing purposes
        let bmt = self.get_allbuckets_tree()?;
        bmt.remove(bucket_name.as_bytes())?;

        let bucket = self.get_bucket_ext(bucket_name)?;
        for key in bucket.get_bucket_keys() {
            let key = key?;
            eprintln!("Deleting object {}", std::str::from_utf8(&key).unwrap());
        }

        self.drop_bucket(bucket_name)?;

        Ok(())
    }
}

impl MetaStore for FjallStore {
    fn get_bucket_ext(&self, name: &str) -> Result<Box<dyn MetaTree + Send + Sync>, MetaError> {
        let bucket = self.get_partition(name)?;
        Ok(Box::new(FjallTree::new(
            self.keyspace.clone(),
            Arc::new(bucket),
        )))
    }

    fn get_allbuckets_tree(&self) -> Result<Box<dyn BaseMetaTree>, MetaError> {
        Ok(Box::new(FjallTree::new(
            self.keyspace.clone(),
            self.bucket_partition.clone(),
        )))
    }

    fn get_block_tree(&self) -> Result<Box<dyn BaseMetaTree>, MetaError> {
        Ok(Box::new(FjallTree::new(
            self.keyspace.clone(),
            self.block_partition.clone(),
        )))
    }

    fn get_path_tree(&self) -> Result<Box<dyn BaseMetaTree>, MetaError> {
        Ok(Box::new(FjallTree::new(
            self.keyspace.clone(),
            self.path_partition.clone(),
        )))
    }

    fn get_multipart_tree(&self) -> Result<Box<dyn BaseMetaTree>, MetaError> {
        Ok(Box::new(FjallTree::new(
            self.keyspace.clone(),
            self.multipart_partition.clone(),
        )))
    }

    fn drop_bucket(&self, name: &str) -> Result<(), MetaError> {
        let partition = self.get_partition(name)?;
        match self.keyspace.delete_partition(partition) {
            Ok(_) => Ok(()),
            Err(e) => Err(MetaError::OtherDBError(e.to_string())),
        }
    }

    fn insert_bucket(&self, bucket_name: String, raw_bucket: Vec<u8>) -> Result<(), MetaError> {
        let mut tx = self.keyspace.write_tx();
        tx.insert(&self.bucket_partition, bucket_name, raw_bucket);

        self.commit_persist(tx)
    }

    fn bucket_exists(&self, bucket_name: &str) -> Result<bool, MetaError> {
        match self
            .keyspace
            .read_tx()
            .contains_key(&self.bucket_partition, bucket_name)
        {
            Ok(true) => Ok(true),
            Ok(false) => Ok(false),
            Err(e) => Err(MetaError::OtherDBError(e.to_string())),
        }
    }

    fn insert_meta_obj(
        &self,
        bucket_name: &str,
        key: &str,
        raw_obj: Vec<u8>,
    ) -> Result<(), MetaError> {
        let bucket = self.get_partition(bucket_name)?;
        let mut tx = self.keyspace.write_tx();
        tx.insert(&bucket, key.as_bytes(), raw_obj);
        self.commit_persist(tx)?;
        Ok(())
    }

    fn get_meta_obj(&self, bucket: &str, key: &str) -> Result<Object, MetaError> {
        let bucket = self.get_partition(bucket)?;
        let read_tx = self.keyspace.read_tx();
        let raw_object = match read_tx.get(&bucket, key) {
            Ok(Some(o)) => o,
            Ok(None) => return Err(MetaError::KeyNotFound),
            Err(e) => return Err(MetaError::OtherDBError(e.to_string())),
        };

        Ok(Object::try_from(&*raw_object).expect("Malformed object"))
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

    fn delete_objects(&self, bucket: &str, key: &str) -> Result<Vec<Block>, MetaError> {
        // Remove an object. This fetches the object, decrements the refcount of all blocks,
        // and removes blocks which are no longer referenced.
        let bucket = self.get_partition(bucket)?;

        // transaction
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

    fn write_block_and_path_meta(
        &self,
        _block_map: Box<dyn BaseMetaTree>,
        _path_map: Box<dyn BaseMetaTree>,
        block_hash: BlockID,
        data_len: usize,
    ) -> Result<bool, MetaError> {
        let blocks = self.block_partition.clone();
        let paths = self.path_partition.clone();

        let mut tx = self.keyspace.write_tx();
        let should_write = match tx.get(&blocks, block_hash) {
            Ok(Some(block_data)) => {
                // Block already exists

                // bump refcount on the block
                let mut block =
                    Block::try_from(&*block_data).expect("Only valid blocks are stored");
                block.increment_refcount();
                // write block back
                // TODO: this could be done in an `update_and_fetch`
                tx.insert(&blocks, block_hash, block.to_vec());

                Ok(false)
            }
            Ok(None) => {
                let mut idx = 0;
                // find a free path
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
                // The loop above can only NOT find a path in case it is duplicate
                // block, wich already breaks out at the start.

                // path is free, insert
                tx.insert(&paths, &block_hash[..idx], block_hash);

                let block = Block::new(data_len, block_hash[..idx].to_vec());

                tx.insert(&blocks, block_hash, block.to_vec());
                Ok(true)
            }
            Err(e) => Err(MetaError::OtherDBError(e.to_string())),
        };
        match should_write {
            Ok(should_write) => {
                if should_write {
                    self.commit_persist(tx)?;
                }
                Ok(should_write)
            }
            Err(e) => Err(e),
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

    fn get(&self, key: &[u8]) -> Result<fjall::Slice, MetaError> {
        match self.partition.get(key) {
            Ok(Some(v)) => Ok(v),
            Ok(None) => Err(MetaError::KeyNotFound),
            Err(e) => Err(MetaError::OtherDBError(e.to_string())),
        }
    }
}

impl BaseMetaTree for FjallTree {
    fn as_any(&self) -> &dyn Any {
        self
    }

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

    fn get_block_obj(&self, key: &[u8]) -> Result<Block, MetaError> {
        let block_data = self.get(key)?;

        let block = match Block::try_from(&*block_data) {
            Ok(b) => b,
            Err(e) => return Err(MetaError::OtherDBError(e.to_string())),
        };
        Ok(block)
    }

    fn get_multipart_part_obj(&self, key: &[u8]) -> Result<MultiPart, MetaError> {
        let part_data = self.get(key)?;
        // unwrap here is safe as it is a coding error
        let mp = MultiPart::try_from(&*part_data).expect("Corrupted multipart data");
        Ok(mp)
    }
}

#[derive(Clone)]
struct KeyIterator {
    keyspace: Arc<fjall::TxKeyspace>,
    partition: Arc<fjall::TxPartitionHandle>,
}

impl KeyIterator {
    fn new(keyspace: Arc<fjall::TxKeyspace>, partition: Arc<fjall::TxPartitionHandle>) -> Self {
        Self {
            keyspace,
            partition,
        }
    }
}

impl Iterator for KeyIterator {
    type Item = Result<Vec<u8>, MetaError>;

    fn next(&mut self) -> Option<Self::Item> {
        let read_tx = self.keyspace.read_tx();
        read_tx
            .range::<Vec<u8>, _>(&self.partition, std::ops::RangeFull)
            .next()
            .map(|res| {
                res.map(|(k, _)| k.to_vec())
                    .map_err(|e| MetaError::OtherDBError(e.to_string()))
            })
    }
}

impl MetaTreeExt for FjallTree {
    fn get_bucket_keys(&self) -> Box<dyn Iterator<Item = Result<Vec<u8>, MetaError>> + Send> {
        Box::new(KeyIterator::new(
            self.keyspace.clone(),
            self.partition.clone(),
        ))
    }

    fn range_filter_skip<'a>(
        &'a self,
        start_bytes: &'a [u8],
        prefix_bytes: &'a [u8],
        start_after: Option<String>,
    ) -> Box<(dyn Iterator<Item = (String, Object)> + 'a)> {
        let read_tx = self.keyspace.read_tx();
        Box::new(
            read_tx
                .range(&self.partition, start_bytes..)
                .filter_map(|read_result| match read_result {
                    Err(_) => None,
                    Ok((k, v)) => Some((k, v)),
                })
                .skip_while(move |(raw_key, _)| match start_after {
                    None => false,
                    Some(ref start_after) => raw_key.deref() <= start_after.as_bytes(),
                })
                .take_while(move |(raw_key, _)| raw_key.starts_with(prefix_bytes))
                .map(|(raw_key, raw_value)| {
                    let key = unsafe { String::from_utf8_unchecked(raw_key.to_vec()) };
                    let obj = Object::try_from(&*raw_value).unwrap();
                    (key, obj)
                }),
        )
    }
    fn range_filter<'a>(
        &'a self,
        start_bytes: &'a [u8],
        prefix_bytes: &'a [u8],
    ) -> Box<(dyn Iterator<Item = (String, Object)> + 'a)> {
        let read_tx = self.keyspace.read_tx();
        Box::new(
            read_tx
                .range(&self.partition, start_bytes..)
                .filter_map(|read_result| match read_result {
                    Err(_) => None,
                    Ok((k, v)) => Some((k, v)),
                })
                .take_while(move |(raw_key, _)| raw_key.starts_with(prefix_bytes))
                .map(|(raw_key, raw_value)| {
                    let key = unsafe { String::from_utf8_unchecked(raw_key.to_vec()) };
                    let obj = Object::try_from(&*raw_value).unwrap();
                    (key, obj)
                }),
        )
    }
}

impl MetaTree for FjallTree {} // Empty impl as marker

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn setup_store() -> (FjallStore, tempfile::TempDir) {
        let dir = tempdir().unwrap();
        let store = FjallStore::new(dir.path().to_path_buf());
        (store, dir)
    }

    #[test]
    fn test_bucket_operations() {
        let (store, _dir) = setup_store();

        // Test bucket creation
        let bucket_name = "test-bucket";
        let bucket_meta = BucketMeta::new(bucket_name.to_string());
        store
            .insert_bucket(bucket_name.to_string(), bucket_meta.to_vec())
            .unwrap();

        // Verify bucket exists
        assert_eq!(store.bucket_exists(bucket_name).unwrap(), true);

        // Test bucket listing
        let buckets = store.list_buckets().unwrap();
        assert_eq!(buckets.len(), 1);
        assert_eq!(buckets[0].name(), bucket_name);

        // Test bucket deletion
        store.bucket_delete(bucket_name).unwrap();

        // Verify bucket not exists
        assert_eq!(store.bucket_exists(bucket_name).unwrap(), false);
    }

    #[test]
    fn test_object_operations() {
        let (store, _dir) = setup_store();
        let bucket_name = "test-bucket";
        let key = "test-bucket/key";

        // Setup bucket first
        let bucket_meta = BucketMeta::new(bucket_name.to_string());
        store
            .insert_bucket(bucket_name.to_string(), bucket_meta.to_vec())
            .unwrap();

        // Test object insertion
        let test_obj = Object::new(
            1024,                   // 1KB object
            BlockID::from([1; 16]), // Sample ETag
            0,
            vec![BlockID::from([1; 16])], // Sample block ID
        );
        store
            .insert_meta_obj(bucket_name, key, test_obj.to_vec())
            .unwrap();

        // Test object retrieval
        let retrieved_obj = store.get_meta_obj(bucket_name, key).unwrap();
        assert_eq!(retrieved_obj.blocks().len(), 1);
        assert_eq!(retrieved_obj.blocks()[0], BlockID::from([1; 16]));

        // Test error cases
        assert!(store.get_meta_obj("nonexistent-bucket", key).is_err());
        assert!(store.get_meta_obj(bucket_name, "nonexistent-key").is_err());
    }

    #[test]
    fn test_errors() {
        let (store, _dir) = setup_store();

        // Test nonexistent bucket
        assert!(store.get_meta_obj("nonexistent", "key").is_err());

        // Test nonexistent object
        let bucket = "test-bucket";
        let bucket_meta = BucketMeta::new(bucket.to_string());
        store
            .insert_bucket(bucket.to_string(), bucket_meta.to_vec())
            .unwrap();
        assert!(store.get_meta_obj(bucket, "nonexistent").is_err());
    }
}
