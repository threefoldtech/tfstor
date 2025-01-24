#![allow(dead_code)]

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

const BUCKET_META_PARTITION: &str = "_BUCKETS";
const BLOCK_PARTITION: &str = "_BLOCKS";
const PATH_PARTITION: &str = "_PATHS";
const MULTIPART_PARTITION: &str = "_MULTIPART_PARTS";

pub struct FjallStore {
    keyspace: fjall::TxKeyspace,
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
            keyspace: tx_keyspace,
            bucket_partition: Arc::new(bucket_partition),
            block_partition: Arc::new(block_partition),
            path_partition: Arc::new(path_partition),
            multipart_partition: Arc::new(multipart_partition),
        }
    }

    fn get_base_tree(&self, name: &str) -> Result<Box<dyn BaseMetaTree>, MetaError> {
        match self.keyspace.open_partition(name, Default::default()) {
            Ok(partition) => Ok(Box::new(FjallTree::new(
                self.keyspace.clone(),
                Arc::new(partition),
            ))),
            Err(e) => Err(MetaError::OtherDBError(e.to_string())),
        }
    }

    fn get_partition(&self, name: &str) -> Result<fjall::TxPartitionHandle, MetaError> {
        match self.keyspace.open_partition(name, Default::default()) {
            Ok(partition) => Ok(partition),
            Err(e) => Err(MetaError::OtherDBError(e.to_string())),
        }
    }

    fn persist(&self) -> Result<(), MetaError> {
        match self.keyspace.persist(fjall::PersistMode::SyncAll) {
            Ok(_) => Ok(()),
            Err(e) => Err(MetaError::PersistError(e.to_string())),
        }
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

    fn get_bucket_tree(&self) -> Result<Box<dyn BaseMetaTree>, MetaError> {
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

    /// drop_tree drops the tree with the given name.
    fn drop_tree(&self, name: &str) -> Result<(), MetaError> {
        let partition = self.get_partition(name)?;
        match self.keyspace.delete_partition(partition) {
            Ok(_) => Ok(()),
            Err(e) => Err(MetaError::OtherDBError(e.to_string())),
        }
    }

    fn insert_bucket(&self, bucket_name: String, raw_bucket: Vec<u8>) -> Result<(), MetaError> {
        let mut tx = self.keyspace.write_tx();
        tx.insert(&self.bucket_partition, bucket_name, raw_bucket);
        tx.commit()
            .map_err(|e| MetaError::TransactionError(e.to_string()))?;
        self.persist()?;
        Ok(())
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
        bucket
            .insert(key.as_bytes(), raw_obj)
            .map_err(|e| MetaError::OtherDBError(e.to_string()))?;
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
        //let mut tx = self.keyspace.write_tx();
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
                        tx.insert(&self.block_partition, block_id, Vec::from(&block));
                    }
                }
            }
        }
        tx.commit()
            .map_err(|e| MetaError::TransactionError(e.to_string()))?;
        self.persist()?;
        Ok(to_delete)
    }

    fn write_meta_for_block(
        &self,
        block_map: Box<dyn BaseMetaTree>,
        path_map: Box<dyn BaseMetaTree>,
        block_hash: BlockID,
        data_len: usize,
    ) -> Result<bool, MetaError> {
        let block_map = convert_to_fjall_tree(&block_map)
            .ok_or_else(|| MetaError::NotMetaTree("block_map is not a fjall tree".to_string()))?;
        let path_map = convert_to_fjall_tree(&path_map)
            .ok_or_else(|| MetaError::NotMetaTree("path_map is not a fjall tree".to_string()))?;

        let blocks = &block_map.partition;
        let paths = &path_map.partition;

        let mut tx = self.keyspace.write_tx();
        let should_write = match tx.get(blocks, block_hash) {
            Ok(Some(block_data)) => {
                // Block already exists
                {
                    // bump refcount on the block
                    let mut block =
                        Block::try_from(&*block_data).expect("Only valid blocks are stored");
                    block.increment_refcount();
                    // write block back
                    // TODO: this could be done in an `update_and_fetch`
                    tx.insert(blocks, block_hash, Vec::from(&block));
                }

                Ok(false)
            }
            Ok(None) => {
                let mut idx = 0;
                // find a free path
                for index in 1..BLOCKID_SIZE {
                    match tx.get(paths, &block_hash[..index]) {
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
                tx.insert(paths, &block_hash[..idx], block_hash);

                let block = Block::new(data_len, block_hash[..idx].to_vec());

                tx.insert(blocks, block_hash, Vec::from(&block));
                Ok(true)
            }
            Err(e) => Err(MetaError::OtherDBError(e.to_string())),
        };
        match should_write {
            Ok(should_write) => {
                if should_write {
                    tx.commit()
                        .map_err(|e| MetaError::TransactionError(e.to_string()))?;
                    self.persist()?;
                }
                Ok(should_write)
            }
            Err(e) => Err(e),
        }
    }
}

pub struct FjallTree {
    keyspace: fjall::TxKeyspace,
    partition: Arc<fjall::TxPartitionHandle>,
}

impl FjallTree {
    pub fn new(keyspace: fjall::TxKeyspace, partition: Arc<fjall::TxPartitionHandle>) -> Self {
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
    keyspace: fjall::TxKeyspace,
    partition: Arc<fjall::TxPartitionHandle>,
}

impl KeyIterator {
    fn new(keyspace: fjall::TxKeyspace, partition: Arc<fjall::TxPartitionHandle>) -> Self {
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

fn convert_to_fjall_tree(tree: &dyn BaseMetaTree) -> Option<&FjallTree> {
    tree.as_any().downcast_ref::<FjallTree>()
}
