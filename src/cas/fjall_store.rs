#![allow(dead_code)]

use std::any::Any;
use std::convert::TryFrom;
use std::ops::Deref;

use std::path::PathBuf;

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

pub struct FjallStore {
    keyspace: fjall::Keyspace,
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
        let keyspace = fjall::Config::new(path).open().unwrap();
        Self { keyspace }
    }

    fn get_partition(&self, name: &str) -> Result<fjall::PartitionHandle, MetaError> {
        match self.keyspace.open_partition(name, Default::default()) {
            Ok(partition) => Ok(partition),
            Err(e) => Err(MetaError::UnknownError(e.to_string())),
        }
    }
}

impl MetaStore for FjallStore {
    fn get_base_tree(&self, name: &str) -> Result<Box<dyn BaseMetaTree>, MetaError> {
        match self.keyspace.open_partition(name, Default::default()) {
            Ok(partition) => Ok(Box::new(FjallTree::new(partition))),
            Err(e) => Err(MetaError::UnknownError(e.to_string())),
        }
    }

    fn get_tree(&self, name: &str) -> Result<Box<dyn MetaTree + Send + Sync>, MetaError> {
        match self.keyspace.open_partition(name, Default::default()) {
            Ok(partition) => Ok(Box::new(FjallTree::new(partition))),
            Err(e) => Err(MetaError::UnknownError(e.to_string())),
        }
    }

    /// drop_tree drops the tree with the given name.
    fn drop_tree(&self, name: &str) -> Result<(), MetaError> {
        let partition = self.get_partition(name)?;
        match self.keyspace.delete_partition(partition) {
            Ok(_) => Ok(()),
            Err(e) => Err(MetaError::UnknownError(e.to_string())),
        }
    }

    fn insert_bucket(&self, bucket_name: String, bm: BucketMeta) -> Result<(), MetaError> {
        let raw_bm = bm.to_vec();

        let bucket_meta = self.get_partition(BUCKET_META_PARTITION)?;
        match bucket_meta.insert(bucket_name, raw_bm) {
            Ok(_) => Ok(()),
            Err(e) => Err(MetaError::UnknownError(e.to_string())),
        }
    }

    fn bucket_exists(&self, bucket_name: &str) -> Result<bool, MetaError> {
        let partition = self.get_partition(BUCKET_META_PARTITION)?;
        match partition.contains_key(bucket_name) {
            Ok(true) => Ok(true),
            Ok(false) => Ok(false),
            Err(e) => Err(MetaError::UnknownError(e.to_string())),
        }
    }

    fn insert_meta_obj(
        &self,
        bucket_name: &str,
        key: &str,
        obj_meta: Object,
    ) -> Result<Object, MetaError> {
        let bucket = self.get_partition(bucket_name)?;

        match bucket.insert(key.as_bytes(), obj_meta.to_vec()) {
            Ok(_) => Ok(obj_meta),
            Err(e) => Err(MetaError::UnknownError(e.to_string())),
        }
    }

    fn get_meta_obj(&self, bucket: &str, key: &str) -> Result<Object, MetaError> {
        let bucket = self.get_partition(bucket)?;
        let object = match bucket.get(key) {
            Ok(o) => o,
            Err(e) => return Err(MetaError::UnknownError(e.to_string())),
        };
        match object {
            Some(o) => Ok(Object::try_from(&*o).expect("Malformed object")),
            None => Err(MetaError::KeyNotFound),
        }
    }

    /// Get a list of all buckets in the system.
    fn list_buckets(&self) -> Result<Vec<BucketMeta>, MetaError> {
        let bucket_tree = match self.get_partition(BUCKET_META_PARTITION) {
            Ok(t) => t,
            Err(e) => return Err(MetaError::UnknownError(e.to_string())),
        };
        let buckets = bucket_tree
            .range::<Vec<u8>, _>(std::ops::RangeFull) // Specify type parameter for range
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
        let blocks = self.get_partition(BLOCK_PARTITION)?;
        let bucket = self.get_partition(bucket)?;

        // transaction
        //let mut tx = self.keyspace.write_tx();
        let raw_object = match bucket.get(key) {
            Ok(Some(o)) => o,
            Ok(None) => return Ok(vec![]),
            Err(e) => return Err(MetaError::UnknownError(e.to_string())),
        };

        let obj = Object::try_from(&*raw_object).expect("Malformed object");
        let mut to_delete: Vec<Block> = Vec::with_capacity(obj.blocks().len());
        // delete the object in the database, we have it in memory to remove the
        // blocks as needed.
        bucket
            .remove(key)
            .map_err(|err| MetaError::UnknownError(err.to_string()))?;

        for block_id in obj.blocks() {
            match blocks.get(block_id) {
                Err(e) => return Err(MetaError::UnknownError(e.to_string())),
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
                        blocks
                            .remove(block_id)
                            .map_err(|e| MetaError::UnknownError(e.to_string()))?;
                        to_delete.push(block);
                    } else {
                        block.decrement_refcount();
                        match blocks.insert(block_id, Vec::from(&block)) {
                            Ok(_) => (),
                            Err(e) => return Err(MetaError::UnknownError(e.to_string())),
                        }
                    }
                }
            }
        }
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

        match blocks.get(block_hash) {
            Ok(Some(block_data)) => {
                // Block already exists
                {
                    // bump refcount on the block
                    let mut block =
                        Block::try_from(&*block_data).expect("Only valid blocks are stored");
                    block.increment_refcount();
                    // write block back
                    // TODO: this could be done in an `update_and_fetch`
                    match blocks.insert(block_hash, Vec::from(&block)) {
                        Ok(_) => (),
                        Err(e) => return Err(MetaError::UnknownError(e.to_string())),
                    }
                }

                Ok(false)
            }
            Ok(None) => {
                // find a free path
                for index in 1..BLOCKID_SIZE {
                    match paths.get(&block_hash[..index]) {
                        Ok(Some(_)) => continue,
                        Ok(None) => (),
                        Err(e) => return Err(MetaError::UnknownError(e.to_string())),
                    }

                    // path is free, insert
                    paths
                        .insert(&block_hash[..index], block_hash)
                        .map_err(|e| MetaError::UnknownError(e.to_string()))?;

                    let block = Block::new(data_len, block_hash[..index].to_vec());

                    blocks
                        .insert(block_hash, Vec::from(&block))
                        .map_err(|e| MetaError::UnknownError(e.to_string()))?;
                    return Ok(true);
                }

                // The loop above can only NOT find a path in case it is duplicate
                // block, wich already breaks out at the start.
                unreachable!();
            }
            Err(e) => Err(MetaError::UnknownError(e.to_string())),
        }
    }
}

pub struct FjallTree {
    partition: fjall::PartitionHandle,
}

impl FjallTree {
    pub fn new(partition: fjall::PartitionHandle) -> Self {
        Self { partition }
    }

    fn get(&self, key: &[u8]) -> Result<fjall::Slice, MetaError> {
        match self.partition.get(key) {
            Ok(Some(v)) => Ok(v),
            Ok(None) => Err(MetaError::KeyNotFound),
            Err(e) => Err(MetaError::UnknownError(e.to_string())),
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
            Err(e) => Err(MetaError::UnknownError(e.to_string())),
        }
    }

    fn remove(&self, key: &[u8]) -> Result<(), MetaError> {
        match self.partition.remove(key) {
            Ok(_) => Ok(()),
            Err(e) => Err(MetaError::UnknownError(e.to_string())),
        }
    }

    fn get_block_obj(&self, key: &[u8]) -> Result<Block, MetaError> {
        let block_data = self.get(key)?;

        let block = match Block::try_from(&*block_data) {
            Ok(b) => b,
            Err(e) => return Err(MetaError::UnknownError(e.to_string())),
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
    partition: fjall::PartitionHandle,
}

impl KeyIterator {
    fn new(partition: fjall::PartitionHandle) -> Self {
        Self { partition }
    }
}

impl Iterator for KeyIterator {
    type Item = Result<Vec<u8>, MetaError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.partition
            .range::<Vec<u8>, _>(std::ops::RangeFull)
            .next()
            .map(|res| {
                res.map(|(k, _)| k.to_vec())
                    .map_err(|e| MetaError::UnknownError(e.to_string()))
            })
    }
}

impl MetaTreeExt for FjallTree {
    fn get_bucket_keys(&self) -> Box<dyn Iterator<Item = Result<Vec<u8>, MetaError>> + Send> {
        Box::new(KeyIterator::new(self.partition.clone()))
    }

    fn range_filter_skip<'a>(
        &'a self,
        start_bytes: &'a [u8],
        prefix_bytes: &'a [u8],
        start_after: Option<String>,
    ) -> Box<(dyn Iterator<Item = (String, Object)> + 'a)> {
        Box::new(
            self.partition
                .range(start_bytes..)
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
        Box::new(
            self.partition
                .range(start_bytes..)
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
