use std::any::Any;
use std::convert::TryFrom;
use std::ops::Deref;

use faster_hex::hex_string;
use sled;
use sled::Transactional;

use super::{
    block::{Block, BlockID, BLOCKID_SIZE},
    bucket_meta::BucketMeta,
    meta_errors::MetaError,
    meta_store::{BaseMetaTree, MetaStore, MetaTree, MetaTreeExt},
    multipart::MultiPart,
    object::Object,
};

impl From<sled::Error> for MetaError {
    fn from(error: sled::Error) -> Self {
        MetaError::UnknownError(error.to_string())
    }
}

const BUCKET_META_TREE: &str = "_BUCKETS";
const BLOCK_TREE: &str = "_BLOCKS";

#[derive(Debug)]
pub struct SledStore {
    db: sled::Db,
}

impl SledStore {
    pub fn new(db: sled::Db) -> Self {
        Self { db }
    }

    fn get_tree(&self, name: &str) -> Result<sled::Tree, MetaError> {
        match self.db.open_tree(name) {
            Ok(tree) => Ok(tree),
            Err(e) => Err(MetaError::UnknownError(e.to_string())),
        }
    }
}

impl MetaStore for SledStore {
    fn get_base_tree(&self, name: &str) -> Result<Box<dyn BaseMetaTree>, MetaError> {
        match self.db.open_tree(name) {
            Ok(tree) => Ok(Box::new(SledTree::new(tree))),
            Err(e) => Err(MetaError::UnknownError(e.to_string())),
        }
    }

    fn get_tree(&self, name: &str) -> Result<Box<dyn MetaTree + Send + Sync>, MetaError> {
        let tree = self.get_tree(name)?;
        Ok(Box::new(SledTree::new(tree)))
    }

    /// drop_tree drops the tree with the given name.
    fn drop_tree(&self, name: &str) -> Result<(), MetaError> {
        match self.db.drop_tree(name) {
            Ok(_) => Ok(()),
            Err(e) => Err(MetaError::UnknownError(e.to_string())),
        }
    }

    fn insert_bucket(&self, bucket_name: String, bm: BucketMeta) -> Result<(), MetaError> {
        let raw_bm = bm.to_vec();

        let bucket_meta = self.get_tree(BUCKET_META_TREE)?;
        match bucket_meta.insert(bucket_name, raw_bm) {
            Ok(_) => Ok(()),
            Err(e) => Err(MetaError::UnknownError(e.to_string())),
        }
    }

    fn bucket_exists(&self, bucket_name: &str) -> Result<bool, MetaError> {
        let tree = self.get_tree(BUCKET_META_TREE)?;
        match tree.contains_key(bucket_name) {
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
        let bucket = self.get_tree(bucket_name)?;

        match bucket.insert(key.as_bytes(), obj_meta.to_vec()) {
            Ok(_) => Ok(obj_meta),
            Err(e) => Err(MetaError::UnknownError(e.to_string())),
        }
    }

    fn get_meta_obj(&self, bucket: &str, key: &str) -> Result<Object, MetaError> {
        let bucket = self.get_tree(bucket)?;
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
        let bucket_tree = match self.get_tree(BUCKET_META_TREE) {
            Ok(t) => t,
            Err(e) => return Err(MetaError::UnknownError(e.to_string())),
        };
        let buckets = bucket_tree
            .scan_prefix([])
            .values()
            .filter_map(|raw_value| {
                let value = match raw_value {
                    Err(_) => return None,
                    Ok(v) => v,
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
        let block_map = self.get_tree(BLOCK_TREE)?;
        let bucket = self.get_tree(bucket)?;
        let blocks_to_delete_res: Result<Vec<Block>, sled::transaction::TransactionError> =
            (&bucket, &block_map).transaction(|(bucket, blocks)| {
                match bucket.get(key)? {
                    None => Ok(vec![]),
                    Some(o) => {
                        // get the objects
                        let obj = Object::try_from(&*o).expect("Malformed object");
                        let mut to_delete = Vec::with_capacity(obj.blocks().len());
                        // delete the object in the database, we have it in memory to remove the
                        // blocks as needed.
                        bucket.remove(key)?;

                        for block_id in obj.blocks() {
                            match blocks.get(block_id)? {
                                // This is technically impossible
                                None => {
                                    eprintln!("missing block {} in block map", hex_string(block_id))
                                }
                                Some(block_data) => {
                                    let mut block =
                                        Block::try_from(&*block_data).expect("corrupt block data");
                                    // We are deleting the last reference to the block, delete the
                                    // whole block.
                                    // Importantly, we don't remove the path yet from the path map.
                                    // Leaving this path dangling in the database ensures it is not
                                    // filled in by another block, before we properly delete the
                                    // path from disk.
                                    if block.rc() == 1 {
                                        blocks.remove(block_id)?;
                                        to_delete.push(block);
                                    } else {
                                        block.decrement_refcount();
                                        blocks.insert(block_id, Vec::from(&block))?;
                                    }
                                }
                            }
                        }
                        Ok(to_delete)
                    }
                }
            });
        let blocks_to_delete = match blocks_to_delete_res {
            Err(sled::transaction::TransactionError::Storage(e)) => {
                return Err(MetaError::UnknownError(e.to_string()))
            }
            Ok(blocks) => blocks,
            // We don't abort manually so this can't happen
            Err(sled::transaction::TransactionError::Abort(_)) => unreachable!(),
        };
        Ok(blocks_to_delete)
    }

    fn write_meta_for_block(
        &self,
        block_map: Box<dyn BaseMetaTree>,
        path_map: Box<dyn BaseMetaTree>,
        block_hash: BlockID,
        data_len: usize,
    ) -> Result<bool, MetaError> {
        let block_map = convert_to_sled_tree(&block_map)
            .ok_or_else(|| MetaError::NotMetaTree("block_map is not a sled tree".to_string()))?;
        let path_map = convert_to_sled_tree(&path_map)
            .ok_or_else(|| MetaError::NotMetaTree("path_map is not a sled tree".to_string()))?;

        let block_map = &block_map.tree;
        let path_map = &path_map.tree;

        // Check if the hash is present in the block map. If it is not, try to find a path, and
        // insert it.
        let should_write: Result<bool, sled::transaction::TransactionError> = (block_map, path_map)
            .transaction(|(blocks, paths)| {
                match blocks.get(block_hash)? {
                    Some(block_data) => {
                        // Block already exists
                        {
                            // bump refcount on the block
                            let mut block = Block::try_from(&*block_data)
                                .expect("Only valid blocks are stored");
                            block.increment_refcount();
                            // write block back
                            // TODO: this could be done in an `update_and_fetch`
                            blocks.insert(&block_hash, Vec::from(&block))?;
                        }

                        Ok(false)
                    }
                    None => {
                        // find a free path
                        for index in 1..BLOCKID_SIZE {
                            if paths.get(&block_hash[..index])?.is_some() {
                                // path already used, try the next one
                                continue;
                            };

                            // path is free, insert
                            paths.insert(&block_hash[..index], &block_hash)?;

                            let block = Block::new(data_len, block_hash[..index].to_vec());

                            blocks.insert(&block_hash, Vec::from(&block))?;
                            return Ok(true);
                        }

                        // The loop above can only NOT find a path in case it is duplicate
                        // block, wich already breaks out at the start.
                        unreachable!();
                    }
                }
            });
        match should_write {
            Ok(b) => Ok(b),
            Err(sled::transaction::TransactionError::Storage(e)) => {
                Err(MetaError::TransactionError(e.to_string()))
            }
            Err(sled::transaction::TransactionError::Abort(_)) => unreachable!(),
        }
    }
}

pub struct SledTree {
    tree: sled::Tree,
}

impl SledTree {
    pub fn new(tree: sled::Tree) -> SledTree {
        SledTree { tree }
    }

    fn get(&self, key: &[u8]) -> Result<Option<sled::IVec>, MetaError> {
        match self.tree.get(key) {
            Ok(Some(v)) => Ok(Some(v)),
            Ok(None) => Ok(None),
            Err(e) => Err(MetaError::UnknownError(e.to_string())),
        }
    }
}

impl BaseMetaTree for SledTree {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn insert(&self, key: &[u8], value: Vec<u8>) -> Result<(), MetaError> {
        match self.tree.insert(key, value) {
            Ok(_) => Ok(()),
            Err(e) => Err(MetaError::UnknownError(e.to_string())),
        }
    }

    fn remove(&self, key: &[u8]) -> Result<(), MetaError> {
        match self.tree.remove(key) {
            Ok(_) => Ok(()),
            Err(e) => Err(MetaError::UnknownError(e.to_string())),
        }
    }

    fn get_block_obj(&self, key: &[u8]) -> Result<Block, MetaError> {
        let block_data = self.get(key)?.ok_or(MetaError::KeyNotFound)?;
        let block = match Block::try_from(&*block_data) {
            Ok(b) => b,
            Err(e) => return Err(MetaError::UnknownError(e.to_string())),
        };
        Ok(block)
    }

    fn get_multipart_part_obj(&self, key: &[u8]) -> Result<MultiPart, MetaError> {
        let part_data = self.get(key)?.ok_or(MetaError::KeyNotFound)?;
        // unwrap here is safe as it is a coding error
        let mp = MultiPart::try_from(&*part_data).expect("Corrupted multipart data");
        Ok(mp)
    }
}

impl MetaTreeExt for SledTree {
    fn get_bucket_keys(&self) -> Box<dyn Iterator<Item = Result<Vec<u8>, MetaError>> + Send> {
        Box::new(self.tree.iter().keys().map(|key_result| {
            key_result
                .map(|ivec| ivec.to_vec())
                .map_err(|e| MetaError::UnknownError(e.to_string()))
        }))
    }

    fn range_filter_skip<'a>(
        &'a self,
        start_bytes: &'a [u8],
        prefix_bytes: &'a [u8],
        start_after: Option<String>,
    ) -> Box<(dyn Iterator<Item = (String, Object)> + 'a)> {
        Box::new(
            self.tree
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
            self.tree
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

impl MetaTree for SledTree {} // Empty impl as marker

fn convert_to_sled_tree(tree: &dyn BaseMetaTree) -> Option<&SledTree> {
    tree.as_any().downcast_ref::<SledTree>()
}
