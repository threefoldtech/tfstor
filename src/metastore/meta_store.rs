use std::convert::TryFrom;
use std::fmt::Debug;
use std::sync::Arc;

use super::{
    BaseMetaTree, Block, BlockID, BucketMeta, BucketTreeExt, MetaError, Object, Store, BLOCKID_SIZE,
};

/// MetaStore is a struct that provides methods to interact with the metadata store.
///
/// It uses a Store implementation to handle the low-level storage operations.
/// The MetaStore provides higher-level operations for buckets, blocks, paths, and objects.
#[derive(Clone)]
pub struct MetaStore {
    store: Arc<dyn Store>,
    inlined_metadata_size: usize,
}
const DEFAULT_BUCKET_TREE: &str = "_BUCKETS";
const DEFAULT_BLOCK_TREE: &str = "_BLOCKS";
const DEFAULT_PATH_TREE: &str = "_PATHS";

impl MetaStore {
    pub fn new(store: impl Store + 'static, inlined_metadata_size: Option<usize>) -> Self {
        const DEFAULT_INLINED_METADATA_SIZE: usize = 1; // setting very low will practically disable it by default

        Self {
            store: Arc::new(store),
            inlined_metadata_size: inlined_metadata_size.unwrap_or(DEFAULT_INLINED_METADATA_SIZE),
        }
    }

    // returns the maximum length of the data that can be inlined in the metadata object
    pub fn max_inlined_data_length(&self) -> usize {
        if self.inlined_metadata_size < Object::minimum_inline_metadata_size() {
            return 0;
        }
        self.inlined_metadata_size - Object::minimum_inline_metadata_size()
    }

    /// returns tree which contains all the buckets.
    /// This tree is used to store the bucket lists and provide
    /// the CRUD for the bucket list.
    pub fn get_allbuckets_tree(&self) -> Result<Box<dyn BucketTreeExt + Send + Sync>, MetaError> {
        self.store.tree_ext_open(DEFAULT_BUCKET_TREE)
    }

    /// get_bucket_ext returns the tree for specific bucket with the extended methods
    /// we use this tree to provide additional methods for the bucket like the range and list methods.
    pub fn get_bucket_ext(
        &self,
        name: &str,
    ) -> Result<Box<dyn BucketTreeExt + Send + Sync>, MetaError> {
        self.store.tree_ext_open(name)
    }

    /// get_block_tree returns the block meta tree.
    /// This tree is used to store the data block metadata.
    pub fn get_block_tree(&self) -> Result<BlockTree, MetaError> {
        let tree = self.store.tree_open(DEFAULT_BLOCK_TREE)?;
        Ok(BlockTree { tree })
    }

    /// get_tree returns the tree with the given name.
    /// It is usually used if the app need to store some metadata for a specific purpose.
    pub fn get_tree(&self, name: &str) -> Result<Box<dyn BaseMetaTree>, MetaError> {
        self.store.tree_open(name)
    }

    /// get_path_tree returns the path meta tree
    /// This tree is used to store the file path metadata.
    pub fn get_path_tree(&self) -> Result<Box<dyn BaseMetaTree>, MetaError> {
        self.store.tree_open(DEFAULT_PATH_TREE)
    }

    /// bucket_exists returns true if the bucket exists.
    pub fn bucket_exists(&self, bucket_name: &str) -> Result<bool, MetaError> {
        self.store.tree_exists(bucket_name)
    }

    /// drop_bucket drops the bucket with the given name.
    pub fn drop_bucket(&self, name: &str) -> Result<(), MetaError> {
        if self.bucket_exists(name)? {
            self.store.tree_delete(name)
        } else {
            Ok(())
        }
    }

    /// insert_bucket inserts raw representation of the bucket into the meta store.
    pub fn insert_bucket(&self, bucket_name: &str, raw_bucket: Vec<u8>) -> Result<(), MetaError> {
        // Insert the bucket metadata into the buckets tree
        let buckets = self.store.tree_open(DEFAULT_BUCKET_TREE)?;
        buckets.insert(bucket_name.as_bytes(), raw_bucket)?;

        // Create the bucket tree if it doesn't exist
        self.store.tree_open(bucket_name)?;

        Ok(())
    }

    /// Get a list of all buckets in the system.
    /// TODO: this should be paginated and return a stream.
    pub fn list_buckets(&self) -> Result<Vec<BucketMeta>, MetaError> {
        /*let bucket_tree = self.get_allbuckets_tree()?;
        let buckets = bucket_tree
            .get_bucket_keys()
            .filter_map(|result| {
                let key = match result {
                    Ok(k) => k,
                    Err(_) => return None,
                };

                let value = match self.store.get(std::str::from_utf8(&key).unwrap()) {
                    Ok(Some(v)) => v,
                    _ => return None,
                };

                let bucket_meta = BucketMeta::try_from(&*value).ok()?;
                Some(bucket_meta)
            })
            .collect();
        Ok(buckets)*/
        self.store.list_buckets(DEFAULT_BUCKET_TREE)
    }

    /// insert_meta inserts a metadata Object into the bucket
    pub fn insert_meta(
        &self,
        bucket_name: &str,
        key: &str,
        raw_obj: Vec<u8>,
    ) -> Result<(), MetaError> {
        let bucket = self.get_bucket_ext(bucket_name)?;
        bucket.insert(key.as_bytes(), raw_obj)
    }

    /// get_meta returns the Object metadata for the given bucket and key.
    /// We return the Object struct instead of the raw bytes for performance reason.
    pub fn get_meta(&self, bucket_name: &str, key: &str) -> Result<Option<Object>, MetaError> {
        let bucket = self.get_bucket_ext(bucket_name)?;
        match bucket.get(key.as_bytes())? {
            Some(data) => {
                let obj = Object::try_from(&*data).expect("Malformed object");
                Ok(Some(obj))
            }
            None => Ok(None),
        }
    }

    /// delete object in a bucket for the given key.
    ///
    /// It should do at least the following:
    /// - get all the blocks from the object
    /// - decrements the refcount of all blocks, then removes blocks which are no longer referenced.
    /// - and return the deleted blocks, so that the caller can remove the blocks from the storage.
    ///
    /// TODO: all the above steps shouldn't be done in the meta storage layer.
    ///       we do it there because we still couldn't abstract the DB transaction.
    pub fn delete_object(&self, bucket: &str, key: &str) -> Result<Vec<Block>, MetaError> {
        let bucket_tree = self.get_bucket_ext(bucket)?;
        let block_tree = self.get_block_tree()?;

        // Get the object metadata
        let raw_object = match bucket_tree.get(key.as_bytes())? {
            Some(o) => o,
            None => return Ok(vec![]),
        };

        let obj = Object::try_from(&*raw_object).expect("Malformed object");
        let mut to_delete: Vec<Block> = Vec::with_capacity(obj.blocks().len());

        // Delete the object from the bucket
        bucket_tree.remove(key.as_bytes())?;

        // Process all blocks in the object
        for block_id in obj.blocks() {
            match block_tree.get(block_id)? {
                Some(block_data) => {
                    let mut block = Block::try_from(&*block_data).expect("Corrupted block data");

                    // If this is the last reference to the block, delete it
                    if block.rc() == 1 {
                        block_tree.remove(block_id)?;
                        to_delete.push(block);
                    } else {
                        // Otherwise decrement the reference count
                        block.decrement_refcount();
                        block_tree.insert(block_id, block.to_vec())?;
                    }
                }
                None => continue, // Block not found, skip it
            }
        }

        Ok(to_delete)
    }

    pub fn begin_transaction(&self) -> Transaction {
        self.store.begin_transaction()
    }

    // returns the number of keys of the bucket, block, and path trees.
    pub fn num_keys(&self) -> (usize, usize, usize) {
        self.store.num_keys()
    }

    // returns the disk space used by the metadata store.
    pub fn disk_space(&self) -> u64 {
        self.store.disk_space()
    }
}

impl Debug for MetaStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetaStore")
            .field("store", &"<Store>")
            .field("bucket_tree_name", &DEFAULT_BUCKET_TREE)
            .field("block_tree_name", &DEFAULT_BLOCK_TREE)
            .field("path_tree_name", &DEFAULT_PATH_TREE)
            .field("inlined_metadata_size", &self.inlined_metadata_size)
            .finish()
    }
}

pub struct BlockTree {
    tree: Box<dyn BaseMetaTree>,
}

impl BlockTree {
    /// get_block_obj returns the `Object` for the given key.
    pub fn get_block(&self, key: &[u8]) -> Result<Option<Block>, MetaError> {
        match self.tree.get(key)? {
            Some(data) => {
                let block = Block::try_from(&*data).expect("Malformed block");
                Ok(Some(block))
            }
            None => Ok(None),
        }
    }

    #[cfg(test)]
    pub fn len(&self) -> Result<usize, MetaError> {
        self.tree.len()
    }

    fn remove(&self, key: &[u8]) -> Result<(), MetaError> {
        self.tree.remove(key)
    }

    fn insert(&self, key: &[u8], value: Vec<u8>) -> Result<(), MetaError> {
        self.tree.insert(key, value)
    }

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, MetaError> {
        self.tree.get(key)
    }
}

/// Represents a database transaction that can be committed or rolled back.
/// It provides methods for writing blocks to the database.
pub struct Transaction {
    // The backend storage implementation
    backend: Box<dyn TransactionBackend>,
}

impl Transaction {
    pub(crate) fn new(backend: Box<dyn TransactionBackend>) -> Self {
        Self { backend }
    }

    pub fn commit(mut self) -> Result<(), MetaError> {
        self.backend.commit()
    }

    pub fn rollback(mut self) {
        // Finally call the backend's rollback method for any other cleanup
        self.backend.rollback();
    }

    /// Writes a block to the database.
    ///
    /// # Arguments
    /// * `block_hash` - The hash of the block to write
    /// * `data_len` - The length of the block data
    /// * `key_has_block` - Whether the key already has this block
    ///
    /// # Returns
    /// A tuple containing:
    /// * A boolean indicating whether the block was newly created
    /// * The Block object
    pub fn write_block(
        &mut self,
        block_hash: BlockID,
        data_len: usize,
        key_has_block: bool,
    ) -> Result<(bool, Block), MetaError> {
        // Check if the block already exists
        match self.backend.get(DEFAULT_BLOCK_TREE, &block_hash)? {
            // Block exists
            Some(block_data) => {
                let mut block = Block::try_from(&*block_data as &[u8])
                    .map_err(|e| MetaError::OtherDBError(e.to_string()))?;

                // If the key doesn't have this block, increment the reference count
                if !key_has_block {
                    block.increment_refcount();
                    self.backend
                        .insert(DEFAULT_BLOCK_TREE, &block_hash, block.to_vec())?;
                }

                Ok((false, block))
            }
            // Block doesn't exist, create it
            None => {
                let mut idx = 0;
                for index in 1..BLOCKID_SIZE {
                    match self.backend.get(DEFAULT_PATH_TREE, &block_hash[..index]) {
                        Ok(Some(_)) => continue,
                        Ok(None) => {
                            idx = index;
                            break;
                        }
                        Err(e) => return Err(MetaError::OtherDBError(e.to_string())),
                    }
                }

                // insert this new path
                self.backend
                    .insert(DEFAULT_PATH_TREE, &block_hash[..idx], block_hash.to_vec())?;

                // insert this new block
                let block = Block::new(data_len, block_hash[..idx].to_vec());

                self.backend
                    .insert(DEFAULT_BLOCK_TREE, &block_hash, block.to_vec())?;

                Ok((true, block))
            }
        }
    }
}

/// Abstracts the storage backend operations needed by Transaction
pub(crate) trait TransactionBackend: Send + Sync {
    /// Commit the transaction
    fn commit(&mut self) -> Result<(), MetaError>;

    /// Perform any backend-specific rollback operations
    fn rollback(&mut self);

    /// Get a block from the backend
    fn get(&mut self, tree_name: &str, key: &[u8]) -> Result<Option<Vec<u8>>, MetaError>;

    /// Insert a new block into the backend
    fn insert(&mut self, tree_name: &str, key: &[u8], data: Vec<u8>) -> Result<(), MetaError>;
}
