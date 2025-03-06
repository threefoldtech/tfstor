use super::{
    block::{Block, BlockID},
    bucket_meta::BucketMeta,
    object::Object,
    MetaError,
};

use std::fmt::Debug;
use std::str::FromStr;

/// MetaStore is the interface that defines the methods to interact with the metadata store.
///
/// Current implementation of the bucket, block, path, and multipart trees are the same,
/// the difference is only the partition/tree name.
/// But we separate the API to make it easier to extend in the future,
/// and give flexibility to the implementer to have different implementations for each tree.
pub trait MetaStore: Send + Sync + Debug + 'static {
    // returns the maximum length of the data that can be inlined in the metadata object
    fn max_inlined_data_length(&self) -> usize;

    /// returns tree which contains all the buckets.
    /// This tree is used to store the bucket lists and provide
    /// the CRUD for the bucket list.
    fn get_allbuckets_tree(&self) -> Result<Box<dyn AllBucketsTree>, MetaError>;

    /// get_bucket_ext returns the tree for specific bucket with the extended methods
    /// we use this tree to provide additional methods for the bucket like the range and list methods.
    fn get_bucket_ext(&self, name: &str)
        -> Result<Box<dyn BucketTreeExt + Send + Sync>, MetaError>;

    /// get_block_tree returns the block meta tree.
    /// This tree is used to store the data block metadata.
    fn get_block_tree(&self) -> Result<Box<dyn BlockTree>, MetaError>;

    /// get_tree returns the tree with the given name.
    /// It is usually used if the app need to store some metadata for a specific purpose.
    fn get_tree(&self, name: &str) -> Result<Box<dyn BaseMetaTree>, MetaError>;

    /// get_path_tree returns the path meta tree
    /// This tree is used to store the file path metadata.
    fn get_path_tree(&self) -> Result<Box<dyn BaseMetaTree>, MetaError>;

    /// bucket_exists returns true if the bucket exists.
    fn bucket_exists(&self, bucket_name: &str) -> Result<bool, MetaError>;

    /// drop_bucket drops the bucket with the given name.
    fn drop_bucket(&self, name: &str) -> Result<(), MetaError>;

    /// insert_bucket inserts raw representation of the bucket into the meta store.
    fn insert_bucket(&self, bucket_name: &str, raw_bucket: Vec<u8>) -> Result<(), MetaError>;

    /// Get a list of all buckets in the system.
    /// TODO: this should be paginated and return a stream.
    fn list_buckets(&self) -> Result<Vec<BucketMeta>, MetaError>;

    /// insert_meta inserts a metadata Object into the bucket
    fn insert_meta(&self, bucket_name: &str, key: &str, raw_obj: Vec<u8>) -> Result<(), MetaError>;

    /// get_meta returns the Object metadata for the given bucket and key.
    /// We return the Object struct instead of the raw bytes for performance reason.
    fn get_meta(&self, bucket_name: &str, key: &str) -> Result<Option<Object>, MetaError>;

    /// delete object in a bucket for the given key.
    ///
    /// It should do at least the following:
    /// - get all the blocks from the object
    /// - decrements the refcount of all blocks, then removes blocks which are no longer referenced.
    /// - and return the deleted blocks, so that the caller can remove the blocks from the storage.
    ///
    /// TODO: all the above steps shouldn't be done in the meta storage layer.
    ///       we do it there because we still couldn't abstract the DB transaction.
    fn delete_object(&self, bucket: &str, key: &str) -> Result<Vec<Block>, MetaError>;

    fn begin_transaction(&self) -> Box<dyn Transaction>;
}

pub trait Transaction: Send + Sync {
    fn commit(self: Box<Self>) -> Result<(), MetaError>;
    fn rollback(self: Box<Self>);
    fn write_block(
        &mut self,
        block_hash: BlockID,
        data_len: usize,
        key_has_block: bool,
    ) -> Result<(bool, Block), MetaError>;
}

pub trait BaseMetaTree: Send + Sync {
    /// insert inserts a key value pair into the tree.
    fn insert(&self, key: &[u8], value: Vec<u8>) -> Result<(), MetaError>;

    /// remove removes a key from the tree.
    fn remove(&self, key: &[u8]) -> Result<(), MetaError>;

    fn contains_key(&self, key: &[u8]) -> Result<bool, MetaError>;

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, MetaError>;
}

pub trait AllBucketsTree: BaseMetaTree {}

impl<T: BaseMetaTree> AllBucketsTree for T {}

pub trait BlockTree: Send + Sync {
    /// get_block_obj returns the `Object` for the given key.
    fn get_block(&self, key: &[u8]) -> Result<Option<Block>, MetaError>;
}

pub trait BucketTreeExt: BaseMetaTree {
    // get all keys of the bucket
    // TODO : make it paginated
    fn get_bucket_keys(&self) -> Box<dyn Iterator<Item = Result<Vec<u8>, MetaError>> + Send>;

    fn range_filter<'a>(
        &'a self,
        start_after: Option<String>,
        prefix: Option<String>,
        continuation_token: Option<String>,
    ) -> Box<(dyn Iterator<Item = (String, Object)> + 'a)>;
}

#[derive(Debug, Clone, Copy)]
pub enum Durability {
    Buffer,
    Fsync,
    Fdatasync,
}

impl FromStr for Durability {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "buffer" => Ok(Durability::Buffer),
            "fsync" => Ok(Durability::Fsync),
            "fdatasync" => Ok(Durability::Fdatasync),
            _ => Err(format!("Unknown durability option: {}", s)),
        }
    }
}
