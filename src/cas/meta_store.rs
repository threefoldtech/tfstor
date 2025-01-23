use std::any::Any;

use super::{
    block::{Block, BlockID},
    bucket_meta::BucketMeta,
    meta_errors::MetaError,
    multipart::MultiPart,
    object::Object,
};
use std::fmt::Debug;

pub trait MetaStore: Send + Sync + Debug + 'static {
    /// get_bucket_tree returns the bucket meta tree
    fn get_bucket_tree(&self) -> Result<Box<dyn BaseMetaTree>, MetaError>;

    /// get_bucket_ext returns the bucket with the extended methods.
    fn get_bucket_ext(&self, name: &str) -> Result<Box<dyn MetaTree + Send + Sync>, MetaError>;

    /// get_block_tree returns the block meta tree
    fn get_block_tree(&self) -> Result<Box<dyn BaseMetaTree>, MetaError>;

    /// get_path_tree returns the path meta tree
    fn get_path_tree(&self) -> Result<Box<dyn BaseMetaTree>, MetaError>;

    /// get_multipart_tree returns the multipart meta tree
    fn get_multipart_tree(&self) -> Result<Box<dyn BaseMetaTree>, MetaError>;

    /// bucket_exists returns true if the bucket exists.
    fn bucket_exists(&self, bucket_name: &str) -> Result<bool, MetaError>;

    /// drop_tree drops the tree with the given name.
    fn drop_tree(&self, name: &str) -> Result<(), MetaError>;

    /// insert_bucket inserts raw representation of the bucket into the meta store.
    fn insert_bucket(&self, bucket_name: String, raw_bucket: Vec<u8>) -> Result<(), MetaError>;

    /// insert_meta_obj inserts an object into the meta store.
    fn insert_meta_obj(
        &self,
        bucket_name: &str,
        key: &str,
        raw_obj: Vec<u8>,
    ) -> Result<(), MetaError>;

    /// get_meta_obj returns the object metadata for the given bucket and key.
    fn get_meta_obj(&self, bucket: &str, key: &str) -> Result<Object, MetaError>;

    /// Get a list of all buckets in the system.
    /// TODO: this should be paginated and return a stream.
    fn list_buckets(&self) -> Result<Vec<BucketMeta>, MetaError>;

    /// delete all objects in a bucket for the given key.
    /// it returns a list of blocks that were deleted.
    fn delete_objects(&self, bucket: &str, key: &str) -> Result<Vec<Block>, MetaError>;

    fn write_meta_for_block(
        &self,
        block_map: Box<dyn BaseMetaTree>,
        path_map: Box<dyn BaseMetaTree>,
        block_hash: BlockID,
        data_len: usize,
    ) -> Result<bool, MetaError>;
}

pub trait BaseMetaTree: Send + Sync {
    fn as_any(&self) -> &dyn Any;

    /// insert inserts a key value pair into the tree.
    fn insert(&self, key: &[u8], value: Vec<u8>) -> Result<(), MetaError>;

    /// remove removes a key from the tree.
    fn remove(&self, key: &[u8]) -> Result<(), MetaError>;

    /// get_block_obj returns the `Object` for the given key.
    fn get_block_obj(&self, key: &[u8]) -> Result<Block, MetaError>;

    /// get_multipart_part_obj returns the `MultiPart` for the given key.
    fn get_multipart_part_obj(&self, key: &[u8]) -> Result<MultiPart, MetaError>;
}

pub trait MetaTreeExt: BaseMetaTree {
    // get all keys of the bucket
    // TODO : make it paginated
    fn get_bucket_keys(&self) -> Box<dyn Iterator<Item = Result<Vec<u8>, MetaError>> + Send>;

    fn range_filter_skip<'a>(
        &'a self,
        start_bytes: &'a [u8],
        prefix_bytes: &'a [u8],
        start_after: Option<String>,
    ) -> Box<(dyn Iterator<Item = (String, Object)> + 'a)>;
    fn range_filter<'a>(
        &'a self,
        start_bytes: &'a [u8],
        prefix_bytes: &'a [u8],
    ) -> Box<(dyn Iterator<Item = (String, Object)> + 'a)>;
}

pub trait MetaTree: BaseMetaTree + MetaTreeExt {}

impl<T: ?Sized + BaseMetaTree> BaseMetaTree for Box<T> {
    fn as_any(&self) -> &dyn Any {
        (**self).as_any()
    }

    fn insert(&self, key: &[u8], value: Vec<u8>) -> Result<(), MetaError> {
        (**self).insert(key, value)
    }

    fn remove(&self, key: &[u8]) -> Result<(), MetaError> {
        (**self).remove(key)
    }
    fn get_block_obj(&self, key: &[u8]) -> Result<Block, MetaError> {
        (**self).get_block_obj(key)
    }

    fn get_multipart_part_obj(&self, key: &[u8]) -> Result<MultiPart, MetaError> {
        (**self).get_multipart_part_obj(key)
    }
}

use std::sync::Arc;
impl<T: ?Sized + BaseMetaTree> BaseMetaTree for Arc<T> {
    fn as_any(&self) -> &dyn Any {
        (**self).as_any()
    }

    fn insert(&self, key: &[u8], value: Vec<u8>) -> Result<(), MetaError> {
        (**self).insert(key, value)
    }

    fn remove(&self, key: &[u8]) -> Result<(), MetaError> {
        (**self).remove(key)
    }

    fn get_block_obj(&self, key: &[u8]) -> Result<Block, MetaError> {
        (**self).get_block_obj(key)
    }
    fn get_multipart_part_obj(&self, key: &[u8]) -> Result<MultiPart, MetaError> {
        (**self).get_multipart_part_obj(key)
    }
}
