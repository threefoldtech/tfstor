use super::{
    block::Block, bucket_meta::BucketMeta, meta_errors::MetaError, multipart::MultiPart,
    object::Object,
};
use std::fmt::Debug;

pub trait MetaStore: Send + Sync + Debug + 'static {
    /// get_base_tree returns a base tree for the given name.
    fn get_base_tree(&self, name: &str) -> Result<Box<dyn BaseMetaTree>, MetaError>;

    /// get_tree returns a tree for the given name.
    fn get_tree(&self, name: &str) -> Result<Box<dyn MetaTree + Send + Sync>, MetaError>;

    /// bucket_exists returns true if the bucket exists.
    fn bucket_exists(&self, bucket_name: &str) -> Result<bool, MetaError>;

    /// drop_tree drops the tree with the given name.
    fn drop_tree(&self, name: &str) -> Result<(), MetaError>;

    /// insert_bucket inserts a bucket into the meta store.
    fn insert_bucket(&self, bucket_name: String, bm: BucketMeta) -> Result<(), MetaError>;

    /// insert_meta_obj inserts an object into the meta store.
    fn insert_meta_obj(
        &self,
        bucket_name: &str,
        key: &str,
        obj_meta: Object,
    ) -> Result<Object, MetaError>;

    /// get_meta_obj returns the object metadata for the given bucket and key.
    fn get_meta_obj(&self, bucket: &str, key: &str) -> Result<Object, MetaError>;

    /// Get a list of all buckets in the system.
    /// TODO: this should be paginated and return a stream.
    fn list_buckets(&self) -> Result<Vec<BucketMeta>, MetaError>;
}

pub trait BaseMetaTree: Send + Sync {
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
