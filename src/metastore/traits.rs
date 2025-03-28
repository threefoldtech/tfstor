use std::fmt::Debug;
use std::str::FromStr;

use super::{bucket_meta::BucketMeta, object::Object, MetaError, Transaction};

pub trait BaseMetaTree: Send + Sync {
    /// insert inserts a key value pair into the tree.
    fn insert(&self, key: &[u8], value: Vec<u8>) -> Result<(), MetaError>;

    /// remove removes a key from the tree.
    fn remove(&self, key: &[u8]) -> Result<(), MetaError>;

    fn contains_key(&self, key: &[u8]) -> Result<bool, MetaError>;

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, MetaError>;

    #[cfg(test)]
    fn len(&self) -> Result<usize, MetaError>;
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

pub trait Store: Send + Sync + Debug + 'static {
    // open the tree with the given name
    // creates the tree if it doesn't exist
    fn tree_open(&self, name: &str) -> Result<Box<dyn BaseMetaTree>, MetaError>;

    fn tree_ext_open(&self, name: &str) -> Result<Box<dyn BucketTreeExt + Send + Sync>, MetaError>;

    // check if the tree exists
    fn tree_exists(&self, name: &str) -> Result<bool, MetaError>;

    // delete the tree with the given name
    fn tree_delete(&self, name: &str) -> Result<(), MetaError>;

    fn begin_transaction(&self) -> Transaction;

    fn num_keys(&self) -> (usize, usize, usize);

    fn disk_space(&self) -> u64;

    // get list of all buckets in the system
    fn list_buckets(&self, bucket_tree_name: &str) -> Result<Vec<BucketMeta>, MetaError>;
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
