use std::fmt::Debug;
use std::str::FromStr;

use super::{object::Object, MetaError, Transaction};

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

pub type KeyValuePairs = Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>), MetaError>> + Send>;

pub trait MetaTreeExt: BaseMetaTree {
    // iterate all key and value pairs
    fn iter_all(&self) -> KeyValuePairs;

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

    fn tree_ext_open(&self, name: &str) -> Result<Box<dyn MetaTreeExt + Send + Sync>, MetaError>;

    // check if the tree exists
    fn tree_exists(&self, name: &str) -> Result<bool, MetaError>;

    // delete the tree with the given name
    fn tree_delete(&self, name: &str) -> Result<(), MetaError>;

    fn begin_transaction(&self) -> Transaction;

    // return number of keys in the tree
    fn num_keys(&self, tree_name: &str) -> Result<usize, MetaError>;

    // return disk space used by keyspace
    fn disk_space(&self) -> u64;
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
