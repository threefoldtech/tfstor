use super::{block::Block, meta_errors::MetaError, multipart::MultiPart, object::Object};
use std::fmt::Debug;

pub trait MetaStore: Send + Sync + Debug + 'static {
    /// get_tree returns a MetaTree for the given name.
    fn get_base_tree(&self, name: &str) -> Result<Box<dyn BaseMetaTree>, MetaError>;
    fn get_tree(&self, name: &str) -> Result<Box<dyn MetaTree + Send + Sync>, MetaError>;
}

pub trait BaseMetaTree: Send + Sync {
    fn insert(&self, key: &[u8], value: Vec<u8>) -> Result<(), MetaError>;
    fn remove(&self, key: &[u8]) -> Result<(), MetaError>;
    fn get_block_obj(&self, key: &[u8]) -> Result<Block, MetaError>;
    fn get_multipart_part_obj(&self, key: &[u8]) -> Result<MultiPart, MetaError>;
}

pub trait MetaTreeExt: BaseMetaTree {
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
