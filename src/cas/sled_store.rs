use std::convert::TryFrom;
use std::ops::Deref;

use sled;

use super::{
    block::Block,
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
