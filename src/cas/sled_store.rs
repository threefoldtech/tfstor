use std::convert::TryFrom;
use std::ops::Deref;

use sled;

use super::{
    block::Block,
    meta_errors::MetaError,
    meta_store::{BaseMetaTree, MetaStore, MetaTree, MetaTreeExt},
    multipart::MultiPart,
    object::Object,
};

#[derive(Debug)]
pub struct SledStore {
    db: sled::Db,
}

impl SledStore {
    pub fn new(db: sled::Db) -> Self {
        Self { db }
    }

    fn get_tree(&self, name: &str) -> Result<Box<dyn MetaTree + Send + Sync>, MetaError> {
        match self.db.open_tree(name) {
            Ok(tree) => Ok(Box::new(SledTree::new(tree))),
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
        self.get_tree(name)
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
        let part_data_enc = self.get(key)?;
        let part_data_enc = match part_data_enc {
            Some(pde) => pde,
            None => {
                return Err(MetaError::KeyNotFound);
            }
        };

        // unwrap here is safe as it is a coding error
        let mp = MultiPart::try_from(&*part_data_enc).expect("Corrupted multipart data");
        Ok(mp)
    }
}

impl MetaTreeExt for SledTree {
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
