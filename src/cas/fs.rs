use crate::metrics::SharedMetrics;
use std::ops::Deref;

use super::{
    block::{Block, BlockID, BLOCKID_SIZE},
    bucket_meta::BucketMeta,
    buffered_byte_stream::BufferedByteStream,
    multipart::MultiPart,
    object::Object,
};

use faster_hex::hex_string;
use futures::{
    channel::mpsc::unbounded,
    sink::SinkExt,
    stream,
    stream::{StreamExt, TryStreamExt},
};
use md5::{Digest, Md5};
use rusoto_core::ByteStream;
use sled::{Db, Transactional};
use std::{
    convert::{TryFrom, TryInto},
    io, mem,
    path::PathBuf,
};
use tracing::info;

pub const BLOCK_SIZE: usize = 1 << 20; // Supposedly 1 MiB
const BUCKET_META_TREE: &str = "_BUCKETS";
const BLOCK_TREE: &str = "_BLOCKS";
const PATH_TREE: &str = "_PATHS";
const MULTIPART_TREE: &str = "_MULTIPART_PARTS";
pub const PTR_SIZE: usize = mem::size_of::<usize>(); // Size of a `usize` in bytes

#[derive(Debug)]
pub struct CasFS {
    db: Db,
    root: PathBuf,
    metrics: SharedMetrics,
}

struct PendingMarker {
    metrics: SharedMetrics,
    in_flight: u64,
}

impl PendingMarker {
    pub fn new(metrics: SharedMetrics) -> Self {
        Self {
            metrics,
            in_flight: 0,
        }
    }

    pub fn block_pending(&mut self) {
        self.metrics.block_pending();
        self.in_flight += 1;
    }

    pub fn block_write_error(&mut self) {
        self.metrics.block_write_error();
        self.in_flight -= 1;
    }

    pub fn block_ignored(&mut self) {
        self.metrics.block_ignored();
    }

    pub fn block_written(&mut self, size: usize) {
        self.metrics.block_written(size);
        self.in_flight -= 1;
    }
}

impl Drop for PendingMarker {
    fn drop(&mut self) {
        self.metrics.blocks_dropped(self.in_flight)
    }
}

use std::error::Error;
use std::fmt;

// Define the error type
#[derive(Debug)]
pub enum MetaError {
    KeyNotFound,
    KeyAlreadyExists,
    CollectionNotFound,
    BucketNotFound,
    UnknownError(String),
}

// Implement the std::error::Error trait
impl Error for MetaError {}

// Implement the Display trait for custom error messages
impl fmt::Display for MetaError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            MetaError::KeyNotFound => write!(f, "Key not found"),
            MetaError::KeyAlreadyExists => write!(f, "Key already exists"),
            MetaError::CollectionNotFound => write!(f, "Collection not found"),
            MetaError::BucketNotFound => write!(f, "Bucket not found"),
            MetaError::UnknownError(ref s) => write!(f, "Unknown error: {}", s),
        }
    }
}

/// MetaTree is a wrapper around a sled::Tree that provides a higher level API for interacting with
/// TODO: it should be part of the meta's `Object` struct
#[derive(Clone)]
pub struct MetaTree {
    tree: sled::Tree,
}

impl MetaTree {
    fn new(tree: sled::Tree) -> Self {
        Self { tree }
    }

    pub fn insert(&self, key: Vec<u8>, value: Vec<u8>) -> Result<(), MetaError> {
        match self.tree.insert(key, value) {
            Ok(_) => Ok(()),
            Err(e) => Err(MetaError::UnknownError(e.to_string())),
        }
    }

    pub fn remove(&self, key: String) -> Result<(), MetaError> {
        match self.tree.remove(key) {
            Ok(_) => Ok(()),
            Err(e) => Err(MetaError::UnknownError(e.to_string())),
        }
    }

    pub fn create_block_obj<K: AsRef<[u8]>>(&self, key: K) -> Result<Block, MetaError> {
        let block_data = self.get(key)?.ok_or(MetaError::KeyNotFound)?;
        let block = match Block::try_from(&*block_data) {
            Ok(b) => b,
            Err(e) => return Err(MetaError::UnknownError(e.to_string())),
        };
        Ok(block)
    }

    pub fn create_multipart_part<K: AsRef<[u8]>>(&self, key: K) -> Result<MultiPart, MetaError> {
        let part_data_enc = self.get(&key)?;
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

    // TODO: merge this method with range_filter
    pub fn range_filter_skip<'a>(
        &self,
        start_bytes: &'a [u8],
        prefix_bytes: &'a [u8],
        start_after: Option<String>,
    ) -> impl Iterator<Item = (String, Object)> + 'a {
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
            })
    }

    // TODO: merge this method with range_filter_skip
    pub fn range_filter<'a>(
        &self,
        start_bytes: &'a [u8],
        prefix_bytes: &'a [u8],
    ) -> impl Iterator<Item = (String, Object)> + 'a {
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
            })
    }

    // get an object from the tree
    fn get<K: AsRef<[u8]>>(&self, key: K) -> Result<Option<sled::IVec>, MetaError> {
        match self.tree.get(key) {
            Ok(Some(v)) => Ok(Some(v)),
            Ok(None) => Ok(None),
            Err(e) => Err(MetaError::UnknownError(e.to_string())),
        }
    }
}

impl CasFS {
    pub fn new(mut root: PathBuf, mut meta_path: PathBuf, metrics: SharedMetrics) -> Self {
        meta_path.push("db");
        root.push("blocks");
        let db = sled::open(meta_path).unwrap();
        // Get the current amount of buckets
        metrics.set_bucket_count(db.open_tree(BUCKET_META_TREE).unwrap().len());
        Self { db, root, metrics }
    }

    pub fn get_bucket(&self, bucket_name: &str) -> Result<MetaTree, MetaError> {
        let tree = self.get_tree(bucket_name)?;
        Ok(MetaTree::new(tree))
    }

    /// Open the tree containing the block map.
    pub fn block_tree(&self) -> Result<MetaTree, MetaError> {
        //self.db.open_tree(BLOCK_TREE)
        let tree = self.get_tree(BLOCK_TREE)?;
        Ok(MetaTree::new(tree))
    }

    pub fn multipart_tree(&self) -> Result<MetaTree, MetaError> {
        let tree = self.get_tree(MULTIPART_TREE)?;
        Ok(MetaTree::new(tree))
    }

    /// Check if a bucket with a given name exists.
    pub fn bucket_exists(&self, bucket_name: &str) -> Result<bool, MetaError> {
        let tree = self.get_tree(BUCKET_META_TREE)?;
        match tree.contains_key(bucket_name) {
            Ok(true) => Ok(true),
            Ok(false) => Ok(false),
            Err(e) => Err(MetaError::UnknownError(e.to_string())),
        }
    }

    // create a meta object and insert it into the database
    pub fn create_insert_meta(
        &self,
        bucket_name: &str,
        key: &str,
        size: u64,
        e_tag: BlockID,
        parts: usize,
        blocks: Vec<BlockID>,
    ) -> Result<Object, MetaError> {
        let obj_meta = Object::new(size, e_tag, parts, blocks);

        let bucket = self.get_tree(bucket_name)?;

        match bucket.insert(key, obj_meta.to_vec()) {
            Ok(_) => Ok(obj_meta),
            Err(e) => Err(MetaError::UnknownError(e.to_string())),
        }
    }

    // get meta object from the DB
    pub fn get_object_meta(&self, bucket: &str, key: &str) -> Result<Object, MetaError> {
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

    // create and insert a new  bucket
    pub fn create_bucket(&self, bucket_name: String) -> Result<(), MetaError> {
        let bucket_meta = self.get_tree(BUCKET_META_TREE)?;

        let bm = BucketMeta::new(bucket_name.clone()).to_vec();

        match bucket_meta.insert(bucket_name, bm) {
            Ok(_) => Ok(()),
            Err(e) => Err(MetaError::UnknownError(e.to_string())),
        }
    }

    /// Open the tree containing the objects in a bucket.
    fn get_tree(&self, bucket_name: &str) -> Result<sled::Tree, MetaError> {
        match self.db.open_tree(bucket_name) {
            Ok(tree) => Ok(tree),
            Err(e) => match e {
                sled::Error::CollectionNotFound(_) => Err(MetaError::CollectionNotFound),
                _ => Err(MetaError::UnknownError(e.to_string())),
            },
        }
    }

    // Open the tree containing the path map.
    fn sled_path_tree(&self) -> Result<sled::Tree, sled::Error> {
        self.db.open_tree(PATH_TREE)
    }

    /// Open the tree containing the block map.
    fn sled_block_tree(&self) -> Result<sled::Tree, sled::Error> {
        self.db.open_tree(BLOCK_TREE)
    }

    /// Open the tree containing the bucket metadata.
    fn sled_bucket_meta_tree(&self) -> Result<sled::Tree, sled::Error> {
        self.db.open_tree(BUCKET_META_TREE)
    }

    fn sled_bucket(&self, bucket_name: &str) -> Result<sled::Tree, sled::Error> {
        self.db.open_tree(bucket_name)
    }

    /// Remove a bucket and its associated metadata.
    // TODO: this is very much not optimal
    pub async fn bucket_delete(&self, bucket_name: &str) -> Result<(), sled::Error> {
        let bmt = self.sled_bucket_meta_tree()?;
        bmt.remove(bucket_name)?;
        let bucket = self.sled_bucket(bucket_name)?;
        for key in bucket.iter().keys() {
            self.delete_object(
                bucket_name,
                std::str::from_utf8(&key?).expect("keys are valid utf-8"),
            )
            .await?;
        }

        self.db.drop_tree(bucket_name)?;
        Ok(())
    }

    /// Get a list of all buckets in the system.
    pub fn list_buckets(&self) -> Result<Vec<BucketMeta>, MetaError> {
        let bucket_tree = match self.sled_bucket_meta_tree() {
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

    /// Delete an object from a bucket.
    pub async fn delete_object(&self, bucket: &str, object: &str) -> Result<(), sled::Error> {
        info!("Deleting object {}", object);

        // Remove an object. This fetches the object, decrements the refcount of all blocks,
        // and removes blocks which are no longer referenced.
        let block_map = self.sled_block_tree()?;
        let path_map = self.sled_path_tree()?;
        let bucket = self.sled_bucket(bucket)?;
        let blocks_to_delete_res: Result<Vec<Block>, sled::transaction::TransactionError> =
            (&bucket, &block_map).transaction(|(bucket, blocks)| {
                match bucket.get(object)? {
                    None => Ok(vec![]),
                    Some(o) => {
                        let obj = Object::try_from(&*o).expect("Malformed object");
                        let mut to_delete = Vec::with_capacity(obj.blocks().len());
                        // delete the object in the database, we have it in memory to remove the
                        // blocks as needed.
                        bucket.remove(object)?;
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
                return Err(e);
            }
            Ok(blocks) => blocks,
            // We don't abort manually so this can't happen
            Err(sled::transaction::TransactionError::Abort(_)) => unreachable!(),
        };

        // Now delete all the blocks from disk, and unlink them in the path map.
        for block in blocks_to_delete {
            async_fs::remove_file(block.disk_path(self.root.clone()))
                .await
                .expect("Could not delete file");
            // Now that the path is free it can be removed from the path map
            if let Err(e) = path_map.remove(block.path()) {
                // Only print error, we might be able to remove the other ones. If we exist
                // here, those will be left dangling.
                eprintln!(
                    "Could not unlink path {} from path map: {}",
                    hex_string(block.path()),
                    e
                );
            };
        }

        Ok(())
    }

    /// Save data on the filesystem. A list of block ID's used as keys for the data blocks is
    /// returned, along with the hash of the full byte stream, and the length of the stream.
    pub async fn store_bytes(&self, data: ByteStream) -> io::Result<(Vec<BlockID>, BlockID, u64)> {
        let block_map = self.sled_block_tree()?;
        let path_map = self.sled_path_tree()?;
        let (tx, rx) = unbounded();
        let mut content_hash = Md5::new();
        let data = BufferedByteStream::new(data);
        let mut size = 0;
        data.map(|res| match res {
            Ok(buffers) => buffers.into_iter().map(Ok).collect(),
            Err(e) => vec![Err(e)],
        })
        .map(stream::iter)
        .flatten()
        .inspect(|maybe_bytes| {
            if let Ok(bytes) = maybe_bytes {
                content_hash.update(bytes);
                size += bytes.len() as u64;
                self.metrics.bytes_received(bytes.len());
            }
        })
        .zip(stream::repeat((tx, block_map, path_map)))
        .enumerate()
        .for_each_concurrent(
            5,
            |(idx, (maybe_chunk, (mut tx, block_map, path_map)))| async move {
                if let Err(e) = maybe_chunk {
                    if let Err(e) = tx
                        .send(Err(std::io::Error::new(e.kind(), e.to_string())))
                        .await
                    {
                        eprintln!("Could not convey result: {}", e);
                    }
                    return;
                }
                // unwrap is safe as we checked that there is no error above
                let bytes: Vec<u8> = maybe_chunk.unwrap();
                let mut hasher = Md5::new();
                hasher.update(&bytes);
                let block_hash: BlockID = hasher.finalize().into();
                let data_len = bytes.len();

                // Check if the hash is present in the block map. If it is not, try to find a path, and
                // insert it.
                let should_write: Result<bool, sled::transaction::TransactionError> =
                    (&block_map, &path_map).transaction(|(blocks, paths)| {
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

                let mut pm = PendingMarker::new(self.metrics.clone());
                match should_write {
                    Err(sled::transaction::TransactionError::Storage(e)) => {
                        if let Err(e) = tx.send(Err(e.into())).await {
                            eprintln!("Could not send transaction error: {}", e);
                        }
                        return;
                    }
                    Ok(false) => {
                        pm.block_ignored();
                        if let Err(e) = tx.send(Ok((idx, block_hash))).await {
                            eprintln!("Could not send block id: {}", e);
                        }
                        return;
                    }
                    Ok(true) => pm.block_pending(),
                    // We don't abort manually so this can't happen
                    Err(sled::transaction::TransactionError::Abort(_)) => unreachable!(),
                };

                // write the actual block
                // first load the block again from the DB
                let block: Block = match block_map.get(block_hash) {
                    Ok(Some(encoded_block)) => (&*encoded_block)
                        .try_into()
                        .expect("Block data is corrupted"),
                    // we just inserted this block, so this is by definition impossible
                    Ok(None) => unreachable!(),
                    Err(e) => {
                        if let Err(e) = tx.send(Err(e.into())).await {
                            pm.block_write_error();
                            eprintln!("Could not send db error: {}", e);
                        }
                        return;
                    }
                };

                let block_path = block.disk_path(self.root.clone());
                if let Err(e) = async_fs::create_dir_all(block_path.parent().unwrap()).await {
                    if let Err(e) = tx.send(Err(e)).await {
                        pm.block_write_error();
                        eprintln!("Could not send path create error: {}", e);
                        return;
                    }
                }
                if let Err(e) = async_fs::write(block_path, &bytes).await {
                    if let Err(e) = tx.send(Err(e)).await {
                        pm.block_write_error();
                        eprintln!("Could not send block write error: {}", e);
                        return;
                    }
                }

                pm.block_written(bytes.len());

                if let Err(e) = tx.send(Ok((idx, block_hash))).await {
                    eprintln!("Could not send block id: {}", e);
                }
            },
        )
        .await;

        let mut ids = rx.try_collect::<Vec<(usize, BlockID)>>().await?;
        // Make sure the chunks are in the proper order
        ids.sort_by_key(|a| a.0);

        Ok((
            ids.into_iter().map(|(_, id)| id).collect(),
            content_hash.finalize().into(),
            size,
        ))
    }
}
