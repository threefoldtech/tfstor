use std::str::FromStr;
use std::sync::Arc;
use std::{io, path::PathBuf};

use super::{
    buffered_byte_stream::BufferedByteStream,
    multipart::{MultiPart, MultiPartTree},
};
use crate::metrics::SharedMetrics;

use crate::metastore::{
    BaseMetaTree, BlockID, BlockTree, BucketMeta, BucketTreeExt, Durability, FjallStore,
    FjallStoreNotx, MetaError, MetaStore, Object, ObjectData,
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

use tracing::error;

pub const BLOCK_SIZE: usize = 1 << 20; // Supposedly 1 MiB

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

use async_trait::async_trait;

#[async_trait]
trait AsyncFileSystem: Send + Sync + std::fmt::Debug {
    async fn create_dir_all(&self, path: &std::path::Path) -> std::io::Result<()>;
    async fn write(&self, path: &std::path::Path, contents: &[u8]) -> std::io::Result<()>;
}

#[derive(Debug)]
struct RealAsyncFs;

#[async_trait]
impl AsyncFileSystem for RealAsyncFs {
    async fn create_dir_all(&self, path: &std::path::Path) -> std::io::Result<()> {
        async_fs::create_dir_all(path).await
    }

    async fn write(&self, path: &std::path::Path, contents: &[u8]) -> std::io::Result<()> {
        async_fs::write(path, contents).await
    }
}

#[derive(Debug)]
pub struct CasFS {
    async_fs: Box<dyn AsyncFileSystem>,
    meta_store: MetaStore,
    root: PathBuf,
    metrics: SharedMetrics,
    multipart_tree: Arc<MultiPartTree>,
}

#[derive(Debug, Clone, Copy)]
pub enum StorageEngine {
    // fjall with transactions support
    Fjall,

    // fjall without transactions support.
    // we implement the rollback logic in our own code
    FjallNotx,
}

impl FromStr for StorageEngine {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "fjall" => Ok(StorageEngine::Fjall),
            "fjall_notx" => Ok(StorageEngine::FjallNotx),
            _ => Err(format!("Unknown storage engine: {}", s)),
        }
    }
}

pub type ObjectPaths = (Object, Vec<(PathBuf, usize)>);

impl CasFS {
    pub fn new(
        mut root: PathBuf,
        mut meta_path: PathBuf,
        metrics: SharedMetrics,
        storage_engine: StorageEngine,
        inlined_metadata_size: Option<usize>,
        durability: Option<Durability>,
    ) -> Self {
        meta_path.push("db");
        root.push("blocks");
        let meta_store = match storage_engine {
            StorageEngine::Fjall => {
                let store = FjallStore::new(meta_path, inlined_metadata_size, durability);
                MetaStore::new(store, inlined_metadata_size)
            }
            StorageEngine::FjallNotx => {
                let store = FjallStoreNotx::new(meta_path, inlined_metadata_size);
                MetaStore::new(store, inlined_metadata_size)
            }
        };
        //let meta_store = MetaStore::new(store, inlined_metadata_size);

        // Get the current amount of buckets
        //metrics.set_bucket_count(db.open_tree(BUCKET_META_TREE).unwrap().len());

        let tree = meta_store.get_tree("_MULTIPART_PARTS").unwrap();
        let multipart_tree = MultiPartTree::new(tree);
        Self {
            async_fs: Box::new(RealAsyncFs),
            meta_store,
            root,
            metrics,
            multipart_tree: Arc::new(multipart_tree),
        }
    }

    fn path_tree(&self) -> Result<Box<dyn BaseMetaTree>, MetaError> {
        self.meta_store.get_path_tree()
    }

    pub fn fs_root(&self) -> &PathBuf {
        &self.root
    }

    pub fn max_inlined_data_length(&self) -> usize {
        self.meta_store.max_inlined_data_length()
    }

    pub fn get_bucket(
        &self,
        bucket_name: &str,
    ) -> Result<Box<dyn BucketTreeExt + Send + Sync>, MetaError> {
        self.meta_store.get_bucket_ext(bucket_name)
    }

    /// Open the tree containing the block map.
    pub fn block_tree(&self) -> Result<BlockTree, MetaError> {
        self.meta_store.get_block_tree()
    }

    /// Check if a bucket with a given name exists.
    pub fn bucket_exists(&self, bucket_name: &str) -> Result<bool, MetaError> {
        self.meta_store.bucket_exists(bucket_name)
    }

    // create a meta object and insert it into the database
    pub fn create_object_meta(
        &self,
        bucket_name: &str,
        key: &str,
        size: u64,
        hash: BlockID,
        object_data: ObjectData,
    ) -> Result<Object, MetaError> {
        let obj_meta = Object::new(size, hash, object_data);
        self.meta_store
            .insert_meta(bucket_name, key, obj_meta.to_vec())?;
        Ok(obj_meta)
    }

    // get meta object from the DB
    pub fn get_object_meta(
        &self,
        bucket_name: &str,
        key: &str,
    ) -> Result<Option<Object>, MetaError> {
        self.meta_store.get_meta(bucket_name, key)
    }

    pub fn get_object_paths(
        &self,
        bucket_name: &str,
        key: &str,
    ) -> Result<Option<ObjectPaths>, MetaError> {
        let obj_meta = self.get_object_meta(bucket_name, key)?;
        let Some(obj_meta) = obj_meta else {
            return Ok(None);
        };

        if obj_meta.is_inlined() {
            Ok(Some((obj_meta, vec![])))
        } else {
            let blocks = obj_meta.blocks();
            let block_map = self.block_tree()?;
            let mut paths = Vec::with_capacity(blocks.len());
            for block in blocks {
                let block_meta = block_map
                    .get_block(block)?
                    .ok_or(MetaError::BlockNotFound)?;
                paths.push((
                    block_meta.disk_path(self.fs_root().clone()),
                    block_meta.size(),
                ));
            }
            Ok(Some((obj_meta, paths)))
        }
    }

    // create and insert a new  bucket
    pub fn create_bucket(&self, bucket_name: &str) -> Result<(), MetaError> {
        let bm = BucketMeta::new(bucket_name.to_string());
        self.meta_store.insert_bucket(bucket_name, bm.to_vec())
    }

    /// Remove a bucket and its associated metadata.
    // TODO: this is very much not optimal
    pub async fn bucket_delete(&self, bucket_name: &str) -> Result<(), MetaError> {
        // remove from the bucket list tree/partition
        let bmt = self.meta_store.get_allbuckets_tree()?;
        bmt.remove(bucket_name.as_bytes())?;

        // removes all objects in the bucket
        let bucket = self.meta_store.get_bucket_ext(bucket_name)?;
        for key in bucket.get_bucket_keys() {
            let key = key?;
            self.delete_object(
                bucket_name,
                std::str::from_utf8(&key).expect("keys are valid utf-8"),
            )
            .await?;
        }

        // remove the bucket tree/partition itself
        self.meta_store.drop_bucket(bucket_name)?;
        Ok(())
    }

    fn part_key(&self, bucket: &str, key: &str, upload_id: &str, part_number: i64) -> String {
        format!("{}-{}-{}-{}", bucket, key, upload_id, part_number)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn insert_multipart_part(
        &self,
        bucket: String,
        key: String,
        size: usize,
        part_number: i64,
        upload_id: String,
        hash: BlockID,
        blocks: Vec<BlockID>,
    ) -> Result<(), MetaError> {
        let mp_map = self.multipart_tree.clone();

        let storage_key = self.part_key(&bucket, &key, &upload_id, part_number);

        let mp = MultiPart::new(size, part_number, bucket, key, upload_id, hash, blocks);

        mp_map.insert(storage_key.as_bytes(), mp)?;
        Ok(())
    }

    pub fn get_multipart_part(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        part_number: i64,
    ) -> Result<Option<MultiPart>, MetaError> {
        let mp_map = self.multipart_tree.clone();
        let part_key = self.part_key(bucket, key, upload_id, part_number);
        mp_map.get_multipart_part(part_key.as_bytes())
    }

    pub fn remove_multipart_part(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        part_number: i64,
    ) -> Result<(), MetaError> {
        let mp_map = self.multipart_tree.clone();
        let part_key = self.part_key(bucket, key, upload_id, part_number);
        mp_map.remove(part_key.as_bytes())
    }

    pub fn key_exists(&self, bucket: &str, key: &str) -> Result<bool, MetaError> {
        let bucket = self.get_bucket(bucket)?;
        bucket.contains_key(key.as_bytes())
    }

    /// Get a list of all buckets in the system.
    pub fn list_buckets(&self) -> Result<Vec<BucketMeta>, MetaError> {
        self.meta_store.list_buckets()
    }

    /// Delete an object from a bucket.
    /// it also delete keys under it's tree
    pub async fn delete_object(&self, bucket: &str, key: &str) -> Result<(), MetaError> {
        let path_map = self.path_tree()?;

        // get blocks that safe to delete
        let blocks_to_delete = self.meta_store.delete_object(bucket, key)?;

        // Now
        // - delete all the blocks from disk
        // - and unlink them in the path map.
        for block in blocks_to_delete {
            async_fs::remove_file(block.disk_path(self.root.clone()))
                .await
                .expect("Could not delete file");
            // Now that the path is free it can be removed from the path map
            if let Err(e) = path_map.remove(block.path()) {
                // Only print error, we might be able to remove the other ones. If we exist
                // here, those will be left dangling.
                error!(
                    "Could not unlink path {} from path map: {}",
                    hex_string(block.path()),
                    e
                );
            };
        }

        Ok(())
    }

    // convenient function to store an object to disk and then store it's metada
    pub async fn store_single_object_and_meta(
        &self,
        bucket_name: &str,
        key: &str,
        data: ByteStream,
    ) -> io::Result<Object> {
        let (blocks, content_hash, size) = self.store_object(bucket_name, key, data).await?;
        let obj = self
            .create_object_meta(
                bucket_name,
                key,
                size,
                content_hash,
                ObjectData::SinglePart { blocks },
            )
            .unwrap();
        Ok(obj)
    }

    /// Save the stream of bytes to disk.
    ///
    /// old_obj_meta is an optional Object that is Some if the key already exists in the metadata.
    ///
    /// The data is streamed in chunks, and each chunk is hashed and stored on disk.
    /// The hash of each chunk is used as a key to store the data in the database.
    ///
    /// A list of block ID's used as keys for the data blocks is
    /// returned, along with the hash of the full byte stream, and the length of the stream.
    pub async fn store_object(
        &self,
        bucket_name: &str,
        key: &str,
        data: ByteStream,
    ) -> io::Result<(Vec<BlockID>, BlockID, u64)> {
        let old_obj_meta = match self.get_object_meta(bucket_name, key) {
            Ok(Some(obj_meta)) => Some(obj_meta),
            _ => None,
        };
        let old_obj_meta = Arc::new(old_obj_meta);

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
        .zip(stream::repeat((tx, old_obj_meta)))
        .enumerate()
        .for_each_concurrent(
            5,
            |(idx, (maybe_chunk, (mut tx, old_obj_meta)))| async move {
                if let Err(e) = maybe_chunk {
                    if let Err(e) = tx
                        .send(Err(std::io::Error::new(e.kind(), e.to_string())))
                        .await
                    {
                        error!("Could not convey result: {}", e);
                    }
                    return;
                }
                // unwrap is safe as we checked that there is no error above
                let bytes: Vec<u8> = maybe_chunk.unwrap();
                let mut hasher = Md5::new();
                hasher.update(&bytes);
                let block_hash: BlockID = hasher.finalize().into();
                let data_len = bytes.len();

                // check if this key already has this block
                let key_has_block = if let Some(obj) = old_obj_meta.as_ref() {
                    obj.has_block(&block_hash)
                } else {
                    false
                };

                // begin the transaction
                // there are two main things we need to do here:
                // 1. write the meta to the database
                //      - if the block already exists, we don't need to write it to the storage
                //      - if the block does not exist, we need to write it to the storage
                // 2. write the actual block to disk
                //
                // we commit the meta database transaction after writing the block to disk
                let mut store_tx = self.meta_store.begin_transaction();
                let write_meta_result = store_tx.write_block(block_hash, data_len, key_has_block);

                let mut pm = PendingMarker::new(self.metrics.clone());

                let block = match write_meta_result {
                    Err(e) => {
                        if let Err(e) = tx.send(Err(e.into())).await {
                            error!("Could not send transaction error: {}", e);
                        }
                        return;
                    }
                    Ok((false, _)) => {
                        // the block already exists, no need to write it to the storage
                        pm.block_ignored();

                        Box::new(store_tx).commit().unwrap();

                        if let Err(e) = tx.send(Ok((idx, block_hash))).await {
                            error!("Could not send block id: {}", e);
                        }
                        return;
                    }
                    Ok((true, block)) => {
                        // the block does not exist, we need to write it to the storage
                        pm.block_pending();
                        block
                    }
                };

                let mut store_tx = Some(store_tx);
                // write the actual block to disk
                // if the disk operation fails, the database transaction is rolled back.
                let block_path = block.disk_path(self.root.clone());
                if let Err(e) = self
                    .async_fs
                    .create_dir_all(block_path.parent().unwrap())
                    .await
                {
                    if let Some(store_tx) = store_tx.take() {
                        Box::new(store_tx).rollback();
                    }

                    if let Err(e) = tx.send(Err(e)).await {
                        pm.block_write_error();
                        error!("Could not send path create error: {}", e);
                        return;
                    }
                }
                if let Err(e) = self.async_fs.write(&block_path, &bytes).await {
                    if let Some(store_tx) = store_tx.take() {
                        Box::new(store_tx).rollback();
                    }

                    if let Err(e) = tx.send(Err(e)).await {
                        pm.block_write_error();
                        error!("Could not send block write error: {}", e);
                        return;
                    }
                }

                // commit the database transaction
                if let Some(store_tx) = store_tx.take() {
                    if let Err(err) = Box::new(store_tx).commit() {
                        // TODO FIXME if the transaction fails, we need to delete the block from the storage
                        if let Err(e) = tx.send(Err(err.into())).await {
                            pm.block_write_error();
                            error!("Could not send transaction error: {}", e);
                        }
                        return;
                    }
                }

                pm.block_written(bytes.len());

                if let Err(e) = tx.send(Ok((idx, block_hash))).await {
                    error!("Could not send block id: {}", e);
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

    // Store an object inlined in the metadata.
    pub fn store_inlined_object(
        &self,
        bucket_name: &str,
        key: &str,
        data: Vec<u8>,
    ) -> Result<Object, MetaError> {
        let content_hash = Md5::digest(&data).into();
        let size = data.len() as u64;
        let obj = self.create_object_meta(
            bucket_name,
            key,
            size,
            content_hash,
            ObjectData::Inline { data },
        )?;
        Ok(obj)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use futures::stream;
    use once_cell::sync::Lazy;
    use rusoto_core::ByteStream;
    use tempfile::tempdir;

    const TEST_ENGINES: [StorageEngine; 2] = [StorageEngine::Fjall, StorageEngine::FjallNotx];

    static METRICS: Lazy<SharedMetrics> = Lazy::new(|| SharedMetrics::new());

    fn setup_test_fs(storage_engine: StorageEngine) -> (CasFS, tempfile::TempDir) {
        let dir = tempdir().unwrap();
        let meta_path = dir.path().join("meta");
        let metrics = METRICS.clone();

        let fs = CasFS::new(
            dir.path().to_path_buf(),
            meta_path,
            metrics,
            storage_engine,
            Some(1),
            Some(Durability::Buffer),
        );
        (fs, dir)
    }

    #[derive(Debug)]
    struct MockFs {
        should_fail_write: bool,
    }

    impl MockFs {
        fn new() -> Self {
            Self {
                should_fail_write: false,
            }
        }
    }

    #[async_trait]
    impl AsyncFileSystem for MockFs {
        async fn create_dir_all(&self, _path: &std::path::Path) -> std::io::Result<()> {
            Ok(())
        }

        async fn write(&self, _path: &std::path::Path, _contents: &[u8]) -> std::io::Result<()> {
            if !self.should_fail_write {
                Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "Mock write failure",
                ))
            } else {
                Ok(())
            }
        }
    }

    impl CasFS {
        #[cfg(test)]
        fn with_mock_fs(mut self) -> (Self, MockFs) {
            // Changed return type
            let mock_fs = MockFs::new();
            self.async_fs = Box::new(mock_fs.clone()); // Implement Clone for MockFs
            (self, mock_fs)
        }
    }

    // Add Clone implementation for MockFs
    impl Clone for MockFs {
        fn clone(&self) -> Self {
            Self {
                should_fail_write: self.should_fail_write,
            }
        }
    }

    #[tokio::test]
    async fn test_store_object_write_failure() {
        for engine in TEST_ENGINES {
            let (fs, _dir) = setup_test_fs(engine).0.with_mock_fs();
            do_test_store_object_write_failure(fs).await;
        }
    }

    async fn do_test_store_object_write_failure(fs: CasFS) {
        let bucket_name = "test_bucket";
        let key = "test_key";
        fs.create_bucket(bucket_name).unwrap();

        let test_data = b"test data".repeat(100);
        let stream = ByteStream::new(stream::once(async move { Ok(Bytes::from(test_data)) }));

        let result = fs.store_object(bucket_name, key, stream).await;
        assert!(result.is_err());

        // Verify the error
        let err = result.unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::Other);
        assert_eq!(err.to_string(), "Mock write failure");

        // Verify no blocks were stored in metadata
        // the block must be rolled back
        let block_tree = fs.meta_store.get_block_tree().unwrap();
        assert_eq!(block_tree.len().unwrap(), 0);

        // Verify object metadata was not created
        assert!(!fs.key_exists(bucket_name, key).unwrap());
    }

    #[tokio::test]
    async fn test_store_object() {
        for engine in TEST_ENGINES {
            let (fs, _dir) = setup_test_fs(engine);
            do_test_store_object(fs).await;
        }
    }

    async fn do_test_store_object(fs: CasFS) {
        const BUCKET_NAME: &str = "test_bucket";
        const KEY1: &str = "test_key1";
        const KEY2: &str = "test_key2";
        fs.create_bucket(BUCKET_NAME).unwrap();

        // Create ByteStream from test data
        let test_data = b"long test data".repeat(100).to_vec();
        let test_data_2 = test_data.clone();
        let test_data_len = test_data.len();
        let stream = ByteStream::new(stream::once(
            async move { Ok(Bytes::from(test_data.clone())) },
        ));

        // Store object
        let obj = fs
            .store_single_object_and_meta(BUCKET_NAME, KEY1, stream)
            .await
            .unwrap();

        // Verify results
        assert_eq!(obj.size(), test_data_len as u64);
        assert_eq!(obj.blocks().len(), 1);

        // Verify block & path was stored
        let block_tree = fs.meta_store.get_block_tree().unwrap();
        assert!(block_tree.len().unwrap() > 0);
        let stored_block = block_tree.get_block(&obj.blocks()[0]).unwrap().unwrap();
        assert_eq!(stored_block.size(), test_data_len);
        assert_eq!(stored_block.rc(), 1);
        assert_eq!(
            fs.path_tree()
                .unwrap()
                .contains_key(stored_block.path())
                .unwrap(),
            true
        );

        // Store the same data again with different key
        // - The same block should be returned
        // - The refcount should be increased

        let stream = ByteStream::new(stream::once(
            async move { Ok(Bytes::from(test_data_2.clone())) },
        ));

        let new_obj = fs
            .store_single_object_and_meta(BUCKET_NAME, KEY2, stream)
            .await
            .unwrap();

        assert_eq!(new_obj.blocks(), obj.blocks());

        let stored_block = block_tree.get_block(&new_obj.blocks()[0]).unwrap().unwrap();
        assert_eq!(stored_block.rc(), 2);
    }

    #[tokio::test]
    async fn test_store_inlined_object() {
        for engine in TEST_ENGINES {
            let (fs, _dir) = setup_test_fs(engine);
            do_test_store_inlined_object(fs).await;
        }
    }

    async fn do_test_store_inlined_object(fs: CasFS) {
        let bucket_name = "test_bucket";
        let key = "test_key1";
        fs.create_bucket(bucket_name).unwrap();

        let small_data = b"small test data".to_vec();
        let obj_meta = fs
            .store_inlined_object(bucket_name, key, small_data.clone())
            .unwrap();

        // Verify inlined data
        assert_eq!(obj_meta.size(), small_data.len() as u64);
        assert_eq!(obj_meta.inlined().unwrap(), &small_data);
    }

    #[tokio::test]
    async fn test_store_object_refcount() {
        for engine in TEST_ENGINES {
            let (fs, _dir) = setup_test_fs(engine);
            do_test_store_object_refcount(fs).await;
        }
    }

    async fn do_test_store_object_refcount(fs: CasFS) {
        let bucket_name = "test_bucket";
        let key1 = "test_key1";
        let key2 = "test_key2";
        fs.create_bucket(bucket_name).unwrap();

        // Create ByteStream from test data
        let test_data = b"long test data".repeat(100).to_vec();
        let test_data_2 = test_data.clone();
        let test_data_3 = test_data.clone();
        let stream = ByteStream::new(stream::once(
            async move { Ok(Bytes::from(test_data.clone())) },
        ));

        // Store object
        let obj = fs
            .store_single_object_and_meta(bucket_name, key1, stream)
            .await
            .unwrap();

        // Initial refcount must be 1
        let block_tree = fs.meta_store.get_block_tree().unwrap();
        let stored_block = block_tree.get_block(&obj.blocks()[0]).unwrap().unwrap();
        assert_eq!(stored_block.rc(), 1);

        {
            // Test using  the same key
            // Refcount must not be increased

            let stream =
                ByteStream::new(stream::once(
                    async move { Ok(Bytes::from(test_data_2.clone())) },
                ));

            let new_obj = fs
                .store_single_object_and_meta(bucket_name, key1, stream)
                .await
                .unwrap();

            assert_eq!(new_obj.blocks(), obj.blocks());

            let stored_block = block_tree.get_block(&new_obj.blocks()[0]).unwrap().unwrap();
            assert_eq!(stored_block.rc(), 1);
        }
        {
            // Test  using a new key
            // Refcount must be increased
            let stream =
                ByteStream::new(stream::once(
                    async move { Ok(Bytes::from(test_data_3.clone())) },
                ));

            let new_obj = fs
                .store_single_object_and_meta(bucket_name, key2, stream)
                .await
                .unwrap();

            assert_eq!(new_obj.blocks(), obj.blocks());

            let stored_block = block_tree.get_block(&new_obj.blocks()[0]).unwrap().unwrap();
            assert_eq!(stored_block.rc(), 2);
        }
    }

    #[tokio::test]
    async fn test_store_and_delete_object() {
        for engine in TEST_ENGINES {
            let (fs, _dir) = setup_test_fs(engine);
            do_test_store_and_delete_object(fs).await;
        }
    }

    // test store and delete object
    // - store an object
    // - delete the object
    async fn do_test_store_and_delete_object(fs: CasFS) {
        let bucket_name = "test-bucket";
        let key = "test/key";

        // Create bucket
        fs.create_bucket(bucket_name).unwrap();

        // Create test data and stream
        let test_data = b"test data".to_vec();
        let stream = ByteStream::new(stream::once(
            async move { Ok(Bytes::from(test_data.clone())) },
        ));

        // Store object
        let obj = fs
            .store_single_object_and_meta(bucket_name, key, stream)
            .await
            .unwrap();

        // Verify object exists
        let exists = fs.key_exists(bucket_name, key).unwrap();
        assert_eq!(exists, true);

        // verify blocks and path exist
        let block_tree = fs.meta_store.get_block_tree().unwrap();
        let mut stored_paths = Vec::new();
        for id in obj.blocks() {
            let block = block_tree.get_block(id).unwrap().unwrap();
            assert_eq!(
                fs.path_tree().unwrap().contains_key(block.path()).unwrap(),
                true
            );
            stored_paths.push(block.path().to_vec());
        }

        // Delete object
        fs.delete_object(bucket_name, key).await.unwrap();

        // Verify object no longer exists
        let exists = fs.key_exists(bucket_name, key).unwrap();
        assert_eq!(exists, false);

        // Verify blocks were cleaned up
        let block_tree = fs.meta_store.get_block_tree().unwrap();
        for id in obj.blocks() {
            assert!(block_tree.get_block(id).unwrap().is_none());
        }
        // Verify paths were cleaned up
        for path in stored_paths {
            assert_eq!(fs.path_tree().unwrap().contains_key(&path).unwrap(), false);
        }
    }

    #[tokio::test]
    async fn test_store_and_delete_object_with_refcount_same_blocks_diffkey() {
        for engine in TEST_ENGINES {
            let (fs, _dir) = setup_test_fs(engine);
            do_test_store_and_delete_object_with_refcount_same_blocks_diffkey(fs).await;
        }
    }

    // Test storing and deleting an object with refcount
    // - store object
    //       refcount == 1
    // - store object again with differrent key
    //      refcount == 2
    // - delete the first object
    // - check block/disk/whatever is still there
    // - delete the second object
    // - check block/disk/whatever should be gone
    async fn do_test_store_and_delete_object_with_refcount_same_blocks_diffkey(fs: CasFS) {
        let bucket = "test-bucket";
        let key1 = "test/key1";
        let key2 = "test/key2";

        // Create bucket
        fs.create_bucket(bucket).unwrap();

        // Create test data
        let test_data = b"test data".to_vec();
        let test_data2 = test_data.clone();
        let stream1 = ByteStream::new(stream::once(
            async move { Ok(Bytes::from(test_data.clone())) },
        ));

        // Store first object
        let obj1 = fs
            .store_single_object_and_meta(bucket, key1, stream1)
            .await
            .unwrap();
        // Verify blocks  exist with rc=1
        let block_tree = fs.meta_store.get_block_tree().unwrap();
        for id in obj1.blocks() {
            let block = block_tree.get_block(id).unwrap().unwrap();
            assert_eq!(block.rc(), 1);
        }

        // Store same data with different key

        let stream2 = ByteStream::new(stream::once(
            async move { Ok(Bytes::from(test_data2.clone())) },
        ));

        let obj2 = fs
            .store_single_object_and_meta(bucket, key2, stream2)
            .await
            .unwrap();

        // Verify both objects share same blocks
        assert_eq!(obj1.blocks(), obj2.blocks());
        assert_eq!(obj1.hash(), obj2.hash());
        // Verify blocks  exist with rc=2
        let block_tree = fs.meta_store.get_block_tree().unwrap();
        for id in obj2.blocks() {
            let block = block_tree.get_block(id).unwrap().unwrap();
            assert_eq!(block.rc(), 2);
        }

        // Delete first object
        fs.delete_object(bucket, key1).await.unwrap();

        // Verify blocks still exist
        let block_tree = fs.meta_store.get_block_tree().unwrap();
        for id in obj1.blocks() {
            let block = block_tree.get_block(id).unwrap().unwrap();
            assert_eq!(block.rc(), 1);
        }

        // Delete second object
        fs.delete_object(bucket, key2).await.unwrap();

        // Verify blocks are gone
        for id in obj1.blocks() {
            assert!(block_tree.get_block(id).unwrap().is_none());
        }
    }

    #[tokio::test]
    async fn test_store_and_delete_object_with_refcount_same_blocks_samekey() {
        for engine in TEST_ENGINES {
            let (fs, _dir) = setup_test_fs(engine);
            do_test_store_and_delete_object_with_refcount_same_blocks_samekey(fs).await;
        }
    }

    // Test storing and deleting an object with refcount
    // - store object
    //       refcount == 1
    // - store object again with differrent key
    //      refcount == 1
    // - delete the object
    // - check block/disk/whatever should be gone
    async fn do_test_store_and_delete_object_with_refcount_same_blocks_samekey(fs: CasFS) {
        let bucket = "test-bucket";
        let key1 = "test/key1";

        // Create bucket
        fs.create_bucket(bucket).unwrap();

        // Create test data
        let test_data = b"test data".to_vec();
        let test_data2 = test_data.clone();
        let stream1 = ByteStream::new(stream::once(
            async move { Ok(Bytes::from(test_data.clone())) },
        ));

        // Store first object
        let obj1 = fs
            .store_single_object_and_meta(bucket, key1, stream1)
            .await
            .unwrap();
        // Verify blocks  exist with rc=1
        let block_tree = fs.meta_store.get_block_tree().unwrap();
        for id in obj1.blocks() {
            let block = block_tree.get_block(id).unwrap().unwrap();
            assert_eq!(block.rc(), 1);
        }

        // Store same data with same key

        let stream2 = ByteStream::new(stream::once(
            async move { Ok(Bytes::from(test_data2.clone())) },
        ));

        let obj2 = fs
            .store_single_object_and_meta(bucket, key1, stream2)
            .await
            .unwrap();

        // Verify both objects share same blocks
        assert_eq!(obj1.blocks(), obj2.blocks());
        assert_eq!(obj1.hash(), obj2.hash());
        // Verify blocks  exist with rc=2
        let block_tree = fs.meta_store.get_block_tree().unwrap();
        for id in obj2.blocks() {
            let block = block_tree.get_block(id).unwrap().unwrap();
            assert_eq!(block.rc(), 1);
        }

        // Delete object
        fs.delete_object(bucket, key1).await.unwrap();

        // Verify blocks are gone
        for id in obj1.blocks() {
            assert!(block_tree.get_block(id).unwrap().is_none());
        }
    }
}
