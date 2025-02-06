use std::sync::Arc;
use std::{io, mem, path::PathBuf};

use super::{
    block::BlockID, bucket_meta::BucketMeta, buffered_byte_stream::BufferedByteStream, fjall_store,
    meta_errors::MetaError, meta_store, object::Object,
};
use crate::metrics::SharedMetrics;

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
pub const PTR_SIZE: usize = mem::size_of::<usize>(); // Size of a `usize` in bytes

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

#[derive(Debug)]
pub struct CasFS {
    meta_store: Box<dyn meta_store::MetaStore>,
    root: PathBuf,
    metrics: SharedMetrics,
}

pub enum StorageEngine {
    Fjall,
}

impl CasFS {
    pub fn new(
        mut root: PathBuf,
        mut meta_path: PathBuf,
        metrics: SharedMetrics,
        storage_engine: StorageEngine,
    ) -> Self {
        meta_path.push("db");
        root.push("blocks");
        let meta_store: Box<dyn meta_store::MetaStore> = match storage_engine {
            StorageEngine::Fjall => Box::new(fjall_store::FjallStore::new(meta_path)),
        };

        // Get the current amount of buckets
        //metrics.set_bucket_count(db.open_tree(BUCKET_META_TREE).unwrap().len());
        Self {
            meta_store,
            root,
            metrics,
        }
    }

    fn path_tree(&self) -> Result<Box<dyn meta_store::BaseMetaTree>, MetaError> {
        self.meta_store.get_path_tree()
    }

    pub fn get_bucket(
        &self,
        bucket_name: &str,
    ) -> Result<Box<dyn meta_store::MetaTree + Send + Sync>, MetaError> {
        self.meta_store.get_bucket_ext(bucket_name)
    }

    /// Open the tree containing the block map.
    pub fn block_tree(&self) -> Result<Box<dyn meta_store::BaseMetaTree>, MetaError> {
        self.meta_store.get_block_tree()
    }

    pub fn multipart_tree(&self) -> Result<Box<dyn meta_store::BaseMetaTree>, MetaError> {
        self.meta_store.get_multipart_tree()
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
        e_tag: BlockID,
        parts: usize,
        blocks: Vec<BlockID>,
    ) -> Result<Object, MetaError> {
        let obj_meta = Object::new(size, e_tag, parts, blocks);
        self.meta_store
            .insert_meta_obj(bucket_name, key, obj_meta.to_vec())?;
        Ok(obj_meta)
    }

    // get meta object from the DB
    pub fn get_object_meta(&self, bucket: &str, key: &str) -> Result<Object, MetaError> {
        self.meta_store.get_meta_obj(bucket, key)
    }

    // create and insert a new  bucket
    pub fn create_bucket(&self, bucket_name: String) -> Result<(), MetaError> {
        let bm = BucketMeta::new(bucket_name.clone());
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

    pub fn key_exists(&self, bucket: &str, key: &str) -> Result<bool, MetaError> {
        let bucket = self.get_bucket(bucket)?;
        bucket.contains_key(key.as_bytes())
    }

    /// Get a list of all buckets in the system.
    pub fn list_buckets(&self) -> Result<Vec<BucketMeta>, MetaError> {
        self.meta_store.list_buckets()
    }

    /// Delete an object from a bucket.
    pub async fn delete_object(&self, bucket: &str, key: &str) -> Result<(), MetaError> {
        let path_map = self.path_tree()?;

        let blocks_to_delete = self.meta_store.delete_objects(bucket, key)?;

        // Now delete all the blocks from disk, and unlink them in the path map.
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
    pub async fn store_object_and_meta(
        &self,
        bucket_name: &str,
        key: &str,
        data: ByteStream,
    ) -> io::Result<(Object, Vec<BlockID>, BlockID, u64)> {
        let (blocks, content_hash, size) = self.store_object(bucket_name, key, data).await?;
        let obj = self
            .create_object_meta(bucket_name, key, size, content_hash, 0, blocks.clone())
            .unwrap();
        Ok((obj, blocks, content_hash, size))
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
        let old_obj_meta = match self.meta_store.get_meta_obj(bucket_name, key) {
            Ok(obj_meta) => Some(obj_meta),
            Err(_) => None,
        };
        let old_obj_meta = Arc::new(old_obj_meta);
        let block_map = Arc::new(self.meta_store.get_block_tree()?);

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
        .zip(stream::repeat((tx, block_map, old_obj_meta)))
        .enumerate()
        .for_each_concurrent(
            5,
            |(idx, (maybe_chunk, (mut tx, block_map, old_obj_meta)))| async move {
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
                let key_has_block = if let Some(obj) = old_obj_meta.as_ref() {
                    obj.has_block(&block_hash)
                } else {
                    false
                };

                let should_write =
                    self.meta_store
                        .write_block_and_path_meta(block_hash, data_len, key_has_block);

                let mut pm = PendingMarker::new(self.metrics.clone());
                match should_write {
                    Err(e) => {
                        if let Err(e) = tx.send(Err(e.into())).await {
                            error!("Could not send transaction error: {}", e);
                        }
                        return;
                    }
                    Ok(false) => {
                        pm.block_ignored();
                        if let Err(e) = tx.send(Ok((idx, block_hash))).await {
                            error!("Could not send block id: {}", e);
                        }
                        return;
                    }
                    Ok(true) => pm.block_pending(),
                };

                // write the actual block
                // first load the block again from the DB
                let block = match block_map.get_block_obj(&block_hash) {
                    Ok(block) => block,
                    Err(e) => {
                        if let Err(e) = tx.send(Err(e.into())).await {
                            pm.block_write_error();
                            error!("Could not send db error: {}", e);
                        }
                        return;
                    }
                };

                let block_path = block.disk_path(self.root.clone());
                if let Err(e) = async_fs::create_dir_all(block_path.parent().unwrap()).await {
                    if let Err(e) = tx.send(Err(e)).await {
                        pm.block_write_error();
                        error!("Could not send path create error: {}", e);
                        return;
                    }
                }
                if let Err(e) = async_fs::write(block_path, &bytes).await {
                    if let Err(e) = tx.send(Err(e)).await {
                        pm.block_write_error();
                        error!("Could not send block write error: {}", e);
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use futures::stream;
    use once_cell::sync::Lazy;
    use rusoto_core::ByteStream;
    use tempfile::tempdir;

    static METRICS: Lazy<SharedMetrics> = Lazy::new(|| SharedMetrics::new());

    fn setup_test_fs() -> (CasFS, tempfile::TempDir) {
        let dir = tempdir().unwrap();
        let meta_path = dir.path().join("meta");
        let metrics = METRICS.clone();

        let fs = CasFS::new(
            dir.path().to_path_buf(),
            meta_path,
            metrics,
            StorageEngine::Fjall,
        );
        (fs, dir)
    }

    #[tokio::test]
    async fn test_store_object() {
        // Setup
        let (fs, _dir) = setup_test_fs();
        let bucket_name = "test_bucket";
        let key1 = "test_key1";
        let key2 = "test_key2";
        fs.create_bucket(bucket_name.to_string()).unwrap();

        // Create ByteStream from test data
        let test_data = b"long test data".repeat(100).to_vec();
        let test_data_2 = test_data.clone();
        let test_data_len = test_data.len();
        let stream = ByteStream::new(stream::once(
            async move { Ok(Bytes::from(test_data.clone())) },
        ));

        // Store object
        let (_, block_ids, _, size) = fs
            .store_object_and_meta(bucket_name, key1, stream)
            .await
            .unwrap();

        // Verify results
        assert_eq!(size, test_data_len as u64);
        assert_eq!(block_ids.len(), 1);

        // Verify block & path was stored
        let block_tree = fs.meta_store.get_block_tree().unwrap();
        let stored_block = block_tree.get_block_obj(&block_ids[0]).unwrap();
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

        let (_, new_blocks, _, _) = fs
            .store_object_and_meta(bucket_name, key2, stream)
            .await
            .unwrap();

        assert_eq!(new_blocks, block_ids);

        let stored_block = block_tree.get_block_obj(&new_blocks[0]).unwrap();
        assert_eq!(stored_block.rc(), 2);
    }

    #[tokio::test]
    async fn test_store_object_refcount() {
        // Setup
        let (fs, _dir) = setup_test_fs();

        let bucket_name = "test_bucket";
        let key1 = "test_key1";
        let key2 = "test_key2";
        fs.create_bucket(bucket_name.to_string()).unwrap();

        // Create ByteStream from test data
        let test_data = b"long test data".repeat(100).to_vec();
        let test_data_2 = test_data.clone();
        let test_data_3 = test_data.clone();
        let stream = ByteStream::new(stream::once(
            async move { Ok(Bytes::from(test_data.clone())) },
        ));

        // Store object
        let (_, block_ids, _, _) = fs
            .store_object_and_meta(bucket_name, key1, stream)
            .await
            .unwrap();

        // Initial refcount must be 1
        let block_tree = fs.meta_store.get_block_tree().unwrap();
        let stored_block = block_tree.get_block_obj(&block_ids[0]).unwrap();
        assert_eq!(stored_block.rc(), 1);

        {
            // Test using  the same key
            // Refcount must not be increased

            let stream =
                ByteStream::new(stream::once(
                    async move { Ok(Bytes::from(test_data_2.clone())) },
                ));

            let (_, new_blocks, _, _) = fs
                .store_object_and_meta(bucket_name, key1, stream)
                .await
                .unwrap();

            assert_eq!(new_blocks, block_ids);

            let stored_block = block_tree.get_block_obj(&new_blocks[0]).unwrap();
            assert_eq!(stored_block.rc(), 1);
        }
        {
            // Test  using a new key
            // Refcount must be increased
            let stream =
                ByteStream::new(stream::once(
                    async move { Ok(Bytes::from(test_data_3.clone())) },
                ));

            let (_, new_blocks, _, _) = fs
                .store_object_and_meta(bucket_name, key2, stream)
                .await
                .unwrap();

            assert_eq!(new_blocks, block_ids);

            let stored_block = block_tree.get_block_obj(&new_blocks[0]).unwrap();
            assert_eq!(stored_block.rc(), 2);
        }
    }

    // test store and delete object
    // - store an object
    // - delete the object
    #[tokio::test]
    async fn test_store_and_delete_object() {
        let (fs, _dir) = setup_test_fs();
        let bucket_name = "test-bucket";
        let key = "test/key";

        // Create bucket
        fs.create_bucket(bucket_name.to_string()).unwrap();

        // Create test data and stream
        let test_data = b"test data".to_vec();
        let stream = ByteStream::new(stream::once(
            async move { Ok(Bytes::from(test_data.clone())) },
        ));

        // Store object
        let (_, block_ids, _, _) = fs
            .store_object_and_meta(bucket_name, key, stream)
            .await
            .unwrap();

        // Verify object exists
        let exists = fs.key_exists(bucket_name, key).unwrap();
        assert_eq!(exists, true);

        // verify blocks and path exist
        let block_tree = fs.meta_store.get_block_tree().unwrap();
        let mut stored_paths = Vec::new();
        for id in block_ids.clone() {
            let block = block_tree.get_block_obj(&id).unwrap();
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
        for id in block_ids {
            assert!(block_tree.get_block_obj(&id).is_err());
        }
        // Verify paths were cleaned up
        for path in stored_paths {
            assert_eq!(fs.path_tree().unwrap().contains_key(&path).unwrap(), false);
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
    #[tokio::test]
    async fn test_store_and_delete_object_with_refcount_same_blocks_diffkey() {
        let (fs, _dir) = setup_test_fs();
        let bucket = "test-bucket";
        let key1 = "test/key1";
        let key2 = "test/key2";

        // Create bucket
        fs.create_bucket(bucket.to_string()).unwrap();

        // Create test data
        let test_data = b"test data".to_vec();
        let test_data2 = test_data.clone();
        let stream1 = ByteStream::new(stream::once(
            async move { Ok(Bytes::from(test_data.clone())) },
        ));

        // Store first object
        let (_, block_ids1, content_hash1, _) = fs
            .store_object_and_meta(bucket, key1, stream1)
            .await
            .unwrap();
        // Verify blocks  exist with rc=1
        let block_tree = fs.meta_store.get_block_tree().unwrap();
        for id in &block_ids1 {
            let block = block_tree.get_block_obj(id).unwrap();
            assert_eq!(block.rc(), 1);
        }

        // Store same data with different key

        let stream2 = ByteStream::new(stream::once(
            async move { Ok(Bytes::from(test_data2.clone())) },
        ));

        let (_, block_ids2, content_hash2, _) = fs
            .store_object_and_meta(bucket, key2, stream2)
            .await
            .unwrap();

        // Verify both objects share same blocks
        assert_eq!(block_ids1, block_ids2);
        assert_eq!(content_hash1, content_hash2);
        // Verify blocks  exist with rc=2
        let block_tree = fs.meta_store.get_block_tree().unwrap();
        for id in &block_ids2 {
            let block = block_tree.get_block_obj(id).unwrap();
            assert_eq!(block.rc(), 2);
        }

        // Delete first object
        fs.delete_object(bucket, key1).await.unwrap();

        // Verify blocks still exist
        let block_tree = fs.meta_store.get_block_tree().unwrap();
        for id in &block_ids1 {
            let block = block_tree.get_block_obj(id).unwrap();
            assert_eq!(block.rc(), 1);
        }

        // Delete second object
        fs.delete_object(bucket, key2).await.unwrap();

        // Verify blocks are gone
        for id in block_ids1 {
            assert!(block_tree.get_block_obj(&id).is_err());
        }
    }

    // Test storing and deleting an object with refcount
    // - store object
    //       refcount == 1
    // - store object again with differrent key
    //      refcount == 1
    // - delete the object
    // - check block/disk/whatever should be gone
    #[tokio::test]
    async fn test_store_and_delete_object_with_refcount_same_blocks_samekey() {
        let (fs, _dir) = setup_test_fs();
        let bucket = "test-bucket";
        let key1 = "test/key1";

        // Create bucket
        fs.create_bucket(bucket.to_string()).unwrap();

        // Create test data
        let test_data = b"test data".to_vec();
        let test_data2 = test_data.clone();
        let stream1 = ByteStream::new(stream::once(
            async move { Ok(Bytes::from(test_data.clone())) },
        ));

        // Store first object
        let (_, block_ids1, content_hash1, _) = fs
            .store_object_and_meta(bucket, key1, stream1)
            .await
            .unwrap();
        // Verify blocks  exist with rc=1
        let block_tree = fs.meta_store.get_block_tree().unwrap();
        for id in &block_ids1 {
            let block = block_tree.get_block_obj(id).unwrap();
            assert_eq!(block.rc(), 1);
        }

        // Store same data with same key

        let stream2 = ByteStream::new(stream::once(
            async move { Ok(Bytes::from(test_data2.clone())) },
        ));

        let (_, block_ids2, content_hash2, _) = fs
            .store_object_and_meta(bucket, key1, stream2)
            .await
            .unwrap();

        // Verify both objects share same blocks
        assert_eq!(block_ids1, block_ids2);
        assert_eq!(content_hash1, content_hash2);
        // Verify blocks  exist with rc=2
        let block_tree = fs.meta_store.get_block_tree().unwrap();
        for id in &block_ids2 {
            let block = block_tree.get_block_obj(id).unwrap();
            assert_eq!(block.rc(), 1);
        }

        // Delete object
        fs.delete_object(bucket, key1).await.unwrap();

        // Verify blocks are gone
        for id in block_ids1 {
            assert!(block_tree.get_block_obj(&id).is_err());
        }
    }
}
