use std::io;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use super::fs::{CasFS, ObjectPaths};
use super::multipart::MultiPart;
use crate::metastore::{
    BlockID, BlockTree, BucketMeta, MetaError, MetaTreeExt, Object, ObjectData,
};
use rusoto_core::ByteStream;

/// A trait that defines the methods of CasFS that we want to mock
#[async_trait::async_trait]
pub trait CasFSTrait: Send + Sync {
    fn block_tree(&self) -> Result<BlockTree, MetaError>;
    fn bucket_exists(&self, bucket_name: &str) -> Result<bool, MetaError>;
    fn get_object_meta(&self, bucket_name: &str, key: &str) -> Result<Option<Object>, MetaError>;
    fn get_object_paths(
        &self,
        bucket_name: &str,
        key: &str,
    ) -> Result<Option<ObjectPaths>, MetaError>;
    fn create_bucket(&self, bucket_name: &str) -> Result<(), MetaError>;
    async fn bucket_delete(&self, bucket_name: &str) -> Result<(), MetaError>;
    fn key_exists(&self, bucket: &str, key: &str) -> Result<bool, MetaError>;
    fn list_buckets(&self) -> Result<Vec<BucketMeta>, MetaError>;
    async fn delete_object(&self, bucket: &str, key: &str) -> Result<(), MetaError>;
    async fn store_single_object_and_meta(
        &self,
        bucket_name: &str,
        key: &str,
        data: ByteStream,
    ) -> io::Result<Object>;
    async fn store_object(
        &self,
        bucket_name: &str,
        key: &str,
        data: ByteStream,
    ) -> io::Result<(Vec<BlockID>, BlockID, u64)>;
    fn store_inlined_object(
        &self,
        bucket_name: &str,
        key: &str,
        data: Vec<u8>,
    ) -> Result<Object, MetaError>;
    fn get_bucket(
        &self,
        bucket_name: &str,
    ) -> Result<Box<dyn MetaTreeExt + Send + Sync>, MetaError>;
    fn fs_root(&self) -> &PathBuf;
    fn max_inlined_data_length(&self) -> usize;
    fn create_object_meta(
        &self,
        bucket_name: &str,
        key: &str,
        size: u64,
        hash: BlockID,
        object_data: ObjectData,
    ) -> Result<Object, MetaError>;
    fn get_multipart_part(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        part_number: i64,
    ) -> Result<Option<MultiPart>, MetaError>;
    fn remove_multipart_part(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        part_number: i64,
    ) -> Result<(), MetaError>;
    #[allow(clippy::too_many_arguments)]
    fn insert_multipart_part(
        &self,
        bucket: String,
        key: String,
        size: usize,
        part_number: i64,
        upload_id: String,
        hash: BlockID,
        blocks: Vec<BlockID>,
    ) -> Result<(), MetaError>;
}

/// Implementation of the trait for the real CasFS
#[async_trait::async_trait]
impl CasFSTrait for CasFS {
    fn block_tree(&self) -> Result<BlockTree, MetaError> {
        CasFS::block_tree(self)
    }

    fn bucket_exists(&self, bucket_name: &str) -> Result<bool, MetaError> {
        CasFS::bucket_exists(self, bucket_name)
    }

    fn get_object_meta(&self, bucket_name: &str, key: &str) -> Result<Option<Object>, MetaError> {
        CasFS::get_object_meta(self, bucket_name, key)
    }

    fn get_object_paths(
        &self,
        bucket_name: &str,
        key: &str,
    ) -> Result<Option<ObjectPaths>, MetaError> {
        CasFS::get_object_paths(self, bucket_name, key)
    }

    fn create_bucket(&self, bucket_name: &str) -> Result<(), MetaError> {
        CasFS::create_bucket(self, bucket_name)
    }

    async fn bucket_delete(&self, bucket_name: &str) -> Result<(), MetaError> {
        CasFS::bucket_delete(self, bucket_name).await
    }

    fn key_exists(&self, bucket: &str, key: &str) -> Result<bool, MetaError> {
        CasFS::key_exists(self, bucket, key)
    }

    fn list_buckets(&self) -> Result<Vec<BucketMeta>, MetaError> {
        CasFS::list_buckets(self)
    }

    async fn delete_object(&self, bucket: &str, key: &str) -> Result<(), MetaError> {
        CasFS::delete_object(self, bucket, key).await
    }

    async fn store_single_object_and_meta(
        &self,
        bucket_name: &str,
        key: &str,
        data: ByteStream,
    ) -> io::Result<Object> {
        CasFS::store_single_object_and_meta(self, bucket_name, key, data).await
    }

    async fn store_object(
        &self,
        bucket_name: &str,
        key: &str,
        data: ByteStream,
    ) -> io::Result<(Vec<BlockID>, BlockID, u64)> {
        CasFS::store_object(self, bucket_name, key, data).await
    }

    fn store_inlined_object(
        &self,
        bucket_name: &str,
        key: &str,
        data: Vec<u8>,
    ) -> Result<Object, MetaError> {
        CasFS::store_inlined_object(self, bucket_name, key, data)
    }

    fn get_bucket(
        &self,
        bucket_name: &str,
    ) -> Result<Box<dyn MetaTreeExt + Send + Sync>, MetaError> {
        CasFS::get_bucket(self, bucket_name)
    }

    fn fs_root(&self) -> &PathBuf {
        CasFS::fs_root(self)
    }

    fn max_inlined_data_length(&self) -> usize {
        CasFS::max_inlined_data_length(self)
    }
    fn create_object_meta(
        &self,
        bucket_name: &str,
        key: &str,
        size: u64,
        hash: BlockID,
        object_data: ObjectData,
    ) -> Result<Object, MetaError> {
        CasFS::create_object_meta(self, bucket_name, key, size, hash, object_data)
    }
    fn get_multipart_part(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        part_number: i64,
    ) -> Result<Option<MultiPart>, MetaError> {
        CasFS::get_multipart_part(self, bucket, key, upload_id, part_number)
    }

    fn remove_multipart_part(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        part_number: i64,
    ) -> Result<(), MetaError> {
        CasFS::remove_multipart_part(self, bucket, key, upload_id, part_number)
    }
    fn insert_multipart_part(
        &self,
        bucket: String,
        key: String,
        size: usize,
        part_number: i64,
        upload_id: String,
        hash: BlockID,
        blocks: Vec<BlockID>,
    ) -> Result<(), MetaError> {
        CasFS::insert_multipart_part(self, bucket, key, size, part_number, upload_id, hash, blocks)
    }
}

/// A mock implementation of CasFS for testing
pub struct MockCasFS {
    // Control which methods should fail
    pub block_tree_fails: Arc<Mutex<bool>>,
    pub bucket_exists_fails: Arc<Mutex<bool>>,
    pub get_object_meta_fails: Arc<Mutex<bool>>,
    pub get_object_paths_fails: Arc<Mutex<bool>>,
    pub create_bucket_fails: Arc<Mutex<bool>>,
    pub bucket_delete_fails: Arc<Mutex<bool>>,
    pub key_exists_fails: Arc<Mutex<bool>>,
    pub list_buckets_fails: Arc<Mutex<bool>>,
    pub delete_object_fails: Arc<Mutex<bool>>,
    pub store_single_object_and_meta_fails: Arc<Mutex<bool>>,
    pub store_object_fails: Arc<Mutex<bool>>,
    pub store_inlined_object_fails: Arc<Mutex<bool>>,
    pub get_bucket_fails: Arc<Mutex<bool>>,

    // Optional real implementation to delegate to when not failing
    real_casfs: Option<CasFS>,

    // Mock data for return values
    root_path: PathBuf,
    max_inlined_length: usize,
}

impl Default for MockCasFS {
    fn default() -> Self {
        Self::new()
    }
}

impl MockCasFS {
    /// Create a new MockCasFS with all methods set to succeed
    pub fn new() -> Self {
        Self {
            block_tree_fails: Arc::new(Mutex::new(false)),
            bucket_exists_fails: Arc::new(Mutex::new(false)),
            get_object_meta_fails: Arc::new(Mutex::new(false)),
            get_object_paths_fails: Arc::new(Mutex::new(false)),
            create_bucket_fails: Arc::new(Mutex::new(false)),
            bucket_delete_fails: Arc::new(Mutex::new(false)),
            key_exists_fails: Arc::new(Mutex::new(false)),
            list_buckets_fails: Arc::new(Mutex::new(false)),
            delete_object_fails: Arc::new(Mutex::new(false)),
            store_single_object_and_meta_fails: Arc::new(Mutex::new(false)),
            store_object_fails: Arc::new(Mutex::new(false)),
            store_inlined_object_fails: Arc::new(Mutex::new(false)),
            get_bucket_fails: Arc::new(Mutex::new(false)),
            real_casfs: None,
            root_path: PathBuf::from("/mock/path"),
            max_inlined_length: 1024,
        }
    }

    /// Create a new MockCasFS that delegates to a real CasFS when not failing
    pub fn with_real_casfs(casfs: CasFS) -> Self {
        let root_path = casfs.fs_root().clone();
        let max_inlined_length = casfs.max_inlined_data_length();

        Self {
            block_tree_fails: Arc::new(Mutex::new(false)),
            bucket_exists_fails: Arc::new(Mutex::new(false)),
            get_object_meta_fails: Arc::new(Mutex::new(false)),
            get_object_paths_fails: Arc::new(Mutex::new(false)),
            create_bucket_fails: Arc::new(Mutex::new(false)),
            bucket_delete_fails: Arc::new(Mutex::new(false)),
            key_exists_fails: Arc::new(Mutex::new(false)),
            list_buckets_fails: Arc::new(Mutex::new(false)),
            delete_object_fails: Arc::new(Mutex::new(false)),
            store_single_object_and_meta_fails: Arc::new(Mutex::new(false)),
            store_object_fails: Arc::new(Mutex::new(false)),
            store_inlined_object_fails: Arc::new(Mutex::new(false)),
            get_bucket_fails: Arc::new(Mutex::new(false)),
            real_casfs: Some(casfs),
            root_path,
            max_inlined_length,
        }
    }

    /// Set a method to fail
    pub fn set_method_fails(&self, method_name: &str, fails: bool) {
        match method_name {
            "block_tree" => *self.block_tree_fails.lock().unwrap() = fails,
            "bucket_exists" => *self.bucket_exists_fails.lock().unwrap() = fails,
            "get_object_meta" => *self.get_object_meta_fails.lock().unwrap() = fails,
            "get_object_paths" => *self.get_object_paths_fails.lock().unwrap() = fails,
            "create_bucket" => *self.create_bucket_fails.lock().unwrap() = fails,
            "bucket_delete" => *self.bucket_delete_fails.lock().unwrap() = fails,
            "key_exists" => *self.key_exists_fails.lock().unwrap() = fails,
            "list_buckets" => *self.list_buckets_fails.lock().unwrap() = fails,
            "delete_object" => *self.delete_object_fails.lock().unwrap() = fails,
            "store_single_object_and_meta" => {
                *self.store_single_object_and_meta_fails.lock().unwrap() = fails
            }
            "store_object" => *self.store_object_fails.lock().unwrap() = fails,
            "store_inlined_object" => *self.store_inlined_object_fails.lock().unwrap() = fails,
            "get_bucket" => *self.get_bucket_fails.lock().unwrap() = fails,
            _ => panic!("Unknown method name: {}", method_name),
        }
    }
}

#[async_trait::async_trait]
impl CasFSTrait for MockCasFS {
    fn block_tree(&self) -> Result<BlockTree, MetaError> {
        if *self.block_tree_fails.lock().unwrap() {
            Err(MetaError::InsertError("Mock block_tree failure".into()))
        } else if let Some(casfs) = &self.real_casfs {
            casfs.block_tree()
        } else {
            Err(MetaError::InsertError(
                "No real CasFS and not configured to fail".into(),
            ))
        }
    }

    fn bucket_exists(&self, bucket_name: &str) -> Result<bool, MetaError> {
        if *self.bucket_exists_fails.lock().unwrap() {
            Err(MetaError::InsertError("Mock bucket_exists failure".into()))
        } else if let Some(casfs) = &self.real_casfs {
            casfs.bucket_exists(bucket_name)
        } else {
            Ok(false)
        }
    }

    fn get_object_meta(&self, bucket_name: &str, key: &str) -> Result<Option<Object>, MetaError> {
        if *self.get_object_meta_fails.lock().unwrap() {
            Err(MetaError::InsertError(
                "Mock get_object_meta failure".into(),
            ))
        } else if let Some(casfs) = &self.real_casfs {
            casfs.get_object_meta(bucket_name, key)
        } else {
            Ok(None)
        }
    }

    fn get_object_paths(
        &self,
        bucket_name: &str,
        key: &str,
    ) -> Result<Option<ObjectPaths>, MetaError> {
        if *self.get_object_paths_fails.lock().unwrap() {
            Err(MetaError::InsertError(
                "Mock get_object_paths failure".into(),
            ))
        } else if let Some(casfs) = &self.real_casfs {
            casfs.get_object_paths(bucket_name, key)
        } else {
            Ok(None)
        }
    }

    fn create_bucket(&self, bucket_name: &str) -> Result<(), MetaError> {
        if *self.create_bucket_fails.lock().unwrap() {
            Err(MetaError::InsertError("Mock create_bucket failure".into()))
        } else if let Some(casfs) = &self.real_casfs {
            casfs.create_bucket(bucket_name)
        } else {
            Ok(())
        }
    }

    async fn bucket_delete(&self, bucket_name: &str) -> Result<(), MetaError> {
        if *self.bucket_delete_fails.lock().unwrap() {
            Err(MetaError::InsertError("Mock bucket_delete failure".into()))
        } else if let Some(casfs) = &self.real_casfs {
            casfs.bucket_delete(bucket_name).await
        } else {
            Ok(())
        }
    }

    fn key_exists(&self, bucket: &str, key: &str) -> Result<bool, MetaError> {
        if *self.key_exists_fails.lock().unwrap() {
            Err(MetaError::InsertError("Mock key_exists failure".into()))
        } else if let Some(casfs) = &self.real_casfs {
            casfs.key_exists(bucket, key)
        } else {
            Ok(false)
        }
    }

    fn list_buckets(&self) -> Result<Vec<BucketMeta>, MetaError> {
        if *self.list_buckets_fails.lock().unwrap() {
            Err(MetaError::InsertError("Mock list_buckets failure".into()))
        } else if let Some(casfs) = &self.real_casfs {
            casfs.list_buckets()
        } else {
            Ok(vec![])
        }
    }

    async fn delete_object(&self, bucket: &str, key: &str) -> Result<(), MetaError> {
        if *self.delete_object_fails.lock().unwrap() {
            Err(MetaError::InsertError("Mock delete_object failure".into()))
        } else if let Some(casfs) = &self.real_casfs {
            casfs.delete_object(bucket, key).await
        } else {
            Ok(())
        }
    }

    async fn store_single_object_and_meta(
        &self,
        bucket_name: &str,
        key: &str,
        data: ByteStream,
    ) -> io::Result<Object> {
        if *self.store_single_object_and_meta_fails.lock().unwrap() {
            Err(io::Error::new(
                io::ErrorKind::Other,
                "Mock store_single_object_and_meta failure",
            ))
        } else if let Some(casfs) = &self.real_casfs {
            casfs
                .store_single_object_and_meta(bucket_name, key, data)
                .await
        } else {
            Err(io::Error::new(
                io::ErrorKind::Other,
                "No real CasFS and not configured to fail",
            ))
        }
    }

    async fn store_object(
        &self,
        bucket_name: &str,
        key: &str,
        data: ByteStream,
    ) -> io::Result<(Vec<BlockID>, BlockID, u64)> {
        if *self.store_object_fails.lock().unwrap() {
            Err(io::Error::new(
                io::ErrorKind::Other,
                "Mock store_object failure",
            ))
        } else if let Some(casfs) = &self.real_casfs {
            casfs.store_object(bucket_name, key, data).await
        } else {
            Err(io::Error::new(
                io::ErrorKind::Other,
                "No real CasFS and not configured to fail",
            ))
        }
    }

    fn store_inlined_object(
        &self,
        bucket_name: &str,
        key: &str,
        data: Vec<u8>,
    ) -> Result<Object, MetaError> {
        if *self.store_inlined_object_fails.lock().unwrap() {
            Err(MetaError::InsertError(
                "Mock store_inlined_object failure".into(),
            ))
        } else if let Some(casfs) = &self.real_casfs {
            casfs.store_inlined_object(bucket_name, key, data)
        } else {
            Err(MetaError::InsertError(
                "No real CasFS and not configured to fail".into(),
            ))
        }
    }

    fn get_bucket(
        &self,
        bucket_name: &str,
    ) -> Result<Box<dyn MetaTreeExt + Send + Sync>, MetaError> {
        if *self.get_bucket_fails.lock().unwrap() {
            Err(MetaError::InsertError("Mock get_bucket failure".into()))
        } else if let Some(casfs) = &self.real_casfs {
            casfs.get_bucket(bucket_name)
        } else {
            Err(MetaError::InsertError(
                "No real CasFS and not configured to fail".into(),
            ))
        }
    }

    fn fs_root(&self) -> &PathBuf {
        if let Some(casfs) = &self.real_casfs {
            casfs.fs_root()
        } else {
            &self.root_path
        }
    }

    fn max_inlined_data_length(&self) -> usize {
        if let Some(casfs) = &self.real_casfs {
            casfs.max_inlined_data_length()
        } else {
            self.max_inlined_length
        }
    }
    fn create_object_meta(
        &self,
        bucket_name: &str,
        key: &str,
        size: u64,
        hash: BlockID,
        object_data: ObjectData,
    ) -> Result<Object, MetaError> {
        if let Some(casfs) = &self.real_casfs {
            casfs.create_object_meta(bucket_name, key, size, hash, object_data)
        } else {
            Err(MetaError::InsertError(
                "No real CasFS and not configured to fail".into(),
            ))
        }
    }
    fn get_multipart_part(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        part_number: i64,
    ) -> Result<Option<MultiPart>, MetaError> {
        if let Some(casfs) = &self.real_casfs {
            casfs.get_multipart_part(bucket, key, upload_id, part_number)
        } else {
            Ok(None)
        }
    }

    fn remove_multipart_part(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        part_number: i64,
    ) -> Result<(), MetaError> {
        if let Some(casfs) = &self.real_casfs {
            casfs.remove_multipart_part(bucket, key, upload_id, part_number)
        } else {
            Ok(())
        }
    }
    fn insert_multipart_part(
        &self,
        bucket: String,
        key: String,
        size: usize,
        part_number: i64,
        upload_id: String,
        hash: BlockID,
        blocks: Vec<BlockID>,
    ) -> Result<(), MetaError> {
        if let Some(casfs) = &self.real_casfs {
            casfs.insert_multipart_part(bucket, key, size, part_number, upload_id, hash, blocks)
        } else {
            Ok(())
        }
    }
}
