use crate::metastore::{BlockID, BucketMeta, MetaError, Object, ObjectData, BucketTreeExt};

pub trait TestStore {
    fn insert_bucket(&self, bucket_name: &str, raw_bucket: Vec<u8>) -> Result<(), MetaError>;
    fn bucket_exists(&self, bucket_name: &str) -> Result<bool, MetaError>;
    fn list_buckets(&self) -> Result<Vec<BucketMeta>, MetaError>;
    fn insert_meta(&self, bucket_name: &str, key: &str, raw_obj: Vec<u8>) -> Result<(), MetaError>;
    fn get_meta(&self, bucket_name: &str, key: &str) -> Result<Option<Object>, MetaError>;
    fn get_bucket_ext(
        &self,
        name: &str,
    ) -> Result<Box<dyn BucketTreeExt + Send + Sync>, MetaError>;
}

pub fn test_errors(store: &impl TestStore) {
    // Test nonexistent bucket
    assert_eq!(store.bucket_exists("nonexistent").unwrap(), false);

    // Test nonexistent object
    let bucket_name = "test-bucket";
    let bucket_meta = BucketMeta::new(bucket_name.to_string());
    store
        .insert_bucket(bucket_name, bucket_meta.to_vec())
        .unwrap();
    assert!(store
        .get_meta(bucket_name, "nonexistent")
        .unwrap()
        .is_none());
}

pub fn test_bucket_operations(store: &impl TestStore) {
    // Test bucket creation
    let bucket_name1 = "test-bucket";
    let bucket_name2 = "test-bucket2";
    let bucket_meta = BucketMeta::new(bucket_name1.to_string());
    store
        .insert_bucket(bucket_name1, bucket_meta.to_vec())
        .unwrap();
    store
        .insert_bucket(
            bucket_name2,
            BucketMeta::new(bucket_name2.to_string()).to_vec(),
        )
        .unwrap();

    // Verify bucket exists
    assert_eq!(store.bucket_exists(bucket_name1).unwrap(), true);
    assert_eq!(store.bucket_exists(bucket_name2).unwrap(), true);

    // Test bucket listing
    let buckets = store.list_buckets().unwrap();
    assert_eq!(buckets.len(), 2);
    assert_eq!(buckets[0].name(), bucket_name1);
    assert_eq!(buckets[1].name(), bucket_name2);
}

pub fn test_object_operations(store: &impl TestStore) {
    let bucket_name = "test-bucket";
    let key = "test-bucket/key";

    // Setup bucket first
    let bucket_meta = BucketMeta::new(bucket_name.to_string());
    store
        .insert_bucket(bucket_name, bucket_meta.to_vec())
        .unwrap();

    // Test object insertion
    let test_obj = Object::new(
        1024,                   // 1KB object
        BlockID::from([1; 16]), // Sample ETag
        ObjectData::SinglePart {
            blocks: vec![BlockID::from([1; 16])],
        },
    );
    store
        .insert_meta(&bucket_name, key, test_obj.to_vec())
        .unwrap();

    // Test object retrieval
    let retrieved_obj = store.get_meta(&bucket_name, key).unwrap().unwrap();
    assert_eq!(retrieved_obj.blocks().len(), 1);
    assert_eq!(retrieved_obj.blocks()[0], BlockID::from([1; 16]));

    // Test error cases of object retrieval
    assert!(store
        .get_meta(bucket_name, "nonexistent-key")
        .unwrap()
        .is_none());
}

pub fn test_get_bucket_keys(store: &impl TestStore) {
    let bucket_name = "testbucketkeys";

    // Setup bucket
    let bucket_meta = BucketMeta::new(bucket_name.to_string());
    store
        .insert_bucket(bucket_name, bucket_meta.to_vec())
        .unwrap();

    // Insert test objects
    let test_keys = vec!["a", "b", "c"];
    for key in &test_keys {
        let obj = Object::new(
            1024,
            BlockID::from([1; 16]),
            ObjectData::SinglePart {
                blocks: vec![BlockID::from([1; 16])],
            },
        );
        store.insert_meta(bucket_name, key, obj.to_vec()).unwrap();
    }

    let bucket = store.get_bucket_ext(bucket_name).unwrap();

    let retrieved_keys: Vec<String> = bucket
        .get_bucket_keys()
        .into_iter()
        .map(|k| String::from_utf8(k.unwrap()).unwrap())
        .collect();

    // Verify all keys present
    assert_eq!(retrieved_keys.len(), test_keys.len());
    for key in retrieved_keys {
        assert!(test_keys.contains(&key.as_str()));
    }

    // Test empty bucket
    let empty_bucket = "empty-bucket";
    store
        .insert_bucket(
            empty_bucket,
            BucketMeta::new(empty_bucket.to_string()).to_vec(),
        )
        .unwrap();
    let empty = store.get_bucket_ext(empty_bucket).unwrap();
    assert_eq!(empty.get_bucket_keys().count(), 0);
}

pub fn test_range_filter(store: &impl TestStore) {
    let bucket_name = "test-bucket";

    // Setup bucket
    let bucket_meta = BucketMeta::new(bucket_name.to_string());
    store
        .insert_bucket(bucket_name, bucket_meta.to_vec())
        .unwrap();

    // Insert test objects with unordered keys
    let test_data = vec![
        ("c/1", "data5"),
        ("b/2", "data4"),
        ("a/1", "data1"),
        ("b/1", "data3"),
        ("a/2", "data2"),
    ];

    for (key, data) in &test_data {
        let obj = Object::new(
            data.len() as u64,
            BlockID::from([1; 16]),
            ObjectData::SinglePart {
                blocks: vec![BlockID::from([1; 16])],
            },
        );
        store.insert_meta(bucket_name, key, obj.to_vec()).unwrap();
    }

    let bucket = store.get_bucket_ext(bucket_name).unwrap();

    // Test cases
    {
        // 1. No filters
        let results: Vec<_> = bucket
            .range_filter(None, None, None)
            .map(|(k, _)| k)
            .collect();
        assert_eq!(results.len(), 5);
        assert_eq!(results[0], "a/1");
    }

    {
        // 2. With start_after
        let results: Vec<_> = bucket
            .range_filter(Some("a/2".to_string()), None, None)
            .map(|(k, _)| k)
            .collect();
        assert_eq!(results.len(), 3);
        assert_eq!(results[0], "b/1");
    }

    {
        // 3. With prefix
        let results: Vec<_> = bucket
            .range_filter(None, Some("b".to_string()), None)
            .map(|(k, _)| k)
            .collect();
        assert_eq!(results.len(), 2);
        assert!(results.iter().all(|k| k.starts_with("b/")));
    }

    {
        // 4. With continuation token
        let results: Vec<_> = bucket
            .range_filter(None, None, Some("b/1".to_string()))
            .map(|(k, _)| k)
            .collect();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0], "b/2");
    }

    {
        // 5. With both start_after and continuation token
        let results: Vec<_> = bucket
            .range_filter(Some("b/1".to_string()), None, Some("a/2".to_string()))
            .map(|(k, _)| k)
            .collect();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0], "b/2");
    }
    {
        // if start_after/continuation_token is greater than prefix, return empty

        // it is clearly greater than prefix
        let results: Vec<_> = bucket
            .range_filter(None, Some("b".to_string()), Some("c".to_string()))
            .map(|(k, _)| k)
            .collect();

        assert_eq!(results.len(), 0);

        // token < prefix, can be discarded
        let results: Vec<_> = bucket
            .range_filter(None, Some("b/".to_string()), Some("b".to_string()))
            .map(|(k, _)| k)
            .collect();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0], "b/1");
        assert_eq!(results[1], "b/2");

        // token has prefix, token > prefix
        let results: Vec<_> = bucket
            .range_filter(None, Some("b/".to_string()), Some("b/0".to_string()))
            .map(|(k, _)| k)
            .collect();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0], "b/1");
        assert_eq!(results[1], "b/2");

        // token has prefix, token > prefix
        let results: Vec<_> = bucket
            .range_filter(None, Some("b/".to_string()), Some("b/1".to_string()))
            .map(|(k, _)| k)
            .collect();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0], "b/2");
    }
}