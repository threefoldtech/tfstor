use std::io::{self, ErrorKind};

use bytes::Bytes;
use faster_hex::{hex_decode, hex_string};
use futures::Stream;
use futures::StreamExt;
use md5::{Digest, Md5};
use tracing::error;
use tracing::info;
use uuid::Uuid;

use rusoto_core::ByteStream;
use s3s::dto::*;
use s3s::s3_error;

use s3s::S3Request;
use s3s::S3Response;
use s3s::S3Result;
use s3s::S3;

use crate::cas::{
    block_stream::BlockStream,
    mock_casfs::CasFSTrait,
    range_request::{parse_range_request, RangeRequest},
};
use crate::metastore::{BlockID, ObjectData};
use crate::metrics::SharedMetrics;

use std::default::Default;

const MAX_KEYS: i32 = 1000;

#[derive(Debug)]
pub struct S3FS<T: CasFSTrait + 'static> {
    casfs: T,
    metrics: SharedMetrics,
}

impl<T: CasFSTrait + 'static> S3FS<T> {
    pub fn new(casfs: T, metrics: SharedMetrics) -> Self {
        // Get the current amount of buckets
        // FIXME: This is a bit of a hack, we should have a better way to get the amount of buckets
        metrics.set_bucket_count(1); //db.open_tree(BUCKET_META_TREE).unwrap().len());

        Self { casfs, metrics }
    }

    // Compute the e_tag of the multpart upload. Per the S3 standard (according to minio), the
    // e_tag of a multipart uploaded object is the Md5 of the Md5 of the parts.
    fn calculate_multipart_hash(&self, blocks: &[BlockID]) -> io::Result<([u8; 16], usize)> {
        let mut hasher = Md5::new();
        let mut size = 0;
        let block_map = self.casfs.block_tree()?;

        for block in blocks {
            let block_info = match block_map.get_block(block).expect("Block data is corrupt") {
                Some(block_info) => block_info,
                None => {
                    return Err(io::Error::new(io::ErrorKind::NotFound, "Block  not found"));
                }
            };
            size += block_info.size();
            hasher.update(block);
        }

        Ok((hasher.finalize().into(), size))
    }
}

fn fmt_content_range(start: u64, end_inclusive: u64, size: u64) -> String {
    format!("bytes {start}-{end_inclusive}/{size}")
}

#[async_trait::async_trait]
impl<T: CasFSTrait + 'static> S3 for S3FS<T> {
    async fn complete_multipart_upload(
        &self,
        req: S3Request<CompleteMultipartUploadInput>,
    ) -> S3Result<S3Response<CompleteMultipartUploadOutput>> {
        let CompleteMultipartUploadInput {
            multipart_upload,
            bucket,
            key,
            upload_id,
            ..
        } = req.input;

        let multipart_upload = if let Some(multipart_upload) = multipart_upload {
            multipart_upload
        } else {
            let err = s3_error!(InvalidPart, "Missing multipart_upload");
            return Err(err);
        };

        let mut blocks = vec![];
        let mut cnt: i32 = 0;
        for part in multipart_upload.parts.iter().flatten() {
            // validate part number
            let part_number = try_!(part
                .part_number
                .ok_or_else(|| { io::Error::new(io::ErrorKind::NotFound, "Missing part_number") }));
            cnt = cnt.wrapping_add(1);
            if part_number != cnt {
                try_!(Err(io::Error::new(
                    io::ErrorKind::Other,
                    "InvalidPartOrder"
                )));
            }

            let result =
                self.casfs
                    .get_multipart_part(&bucket, &key, &upload_id, part_number as i64);
            let mp = match result {
                Ok(Some(mp)) => mp,
                Ok(None) => {
                    error!(
                        "Missing part \"{}\" in multipart upload: part not found",
                        part_number
                    );
                    return Err(s3_error!(InvalidArgument, "Part not uploaded"));
                }
                Err(e) => {
                    error!(
                        "Missing part \"{}\" in multipart upload: {}",
                        part_number, e
                    );
                    return Err(s3_error!(InvalidArgument, "Part not uploaded"));
                }
            };
            blocks.extend_from_slice(mp.blocks());
        }

        let (content_hash, size) = try_!(self.calculate_multipart_hash(&blocks));

        let object_meta = try_!(self.casfs.create_object_meta(
            &bucket,
            &key,
            size as u64,
            content_hash,
            ObjectData::MultiPart {
                blocks,
                parts: cnt as usize
            },
        ));

        // Try to delete the multipart metadata. If this fails, it is not really an issue.
        for part in multipart_upload.parts.into_iter().flatten() {
            if let Err(e) = self.casfs.remove_multipart_part(
                &bucket,
                &key,
                &upload_id,
                part.part_number.unwrap() as i64,
            ) {
                error!("Could not remove part: {}", e);
            };
        }

        let output = CompleteMultipartUploadOutput {
            bucket: Some(bucket),
            key: Some(key),
            e_tag: Some(object_meta.format_e_tag()),
            ..Default::default()
        };
        Ok(S3Response::new(output))
    }

    async fn copy_object(
        &self,
        _: S3Request<CopyObjectInput>,
    ) -> S3Result<S3Response<CopyObjectOutput>> {
        // see https://github.com/threefoldtech/s3-cas/blob/bee016998a4167b781082e1897072af0a64992e2/src/cas/fs.rs#L522-L560
        // which i'm not sure if it's implemented correctly
        Err(s3_error!(NotImplemented))
    }

    async fn create_bucket(
        &self,
        req: S3Request<CreateBucketInput>,
    ) -> S3Result<S3Response<CreateBucketOutput>> {
        let input = req.input;

        info!("create bucket");
        if try_!(self.casfs.bucket_exists(&input.bucket)) {
            return Err(s3_error!(
                BucketAlreadyExists,
                "A bucket with this name already exists"
            ));
        }

        try_!(self.casfs.create_bucket(&input.bucket));

        self.metrics.inc_bucket_count();

        let output = CreateBucketOutput::default(); // TODO: handle other fields
        Ok(S3Response::new(output))
    }

    // create_multipart_upload doesn't do any bookkeeping, it just do some checking
    // and returns an upload id
    async fn create_multipart_upload(
        &self,
        req: S3Request<CreateMultipartUploadInput>,
    ) -> S3Result<S3Response<CreateMultipartUploadOutput>> {
        let CreateMultipartUploadInput { bucket, key, .. } = req.input;

        if !try_!(self.casfs.bucket_exists(&bucket)) {
            return Err(s3_error!(NoSuchBucket, "Bucket does not exist"));
        }

        let upload_id = Uuid::new_v4().to_string();

        let output = CreateMultipartUploadOutput {
            bucket: Some(bucket),
            key: Some(key),
            upload_id: Some(upload_id.to_string()),
            ..Default::default()
        };

        Ok(S3Response::new(output))
    }

    async fn delete_bucket(
        &self,
        req: S3Request<DeleteBucketInput>,
    ) -> S3Result<S3Response<DeleteBucketOutput>> {
        let DeleteBucketInput { bucket, .. } = req.input;

        try_!(self.casfs.bucket_delete(&bucket).await);

        self.metrics.dec_bucket_count();

        Ok(S3Response::new(DeleteBucketOutput {}))
    }

    async fn delete_object(
        &self,
        req: S3Request<DeleteObjectInput>,
    ) -> S3Result<S3Response<DeleteObjectOutput>> {
        info!("DELETE OBJECT: {:?}", req.input);

        let DeleteObjectInput { bucket, key, .. } = req.input;

        if !try_!(self.casfs.key_exists(&bucket, &key)) {
            return Err(s3_error!(NoSuchKey, "Key does not exist"));
        }

        // TODO: check for the key existence?
        try_!(self.casfs.delete_object(&bucket, &key).await);

        let output = DeleteObjectOutput::default(); // TODO: handle other fields
        Ok(S3Response::new(output))
    }

    async fn delete_objects(
        &self,
        req: S3Request<DeleteObjectsInput>,
    ) -> S3Result<S3Response<DeleteObjectsOutput>> {
        info!("DELETE OBJECTS: {:?}", req.input);

        let DeleteObjectsInput { bucket, delete, .. } = req.input;

        if !try_!(self.casfs.bucket_exists(&bucket)) {
            return Err(s3_error!(NoSuchBucket, "Bucket does not exist"));
        }

        let mut deleted_objects = Vec::with_capacity(delete.objects.len());
        let errors = Vec::new();

        for object in delete.objects {
            match self.casfs.delete_object(&bucket, &object.key).await {
                Ok(_) => {
                    deleted_objects.push(DeletedObject {
                        key: Some(object.key),
                        ..DeletedObject::default()
                    });
                }
                Err(e) => {
                    error!(
                        "Could not remove key {} from bucket {}, error: {}",
                        &object.key, &bucket, e
                    );
                    // TODO
                    // errors.push(code_error!(InternalError, "Could not delete key"));
                }
            };
        }

        let output = DeleteObjectsOutput {
            deleted: Some(deleted_objects),
            errors: if errors.is_empty() {
                None
            } else {
                Some(errors)
            },
            ..Default::default()
        };
        Ok(S3Response::new(output))
    }

    async fn get_bucket_location(
        &self,
        req: S3Request<GetBucketLocationInput>,
    ) -> S3Result<S3Response<GetBucketLocationOutput>> {
        let input = req.input;
        let exists = try_!(self.casfs.bucket_exists(&input.bucket));

        if !exists {
            return Err(s3_error!(NoSuchBucket));
        }

        let output = GetBucketLocationOutput::default();
        Ok(S3Response::new(output))
    }

    async fn get_object(
        &self,
        req: S3Request<GetObjectInput>,
    ) -> S3Result<S3Response<GetObjectOutput>> {
        let input = req.input;
        info!("GET object {:?}", input);

        let GetObjectInput {
            bucket, key, range, ..
        } = input;

        //if !try_!(self.casfs.bucket_exists(&bucket)) {
        //    return Err(s3_error!(NoSuchBucket, "Bucket does not exist"));
        //}

        // load metadata

        let (obj_meta, paths) = match self.casfs.get_object_paths(&bucket, &key) {
            Ok(Some((obj_meta, paths))) => (obj_meta, paths),
            Ok(None) => {
                return Err(s3_error!(NoSuchKey, "Object does not exist"));
            }
            Err(e) => {
                error!("Could not get object metadata: {}", e);
                return Err(s3_error!(ServiceUnavailable, "service unavailable"));
            }
        };

        // if the object is inlined, we return it directly
        if let Some(data) = obj_meta.inlined() {
            let bytes = bytes::Bytes::from(data.clone());

            let body = s3s::Body::from(bytes);
            let stream = StreamingBlob::from(body);

            let stream_size = data.len() as u64;
            let output = GetObjectOutput {
                body: Some(stream),
                content_length: Some(stream_size as i64),
                content_range: Some(fmt_content_range(0, stream_size - 1, stream_size)),
                last_modified: Some(Timestamp::from(obj_meta.last_modified())),
                e_tag: Some(obj_meta.format_e_tag()),
                ..Default::default()
            };
            return Ok(S3Response::new(output));
        }

        let stream_size = obj_meta.size();
        let range = match range {
            Some(range) => {
                let header_string = Some(range.to_header_string());
                parse_range_request(&header_string)
            }
            None => RangeRequest::All,
        };

        let block_size: usize = paths.iter().map(|(_, size)| size).sum();

        debug_assert!(obj_meta.size() as usize == block_size);
        let block_stream = BlockStream::new(paths, block_size, range, self.metrics.clone());
        let stream = StreamingBlob::wrap(block_stream);

        let output = GetObjectOutput {
            body: Some(stream),
            content_length: Some(stream_size as i64),
            content_range: Some(fmt_content_range(0, stream_size - 1, stream_size)),
            last_modified: Some(Timestamp::from(obj_meta.last_modified())),
            //metadata: object_metadata,
            e_tag: Some(obj_meta.format_e_tag()),
            ..Default::default()
        };
        Ok(S3Response::new(output))
    }

    async fn head_bucket(
        &self,
        req: S3Request<HeadBucketInput>,
    ) -> S3Result<S3Response<HeadBucketOutput>> {
        let HeadBucketInput { bucket, .. } = req.input;

        if !try_!(self.casfs.bucket_exists(&bucket)) {
            return Err(s3_error!(
                NoSuchBucket,
                "The specified bucket does not exist"
            ));
        }

        Ok(S3Response::new(HeadBucketOutput::default()))
    }

    async fn head_object(
        &self,
        req: S3Request<HeadObjectInput>,
    ) -> S3Result<S3Response<HeadObjectOutput>> {
        let HeadObjectInput { bucket, key, .. } = req.input;

        if !try_!(self.casfs.bucket_exists(&bucket)) {
            return Err(s3_error!(NoSuchBucket, "Bucket does not exist"));
        }

        let obj_meta = match self.casfs.get_object_meta(&bucket, &key) {
            Ok(Some(obj_meta)) => obj_meta,
            Ok(None) => {
                return Err(s3_error!(NoSuchKey, "Object does not exist"));
            }
            Err(e) => {
                error!("Could not get object metadata: {}", e);
                return Err(s3_error!(ServiceUnavailable, "service unavailable"));
            }
        };

        let output = HeadObjectOutput {
            content_length: Some(obj_meta.size() as i64),
            //content_type: Some(content_type),
            last_modified: Some(obj_meta.last_modified().into()),
            //metadata: object_metadata,
            ..Default::default()
        };
        Ok(S3Response::new(output))
    }

    async fn list_buckets(
        &self,
        _: S3Request<ListBucketsInput>,
    ) -> S3Result<S3Response<ListBucketsOutput>> {
        let csfs_buckets = try_!(self.casfs.list_buckets());
        let mut buckets = Vec::with_capacity(csfs_buckets.len());
        for bucket in csfs_buckets {
            let bucket = Bucket {
                creation_date: Some(Timestamp::from(bucket.ctime())),
                name: Some(bucket.name().into()),
            };
            buckets.push(bucket);
        }
        let output = ListBucketsOutput {
            buckets: Some(buckets),
            owner: None,
            ..Default::default()
        };
        Ok(S3Response::new(output))
    }

    async fn list_objects(
        &self,
        req: S3Request<ListObjectsInput>,
    ) -> S3Result<S3Response<ListObjectsOutput>> {
        let ListObjectsInput {
            bucket,
            delimiter,
            prefix,
            encoding_type,
            marker,
            max_keys,
            ..
        } = req.input;

        let key_count = max_keys
            .map(|mk| if mk > MAX_KEYS { MAX_KEYS } else { mk })
            .unwrap_or(MAX_KEYS);

        let b = try_!(self.casfs.get_bucket(&bucket));

        let mut objects = b
            .range_filter(marker.clone(), prefix.clone(), None)
            .map(|(key, obj)| s3s::dto::Object {
                key: Some(key),
                e_tag: Some(obj.format_e_tag()),
                last_modified: Some(obj.last_modified().into()),
                owner: None,
                size: Some(obj.size() as i64),
                storage_class: None,
                ..Default::default()
            })
            .take((key_count + 1) as usize)
            .collect::<Vec<_>>();

        let mut next_marker = None;
        let truncated = objects.len() == key_count as usize + 1;
        if truncated {
            next_marker = Some(objects.pop().unwrap().key.unwrap())
        }

        let output = ListObjectsOutput {
            contents: Some(objects),
            delimiter,
            encoding_type,
            name: Some(bucket),
            //common_prefixes: None,
            is_truncated: Some(truncated),
            next_marker: if marker.is_some() { next_marker } else { None },
            marker,
            max_keys: Some(key_count),
            prefix,
            ..Default::default()
        };
        Ok(S3Response::new(output))
    }

    /// <p>StartAfter is where you want Amazon S3 to start listing from. Amazon S3 starts listing after this
    /// specified key. StartAfter can be any key in the bucket.</p>
    ///
    /// <code>ContinuationToken</code> indicates to Amazon S3 that the list is being continued on
    /// this bucket with a token. <code>ContinuationToken</code> is obfuscated and is not a real
    /// key. You can use this <code>ContinuationToken</code> for pagination of the list results.  </p>
    async fn list_objects_v2(
        &self,
        req: S3Request<ListObjectsV2Input>,
    ) -> S3Result<S3Response<ListObjectsV2Output>> {
        info!("LIST OBJECTS V2: {:?}", req.input);
        let ListObjectsV2Input {
            bucket,
            delimiter,
            prefix,
            encoding_type,
            start_after,
            max_keys,
            continuation_token,
            ..
        } = req.input;

        let b = try_!(self.casfs.get_bucket(&bucket));

        // max number of keys to return, default is MAX_KEYS(1000)
        let requested_keys = max_keys.unwrap_or(MAX_KEYS);
        let key_count = std::cmp::min(requested_keys, MAX_KEYS);

        // continuation token
        let decoded_continuation_token = decode_continuation_token(continuation_token.as_deref())?;

        let objects: Vec<_> = b
            .range_filter(
                start_after.clone(),
                prefix.clone(),
                decoded_continuation_token,
            )
            .map(|(key, obj)| s3s::dto::Object {
                key: Some(key),
                e_tag: Some(obj.format_e_tag()),
                last_modified: Some(obj.last_modified().into()),
                owner: None,
                size: Some(obj.size() as i64),
                storage_class: None,
                ..Default::default()
            })
            .take(key_count as usize)
            .collect();

        let mut next_token = None;
        let has_next = objects.len() == key_count as usize;
        if has_next {
            next_token = Some(hex_string(
                objects[(key_count - 1) as usize]
                    .key
                    .as_ref()
                    .unwrap()
                    .as_bytes(),
            ))
        }

        let output = ListObjectsV2Output {
            key_count: Some(key_count),
            max_keys: Some(key_count),
            contents: Some(objects),
            continuation_token,
            delimiter,
            encoding_type,
            name: Some(bucket),
            prefix,
            start_after,
            next_continuation_token: next_token,
            ..Default::default()
        };
        Ok(S3Response::new(output))
    }

    async fn put_object(
        &self,
        req: S3Request<PutObjectInput>,
    ) -> S3Result<S3Response<PutObjectOutput>> {
        let input = req.input;
        info!("PUT object {:?}", input);
        if let Some(ref storage_class) = input.storage_class {
            let is_valid = ["STANDARD", "REDUCED_REDUNDANCY"].contains(&storage_class.as_str());
            if !is_valid {
                return Err(s3_error!(InvalidStorageClass));
            }
        }

        let PutObjectInput {
            body,
            bucket,
            key,
            content_length,
            ..
        } = input;

        let Some(body) = body else {
            return Err(s3_error!(IncompleteBody));
        };

        if !try_!(self.casfs.bucket_exists(&bucket)) {
            return Err(s3_error!(NoSuchBucket, "Bucket does not exist"));
        }

        // if the content length is less than the max inlined data length, we store the object in the
        // metadata store, otherwise we store it in the cas layer.
        if let Some(content_length) = content_length {
            use futures::TryStreamExt;
            if content_length <= self.casfs.max_inlined_data_length() as i64 {
                // Collect stream into Vec<u8>
                // it is safe to collect the stream into memory as the content length is
                // considered small
                let data: Vec<u8> = body
                    .try_collect::<Vec<_>>()
                    .await
                    .map_err(|e| s3_error!(InternalError, "Failed to read body: {}", e))?
                    .into_iter()
                    .flatten()
                    .collect();
                let obj_meta = try_!(self.casfs.store_inlined_object(&bucket, &key, data));

                let output = PutObjectOutput {
                    e_tag: Some(obj_meta.format_e_tag()),
                    ..Default::default()
                };
                return Ok(S3Response::new(output));
            }
        }

        // save the datadata
        let converted_stream = convert_stream_error(body);
        let byte_stream =
            ByteStream::new_with_size(converted_stream, content_length.unwrap() as usize);
        let obj_meta = try_!(
            self.casfs
                .store_single_object_and_meta(&bucket, &key, byte_stream)
                .await
        );

        let output = PutObjectOutput {
            e_tag: Some(obj_meta.format_e_tag()),
            ..Default::default()
        };
        Ok(S3Response::new(output))
    }

    async fn upload_part(
        &self,
        req: S3Request<UploadPartInput>,
    ) -> S3Result<S3Response<UploadPartOutput>> {
        let UploadPartInput {
            body,
            bucket,
            content_length,
            content_md5: _, // TODO: Verify
            key,
            part_number,
            upload_id,
            ..
        } = req.input;

        let Some(body) = body else {
            return Err(s3_error!(IncompleteBody));
        };

        let content_length = content_length.ok_or_else(|| {
            s3_error!(
                MissingContentLength,
                "You did not provide the number of bytes in the Content-Length HTTP header."
            )
        })?;

        let converted_stream = convert_stream_error(body);
        let byte_stream = ByteStream::new_with_size(converted_stream, content_length as usize);

        // we only store the object here, metadata is not stored in the meta store.
        // it is stored in the multipart metadata, in the `cas` layer.
        // the multipart metadata will be deleted when the multipart upload is completed
        // and replaced with the object metadata in metastore in the `complete_multipart_upload` function.
        let (blocks, hash, size) = try_!(self.casfs.store_object(&bucket, &key, byte_stream).await);

        if size != content_length as u64 {
            return Err(s3_error!(
                InvalidRequest,
                "You did not send the amount of bytes specified by the Content-Length HTTP header."
            ));
        }

        try_!(self.casfs.insert_multipart_part(
            bucket,
            key,
            size as usize,
            part_number as i64,
            upload_id,
            hash,
            blocks
        ));

        let e_tag = format!("\"{}\"", hex_string(&hash));

        let output = UploadPartOutput {
            e_tag: Some(e_tag),
            ..Default::default()
        };
        Ok(S3Response::new(output))
    }
}

// Add helper function
fn convert_stream_error(body: StreamingBlob) -> impl Stream<Item = Result<Bytes, io::Error>> {
    body.map(|r| r.map_err(|e| io::Error::new(ErrorKind::Other, e.to_string())))
}

fn decode_continuation_token(rt: Option<&str>) -> Result<Option<String>, s3s::S3Error> {
    if let Some(rt) = rt {
        let mut out = vec![0; rt.len() / 2];
        if hex_decode(rt.as_bytes(), &mut out).is_err() {
            return Err(s3_error!(
                InvalidToken,
                "continuation token has an invalid format"
            ));
        };

        String::from_utf8(out)
            .map(Some)
            .map_err(|_| s3_error!(InvalidToken, "continuation token is invalid"))
    } else {
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cas::mock_casfs::{CasFSTrait, MockCasFS};
    use once_cell::sync::Lazy;
    use s3s::S3ErrorCode::ServiceUnavailable;
    use std::sync::Arc;
    static METRICS: Lazy<SharedMetrics> = Lazy::new(|| SharedMetrics::new());

    // Test that demonstrates how to mock CasFS for negative testing
    #[tokio::test]
    async fn test_mock_casfs_for_negative_testing() {
        //let _metrics = SharedMetrics::new();
        let mock_casfs = Arc::new(MockCasFS::new());

        // Initially, methods should succeed (not fail)
        assert!(!*mock_casfs.get_object_paths_fails.lock().unwrap());
        assert!(!*mock_casfs
            .store_single_object_and_meta_fails
            .lock()
            .unwrap());

        // Configure the mock to fail for specific methods
        mock_casfs.set_method_fails("get_object_paths", true);
        mock_casfs.set_method_fails("store_single_object_and_meta", true);

        // Verify the methods are now set to fail
        assert!(*mock_casfs.get_object_paths_fails.lock().unwrap());
        assert!(*mock_casfs
            .store_single_object_and_meta_fails
            .lock()
            .unwrap());

        // Example of how you would use this in a real test:
        // 1. Call a method that uses get_object_paths internally
        let result = mock_casfs.get_object_paths("bucket", "key");

        // 2. Verify it fails as expected
        assert!(result.is_err());

        // Example of testing an async method
        let result = mock_casfs
            .store_single_object_and_meta(
                "bucket",
                "key",
                ByteStream::new(futures::stream::empty()),
            )
            .await;

        // Verify the async method fails as expected
        assert!(result.is_err());

        // This demonstrates how you can use MockCasFS to test negative cases
        // in your S3FS implementation
    }

    #[tokio::test]
    async fn test_s3fs_get_object_failed_meta() {
        let metrics = METRICS.clone();
        let mock_casfs = MockCasFS::new();
        mock_casfs.set_method_fails("get_object_paths", true);

        let s3fs = S3FS::new(mock_casfs, metrics);

        let input = s3s::dto::GetObjectInput {
            bucket: "bucket".to_string(),
            key: "key".to_string(),
            range: None,
            part_number: None,
            response_cache_control: None,
            response_content_disposition: None,
            response_content_encoding: None,
            response_content_language: None,
            response_content_type: None,
            response_expires: None,
            sse_customer_algorithm: None,
            sse_customer_key: None,
            sse_customer_key_md5: None,
            request_payer: None,
            version_id: None,
            checksum_mode: None,
            expected_bucket_owner: None,
            if_match: None,
            if_modified_since: None,
            if_none_match: None,
            if_unmodified_since: None,
        };
        let result = s3fs.get_object(S3Request::new(input)).await;

        // First assert that we got an error
        assert!(result.is_err());

        // Then check the error code is ServiceUnavailable
        if let Err(err) = result {
            assert_eq!(*err.code(), ServiceUnavailable);
        }
    }
}
