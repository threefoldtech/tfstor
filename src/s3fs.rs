use std::convert::TryFrom;
use std::io::{self, ErrorKind};
use std::ops::Deref;
use std::path::PathBuf;

use bytes::Bytes;
use chrono::prelude::*;
use faster_hex::{hex_decode, hex_string};
use futures::Stream;
use futures::StreamExt;
use md5::{Digest, Md5};
use tracing::error;
use tracing::info;
use uuid::Uuid;

use s3_server::dto::ByteStream;
use s3s::dto::StreamingBlob;
use s3s::dto::Timestamp;
use s3s::dto::{
    Bucket, CompleteMultipartUploadInput, CompleteMultipartUploadOutput, CreateBucketInput,
    CreateBucketOutput, CreateMultipartUploadInput, CreateMultipartUploadOutput, DeleteBucketInput,
    DeleteBucketOutput, DeleteObjectInput, DeleteObjectOutput, DeleteObjectsInput,
    DeleteObjectsOutput, DeletedObject, GetBucketLocationInput, GetBucketLocationOutput,
    GetObjectInput, GetObjectOutput, HeadBucketInput, HeadBucketOutput, HeadObjectInput,
    HeadObjectOutput, ListBucketsInput, ListBucketsOutput, ListObjectsInput, ListObjectsOutput,
    ListObjectsV2Input, ListObjectsV2Output, PutObjectInput, PutObjectOutput, UploadPartInput,
    UploadPartOutput,
};
use s3s::s3_error;
use s3s::S3Result;
use s3s::S3;
use s3s::{S3Request, S3Response};

use crate::cas::block::Block;
use crate::cas::block_stream::BlockStream;
use crate::cas::bucket_meta::BucketMeta;
use crate::cas::multipart::MultiPart;
use crate::cas::object::Object;
use crate::cas::range_request::parse_range_request;
use crate::cas::CasFS;
use crate::metrics::SharedMetrics;

const MAX_KEYS: i32 = 1000;

#[derive(Debug)]
pub struct S3FS {
    root: PathBuf,
    casfs: CasFS,
    metrics: SharedMetrics,
}

use crate::cas::range_request::RangeRequest;
impl S3FS {
    pub fn new(
        mut root: PathBuf,
        mut meta_path: PathBuf,
        casfs: CasFS,
        metrics: SharedMetrics,
    ) -> Self {
        meta_path.push("db");
        root.push("blocks");

        //let db = sled::open(meta_path).unwrap();
        // Get the current amount of buckets
        // FIXME: This is a bit of a hack, we should have a better way to get the amount of buckets
        metrics.set_bucket_count(1); //db.open_tree(BUCKET_META_TREE).unwrap().len());

        Self {
            root,
            casfs,
            metrics,
        }
    }
}

fn fmt_content_range(start: u64, end_inclusive: u64, size: u64) -> String {
    format!("bytes {start}-{end_inclusive}/{size}")
}

#[async_trait::async_trait]
impl S3 for S3FS {
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
            return Err(err.into());
        };

        let multipart_map = try_!(self.casfs.multipart_tree());

        let mut blocks = vec![];
        let mut cnt: i32 = 0;
        for part in multipart_upload.parts.iter().flatten() {
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
            let part_key = format!("{}-{}-{}-{}", &bucket, &key, &upload_id, part_number);
            let part_data_enc = try_!(multipart_map.get(&part_key));
            let part_data_enc = match part_data_enc {
                Some(pde) => pde,
                None => {
                    error!("Missing part \"{}\" in multipart upload", part_key);
                    return Err(s3_error!(InvalidArgument, "Part not uploaded").into());
                }
            };

            // unwrap here is safe as it is a coding error
            let mp = MultiPart::try_from(&*part_data_enc).expect("Corrupted multipart data");

            blocks.extend_from_slice(mp.blocks());
        }

        // Compute the e_tag of the multpart upload. Per the S3 standard (according to minio), the
        // e_tag of a multipart uploaded object is the Md5 of the Md5 of the parts.
        let mut hasher = Md5::new();
        let mut size = 0;
        let block_map = try_!(self.casfs.block_tree());
        for block in &blocks {
            let bi = try_!(block_map.get(&block)).unwrap(); // unwrap is fine as all blocks in must be present
            let block_info = Block::try_from(&*bi).expect("Block data is corrupt");
            size += block_info.size();
            hasher.update(&block);
        }
        let e_tag = hasher.finalize().into();

        let bc = try_!(self.casfs.bucket(&bucket));

        let object = Object::new(size as u64, e_tag, cnt as usize, blocks);

        try_!(bc.insert(&key, Vec::<u8>::from(&object)));

        // Try to delete the multipart metadata. If this fails, it is not really an issue.
        for part in multipart_upload.parts.into_iter().flatten() {
            let part_key = format!(
                "{}-{}-{}-{}",
                &bucket,
                &key,
                &upload_id,
                part.part_number.unwrap()
            );

            if let Err(e) = multipart_map.remove(part_key) {
                error!("Could not remove part: {}", e);
            };
        }

        let output = CompleteMultipartUploadOutput {
            bucket: Some(bucket),
            key: Some(key),
            e_tag: Some(object.format_e_tag()),
            ..Default::default()
        };
        Ok(S3Response::new(output))
    }

    async fn create_bucket(
        &self,
        req: S3Request<CreateBucketInput>,
    ) -> S3Result<S3Response<CreateBucketOutput>> {
        let input = req.input;

        if try_!(self.casfs.bucket_exists(&input.bucket)) {
            return Err(s3_error!(
                BucketAlreadyExists,
                "A bucket with this name already exists"
            )
            .into());
        }

        // TODO:
        // - move ALL bucket _creation_ to the casfs
        // - revert the bucket_meta_tree() to private method
        let bucket_meta = try_!(self.casfs.bucket_meta_tree());

        let bm = Vec::from(&BucketMeta::new(
            Utc::now().timestamp(),
            input.bucket.clone(),
        ));

        try_!(bucket_meta.insert(&input.bucket, bm));

        self.metrics.inc_bucket_count();

        let output = CreateBucketOutput::default(); // TODO: handle other fields
        Ok(S3Response::new(output))
    }

    async fn create_multipart_upload(
        &self,
        req: S3Request<CreateMultipartUploadInput>,
    ) -> S3Result<S3Response<CreateMultipartUploadOutput>> {
        let CreateMultipartUploadInput { bucket, key, .. } = req.input;

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

        if !try_!(self.casfs.bucket_exists(&bucket)) {
            return Err(s3_error!(NoSuchBucket, "Bucket does not exist").into());
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
            return Err(s3_error!(NoSuchBucket, "Bucket does not exist").into());
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

        // load metadata
        let bk = try_!(self.casfs.bucket(&bucket));
        let obj = match try_!(bk.get(&key)) {
            None => return Err(s3_error!(NoSuchKey, "The specified key does not exist").into()),
            Some(obj) => obj,
        };
        let obj_meta = try_!(Object::try_from(&obj.to_vec()[..]));

        let e_tag = obj_meta.format_e_tag();
        let stream_size = obj_meta.size();
        let range = match range {
            Some(range) => {
                let header_string = Some(range.to_header_string());
                parse_range_request(&header_string)
            }
            None => RangeRequest::All,
        };

        // load the data
        let block_map = try_!(self.casfs.block_tree());
        let mut paths = Vec::with_capacity(obj_meta.blocks().len());
        let mut block_size = 0;
        for block in obj_meta.blocks() {
            // unwrap here is safe as we only add blocks to the list of an object if they are
            // corectly inserted in the block map
            let block_meta_enc = try_!(block_map.get(block)).unwrap();
            let block_meta = try_!(Block::try_from(&*block_meta_enc));
            block_size += block_meta.size();
            paths.push((block_meta.disk_path(self.root.clone()), block_meta.size()));
        }
        debug_assert!(obj_meta.size() as usize == block_size);
        let block_stream = BlockStream::new(paths, block_size, range, self.metrics.clone());
        let stream = StreamingBlob::wrap(block_stream);

        let output = GetObjectOutput {
            body: Some(stream),
            content_length: Some(stream_size as i64),
            content_range: Some(fmt_content_range(0, stream_size - 1, stream_size)),
            last_modified: Some(Timestamp::from(obj_meta.last_modified())),
            //metadata: object_metadata,
            e_tag: Some(e_tag),
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
            return Err(s3_error!(NoSuchBucket, "The specified bucket does not exist").into());
        }

        Ok(S3Response::new(HeadBucketOutput::default()))
    }

    async fn head_object(
        &self,
        req: S3Request<HeadObjectInput>,
    ) -> S3Result<S3Response<HeadObjectOutput>> {
        let HeadObjectInput { bucket, key, .. } = req.input;
        let bk = try_!(self.casfs.bucket(&bucket));

        // TODO: move this to get_object_meta
        let obj = match try_!(bk.get(&key)) {
            None => return Err(s3_error!(NoSuchKey, "The specified key does not exist").into()),
            Some(obj) => obj,
        };
        let obj_meta = try_!(Object::try_from(&obj.to_vec()[..]));

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
        let csfs_buckets = try_!(self.casfs.buckets());
        let mut buckets = Vec::with_capacity(csfs_buckets.len());
        for bucket in csfs_buckets {
            let bucket = Bucket {
                creation_date: None, //creation_date: bucket.creation_date, TODO: fix it
                name: bucket.name,
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

        let b = try_!(self.casfs.bucket(&bucket));

        let start_bytes = if let Some(ref marker) = marker {
            marker.as_bytes()
        } else if let Some(ref prefix) = prefix {
            prefix.as_bytes()
        } else {
            &[]
        };
        let prefix_bytes = prefix.as_deref().or(Some("")).unwrap().as_bytes();

        let mut objects = b
            .range(start_bytes..)
            .filter_map(|read_result| match read_result {
                Err(_) => None,
                Ok((k, v)) => Some((k, v)),
            })
            .take_while(|(raw_key, _)| raw_key.starts_with(prefix_bytes))
            .map(|(raw_key, raw_value)| {
                // SAFETY: we only insert valid utf8 strings
                let key = unsafe { String::from_utf8_unchecked(raw_key.to_vec()) };
                // unwrap is fine as it would mean either a coding error or a corrupt DB
                let obj = Object::try_from(&*raw_value).unwrap();

                s3s::dto::Object {
                    key: Some(key),
                    e_tag: Some(obj.format_e_tag()),
                    last_modified: Some(obj.last_modified().into()),
                    owner: None,
                    size: Some(obj.size() as i64),
                    storage_class: None,
                    ..Default::default()
                }
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

        let b = try_!(self.casfs.bucket(&bucket));

        let key_count = max_keys
            .map(|mk| if mk > MAX_KEYS { MAX_KEYS } else { mk })
            .unwrap_or(MAX_KEYS);

        let token = if let Some(ref rt) = continuation_token {
            let mut out = vec![0; rt.len() / 2];
            if hex_decode(rt.as_bytes(), &mut out).is_err() {
                return Err(
                    s3_error!(InvalidToken, "continuation token has an invalid format").into(),
                );
            };
            match String::from_utf8(out) {
                Ok(s) => Some(s),
                Err(_) => {
                    return Err(s3_error!(InvalidToken, "continuation token is invalid").into())
                }
            }
        } else {
            None
        };

        let start_bytes = if let Some(ref token) = token {
            token.as_bytes()
        } else if let Some(ref prefix) = prefix {
            prefix.as_bytes()
        } else if let Some(ref start_after) = start_after {
            start_after.as_bytes()
        } else {
            &[]
        };
        let prefix_bytes = prefix.as_deref().or(Some("")).unwrap().as_bytes();

        let mut objects: Vec<_> = b
            .range(start_bytes..)
            .filter_map(|read_result| match read_result {
                Ok((r, k)) => Some((r, k)),
                Err(_) => None,
            })
            .skip_while(|(raw_key, _)| match start_after {
                None => false,
                Some(ref start_after) => raw_key.deref() <= start_after.as_bytes(),
            })
            .take_while(|(raw_key, _)| raw_key.starts_with(prefix_bytes))
            .map(|(raw_key, raw_value)| {
                // SAFETY: we only insert valid utf8 strings
                let key = unsafe { String::from_utf8_unchecked(raw_key.to_vec()) };
                // unwrap is fine as it would mean either a coding error or a corrupt DB
                let obj = Object::try_from(&*raw_value).unwrap();

                s3s::dto::Object {
                    key: Some(key),
                    e_tag: Some(obj.format_e_tag()),
                    last_modified: Some(obj.last_modified().into()),
                    owner: None,
                    size: Some(obj.size() as i64),
                    storage_class: None,
                    ..Default::default()
                }
            })
            .take((key_count + 1) as usize)
            .collect();

        let mut next_token = None;
        let truncated = objects.len() == key_count as usize + 1;
        if truncated {
            next_token = Some(hex_string(objects.pop().unwrap().key.unwrap().as_bytes()))
        }

        let output = ListObjectsV2Output {
            key_count: Some(key_count),
            max_keys: Some(key_count),
            contents: Some(objects),
            continuation_token,
            delimiter: delimiter,
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
            return Err(s3_error!(NoSuchBucket, "Bucket does not exist").into());
        }

        // save the data
        let converted_stream = convert_stream_error(body);
        let byte_stream =
            ByteStream::new_with_size(converted_stream, content_length.unwrap() as usize);
        let (blocks, hash, size) = try_!(self.casfs.store_bytes(byte_stream).await);

        // save the metadata
        let obj_meta = Object::new(size, hash, 0, blocks);
        try_!(try_!(self.casfs.bucket(&bucket)).insert(&key, Vec::<u8>::from(&obj_meta)));

        let output = PutObjectOutput {
            //e_tag: Some(obj_meta.format_e_tag()),
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
        let (blocks, hash, size) = try_!(self.casfs.store_bytes(byte_stream).await);

        if size != content_length as u64 {
            return Err(s3_error!(
                InvalidRequest,
                "You did not send the amount of bytes specified by the Content-Length HTTP header."
            )
            .into());
        }

        let mp_map = try_!(self.casfs.multipart_tree());

        let e_tag = format!("\"{}\"", hex_string(&hash));
        let storage_key = format!("{}-{}-{}-{}", &bucket, &key, &upload_id, part_number);
        let mp = MultiPart::new(
            size as usize,
            part_number as i64,
            bucket,
            key,
            upload_id,
            hash,
            blocks,
        );

        let enc_mp = Vec::from(&mp);

        try_!(mp_map.insert(storage_key, enc_mp));

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
