use crate::cas::block::Block;
use crate::cas::block_stream::BlockStream;
use crate::cas::object::Object;
use crate::cas::range_request::parse_range_request;
use crate::cas::CasFS;
use crate::metrics::SharedMetrics;
use bytes::Bytes;
use chrono::prelude::*;
use futures::Stream;
use futures::StreamExt;
use s3_server::dto::ByteStream;
use s3s::dto::StreamingBlob;
use s3s::dto::Timestamp;
use s3s::dto::{
    CreateBucketInput, CreateBucketOutput, GetBucketLocationInput, GetBucketLocationOutput,
    GetObjectInput, GetObjectOutput, PutObjectInput, PutObjectOutput,
};
use s3s::s3_error;
use s3s::S3Result;
use s3s::S3;
use s3s::{S3Request, S3Response};
use std::convert::TryFrom;
use std::io::{self, ErrorKind};
use std::path::PathBuf;
use tracing::info;

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

use crate::cas::bucket_meta::BucketMeta;
#[async_trait::async_trait]
impl S3 for S3FS {
    async fn create_bucket(
        &self,
        req: S3Request<CreateBucketInput>,
    ) -> S3Result<S3Response<CreateBucketOutput>> {
        /*
        ///
        let bucket_meta = trace_try!(self.bucket_meta_tree());

        let bm = Vec::from(&BucketMeta::new(Utc::now().timestamp(), bucket.clone()));

        trace_try!(bucket_meta.insert(&bucket, bm));

        self.metrics.inc_bucket_count();*/

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
            //content_range,
            last_modified: Some(Timestamp::from(obj_meta.last_modified())),
            //metadata: object_metadata,
            e_tag: Some(e_tag),
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
}

// Add helper function
fn convert_stream_error(body: StreamingBlob) -> impl Stream<Item = Result<Bytes, io::Error>> {
    body.map(|r| r.map_err(|e| io::Error::new(ErrorKind::Other, e.to_string())))
}
