use std::path::PathBuf;

use anyhow::Result;
use bytes::Bytes;
use clap::Parser;
use futures::StreamExt;
use md5::{Digest, Md5};

use crate::cas::block_stream::BlockStream;
use crate::cas::range_request::RangeRequest;
use crate::cas::CasFS;
use crate::cas::StorageEngine;
use crate::metrics::SharedMetrics;

#[derive(Parser, Debug)]
pub struct CheckConfig {
    #[arg(long, default_value = ".")]
    pub meta_root: PathBuf,

    #[arg(long, default_value = ".")]
    pub fs_root: PathBuf,

    #[arg(
        long,
        default_value = "fjall",
        help = "Metadata DB  (fjall, fjall_notx)"
    )]
    pub metadata_db: StorageEngine,

    #[arg(required = true, help = "Bucket name")]
    pub bucket: String,

    #[arg(required = true, help = "Object key")]
    pub key: String,
}

#[tokio::main]
pub async fn check_integrity(args: CheckConfig) -> Result<()> {
    let storage_engine = args.metadata_db;
    let metrics = SharedMetrics::new();
    let casfs = CasFS::new(
        args.fs_root.clone(),
        args.meta_root.clone(),
        metrics.clone(),
        storage_engine,
        None,
        None,
    );

    let (obj_meta, _) = match casfs.get_object_paths(&args.bucket, &args.key)? {
        Some((obj, paths)) => (obj, paths),
        None => {
            eprintln!("Object not found");
            return Ok(());
        }
    };

    let Some(data) = get_object_data(&casfs, &args.bucket, &args.key, metrics).await? else {
        eprintln!("Object not found");
        return Ok(());
    };

    let hash: [u8; 16] = Md5::digest(data).into();
    if hash != *obj_meta.hash() {
        eprintln!("check failed: hash mismatch");
    } else {
        println!("check passed: hash matched");
    }

    Ok(())
}

async fn get_object_data(
    casfs: &CasFS,
    bucket: &str,
    key: &str,
    metrics: SharedMetrics,
) -> Result<Option<Vec<u8>>> {
    let (obj_meta, paths) = match casfs.get_object_paths(bucket, key)? {
        Some((obj, paths)) => (obj, paths),
        None => return Ok(None),
    };

    let data = if let Some(inline_data) = obj_meta.inlined() {
        inline_data.to_vec()
    } else {
        let block_size: usize = paths.iter().map(|(_, size)| size).sum();
        debug_assert!(obj_meta.size() as usize == block_size);

        let mut block_stream = BlockStream::new(paths, block_size, RangeRequest::All, metrics);
        let mut data = Vec::with_capacity(block_size);

        while let Some(chunk_result) = block_stream.next().await {
            let chunk: Bytes = chunk_result?;
            data.extend_from_slice(&chunk);
        }
        data
    };

    Ok(Some(data))
}
