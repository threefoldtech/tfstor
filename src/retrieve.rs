use std::path::PathBuf;

use anyhow::Result;
use bytes::Bytes;
use clap::Parser;
use futures::StreamExt;
use tokio::io::AsyncWriteExt;

use crate::cas::block_stream::BlockStream;
use crate::cas::range_request::RangeRequest;
use crate::cas::CasFS;
use crate::cas::StorageEngine;
use crate::metrics::SharedMetrics;

#[derive(Parser, Debug)]
pub struct RetrieveConfig {
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

    #[arg(required = true, help = "Destination file path")]
    pub dest: String,
}

#[tokio::main]
pub async fn retrieve(args: RetrieveConfig) -> Result<()> {
    tracing::info!(
        "Retrieving object from bucket: {}, key: {}",
        args.bucket,
        args.key
    );
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

    tracing::info!("get_object_blocks");
    let (obj_meta, blocks) = match casfs.get_object_blocks(&args.bucket, &args.key)? {
        Some((obj, blocks)) => (obj, blocks),
        None => {
            println!("Object not found");
            return Ok(());
        }
    };

    tracing::info!("Object found, size: {}", obj_meta.size());
    if let Some(data) = obj_meta.inlined() {
        let mut file = tokio::fs::File::create(&args.dest).await?;
        file.write_all(data).await?;
        return Ok(());
    }

    let mut paths = Vec::with_capacity(blocks.len());
    let mut block_size = 0;
    for block in blocks {
        block_size += block.size();
        paths.push((block.disk_path(casfs.fs_root().clone()), block.size()));
        tracing::info!("block path: {:?}", block.disk_path(casfs.fs_root().clone()));
    }

    debug_assert!(obj_meta.size() as usize == block_size);
    tracing::info!("creating block stream");
    let mut block_stream = BlockStream::new(paths, block_size, RangeRequest::All, metrics);

    // Create the destination file
    tracing::info!("creating destination file: {}", args.dest);
    let mut file = tokio::fs::File::create(&args.dest).await?;

    // Read from block stream and write to file
    while let Some(chunk_result) = block_stream.next().await {
        let chunk: Bytes = chunk_result?;
        tracing::info!("writing chunk of size: {}", chunk.len());
        file.write_all(&chunk).await?;
    }

    // Ensure all data is written to disk
    file.flush().await?;

    Ok(())
}
