use anyhow::Result;
use clap::Parser;
use std::path::PathBuf;
use tracing::info;

mod cmd;
mod conn;
mod namespace;
mod resp;
mod server;
mod storage;

#[derive(Parser, Debug)]
#[clap(name = "respd", about = "Redis-compatible server using metastore")]
struct Opt {
    /// Path to the data directory
    #[clap(long, default_value = "./data")]
    data_dir: PathBuf,

    /// Port to listen on
    #[clap(long, default_value = "6379")]
    port: u16,

    /// Host to bind to
    #[clap(long, default_value = "127.0.0.1")]
    host: String,

    /// Size limit for inlined metadata in bytes
    #[clap(long)]
    inlined_metadata_size: Option<usize>,

    /// Admin password for authentication
    /// If not provided, all connections are automatically granted admin privileges
    #[clap(long)]
    admin: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    // Parse command line arguments
    let opt = Opt::parse();

    info!("Data directory: {:?}", opt.data_dir);
    info!("Inlined metadata size: {:?}", opt.inlined_metadata_size);

    // Create data directory if it doesn't exist
    if !opt.data_dir.exists() {
        std::fs::create_dir_all(&opt.data_dir)?;
    }

    // Initialize storage
    let storage = storage::Storage::new(opt.data_dir.clone(), opt.inlined_metadata_size);

    // Start server
    info!("Starting respd server on {}:{}", opt.host, opt.port);
    if opt.admin.is_some() {
        info!("Admin authentication is required");
    } else {
        info!("Admin authentication is disabled - all connections have admin privileges");
    }
    let addr = format!("{}:{}", opt.host, opt.port);
    server::run(addr, storage, opt.admin).await
}
