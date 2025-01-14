use s3_cas::cas::CasFS;
use s3_server::S3Service;
use s3_server::SimpleAuth;

use std::convert::Infallible;
use std::net::TcpListener;
use std::path::PathBuf;

use anyhow::Result;
use futures::{future, try_join};
use hyper::server::Server;
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Request, Response};
use prometheus::{Encoder, TextEncoder};
use structopt::StructOpt;
use tracing::{error, info, Level};
use tracing_subscriber::FmtSubscriber;

#[derive(StructOpt)]
struct Args {
    #[structopt(long, default_value = ".")]
    fs_root: PathBuf,

    #[structopt(long, default_value = ".")]
    meta_root: PathBuf,

    #[structopt(long, default_value = "localhost")]
    host: String,

    #[structopt(long, default_value = "8014")]
    port: u16,

    #[structopt(long, default_value = "localhost")]
    metric_host: String,

    #[structopt(long, default_value = "9100")]
    metric_port: u16,

    #[structopt(long, requires("secret-key"), display_order = 1000)]
    access_key: Option<String>,

    #[structopt(long, requires("access-key"), display_order = 1000)]
    secret_key: Option<String>,

    #[structopt(long = "nugine", parse(from_flag = std::ops::Not::not))]
    nugine: bool,
}

pub fn setup_tracing() {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();

    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");
}

fn main() -> Result<()> {
    dotenv::dotenv().ok();

    setup_tracing();

    let args: Args = Args::from_args();

    if args.nugine {
        info!("run with nugine");
        return run(args);
    } else {
        return run_old(args);
    }
}

use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder as ConnBuilder;
use s3s::service::S3ServiceBuilder;

#[tokio::main]
async fn run(args: Args) -> anyhow::Result<()> {
    // provider
    let metrics = s3_cas::metrics::SharedMetrics::new();
    let casfs = CasFS::new(
        args.fs_root.clone(),
        args.meta_root.clone(),
        metrics.clone(),
    );
    let s3fs = s3_cas::s3fs::S3FS::new(args.fs_root, args.meta_root, casfs, metrics.clone());

    // Setup S3 service
    let service = {
        let mut b = S3ServiceBuilder::new(s3fs);

        // Enable authentication
        if let (Some(ak), Some(sk)) = (args.access_key, args.secret_key) {
            b.set_auth(s3s::auth::SimpleAuth::from_single(ak, sk));
            info!("authentication is enabled");
        }

        b.build()
    };

    // Run server
    // Update TcpListener bind with proper error context
    let listener = tokio::net::TcpListener::bind((args.host.as_str(), args.port)).await?;
    let local_addr = listener.local_addr()?;

    let hyper_service = service.into_shared();

    let http_server = ConnBuilder::new(TokioExecutor::new());
    let graceful = hyper_util::server::graceful::GracefulShutdown::new();

    let mut ctrl_c = std::pin::pin!(tokio::signal::ctrl_c());

    info!("server is running at http://{local_addr}");

    loop {
        let (socket, _) = tokio::select! {
            res =  listener.accept() => {
                match res {
                    Ok(conn) => conn,
                    Err(err) => {
                        tracing::error!("error accepting connection: {err}");
                        continue;
                    }
                }
            }
            _ = ctrl_c.as_mut() => {
                break;
            }
        };

        let conn = http_server.serve_connection(TokioIo::new(socket), hyper_service.clone());
        let conn = graceful.watch(conn.into_owned());
        tokio::spawn(async move {
            let _ = conn.await;
        });
    }

    tokio::select! {
        () = graceful.shutdown() => {
             tracing::debug!("Gracefully shutdown!");
        },
        () = tokio::time::sleep(std::time::Duration::from_secs(10)) => {
             tracing::debug!("Waited 10 seconds for graceful shutdown, aborting...");
        }
    }

    info!("server is stopped");
    Ok(())
}

#[tokio::main]
async fn run_old(args: Args) -> anyhow::Result<()> {
    // setup the storage
    // let fs = FileSystem::new(&args.fs_root)?;
    let metrics = s3_cas::metrics::SharedMetrics::new();
    let fs = CasFS::new(args.fs_root, args.meta_root, metrics.clone());
    let fs = s3_cas::metrics::MetricFs::new(fs, metrics);
    // debug!(?fs);

    // setup the service
    // let mut service = S3Service::new(Passthrough::new(fs));
    let mut service = S3Service::new(fs);

    if let (Some(access_key), Some(secret_key)) = (args.access_key, args.secret_key) {
        let mut auth = SimpleAuth::new();
        auth.register(access_key, secret_key);
        // debug!(?auth);
        service.set_auth(auth);
    }

    let server = {
        let service = service.into_shared();
        let listener = TcpListener::bind((args.host.as_str(), args.port))?;
        let make_service: _ =
            make_service_fn(move |_| future::ready(Ok::<_, anyhow::Error>(service.clone())));
        Server::from_tcp(listener)?.serve(make_service)
    };

    async fn serve_metrics(req: Request<Body>) -> Result<Response<Body>, Infallible> {
        let mut response = Response::new(Body::empty());
        match (req.method(), req.uri().path()) {
            (&hyper::Method::GET, "/metrics") => {
                let mut buffer = Vec::new();
                let encoder = TextEncoder::new();

                let metric_families = prometheus::gather();
                encoder.encode(&metric_families, &mut buffer).unwrap();
                *response.body_mut() = Body::from(buffer);
            }
            _ => *response.status_mut() = hyper::StatusCode::NOT_FOUND,
        }
        Ok(response)
    }

    let metric_server = {
        let listener = TcpListener::bind((args.metric_host.as_str(), args.metric_port))?;
        let make_service: _ = make_service_fn(move |_| {
            future::ready(Ok::<_, anyhow::Error>(service_fn(serve_metrics)))
        });
        Server::from_tcp(listener)?.serve(make_service)
    };

    info!("server is running at http://{}:{}/", args.host, args.port);
    info!(
        "metric server is running at http://{}:{}",
        args.metric_host, args.metric_port
    );

    if let Err(e) = try_join!(metric_server, server) {
        error!("Server failed: {}", e);
    }

    Ok(())
}
