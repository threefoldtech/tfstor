#![forbid(unsafe_code)]
#![deny(
    clippy::all, //
    clippy::must_use_candidate, //
)]

use s3s::host::SingleDomain;
use s3s::service::S3ServiceBuilder;

use std::env;

use aws_config::SdkConfig;
use aws_credential_types::provider::SharedCredentialsProvider;
use aws_sdk_s3::config::Credentials;
use aws_sdk_s3::config::Region;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client;

use aws_sdk_s3::types::BucketLocationConstraint;
//use aws_sdk_s3::types::ChecksumMode;
//use aws_sdk_s3::types::CompletedMultipartUpload;
//use aws_sdk_s3::types::CompletedPart;
use aws_sdk_s3::types::CreateBucketConfiguration;

use anyhow::Result;
use once_cell::sync::Lazy;
use std::convert::TryInto;
use tokio::sync::Mutex;
use tokio::sync::MutexGuard;
use tracing::debug;
use uuid::Uuid;

const FS_ROOT: &str = concat!(env!("CARGO_TARGET_TMPDIR"), "/s3s-fs-tests-aws");
const DOMAIN_NAME: &str = "localhost:8014";
const REGION: &str = "us-west-2";

pub fn setup_tracing() {
    use tracing::Level;
    use tracing_subscriber::FmtSubscriber;
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();

    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");
}

fn config() -> &'static SdkConfig {
    static CONFIG: Lazy<SdkConfig> = Lazy::new(|| {
        setup_tracing();

        // Fake credentials
        let cred = Credentials::for_tests();

        // Setup S3 provider
        //fs::create_dir_all(FS_ROOT).unwrap();
        //let fs = FileSystem::new(FS_ROOT).unwrap();

        let metrics = s3_cas::metrics::SharedMetrics::new();
        let casfs = s3_cas::cas::CasFS::new(FS_ROOT.into(), FS_ROOT.into(), metrics.clone());
        let s3fs = s3_cas::s3fs::S3FS::new(FS_ROOT.into(), FS_ROOT.into(), casfs, metrics.clone());

        // Setup S3 service
        let service = {
            let mut b = S3ServiceBuilder::new(s3fs);
            b.set_auth(s3s::auth::SimpleAuth::from_single(
                cred.access_key_id(),
                cred.secret_access_key(),
            ));
            b.set_host(SingleDomain::new(DOMAIN_NAME).unwrap());
            b.build()
        };

        // Convert to aws http client
        let client = s3s_aws::Client::from(service.into_shared());

        // Setup aws sdk config
        SdkConfig::builder()
            .credentials_provider(SharedCredentialsProvider::new(cred))
            .http_client(client)
            .region(Region::new(REGION))
            .endpoint_url(format!("http://{DOMAIN_NAME}"))
            .build()
    });
    &CONFIG
}

async fn serial() -> MutexGuard<'static, ()> {
    static LOCK: Lazy<Mutex<()>> = Lazy::new(|| Mutex::new(()));
    LOCK.lock().await
}

async fn create_bucket(c: &Client, bucket: &str) -> Result<()> {
    let location = BucketLocationConstraint::from(REGION);
    let cfg = CreateBucketConfiguration::builder()
        .location_constraint(location)
        .build();

    c.create_bucket()
        .create_bucket_configuration(cfg)
        .bucket(bucket)
        .send()
        .await?;

    debug!("created bucket: {bucket:?}");
    Ok(())
}

#[tokio::test]
#[tracing::instrument]
async fn test_single_object() -> Result<()> {
    let _guard = serial().await;

    let c = Client::new(config());
    let bucket = format!("test-single-object-{}", Uuid::new_v4());
    let bucket = bucket.as_str();
    let key = "sample.txt";
    let content = "hello hello hello hello hello hello hello\n";
    //let crc32c =
    //    base64_simd::STANDARD.encode_to_string(crc32c::crc32c(content.as_bytes()).to_be_bytes());

    create_bucket(&c, bucket).await?;

    {
        let body = ByteStream::from_static(content.as_bytes());
        c.put_object()
            .bucket(bucket)
            .key(key)
            .body(body)
            //.checksum_crc32_c(crc32c.as_str())
            .send()
            .await?;
    }

    {
        let ans = c
            .get_object()
            .bucket(bucket)
            .key(key)
            //.checksum_mode(ChecksumMode::Enabled)
            .send()
            .await?;

        let content_length: usize = ans.content_length().unwrap().try_into().unwrap();
        //let checksum_crc32c = ans.checksum_crc32_c.unwrap();
        let body = ans.body.collect().await?.into_bytes();

        assert_eq!(content_length, content.len());
        //assert_eq!(checksum_crc32c, crc32c);
        assert_eq!(body.as_ref(), content.as_bytes());
    }

    {
        delete_object(&c, bucket, key).await?;
        delete_bucket(&c, bucket).await?;
    }

    Ok(())
}

async fn delete_object(c: &Client, bucket: &str, key: &str) -> Result<()> {
    c.delete_object().bucket(bucket).key(key).send().await?;
    Ok(())
}

async fn delete_bucket(c: &Client, bucket: &str) -> Result<()> {
    c.delete_bucket().bucket(bucket).send().await?;
    Ok(())
}
