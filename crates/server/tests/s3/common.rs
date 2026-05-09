//! Shared test scaffolding for the S3 integration suite. Reuses the same
//! `OnceLock` test server the REST suite uses (one binary boot per `cargo
//! test` invocation, regardless of how many test files reference it).

use std::sync::OnceLock;

use aws_config::Region;
use aws_sdk_s3::Client as S3Client;
use aws_sdk_s3::config::{Builder as S3ConfigBuilder, Credentials, SharedCredentialsProvider};
use aws_sdk_s3::primitives::ByteStream;
use aws_smithy_async::rt::sleep::default_async_sleep;
use beyond_objects::{Config, test_support::TestServer};
use hmac::{Hmac, Mac};
use sha2::Sha256;

const ROOT_TOKEN: &str = "test-root-token";

static SERVER: OnceLock<TestServer> = OnceLock::new();

pub fn server() -> &'static TestServer {
    SERVER.get_or_init(|| {
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .expect("test runtime");
            rt.block_on(async move {
                let dir = tempfile::tempdir().expect("tempdir");
                let config = Config {
                    objects_root_token: secrecy::Secret::new(ROOT_TOKEN.into()),
                    data_dir: dir.path().join("data"),
                    index_dir: dir.path().join("index"),
                    address: "127.0.0.1:0".into(),
                    metrics_address: "127.0.0.1:0".into(),
                    log_level: "error".into(),
                    otlp_enabled: false,
                    otlp_endpoint: "http://localhost:4317".into(),
                    public_url: None,
                    sync_linger_ms: 0,
                    drain_timeout_secs: 0,
                    otlp_sample_rate: 1.0,
                    gc_temp_ttl_secs: 3600,
                    gc_multipart_ttl_secs: 86400,
                };
                let server = beyond_objects::test_support::start(config)
                    .await
                    .expect("test server start");
                tx.send(server).expect("send TestServer");
                tokio::signal::ctrl_c().await.ok();
            });
            std::process::exit(130);
        });
        rx.recv().expect("recv TestServer")
    })
}

pub fn root_token() -> &'static str {
    ROOT_TOKEN
}

pub fn bucket_secret(bucket: &str) -> String {
    let mut mac = Hmac::<Sha256>::new_from_slice(ROOT_TOKEN.as_bytes()).unwrap();
    mac.update(bucket.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

/// Build an `aws-sdk-s3` client pointed at the test server. `bucket_or_root`
/// becomes the access key id; the secret is `HMAC(root, key_id)` for buckets
/// and `root_token` for the special `"root"` id.
pub fn s3_client(access_key_id: &str) -> S3Client {
    let secret = if access_key_id == "root" {
        ROOT_TOKEN.to_string()
    } else {
        bucket_secret(access_key_id)
    };

    let credentials = Credentials::new(access_key_id, secret, None, None, "beyond-objects-test");
    let url = server().url.clone();

    let mut builder = S3ConfigBuilder::new()
        .credentials_provider(SharedCredentialsProvider::new(credentials))
        .region(Region::new("us-east-1"))
        .endpoint_url(url)
        .force_path_style(true)
        .behavior_version(aws_config::BehaviorVersion::latest());
    if let Some(sleep) = default_async_sleep() {
        builder = builder.sleep_impl(sleep);
    }
    S3Client::from_conf(builder.build())
}

/// Convenience: create a test bucket via the REST surface (which is the
/// canonical bucket-CRUD path) so we don't require S3 `CreateBucket` to be
/// the System Under Test for unrelated tests.
pub async fn create_bucket_via_rest(name: &str, access: &str) {
    let s = server();
    let client = reqwest::Client::new();
    let res = client
        .post(format!("{}/v1/buckets", s.url))
        .bearer_auth(root_token())
        .json(&serde_json::json!({ "name": name, "access": access }))
        .send()
        .await
        .expect("create_bucket request");
    assert_eq!(
        res.status(),
        reqwest::StatusCode::CREATED,
        "bucket create failed: {}",
        res.text().await.unwrap_or_default()
    );
}

pub fn unique_bucket(prefix: &str) -> String {
    format!(
        "{prefix}-{}",
        uuid::Uuid::new_v4().simple().to_string().get(..12).unwrap()
    )
}

pub fn byte_stream(bytes: &[u8]) -> ByteStream {
    ByteStream::from(bytes.to_vec())
}
