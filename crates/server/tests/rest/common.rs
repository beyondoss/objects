use std::sync::OnceLock;

use beyond_objects::{Config, test_support::TestServer};

/// Shared root token for the singleton test server. Buckets in tests are
/// created via the bucket CRUD API (which requires the root token), so
/// per-test isolation comes from unique bucket names rather than per-test
/// servers.
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

pub fn url(path: &str) -> String {
    format!("{}{}", server().url, path)
}

pub fn client() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()
        .expect("reqwest client")
}

pub fn root_token() -> &'static str {
    ROOT_TOKEN
}

pub fn bucket_token(bucket: &str) -> String {
    use hmac::{Hmac, Mac};
    use sha2::Sha256;
    let mut mac = Hmac::<Sha256>::new_from_slice(ROOT_TOKEN.as_bytes()).unwrap();
    mac.update(bucket.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

/// Generate a unique bucket name for a test, isolating it from other tests.
pub fn unique_bucket(prefix: &str) -> String {
    format!(
        "{prefix}-{}",
        uuid::Uuid::new_v4().simple().to_string().get(..12).unwrap()
    )
}

pub async fn create_bucket(name: &str, access: &str) {
    let res = client()
        .post(url("/v1/buckets"))
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
