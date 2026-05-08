use std::path::PathBuf;

use clap::Args;

#[derive(Args, Clone)]
pub struct Config {
    /// Root token. The default bucket validates against this directly; per-bucket
    /// tokens are derived as `HMAC-SHA256(OBJECTS_ROOT_TOKEN, bucket_name)`.
    #[arg(long, env = "OBJECTS_ROOT_TOKEN")]
    pub objects_root_token: secrecy::Secret<String>,

    /// Filesystem root for object storage. Buckets are top-level directories;
    /// `.tmp/` and `.multipart/` are reserved.
    #[arg(long, env = "OBJECTS_DATA_DIR", default_value = "/data")]
    pub data_dir: PathBuf,

    /// Directory for the fjall listing index. Should be on the same volume as
    /// `data_dir` so it forks atomically with the objects volume.
    #[arg(long, env = "OBJECTS_INDEX_DIR", default_value = "/data/.index")]
    pub index_dir: PathBuf,

    #[arg(long, env = "ADDRESS", default_value = "0.0.0.0:9000")]
    pub address: String,

    /// Internal-only metrics address. Bind this to a private interface — the
    /// `/metrics` endpoint is not authenticated.
    #[arg(long, env = "METRICS_ADDRESS", default_value = "127.0.0.1:9001")]
    pub metrics_address: String,

    #[arg(long, env = "LOG_LEVEL", default_value = "info")]
    pub log_level: String,

    #[arg(long, env = "OTLP_ENABLED", default_value_t = false)]
    pub otlp_enabled: bool,

    #[arg(long, env = "OTLP_ENDPOINT", default_value = "http://localhost:4317")]
    pub otlp_endpoint: String,

    /// Public base URL of this service (e.g. "https://objects.my-project.beyond.page").
    /// Used to construct `url` fields in list responses. If unset, derived from
    /// the bind address.
    #[arg(long, env = "OBJECTS_URL")]
    pub public_url: Option<String>,

    /// Write-sync linger window in milliseconds.
    ///
    /// Concurrent uploads within this window share a single fdatasync, turning N
    /// filesystem flushes into 1 on Linux ext4/xfs. Set to 0 to disable (each
    /// upload syncs immediately). Tradeoff: up to SYNC_LINGER_MS added tail
    /// latency; in-flight unsynced data is lost on crash (same as any in-flight
    /// request).
    #[arg(long, env = "SYNC_LINGER_MS", default_value = "5")]
    pub sync_linger_ms: u64,
}

impl std::fmt::Debug for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Config")
            .field("objects_root_token", &"[redacted]")
            .field("data_dir", &self.data_dir)
            .field("index_dir", &self.index_dir)
            .field("address", &self.address)
            .field("metrics_address", &self.metrics_address)
            .field("log_level", &self.log_level)
            .field("otlp_enabled", &self.otlp_enabled)
            .field("otlp_endpoint", &self.otlp_endpoint)
            .field("public_url", &self.public_url)
            .field("sync_linger_ms", &self.sync_linger_ms)
            .finish()
    }
}

impl Config {
    pub fn base_url(&self) -> String {
        self.public_url
            .clone()
            .unwrap_or_else(|| format!("http://{}", self.address))
    }
}
