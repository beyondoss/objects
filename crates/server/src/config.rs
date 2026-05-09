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

    /// Maximum seconds to wait for in-flight requests to drain after a shutdown
    /// signal before forcing exit. Set to 0 to drain indefinitely (rely on
    /// systemd `TimeoutStopSec` or Kubernetes `terminationGracePeriodSeconds`).
    #[arg(long, env = "DRAIN_TIMEOUT_SECS", default_value_t = 30)]
    pub drain_timeout_secs: u64,

    /// OTLP trace sample rate (0.0 = never, 1.0 = always, 0.1 = 10%).
    /// Only effective when OTLP_ENABLED=true.
    #[arg(long, env = "OTLP_SAMPLE_RATE", default_value_t = 0.1)]
    pub otlp_sample_rate: f64,

    /// Minimum age in seconds for orphaned temp files (`.tmp/` directory) to be
    /// eligible for GC at startup. Files younger than this threshold are assumed
    /// to belong to an in-flight upload from a previous process and are skipped.
    #[arg(long, env = "GC_TEMP_TTL_SECS", default_value_t = 3600)]
    pub gc_temp_ttl_secs: u64,

    /// Minimum age in seconds for an incomplete multipart upload to be eligible
    /// for GC at startup. Sessions older than this threshold are assumed
    /// abandoned and will be deleted. Increase if your workload includes
    /// multi-hour uploads of very large objects.
    #[arg(long, env = "GC_MULTIPART_TTL_SECS", default_value_t = 86400)]
    pub gc_multipart_ttl_secs: u64,
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
            .field("drain_timeout_secs", &self.drain_timeout_secs)
            .field("otlp_sample_rate", &self.otlp_sample_rate)
            .field("gc_temp_ttl_secs", &self.gc_temp_ttl_secs)
            .field("gc_multipart_ttl_secs", &self.gc_multipart_ttl_secs)
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
