//! Download throughput benchmarks through the full axum HTTP stack.
//!
//! Complements `download_throughput.rs` (storage layer only) so we can see
//! how much overhead routing, header parsing, and the response body path add.
//!
//! Sizes span both sides of the INLINE_THRESHOLD (4 MiB):
//!   - ≤4 MiB: single spawn_blocking reads entire file (inline path)
//!   - >4 MiB: tokio::fs streaming via ReaderStream
//!
//! Four groups (two transport variants × two request patterns):
//!   - `download_http`                 — HTTP/1.1, sequential GETs across payload sizes
//!   - `download_http_concurrent`      — HTTP/1.1, 4 KiB and 1 MiB, varying concurrency
//!   - `download_http_h2c`             — HTTP/2 cleartext, sequential GETs
//!   - `download_http_h2c_concurrent`  — HTTP/2 cleartext, varying concurrency
//!
//! h1 vs h2c comparison shows whether multiplexing or framing overhead matters
//! for this workload. HOL blocking appears at high concurrency with h1.

use std::io::Cursor;
use std::sync::Arc;

use beyond_objects::Config;
use beyond_objects_storage::{AccessLevel, ObjectMeta, Storage};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use reqwest::Client;

const BUCKET: &str = "bench";

struct Server {
    url: String,
    // Keep the tempdir alive for the entire benchmark run.
    _dir: tempfile::TempDir,
}

/// Start the objects server on a background multi-thread runtime. Returns the
/// base URL and a runtime handle for benchmark tasks (separate runtimes so
/// client and server workers don't share threads).
fn start_server() -> (Server, tokio::runtime::Runtime) {
    let (tx, rx) = std::sync::mpsc::channel::<(String, tempfile::TempDir)>();

    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async move {
            let dir = tempfile::tempdir().unwrap();
            let data_dir = dir.path().join("data");
            let index_dir = dir.path().join("index");
            tokio::fs::create_dir_all(&data_dir).await.unwrap();
            tokio::fs::create_dir_all(&index_dir).await.unwrap();

            let storage = Storage::new(&data_dir);
            storage
                .create_bucket(BUCKET, AccessLevel::Public)
                .await
                .unwrap();
            for (label, size) in [
                ("4KiB", 4 * 1024usize),
                ("64KiB", 64 * 1024),
                ("1MiB", 1024 * 1024),
                ("8MiB", 8 * 1024 * 1024),
                ("16MiB", 16 * 1024 * 1024),
            ] {
                let payload: Vec<u8> = (0..size).map(|i| i as u8).collect();
                storage
                    .write_object(
                        BUCKET,
                        label,
                        Cursor::new(payload),
                        ObjectMeta::default(),
                        None,
                    )
                    .await
                    .unwrap();
            }

            let config = Config {
                objects_root_token: secrecy::Secret::new("bench-token".into()),
                data_dir,
                index_dir,
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
                tls_cert: None,
                tls_key: None,
                tls_ca: None,
                handoff_socket_path: dir.path().join("control.sock"),
            };
            let server = beyond_objects::test_support::start(config).await.unwrap();
            tx.send((server.url, dir)).unwrap();
            std::future::pending::<()>().await
        });
    });

    let (url, dir) = rx.recv().unwrap();
    let bench_rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    (Server { url, _dir: dir }, bench_rt)
}

// ── HTTP/1.1 ─────────────────────────────────────────────────────────────────

fn bench_download_http(c: &mut Criterion) {
    let (server, rt) = start_server();
    let client = Client::builder().build().unwrap();

    let sizes = [
        ("4KiB", 4 * 1024usize),
        ("64KiB", 64 * 1024),
        ("1MiB", 1024 * 1024),
        ("8MiB", 8 * 1024 * 1024),
        ("16MiB", 16 * 1024 * 1024),
    ];

    let mut group = c.benchmark_group("download_http");
    for (label, size) in sizes {
        let url = format!("{}/v1/{BUCKET}/{label}", server.url);
        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(label), &url, |b, url| {
            b.to_async(&rt).iter(|| async {
                let bytes = client.get(url).send().await.unwrap().bytes().await.unwrap();
                std::hint::black_box(bytes);
            });
        });
    }
    group.finish();
}

fn bench_download_http_concurrent(c: &mut Criterion) {
    const SMALL: usize = 4 * 1024;
    const LARGE: usize = 1024 * 1024;

    let (server, rt) = start_server();
    let client = Arc::new(Client::builder().build().unwrap());
    let concurrency_levels = [1usize, 4, 16, 64];

    for (key, size) in [("4KiB", SMALL), ("1MiB", LARGE)] {
        let mut group = c.benchmark_group(format!("download_http_concurrent_{key}"));
        for &n in &concurrency_levels {
            let url = Arc::new(format!("{}/v1/{BUCKET}/{key}", server.url));
            group.throughput(Throughput::Bytes((n * size) as u64));
            group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
                b.to_async(&rt).iter(|| {
                    let client = Arc::clone(&client);
                    let url = Arc::clone(&url);
                    async move {
                        let tasks: Vec<_> = (0..n)
                            .map(|_| {
                                let client = Arc::clone(&client);
                                let url = Arc::clone(&url);
                                tokio::spawn(async move {
                                    let bytes = client
                                        .get(url.as_str())
                                        .send()
                                        .await
                                        .unwrap()
                                        .bytes()
                                        .await
                                        .unwrap();
                                    std::hint::black_box(bytes);
                                })
                            })
                            .collect();
                        for t in tasks {
                            t.await.unwrap();
                        }
                    }
                });
            });
        }
        group.finish();
    }
}

// ── HTTP/2 cleartext (h2c) ───────────────────────────────────────────────────

fn bench_download_http_h2c(c: &mut Criterion) {
    let (server, rt) = start_server();
    let client = Client::builder().http2_prior_knowledge().build().unwrap();

    let sizes = [
        ("4KiB", 4 * 1024usize),
        ("64KiB", 64 * 1024),
        ("1MiB", 1024 * 1024),
        ("8MiB", 8 * 1024 * 1024),
        ("16MiB", 16 * 1024 * 1024),
    ];

    let mut group = c.benchmark_group("download_http_h2c");
    for (label, size) in sizes {
        let url = format!("{}/v1/{BUCKET}/{label}", server.url);
        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(label), &url, |b, url| {
            b.to_async(&rt).iter(|| async {
                let bytes = client.get(url).send().await.unwrap().bytes().await.unwrap();
                std::hint::black_box(bytes);
            });
        });
    }
    group.finish();
}

fn bench_download_http_h2c_concurrent(c: &mut Criterion) {
    const SMALL: usize = 4 * 1024;
    const LARGE: usize = 1024 * 1024;

    let (server, rt) = start_server();
    // Single h2c client: all concurrent requests multiplex over one connection.
    let client = Arc::new(Client::builder().http2_prior_knowledge().build().unwrap());
    let concurrency_levels = [1usize, 4, 16, 64];

    for (key, size) in [("4KiB", SMALL), ("1MiB", LARGE)] {
        let mut group = c.benchmark_group(format!("download_http_h2c_concurrent_{key}"));
        for &n in &concurrency_levels {
            let url = Arc::new(format!("{}/v1/{BUCKET}/{key}", server.url));
            group.throughput(Throughput::Bytes((n * size) as u64));
            group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
                b.to_async(&rt).iter(|| {
                    let client = Arc::clone(&client);
                    let url = Arc::clone(&url);
                    async move {
                        let tasks: Vec<_> = (0..n)
                            .map(|_| {
                                let client = Arc::clone(&client);
                                let url = Arc::clone(&url);
                                tokio::spawn(async move {
                                    let bytes = client
                                        .get(url.as_str())
                                        .send()
                                        .await
                                        .unwrap()
                                        .bytes()
                                        .await
                                        .unwrap();
                                    std::hint::black_box(bytes);
                                })
                            })
                            .collect();
                        for t in tasks {
                            t.await.unwrap();
                        }
                    }
                });
            });
        }
        group.finish();
    }
}

criterion_group!(
    benches,
    bench_download_http,
    bench_download_http_concurrent,
    bench_download_http_h2c,
    bench_download_http_h2c_concurrent,
);
criterion_main!(benches);
