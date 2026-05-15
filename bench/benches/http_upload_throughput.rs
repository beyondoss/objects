//! Upload throughput benchmarks through the full axum HTTP stack.
//!
//! Complements `upload_throughput.rs` (storage layer only) to show how much
//! overhead routing, header parsing, and body ingestion add.
//!
//! Sizes span the same range as the download bench so comparisons are direct.
//!
//! Four groups (two transport variants × two request patterns):
//!   - `upload_http`                 — HTTP/1.1, sequential PUTs across payload sizes
//!   - `upload_http_concurrent`      — HTTP/1.1, 4 KiB and 1 MiB, varying concurrency
//!   - `upload_http_h2c`             — HTTP/2 cleartext, sequential PUTs
//!   - `upload_http_h2c_concurrent`  — HTTP/2 cleartext, varying concurrency
//!
//! h1 vs h2c comparison is especially interesting under high concurrency where
//! HTTP/1.1 needs separate connections while h2c multiplexes over one.

use std::sync::Arc;

use beyond_objects::Config;
use beyond_objects_storage::{AccessLevel, Storage};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use reqwest::Client;

const BUCKET: &str = "bench";

struct Server {
    url: String,
    token: String,
    _dir: tempfile::TempDir,
}

fn start_server() -> (Server, tokio::runtime::Runtime) {
    let (tx, rx) = std::sync::mpsc::channel::<(String, String, tempfile::TempDir)>();

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
                .create_bucket(BUCKET, AccessLevel::Private)
                .await
                .unwrap();

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
            };
            let server = beyond_objects::test_support::start(config).await.unwrap();
            tx.send((server.url, server.root_token, dir)).unwrap();
            std::future::pending::<()>().await
        });
    });

    let (url, token, dir) = rx.recv().unwrap();
    let bench_rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    (
        Server {
            url,
            token,
            _dir: dir,
        },
        bench_rt,
    )
}

// ── HTTP/1.1 ─────────────────────────────────────────────────────────────────

fn bench_upload_http(c: &mut Criterion) {
    let (server, rt) = start_server();
    let client = Arc::new(Client::builder().build().unwrap());
    let token = Arc::new(server.token.clone());

    let sizes = [
        ("4KiB", 4 * 1024usize),
        ("64KiB", 64 * 1024),
        ("1MiB", 1024 * 1024),
        ("8MiB", 8 * 1024 * 1024),
        ("16MiB", 16 * 1024 * 1024),
    ];

    let mut group = c.benchmark_group("upload_http");
    for (label, size) in sizes {
        let payload: Arc<Vec<u8>> = Arc::new((0..size).map(|i| i as u8).collect());
        let url = Arc::new(format!("{}/v1/{BUCKET}/{label}", server.url));
        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(label), &url, |b, url| {
            b.to_async(&rt).iter(|| {
                let client = Arc::clone(&client);
                let token = Arc::clone(&token);
                let payload = Arc::clone(&payload);
                let url = Arc::clone(url);
                async move {
                    let resp = client
                        .put(url.as_str())
                        .bearer_auth(token.as_str())
                        .header("content-type", "application/octet-stream")
                        .body(payload.as_slice().to_vec())
                        .send()
                        .await
                        .unwrap();
                    assert!(
                        resp.status().is_success(),
                        "upload failed: {}",
                        resp.status()
                    );
                }
            });
        });
    }
    group.finish();
}

fn bench_upload_http_concurrent(c: &mut Criterion) {
    const SMALL: usize = 4 * 1024;
    const LARGE: usize = 1024 * 1024;

    let (server, rt) = start_server();
    let server = Arc::new(server);
    let client = Arc::new(Client::builder().build().unwrap());
    let concurrency_levels = [1usize, 4, 16, 64];

    for (key, size) in [("4KiB", SMALL), ("1MiB", LARGE)] {
        let payload: Arc<Vec<u8>> = Arc::new((0..size).map(|i| i as u8).collect());
        let mut group = c.benchmark_group(format!("upload_http_concurrent_{key}"));
        for &n in &concurrency_levels {
            group.throughput(Throughput::Bytes((n * size) as u64));
            group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
                b.to_async(&rt).iter(|| {
                    let server = Arc::clone(&server);
                    let client = Arc::clone(&client);
                    let payload = Arc::clone(&payload);
                    async move {
                        let tasks: Vec<_> = (0..n)
                            .map(|i| {
                                let server = Arc::clone(&server);
                                let client = Arc::clone(&client);
                                let payload = Arc::clone(&payload);
                                tokio::spawn(async move {
                                    let url = format!("{}/v1/{BUCKET}/{key}-{i}", server.url);
                                    let resp = client
                                        .put(&url)
                                        .bearer_auth(&server.token)
                                        .header("content-type", "application/octet-stream")
                                        .body(payload.as_slice().to_vec())
                                        .send()
                                        .await
                                        .unwrap();
                                    assert!(
                                        resp.status().is_success(),
                                        "upload failed: {}",
                                        resp.status()
                                    );
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

fn bench_upload_http_h2c(c: &mut Criterion) {
    let (server, rt) = start_server();
    let client = Arc::new(Client::builder().http2_prior_knowledge().build().unwrap());
    let token = Arc::new(server.token.clone());

    let sizes = [
        ("4KiB", 4 * 1024usize),
        ("64KiB", 64 * 1024),
        ("1MiB", 1024 * 1024),
        ("8MiB", 8 * 1024 * 1024),
        ("16MiB", 16 * 1024 * 1024),
    ];

    let mut group = c.benchmark_group("upload_http_h2c");
    for (label, size) in sizes {
        let payload: Arc<Vec<u8>> = Arc::new((0..size).map(|i| i as u8).collect());
        let url = Arc::new(format!("{}/v1/{BUCKET}/{label}", server.url));
        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(label), &url, |b, url| {
            b.to_async(&rt).iter(|| {
                let client = Arc::clone(&client);
                let token = Arc::clone(&token);
                let payload = Arc::clone(&payload);
                let url = Arc::clone(url);
                async move {
                    let resp = client
                        .put(url.as_str())
                        .bearer_auth(token.as_str())
                        .header("content-type", "application/octet-stream")
                        .body(payload.as_slice().to_vec())
                        .send()
                        .await
                        .unwrap();
                    assert!(
                        resp.status().is_success(),
                        "upload failed: {}",
                        resp.status()
                    );
                }
            });
        });
    }
    group.finish();
}

fn bench_upload_http_h2c_concurrent(c: &mut Criterion) {
    const SMALL: usize = 4 * 1024;
    const LARGE: usize = 1024 * 1024;

    let (server, rt) = start_server();
    let server = Arc::new(server);
    // Single h2c client: all concurrent requests multiplex over one connection.
    let client = Arc::new(Client::builder().http2_prior_knowledge().build().unwrap());
    let concurrency_levels = [1usize, 4, 16, 64];

    for (key, size) in [("4KiB", SMALL), ("1MiB", LARGE)] {
        let payload: Arc<Vec<u8>> = Arc::new((0..size).map(|i| i as u8).collect());
        let mut group = c.benchmark_group(format!("upload_http_h2c_concurrent_{key}"));
        for &n in &concurrency_levels {
            group.throughput(Throughput::Bytes((n * size) as u64));
            group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
                b.to_async(&rt).iter(|| {
                    let server = Arc::clone(&server);
                    let client = Arc::clone(&client);
                    let payload = Arc::clone(&payload);
                    async move {
                        let tasks: Vec<_> = (0..n)
                            .map(|i| {
                                let server = Arc::clone(&server);
                                let client = Arc::clone(&client);
                                let payload = Arc::clone(&payload);
                                tokio::spawn(async move {
                                    let url = format!("{}/v1/{BUCKET}/{key}-{i}", server.url);
                                    let resp = client
                                        .put(&url)
                                        .bearer_auth(&server.token)
                                        .header("content-type", "application/octet-stream")
                                        .body(payload.as_slice().to_vec())
                                        .send()
                                        .await
                                        .unwrap();
                                    assert!(
                                        resp.status().is_success(),
                                        "upload failed: {}",
                                        resp.status()
                                    );
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
    bench_upload_http,
    bench_upload_http_concurrent,
    bench_upload_http_h2c,
    bench_upload_http_h2c_concurrent,
);
criterion_main!(benches);
