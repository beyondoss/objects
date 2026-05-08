//! Upload throughput benchmarks against a tempfile-backed `Storage`.
//!
//! Three groups:
//!   - `upload`                   — single-client sequential writes across payload sizes
//!   - `upload_concurrent`        — fixed 4 KiB payload, varying concurrency, inline sync
//!   - `upload_concurrent_grouped`— same, but with a 5 ms group-commit linger window

use std::io::Cursor;
use std::sync::Arc;
use std::time::Duration;

use beyond_objects_storage::{ObjectMeta, Storage};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};

fn bench_upload(c: &mut Criterion) {
    let dir = tempfile::tempdir().expect("tempdir");
    let storage = Storage::new(dir.path());

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("rt");

    rt.block_on(async {
        tokio::fs::create_dir_all(dir.path().join("bench"))
            .await
            .unwrap();
    });

    let sizes: Vec<(&str, usize)> = vec![
        ("4KiB", 4 * 1024),
        ("64KiB", 64 * 1024),
        ("1MiB", 1024 * 1024),
    ];

    let mut group = c.benchmark_group("upload");
    for (label, size) in &sizes {
        let payload: Vec<u8> = (0..*size).map(|i| i as u8).collect();
        group.throughput(Throughput::Bytes(*size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(label), size, |b, _| {
            b.to_async(&rt).iter(|| async {
                storage
                    .write_object(
                        "bench",
                        "blob",
                        Cursor::new(payload.clone()),
                        ObjectMeta::default(),
                        None,
                    )
                    .await
                    .unwrap();
            });
        });
    }
    group.finish();
}

fn bench_upload_concurrent(c: &mut Criterion) {
    const SIZE: usize = 4 * 1024;

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("rt");

    let dir = tempfile::tempdir().expect("tempdir");
    let storage = Arc::new(Storage::new(dir.path()));
    rt.block_on(async {
        tokio::fs::create_dir_all(dir.path().join("bench"))
            .await
            .unwrap();
    });

    let payload: Arc<Vec<u8>> = Arc::new((0..SIZE).map(|i| i as u8).collect());
    let concurrency_levels: Vec<usize> = vec![1, 4, 16, 64];

    let mut group = c.benchmark_group("upload_concurrent");
    for &n in &concurrency_levels {
        group.throughput(Throughput::Bytes((n * SIZE) as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.to_async(&rt).iter(|| {
                let storage = Arc::clone(&storage);
                let payload = Arc::clone(&payload);
                async move {
                    let tasks: Vec<_> = (0..n)
                        .map(|i| {
                            let storage = Arc::clone(&storage);
                            let payload = Arc::clone(&payload);
                            tokio::spawn(async move {
                                storage
                                    .write_object(
                                        "bench",
                                        &format!("blob-{i}"),
                                        Cursor::new(payload.as_slice().to_vec()),
                                        ObjectMeta::default(),
                                        None,
                                    )
                                    .await
                                    .unwrap();
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

fn bench_upload_concurrent_grouped(c: &mut Criterion) {
    const SIZE: usize = 4 * 1024;
    const LINGER: Duration = Duration::from_millis(5);

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("rt");

    let dir = tempfile::tempdir().expect("tempdir");
    let storage = Arc::new(rt.block_on(async {
        tokio::fs::create_dir_all(dir.path().join("bench"))
            .await
            .unwrap();
        Storage::with_linger(dir.path(), LINGER)
    }));

    let payload: Arc<Vec<u8>> = Arc::new((0..SIZE).map(|i| i as u8).collect());
    let concurrency_levels: Vec<usize> = vec![1, 4, 16, 64];

    let mut group = c.benchmark_group("upload_concurrent_grouped");
    for &n in &concurrency_levels {
        group.throughput(Throughput::Bytes((n * SIZE) as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.to_async(&rt).iter(|| {
                let storage = Arc::clone(&storage);
                let payload = Arc::clone(&payload);
                async move {
                    let tasks: Vec<_> = (0..n)
                        .map(|i| {
                            let storage = Arc::clone(&storage);
                            let payload = Arc::clone(&payload);
                            tokio::spawn(async move {
                                storage
                                    .write_object(
                                        "bench",
                                        &format!("blob-{i}"),
                                        Cursor::new(payload.as_slice().to_vec()),
                                        ObjectMeta::default(),
                                        None,
                                    )
                                    .await
                                    .unwrap();
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

criterion_group!(
    benches,
    bench_upload,
    bench_upload_concurrent,
    bench_upload_concurrent_grouped
);
criterion_main!(benches);
