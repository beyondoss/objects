//! Download throughput benchmarks against a tempfile-backed `Storage`.
//!
//! Two groups:
//!   - `download`            — single-client sequential reads across payload sizes
//!   - `download_concurrent` — fixed 4 KiB payload, varying concurrency levels

use std::io::Cursor;
use std::sync::Arc;

use beyond_objects_storage::{ObjectMeta, Storage};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use tokio::io::AsyncReadExt;

fn bench_download(c: &mut Criterion) {
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
        ("8MiB", 8 * 1024 * 1024),
        ("16MiB", 16 * 1024 * 1024),
    ];

    for (label, size) in &sizes {
        let payload: Arc<Vec<u8>> = Arc::new((0..*size).map(|i| i as u8).collect());
        let key = format!("blob-{label}");
        rt.block_on(async {
            storage
                .write_object(
                    "bench",
                    &key,
                    Cursor::new(payload.as_slice().to_vec()),
                    ObjectMeta::default(),
                    None,
                )
                .await
                .unwrap();
        });
    }

    let mut group = c.benchmark_group("download");
    for (label, size) in &sizes {
        let key = format!("blob-{label}");
        group.throughput(Throughput::Bytes(*size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(label), size, |b, _| {
            b.to_async(&rt).iter(|| async {
                let (_info, mut file) = storage.open_object("bench", &key).await.unwrap();
                let mut sink = Vec::with_capacity(*size);
                file.read_to_end(&mut sink).await.unwrap();
                std::hint::black_box(sink);
            });
        });
    }
    group.finish();
}

fn bench_download_concurrent(c: &mut Criterion) {
    const SIZE: usize = 4 * 1024;
    const KEY: &str = "blob-4KiB";

    let dir = tempfile::tempdir().expect("tempdir");
    let storage = Arc::new(Storage::new(dir.path()));

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("rt");

    rt.block_on(async {
        tokio::fs::create_dir_all(dir.path().join("bench"))
            .await
            .unwrap();
        let payload: Vec<u8> = (0..SIZE).map(|i| i as u8).collect();
        storage
            .write_object(
                "bench",
                KEY,
                Cursor::new(payload),
                ObjectMeta::default(),
                None,
            )
            .await
            .unwrap();
    });

    let concurrency_levels: Vec<usize> = vec![1, 4, 16, 64];

    let mut group = c.benchmark_group("download_concurrent");
    for &n in &concurrency_levels {
        group.throughput(Throughput::Bytes((n * SIZE) as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.to_async(&rt).iter(|| {
                let storage = Arc::clone(&storage);
                async move {
                    let tasks: Vec<_> = (0..n)
                        .map(|_| {
                            let storage = Arc::clone(&storage);
                            tokio::spawn(async move {
                                let (_info, mut file) =
                                    storage.open_object("bench", KEY).await.unwrap();
                                let mut sink = Vec::with_capacity(SIZE);
                                file.read_to_end(&mut sink).await.unwrap();
                                std::hint::black_box(sink);
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

criterion_group!(benches, bench_download, bench_download_concurrent);
criterion_main!(benches);
