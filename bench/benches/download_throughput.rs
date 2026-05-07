//! Single-client download throughput against a tempfile-backed `Storage`.
//!
//! Phase 2 uses `tokio_util::io::ReaderStream` over `tokio::fs::File`. The plan
//! is to revisit a true `sendfile()` integration only if this benchmark
//! identifies the read path as the constraint (per the Theory of Constraints
//! discipline in CLAUDE.md). On a network-attached filesystem like GlideFS, the
//! storage network is the dominant cost; the userspace memcpy here should be
//! noise relative to network transit.

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

    // Prepare three sizes spanning small / medium / large.
    let sizes: Vec<(&str, usize)> = vec![
        ("4KiB", 4 * 1024),
        ("64KiB", 64 * 1024),
        ("1MiB", 1024 * 1024),
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

criterion_group!(benches, bench_download);
criterion_main!(benches);
