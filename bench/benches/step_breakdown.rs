//! Step-by-step breakdown of the GET and PUT hot paths.
//!
//! Theory of Constraints: measure each operation in isolation before deciding
//! what to fix. Every group is a hypothesis about where time is actually spent.
//!
//! GET groups:
//!   - `get_step/tokio_open`          — tokio::fs::File::open (1 spawn_blocking)
//!   - `get_step/tokio_metadata`      — open + file.metadata() (2 spawn_blocking)
//!   - `get_step/open_meta_xattr`     — current open_object: 2 implicit + 1 sync xattr
//!   - `get_step/spawn_blocking_noop` — pure spawn_blocking dispatch cost (empty closure)
//!   - `get_step/mmap_only`           — open + into_std + spawn_blocking(mmap)
//!   - `get_step/single_blocking`     — proposed: 1 spawn_blocking for open+meta+xattr+mmap
//!
//! PUT groups:
//!   - `put_step/write_4k`            — BufWriter write + flush (no fsync)
//!   - `put_step/fdatasync`           — write + flush + fdatasync
//!   - `put_step/xattr_set`           — setxattr (sync syscall on async thread)
//!   - `put_step/rename`              — fs::rename (1 spawn_blocking)
//!   - `put_step/write_fsync_rename`  — write + fdatasync + rename (full non-xattr PUT)

use std::io::Cursor;
use std::os::unix::io::AsRawFd;
use std::path::PathBuf;
use std::sync::Arc;

use beyond_objects_storage::{ObjectMeta, Storage};
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use tokio::io::{AsyncWriteExt, BufWriter};

const SIZE_4K: usize = 4 * 1024;

// ── helpers ───────────────────────────────────────────────────────────────────

fn setup(rt: &tokio::runtime::Runtime) -> (Storage, tempfile::TempDir, PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let data_dir = dir.path().to_path_buf();
    let storage = Storage::new(&data_dir);
    rt.block_on(async {
        tokio::fs::create_dir_all(data_dir.join("bench"))
            .await
            .unwrap();
        tokio::fs::create_dir_all(data_dir.join(".tmp"))
            .await
            .unwrap();
    });
    (storage, dir, data_dir)
}

//── GET breakdown ─────────────────────────────────────────────────────────────

fn bench_get_steps(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let (storage, _dir, data_dir) = setup(&rt);

    // Write the object and build the path directly from data_dir
    let payload: Vec<u8> = (0..SIZE_4K).map(|i| i as u8).collect();
    rt.block_on(async {
        storage
            .write_object(
                "bench",
                "obj",
                Cursor::new(payload),
                ObjectMeta::default(),
                None,
            )
            .await
            .unwrap();
    });
    let path = Arc::new(data_dir.join("bench").join("obj"));

    let mut group = c.benchmark_group("get_step");
    group.throughput(Throughput::Elements(1));

    // 1. tokio::fs::File::open — one implicit spawn_blocking dispatch
    group.bench_function("tokio_open", |b| {
        let path = Arc::clone(&path);
        b.to_async(&rt).iter(|| async {
            let f = tokio::fs::File::open(path.as_ref()).await.unwrap();
            std::hint::black_box(f);
        });
    });

    // 2. open + metadata — two implicit spawn_blocking dispatches
    group.bench_function("tokio_metadata", |b| {
        let path = Arc::clone(&path);
        b.to_async(&rt).iter(|| async {
            let f = tokio::fs::File::open(path.as_ref()).await.unwrap();
            let m = f.metadata().await.unwrap();
            std::hint::black_box((f, m));
        });
    });

    // 3. open + metadata + xattr — the current open_object path.
    //    fgetxattr runs synchronously on the async thread.
    group.bench_function("open_meta_xattr", |b| {
        let path = Arc::clone(&path);
        b.to_async(&rt).iter(|| async {
            let f = tokio::fs::File::open(path.as_ref()).await.unwrap();
            let m = f.metadata().await.unwrap();
            let _fd = f.as_raw_fd();
            let attrs = xattr::get(path.as_ref(), "user.etag").unwrap();
            std::hint::black_box((f, m, attrs));
        });
    });

    // 4. spawn_blocking dispatch cost alone — empty closure measures pure overhead
    group.bench_function("spawn_blocking_noop", |b| {
        b.to_async(&rt).iter(|| async {
            let v = tokio::task::spawn_blocking(|| 42u64).await.unwrap();
            std::hint::black_box(v);
        });
    });

    // 5. open + into_std + spawn_blocking(mmap) — the full current GET path
    group.bench_function("mmap_only", |b| {
        let path = Arc::clone(&path);
        b.to_async(&rt).iter(|| async {
            let f = tokio::fs::File::open(path.as_ref()).await.unwrap();
            let std_f = f.into_std().await;
            let m = tokio::task::spawn_blocking(move || {
                unsafe { memmap2::MmapOptions::new().map(&std_f) }.unwrap()
            })
            .await
            .unwrap();
            std::hint::black_box(m);
        });
    });

    // 6. Proposed: one spawn_blocking does open + metadata + xattr + mmap.
    //    Should save ~2 dispatch round-trips vs the current path.
    group.bench_function("single_blocking", |b| {
        let path = Arc::clone(&path);
        b.to_async(&rt).iter(|| async {
            let p = Arc::clone(&path);
            let result = tokio::task::spawn_blocking(move || {
                let f = std::fs::File::open(p.as_ref()).unwrap();
                let m = f.metadata().unwrap();
                let attrs = xattr::get(p.as_ref(), "user.etag").unwrap();
                let mmap = unsafe { memmap2::MmapOptions::new().map(&f) }.unwrap();
                (m, attrs, mmap)
            })
            .await
            .unwrap();
            std::hint::black_box(result);
        });
    });

    group.finish();
}

// ── PUT breakdown ─────────────────────────────────────────────────────────────

fn bench_put_steps(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let (_storage, _dir, data_dir) = setup(&rt);
    let payload: Arc<[u8]> = (0..SIZE_4K as u8).collect::<Vec<u8>>().into();
    let tmp_dir = Arc::new(data_dir.join(".tmp"));
    let final_dir = Arc::new(data_dir.join("bench"));

    let mut group = c.benchmark_group("put_step");
    group.throughput(Throughput::Bytes(SIZE_4K as u64));

    // 1. BufWriter write + flush — no fsync, no xattr, no rename
    group.bench_function("write_4k", |b| {
        let payload = Arc::clone(&payload);
        let tmp_dir = Arc::clone(&tmp_dir);
        b.to_async(&rt).iter(|| async {
            let path: PathBuf = tmp_dir.join(uuid::Uuid::new_v4().to_string());
            let f = tokio::fs::File::create(&path).await.unwrap();
            let mut w = BufWriter::with_capacity(256 * 1024, f);
            w.write_all(&payload).await.unwrap();
            w.flush().await.unwrap();
            let _ = tokio::fs::remove_file(&path).await;
        });
    });

    // 2. Write + fdatasync — isolates the sync cost on top of write
    group.bench_function("fdatasync", |b| {
        let payload = Arc::clone(&payload);
        let tmp_dir = Arc::clone(&tmp_dir);
        b.to_async(&rt).iter(|| async {
            let path: PathBuf = tmp_dir.join(uuid::Uuid::new_v4().to_string());
            let f = tokio::fs::File::create(&path).await.unwrap();
            let mut w = BufWriter::with_capacity(256 * 1024, f);
            w.write_all(&payload).await.unwrap();
            w.flush().await.unwrap();
            let f = w.into_inner();
            f.sync_data().await.unwrap();
            let _ = tokio::fs::remove_file(&path).await;
        });
    });

    // 3. setxattr — sync syscall currently called on the async thread after fsync
    group.bench_function("xattr_set", |b| {
        let tmp_dir = Arc::clone(&tmp_dir);
        b.to_async(&rt).iter(|| async {
            let path: PathBuf = tmp_dir.join(uuid::Uuid::new_v4().to_string());
            tokio::fs::write(&path, b"x").await.unwrap();
            xattr::set(&path, "user.etag", b"\"abc123\"").unwrap();
            let _ = tokio::fs::remove_file(&path).await;
        });
    });

    // 4. rename — one implicit spawn_blocking
    group.bench_function("rename", |b| {
        let tmp_dir = Arc::clone(&tmp_dir);
        b.to_async(&rt).iter(|| async {
            let src: PathBuf = tmp_dir.join(uuid::Uuid::new_v4().to_string());
            let dst: PathBuf = tmp_dir.join(uuid::Uuid::new_v4().to_string());
            tokio::fs::write(&src, b"x").await.unwrap();
            tokio::fs::rename(&src, &dst).await.unwrap();
            let _ = tokio::fs::remove_file(&dst).await;
        });
    });

    // 5. write + fdatasync + rename — the full PUT sequence minus xattr
    group.bench_function("write_fsync_rename", |b| {
        let payload = Arc::clone(&payload);
        let tmp_dir = Arc::clone(&tmp_dir);
        let final_dir = Arc::clone(&final_dir);
        b.to_async(&rt).iter(|| async {
            let tmp: PathBuf = tmp_dir.join(uuid::Uuid::new_v4().to_string());
            let dst: PathBuf = final_dir.join(uuid::Uuid::new_v4().to_string());
            let f = tokio::fs::File::create(&tmp).await.unwrap();
            let mut w = BufWriter::with_capacity(256 * 1024, f);
            w.write_all(&payload).await.unwrap();
            w.flush().await.unwrap();
            let f = w.into_inner();
            f.sync_data().await.unwrap();
            tokio::fs::rename(&tmp, &dst).await.unwrap();
            let _ = tokio::fs::remove_file(&dst).await;
        });
    });

    group.finish();
}

criterion_group!(benches, bench_get_steps, bench_put_steps);
criterion_main!(benches);
