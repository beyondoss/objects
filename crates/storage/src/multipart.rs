//! Multipart uploads (S3-compatible only).
//!
//! On-disk layout:
//!
//! ```text
//! {data_dir}/.multipart/{upload_id}/.meta.json   ← {bucket, key, content_type, access, user_metadata, init_time}
//! {data_dir}/.multipart/{upload_id}/{part_n}     ← raw part bytes
//! ```
//!
//! Each part file carries its quoted-MD5 etag in a `user.etag` xattr — set on
//! `write_part` so `complete_multipart` can assert without re-reading the bytes.
//!
//! The native PUT path (`Storage::write_object`) is the streaming fast path and
//! is what the REST surface uses for any size. Multipart exists solely so AWS
//! SDKs that always-multipart for large files have a working server.
//!
//! Crash recovery:
//! - Crash mid-`write_part`: tmp file in `.tmp/` orphaned, GC'd by `gc_temp_files`.
//! - Crash after `complete_multipart` rename, before `.multipart/{id}` cleanup:
//!   orphan multipart dir, GC'd by `gc_multipart_uploads`.
//! - Crash before rename: nothing visible to readers; `abort_multipart` (or GC)
//!   will eventually clean it up.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use md5::{Digest, Md5};
use serde::{Deserialize, Serialize};
use tokio::fs;
use uuid::Uuid;

use crate::types::{AccessLevel, CompletedPart, MultipartInfo, ObjectMeta, PartInfo};
use crate::write::{stream_to_tmp, validate_bucket, validate_key};
use crate::{Result, Storage, StorageError, xattr};

const MULTIPART_DIRNAME: &str = ".multipart";
const META_FILENAME: &str = ".meta.json";

/// On-disk representation of an in-progress multipart upload's metadata.
/// Held in `{upload_id}/.meta.json`. `init_time` is seconds since the Unix
/// epoch — keeps the storage crate free of a chrono dependency.
#[derive(Serialize, Deserialize)]
struct MultipartMeta {
    bucket: String,
    key: String,
    #[serde(default)]
    content_type: Option<String>,
    #[serde(default)]
    access: Option<AccessLevel>,
    #[serde(default)]
    user_metadata: HashMap<String, String>,
    init_time_secs: u64,
}

impl Storage {
    pub(crate) fn multipart_root(&self) -> PathBuf {
        self.data_dir.join(MULTIPART_DIRNAME)
    }

    fn upload_dir(&self, upload_id: &str) -> PathBuf {
        self.multipart_root().join(upload_id)
    }

    fn part_path(&self, upload_id: &str, part_number: u32) -> PathBuf {
        self.upload_dir(upload_id).join(part_number.to_string())
    }

    fn meta_path(&self, upload_id: &str) -> PathBuf {
        self.upload_dir(upload_id).join(META_FILENAME)
    }

    /// Initiate a multipart upload. Returns a fresh upload_id (UUID v4).
    pub async fn init_multipart(
        &self,
        bucket: &str,
        key: &str,
        meta: ObjectMeta,
    ) -> Result<String> {
        validate_bucket(bucket)?;
        validate_key(key)?;
        let upload_id = Uuid::new_v4().to_string();
        let dir = self.upload_dir(&upload_id);
        fs::create_dir_all(&dir).await?;

        let init_time_secs = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        let body = MultipartMeta {
            bucket: bucket.into(),
            key: key.into(),
            content_type: meta.content_type,
            access: meta.access,
            user_metadata: meta.user_metadata,
            init_time_secs,
        };
        let json = serde_json::to_vec(&body)
            .map_err(|e| StorageError::InvalidValue(format!("multipart meta encode: {e}")))?;
        fs::write(self.meta_path(&upload_id), json).await?;
        Ok(upload_id)
    }

    /// Upload one part. Returns its quoted-MD5 etag. Re-uploading the same
    /// `part_number` overwrites the previous bytes (matches S3 semantics).
    pub async fn write_part(
        &self,
        upload_id: &str,
        part_number: u32,
        mut reader: impl tokio::io::AsyncRead + Unpin + Send,
    ) -> Result<String> {
        if !(1..=10_000).contains(&part_number) {
            return Err(StorageError::InvalidPart(format!(
                "part_number {part_number} out of range 1..=10000"
            )));
        }
        let dir = self.upload_dir(upload_id);
        if !dir.try_exists()? {
            return Err(StorageError::UploadNotFound(upload_id.into()));
        }

        let tmp_dir = self.data_dir.join(".tmp");
        fs::create_dir_all(&tmp_dir).await?;
        let tmp_path = tmp_dir.join(Uuid::new_v4().to_string());

        let (etag, _size) = stream_to_tmp(&tmp_path, &mut reader)
            .await
            .inspect_err(|_| {
                let p = tmp_path.clone();
                tokio::spawn(async move {
                    if let Err(e) = fs::remove_file(&p).await {
                        tracing::warn!(path = %p.display(), err = %e, "temp file cleanup failed");
                    }
                });
            })?;

        xattr::set(&tmp_path, xattr::ETAG, etag.as_bytes()).inspect_err(|_| {
            let p = tmp_path.clone();
            tokio::spawn(async move {
                if let Err(e) = fs::remove_file(&p).await {
                    tracing::warn!(path = %p.display(), err = %e, "temp file cleanup failed");
                }
            });
        })?;

        let final_path = self.part_path(upload_id, part_number);
        fs::rename(&tmp_path, &final_path).await?;
        Ok(etag)
    }

    /// Read part metadata by enumerating files in the upload dir. Skips the
    /// `.meta.json` sidecar and any non-numeric entries.
    pub async fn list_parts(&self, upload_id: &str) -> Result<Vec<PartInfo>> {
        let dir = self.upload_dir(upload_id);
        if !dir.try_exists()? {
            return Err(StorageError::UploadNotFound(upload_id.into()));
        }
        let mut entries = fs::read_dir(&dir).await?;
        let mut parts = Vec::new();
        while let Some(entry) = entries.next_entry().await? {
            let raw_name = entry.file_name();
            let Some(name) = raw_name.to_str() else {
                continue;
            };
            if name == META_FILENAME {
                continue;
            }
            let Ok(number) = name.parse::<u32>() else {
                continue;
            };
            let meta = entry.metadata().await?;
            let etag = xattr::get(&entry.path(), xattr::ETAG)?
                .and_then(|b| String::from_utf8(b).ok())
                .unwrap_or_default();
            parts.push(PartInfo {
                number,
                etag,
                size: meta.len(),
                last_modified: meta.modified()?,
            });
        }
        parts.sort_by_key(|p| p.number);
        Ok(parts)
    }

    /// List in-progress uploads, optionally filtered by bucket and key prefix.
    pub async fn list_multipart_uploads(
        &self,
        bucket: &str,
        key_prefix: Option<&str>,
    ) -> Result<Vec<MultipartInfo>> {
        validate_bucket(bucket)?;
        let root = self.multipart_root();
        if !root.try_exists()? {
            return Ok(Vec::new());
        }
        let mut entries = fs::read_dir(&root).await?;
        let mut uploads = Vec::new();
        while let Some(entry) = entries.next_entry().await? {
            let raw_name = entry.file_name();
            let Some(upload_id) = raw_name.to_str().map(str::to_owned) else {
                continue;
            };
            if !entry.file_type().await?.is_dir() {
                continue;
            }
            let meta_path = entry.path().join(META_FILENAME);
            let bytes = match fs::read(&meta_path).await {
                Ok(b) => b,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(e) => return Err(e.into()),
            };
            let Ok(meta) = serde_json::from_slice::<MultipartMeta>(&bytes) else {
                tracing::warn!(?meta_path, "skipping multipart upload with unreadable meta");
                continue;
            };
            if meta.bucket != bucket {
                continue;
            }
            if let Some(p) = key_prefix
                && !meta.key.starts_with(p)
            {
                continue;
            }
            uploads.push(MultipartInfo {
                upload_id,
                bucket: meta.bucket,
                key: meta.key,
                init_time: SystemTime::UNIX_EPOCH + Duration::from_secs(meta.init_time_secs),
            });
        }
        uploads.sort_by(|a, b| a.key.cmp(&b.key).then(a.upload_id.cmp(&b.upload_id)));
        Ok(uploads)
    }

    /// Abort a multipart upload — best-effort recursive removal of its directory.
    /// Idempotent: succeeds silently when the upload is already gone.
    pub async fn abort_multipart(&self, upload_id: &str) -> Result<()> {
        let dir = self.upload_dir(upload_id);
        match fs::remove_dir_all(&dir).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    /// Concatenate `parts` (in caller-supplied order, must be strictly ascending)
    /// into the final object. Returns the assembled `("{md5_of_part_md5s}-{N}", size)`.
    pub async fn complete_multipart(
        &self,
        upload_id: &str,
        parts: &[CompletedPart],
    ) -> Result<(String, u64)> {
        let dir = self.upload_dir(upload_id);
        if !dir.try_exists()? {
            return Err(StorageError::UploadNotFound(upload_id.into()));
        }
        if parts.is_empty() {
            return Err(StorageError::InvalidPart("no parts supplied".into()));
        }
        let mut last_n: u32 = 0;
        for p in parts {
            if p.number <= last_n {
                return Err(StorageError::InvalidPart(format!(
                    "parts must be in strictly ascending order; got {} after {last_n}",
                    p.number
                )));
            }
            last_n = p.number;
        }

        let meta_bytes = match fs::read(self.meta_path(upload_id)).await {
            Ok(b) => b,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Err(StorageError::UploadNotFound(upload_id.into()));
            }
            Err(e) => return Err(e.into()),
        };
        let meta: MultipartMeta = serde_json::from_slice(&meta_bytes)
            .map_err(|e| StorageError::InvalidValue(format!("multipart meta decode: {e}")))?;

        let tmp_dir = self.data_dir.join(".tmp");
        fs::create_dir_all(&tmp_dir).await?;
        let tmp_path = tmp_dir.join(Uuid::new_v4().to_string());

        let (final_etag, total_size) = assemble_parts(&dir, parts, &tmp_path).await.inspect_err(
            |_| {
                let p = tmp_path.clone();
                tokio::spawn(async move {
                    if let Err(e) = fs::remove_file(&p).await {
                        tracing::warn!(path = %p.display(), err = %e, "temp file cleanup failed");
                    }
                });
            },
        )?;

        xattr::set_object(
            &tmp_path,
            &final_etag,
            meta.content_type.as_deref(),
            meta.access,
            &meta.user_metadata,
        )
        .inspect_err(|_| {
            let p = tmp_path.clone();
            tokio::spawn(async move {
                if let Err(e) = fs::remove_file(&p).await {
                    tracing::warn!(path = %p.display(), err = %e, "temp file cleanup failed");
                }
            });
        })?;

        let final_path = self.data_dir.join(&meta.bucket).join(&meta.key);
        if let Some(parent) = final_path.parent() {
            fs::create_dir_all(parent).await?;
        }
        fs::rename(&tmp_path, &final_path).await?;

        // Best-effort cleanup. If this fails, gc_multipart_uploads will handle it.
        if let Err(e) = fs::remove_dir_all(&dir).await {
            tracing::warn!(upload_id, error = %e, "failed to clean up multipart dir post-complete");
        }
        Ok((final_etag, total_size))
    }

    /// Read a multipart upload's metadata. Used by handlers that need to
    /// know the bucket/key/access for an `upload_id` (e.g. for authorization
    /// on `UploadPart`/`AbortMultipartUpload`).
    pub async fn get_multipart(&self, upload_id: &str) -> Result<MultipartInfo> {
        let bytes = match fs::read(self.meta_path(upload_id)).await {
            Ok(b) => b,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Err(StorageError::UploadNotFound(upload_id.into()));
            }
            Err(e) => return Err(e.into()),
        };
        let meta: MultipartMeta = serde_json::from_slice(&bytes)
            .map_err(|e| StorageError::InvalidValue(format!("multipart meta decode: {e}")))?;
        Ok(MultipartInfo {
            upload_id: upload_id.into(),
            bucket: meta.bucket,
            key: meta.key,
            init_time: SystemTime::UNIX_EPOCH + Duration::from_secs(meta.init_time_secs),
        })
    }

    /// Delete multipart upload directories whose meta sidecar is older than `max_age`.
    /// Mirrors `gc_temp_files`. Returns the count removed.
    pub async fn gc_multipart_uploads(&self, max_age: Duration) -> Result<usize> {
        let root = self.multipart_root();
        if !root.try_exists()? {
            return Ok(0);
        }
        let cutoff = SystemTime::now()
            .checked_sub(max_age)
            .unwrap_or(SystemTime::UNIX_EPOCH);
        let mut entries = fs::read_dir(&root).await?;
        let mut removed = 0usize;
        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            let meta_path = path.join(META_FILENAME);
            let mtime = match fs::metadata(&meta_path).await {
                Ok(m) => m.modified().ok(),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
                Err(e) => {
                    tracing::warn!(?path, error = %e, "gc: failed to stat multipart meta");
                    continue;
                }
            };
            // No meta sidecar (or unreadable) and the dir is older than cutoff →
            // treat as orphan. With meta: only delete if the meta is older.
            let stale = match mtime {
                Some(t) => t < cutoff,
                None => entry
                    .metadata()
                    .await
                    .ok()
                    .and_then(|m| m.modified().ok())
                    .map(|t| t < cutoff)
                    .unwrap_or(false),
            };
            if stale {
                match fs::remove_dir_all(&path).await {
                    Ok(()) => removed += 1,
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                    Err(e) => {
                        tracing::warn!(?path, error = %e, "gc: failed to remove multipart dir");
                    }
                }
            }
        }
        if removed > 0 {
            tracing::info!(removed, "gc: removed orphaned multipart uploads");
        }
        Ok(removed)
    }
}

async fn assemble_parts(
    upload_dir: &Path,
    parts: &[CompletedPart],
    tmp_path: &Path,
) -> Result<(String, u64)> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let mut out = fs::File::create(tmp_path).await?;
    let mut total_size: u64 = 0;
    let mut composite = Md5::new();
    let mut buf = vec![0u8; 64 * 1024];

    for part in parts {
        let path = upload_dir.join(part.number.to_string());
        let stored_etag = xattr::get(&path, xattr::ETAG)?
            .and_then(|b| String::from_utf8(b).ok())
            .ok_or_else(|| {
                StorageError::InvalidPart(format!("part {} not uploaded", part.number))
            })?;
        if stored_etag != part.etag {
            return Err(StorageError::InvalidPart(format!(
                "part {} etag mismatch (stored {stored_etag}, supplied {})",
                part.number, part.etag
            )));
        }
        // The raw 16 bytes go into the composite digest; the etag is "hex_md5".
        let raw_md5 = stored_etag
            .trim_matches('"')
            .as_bytes()
            .chunks(2)
            .map(|c| {
                std::str::from_utf8(c)
                    .ok()
                    .and_then(|s| u8::from_str_radix(s, 16).ok())
                    .ok_or_else(|| {
                        StorageError::InvalidPart(format!(
                            "part {} stored etag not hex: {stored_etag}",
                            part.number
                        ))
                    })
            })
            .collect::<Result<Vec<u8>>>()?;
        composite.update(&raw_md5);

        let mut input = fs::File::open(&path).await?;
        loop {
            let n = input.read(&mut buf).await?;
            if n == 0 {
                break;
            }
            total_size += n as u64;
            out.write_all(&buf[..n]).await?;
        }
    }
    out.flush().await?;
    out.sync_all().await?;

    let composite_hex = composite
        .finalize()
        .iter()
        .fold(String::with_capacity(32), |mut s, b| {
            use std::fmt::Write as _;
            let _ = write!(s, "{b:02x}");
            s
        });
    Ok((format!("\"{composite_hex}-{}\"", parts.len()), total_size))
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;

    async fn make_storage() -> (Storage, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let s = Storage::new(dir.path());
        fs::create_dir_all(s.data_dir.join("bucket")).await.unwrap();
        (s, dir)
    }

    #[tokio::test]
    async fn happy_path() {
        let (s, _dir) = make_storage().await;
        let upload_id = s
            .init_multipart(
                "bucket",
                "big.bin",
                ObjectMeta {
                    content_type: Some("application/octet-stream".into()),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        let etag1 = s
            .write_part(&upload_id, 1, Cursor::new(b"hello "))
            .await
            .unwrap();
        let etag2 = s
            .write_part(&upload_id, 2, Cursor::new(b"world"))
            .await
            .unwrap();

        let parts = s.list_parts(&upload_id).await.unwrap();
        assert_eq!(parts.len(), 2);
        assert_eq!(parts[0].etag, etag1);
        assert_eq!(parts[1].etag, etag2);

        let (final_etag, size) = s
            .complete_multipart(
                &upload_id,
                &[
                    CompletedPart {
                        number: 1,
                        etag: etag1,
                    },
                    CompletedPart {
                        number: 2,
                        etag: etag2,
                    },
                ],
            )
            .await
            .unwrap();

        assert!(final_etag.ends_with("-2\""));
        assert_eq!(size, b"hello world".len() as u64);

        let info = s.head_object("bucket", "big.bin").await.unwrap();
        assert_eq!(info.size, size);
        assert_eq!(info.etag, final_etag);
        assert_eq!(
            info.content_type.as_deref(),
            Some("application/octet-stream")
        );

        // Multipart dir cleaned up.
        assert!(!s.upload_dir(&upload_id).exists());
    }

    #[tokio::test]
    async fn abort_is_idempotent() {
        let (s, _dir) = make_storage().await;
        let upload_id = s
            .init_multipart("bucket", "x", ObjectMeta::default())
            .await
            .unwrap();
        s.write_part(&upload_id, 1, Cursor::new(b"abc"))
            .await
            .unwrap();
        s.abort_multipart(&upload_id).await.unwrap();
        s.abort_multipart(&upload_id).await.unwrap();
        assert!(matches!(
            s.list_parts(&upload_id).await.unwrap_err(),
            StorageError::UploadNotFound(_)
        ));
    }

    #[tokio::test]
    async fn upload_not_found() {
        let (s, _dir) = make_storage().await;
        assert!(matches!(
            s.write_part("nonsuch", 1, Cursor::new(b""))
                .await
                .unwrap_err(),
            StorageError::UploadNotFound(_)
        ));
        assert!(matches!(
            s.complete_multipart(
                "nonsuch",
                &[CompletedPart {
                    number: 1,
                    etag: "\"x\"".into()
                }]
            )
            .await
            .unwrap_err(),
            StorageError::UploadNotFound(_)
        ));
    }

    #[tokio::test]
    async fn part_number_out_of_range() {
        let (s, _dir) = make_storage().await;
        let upload_id = s
            .init_multipart("bucket", "x", ObjectMeta::default())
            .await
            .unwrap();
        assert!(matches!(
            s.write_part(&upload_id, 0, Cursor::new(b""))
                .await
                .unwrap_err(),
            StorageError::InvalidPart(_)
        ));
        assert!(matches!(
            s.write_part(&upload_id, 10_001, Cursor::new(b""))
                .await
                .unwrap_err(),
            StorageError::InvalidPart(_)
        ));
    }

    #[tokio::test]
    async fn complete_rejects_out_of_order() {
        let (s, _dir) = make_storage().await;
        let upload_id = s
            .init_multipart("bucket", "x", ObjectMeta::default())
            .await
            .unwrap();
        let e1 = s
            .write_part(&upload_id, 1, Cursor::new(b"a"))
            .await
            .unwrap();
        let e2 = s
            .write_part(&upload_id, 2, Cursor::new(b"b"))
            .await
            .unwrap();
        let err = s
            .complete_multipart(
                &upload_id,
                &[
                    CompletedPart {
                        number: 2,
                        etag: e2,
                    },
                    CompletedPart {
                        number: 1,
                        etag: e1,
                    },
                ],
            )
            .await
            .unwrap_err();
        assert!(matches!(err, StorageError::InvalidPart(_)));
    }

    #[tokio::test]
    async fn complete_rejects_etag_mismatch() {
        let (s, _dir) = make_storage().await;
        let upload_id = s
            .init_multipart("bucket", "x", ObjectMeta::default())
            .await
            .unwrap();
        s.write_part(&upload_id, 1, Cursor::new(b"a"))
            .await
            .unwrap();
        let err = s
            .complete_multipart(
                &upload_id,
                &[CompletedPart {
                    number: 1,
                    etag: "\"deadbeef\"".into(),
                }],
            )
            .await
            .unwrap_err();
        assert!(matches!(err, StorageError::InvalidPart(_)));
    }

    #[tokio::test]
    async fn list_uploads_filters_by_bucket_and_prefix() {
        let (s, _dir) = make_storage().await;
        fs::create_dir_all(s.data_dir.join("other")).await.unwrap();

        let _ = s
            .init_multipart("bucket", "avatars/a", ObjectMeta::default())
            .await
            .unwrap();
        let _ = s
            .init_multipart("bucket", "avatars/b", ObjectMeta::default())
            .await
            .unwrap();
        let _ = s
            .init_multipart("bucket", "other-key", ObjectMeta::default())
            .await
            .unwrap();
        let _ = s
            .init_multipart("other", "x", ObjectMeta::default())
            .await
            .unwrap();

        let all_bucket = s.list_multipart_uploads("bucket", None).await.unwrap();
        assert_eq!(all_bucket.len(), 3);

        let avatars = s
            .list_multipart_uploads("bucket", Some("avatars/"))
            .await
            .unwrap();
        assert_eq!(avatars.len(), 2);
        assert_eq!(avatars[0].key, "avatars/a");
        assert_eq!(avatars[1].key, "avatars/b");
    }

    #[tokio::test]
    async fn gc_removes_old_uploads() {
        let (s, _dir) = make_storage().await;
        let upload_id = s
            .init_multipart("bucket", "x", ObjectMeta::default())
            .await
            .unwrap();
        // Sleep so the meta mtime is strictly before the gc cutoff.
        tokio::time::sleep(Duration::from_millis(50)).await;
        let removed = s.gc_multipart_uploads(Duration::ZERO).await.unwrap();
        assert_eq!(removed, 1);
        assert!(!s.upload_dir(&upload_id).exists());
    }

    #[tokio::test]
    async fn gc_keeps_recent_uploads() {
        let (s, _dir) = make_storage().await;
        let upload_id = s
            .init_multipart("bucket", "x", ObjectMeta::default())
            .await
            .unwrap();
        let removed = s
            .gc_multipart_uploads(Duration::from_secs(86_400))
            .await
            .unwrap();
        assert_eq!(removed, 0);
        assert!(s.upload_dir(&upload_id).exists());
    }

    #[tokio::test]
    async fn parts_are_overwritable() {
        let (s, _dir) = make_storage().await;
        let upload_id = s
            .init_multipart("bucket", "x", ObjectMeta::default())
            .await
            .unwrap();
        let _ = s
            .write_part(&upload_id, 1, Cursor::new(b"first"))
            .await
            .unwrap();
        let etag2 = s
            .write_part(&upload_id, 1, Cursor::new(b"second"))
            .await
            .unwrap();
        let parts = s.list_parts(&upload_id).await.unwrap();
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0].etag, etag2);
        assert_eq!(parts[0].size, b"second".len() as u64);
    }
}
