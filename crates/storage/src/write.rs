use std::path::PathBuf;

use md5::{Digest, Md5};
use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use uuid::Uuid;

use crate::types::{AccessLevel, ObjectMeta, WriteCondition};
use crate::{Result, Storage, StorageError, xattr};

impl Storage {
    /// Update the access xattr on an existing object. Returns `NotFound` if the
    /// object does not exist. Used by the metadata-PATCH path.
    pub async fn update_object_access(
        &self,
        bucket: &str,
        key: &str,
        access: AccessLevel,
    ) -> Result<()> {
        validate_bucket(bucket)?;
        validate_key(key)?;
        let path = self.object_path(bucket, key);
        if !path.try_exists()? {
            return Err(StorageError::NotFound {
                bucket: bucket.into(),
                key: key.into(),
            });
        }
        xattr::set(&path, xattr::ACCESS, access.as_str().as_bytes())
    }
}

impl Storage {
    pub async fn write_object(
        &self,
        bucket: &str,
        key: &str,
        mut reader: impl tokio::io::AsyncRead + Unpin + Send,
        meta: ObjectMeta,
        condition: Option<WriteCondition>,
    ) -> Result<(String, u64)> {
        validate_bucket(bucket)?;
        validate_key(key)?;

        let final_path = self.object_path(bucket, key);
        fs::create_dir_all(&self.tmp_dir).await?;
        let tmp_path = self.tmp_dir.join(Uuid::new_v4().to_string());

        let (etag, size, file) = stream_to_tmp(&tmp_path, &mut reader)
            .await
            .inspect_err(|_| {
                let p = tmp_path.clone();
                tokio::spawn(async move {
                    if let Err(e) = fs::remove_file(&p).await {
                        tracing::warn!(path = %p.display(), err = %e, "temp file cleanup failed");
                    }
                });
            })?;

        self.sync
            .sync_file(file)
            .await
            .map_err(StorageError::Io)
            .inspect_err(|_| {
                let p = tmp_path.clone();
                tokio::spawn(async move {
                    if let Err(e) = fs::remove_file(&p).await {
                        tracing::warn!(path = %p.display(), err = %e, "temp file cleanup failed");
                    }
                });
            })?;

        xattr::set_object(
            &tmp_path,
            &etag,
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

        match &condition {
            Some(WriteCondition::IfNoneMatch) => {
                // Stat-check before rename: there is a narrow TOCTOU window where two
                // concurrent IfNoneMatch writes can both pass this check. On a single-node
                // GlideFS deployment this window is sub-microsecond; the trade-off is
                // accepted for Phase 1. A future upgrade path is `renameat2(RENAME_NOREPLACE)`
                // on Linux, which makes the check-and-rename atomic at the syscall level.
                // TODO: upgrade to renameat2(RENAME_NOREPLACE) on Linux for atomic CAS semantics.
                if final_path.try_exists()? {
                    if let Err(e) = fs::remove_file(&tmp_path).await {
                        tracing::warn!(path = %tmp_path.display(), err = %e, "temp file cleanup failed");
                    }
                    return Err(StorageError::ObjectExists {
                        bucket: bucket.into(),
                        key: key.into(),
                    });
                }
            }
            Some(WriteCondition::IfMatch(expected)) => {
                match xattr::get(&final_path, xattr::ETAG)? {
                    None => {
                        if let Err(e) = fs::remove_file(&tmp_path).await {
                            tracing::warn!(path = %tmp_path.display(), err = %e, "temp file cleanup failed");
                        }
                        return Err(StorageError::NotFound {
                            bucket: bucket.into(),
                            key: key.into(),
                        });
                    }
                    Some(actual) if actual.as_slice() != expected.as_bytes() => {
                        if let Err(e) = fs::remove_file(&tmp_path).await {
                            tracing::warn!(path = %tmp_path.display(), err = %e, "temp file cleanup failed");
                        }
                        return Err(StorageError::EtagMismatch);
                    }
                    _ => {}
                }
            }
            None => {}
        }

        // Only needed for keys with path separators; bucket dir already exists for flat keys.
        if key.contains('/')
            && let Some(parent) = final_path.parent()
        {
            fs::create_dir_all(parent).await?;
        }
        fs::rename(&tmp_path, &final_path).await?;
        tracing::debug!(bucket, key, etag, size, "object written");
        Ok((etag, size))
    }

    pub(crate) fn object_path(&self, bucket: &str, key: &str) -> PathBuf {
        self.data_dir.join(bucket).join(key)
    }
}

/// Stream `reader` to a temp file, returning `(etag, size, file)`.
///
/// The file is flushed but not synced — the caller is responsible for calling
/// `sync_data()` (directly or via `SyncGroup`) before making the file visible.
pub(crate) async fn stream_to_tmp(
    tmp_path: &std::path::Path,
    reader: &mut (impl tokio::io::AsyncRead + Unpin),
) -> Result<(String, u64, fs::File)> {
    let file = fs::File::create(tmp_path).await?;
    let mut buf_file = BufWriter::with_capacity(256 * 1024, file);
    let mut hasher = Md5::new();
    let mut buf = vec![0u8; 64 * 1024];
    let mut total: u64 = 0;
    loop {
        let n = reader.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        total += n as u64;
        hasher.update(&buf[..n]);
        buf_file.write_all(&buf[..n]).await?;
    }
    buf_file.flush().await?;
    let file = buf_file.into_inner();
    let etag = format!("\"{}\"", hex::encode(hasher.finalize()));
    Ok((etag, total, file))
}

pub(crate) fn validate_key(key: &str) -> Result<()> {
    if key.is_empty()
        || key.contains('\0')
        || key.starts_with('/')
        || key.split('/').any(|c| c == "..")
    {
        return Err(StorageError::InvalidKey(key.into()));
    }
    Ok(())
}

pub(crate) fn validate_bucket(name: &str) -> Result<()> {
    if name.is_empty() || name.starts_with('.') || name.contains('/') || name.contains('\0') {
        return Err(StorageError::InvalidKey(name.into()));
    }
    Ok(())
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
    async fn write_and_head() {
        let (s, _dir) = make_storage().await;
        let (etag, size) = s
            .write_object(
                "bucket",
                "hello.txt",
                Cursor::new(b"hello"),
                ObjectMeta::default(),
                None,
            )
            .await
            .unwrap();
        assert!(etag.starts_with('"'));
        assert_eq!(size, 5);
        let info = s.head_object("bucket", "hello.txt").await.unwrap();
        assert_eq!(info.size, 5);
        assert_eq!(info.etag, etag);
    }

    #[tokio::test]
    async fn if_none_match_blocks_overwrite() {
        let (s, _dir) = make_storage().await;
        s.write_object(
            "bucket",
            "f.txt",
            Cursor::new(b"v1"),
            ObjectMeta::default(),
            None,
        )
        .await
        .unwrap();
        let err = s
            .write_object(
                "bucket",
                "f.txt",
                Cursor::new(b"v2"),
                ObjectMeta::default(),
                Some(WriteCondition::IfNoneMatch),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, StorageError::ObjectExists { .. }));
    }

    #[tokio::test]
    async fn if_match_blocks_stale_update() {
        let (s, _dir) = make_storage().await;
        let (etag, _size) = s
            .write_object(
                "bucket",
                "f.txt",
                Cursor::new(b"v1"),
                ObjectMeta::default(),
                None,
            )
            .await
            .unwrap();
        let err = s
            .write_object(
                "bucket",
                "f.txt",
                Cursor::new(b"v2"),
                ObjectMeta::default(),
                Some(WriteCondition::IfMatch("\"wrong_etag\"".into())),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, StorageError::EtagMismatch));

        // correct etag succeeds
        s.write_object(
            "bucket",
            "f.txt",
            Cursor::new(b"v2"),
            ObjectMeta::default(),
            Some(WriteCondition::IfMatch(etag)),
        )
        .await
        .unwrap();
    }

    #[test]
    fn key_validation() {
        assert!(validate_key("").is_err());
        assert!(validate_key("a/../../etc/passwd").is_err());
        assert!(validate_key("/absolute").is_err());
        assert!(validate_key("a\0b").is_err());
        assert!(validate_key("valid/nested/key.txt").is_ok());
    }

    #[test]
    fn bucket_validation() {
        assert!(validate_bucket("").is_err());
        assert!(validate_bucket("..").is_err());
        assert!(validate_bucket(".hidden").is_err());
        assert!(validate_bucket("a/b").is_err());
        assert!(validate_bucket("a\0b").is_err());
        assert!(validate_bucket("images").is_ok());
        assert!(validate_bucket("my-bucket").is_ok());
    }

    #[tokio::test]
    async fn traversal_bucket_rejected() {
        let (s, _dir) = make_storage().await;
        let err = s
            .write_object(
                "..",
                "key.txt",
                Cursor::new(b"x"),
                ObjectMeta::default(),
                None,
            )
            .await
            .unwrap_err();
        assert!(matches!(err, StorageError::InvalidKey(_)));
    }
}
