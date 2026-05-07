use std::path::PathBuf;

use md5::{Digest, Md5};
use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use uuid::Uuid;

use crate::types::{AccessLevel, ObjectMeta, WriteCondition};
use crate::{Result, Storage, StorageError, xattr};

impl Storage {
    pub async fn write_object(
        &self,
        bucket: &str,
        key: &str,
        mut reader: impl tokio::io::AsyncRead + Unpin + Send,
        meta: ObjectMeta,
        condition: Option<WriteCondition>,
    ) -> Result<String> {
        validate_bucket(bucket)?;
        validate_key(key)?;

        let final_path = self.object_path(bucket, key);
        let tmp_dir = self.data_dir.join(".tmp");
        fs::create_dir_all(&tmp_dir).await?;
        let tmp_path = tmp_dir.join(Uuid::new_v4().to_string());

        let etag = stream_to_tmp(&tmp_path, &mut reader)
            .await
            .inspect_err(|_| {
                let p = tmp_path.clone();
                tokio::spawn(async move {
                    let _ = fs::remove_file(p).await;
                });
            })?;

        let access = meta.access.unwrap_or(AccessLevel::Private);
        xattr::set_object(
            &tmp_path,
            &etag,
            meta.content_type.as_deref(),
            access,
            &meta.user_metadata,
        )
        .inspect_err(|_| {
            let p = tmp_path.clone();
            tokio::spawn(async move {
                let _ = fs::remove_file(p).await;
            });
        })?;

        match &condition {
            Some(WriteCondition::IfNoneMatch) => {
                // Stat-check before rename: there is a narrow TOCTOU window where two
                // concurrent IfNoneMatch writes can both pass this check. On a single-node
                // GlideFS deployment this window is sub-microsecond; the trade-off is
                // accepted for Phase 1. A future upgrade path is `renameat2(RENAME_NOREPLACE)`
                // on Linux, which makes the check-and-rename atomic at the syscall level.
                if final_path.try_exists()? {
                    let _ = fs::remove_file(&tmp_path).await;
                    return Err(StorageError::ObjectExists {
                        bucket: bucket.into(),
                        key: key.into(),
                    });
                }
            }
            Some(WriteCondition::IfMatch(expected)) => {
                match xattr::get(&final_path, xattr::ETAG)? {
                    None => {
                        let _ = fs::remove_file(&tmp_path).await;
                        return Err(StorageError::NotFound {
                            bucket: bucket.into(),
                            key: key.into(),
                        });
                    }
                    Some(actual) if actual.as_slice() != expected.as_bytes() => {
                        let _ = fs::remove_file(&tmp_path).await;
                        return Err(StorageError::EtagMismatch);
                    }
                    _ => {}
                }
            }
            None => {}
        }

        if let Some(parent) = final_path.parent() {
            fs::create_dir_all(parent).await?;
        }
        fs::rename(&tmp_path, &final_path).await?;
        tracing::debug!(bucket, key, etag, "object written");
        Ok(etag)
    }

    pub(crate) fn object_path(&self, bucket: &str, key: &str) -> PathBuf {
        self.data_dir.join(bucket).join(key)
    }
}

async fn stream_to_tmp(
    tmp_path: &std::path::Path,
    reader: &mut (impl tokio::io::AsyncRead + Unpin),
) -> Result<String> {
    let mut file = fs::File::create(tmp_path).await?;
    let mut hasher = Md5::new();
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        let n = reader.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
        file.write_all(&buf[..n]).await?;
    }
    file.flush().await?;
    file.sync_all().await?;
    let digest = hasher.finalize();
    let hex: String = digest.iter().map(|b| format!("{b:02x}")).collect();
    Ok(format!("\"{hex}\""))
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
        let etag = s
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
        let etag = s
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
