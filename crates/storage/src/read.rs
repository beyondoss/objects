use tokio::fs;

use crate::types::ObjectInfo;
use crate::write::{validate_bucket, validate_key};
use crate::{Result, Storage, StorageError, xattr};

impl Storage {
    pub async fn head_object(&self, bucket: &str, key: &str) -> Result<ObjectInfo> {
        validate_bucket(bucket)?;
        validate_key(key)?;
        let path = self.data_dir.join(bucket).join(key);
        let meta = fs::metadata(&path).await.map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                StorageError::NotFound {
                    bucket: bucket.into(),
                    key: key.into(),
                }
            } else {
                e.into()
            }
        })?;
        let (etag, content_type, access, user_metadata) = xattr::read_object(&path)?;
        Ok(ObjectInfo {
            size: meta.len(),
            etag,
            last_modified: meta.modified()?,
            content_type,
            access,
            user_metadata,
        })
    }

    /// Returns object info and an open file handle. Caller uses the file for sendfile.
    pub async fn open_object(&self, bucket: &str, key: &str) -> Result<(ObjectInfo, fs::File)> {
        let info = self.head_object(bucket, key).await?;
        let path = self.data_dir.join(bucket).join(key);
        let file = fs::File::open(&path).await?;
        Ok((info, file))
    }

    pub async fn delete_object(&self, bucket: &str, key: &str) -> Result<()> {
        validate_bucket(bucket)?;
        validate_key(key)?;
        let path = self.data_dir.join(bucket).join(key);
        fs::remove_file(&path).await.map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                StorageError::NotFound {
                    bucket: bucket.into(),
                    key: key.into(),
                }
            } else {
                e.into()
            }
        })
    }

    /// Copy within or across buckets; returns the etag (same as source).
    ///
    /// Note: `tokio::fs::copy` does not preserve xattrs — we re-read them from the
    /// source and set them on the destination explicitly.
    pub async fn copy_object(
        &self,
        src_bucket: &str,
        src_key: &str,
        dst_bucket: &str,
        dst_key: &str,
    ) -> Result<String> {
        validate_bucket(src_bucket)?;
        validate_key(src_key)?;
        validate_bucket(dst_bucket)?;
        validate_key(dst_key)?;
        let src = self.data_dir.join(src_bucket).join(src_key);
        let dst = self.data_dir.join(dst_bucket).join(dst_key);
        // TOCTOU: exists-check then copy has a narrow race with concurrent deletes.
        // Accepted for Phase 1 (single-node); a future path is open-then-copy-by-fd.
        if !src.try_exists()? {
            return Err(StorageError::NotFound {
                bucket: src_bucket.into(),
                key: src_key.into(),
            });
        }
        if let Some(p) = dst.parent() {
            fs::create_dir_all(p).await?;
        }
        fs::copy(&src, &dst).await?;
        let (etag, content_type, access, user_metadata) = xattr::read_object(&src)?;
        xattr::set_object(&dst, &etag, content_type.as_deref(), access, &user_metadata)?;
        Ok(etag)
    }

    /// Rename within same bucket or across buckets. Atomic when on the same volume.
    pub async fn move_object(
        &self,
        src_bucket: &str,
        src_key: &str,
        dst_bucket: &str,
        dst_key: &str,
    ) -> Result<()> {
        validate_bucket(src_bucket)?;
        validate_key(src_key)?;
        validate_bucket(dst_bucket)?;
        validate_key(dst_key)?;
        let src = self.data_dir.join(src_bucket).join(src_key);
        let dst = self.data_dir.join(dst_bucket).join(dst_key);
        // TOCTOU: exists-check then rename has a narrow race with concurrent deletes.
        // Accepted for Phase 1 (single-node); a future path is renameat2(RENAME_NOREPLACE).
        if !src.try_exists()? {
            return Err(StorageError::NotFound {
                bucket: src_bucket.into(),
                key: src_key.into(),
            });
        }
        if let Some(p) = dst.parent() {
            fs::create_dir_all(p).await?;
        }
        fs::rename(&src, &dst).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;
    use crate::ObjectMeta;
    use crate::types::AccessLevel;

    async fn make_storage() -> (Storage, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let s = Storage::new(dir.path());
        fs::create_dir_all(s.data_dir.join("bucket")).await.unwrap();
        (s, dir)
    }

    #[tokio::test]
    async fn head_not_found() {
        let (s, _dir) = make_storage().await;
        let err = s.head_object("bucket", "missing.txt").await.unwrap_err();
        assert!(matches!(err, StorageError::NotFound { .. }));
    }

    #[tokio::test]
    async fn copy_preserves_xattrs() {
        let (s, _dir) = make_storage().await;
        let mut meta = ObjectMeta::default();
        meta.content_type = Some("text/plain".into());
        meta.access = Some(AccessLevel::Public);

        let etag = s
            .write_object("bucket", "src.txt", Cursor::new(b"data"), meta, None)
            .await
            .unwrap();

        let copied_etag = s
            .copy_object("bucket", "src.txt", "bucket", "dst.txt")
            .await
            .unwrap();
        assert_eq!(etag, copied_etag);

        let info = s.head_object("bucket", "dst.txt").await.unwrap();
        assert_eq!(info.etag, etag);
        assert_eq!(info.content_type.as_deref(), Some("text/plain"));
        assert_eq!(info.access, AccessLevel::Public);
    }

    #[tokio::test]
    async fn move_removes_source() {
        let (s, _dir) = make_storage().await;
        s.write_object(
            "bucket",
            "orig.txt",
            Cursor::new(b"hi"),
            ObjectMeta::default(),
            None,
        )
        .await
        .unwrap();
        s.move_object("bucket", "orig.txt", "bucket", "moved.txt")
            .await
            .unwrap();

        assert!(matches!(
            s.head_object("bucket", "orig.txt").await.unwrap_err(),
            StorageError::NotFound { .. }
        ));
        assert!(s.head_object("bucket", "moved.txt").await.is_ok());
    }

    #[tokio::test]
    async fn delete_is_idempotent_not() {
        let (s, _dir) = make_storage().await;
        let err = s.delete_object("bucket", "ghost.txt").await.unwrap_err();
        assert!(matches!(err, StorageError::NotFound { .. }));
    }

    #[tokio::test]
    async fn traversal_keys_rejected_in_read_ops() {
        let (s, _dir) = make_storage().await;
        let bad_key = "../../etc/passwd";
        assert!(matches!(
            s.head_object("bucket", bad_key).await.unwrap_err(),
            StorageError::InvalidKey(_)
        ));
        assert!(matches!(
            s.open_object("bucket", bad_key).await.unwrap_err(),
            StorageError::InvalidKey(_)
        ));
        assert!(matches!(
            s.delete_object("bucket", bad_key).await.unwrap_err(),
            StorageError::InvalidKey(_)
        ));
        assert!(matches!(
            s.copy_object("bucket", bad_key, "bucket", "dst.txt")
                .await
                .unwrap_err(),
            StorageError::InvalidKey(_)
        ));
        assert!(matches!(
            s.move_object("bucket", bad_key, "bucket", "dst.txt")
                .await
                .unwrap_err(),
            StorageError::InvalidKey(_)
        ));
    }
}
