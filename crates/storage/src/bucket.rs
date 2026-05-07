use tokio::fs;

use crate::types::{AccessLevel, BucketMeta};
use crate::write::validate_bucket;
use crate::{Result, Storage, StorageError, xattr};

impl Storage {
    /// Create a bucket. Idempotent: succeeds silently if the bucket already exists.
    pub async fn create_bucket(&self, name: &str, access: AccessLevel) -> Result<()> {
        validate_bucket(name)?;
        let path = self.data_dir.join(name);
        match fs::create_dir(&path).await {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(e) => return Err(e.into()),
        }
        xattr::set(&path, xattr::ACCESS, access.as_str().as_bytes())
    }

    /// Delete a bucket. Idempotent: succeeds silently if the bucket is already gone.
    /// Returns `BucketNotEmpty` if it still contains objects.
    pub async fn delete_bucket(&self, name: &str) -> Result<()> {
        validate_bucket(name)?;
        let path = self.data_dir.join(name);
        match fs::remove_dir(&path).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            // POSIX: ENOTEMPTY maps to ErrorKind::DirectoryNotEmpty (stable since 1.82)
            Err(e) if e.kind() == std::io::ErrorKind::DirectoryNotEmpty => {
                Err(StorageError::BucketNotEmpty)
            }
            Err(e) => Err(e.into()),
        }
    }

    /// List all buckets, sorted by name. Skips dot-prefixed directories (`.tmp`, `.multipart`).
    pub async fn list_buckets(&self) -> Result<Vec<BucketMeta>> {
        let mut entries = fs::read_dir(&self.data_dir).await?;
        let mut buckets = Vec::new();
        while let Some(entry) = entries.next_entry().await? {
            let name = entry.file_name().to_string_lossy().into_owned();
            if name.starts_with('.') {
                continue;
            }
            if !entry.file_type().await?.is_dir() {
                continue;
            }
            let path = entry.path();
            let access = xattr::get(&path, xattr::ACCESS)?
                .map(|b| {
                    String::from_utf8(b)
                        .map_err(|e| crate::StorageError::Xattr(format!("access: {e}")))?
                        .parse::<AccessLevel>()
                })
                .transpose()?
                .unwrap_or_default();
            buckets.push(BucketMeta { name, access });
        }
        buckets.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(buckets)
    }

    pub async fn get_bucket(&self, name: &str) -> Result<BucketMeta> {
        validate_bucket(name)?;
        let path = self.data_dir.join(name);
        let meta = fs::metadata(&path).await.map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                StorageError::BucketNotFound(name.into())
            } else {
                e.into()
            }
        })?;
        if !meta.is_dir() {
            return Err(StorageError::BucketNotFound(name.into()));
        }
        let access = xattr::get(&path, xattr::ACCESS)?
            .map(|b| {
                String::from_utf8(b)
                    .map_err(|e| crate::StorageError::Xattr(format!("access: {e}")))?
                    .parse::<AccessLevel>()
            })
            .transpose()?
            .unwrap_or_default();
        Ok(BucketMeta {
            name: name.into(),
            access,
        })
    }

    pub async fn update_bucket(&self, name: &str, access: AccessLevel) -> Result<()> {
        validate_bucket(name)?;
        let path = self.data_dir.join(name);
        if !path.try_exists()? {
            return Err(StorageError::BucketNotFound(name.into()));
        }
        xattr::set(&path, xattr::ACCESS, access.as_str().as_bytes())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn make_storage() -> (Storage, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let s = Storage::new(dir.path());
        (s, dir)
    }

    #[tokio::test]
    async fn create_get_list_update_delete() {
        let (s, _dir) = make_storage().await;

        s.create_bucket("images", AccessLevel::Public)
            .await
            .unwrap();
        s.create_bucket("docs", AccessLevel::Private).await.unwrap();

        let b = s.get_bucket("images").await.unwrap();
        assert_eq!(b.name, "images");
        assert_eq!(b.access, AccessLevel::Public);

        let mut list = s.list_buckets().await.unwrap();
        list.sort_by(|a, b| a.name.cmp(&b.name));
        assert_eq!(list.len(), 2);
        assert_eq!(list[0].name, "docs");
        assert_eq!(list[1].name, "images");

        s.update_bucket("images", AccessLevel::Private)
            .await
            .unwrap();
        assert_eq!(
            s.get_bucket("images").await.unwrap().access,
            AccessLevel::Private
        );

        s.delete_bucket("images").await.unwrap();
        assert!(matches!(
            s.get_bucket("images").await.unwrap_err(),
            StorageError::BucketNotFound(_)
        ));
    }

    #[tokio::test]
    async fn create_is_idempotent() {
        let (s, _dir) = make_storage().await;
        s.create_bucket("b", AccessLevel::Private).await.unwrap();
        s.create_bucket("b", AccessLevel::Private).await.unwrap();
    }

    #[tokio::test]
    async fn delete_is_idempotent() {
        let (s, _dir) = make_storage().await;
        s.delete_bucket("ghost").await.unwrap();
    }

    #[tokio::test]
    async fn list_skips_dot_dirs() {
        let (s, _dir) = make_storage().await;
        // .tmp is created by write_object; should never appear in bucket list
        fs::create_dir_all(s.data_dir.join(".tmp")).await.unwrap();
        s.create_bucket("real", AccessLevel::Private).await.unwrap();
        let list = s.list_buckets().await.unwrap();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].name, "real");
    }

    #[tokio::test]
    async fn traversal_bucket_names_rejected() {
        let (s, _dir) = make_storage().await;
        let cases = ["../escape", "..", ".hidden", "a/b", "a\0b", ""];
        for name in cases {
            assert!(
                matches!(
                    s.create_bucket(name, AccessLevel::Private)
                        .await
                        .unwrap_err(),
                    StorageError::InvalidKey(_)
                ),
                "expected InvalidKey for bucket name {name:?}"
            );
            assert!(
                matches!(
                    s.delete_bucket(name).await.unwrap_err(),
                    StorageError::InvalidKey(_)
                ),
                "expected InvalidKey for bucket name {name:?}"
            );
            assert!(
                matches!(
                    s.get_bucket(name).await.unwrap_err(),
                    StorageError::InvalidKey(_)
                ),
                "expected InvalidKey for bucket name {name:?}"
            );
            assert!(
                matches!(
                    s.update_bucket(name, AccessLevel::Public)
                        .await
                        .unwrap_err(),
                    StorageError::InvalidKey(_)
                ),
                "expected InvalidKey for bucket name {name:?}"
            );
        }
    }
}
