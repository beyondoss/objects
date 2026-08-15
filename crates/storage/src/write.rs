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
    #[tracing::instrument(skip_all, fields(bucket, key))]
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
    #[tracing::instrument(skip_all, fields(bucket, key))]
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

        let (etag, size, file) = match stream_to_tmp(&tmp_path, &mut reader).await {
            Ok(v) => v,
            Err(e) => {
                Storage::cleanup_tmp(&tmp_path).await;
                return Err(e);
            }
        };

        if let Err(e) = self.sync.sync_file(file).await.map_err(StorageError::Io) {
            Storage::cleanup_tmp(&tmp_path).await;
            return Err(e);
        }

        // Only needed for keys with path separators; bucket dir already exists for flat keys.
        // Must happen before any rename so the destination directory exists.
        if key.contains('/')
            && let Some(parent) = final_path.parent()
        {
            fs::create_dir_all(parent).await?;
        }

        // Resolve the IfMatch condition (reads the existing etag xattr) before
        // we commit, while we can still clean up the tmp file on failure.
        if let Some(WriteCondition::IfMatch(expected)) = &condition {
            let dest = final_path.clone();
            let expected = expected.clone();
            let any = expected == "*" || expected == "\"*\"";
            let check = tokio::task::spawn_blocking(move || xattr::get(&dest, xattr::ETAG))
                .await
                .map_err(|e| StorageError::Io(std::io::Error::other(e)))?;
            match check? {
                None => {
                    Storage::cleanup_tmp(&tmp_path).await;
                    return Err(StorageError::NotFound {
                        bucket: bucket.into(),
                        key: key.into(),
                    });
                }
                Some(_) if any => {}
                Some(actual) if actual.as_slice() != expected.as_bytes() => {
                    Storage::cleanup_tmp(&tmp_path).await;
                    return Err(StorageError::EtagMismatch);
                }
                _ => {}
            }
        }

        // For the IfNoneMatch + Linux path, renameat2(RENAME_NOREPLACE) makes the
        // check-and-rename atomic at the syscall level. xattr is written inside the
        // same spawn_blocking so the whole commit (xattr + rename) is off the async thread.
        #[cfg(target_os = "linux")]
        if matches!(condition, Some(WriteCondition::IfNoneMatch)) {
            let tmp = tmp_path.clone();
            let dest = final_path.clone();
            let etag_c = etag.clone();
            let content_type = meta.content_type.clone();
            let user_metadata = meta.user_metadata.clone();
            return match tokio::task::spawn_blocking(move || {
                xattr::set_object(
                    &tmp,
                    &etag_c,
                    content_type.as_deref(),
                    meta.access,
                    &user_metadata,
                )
                .map_err(std::io::Error::other)?;
                rename_noreplace_sync(&tmp, &dest)
            })
            .await
            .map_err(|e| StorageError::Io(std::io::Error::other(e)))?
            {
                Ok(()) => {
                    tracing::debug!(bucket, key, etag, size, "object written");
                    Ok((etag, size))
                }
                Err(e) if e.raw_os_error() == Some(libc::EEXIST) => {
                    Storage::cleanup_tmp(&tmp_path).await;
                    Err(StorageError::ObjectExists {
                        bucket: bucket.into(),
                        key: key.into(),
                    })
                }
                Err(e) => {
                    Storage::cleanup_tmp(&tmp_path).await;
                    Err(StorageError::Io(e))
                }
            };
        }

        #[cfg(not(target_os = "linux"))]
        if matches!(condition, Some(WriteCondition::IfNoneMatch)) && final_path.try_exists()? {
            Storage::cleanup_tmp(&tmp_path).await;
            return Err(StorageError::ObjectExists {
                bucket: bucket.into(),
                key: key.into(),
            });
        }

        // Common commit path: xattr write + rename in a single spawn_blocking.
        // Holds an exclusive flock on `{dest}.lock` so If-Match / If-None-Match
        // re-check the dest under the lock (check-then-rename was racy).
        let tmp = tmp_path.clone();
        let dest = final_path.clone();
        let etag_c = etag.clone();
        let content_type = meta.content_type.clone();
        let user_metadata = meta.user_metadata.clone();
        let cond = condition;
        let bucket_owned = bucket.to_string();
        let key_owned = key.to_string();
        tokio::task::spawn_blocking(move || {
            xattr::set_object(
                &tmp,
                &etag_c,
                content_type.as_deref(),
                meta.access,
                &user_metadata,
            )?;
            if cond.is_some() {
                commit_locked(&tmp, &dest, cond, bucket_owned, key_owned)
            } else {
                std::fs::rename(&tmp, &dest).map_err(StorageError::Io)
            }
        })
        .await
        .map_err(|e| StorageError::Io(std::io::Error::other(e)))??;

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
#[tracing::instrument(skip_all, fields(size_bytes = tracing::field::Empty))]
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
    tracing::Span::current().record("size_bytes", total);
    Ok((etag, total, file))
}

fn commit_locked(
    tmp: &std::path::Path,
    dest: &std::path::Path,
    cond: Option<WriteCondition>,
    bucket: String,
    key: String,
) -> Result<()> {
    use std::fs::OpenOptions;
    use std::os::unix::io::AsRawFd;

    // Append ".lock" — do not use with_extension, which replaces ".json" etc.
    let mut lock_path = dest.as_os_str().to_os_string();
    lock_path.push(".lock");
    let lock_path = std::path::PathBuf::from(lock_path);
    let lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(&lock_path)
        .map_err(StorageError::Io)?;
    let rc = unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX) };
    if rc != 0 {
        return Err(StorageError::Io(std::io::Error::last_os_error()));
    }
    let _hold = lock;

    match cond {
        Some(WriteCondition::IfNoneMatch) if dest.exists() => {
            return Err(StorageError::ObjectExists { bucket, key });
        }
        Some(WriteCondition::IfMatch(expected)) => {
            let any = expected == "*" || expected == "\"*\"";
            match xattr::get(dest, xattr::ETAG)? {
                None => {
                    return Err(StorageError::NotFound { bucket, key });
                }
                Some(_) if any => {}
                Some(actual) if actual.as_slice() != expected.as_bytes() => {
                    return Err(StorageError::EtagMismatch);
                }
                _ => {}
            }
        }
        _ => {}
    }

    std::fs::rename(tmp, dest).map_err(StorageError::Io)?;
    let _ = std::fs::remove_file(&lock_path);
    Ok(())
}

/// Atomic create-or-fail rename using `renameat2(RENAME_NOREPLACE)`.
/// Returns `Err` with `EEXIST` if the destination already exists.
/// Only available on Linux; callers guard with `#[cfg(target_os = "linux")]`.
#[cfg(target_os = "linux")]
fn rename_noreplace_sync(from: &std::path::Path, to: &std::path::Path) -> std::io::Result<()> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;
    const RENAME_NOREPLACE: libc::c_uint = 1;
    let from_c = CString::new(from.as_os_str().as_bytes())
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidInput, "non-UTF-8 path"))?;
    let to_c = CString::new(to.as_os_str().as_bytes())
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidInput, "non-UTF-8 path"))?;
    let ret = unsafe {
        libc::syscall(
            libc::SYS_renameat2,
            libc::AT_FDCWD as libc::c_long,
            from_c.as_ptr(),
            libc::AT_FDCWD as libc::c_long,
            to_c.as_ptr(),
            RENAME_NOREPLACE as libc::c_ulong,
        )
    };
    if ret == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
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
    async fn write_object_fsyncs_the_parent_directory() {
        crate::sync::DIR_FSYNCS.store(0, std::sync::atomic::Ordering::SeqCst);
        let (s, _dir) = make_storage().await;
        s.write_object(
            "bucket",
            "hello.txt",
            Cursor::new(b"hello"),
            ObjectMeta::default(),
            None,
        )
        .await
        .unwrap();
        assert!(
            crate::sync::DIR_FSYNCS.load(std::sync::atomic::Ordering::SeqCst) > 0,
            "PUT rename must fsync the parent directory"
        );
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
