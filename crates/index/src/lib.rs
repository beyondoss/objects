use std::{
    collections::HashSet,
    path::{Path, PathBuf},
};

#[derive(Debug, thiserror::Error)]
#[must_use = "errors must be handled or explicitly ignored with `let _ =`"]
pub enum IndexError {
    #[error(transparent)]
    Fjall(#[from] fjall::Error),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, IndexError>;

pub struct ReconcileStats {
    pub inserted: usize,
    pub removed: usize,
}

/// Listing index backed by fjall. Keys are stored as `"{bucket}\x00{key}"` — the null
/// byte prevents ambiguity between bucket names and key paths that contain slashes.
///
/// fjall is synchronous; callers in async context should use `tokio::task::spawn_blocking`.
pub struct Index {
    keyspace: fjall::Keyspace,
    partition: fjall::PartitionHandle,
}

// Compile-time proof that Index is safe to pass to spawn_blocking.
const _: fn() = || {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<Index>();
};

impl Index {
    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        std::fs::create_dir_all(&path)?;
        let keyspace = fjall::Config::new(&path).open()?;
        let partition =
            keyspace.open_partition("objects", fjall::PartitionCreateOptions::default())?;
        Ok(Self {
            keyspace,
            partition,
        })
    }

    /// Synchronously flush the keyspace's journal to disk. fjall's default
    /// config persists per-write, so this is defensive — load-bearing only if
    /// a future config opts into lazy flushes.
    pub fn persist(&self) -> Result<()> {
        self.keyspace.persist(fjall::PersistMode::SyncAll)?;
        Ok(())
    }

    fn encode(bucket: &str, key: &str) -> Vec<u8> {
        let mut v = Vec::with_capacity(bucket.len() + 1 + key.len());
        v.extend_from_slice(bucket.as_bytes());
        v.push(b'\x00');
        v.extend_from_slice(key.as_bytes());
        v
    }

    pub fn insert(&self, bucket: &str, key: &str) -> Result<()> {
        Ok(self.partition.insert(Self::encode(bucket, key), b"")?)
    }

    pub fn delete(&self, bucket: &str, key: &str) -> Result<()> {
        Ok(self.partition.remove(Self::encode(bucket, key))?)
    }

    /// Prefix-scan within `bucket`. Returns plain keys (no bucket prefix), up to `limit`.
    ///
    /// `cursor` is the last key from the previous page (exclusive — the cursor key itself
    /// is not included in results). Pass `None` on the first page.
    ///
    /// **Precondition**: when `prefix` is non-empty, `cursor` must be a key returned by a
    /// prior `scan` call with the same `bucket` and `prefix`. Passing a cursor from a
    /// different prefix produces undefined results.
    #[tracing::instrument(skip(self), fields(results = tracing::field::Empty))]
    pub fn scan(
        &self,
        bucket: &str,
        prefix: &str,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<Vec<String>> {
        let lo = match cursor {
            // Start just after cursor by appending a null byte — exclusive bound.
            Some(c) => {
                debug_assert!(
                    prefix.is_empty() || c.starts_with(prefix),
                    "cursor `{c}` does not start with prefix `{prefix}`"
                );
                let mut v = Self::encode(bucket, c);
                v.push(b'\x00');
                v
            }
            None => Self::encode(bucket, prefix),
        };
        let hi = hi_bound(bucket, prefix);

        let iter = self.partition.range(lo..hi);

        let mut results = Vec::with_capacity(limit.min(256));
        for item in iter {
            if results.len() >= limit {
                break;
            }
            let (k, _) = item?;
            let raw: &[u8] = &k;
            let prefix_len = bucket.len() + 1;
            if raw.len() <= prefix_len {
                continue;
            }
            match std::str::from_utf8(&raw[prefix_len..]) {
                Ok(key) => results.push(key.to_owned()),
                Err(_) => tracing::warn!("skipping index entry with non-UTF-8 key"),
            }
        }
        tracing::Span::current().record("results", results.len());
        Ok(results)
    }

    /// Walk `data_dir`, insert keys missing from the index, and remove index entries
    /// whose backing file no longer exists. Intended to run once at startup.
    pub fn reconcile(&self, data_dir: &Path) -> Result<ReconcileStats> {
        let mut stats = ReconcileStats {
            inserted: 0,
            removed: 0,
        };

        // Pass 1: filesystem → index (insert missing keys).
        for entry in std::fs::read_dir(data_dir)? {
            let entry = entry?;
            let raw_name = entry.file_name();
            let Some(bucket_name) = raw_name.to_str() else {
                tracing::warn!(path = ?entry.path(), "skipping non-UTF-8 bucket name");
                continue;
            };
            if bucket_name.starts_with('.') || !entry.file_type()?.is_dir() {
                continue;
            }

            // Build a HashSet of currently indexed keys for this bucket so the
            // filesystem walk can check membership in O(1) instead of a point
            // lookup per file.
            let indexed: HashSet<Vec<u8>> = self
                .partition
                .range(Self::encode(bucket_name, "")..hi_bound(bucket_name, ""))
                .filter_map(|r| r.ok().map(|(k, _)| k.to_vec()))
                .collect();

            self.walk_bucket(data_dir, bucket_name, "", &mut stats, &indexed)?;
        }

        // Pass 2: index → filesystem (remove stale entries).
        // Collect first: can't call delete() while the partition iterator is live.
        let mut to_remove: Vec<(String, String)> = Vec::new();
        for item in self.partition.range::<Vec<u8>, _>(..) {
            let (k, _) = item?;
            let raw: &[u8] = &k;
            if let Some(sep) = raw.iter().position(|&b| b == b'\x00') {
                let bucket = match std::str::from_utf8(&k[..sep]) {
                    Ok(s) => s,
                    Err(_) => {
                        tracing::warn!("skipping index entry with non-UTF-8 bucket");
                        continue;
                    }
                };
                let key = match std::str::from_utf8(&k[sep + 1..]) {
                    Ok(s) => s,
                    Err(_) => {
                        tracing::warn!("skipping index entry with non-UTF-8 key");
                        continue;
                    }
                };
                if !data_dir.join(bucket).join(key).exists() {
                    to_remove.push((bucket.to_owned(), key.to_owned()));
                }
            }
        }
        for (bucket, key) in to_remove {
            self.delete(&bucket, &key)?;
            stats.removed += 1;
        }

        tracing::info!(
            inserted = stats.inserted,
            removed = stats.removed,
            "index reconcile complete"
        );
        Ok(stats)
    }

    fn walk_bucket(
        &self,
        data_dir: &Path,
        bucket: &str,
        prefix: &str,
        stats: &mut ReconcileStats,
        indexed: &HashSet<Vec<u8>>,
    ) -> Result<()> {
        let dir = if prefix.is_empty() {
            data_dir.join(bucket)
        } else {
            data_dir.join(bucket).join(prefix)
        };
        for entry in std::fs::read_dir(&dir)? {
            let entry = entry?;
            let raw_name = entry.file_name();
            let Some(name) = raw_name.to_str() else {
                tracing::warn!(path = ?entry.path(), "skipping non-UTF-8 filename");
                continue;
            };
            let rel_key = if prefix.is_empty() {
                name.to_owned()
            } else {
                format!("{prefix}/{name}")
            };
            if entry.file_type()?.is_dir() {
                self.walk_bucket(data_dir, bucket, &rel_key, stats, indexed)?;
            } else {
                let k = Self::encode(bucket, &rel_key);
                if !indexed.contains(&k) {
                    self.insert(bucket, &rel_key)?;
                    stats.inserted += 1;
                }
            }
        }
        Ok(())
    }
}

/// Upper bound for a prefix scan: `"{bucket}\x00{prefix_incremented}"`.
///
/// When `prefix` is empty, we use `"{bucket}\x01"` to cover all keys in the bucket
/// (since `\x01 > \x00`).
fn hi_bound(bucket: &str, prefix: &str) -> Vec<u8> {
    let mut v = bucket.as_bytes().to_vec();
    if prefix.is_empty() {
        v.push(b'\x01');
    } else {
        v.push(b'\x00');
        v.extend_from_slice(prefix.as_bytes());
        // Increment the last byte; carry if it overflows.
        loop {
            match v.last_mut() {
                Some(b) if *b < u8::MAX => {
                    *b += 1;
                    break;
                }
                Some(_) => {
                    v.pop();
                }
                None => break,
            }
        }
    }
    v
}

#[cfg(test)]
mod tests {
    use super::*;

    fn open_index() -> (Index, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let idx = Index::open(dir.path()).unwrap();
        (idx, dir)
    }

    #[test]
    fn insert_scan_delete() {
        let (idx, _dir) = open_index();
        idx.insert("images", "a.png").unwrap();
        idx.insert("images", "b.png").unwrap();
        idx.insert("images", "c.png").unwrap();

        let keys = idx.scan("images", "", None, 10).unwrap();
        assert_eq!(keys, vec!["a.png", "b.png", "c.png"]);

        idx.delete("images", "b.png").unwrap();
        let keys = idx.scan("images", "", None, 10).unwrap();
        assert_eq!(keys, vec!["a.png", "c.png"]);
    }

    #[test]
    fn prefix_filter() {
        let (idx, _dir) = open_index();
        idx.insert("docs", "avatars/a.png").unwrap();
        idx.insert("docs", "avatars/b.png").unwrap();
        idx.insert("docs", "other/c.txt").unwrap();

        let keys = idx.scan("docs", "avatars/", None, 10).unwrap();
        assert_eq!(keys, vec!["avatars/a.png", "avatars/b.png"]);
    }

    #[test]
    fn cursor_pagination() {
        let (idx, _dir) = open_index();
        for i in 0..5u8 {
            idx.insert("b", &format!("file{i}.txt")).unwrap();
        }

        // First page: limit 2
        let page1 = idx.scan("b", "", None, 2).unwrap();
        assert_eq!(page1.len(), 2);

        // Second page using last key of page1 as cursor
        let cursor = page1.last().unwrap().as_str();
        let page2 = idx.scan("b", "", Some(cursor), 2).unwrap();
        assert_eq!(page2.len(), 2);
        assert!(!page2.contains(&cursor.to_string()));

        // Third page
        let cursor2 = page2.last().unwrap().as_str();
        let page3 = idx.scan("b", "", Some(cursor2), 2).unwrap();
        assert_eq!(page3.len(), 1);
    }

    #[test]
    fn bucket_isolation() {
        let (idx, _dir) = open_index();
        idx.insert("a", "file.txt").unwrap();
        idx.insert("b", "file.txt").unwrap();

        let a = idx.scan("a", "", None, 10).unwrap();
        assert_eq!(a, vec!["file.txt"]);
        let b = idx.scan("b", "", None, 10).unwrap();
        assert_eq!(b, vec!["file.txt"]);
    }

    #[test]
    fn reconcile_inserts_missing_removes_stale() {
        let dir = tempfile::tempdir().unwrap();
        let data_dir = dir.path().join("data");
        std::fs::create_dir_all(data_dir.join("images")).unwrap();
        std::fs::write(data_dir.join("images").join("a.png"), b"").unwrap();
        std::fs::write(data_dir.join("images").join("b.png"), b"").unwrap();

        let idx_dir = dir.path().join("index");
        let idx = Index::open(&idx_dir).unwrap();

        // Pre-seed one stale entry (file doesn't exist)
        idx.insert("images", "stale.png").unwrap();

        let stats = idx.reconcile(&data_dir).unwrap();
        assert_eq!(stats.inserted, 2); // a.png + b.png
        assert_eq!(stats.removed, 1); // stale.png

        let keys = idx.scan("images", "", None, 10).unwrap();
        assert!(keys.contains(&"a.png".to_string()));
        assert!(keys.contains(&"b.png".to_string()));
        assert!(!keys.contains(&"stale.png".to_string()));
    }

    #[test]
    fn scan_limit_zero() {
        let (idx, _dir) = open_index();
        idx.insert("a", "file.txt").unwrap();
        let keys = idx.scan("a", "", None, 0).unwrap();
        assert!(keys.is_empty());
    }

    #[test]
    fn hi_bound_empty_prefix() {
        let b = hi_bound("images", "");
        assert_eq!(&b, b"images\x01");
    }

    #[test]
    fn hi_bound_prefix() {
        let b = hi_bound("images", "av");
        // "images\x00av" with last byte incremented: 'v'(0x76) → 'w'(0x77)
        assert_eq!(&b, b"images\x00aw");
    }
}
