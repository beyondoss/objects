pub mod error;
pub mod types;

mod bucket;
mod gc;
mod multipart;
mod read;
pub(crate) mod sync;
pub(crate) mod write;
mod xattr;

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::time::Duration;

pub use error::StorageError;
pub use types::{
    AccessLevel, BucketMeta, CompletedPart, MultipartInfo, ObjectInfo, ObjectMeta, PartInfo,
    WriteCondition,
};

use sync::SyncGroup;

pub type Result<T> = std::result::Result<T, StorageError>;

#[derive(Clone, Debug)]
pub struct Storage {
    pub(crate) data_dir: PathBuf,
    pub(crate) tmp_dir: PathBuf,
    pub(crate) sync: SyncGroup,
    /// In-memory cache of bucket access levels. Eliminates a `getxattr` on the
    /// bucket directory for every object GET whose access inherits from the bucket.
    pub(crate) bucket_access: Arc<RwLock<HashMap<String, AccessLevel>>>,
}

impl Storage {
    pub fn new(data_dir: impl Into<PathBuf>) -> Self {
        let data_dir = data_dir.into();
        let tmp_dir = data_dir.join(".tmp");
        Self {
            data_dir,
            tmp_dir,
            sync: SyncGroup::inline(),
            bucket_access: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Create a `Storage` that batches `fdatasync` calls within a linger window.
    ///
    /// Must be called from within a tokio runtime. The background sync task lives
    /// until all `Storage` clones are dropped.
    pub fn with_linger(data_dir: impl Into<PathBuf>, linger: Duration) -> Self {
        let data_dir = data_dir.into();
        let tmp_dir = data_dir.join(".tmp");
        Self {
            data_dir,
            tmp_dir,
            sync: SyncGroup::start(linger),
            bucket_access: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Remove `path` and log a warning if it fails. Used for deterministic temp-file
    /// cleanup on error paths — avoids fire-and-forget spawns that can silently
    /// vanish under runtime shutdown.
    pub(crate) async fn cleanup_tmp(path: &std::path::Path) {
        if let Err(e) = tokio::fs::remove_file(path).await {
            tracing::warn!(path = %path.display(), err = %e, "temp file cleanup failed");
        }
    }
}

impl Storage {
    /// Resolve bucket access level: cache-first, falling back to a filesystem
    /// `getxattr` on a miss. Populates the cache on the slow path.
    pub(crate) fn cached_bucket_access(
        &self,
        bucket: &str,
        bucket_path: &Path,
    ) -> Result<AccessLevel> {
        if let Ok(cache) = self.bucket_access.read()
            && let Some(&access) = cache.get(bucket)
        {
            return Ok(access);
        }
        let access = xattr::read_access(bucket_path)?.unwrap_or_default();
        if let Ok(mut cache) = self.bucket_access.write() {
            cache.insert(bucket.to_owned(), access);
        }
        Ok(access)
    }
}
