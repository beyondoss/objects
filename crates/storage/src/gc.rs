use std::time::{Duration, SystemTime};

use crate::{Result, Storage};
use tokio::fs;

impl Storage {
    /// Delete temp files in `.tmp/` older than `max_age`. Returns the count deleted.
    pub async fn gc_temp_files(&self, max_age: Duration) -> Result<usize> {
        let tmp_dir = self.data_dir.join(".tmp");
        if !tmp_dir.try_exists()? {
            return Ok(0);
        }
        let cutoff = SystemTime::now()
            .checked_sub(max_age)
            .unwrap_or(SystemTime::UNIX_EPOCH);
        let mut entries = fs::read_dir(&tmp_dir).await?;
        let mut removed = 0usize;
        while let Some(entry) = entries.next_entry().await? {
            let meta = match entry.metadata().await {
                Ok(m) => m,
                Err(e) => {
                    tracing::warn!(path = ?entry.path(), error = %e, "gc: failed to read temp file metadata");
                    continue;
                }
            };
            if meta.modified().map(|t| t < cutoff).unwrap_or(false) {
                match fs::remove_file(entry.path()).await {
                    Ok(()) => removed += 1,
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                    Err(e) => {
                        tracing::warn!(path = ?entry.path(), error = %e, "gc: failed to remove temp file");
                    }
                }
            }
        }
        if removed > 0 {
            tracing::info!(removed, "gc: removed orphaned temp files");
        }
        Ok(removed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn no_tmp_dir_returns_zero() {
        let dir = tempfile::tempdir().unwrap();
        let s = Storage::new(dir.path());
        assert_eq!(s.gc_temp_files(Duration::from_secs(0)).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn large_max_age_keeps_all() {
        let dir = tempfile::tempdir().unwrap();
        let s = Storage::new(dir.path());
        let tmp = s.data_dir.join(".tmp");
        fs::create_dir_all(&tmp).await.unwrap();
        std::fs::write(tmp.join("a"), b"").unwrap();
        std::fs::write(tmp.join("b"), b"").unwrap();
        let removed = s.gc_temp_files(Duration::from_secs(86_400)).await.unwrap();
        assert_eq!(removed, 0);
    }

    #[tokio::test]
    async fn zero_max_age_removes_stale() {
        let dir = tempfile::tempdir().unwrap();
        let s = Storage::new(dir.path());
        let tmp = s.data_dir.join(".tmp");
        fs::create_dir_all(&tmp).await.unwrap();
        let p = tmp.join("stale");
        std::fs::write(&p, b"").unwrap();
        // Sleep so mtime is strictly before the cutoff computed inside gc_temp_files.
        tokio::time::sleep(Duration::from_millis(50)).await;
        let removed = s.gc_temp_files(Duration::ZERO).await.unwrap();
        assert_eq!(removed, 1);
        assert!(!p.exists());
    }
}
