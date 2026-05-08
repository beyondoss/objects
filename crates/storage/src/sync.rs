use std::io;
use std::time::Duration;

use futures::future::join_all;
use tokio::fs;
use tokio::sync::{mpsc, oneshot};
use tokio::time::timeout_at;

pub(crate) struct SyncRequest {
    file: fs::File,
    tx: oneshot::Sender<io::Result<()>>,
}

/// Batches `fdatasync` calls across concurrent writers within a linger window.
///
/// On Linux ext4/xfs, concurrent `fdatasync` calls on different files in the
/// same filesystem share a journal commit and block device flush — the linger
/// window ensures concurrent uploads hit the sync together rather than serially.
///
/// `Inline` mode (the default) syncs each file immediately in the caller, matching
/// the original behavior. `Batched` mode starts a background task.
#[derive(Clone)]
pub(crate) enum SyncGroup {
    Inline,
    Batched(mpsc::Sender<SyncRequest>),
}

impl std::fmt::Debug for SyncGroup {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Inline => write!(f, "SyncGroup::Inline"),
            Self::Batched(_) => write!(f, "SyncGroup::Batched(..)"),
        }
    }
}

impl SyncGroup {
    pub fn inline() -> Self {
        Self::Inline
    }

    /// Start a background sync task with the given linger window.
    ///
    /// The returned `SyncGroup` is a cloneable handle. The background task exits
    /// when all handles are dropped (channel closes).
    pub fn start(linger: Duration) -> Self {
        let (tx, rx) = mpsc::channel(16_384);
        tokio::spawn(run(rx, linger));
        Self::Batched(tx)
    }

    /// Sync `file` — either inline or by enrolling in the current batch.
    pub async fn sync_file(&self, file: fs::File) -> io::Result<()> {
        match self {
            Self::Inline => file.sync_data().await,
            Self::Batched(tx) => {
                let (resp_tx, resp_rx) = oneshot::channel();
                tx.send(SyncRequest { file, tx: resp_tx })
                    .await
                    .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "sync group closed"))?;
                resp_rx
                    .await
                    .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "sync group dropped"))?
            }
        }
    }
}

async fn run(mut rx: mpsc::Receiver<SyncRequest>, linger: Duration) {
    loop {
        let first = match rx.recv().await {
            Some(r) => r,
            None => return,
        };

        let deadline = tokio::time::Instant::now() + linger;
        let mut batch = vec![first];
        while let Ok(Some(req)) = timeout_at(deadline, rx.recv()).await {
            batch.push(req);
        }

        flush(batch).await;
    }
}

async fn flush(batch: Vec<SyncRequest>) {
    let (files, txs): (Vec<_>, Vec<_>) = batch.into_iter().map(|r| (r.file, r.tx)).unzip();
    let results = join_all(
        files
            .into_iter()
            .map(|f| async move { f.sync_data().await }),
    )
    .await;
    for (tx, result) in txs.into_iter().zip(results) {
        let _ = tx.send(result);
    }
}
