//! Glue between this service and the in-house [`handoff`] library.
//!
//! Two pieces live here:
//!
//! - [`ObjectsHandoff`] — the [`handoff::Drainable`] impl. The handoff control
//!   thread (spawned by `serve()`) calls into it on the supervisor's cue:
//!   drain → seal → commit (or resume_after_abort on failure). objects is a
//!   single-tokio-runtime service with no per-shard workers, so the bridge is
//!   much smaller than kv's equivalent — no channels, no per-worker fan-out.
//!
//! - [`PausableListener`] — wraps a `tokio::net::TcpListener` and implements
//!   [`axum::serve::Listener`]. While `accept_closed` is set (during drain),
//!   `accept().await` simply suspends instead of returning; the kernel SYN
//!   queue absorbs incoming connections. When the incumbent exits and the
//!   successor binds the inherited FD, those queued connections drain into
//!   the new process. Mirrors kv's sync `accept_closed` pause behavior.
//!
//! Test hooks (`OBJECTS_TEST_FAIL_ONCE_FILE`) are honored in [`ObjectsHandoff::seal`]
//! to let integration tests exercise the seal-failure → resume-after-abort path.
//! Production never sets these env vars.

use std::io;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use beyond_objects_index::Index;
use handoff::{DrainReport, Drainable, ReadinessSnapshot, SealReport, StateSnapshot};

use crate::metrics::Metrics;

/// Bridge between the sync [`Drainable`] API and the rest of objects.
///
/// `accept_closed` is shared with the [`PausableListener`] in the axum serve
/// loop (and with the TLS accept loop): when set, both stop dispatching new
/// connections to handlers. Cleared on `resume_after_abort`.
pub struct ObjectsHandoff {
    pub accept_closed: Arc<AtomicBool>,
    index: Arc<Index>,
    metrics: Arc<Metrics>,
}

impl ObjectsHandoff {
    pub fn new(accept_closed: Arc<AtomicBool>, index: Arc<Index>, metrics: Arc<Metrics>) -> Self {
        Self {
            accept_closed,
            index,
            metrics,
        }
    }
}

impl Drainable for ObjectsHandoff {
    fn drain(&self, deadline: Instant) -> handoff::Result<DrainReport> {
        let started = Instant::now();
        // Stop new dispatches immediately. The kernel listen backlog absorbs
        // SYNs that arrive in this window; the successor's first accept on
        // the inherited FD drains them.
        self.accept_closed.store(true, Ordering::SeqCst);

        // Per-request `SyncGroup::sync_file` already syncs each upload before
        // its handler returns — there is no global pending-write state to
        // flush here. We just wait for in-flight handlers to drain.
        while self.metrics.http_connections_active.get() > 0.0 && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(25));
        }

        let open = self.metrics.http_connections_active.get().max(0.0) as u32;
        self.metrics
            .handoff_drain_seconds
            .observe(started.elapsed().as_secs_f64());
        Ok(DrainReport {
            open_conns_remaining: open,
            accept_closed: true,
        })
    }

    fn seal(&self) -> handoff::Result<SealReport> {
        let started = Instant::now();

        // Test hook: consume a signal file and return an error so the
        // supervisor sees `SealFailed`. Production never sets this env var.
        if let Ok(path) = std::env::var("OBJECTS_TEST_FAIL_ONCE_FILE")
            && !path.is_empty()
        {
            let p = Path::new(&path);
            if p.exists() {
                let _ = std::fs::remove_file(p);
                self.metrics.handoff_seal_failures_total.inc();
                self.metrics
                    .handoff_handoffs_total
                    .with_label_values(&["seal_failed"])
                    .inc();
                return Err(handoff::Error::Protocol(
                    "seal failed: test hook".to_string(),
                ));
            }
        }

        // fjall's default config persists per-write, so this is defensive.
        // Wrap in spawn_blocking-free sync call — we're on the dedicated
        // handoff control thread already (outside any tokio runtime).
        if let Err(e) = self.index.persist() {
            self.metrics.handoff_seal_failures_total.inc();
            self.metrics
                .handoff_handoffs_total
                .with_label_values(&["seal_failed"])
                .inc();
            return Err(handoff::Error::Protocol(format!("seal failed: {e}")));
        }

        self.metrics
            .handoff_seal_seconds
            .observe(started.elapsed().as_secs_f64());
        Ok(SealReport {
            last_revision_per_shard: Vec::new(),
            data_dir_fingerprint: [0u8; 32],
        })
    }

    fn resume_after_abort(&self) -> handoff::Result<()> {
        // No filesystem state to reopen — seal didn't transform anything that
        // would need rolling back. Just re-arm the accept path.
        self.accept_closed.store(false, Ordering::SeqCst);
        self.metrics.handoff_rolled_back_total.inc();
        self.metrics
            .handoff_handoffs_total
            .with_label_values(&["resumed"])
            .inc();
        Ok(())
    }

    fn snapshot_state(&self) -> StateSnapshot {
        StateSnapshot {
            shard_count: 1,
            open_conns: self.metrics.http_connections_active.get().max(0.0) as u32,
            last_revision_per_shard: Vec::new(),
        }
    }
}

/// `tokio::net::TcpListener` wrapper that suspends `accept` while
/// `accept_closed` is set. Used as the `axum::serve` listener so we can pause
/// handing off new connections during a handoff drain without closing the
/// underlying FD (the supervisor's dup of which the successor will inherit).
pub struct PausableListener {
    pub inner: tokio::net::TcpListener,
    pub accept_closed: Arc<AtomicBool>,
}

impl axum::serve::Listener for PausableListener {
    type Io = tokio::net::TcpStream;
    type Addr = std::net::SocketAddr;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        loop {
            if self.accept_closed.load(Ordering::Relaxed) {
                tokio::time::sleep(Duration::from_millis(25)).await;
                continue;
            }
            match self.inner.accept().await {
                Ok(x) => return x,
                Err(e) => {
                    tracing::error!(error = %e, "accept error");
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            }
        }
    }

    fn local_addr(&self) -> io::Result<Self::Addr> {
        self.inner.local_addr()
    }
}

/// Pull the `"http"` listener out of a [`handoff::Role`], returning it as a
/// `tokio::net::TcpListener` set to non-blocking. Returns `None` on cold start
/// when the supervisor did not pre-bind a listener for us.
pub fn take_http_listener(
    role: &mut Option<handoff::role::BegunSuccessor>,
    inherited: &mut handoff::role::InheritedListeners,
) -> handoff::Result<Option<tokio::net::TcpListener>> {
    let std_listener = match role {
        Some(s) => s.take_listener("http"),
        None => inherited.take("http"),
    };
    match std_listener {
        Some(l) => {
            l.set_nonblocking(true)?;
            Ok(Some(tokio::net::TcpListener::from_std(l)?))
        }
        None => Ok(None),
    }
}

/// Build a `ReadinessSnapshot` from the bind address. Used by the successor
/// just before `announce_and_bind`.
pub fn readiness_snapshot(address: &str) -> ReadinessSnapshot {
    ReadinessSnapshot {
        listening_on: vec![address.to_string()],
        healthz_ok: true,
        advertised_revision_per_shard: Vec::new(),
    }
}
