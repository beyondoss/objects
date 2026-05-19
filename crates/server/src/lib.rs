pub mod cli;
pub mod config;
pub mod error;
pub mod handoff;
pub mod metrics;
pub mod middleware;
pub mod routes;
pub mod s3;
pub mod telemetry;
pub mod test_support;
pub mod upload_token;

use std::{sync::Arc, time::Instant};

use anyhow::Result;
use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, MatchedPath, Request, State},
    http::header,
    middleware::{Next, from_fn_with_state},
    response::{IntoResponse, Response},
    routing::get,
};
use tower::{ServiceBuilder, ServiceExt};
use tower_http::{
    catch_panic::CatchPanicLayer,
    cors::{Any, CorsLayer},
    request_id::{MakeRequestId, PropagateRequestIdLayer, RequestId, SetRequestIdLayer},
    trace::{MakeSpan, TraceLayer},
};
use utoipa::OpenApi;
use uuid::Uuid;

use beyond_objects_index::Index;
use beyond_objects_storage::Storage;

pub use config::Config;

/// Process-wide HTTP service state. All inner mutable/shared state is wrapped
/// in `Arc` so `AppState::clone()` is cheap.
#[derive(Clone)]
pub struct AppState {
    pub config: Arc<Config>,
    pub storage: Storage,
    pub index: Arc<Index>,
    pub metrics: Arc<metrics::Metrics>,
}

impl AppState {
    #[tracing::instrument(skip(self))]
    pub async fn index_insert(&self, bucket: &str, key: &str) -> Result<(), error::ApiError> {
        let idx = self.index.clone();
        let bucket = bucket.to_owned();
        let key = key.to_owned();
        tokio::task::spawn_blocking(move || idx.insert(&bucket, &key))
            .await
            .map_err(|e| error::ApiError::Internal(anyhow::anyhow!("index insert join: {e}")))??;
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    pub async fn index_delete(&self, bucket: &str, key: &str) -> Result<(), error::ApiError> {
        let idx = self.index.clone();
        let bucket = bucket.to_owned();
        let key = key.to_owned();
        tokio::task::spawn_blocking(move || idx.delete(&bucket, &key))
            .await
            .map_err(|e| error::ApiError::Internal(anyhow::anyhow!("index delete join: {e}")))??;
        Ok(())
    }

    /// Hook for future event publication to Beyond Queue (Phase 3+). No-op for
    /// now — kept on AppState so handlers don't need to know whether events are
    /// wired up.
    pub fn publish(&self, _base_url: &str, _bucket: &str, _key: &str) {}

    /// One page of a prefix-paginated listing. Used by both the native REST
    /// handler and the S3 `ListObjectsV2` handler so they share a single
    /// scan plus buffered-head implementation. Index entries whose backing
    /// file disappeared (typically after a crash) are skipped silently;
    /// startup reconcile drops them on the next boot.
    pub async fn list_page(
        &self,
        bucket: &str,
        prefix: &str,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<ListPage, error::ApiError> {
        use beyond_objects_storage::StorageError;
        use futures::TryStreamExt;
        use futures::stream::StreamExt;

        let index = self.index.clone();
        let bucket_owned = bucket.to_owned();
        let prefix_owned = prefix.to_owned();
        let cursor_owned = cursor.map(str::to_owned);
        let parent_span = tracing::Span::current();
        let keys = tokio::task::spawn_blocking(move || {
            let _enter = parent_span.enter();
            index.scan(&bucket_owned, &prefix_owned, cursor_owned.as_deref(), limit)
        })
        .await
        .map_err(|e| error::ApiError::Internal(anyhow::anyhow!("index scan join: {e}")))??;

        let next_cursor = if keys.len() == limit {
            keys.last().cloned()
        } else {
            None
        };

        let bucket_owned = bucket.to_owned();
        let items: Vec<ListItem> = futures::stream::iter(keys.into_iter().map(|k| {
            let storage = self.storage.clone();
            let b = bucket_owned.clone();
            async move {
                match storage.head_object(&b, &k).await {
                    Ok(info) => Ok(Some(ListItem { key: k, info })),
                    Err(StorageError::NotFound { .. }) => {
                        tracing::debug!(
                            bucket = %b,
                            key = %k,
                            "skipping index entry without backing file"
                        );
                        Ok::<Option<ListItem>, error::ApiError>(None)
                    }
                    Err(e) => Err(error::ApiError::from(e)),
                }
            }
        }))
        .buffered(64)
        .try_collect::<Vec<_>>()
        .await?
        .into_iter()
        .flatten()
        .collect();

        Ok(ListPage { items, next_cursor })
    }
}

/// One key+metadata pair returned by `AppState::list_page`. Surface layers
/// (REST, S3) wrap this into their own response shapes.
pub struct ListItem {
    pub key: String,
    pub info: beyond_objects_storage::ObjectInfo,
}

pub struct ListPage {
    pub items: Vec<ListItem>,
    pub next_cursor: Option<String>,
}

/// Propagates W3C trace context (`traceparent`/`tracestate`) from incoming
/// requests so spans are children of the caller's trace, not fresh roots.
#[derive(Clone, Default)]
struct OtelMakeSpan;

impl<B> MakeSpan<B> for OtelMakeSpan {
    fn make_span(&mut self, request: &axum::http::Request<B>) -> tracing::Span {
        use tracing_opentelemetry::OpenTelemetrySpanExt as _;

        let span = tracing::info_span!(
            "http.request",
            http.method = %request.method(),
            http.target = %request.uri(),
            http.flavor = ?request.version(),
            otel.kind = "server",
            http.route = tracing::field::Empty,
            http.status_code = tracing::field::Empty,
            bucket = tracing::field::Empty,
            key = tracing::field::Empty,
        );
        let parent_cx = telemetry::extract_trace_context(request.headers());
        let _ = span.set_parent(parent_cx);
        span
    }
}

#[derive(Clone)]
struct MakeRequestUuid;

impl MakeRequestId for MakeRequestUuid {
    fn make_request_id<B>(&mut self, _: &axum::http::Request<B>) -> Option<RequestId> {
        let id = Uuid::new_v4().to_string().parse().ok()?;
        Some(RequestId::new(id))
    }
}

/// Build the full router. Public routes (`/livez`, `/readyz`, `/metrics`,
/// `/v1/openapi.json`) sit outside the auth boundary; per-resource auth is
/// applied inside `routes::router`.
///
/// The S3-compatible surface is mounted as `fallback_service` so that explicit
/// `/v1/*`, `/livez`, `/readyz`, and `/v1/openapi.json` routes always win. Any
/// unmatched URL (e.g. `GET /` for `ListBuckets`, `PUT /{bucket}/{key}` for
/// `PutObject`) is handed to s3s.
pub fn build_router(state: AppState) -> Router {
    let openapi = routes::ApiDoc::openapi();
    let s3_fallback = s3::service(state.clone());

    routes::router(state.clone())
        .route(
            "/v1/openapi.json",
            get(move || {
                let openapi = openapi.clone();
                async move { Json(openapi) }
            }),
        )
        .route("/livez", get(routes::healthz::livez))
        .route("/readyz", get(routes::healthz::readyz))
        .route("/metrics", get(metrics_handler))
        .with_state(state.clone())
        .fallback_service(
            ServiceBuilder::new()
                .layer(DefaultBodyLimit::disable())
                .service(s3_fallback),
        )
        .route_layer(from_fn_with_state(state, record_metrics))
        .route_layer(DefaultBodyLimit::max(64 * 1024))
        .layer(
            ServiceBuilder::new()
                .layer(SetRequestIdLayer::x_request_id(MakeRequestUuid))
                .layer(PropagateRequestIdLayer::x_request_id())
                .layer(TraceLayer::new_for_http().make_span_with(OtelMakeSpan))
                .layer(CatchPanicLayer::new()),
        )
        .layer(
            CorsLayer::new()
                .allow_origin(Any)
                .allow_methods(Any)
                .allow_headers([header::AUTHORIZATION, header::CONTENT_TYPE]),
        )
}

async fn record_metrics(State(state): State<AppState>, req: Request, next: Next) -> Response {
    state.metrics.http_connections_active.inc();
    let method = req.method().clone();
    // Fallback to "<unmatched>" — not the raw URI — to avoid unbounded label
    // cardinality from S3 requests that hit the fallback_service (which sets
    // no MatchedPath) with arbitrary bucket/key paths.
    let path = req
        .extensions()
        .get::<MatchedPath>()
        .map(|m| m.as_str().to_string())
        .unwrap_or_else(|| "<unmatched>".to_string());
    tracing::Span::current().record("http.route", &path);
    let timer = state
        .metrics
        .http_request_duration_seconds
        .with_label_values(&[method.as_str(), &path]);
    let start = Instant::now();

    let response = next.run(req).await;

    state.metrics.http_connections_active.dec();
    let status = response.status().as_u16();
    state
        .metrics
        .http_requests_total
        .with_label_values(&[method.as_str(), &path, &status.to_string()])
        .inc();
    timer.observe(start.elapsed().as_secs_f64());

    tracing::Span::current().record("http.status_code", status);

    response
}

async fn metrics_handler(State(state): State<AppState>) -> impl IntoResponse {
    (
        axum::http::StatusCode::OK,
        [(
            header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        state.metrics.encode(),
    )
        .into_response()
}

pub async fn serve(config: Config) -> Result<()> {
    // Validate LOG_LEVEL before initializing telemetry so we get a clear error
    // on misconfiguration rather than silently falling back to the default.
    tracing_subscriber::EnvFilter::try_new(&config.log_level)
        .map_err(|e| anyhow::anyhow!("invalid LOG_LEVEL {:?}: {e}", config.log_level))?;

    let otel_config = telemetry::OtelConfig {
        enabled: config.otlp_enabled,
        otlp_endpoint: config.otlp_endpoint.clone(),
        service_name: "beyond-objects".into(),
        sample_rate: config.otlp_sample_rate,
    };
    let otel_guard = telemetry::init(&otel_config, vec![], &config.log_level)?;

    if config.public_url.is_none() {
        tracing::warn!(
            address = %config.address,
            "OBJECTS_URL is not set; object URLs in list responses will use the bind address, \
             which may be unreachable by clients. Set OBJECTS_URL to the public base URL."
        );
    }

    // Decide our role before opening anything: a Successor must wait for the
    // supervisor's `Begin` cue before acquiring the data-dir lock, since the
    // incumbent still holds it until SealComplete. The typestate chain
    // (Successor → HandshookSuccessor → BegunSuccessor) makes out-of-order
    // calls compile-time impossible.
    let build_id = env!("CARGO_PKG_VERSION").as_bytes().to_vec();
    let (mut inherited_listeners, mut successor) = match ::handoff::detect_role()
        .map_err(|e| anyhow::anyhow!("handoff::detect_role: {e}"))?
    {
        ::handoff::Role::ColdStart { inherited } => {
            tracing::info!(
                inherited_listeners = ?inherited.names(),
                "starting in cold-start mode"
            );
            (inherited, None)
        }
        ::handoff::Role::Successor(s) => {
            let s = s
                .handshake(build_id.clone())
                .map_err(|e| anyhow::anyhow!("handshake: {e}"))?;
            tracing::info!(handoff_id = %s.handoff_id(), "handshake complete; waiting for Begin");
            let s = s
                .wait_for_begin()
                .map_err(|e| anyhow::anyhow!("wait_for_begin: {e}"))?;
            tracing::info!(
                handoff_id = %s.handoff_id(),
                "Begin received; proceeding with successor startup"
            );
            (::handoff::role::InheritedListeners::default(), Some(s))
        }
    };

    tokio::fs::create_dir_all(&config.data_dir).await?;
    tokio::fs::create_dir_all(&config.index_dir).await?;

    // Best-effort: create the parent of the handoff control socket so a
    // fresh deploy that hasn't set up systemd's RuntimeDirectory= doesn't
    // hit a confusing "bind: permission denied". If the parent isn't
    // writable, the bind below will surface the real error.
    if let Some(parent) = config.handoff_socket_path.parent()
        && !parent.as_os_str().is_empty()
    {
        let _ = tokio::fs::create_dir_all(parent).await;
    }

    // Acquire the data-dir lock. For a Successor this succeeds immediately
    // because the prior incumbent has already released it (post-SealComplete).
    // For a Cold Start we break stale pidfiles from crashed predecessors.
    let data_dir_lock = ::handoff::DataDirLock::acquire_or_break_stale(&config.data_dir)
        .map_err(|e| anyhow::anyhow!("acquire data-dir lock {}: {e}", config.data_dir.display()))?;

    let storage = if config.sync_linger_ms > 0 {
        let linger = std::time::Duration::from_millis(config.sync_linger_ms);
        Storage::with_linger(&config.data_dir, linger)
    } else {
        Storage::new(&config.data_dir)
    };

    // The `default` bucket is auto-managed: clients may PUT/GET against it
    // without an explicit POST /v1/buckets. Materialize its directory so the
    // write path's rename target exists. Idempotent.
    storage
        .create_bucket("default", beyond_objects_storage::AccessLevel::default())
        .await
        .map_err(|e| anyhow::anyhow!("create default bucket: {e}"))?;

    let index = Arc::new(Index::open(&config.index_dir)?);

    // Reconcile the listing index against the filesystem before serving.
    {
        let idx = index.clone();
        let data_dir = config.data_dir.clone();
        tracing::info!("starting index reconcile");
        let reconcile_start = std::time::Instant::now();
        let stats = tokio::task::spawn_blocking(move || idx.reconcile(&data_dir))
            .await
            .map_err(|e| anyhow::anyhow!("reconcile join: {e}"))??;
        let elapsed_ms = reconcile_start.elapsed().as_millis();
        if stats.inserted > 0 || stats.removed > 0 {
            tracing::warn!(
                elapsed_ms,
                recovered = stats.inserted,
                removed = stats.removed,
                "startup reconcile recovered objects missing from index — prior instance may have crashed mid-write"
            );
        } else {
            tracing::info!(elapsed_ms, "startup reconcile complete, index consistent");
        }
    }

    // GC orphaned temp files and stale multipart uploads left by prior crashes.
    // Runs after reconcile so the index is consistent before we clean storage.
    match storage
        .gc_temp_files(std::time::Duration::from_secs(config.gc_temp_ttl_secs))
        .await
    {
        Ok(0) | Ok(_) => {}
        Err(e) => tracing::warn!(err = %e, "startup gc_temp_files failed"),
    }
    match storage
        .gc_multipart_uploads(std::time::Duration::from_secs(config.gc_multipart_ttl_secs))
        .await
    {
        Ok(0) | Ok(_) => {}
        Err(e) => tracing::warn!(err = %e, "startup gc_multipart_uploads failed"),
    }

    let address = config.address.clone();
    let drain_timeout_secs = config.drain_timeout_secs;
    let handoff_socket_path = config.handoff_socket_path.clone();
    let state_tls_cert = config.tls_cert.clone();
    let state_tls_key = config.tls_key.clone();
    let state_tls_ca = config.tls_ca.clone();
    let state = AppState {
        config: Arc::new(config),
        storage,
        index: index.clone(),
        metrics: Arc::new(metrics::Metrics::new()),
    };
    let metrics = state.metrics.clone();

    let app = build_router(state);

    // Prefer the inherited listener (from the supervisor, either at cold start
    // or via the successor handshake) over a fresh bind so the kernel SYN
    // queue carries over.
    let listener = match handoff::take_http_listener(&mut successor, &mut inherited_listeners)
        .map_err(|e| anyhow::anyhow!("inherit http listener: {e}"))?
    {
        Some(l) => {
            tracing::info!(addr = ?l.local_addr().ok(), "HTTP listening on inherited fd");
            l
        }
        None => {
            tracing::info!(address = %address, "HTTP listening on fresh bind");
            tokio::net::TcpListener::bind(&address).await?
        }
    };

    let tls = match (&state_tls_cert, &state_tls_key, &state_tls_ca) {
        (Some(cert), Some(key), Some(ca)) => Some((cert.clone(), key.clone(), ca.clone())),
        (None, None, None) => None,
        _ => anyhow::bail!(
            "BEYOND_TLS_CERT, BEYOND_TLS_KEY, and BEYOND_TLS_CA must all be set or all unset"
        ),
    };
    tracing::info!(address = %address, tls = tls.is_some(), "listening");

    // Build the handoff control thread's view of the world. `accept_closed`
    // is shared with both the plaintext PausableListener and the TLS accept
    // loop so they stop dispatching new connections during a drain.
    let accept_closed = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let objects_handoff =
        handoff::ObjectsHandoff::new(accept_closed.clone(), index.clone(), metrics.clone());

    // Test hook: simulate a successor crash *before* Ready so the supervisor
    // hits the ResumeAfterAbort path and the old incumbent has to recover
    // for real. Honored only when the env var is set; production never sets it.
    if successor.is_some() && std::env::var("OBJECTS_TEST_PANIC_BEFORE_READY").is_ok() {
        tracing::warn!("OBJECTS_TEST_PANIC_BEFORE_READY set; exiting before announce_and_bind");
        std::process::exit(42);
    }

    // Bind the control socket. For a successor we go through
    // `announce_and_bind` so Ready is sent before we touch the path; for
    // cold start we go directly to `bind_cold_start`. The successor path's
    // bind happens AFTER Ready (and thus after the supervisor will commit
    // the prior incumbent), so a successor that dies pre-Ready never touches
    // the path.
    let incumbent = match successor.take() {
        Some(s) => s
            .announce_and_bind(
                handoff::readiness_snapshot(&address),
                &handoff_socket_path,
                data_dir_lock,
            )
            .map_err(|e| anyhow::anyhow!("announce_and_bind: {e}"))?,
        None => ::handoff::Incumbent::bind_cold_start(&handoff_socket_path, data_dir_lock)
            .map_err(|e| anyhow::anyhow!("bind handoff control socket: {e}"))?,
    }
    .with_build_id(build_id);

    // Run the incumbent control loop on tokio's blocking pool. On Ok(()) the
    // supervisor has committed; signal the serve loop to shut down gracefully.
    let (commit_tx, commit_rx) = tokio::sync::oneshot::channel::<()>();
    let metrics_for_handoff = metrics.clone();
    tokio::task::spawn_blocking(move || match incumbent.serve(objects_handoff) {
        Ok(()) => {
            metrics_for_handoff
                .handoff_handoffs_total
                .with_label_values(&["committed"])
                .inc();
            tracing::info!("handoff committed; signaling main to exit");
            let _ = commit_tx.send(());
        }
        Err(e) => {
            tracing::error!(error = %e, "handoff control thread exited with error");
        }
    });

    if let Some((cert, key, ca)) = tls {
        serve_tls(listener, &cert, &key, &ca, app, accept_closed, commit_rx).await?;
    } else {
        let pausable = handoff::PausableListener {
            inner: listener,
            accept_closed,
        };

        // Pair a oneshot with the shutdown signal so the drain timer arms only
        // when shutdown was signal-driven (not commit-driven — at that point
        // the handoff thread has already drained + sealed, and any in-flight
        // connections will close as the process exits).
        let (signal_tx, signal_rx) = tokio::sync::oneshot::channel::<()>();
        let serve = axum::serve(pausable, app).with_graceful_shutdown(async move {
            tokio::select! {
                _ = shutdown_signal() => {
                    signal_tx.send(()).ok();
                }
                _ = commit_rx => {
                    tracing::info!("commit-driven shutdown; draining connections");
                }
            }
        });

        if drain_timeout_secs > 0 {
            tokio::select! {
                result = serve => { result?; }
                _ = async move {
                    if signal_rx.await.is_ok() {
                        tokio::time::sleep(std::time::Duration::from_secs(drain_timeout_secs)).await;
                        tracing::warn!(
                            drain_timeout_secs,
                            "drain timeout exceeded, forcing shutdown"
                        );
                    }
                } => {}
            }
        } else {
            serve.await?;
        }
    }

    // The `Incumbent::serve` blocking thread is parked reading the control
    // socket and cannot be cancelled by dropping the tokio runtime. Without
    // this exit call, a SIGTERM that triggers graceful shutdown leaves the
    // process alive on its detached blocking thread until k8s/systemd
    // SIGKILLs it. Matches kv's sync-main behavior where main returning IS
    // process exit.
    //
    // `process::exit(0)` skips all Rust destructors, so the OTel guard's
    // Drop (which flushes buffered spans with a 5s deadline) would not run.
    // Drop it explicitly first.
    drop(otel_guard);
    std::process::exit(0);
}

async fn shutdown_signal() {
    use tokio::signal;

    let ctrl_c = async {
        if let Err(e) = signal::ctrl_c().await {
            tracing::warn!(err = %e, "Ctrl+C handler unavailable; relying on SIGTERM");
            std::future::pending::<()>().await;
        }
    };

    #[cfg(unix)]
    let terminate = async {
        match signal::unix::signal(signal::unix::SignalKind::terminate()) {
            Ok(mut sig) => {
                sig.recv().await;
            }
            Err(e) => {
                tracing::warn!(err = %e, "SIGTERM handler unavailable; relying on Ctrl+C");
                std::future::pending::<()>().await;
            }
        }
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }

    tracing::info!("shutdown signal received, draining connections");
}

async fn serve_tls(
    listener: tokio::net::TcpListener,
    cert_path: &str,
    key_path: &str,
    ca_path: &str,
    app: Router,
    accept_closed: Arc<std::sync::atomic::AtomicBool>,
    commit_rx: tokio::sync::oneshot::Receiver<()>,
) -> Result<()> {
    use hyper_util::rt::{TokioExecutor, TokioIo};
    use hyper_util::server::conn::auto::Builder;
    use rustls::RootCertStore;
    use rustls::ServerConfig;
    use rustls::server::WebPkiClientVerifier;
    use std::sync::atomic::Ordering;
    use tokio_rustls::TlsAcceptor;

    let server_certs = tls_load_certs(cert_path)?;
    let server_key = tls_load_key(key_path)?;
    let ca_certs = tls_load_certs(ca_path)?;

    let mut ca_store = RootCertStore::empty();
    for cert in ca_certs {
        ca_store.add(cert)?;
    }

    let provider = std::sync::Arc::new(rustls::crypto::ring::default_provider());
    let verifier = WebPkiClientVerifier::builder_with_provider(
        std::sync::Arc::new(ca_store),
        provider.clone(),
    )
    .build()?;

    let mut cfg = ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()?
        .with_client_cert_verifier(verifier)
        .with_single_cert(server_certs, server_key)?;
    cfg.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];

    let acceptor = TlsAcceptor::from(std::sync::Arc::new(cfg));

    // Unify the signal-driven and commit-driven shutdown triggers. Mirrors
    // the plaintext path so behavior is identical across modes.
    let shutdown = async {
        tokio::select! {
            _ = shutdown_signal() => {}
            _ = commit_rx => {
                tracing::info!("commit-driven shutdown; closing TLS accept loop");
            }
        }
    };
    tokio::pin!(shutdown);

    loop {
        // During a handoff drain, suspend new accepts instead of exiting —
        // the kernel SYN queue absorbs incoming connections until the
        // successor's accept on the inherited FD drains them.
        if accept_closed.load(Ordering::Relaxed) {
            tokio::select! {
                _ = tokio::time::sleep(std::time::Duration::from_millis(25)) => continue,
                _ = &mut shutdown => break,
            }
        }
        tokio::select! {
            result = listener.accept() => {
                let (tcp, _) = result?;
                let acceptor = acceptor.clone();
                let app = app.clone();
                tokio::spawn(async move {
                    match acceptor.accept(tcp).await {
                        Ok(tls_stream) => {
                            let io = TokioIo::new(tls_stream);
                            let svc = hyper::service::service_fn(move |req: axum::http::Request<hyper::body::Incoming>| app.clone().oneshot(req));
                            Builder::new(TokioExecutor::new())
                                .serve_connection_with_upgrades(io, svc)
                                .await
                                .ok();
                        }
                        Err(e) => tracing::debug!(error = %e, "TLS handshake failed"),
                    }
                });
            }
            _ = &mut shutdown => break,
        }
    }
    Ok(())
}

/// Spin up the HTTP server on a pre-bound listener with optional TLS. Used by
/// in-process tests that bypass the handoff lifecycle. The handoff machinery
/// is intentionally *not* wired here — these calls never see a supervisor.
pub async fn serve_with_listener(
    listener: tokio::net::TcpListener,
    tls: Option<(String, String, String)>,
    app: Router,
) -> Result<()> {
    if let Some((cert, key, ca)) = tls {
        // No commit channel, no accept_closed gate — pass inert versions so
        // the path runs end-to-end without a handoff control thread.
        let (_tx, rx) = tokio::sync::oneshot::channel::<()>();
        let inert = Arc::new(std::sync::atomic::AtomicBool::new(false));
        serve_tls(listener, &cert, &key, &ca, app, inert, rx).await
    } else {
        axum::serve(listener, app).await?;
        Ok(())
    }
}

fn tls_load_certs(path: &str) -> Result<Vec<rustls::pki_types::CertificateDer<'static>>> {
    let f = std::fs::File::open(path)?;
    rustls_pemfile::certs(&mut std::io::BufReader::new(f))
        .collect::<Result<Vec<_>, _>>()
        .map_err(Into::into)
}

fn tls_load_key(path: &str) -> Result<rustls::pki_types::PrivateKeyDer<'static>> {
    let f = std::fs::File::open(path)?;
    rustls_pemfile::private_key(&mut std::io::BufReader::new(f))?
        .ok_or_else(|| anyhow::anyhow!("no private key found in {path}"))
}
