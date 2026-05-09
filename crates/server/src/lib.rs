pub mod cli;
pub mod config;
pub mod error;
pub mod metrics;
pub mod middleware;
pub mod routes;
pub mod s3;
pub mod telemetry;
pub mod test_support;

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
use tower::ServiceBuilder;
use tower_http::{
    catch_panic::CatchPanicLayer,
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
    pub async fn index_insert(&self, bucket: &str, key: &str) -> Result<(), error::ApiError> {
        let idx = self.index.clone();
        let bucket = bucket.to_owned();
        let key = key.to_owned();
        tokio::task::spawn_blocking(move || idx.insert(&bucket, &key))
            .await
            .map_err(|e| error::ApiError::Internal(anyhow::anyhow!("index insert join: {e}")))??;
        Ok(())
    }

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
        let keys = tokio::task::spawn_blocking(move || {
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
            http.uri = %request.uri(),
            http.version = ?request.version(),
            otel.kind = "server",
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

/// Build the full router. Public routes (`/livez`, `/readyz`, `/v1/openapi.json`)
/// sit outside the auth boundary; per-resource auth is applied inside
/// `routes::router`. The `/metrics` endpoint lives on a separate internal-only
/// listener built via `build_metrics_router`.
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
}

/// Build the internal metrics router. Bind this on a private interface — the
/// `/metrics` endpoint is unauthenticated.
pub fn build_metrics_router(state: AppState) -> Router {
    Router::new()
        .route("/metrics", get(metrics_handler))
        .with_state(state)
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
    let timer = state
        .metrics
        .http_request_duration_seconds
        .with_label_values(&[method.as_str(), &path]);
    let start = Instant::now();

    let response = next.run(req).await;

    state.metrics.http_connections_active.dec();
    let status = response.status().as_u16().to_string();
    state
        .metrics
        .http_requests_total
        .with_label_values(&[method.as_str(), &path, &status])
        .inc();
    timer.observe(start.elapsed().as_secs_f64());

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
    let _otel_guard = telemetry::init(&otel_config, vec![], &config.log_level)?;

    if config.public_url.is_none() {
        tracing::warn!(
            address = %config.address,
            "OBJECTS_URL is not set; object URLs in list responses will use the bind address, \
             which may be unreachable by clients. Set OBJECTS_URL to the public base URL."
        );
    }

    tokio::fs::create_dir_all(&config.data_dir).await?;
    tokio::fs::create_dir_all(&config.index_dir).await?;

    let storage = if config.sync_linger_ms > 0 {
        let linger = std::time::Duration::from_millis(config.sync_linger_ms);
        Storage::with_linger(&config.data_dir, linger)
    } else {
        Storage::new(&config.data_dir)
    };
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
    let metrics_address = config.metrics_address.clone();
    let drain_timeout_secs = config.drain_timeout_secs;
    let state = AppState {
        config: Arc::new(config),
        storage,
        index,
        metrics: Arc::new(metrics::Metrics::new()),
    };

    let app = build_router(state.clone());
    let metrics_app = build_metrics_router(state);
    let listener = tokio::net::TcpListener::bind(&address).await?;
    tracing::info!(address = %address, "listening");

    let metrics_listener = tokio::net::TcpListener::bind(&metrics_address).await?;
    tracing::info!(address = %metrics_address, "metrics listening");
    let metrics_task = tokio::spawn(async move {
        if let Err(e) = axum::serve(metrics_listener, metrics_app).await {
            tracing::error!(err = %e, "metrics server exited");
        }
    });

    // Pair a oneshot with the shutdown future so we can start the drain timer
    // only after the signal fires, not from process start.
    let (signal_tx, signal_rx) = tokio::sync::oneshot::channel::<()>();
    let serve = axum::serve(listener, app).with_graceful_shutdown(async move {
        shutdown_signal().await;
        signal_tx.send(()).ok();
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

    metrics_task.abort();
    Ok(())
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
