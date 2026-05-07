pub mod cli;
pub mod config;
pub mod error;
pub mod metrics;
pub mod middleware;
pub mod routes;
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
    trace::TraceLayer,
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
}

#[derive(Clone)]
struct MakeRequestUuid;

impl MakeRequestId for MakeRequestUuid {
    fn make_request_id<B>(&mut self, _: &axum::http::Request<B>) -> Option<RequestId> {
        let id = Uuid::new_v4().to_string().parse().ok()?;
        Some(RequestId::new(id))
    }
}

/// Build the full router. Public routes (`/healthz`, `/v1/openapi.json`)
/// sit outside the auth boundary; per-resource auth is applied inside
/// `routes::router`. The `/metrics` endpoint lives on a separate internal-only
/// listener built via `build_metrics_router`.
pub fn build_router(state: AppState) -> Router {
    let openapi = routes::ApiDoc::openapi();

    routes::router(state.clone())
        .route(
            "/v1/openapi.json",
            get(move || {
                let openapi = openapi.clone();
                async move { Json(openapi) }
            }),
        )
        .route("/healthz", get(routes::healthz::handler))
        .with_state(state.clone())
        .route_layer(from_fn_with_state(state, record_metrics))
        .layer(DefaultBodyLimit::max(64 * 1024))
        .layer(
            ServiceBuilder::new()
                .layer(SetRequestIdLayer::x_request_id(MakeRequestUuid))
                .layer(PropagateRequestIdLayer::x_request_id())
                .layer(TraceLayer::new_for_http())
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
    let method = req.method().clone();
    let path = req
        .extensions()
        .get::<MatchedPath>()
        .map(|m| m.as_str().to_string())
        .unwrap_or_else(|| req.uri().path().to_string());
    let timer = state
        .metrics
        .http_request_duration_seconds
        .with_label_values(&[method.as_str(), &path]);
    let start = Instant::now();

    let response = next.run(req).await;

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
    match state.metrics.render() {
        Ok(body) => (
            axum::http::StatusCode::OK,
            [(
                header::CONTENT_TYPE,
                "text/plain; version=0.0.4; charset=utf-8",
            )],
            body,
        )
            .into_response(),
        Err(e) => {
            tracing::error!(error = %e, "failed to encode metrics");
            axum::http::StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

pub async fn serve(config: Config) -> Result<()> {
    let otel_config = telemetry::OtelConfig {
        enabled: config.otlp_enabled,
        otlp_endpoint: config.otlp_endpoint.clone(),
        service_name: "beyond-objects".into(),
        sample_rate: 1.0,
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

    let storage = Storage::new(&config.data_dir);
    let index = Arc::new(Index::open(&config.index_dir)?);

    // Reconcile the listing index against the filesystem before serving.
    {
        let idx = index.clone();
        let data_dir = config.data_dir.clone();
        tokio::task::spawn_blocking(move || idx.reconcile(&data_dir))
            .await
            .map_err(|e| anyhow::anyhow!("reconcile join: {e}"))??;
    }

    let address = config.address.clone();
    let metrics_address = config.metrics_address.clone();
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
    tokio::spawn(async move {
        axum::serve(metrics_listener, metrics_app).await.ok();
    });

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    Ok(())
}

async fn shutdown_signal() {
    use tokio::signal;

    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }

    tracing::info!("shutdown signal received, draining connections");
}
