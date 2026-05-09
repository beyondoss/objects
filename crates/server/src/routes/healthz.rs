use axum::{Json, extract::State, http::StatusCode};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::AppState;

/// Health check response.
#[derive(Serialize, Deserialize, ToSchema)]
pub struct HealthzResponse {
    /// `"ok"` when both the data directory and the listing index are reachable;
    /// `"degraded"` when one of them is not.
    #[schema(example = "ok")]
    pub status: &'static str,
    /// Service version from `CARGO_PKG_VERSION` at compile time.
    #[schema(example = "0.1.0")]
    pub version: &'static str,
}

/// Liveness probe. Returns 200 while the process is running. Does **not** probe
/// dependencies — use `/readyz` for Kubernetes `readinessProbe`.
#[utoipa::path(
    get,
    path = "/livez",
    operation_id = "livez",
    tag = "system",
    responses(
        (status = 200, description = "Process is alive.", body = HealthzResponse),
    )
)]
pub async fn livez() -> (StatusCode, Json<HealthzResponse>) {
    (
        StatusCode::OK,
        Json(HealthzResponse {
            status: "ok",
            version: env!("CARGO_PKG_VERSION"),
        }),
    )
}

/// Readiness probe. Probes the data directory and the listing index. Returns 200
/// when both are reachable, 503 otherwise. Use for Kubernetes `readinessProbe`.
#[utoipa::path(
    get,
    path = "/readyz",
    operation_id = "readyz",
    tag = "system",
    responses(
        (status = 200, description = "Ready: data directory and listing index are reachable.", body = HealthzResponse),
        (status = 503, description = "Degraded: data directory or listing index is unreachable.", body = HealthzResponse),
    )
)]
pub async fn readyz(State(state): State<AppState>) -> (StatusCode, Json<HealthzResponse>) {
    // Probe the data directory with a write-then-delete so a read-only
    // filesystem remount (a common disk-error recovery mode) is caught before
    // the first real PUT fails in production.
    let sentinel = state
        .config
        .data_dir
        .join(".tmp")
        .join(format!(".readyz-{}", uuid::Uuid::new_v4()));
    let write_ok = async {
        tokio::fs::create_dir_all(sentinel.parent().unwrap())
            .await
            .ok();
        tokio::fs::write(&sentinel, b"").await.is_ok()
            && tokio::fs::remove_file(&sentinel).await.is_ok()
    }
    .await;

    let index = state.index.clone();
    let index_ok = tokio::task::spawn_blocking(move || index.scan("__readyz__", "", None, 1))
        .await
        .ok()
        .and_then(Result::ok)
        .is_some();

    if write_ok && index_ok {
        (
            StatusCode::OK,
            Json(HealthzResponse {
                status: "ok",
                version: env!("CARGO_PKG_VERSION"),
            }),
        )
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(HealthzResponse {
                status: "degraded",
                version: env!("CARGO_PKG_VERSION"),
            }),
        )
    }
}
