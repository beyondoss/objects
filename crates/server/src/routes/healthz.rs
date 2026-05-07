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

/// Health check. Probes the data directory and the listing index. Returns 200
/// when both are reachable, 503 otherwise.
#[utoipa::path(
    get,
    path = "/healthz",
    operation_id = "healthz",
    tag = "system",
    responses(
        (status = 200, description = "Healthy: data directory and listing index are reachable.", body = HealthzResponse),
        (status = 503, description = "Degraded: data directory or listing index is unreachable.", body = HealthzResponse),
    )
)]
pub async fn handler(State(state): State<AppState>) -> (StatusCode, Json<HealthzResponse>) {
    let data_ok = tokio::fs::metadata(&state.config.data_dir).await.is_ok();
    let index = state.index.clone();
    let index_ok = tokio::task::spawn_blocking(move || index.scan("__healthz__", "", None, 1))
        .await
        .ok()
        .and_then(Result::ok)
        .is_some();

    if data_ok && index_ok {
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
