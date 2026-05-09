use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
};
use secrecy::ExposeSecret;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::{AppState, error::ApiError, upload_token};

fn default_ttl() -> u64 {
    upload_token::DEFAULT_TTL_SECS
}

/// Body for `POST /v1/{bucket}/upload-tokens`.
#[derive(Deserialize, ToSchema)]
pub struct CreateUploadTokenRequest {
    /// Object key this token authorizes a single PUT for.
    #[schema(example = "avatars/user123.png")]
    pub key: String,
    /// Token lifetime in seconds. Must be between 1 and 86400 (1 day).
    /// Defaults to 3600 (1 hour).
    #[serde(default = "default_ttl")]
    #[schema(example = 3600, minimum = 1, maximum = 86400)]
    pub ttl_secs: u64,
}

/// A short-lived upload token scoped to a single object key.
#[derive(Serialize, ToSchema)]
pub struct UploadTokenResponse {
    /// Bearer token to present in `Authorization: Bearer <token>` when calling
    /// `PUT /v1/{bucket}/{key}`. Valid only for that exact key until `expires_at`.
    #[schema(example = "1748000000:a3f9b2c1...")]
    pub token: String,
    /// Unix timestamp (seconds) after which the token is rejected.
    #[schema(example = 1748003600)]
    pub expires_at: u64,
}

/// Create a short-lived upload token scoped to a specific object key.
///
/// The token may be handed to a browser client, which can use it as a `Bearer`
/// credential for exactly one `PUT /v1/{bucket}/{key}` request before expiry.
/// It cannot be used for GET, DELETE, or any other verb, nor for any other key.
#[utoipa::path(
    post,
    path = "/v1/{bucket}/upload-tokens",
    operation_id = "create_upload_token",
    tag = "objects",
    params(
        ("bucket" = String, Path, description = "Bucket name.", example = "photos")
    ),
    request_body = CreateUploadTokenRequest,
    security(("BearerAuth" = [])),
    responses(
        (status = 201, description = "Upload token created.", body = UploadTokenResponse),
        (status = 400, description = "Invalid request.", body = crate::error::ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token.", body = crate::error::ErrorResponse),
    )
)]
pub async fn create_upload_token(
    State(state): State<AppState>,
    Path(bucket): Path<String>,
    Json(req): Json<CreateUploadTokenRequest>,
) -> Result<impl IntoResponse, ApiError> {
    if req.ttl_secs == 0 || req.ttl_secs > upload_token::MAX_TTL_SECS {
        return Err(ApiError::BadRequest(
            "ttl_secs must be between 1 and 86400".into(),
        ));
    }

    let (token, expires_at) = upload_token::create(
        state.config.objects_root_token.expose_secret(),
        &bucket,
        &req.key,
        req.ttl_secs,
    );

    Ok((
        StatusCode::CREATED,
        Json(UploadTokenResponse { token, expires_at }),
    ))
}
