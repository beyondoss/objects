use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use beyond_objects_storage::{AccessLevel, StorageError};

use crate::{AppState, error::ApiError};

/// Body for `POST /v1/buckets`.
#[derive(Deserialize, ToSchema)]
pub struct CreateBucketRequest {
    /// Bucket name. Must be a single path segment (no slashes); cannot be the
    /// reserved `default` literal — that bucket is auto-managed.
    #[schema(example = "photos")]
    pub name: String,
    /// Default access level inherited by objects in this bucket when an object
    /// has no explicit `access` xattr. Defaults to `private`.
    #[serde(default)]
    #[schema(value_type = String, example = "private")]
    pub access: AccessLevel,
}

/// Body for `PATCH /v1/buckets/{name}`.
#[derive(Deserialize, ToSchema)]
pub struct UpdateBucketRequest {
    /// New default access level for objects in the bucket. Existing objects
    /// keep their per-object xattr; only the inherited default changes.
    #[schema(value_type = String, example = "public")]
    pub access: AccessLevel,
}

/// Bucket metadata.
#[derive(Serialize, ToSchema)]
pub struct BucketResponse {
    /// Bucket name.
    #[schema(example = "photos")]
    pub name: String,
    /// Default access level for objects in this bucket.
    #[schema(value_type = String, example = "private")]
    pub access: AccessLevel,
}

/// Result of `GET /v1/buckets`.
#[derive(Serialize, ToSchema)]
pub struct ListBucketsResponse {
    /// Buckets, sorted by name.
    pub buckets: Vec<BucketResponse>,
}

/// Create a bucket. Idempotent: succeeds if the bucket already exists, but the
/// access level is updated to match the request.
#[utoipa::path(
    post,
    path = "/v1/buckets",
    operation_id = "create_bucket",
    tag = "buckets",
    request_body = CreateBucketRequest,
    security(("BearerAuth" = [])),
    responses(
        (status = 201, description = "Bucket created (or already existed with the same access level).", body = BucketResponse),
        (status = 400, description = "Bucket name is invalid (contains `/`, is empty, or is the reserved `default`).", body = crate::error::ErrorResponse),
        (status = 401, description = "Missing or invalid root token.", body = crate::error::ErrorResponse),
    )
)]
pub async fn create_bucket(
    State(state): State<AppState>,
    Json(req): Json<CreateBucketRequest>,
) -> Result<impl IntoResponse, ApiError> {
    state.storage.create_bucket(&req.name, req.access).await?;
    Ok((
        StatusCode::CREATED,
        Json(BucketResponse {
            name: req.name,
            access: req.access,
        }),
    ))
}

/// List all buckets, sorted by name.
#[utoipa::path(
    get,
    path = "/v1/buckets",
    operation_id = "list_buckets",
    tag = "buckets",
    security(("BearerAuth" = [])),
    responses(
        (status = 200, description = "All buckets, sorted by name.", body = ListBucketsResponse),
        (status = 401, description = "Missing or invalid root token.", body = crate::error::ErrorResponse),
    )
)]
pub async fn list_buckets(
    State(state): State<AppState>,
) -> Result<Json<ListBucketsResponse>, ApiError> {
    let buckets = state.storage.list_buckets().await?;
    Ok(Json(ListBucketsResponse {
        buckets: buckets
            .into_iter()
            .map(|b| BucketResponse {
                name: b.name,
                access: b.access,
            })
            .collect(),
    }))
}

/// Get bucket metadata.
#[utoipa::path(
    get,
    path = "/v1/buckets/{name}",
    operation_id = "get_bucket",
    tag = "buckets",
    params(("name" = String, Path, description = "Bucket name.")),
    security(("BearerAuth" = [])),
    responses(
        (status = 200, description = "Bucket metadata.", body = BucketResponse),
        (status = 401, description = "Missing or invalid root token.", body = crate::error::ErrorResponse),
        (status = 404, description = "Bucket does not exist.", body = crate::error::ErrorResponse),
    )
)]
pub async fn get_bucket(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<Json<BucketResponse>, ApiError> {
    let b = state.storage.get_bucket(&name).await?;
    Ok(Json(BucketResponse {
        name: b.name,
        access: b.access,
    }))
}

/// Update bucket configuration.
#[utoipa::path(
    patch,
    path = "/v1/buckets/{name}",
    operation_id = "update_bucket",
    tag = "buckets",
    params(("name" = String, Path, description = "Bucket name.")),
    request_body = UpdateBucketRequest,
    security(("BearerAuth" = [])),
    responses(
        (status = 200, description = "Updated bucket metadata.", body = BucketResponse),
        (status = 401, description = "Missing or invalid root token.", body = crate::error::ErrorResponse),
        (status = 404, description = "Bucket does not exist.", body = crate::error::ErrorResponse),
    )
)]
pub async fn update_bucket(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Json(req): Json<UpdateBucketRequest>,
) -> Result<Json<BucketResponse>, ApiError> {
    state.storage.update_bucket(&name, req.access).await?;
    Ok(Json(BucketResponse {
        name,
        access: req.access,
    }))
}

/// Delete a bucket. The bucket must be empty.
#[utoipa::path(
    delete,
    path = "/v1/buckets/{name}",
    operation_id = "delete_bucket",
    tag = "buckets",
    params(("name" = String, Path, description = "Bucket name.")),
    security(("BearerAuth" = [])),
    responses(
        (status = 204, description = "Bucket deleted, or did not exist (idempotent)."),
        (status = 401, description = "Missing or invalid root token.", body = crate::error::ErrorResponse),
        (status = 409, description = "Bucket still contains objects — delete them first.", body = crate::error::ErrorResponse),
    )
)]
pub async fn delete_bucket(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<StatusCode, ApiError> {
    match state.storage.delete_bucket(&name).await {
        Ok(()) | Err(StorageError::BucketNotFound(_)) => {}
        Err(e) => return Err(e.into()),
    }
    Ok(StatusCode::NO_CONTENT)
}
