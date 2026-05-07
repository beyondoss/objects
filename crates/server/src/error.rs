use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use utoipa::ToSchema;

use beyond_objects_index::IndexError;
use beyond_objects_storage::StorageError;

/// Wire-format error body. The `code` field is the stable contract — clients
/// should switch on `code`, not on `message` (which is human-readable and may
/// change between versions).
#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct ErrorBody {
    /// Machine-readable error code. One of: `unauthorized`, `forbidden`,
    /// `object_not_found`, `bucket_not_found`, `bucket_not_empty`,
    /// `object_exists`, `etag_mismatch`, `invalid_key`, `bad_request`,
    /// `range_not_satisfiable`, `internal_error`.
    #[schema(example = "object_not_found")]
    pub code: String,
    /// Human-readable description. Suitable for logs and server-side debugging,
    /// not for UI display.
    #[schema(example = "not found: photos/avatar.png")]
    pub message: String,
    /// Optional actionable guidance, when one is available.
    #[schema(nullable)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hint: Option<String>,
}

/// Top-level error envelope returned on every non-2xx response.
#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct ErrorResponse {
    /// Error details.
    pub error: ErrorBody,
}

#[derive(Debug, Error)]
#[must_use = "errors must be handled or explicitly ignored with `let _ =`"]
pub enum ApiError {
    #[error("unauthorized")]
    Unauthorized,

    #[error("forbidden")]
    Forbidden,

    #[error("not found: {bucket}/{key}")]
    ObjectNotFound { bucket: String, key: String },

    #[error("bucket not found: {0}")]
    BucketNotFound(String),

    #[error("bucket not empty")]
    BucketNotEmpty,

    #[error("object already exists")]
    ObjectExists,

    #[error("etag mismatch")]
    EtagMismatch,

    #[error("invalid key: {0}")]
    InvalidKey(String),

    #[error("bad request: {0}")]
    BadRequest(String),

    #[error("range not satisfiable")]
    RangeNotSatisfiable,

    #[error("internal error: {0}")]
    Internal(#[from] anyhow::Error),
}

impl ApiError {
    pub fn bad_request(msg: impl Into<String>) -> Self {
        Self::BadRequest(msg.into())
    }
}

impl From<StorageError> for ApiError {
    fn from(e: StorageError) -> Self {
        match e {
            StorageError::NotFound { bucket, key } => Self::ObjectNotFound { bucket, key },
            StorageError::BucketNotFound(b) => Self::BucketNotFound(b),
            StorageError::BucketNotEmpty => Self::BucketNotEmpty,
            StorageError::ObjectExists { .. } => Self::ObjectExists,
            StorageError::EtagMismatch => Self::EtagMismatch,
            StorageError::InvalidKey(k) => Self::InvalidKey(k),
            StorageError::InvalidValue(v) => Self::BadRequest(v),
            StorageError::Xattr(msg) => Self::Internal(anyhow::anyhow!("xattr failure: {msg}")),
            StorageError::Io(e) => Self::Internal(anyhow::anyhow!("io: {e}")),
        }
    }
}

impl From<IndexError> for ApiError {
    fn from(e: IndexError) -> Self {
        Self::Internal(anyhow::anyhow!("index: {e}"))
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, code, message) = match &self {
            ApiError::Unauthorized => (StatusCode::UNAUTHORIZED, "unauthorized", self.to_string()),
            ApiError::Forbidden => (StatusCode::FORBIDDEN, "forbidden", self.to_string()),
            ApiError::ObjectNotFound { .. } => {
                (StatusCode::NOT_FOUND, "object_not_found", self.to_string())
            }
            ApiError::BucketNotFound(_) => {
                (StatusCode::NOT_FOUND, "bucket_not_found", self.to_string())
            }
            ApiError::BucketNotEmpty => {
                (StatusCode::CONFLICT, "bucket_not_empty", self.to_string())
            }
            ApiError::ObjectExists => (
                StatusCode::PRECONDITION_FAILED,
                "object_exists",
                self.to_string(),
            ),
            ApiError::EtagMismatch => (
                StatusCode::PRECONDITION_FAILED,
                "etag_mismatch",
                self.to_string(),
            ),
            ApiError::InvalidKey(_) => (StatusCode::BAD_REQUEST, "invalid_key", self.to_string()),
            ApiError::BadRequest(_) => (StatusCode::BAD_REQUEST, "bad_request", self.to_string()),
            ApiError::RangeNotSatisfiable => (
                StatusCode::RANGE_NOT_SATISFIABLE,
                "range_not_satisfiable",
                self.to_string(),
            ),
            ApiError::Internal(_) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal_error",
                "an internal error occurred".to_string(),
            ),
        };

        if status == StatusCode::INTERNAL_SERVER_ERROR {
            tracing::error!(error = %self, "internal error");
        }

        let body = ErrorResponse {
            error: ErrorBody {
                code: code.to_string(),
                message,
                hint: None,
            },
        };
        (status, Json(body)).into_response()
    }
}
