//! Bearer-token auth.
//!
//! - Default bucket: presented token must equal `OBJECTS_ROOT_TOKEN` byte-for-byte.
//! - Other buckets: presented token must equal `HMAC-SHA256(OBJECTS_ROOT_TOKEN, bucket_name)`
//!   in lowercase hex, **or** equal `OBJECTS_ROOT_TOKEN` (root-token override).
//! - Bucket CRUD endpoints (`/v1/buckets*`) require the root token.
//!
//! Comparison is constant-time via `subtle::ConstantTimeEq`.

use axum::{
    extract::{Path, Request, State},
    http::{HeaderMap, header},
    middleware::Next,
    response::Response,
};
use hmac::{Hmac, Mac};
use secrecy::ExposeSecret;
use sha2::Sha256;
use subtle::ConstantTimeEq;

use crate::{AppState, error::ApiError};

const DEFAULT_BUCKET: &str = "default";

/// Extract the bearer token from `Authorization: Bearer <token>`.
pub(crate) fn extract_bearer(headers: &HeaderMap) -> Option<String> {
    headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| {
            v.strip_prefix("Bearer ")
                .or_else(|| v.strip_prefix("bearer "))
        })
        .map(str::to_owned)
}

/// Constant-time check: `presented == root_token`.
fn is_root(root_token: &str, presented: &str) -> bool {
    bool::from(root_token.as_bytes().ct_eq(presented.as_bytes()))
}

/// Compute `HMAC-SHA256(root_token, bucket)` and compare to `presented` (lowercase hex).
fn is_bucket(root_token: &str, bucket: &str, presented: &str) -> bool {
    let Ok(mut mac) = Hmac::<Sha256>::new_from_slice(root_token.as_bytes()) else {
        return false; // empty root_token — treat as auth failure
    };
    mac.update(bucket.as_bytes());
    let expected = hex::encode(mac.finalize().into_bytes());
    bool::from(expected.as_bytes().ct_eq(presented.as_bytes()))
}

/// Verify that `presented` is a valid token for `bucket`. The root token is
/// accepted for any bucket (root has full access). The default bucket validates
/// against the root token directly.
pub fn verify(root_token: &str, bucket: &str, presented: &str) -> bool {
    if is_root(root_token, presented) {
        return true;
    }
    if bucket == DEFAULT_BUCKET {
        return false;
    }
    is_bucket(root_token, bucket, presented)
}

/// Reject the request unless the presented bearer token equals the root token.
/// Used on bucket CRUD endpoints.
pub async fn require_root(
    State(state): State<AppState>,
    req: Request,
    next: Next,
) -> Result<Response, ApiError> {
    let presented = extract_bearer(req.headers()).ok_or(ApiError::Unauthorized)?;
    if !is_root(state.config.objects_root_token.expose_secret(), &presented) {
        return Err(ApiError::Unauthorized);
    }
    Ok(next.run(req).await)
}

/// Reject the request unless the presented bearer token is valid for the
/// `{bucket}` path parameter. Used on object write/list endpoints.
pub async fn require_bucket(
    State(state): State<AppState>,
    Path(bucket): Path<String>,
    req: Request,
    next: Next,
) -> Result<Response, ApiError> {
    let presented = extract_bearer(req.headers()).ok_or(ApiError::Unauthorized)?;
    if !verify(
        state.config.objects_root_token.expose_secret(),
        &bucket,
        &presented,
    ) {
        return Err(ApiError::Unauthorized);
    }
    Ok(next.run(req).await)
}

/// Used on routes that have BOTH `{bucket}` and `{*key}` path parameters: the
/// `Path` extractor needs to capture both because the matched path includes both.
pub async fn require_bucket_with_key(
    State(state): State<AppState>,
    Path((bucket, _key)): Path<(String, String)>,
    req: Request,
    next: Next,
) -> Result<Response, ApiError> {
    let presented = extract_bearer(req.headers()).ok_or(ApiError::Unauthorized)?;
    if !verify(
        state.config.objects_root_token.expose_secret(),
        &bucket,
        &presented,
    ) {
        return Err(ApiError::Unauthorized);
    }
    Ok(next.run(req).await)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn root_token_works_for_default() {
        assert!(verify("rootsecret", DEFAULT_BUCKET, "rootsecret"));
        assert!(!verify("rootsecret", DEFAULT_BUCKET, "notroot"));
    }

    #[test]
    fn root_token_works_for_any_bucket() {
        assert!(verify("rootsecret", "images", "rootsecret"));
    }

    #[test]
    fn derived_bucket_token_works() {
        let mut mac = Hmac::<Sha256>::new_from_slice(b"rootsecret").unwrap();
        mac.update(b"images");
        let derived = hex::encode(mac.finalize().into_bytes());

        assert!(verify("rootsecret", "images", &derived));
        // Cross-bucket: derived(images) does not authenticate against `docs`.
        assert!(!verify("rootsecret", "docs", &derived));
    }

    #[test]
    fn derived_token_rejected_for_default() {
        let mut mac = Hmac::<Sha256>::new_from_slice(b"rootsecret").unwrap();
        mac.update(b"default");
        let derived = hex::encode(mac.finalize().into_bytes());
        // Default bucket only accepts the root token, not a derived one.
        assert!(!verify("rootsecret", DEFAULT_BUCKET, &derived));
    }
}
