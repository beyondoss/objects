use std::str::FromStr;

use axum::{
    Json,
    body::Body,
    extract::{Path, Query, Request, State},
    http::{HeaderMap, HeaderName, HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
};
use chrono::{DateTime, Utc};
use futures::TryStreamExt;
use futures::stream::StreamExt;
use secrecy::ExposeSecret;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncSeekExt, SeekFrom};
use tokio_util::io::{ReaderStream, StreamReader};
use utoipa::ToSchema;

use beyond_objects_storage::{AccessLevel, ObjectMeta, StorageError, WriteCondition};

use crate::{AppState, error::ApiError};

const USER_META_PREFIX: &str = "x-amz-meta-";
const ACCESS_HEADER: &str = "x-beyond-access";

/// Result of a successful upload, move, or access change.
#[derive(Serialize, ToSchema)]
pub struct PutObjectResponse {
    /// Final key of the object (post-move for PATCH, otherwise the request key).
    #[schema(example = "avatars/u123.png")]
    pub key: String,
    /// Strong entity tag (quoted hex BLAKE3 of the object bytes).
    #[schema(example = "\"d4735e3a265e16eee03f59718b9b5d03019c07d8b6c51f90da3a666eec13ab35\"")]
    pub etag: String,
    /// Object size in bytes.
    #[schema(example = 4096)]
    pub size: u64,
}

/// One entry in a list response.
#[derive(Serialize, ToSchema)]
pub struct ObjectItem {
    /// Object key (path within the bucket).
    #[schema(example = "avatars/u123.png")]
    pub key: String,
    /// Object size in bytes.
    #[schema(example = 4096)]
    pub size: u64,
    /// Strong entity tag (quoted hex BLAKE3 of the object bytes).
    #[schema(example = "\"d4735e3a265e16eee03f59718b9b5d03019c07d8b6c51f90da3a666eec13ab35\"")]
    pub etag: String,
    /// Stored `Content-Type`, when one was provided at upload time.
    #[schema(nullable, example = "image/png")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content_type: Option<String>,
    /// Effective access level — the object's own xattr, falling back to the
    /// bucket default when absent.
    #[schema(value_type = String, example = "private")]
    pub access: AccessLevel,
    /// Last-modified timestamp from the underlying file.
    pub last_modified: DateTime<Utc>,
    /// Absolute URL where the object can be fetched (uses `OBJECTS_URL` when set,
    /// otherwise the bound address).
    #[schema(example = "https://objects.example.com/v1/photos/avatars/u123.png")]
    pub url: String,
}

/// Page of objects matching a list query.
#[derive(Serialize, ToSchema)]
pub struct ListObjectsResponse {
    /// Objects on this page, in ascending key order.
    pub objects: Vec<ObjectItem>,
    /// Opaque cursor to pass as `?cursor=` to fetch the next page. `null` when
    /// the page is final.
    #[schema(nullable, example = "avatars/u123.png")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

#[derive(Deserialize)]
pub struct ListQuery {
    #[serde(default)]
    pub prefix: Option<String>,
    #[serde(default)]
    pub cursor: Option<String>,
    #[serde(default)]
    pub limit: Option<usize>,
}

/// Body for `PATCH /v1/{bucket}/{key}`. At least one field must be set.
#[derive(Deserialize, ToSchema)]
pub struct PatchObjectRequest {
    /// New key path. When set, the object is moved within the bucket.
    #[schema(nullable, example = "archive/avatars/u123.png")]
    #[serde(default)]
    pub key: Option<String>,
    /// New access level. When set, the object's visibility xattr is updated.
    #[schema(value_type = Option<String>, nullable, example = "public")]
    #[serde(default)]
    pub access: Option<AccessLevel>,
}

/// Body for `POST /v1/{bucket}/{key}` (server-side copy).
#[derive(Deserialize, ToSchema)]
pub struct CopyObjectRequest {
    /// Source key within the same bucket. Cross-bucket copy is not yet supported.
    #[schema(example = "originals/u123.png")]
    pub source: String,
}

/// Result of a successful server-side copy.
#[derive(Serialize, ToSchema)]
pub struct CopyObjectResponse {
    /// Destination key.
    #[schema(example = "thumbnails/u123.png")]
    pub key: String,
    /// Etag of the destination object (identical to the source's etag).
    #[schema(example = "\"d4735e3a265e16eee03f59718b9b5d03019c07d8b6c51f90da3a666eec13ab35\"")]
    pub etag: String,
}

const DEFAULT_LIST_LIMIT: usize = 1000;
const MAX_LIST_LIMIT: usize = 1000;

/// Stream-upload an object. Honors `If-None-Match: *` and `If-Match: "<etag>"`
/// for conditional writes.
#[utoipa::path(
    put,
    path = "/v1/{bucket}/{key}",
    operation_id = "put_object",
    tag = "objects",
    params(
        ("bucket" = String, Path, description = "Bucket name."),
        ("key" = String, Path, description = "Object key (may contain slashes)."),
        ("Content-Type" = Option<String>, Header, description = "MIME type stored alongside the object."),
        ("If-None-Match" = Option<String>, Header, description = "Set to `*` to write only when the object does not exist."),
        ("If-Match" = Option<String>, Header, description = "Quoted etag — write only when the current etag matches."),
        ("X-Beyond-Access" = Option<String>, Header, description = "`public` or `private` — overrides the bucket default."),
    ),
    request_body(content_type = "application/octet-stream", description = "Raw object bytes (streamed)."),
    security(("BearerAuth" = [])),
    responses(
        (status = 201, description = "Object written.", body = PutObjectResponse),
        (status = 400, description = "Conflicting or malformed conditional headers.", body = crate::error::ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token for this bucket.", body = crate::error::ErrorResponse),
        (status = 412, description = "Conditional write failed (`If-None-Match` matched, or `If-Match` etag did not).", body = crate::error::ErrorResponse),
    )
)]
pub async fn put_object(
    State(state): State<AppState>,
    Path((bucket, key)): Path<(String, String)>,
    headers: HeaderMap,
    request: Request,
) -> Result<impl IntoResponse, ApiError> {
    let condition = parse_condition(&headers)?;
    let content_type = header_str(&headers, header::CONTENT_TYPE).map(str::to_owned);
    let access = parse_access_header(&headers)?;
    let user_metadata = collect_user_metadata(&headers);

    let body_stream = request
        .into_body()
        .into_data_stream()
        .map_err(std::io::Error::other);
    let mut reader = StreamReader::new(body_stream);

    let meta = ObjectMeta {
        content_type,
        access,
        user_metadata,
    };

    let (etag, size) = state
        .storage
        .write_object(&bucket, &key, &mut reader, meta, condition)
        .await?;

    state.index_insert(&bucket, &key).await?;
    state.publish(&state.config.base_url(), &bucket, &key);

    let mut resp_headers = HeaderMap::new();
    resp_headers.insert(
        header::ETAG,
        etag.parse()
            .map_err(|_| ApiError::Internal(anyhow::anyhow!("etag is not a valid header value")))?,
    );

    Ok((
        StatusCode::CREATED,
        resp_headers,
        Json(PutObjectResponse { key, etag, size }),
    ))
}

/// Download an object. Public objects are served without auth (and with
/// `Access-Control-Allow-Origin: *`); private objects require a valid bearer
/// token. Single-range requests (`Range: bytes=a-b`) return 206.
#[utoipa::path(
    get,
    path = "/v1/{bucket}/{key}",
    operation_id = "get_object",
    tag = "objects",
    params(
        ("bucket" = String, Path, description = "Bucket name."),
        ("key" = String, Path, description = "Object key."),
        ("Range" = Option<String>, Header, description = "Single byte range, e.g. `bytes=0-1023`."),
    ),
    security(("BearerAuth" = [])),
    responses(
        (status = 200, description = "Full object bytes.", content_type = "application/octet-stream"),
        (status = 206, description = "Single-range partial content.", content_type = "application/octet-stream"),
        (status = 401, description = "Object is private and no valid bearer token was presented.", body = crate::error::ErrorResponse),
        (status = 404, description = "Object does not exist.", body = crate::error::ErrorResponse),
        (status = 416, description = "Range header is malformed, multi-range, or outside the object size.", body = crate::error::ErrorResponse),
    )
)]
pub async fn get_object(
    State(state): State<AppState>,
    Path((bucket, key)): Path<(String, String)>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let (info, mut file) = state.storage.open_object(&bucket, &key).await?;
    enforce_object_auth(&state, &bucket, &info.access, &headers)?;

    let mut resp_headers = build_object_headers(&info);

    let range = parse_range(&headers, info.size)?;
    let (status, start, end_inclusive) = match range {
        Some((s, e)) => {
            resp_headers.insert(
                header::CONTENT_RANGE,
                HeaderValue::from_str(&format!("bytes {s}-{e}/{}", info.size))
                    .map_err(|_| ApiError::Internal(anyhow::anyhow!("content-range encode")))?,
            );
            (StatusCode::PARTIAL_CONTENT, s, e)
        }
        None => (StatusCode::OK, 0, info.size.saturating_sub(1)),
    };

    let length = end_inclusive.saturating_sub(start).saturating_add(1);
    resp_headers.insert(
        header::CONTENT_LENGTH,
        HeaderValue::from_str(&length.to_string())
            .map_err(|_| ApiError::Internal(anyhow::anyhow!("content-length encode")))?,
    );

    if start > 0 {
        file.seek(SeekFrom::Start(start))
            .await
            .map_err(|e| ApiError::Internal(anyhow::anyhow!("seek: {e}")))?;
    }
    let limited = file.take(length);
    let body = Body::from_stream(ReaderStream::new(limited));

    Ok((status, resp_headers, body).into_response())
}

/// Object metadata, identical headers to GET but no body.
#[utoipa::path(
    head,
    path = "/v1/{bucket}/{key}",
    operation_id = "head_object",
    tag = "objects",
    params(
        ("bucket" = String, Path, description = "Bucket name."),
        ("key" = String, Path, description = "Object key."),
    ),
    security(("BearerAuth" = [])),
    responses(
        (status = 200, description = "Object metadata in headers (no body)."),
        (status = 401, description = "Object is private and no valid bearer token was presented.", body = crate::error::ErrorResponse),
        (status = 404, description = "Object does not exist.", body = crate::error::ErrorResponse),
    )
)]
pub async fn head_object(
    State(state): State<AppState>,
    Path((bucket, key)): Path<(String, String)>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let info = state.storage.head_object(&bucket, &key).await?;
    enforce_object_auth(&state, &bucket, &info.access, &headers)?;

    let mut resp_headers = build_object_headers(&info);
    resp_headers.insert(
        header::CONTENT_LENGTH,
        HeaderValue::from_str(&info.size.to_string())
            .map_err(|_| ApiError::Internal(anyhow::anyhow!("content-length encode")))?,
    );
    Ok((StatusCode::OK, resp_headers).into_response())
}

/// Delete an object. Idempotent.
#[utoipa::path(
    delete,
    path = "/v1/{bucket}/{key}",
    operation_id = "delete_object",
    tag = "objects",
    params(
        ("bucket" = String, Path, description = "Bucket name."),
        ("key" = String, Path, description = "Object key."),
    ),
    security(("BearerAuth" = [])),
    responses(
        (status = 204, description = "Object deleted, or did not exist (idempotent)."),
        (status = 401, description = "Missing or invalid bearer token for this bucket.", body = crate::error::ErrorResponse),
    )
)]
pub async fn delete_object(
    State(state): State<AppState>,
    Path((bucket, key)): Path<(String, String)>,
) -> Result<StatusCode, ApiError> {
    // Failure ordering: if storage delete succeeds but index delete fails, the object
    // is gone from disk but still appears in listings. Reconcile on the next startup
    // drops index entries with no backing file, recovering this state.
    match state.storage.delete_object(&bucket, &key).await {
        Ok(()) => state.index_delete(&bucket, &key).await?,
        Err(StorageError::NotFound { .. }) => {
            // Already gone — idempotent. Best-effort index cleanup; reconcile handles gaps.
            let _ = state.index_delete(&bucket, &key).await;
        }
        Err(e) => return Err(e.into()),
    }
    Ok(StatusCode::NO_CONTENT)
}

/// Move (rename) an object or update its access level. Body is `{ "key": "..." }`,
/// `{ "access": "public"|"private" }`, or both.
#[utoipa::path(
    patch,
    path = "/v1/{bucket}/{key}",
    operation_id = "patch_object",
    tag = "objects",
    params(
        ("bucket" = String, Path, description = "Bucket name."),
        ("key" = String, Path, description = "Object key."),
    ),
    request_body = PatchObjectRequest,
    security(("BearerAuth" = [])),
    responses(
        (status = 200, description = "Updated metadata after the move and/or access change.", body = PutObjectResponse),
        (status = 400, description = "Body did not contain `key` or `access`.", body = crate::error::ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token for this bucket.", body = crate::error::ErrorResponse),
        (status = 404, description = "Source object does not exist.", body = crate::error::ErrorResponse),
    )
)]
pub async fn patch_object(
    State(state): State<AppState>,
    Path((bucket, key)): Path<(String, String)>,
    Json(req): Json<PatchObjectRequest>,
) -> Result<Json<PutObjectResponse>, ApiError> {
    if req.key.is_none() && req.access.is_none() {
        return Err(ApiError::bad_request("body must contain `key` or `access`"));
    }

    let mut current_key = key.clone();

    if let Some(new_key) = &req.key {
        // Insert the new index entry before moving so the object is never
        // absent from the index. Failure order:
        //   insert fails  → nothing moved, old key still valid
        //   move fails    → new key in index but no file there yet; old file
        //                   still at old path; reconcile cleans the orphan
        //   delete fails  → both keys in index, file at new path; reconcile
        //                   drops the stale old entry (no backing file)
        state.index_insert(&bucket, new_key).await?;
        state
            .storage
            .move_object(&bucket, &key, &bucket, new_key)
            .await?;
        state.index_delete(&bucket, &key).await?;
        current_key = new_key.clone();
    }

    if let Some(access) = req.access {
        state
            .storage
            .update_object_access(&bucket, &current_key, access)
            .await?;
    }

    let info = state.storage.head_object(&bucket, &current_key).await?;
    Ok(Json(PutObjectResponse {
        key: current_key,
        etag: info.etag,
        size: info.size,
    }))
}

/// Server-side copy from a source key in the same bucket.
#[utoipa::path(
    post,
    path = "/v1/{bucket}/{key}",
    operation_id = "copy_object",
    tag = "objects",
    params(
        ("bucket" = String, Path, description = "Bucket name."),
        ("key" = String, Path, description = "Destination key."),
    ),
    request_body = CopyObjectRequest,
    security(("BearerAuth" = [])),
    responses(
        (status = 201, description = "Destination object created.", body = CopyObjectResponse),
        (status = 401, description = "Missing or invalid bearer token for this bucket.", body = crate::error::ErrorResponse),
        (status = 404, description = "Source object does not exist.", body = crate::error::ErrorResponse),
    )
)]
pub async fn copy_object(
    State(state): State<AppState>,
    Path((bucket, key)): Path<(String, String)>,
    Json(req): Json<CopyObjectRequest>,
) -> Result<(StatusCode, Json<CopyObjectResponse>), ApiError> {
    let etag = state
        .storage
        .copy_object(&bucket, &req.source, &bucket, &key)
        .await?;
    // If index_insert fails here the object exists on disk but won't appear in listings
    // until reconcile repairs the gap on next startup. The file itself is accessible by
    // direct key — no cleanup needed.
    state.index_insert(&bucket, &key).await?;
    Ok((StatusCode::CREATED, Json(CopyObjectResponse { key, etag })))
}

/// List object keys in a bucket, with prefix scan and cursor pagination.
#[utoipa::path(
    get,
    path = "/v1/{bucket}",
    operation_id = "list_objects",
    tag = "objects",
    params(
        ("bucket" = String, Path, description = "Bucket name."),
        ("prefix" = Option<String>, Query, description = "Only return keys with this prefix."),
        ("cursor" = Option<String>, Query, description = "Last key of the previous page (exclusive)."),
        ("limit" = Option<usize>, Query, description = "Max keys per page (default 1000, max 1000)."),
    ),
    security(("BearerAuth" = [])),
    responses(
        (status = 200, description = "Page of objects, in ascending key order.", body = ListObjectsResponse),
        (status = 401, description = "Missing or invalid bearer token for this bucket.", body = crate::error::ErrorResponse),
    )
)]
pub async fn list_objects(
    State(state): State<AppState>,
    Path(bucket): Path<String>,
    Query(query): Query<ListQuery>,
) -> Result<Json<ListObjectsResponse>, ApiError> {
    let prefix = query.prefix.unwrap_or_default();
    let limit = query
        .limit
        .unwrap_or(DEFAULT_LIST_LIMIT)
        .min(MAX_LIST_LIMIT);

    let index = state.index.clone();
    let bucket_for_scan = bucket.clone();
    let prefix_for_scan = prefix.clone();
    let cursor_for_scan = query.cursor.clone();
    let keys = tokio::task::spawn_blocking(move || {
        index.scan(
            &bucket_for_scan,
            &prefix_for_scan,
            cursor_for_scan.as_deref(),
            limit,
        )
    })
    .await
    .map_err(|e| ApiError::Internal(anyhow::anyhow!("index scan join: {e}")))??;

    let next_cursor = if keys.len() == limit {
        keys.last().cloned()
    } else {
        None
    };

    let base = state.config.base_url();
    let objects: Vec<ObjectItem> = futures::stream::iter(keys.into_iter().map(|k| {
        let st = state.clone();
        let b = bucket.clone();
        let base = base.clone();
        async move {
            match st.storage.head_object(&b, &k).await {
                Ok(info) => Ok(Some(ObjectItem {
                    url: format!("{base}/v1/{b}/{k}"),
                    key: k,
                    size: info.size,
                    etag: info.etag,
                    content_type: info.content_type,
                    access: info.access,
                    last_modified: DateTime::<Utc>::from(info.last_modified),
                })),
                Err(StorageError::NotFound { .. }) => {
                    tracing::debug!(bucket = %b, key = %k, "skipping index entry without backing file");
                    Ok::<Option<ObjectItem>, ApiError>(None)
                }
                Err(e) => Err(ApiError::from(e)),
            }
        }
    }))
    .buffered(64)
    .try_collect::<Vec<_>>()
    .await?
    .into_iter()
    .flatten()
    .collect();

    Ok(Json(ListObjectsResponse {
        objects,
        next_cursor,
    }))
}

// ---------- helpers ----------

fn header_str(headers: &HeaderMap, name: impl AsRef<str>) -> Option<&str> {
    headers.get(name.as_ref()).and_then(|v| v.to_str().ok())
}

fn parse_condition(headers: &HeaderMap) -> Result<Option<WriteCondition>, ApiError> {
    let if_none_match = header_str(headers, header::IF_NONE_MATCH);
    let if_match = header_str(headers, header::IF_MATCH);

    match (if_none_match, if_match) {
        (Some(_), Some(_)) => Err(ApiError::bad_request(
            "If-None-Match and If-Match are mutually exclusive",
        )),
        (Some(v), None) => {
            if v.trim() == "*" {
                Ok(Some(WriteCondition::IfNoneMatch))
            } else {
                Err(ApiError::bad_request(
                    "If-None-Match: only `*` is supported",
                ))
            }
        }
        (None, Some(v)) => Ok(Some(WriteCondition::IfMatch(v.to_owned()))),
        (None, None) => Ok(None),
    }
}

fn parse_access_header(headers: &HeaderMap) -> Result<Option<AccessLevel>, ApiError> {
    match header_str(headers, ACCESS_HEADER) {
        Some(v) => AccessLevel::from_str(v.trim())
            .map(Some)
            .map_err(|_| ApiError::bad_request(format!("invalid {ACCESS_HEADER}: {v}"))),
        None => Ok(None),
    }
}

fn collect_user_metadata(headers: &HeaderMap) -> std::collections::HashMap<String, String> {
    headers
        .iter()
        .filter_map(|(k, v)| {
            let name = k.as_str();
            let stripped = name.strip_prefix(USER_META_PREFIX)?;
            let value = v.to_str().ok()?;
            Some((stripped.to_owned(), value.to_owned()))
        })
        .collect()
}

fn build_object_headers(info: &beyond_objects_storage::ObjectInfo) -> HeaderMap {
    let mut h = HeaderMap::new();
    if let Ok(v) = info.etag.parse() {
        h.insert(header::ETAG, v);
    }
    if let Some(ct) = info.content_type.as_deref()
        && let Ok(v) = ct.parse()
    {
        h.insert(header::CONTENT_TYPE, v);
    }
    let dt: chrono::DateTime<Utc> = info.last_modified.into();
    if let Ok(v) = dt.to_rfc2822().parse() {
        h.insert(header::LAST_MODIFIED, v);
    }
    if info.access == AccessLevel::Public
        && let Ok(v) = "*".parse()
    {
        h.insert(header::ACCESS_CONTROL_ALLOW_ORIGIN, v);
    }
    for (k, val) in &info.user_metadata {
        let header_name = format!("{USER_META_PREFIX}{k}");
        if let (Ok(name), Ok(value)) = (HeaderName::try_from(header_name), val.parse()) {
            h.insert(name, value);
        }
    }
    h
}

/// Validate auth for a GET/HEAD request: public objects bypass auth; private
/// objects require a valid bucket token (or the root token).
fn enforce_object_auth(
    state: &AppState,
    bucket: &str,
    access: &AccessLevel,
    headers: &HeaderMap,
) -> Result<(), ApiError> {
    if *access == AccessLevel::Public {
        return Ok(());
    }
    let presented =
        crate::middleware::auth::extract_bearer(headers).ok_or(ApiError::Unauthorized)?;
    if !crate::middleware::auth::verify(
        state.config.objects_root_token.expose_secret(),
        bucket,
        &presented,
    ) {
        return Err(ApiError::Unauthorized);
    }
    Ok(())
}

/// Parse a single-range `Range` header. Returns inclusive `(start, end)` bytes,
/// or `None` if no range header is present. Multi-range requests return 416.
fn parse_range(headers: &HeaderMap, size: u64) -> Result<Option<(u64, u64)>, ApiError> {
    let raw = match header_str(headers, header::RANGE) {
        Some(v) => v,
        None => return Ok(None),
    };
    let spec = raw
        .strip_prefix("bytes=")
        .ok_or(ApiError::RangeNotSatisfiable)?;
    if spec.contains(',') {
        return Err(ApiError::RangeNotSatisfiable);
    }
    let (start_s, end_s) = spec.split_once('-').ok_or(ApiError::RangeNotSatisfiable)?;

    if size == 0 {
        return Err(ApiError::RangeNotSatisfiable);
    }

    let (start, end) = match (start_s.trim(), end_s.trim()) {
        ("", "") => return Err(ApiError::RangeNotSatisfiable),
        ("", suffix) => {
            let n: u64 = suffix.parse().map_err(|_| ApiError::RangeNotSatisfiable)?;
            if n == 0 {
                return Err(ApiError::RangeNotSatisfiable);
            }
            let start = size.saturating_sub(n);
            (start, size - 1)
        }
        (s, "") => {
            let start: u64 = s.parse().map_err(|_| ApiError::RangeNotSatisfiable)?;
            (start, size - 1)
        }
        (s, e) => {
            let start: u64 = s.parse().map_err(|_| ApiError::RangeNotSatisfiable)?;
            let end: u64 = e.parse().map_err(|_| ApiError::RangeNotSatisfiable)?;
            (start, end.min(size - 1))
        }
    };
    if start > end || start >= size {
        return Err(ApiError::RangeNotSatisfiable);
    }
    Ok(Some((start, end)))
}
