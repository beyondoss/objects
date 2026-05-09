pub mod buckets;
pub mod healthz;
pub mod objects;
pub mod upload_tokens;

use std::time::Duration;

use axum::{
    Router,
    http::StatusCode,
    middleware::from_fn_with_state,
    routing::{get, post, put},
};
use tower_http::timeout::TimeoutLayer;
use utoipa::OpenApi;
use utoipa::openapi::security::{HttpAuthScheme, HttpBuilder, SecurityScheme};

// Applies to every route except object writes; uploads are unbounded by design.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

use crate::{
    AppState,
    middleware::auth::{require_bucket, require_bucket_with_key, require_root},
};

use upload_tokens::{CreateUploadTokenRequest, UploadTokenResponse};

pub struct BearerAuth;

impl utoipa::Modify for BearerAuth {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        let components = openapi.components.get_or_insert_with(Default::default);
        components.add_security_scheme(
            "BearerAuth",
            SecurityScheme::Http(
                HttpBuilder::new()
                    .scheme(HttpAuthScheme::Bearer)
                    .bearer_format("OBJECTS_ROOT_TOKEN or HMAC-SHA256(OBJECTS_ROOT_TOKEN, bucket)")
                    .build(),
            ),
        );
    }
}

#[derive(OpenApi)]
#[openapi(
    info(
        title = "Beyond Objects",
        version = "1",
        description = "Object storage with HMAC-derived bucket tokens, conditional writes, and prefix-paginated listing."
    ),
    modifiers(&BearerAuth),
    paths(
        healthz::livez,
        healthz::readyz,
        objects::put_object,
        objects::get_object,
        objects::head_object,
        objects::delete_object,
        objects::patch_object,
        objects::copy_object,
        objects::list_objects,
        buckets::create_bucket,
        buckets::list_buckets,
        buckets::get_bucket,
        buckets::update_bucket,
        buckets::delete_bucket,
        upload_tokens::create_upload_token,
    ),
    components(schemas(
        crate::error::ErrorBody,
        crate::error::ErrorResponse,
        healthz::HealthzResponse,
        objects::PutObjectResponse,
        objects::PatchObjectRequest,
        objects::CopyObjectRequest,
        objects::CopyObjectResponse,
        objects::ListObjectsResponse,
        objects::ObjectItem,
        buckets::CreateBucketRequest,
        buckets::UpdateBucketRequest,
        buckets::BucketResponse,
        buckets::ListBucketsResponse,
        CreateUploadTokenRequest,
        UploadTokenResponse,
    )),
    tags(
        (name = "system", description = "Health and service metadata."),
        (name = "objects", description = "Object lifecycle: upload, download, list, move, copy, delete."),
        (name = "buckets", description = "Bucket management."),
    )
)]
pub struct ApiDoc;

pub fn router(state: AppState) -> Router<AppState> {
    let buckets_router = Router::new()
        .route(
            "/v1/buckets",
            post(buckets::create_bucket).get(buckets::list_buckets),
        )
        .route(
            "/v1/buckets/{name}",
            get(buckets::get_bucket)
                .patch(buckets::update_bucket)
                .delete(buckets::delete_bucket),
        )
        .route_layer(from_fn_with_state(state.clone(), require_root))
        .layer(TimeoutLayer::with_status_code(
            StatusCode::REQUEST_TIMEOUT,
            REQUEST_TIMEOUT,
        ));

    // Upload token issuance: requires a valid bucket token, returns a short-lived
    // key-scoped token the browser can use for a single PUT.
    let upload_tokens_router = Router::new()
        .route(
            "/v1/{bucket}/upload-tokens",
            post(upload_tokens::create_upload_token),
        )
        .route_layer(from_fn_with_state(state.clone(), require_bucket))
        .layer(TimeoutLayer::with_status_code(
            StatusCode::REQUEST_TIMEOUT,
            REQUEST_TIMEOUT,
        ));

    // Mutating verbs require a valid bucket token at the middleware layer.
    // No TimeoutLayer: upload size is unbounded, so a wall-clock deadline would
    // kill legitimate large transfers. Dead connections are handled by TCP keepalives.
    let object_writes = Router::new()
        .route(
            "/v1/{bucket}/{*key}",
            put(objects::put_object)
                .delete(objects::delete_object)
                .patch(objects::patch_object)
                .post(objects::copy_object),
        )
        .layer(axum::extract::DefaultBodyLimit::disable())
        .route_layer(from_fn_with_state(state.clone(), require_bucket_with_key));

    // Reads do auth inside the handler so public objects short-circuit before
    // the bearer check (no token required for `access=public`).
    let object_reads = Router::new()
        .route(
            "/v1/{bucket}/{*key}",
            get(objects::get_object).head(objects::head_object),
        )
        .layer(TimeoutLayer::with_status_code(
            StatusCode::REQUEST_TIMEOUT,
            REQUEST_TIMEOUT,
        ));

    let bucket_listing = Router::new()
        .route("/v1/{bucket}", get(objects::list_objects))
        .route_layer(from_fn_with_state(state.clone(), require_bucket))
        .layer(TimeoutLayer::with_status_code(
            StatusCode::REQUEST_TIMEOUT,
            REQUEST_TIMEOUT,
        ));

    Router::new()
        .merge(buckets_router)
        .merge(upload_tokens_router)
        .merge(object_writes)
        .merge(object_reads)
        .merge(bucket_listing)
}
