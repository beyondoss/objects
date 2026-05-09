//! S3-compatible wire-protocol surface, sibling of `routes/`.
//!
//! Mirrors `../queue/src/sqs/` in shape: separate module per concern (auth,
//! access, error mapping, the trait impl). `s3s::S3Service` already does the
//! routing/parsing/multipart-streaming work, so there's no equivalent of
//! queue's hand-rolled `actions/` dispatch table.

mod access;
mod auth;
mod error;
mod handler;

pub use self::handler::ObjectsS3;

use std::convert::Infallible;
use std::task::{Context, Poll};

use axum::{
    body::Body,
    http::{Request, Response, StatusCode},
    response::IntoResponse,
};
use futures::future::BoxFuture;
use s3s::service::{S3Service, S3ServiceBuilder};

use crate::AppState;

/// Build the fallback service. Mounted via `Router::fallback_service` so
/// explicit `/v1/*`, `/livez`, and `/readyz` routes always win.
pub fn service(state: AppState) -> FallbackS3 {
    use secrecy::ExposeSecret;
    let token = state.config.objects_root_token.expose_secret().clone();
    let auth = auth::HmacAuth::new(secrecy::Secret::new(token));
    let s3 = ObjectsS3 { state };
    let mut builder = S3ServiceBuilder::new(s3);
    builder.set_auth(auth);
    builder.set_access(access::BucketScopedAccess);
    FallbackS3 {
        inner: builder.build(),
    }
}

/// Adapter from `s3s::S3Service` (`Error = HttpError`) to a service
/// `axum::Router::fallback_service` accepts (`Error = Infallible`,
/// `Response: IntoResponse`). Truly-fatal `HttpError`s — which only occur
/// when the response itself can't be built — are logged and surfaced as a
/// generic 500. Regular S3 errors are already serialized as XML responses
/// inside `S3Service::call`, so they round-trip as `Ok`.
#[derive(Clone)]
pub struct FallbackS3 {
    inner: S3Service,
}

impl tower::Service<Request<Body>> for FallbackS3 {
    type Response = Response<Body>;
    type Error = Infallible;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request<Body>) -> Self::Future {
        let fut = tower::Service::call(&mut self.inner, req);
        Box::pin(async move {
            match fut.await {
                Ok(resp) => Ok(resp.map(Body::new)),
                Err(err) => {
                    tracing::error!(error = ?err, "s3 service produced a fatal HttpError");
                    Ok((StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response())
                }
            }
        })
    }
}
