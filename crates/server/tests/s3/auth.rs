//! Auth/access tests: SigV4 verification (wrong secret), bucket scoping
//! (right secret but for a different bucket), root-key override, anonymous
//! access on public objects.

use aws_config::Region;
use aws_sdk_s3::Client;
use aws_sdk_s3::config::{Builder as S3ConfigBuilder, Credentials, SharedCredentialsProvider};
use aws_smithy_async::rt::sleep::default_async_sleep;

use crate::common::{
    bucket_secret, byte_stream, create_bucket_via_rest, s3_client, server, unique_bucket,
};

fn s3_client_with(creds: Credentials) -> Client {
    let mut builder = S3ConfigBuilder::new()
        .credentials_provider(SharedCredentialsProvider::new(creds))
        .region(Region::new("us-east-1"))
        .endpoint_url(server().url.clone())
        .force_path_style(true)
        .behavior_version(aws_config::BehaviorVersion::latest());
    if let Some(sleep) = default_async_sleep() {
        builder = builder.sleep_impl(sleep);
    }
    Client::from_conf(builder.build())
}

#[tokio::test]
async fn wrong_secret_fails_signature() {
    let bucket = unique_bucket("s3auth1");
    create_bucket_via_rest(&bucket, "private").await;

    let s3 = s3_client_with(Credentials::new(
        &bucket,
        "not-the-real-secret",
        None,
        None,
        "wrong",
    ));

    let err = s3
        .put_object()
        .bucket(&bucket)
        .key("x")
        .body(byte_stream(b"hi"))
        .send()
        .await
        .expect_err("must fail with bad sig");
    let err_str = format!("{err:?}");
    assert!(
        err_str.contains("SignatureDoesNotMatch") || err_str.contains("403"),
        "expected SignatureDoesNotMatch, got: {err_str}"
    );
}

#[tokio::test]
async fn bucket_token_cannot_cross_buckets() {
    let alpha = unique_bucket("s3auth2a");
    let beta = unique_bucket("s3auth2b");
    create_bucket_via_rest(&alpha, "private").await;
    create_bucket_via_rest(&beta, "private").await;

    let s3 = s3_client_with(Credentials::new(
        &alpha,
        bucket_secret(&alpha),
        None,
        None,
        "alpha",
    ));

    let err = s3
        .put_object()
        .bucket(&beta)
        .key("x")
        .body(byte_stream(b"hi"))
        .send()
        .await
        .expect_err("must be denied cross-bucket");
    let err_str = format!("{err:?}");
    assert!(
        err_str.contains("AccessDenied") || err_str.contains("403"),
        "expected AccessDenied, got: {err_str}"
    );
}

#[tokio::test]
async fn root_key_can_access_any_bucket() {
    let bucket = unique_bucket("s3auth3");
    create_bucket_via_rest(&bucket, "private").await;
    let s3 = s3_client("root");

    s3.put_object()
        .bucket(&bucket)
        .key("from-root.bin")
        .body(byte_stream(b"ok"))
        .send()
        .await
        .expect("root put");
}

#[tokio::test]
async fn anonymous_get_allowed_only_for_public_objects() {
    // The aws-sdk-s3 client always signs requests, so a "real" anonymous
    // request is most easily issued with bare reqwest. We're testing OUR
    // server's behavior, not the SDK's.
    let bucket = unique_bucket("s3auth4");
    create_bucket_via_rest(&bucket, "private").await;
    let s3 = s3_client(&bucket);

    s3.put_object()
        .bucket(&bucket)
        .key("private.bin")
        .body(byte_stream(b"secret"))
        .send()
        .await
        .expect("put private");

    s3.put_object()
        .bucket(&bucket)
        .key("public.bin")
        .acl(aws_sdk_s3::types::ObjectCannedAcl::PublicRead)
        .body(byte_stream(b"shared"))
        .send()
        .await
        .expect("put public");

    let base = &server().url;

    // Public via S3 path-style URL with no Authorization header → 200.
    let res = reqwest::Client::new()
        .get(format!("{base}/{bucket}/public.bin"))
        .send()
        .await
        .expect("anon get public");
    assert_eq!(res.status(), reqwest::StatusCode::OK);
    let bytes = res.bytes().await.unwrap();
    assert_eq!(&bytes[..], b"shared");

    // Private via the same path → 403.
    let res = reqwest::Client::new()
        .get(format!("{base}/{bucket}/private.bin"))
        .send()
        .await
        .expect("anon get private");
    assert_eq!(res.status(), reqwest::StatusCode::FORBIDDEN);
}
