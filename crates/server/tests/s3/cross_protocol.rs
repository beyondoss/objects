//! Sanity checks that the two protocol surfaces share storage. PUT via REST,
//! GET via S3 (and the reverse) should round-trip the same bytes, etag, and
//! visibility — there is one filesystem and one fjall index, and these tests
//! prove it.

use crate::common::{bucket_secret, byte_stream, create_bucket_via_rest, s3_client, unique_bucket};

#[tokio::test]
async fn rest_put_visible_to_s3_get() {
    let bucket = unique_bucket("xptors3");
    create_bucket_via_rest(&bucket, "private").await;
    let token = bucket_secret(&bucket);

    // PUT via REST
    let url = format!("{}/v1/{bucket}/cross.bin", crate::common::server().url);
    let body = b"shared bytes";
    let res = reqwest::Client::new()
        .put(&url)
        .bearer_auth(&token)
        .body(body.to_vec())
        .send()
        .await
        .expect("rest put");
    assert_eq!(res.status(), reqwest::StatusCode::CREATED);
    let put_body: serde_json::Value = res.json().await.unwrap();
    let rest_etag = put_body["etag"]
        .as_str()
        .unwrap()
        .trim_matches('"')
        .to_owned();

    // GET via S3
    let s3 = s3_client(&bucket);
    let got = s3
        .get_object()
        .bucket(&bucket)
        .key("cross.bin")
        .send()
        .await
        .expect("s3 get");
    let s3_etag = got.e_tag().unwrap().trim_matches('"').to_owned();
    assert_eq!(rest_etag, s3_etag);
    let bytes = got.body.collect().await.unwrap().to_vec();
    assert_eq!(bytes, body);
}

#[tokio::test]
async fn s3_put_visible_to_rest_get() {
    let bucket = unique_bucket("xpts3tor");
    create_bucket_via_rest(&bucket, "private").await;
    let s3 = s3_client(&bucket);

    let body = b"reverse path";
    s3.put_object()
        .bucket(&bucket)
        .key("rev.bin")
        .body(byte_stream(body))
        .send()
        .await
        .expect("s3 put");

    let token = bucket_secret(&bucket);
    let url = format!("{}/v1/{bucket}/rev.bin", crate::common::server().url);
    let res = reqwest::Client::new()
        .get(&url)
        .bearer_auth(&token)
        .send()
        .await
        .expect("rest get");
    assert_eq!(res.status(), reqwest::StatusCode::OK);
    let bytes = res.bytes().await.unwrap();
    assert_eq!(&bytes[..], &body[..]);
}

#[tokio::test]
async fn s3_put_appears_in_rest_list() {
    let bucket = unique_bucket("xptlist");
    create_bucket_via_rest(&bucket, "private").await;
    let s3 = s3_client(&bucket);

    s3.put_object()
        .bucket(&bucket)
        .key("only.txt")
        .body(byte_stream(b"x"))
        .send()
        .await
        .expect("s3 put");

    let token = bucket_secret(&bucket);
    let url = format!("{}/v1/{bucket}", crate::common::server().url);
    let res = reqwest::Client::new()
        .get(&url)
        .bearer_auth(&token)
        .send()
        .await
        .expect("rest list");
    let v: serde_json::Value = res.json().await.unwrap();
    let keys: Vec<&str> = v["objects"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|o| o["key"].as_str())
        .collect();
    assert!(keys.contains(&"only.txt"));
}
