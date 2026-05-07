use crate::common::{bucket_token, client, create_bucket, root_token, unique_bucket, url};

#[tokio::test]
async fn put_get_head_delete_roundtrip() {
    let bucket = unique_bucket("rt");
    create_bucket(&bucket, "private").await;
    let token = bucket_token(&bucket);

    // PUT
    let body = b"hello world";
    let res = client()
        .put(url(&format!("/v1/{bucket}/greeting.txt")))
        .bearer_auth(&token)
        .header("content-type", "text/plain")
        .body(body.to_vec())
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), reqwest::StatusCode::CREATED);
    let etag_header = res
        .headers()
        .get("etag")
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);
    assert!(etag_header.as_deref().unwrap_or("").starts_with('"'));
    let put_body: serde_json::Value = res.json().await.unwrap();
    assert_eq!(put_body["size"], body.len());
    let etag = put_body["etag"].as_str().unwrap().to_owned();

    // HEAD
    let res = client()
        .head(url(&format!("/v1/{bucket}/greeting.txt")))
        .bearer_auth(&token)
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), reqwest::StatusCode::OK);
    assert_eq!(
        res.headers().get("etag").and_then(|v| v.to_str().ok()),
        Some(etag.as_str())
    );
    assert_eq!(
        res.headers()
            .get("content-length")
            .and_then(|v| v.to_str().ok()),
        Some(body.len().to_string()).as_deref()
    );

    // GET
    let res = client()
        .get(url(&format!("/v1/{bucket}/greeting.txt")))
        .bearer_auth(&token)
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), reqwest::StatusCode::OK);
    let bytes = res.bytes().await.unwrap();
    assert_eq!(&bytes[..], &body[..]);

    // DELETE
    let res = client()
        .delete(url(&format!("/v1/{bucket}/greeting.txt")))
        .bearer_auth(&token)
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), reqwest::StatusCode::NO_CONTENT);

    // GET after delete
    let res = client()
        .get(url(&format!("/v1/{bucket}/greeting.txt")))
        .bearer_auth(&token)
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), reqwest::StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn private_object_requires_auth() {
    let bucket = unique_bucket("priv");
    create_bucket(&bucket, "private").await;
    let token = bucket_token(&bucket);

    client()
        .put(url(&format!("/v1/{bucket}/secret.txt")))
        .bearer_auth(&token)
        .body("hush")
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();

    // No auth on GET → 401
    let res = client()
        .get(url(&format!("/v1/{bucket}/secret.txt")))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), reqwest::StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn public_object_served_without_auth() {
    let bucket = unique_bucket("pub");
    create_bucket(&bucket, "public").await;
    let token = bucket_token(&bucket);

    let res = client()
        .put(url(&format!("/v1/{bucket}/avatar.txt")))
        .bearer_auth(&token)
        .header("x-beyond-access", "public")
        .body("img")
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), reqwest::StatusCode::CREATED);

    let res = client()
        .get(url(&format!("/v1/{bucket}/avatar.txt")))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), reqwest::StatusCode::OK);
    assert_eq!(
        res.headers()
            .get("access-control-allow-origin")
            .and_then(|v| v.to_str().ok()),
        Some("*")
    );
}

#[tokio::test]
async fn if_none_match_blocks_overwrite() {
    let bucket = unique_bucket("ifnone");
    create_bucket(&bucket, "private").await;
    let token = bucket_token(&bucket);

    let put = || {
        client()
            .put(url(&format!("/v1/{bucket}/lock")))
            .bearer_auth(&token)
            .header("if-none-match", "*")
            .body("v1")
    };

    let first = put().send().await.unwrap();
    assert_eq!(first.status(), reqwest::StatusCode::CREATED);

    let second = put().send().await.unwrap();
    assert_eq!(second.status(), reqwest::StatusCode::PRECONDITION_FAILED);
    let body: serde_json::Value = second.json().await.unwrap();
    assert_eq!(body["error"]["code"], "object_exists");
}

#[tokio::test]
async fn if_match_enforces_etag() {
    let bucket = unique_bucket("ifmatch");
    create_bucket(&bucket, "private").await;
    let token = bucket_token(&bucket);

    let res = client()
        .put(url(&format!("/v1/{bucket}/cfg.json")))
        .bearer_auth(&token)
        .body("a")
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = res.json().await.unwrap();
    let etag = body["etag"].as_str().unwrap().to_owned();

    // Wrong etag
    let res = client()
        .put(url(&format!("/v1/{bucket}/cfg.json")))
        .bearer_auth(&token)
        .header("if-match", "\"wrong\"")
        .body("b")
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), reqwest::StatusCode::PRECONDITION_FAILED);

    // Correct etag
    let res = client()
        .put(url(&format!("/v1/{bucket}/cfg.json")))
        .bearer_auth(&token)
        .header("if-match", &etag)
        .body("c")
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), reqwest::StatusCode::CREATED);
}

#[tokio::test]
async fn list_with_prefix_and_cursor() {
    let bucket = unique_bucket("list");
    create_bucket(&bucket, "private").await;
    let token = bucket_token(&bucket);

    for i in 0..5 {
        let key = format!("avatars/u{i}.png");
        client()
            .put(url(&format!("/v1/{bucket}/{key}")))
            .bearer_auth(&token)
            .body(vec![b'x'; (i + 1) as usize])
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap();
    }
    client()
        .put(url(&format!("/v1/{bucket}/other/file.txt")))
        .bearer_auth(&token)
        .body("nope")
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();

    let res = client()
        .get(url(&format!("/v1/{bucket}?prefix=avatars/&limit=2")))
        .bearer_auth(&token)
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = res.json().await.unwrap();
    let arr = body["objects"].as_array().unwrap();
    assert_eq!(arr.len(), 2);
    let cursor = body["next_cursor"].as_str().unwrap().to_owned();

    let res = client()
        .get(url(&format!(
            "/v1/{bucket}?prefix=avatars/&cursor={cursor}&limit=10"
        )))
        .bearer_auth(&token)
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = res.json().await.unwrap();
    let arr = body["objects"].as_array().unwrap();
    assert_eq!(arr.len(), 3);
    assert!(body["next_cursor"].is_null());
}

#[tokio::test]
async fn patch_move_and_access() {
    let bucket = unique_bucket("patch");
    create_bucket(&bucket, "private").await;
    let token = bucket_token(&bucket);

    client()
        .put(url(&format!("/v1/{bucket}/orig.txt")))
        .bearer_auth(&token)
        .body("data")
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();

    // Move
    let res = client()
        .patch(url(&format!("/v1/{bucket}/orig.txt")))
        .bearer_auth(&token)
        .json(&serde_json::json!({ "key": "archive/orig.txt" }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = res.json().await.unwrap();
    assert_eq!(body["key"], "archive/orig.txt");

    // Verify the original is gone
    let res = client()
        .head(url(&format!("/v1/{bucket}/orig.txt")))
        .bearer_auth(&token)
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), reqwest::StatusCode::NOT_FOUND);

    // Patch access → public
    let res = client()
        .patch(url(&format!("/v1/{bucket}/archive/orig.txt")))
        .bearer_auth(&token)
        .json(&serde_json::json!({ "access": "public" }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), reqwest::StatusCode::OK);

    // Now publicly accessible
    let res = client()
        .get(url(&format!("/v1/{bucket}/archive/orig.txt")))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), reqwest::StatusCode::OK);
}

#[tokio::test]
async fn copy_object() {
    let bucket = unique_bucket("copy");
    create_bucket(&bucket, "private").await;
    let token = bucket_token(&bucket);

    client()
        .put(url(&format!("/v1/{bucket}/src.txt")))
        .bearer_auth(&token)
        .body("payload")
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();

    let res = client()
        .post(url(&format!("/v1/{bucket}/dst.txt")))
        .bearer_auth(&token)
        .json(&serde_json::json!({ "source": "src.txt" }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), reqwest::StatusCode::CREATED);

    let res = client()
        .get(url(&format!("/v1/{bucket}/dst.txt")))
        .bearer_auth(&token)
        .send()
        .await
        .unwrap();
    assert_eq!(res.bytes().await.unwrap().as_ref(), b"payload");
}

#[tokio::test]
async fn range_request_returns_206() {
    let bucket = unique_bucket("range");
    create_bucket(&bucket, "private").await;
    let token = bucket_token(&bucket);

    let body: Vec<u8> = (0..1024u32).map(|i| (i % 256) as u8).collect();
    client()
        .put(url(&format!("/v1/{bucket}/blob.bin")))
        .bearer_auth(&token)
        .body(body.clone())
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();

    let res = client()
        .get(url(&format!("/v1/{bucket}/blob.bin")))
        .bearer_auth(&token)
        .header("range", "bytes=10-19")
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), reqwest::StatusCode::PARTIAL_CONTENT);
    assert_eq!(
        res.headers()
            .get("content-range")
            .and_then(|v| v.to_str().ok()),
        Some("bytes 10-19/1024")
    );
    let bytes = res.bytes().await.unwrap();
    assert_eq!(bytes.len(), 10);
    assert_eq!(&bytes[..], &body[10..=19]);
}

#[tokio::test]
async fn root_token_overrides_bucket_token() {
    let bucket = unique_bucket("root");
    create_bucket(&bucket, "private").await;

    // Write with root token
    let res = client()
        .put(url(&format!("/v1/{bucket}/file.txt")))
        .bearer_auth(root_token())
        .body("via root")
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), reqwest::StatusCode::CREATED);

    // Cross-bucket bucket token rejected
    let other = unique_bucket("other");
    let wrong = bucket_token(&other);
    let res = client()
        .get(url(&format!("/v1/{bucket}/file.txt")))
        .bearer_auth(wrong)
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), reqwest::StatusCode::UNAUTHORIZED);
}
