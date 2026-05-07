use crate::common::{client, root_token, unique_bucket, url};

#[tokio::test]
async fn create_get_update_delete_bucket() {
    let bucket = unique_bucket("crud");
    // create
    let res = client()
        .post(url("/v1/buckets"))
        .bearer_auth(root_token())
        .json(&serde_json::json!({ "name": &bucket, "access": "private" }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), reqwest::StatusCode::CREATED);

    // get
    let res = client()
        .get(url(&format!("/v1/buckets/{bucket}")))
        .bearer_auth(root_token())
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = res.json().await.unwrap();
    assert_eq!(body["name"], bucket);
    assert_eq!(body["access"], "private");

    // patch
    let res = client()
        .patch(url(&format!("/v1/buckets/{bucket}")))
        .bearer_auth(root_token())
        .json(&serde_json::json!({ "access": "public" }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = res.json().await.unwrap();
    assert_eq!(body["access"], "public");

    // list (must contain our bucket)
    let res = client()
        .get(url("/v1/buckets"))
        .bearer_auth(root_token())
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = res.json().await.unwrap();
    let names: Vec<&str> = body["buckets"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|v| v["name"].as_str())
        .collect();
    assert!(names.contains(&bucket.as_str()));

    // delete
    let res = client()
        .delete(url(&format!("/v1/buckets/{bucket}")))
        .bearer_auth(root_token())
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), reqwest::StatusCode::NO_CONTENT);
}

#[tokio::test]
async fn bucket_crud_requires_root_token() {
    let bucket = unique_bucket("auth");
    // No auth → 401
    let res = client()
        .post(url("/v1/buckets"))
        .json(&serde_json::json!({ "name": &bucket }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), reqwest::StatusCode::UNAUTHORIZED);

    // Bucket-derived token (not root) → 401
    let derived = crate::common::bucket_token(&bucket);
    let res = client()
        .post(url("/v1/buckets"))
        .bearer_auth(derived)
        .json(&serde_json::json!({ "name": &bucket }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), reqwest::StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn delete_non_empty_bucket_conflicts() {
    let bucket = unique_bucket("nonempty");
    crate::common::create_bucket(&bucket, "private").await;

    // Add an object
    let res = client()
        .put(url(&format!("/v1/{bucket}/file.txt")))
        .bearer_auth(crate::common::bucket_token(&bucket))
        .body("hello")
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), reqwest::StatusCode::CREATED);

    // Try to delete bucket
    let res = client()
        .delete(url(&format!("/v1/buckets/{bucket}")))
        .bearer_auth(root_token())
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), reqwest::StatusCode::CONFLICT);
}
