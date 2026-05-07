use crate::common::{client, url};

#[tokio::test]
async fn healthz_ok() {
    let res = client().get(url("/healthz")).send().await.unwrap();
    assert_eq!(res.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = res.json().await.unwrap();
    assert_eq!(body["status"], "ok");
    assert!(body["version"].is_string());
}
