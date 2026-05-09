use crate::common::{client, url};

#[tokio::test]
async fn openapi_json_served() {
    let res = client().get(url("/v1/openapi.json")).send().await.unwrap();
    assert_eq!(res.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = res.json().await.unwrap();
    assert_eq!(
        body["openapi"].as_str().unwrap_or("").chars().next(),
        Some('3')
    );
    assert!(body["paths"]["/v1/buckets"].is_object());
    assert!(body["paths"]["/livez"].is_object());
    assert!(body["paths"]["/readyz"].is_object());
    assert!(body["components"]["securitySchemes"]["BearerAuth"].is_object());
}

#[tokio::test]
async fn metrics_text() {
    let res = client()
        .get(format!("{}/metrics", crate::common::server().metrics_url))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), reqwest::StatusCode::OK);
    let text = res.text().await.unwrap();
    assert!(text.contains("http_requests_total"));
}
