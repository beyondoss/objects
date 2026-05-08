use crate::common::{byte_stream, create_bucket_via_rest, s3_client, unique_bucket};

#[tokio::test]
async fn put_get_head_delete_round_trip() {
    let bucket = unique_bucket("s3rt");
    create_bucket_via_rest(&bucket, "private").await;
    let s3 = s3_client(&bucket);

    let body = b"hello s3";

    s3.put_object()
        .bucket(&bucket)
        .key("hello.txt")
        .content_type("text/plain")
        .body(byte_stream(body))
        .send()
        .await
        .expect("put_object");

    let head = s3
        .head_object()
        .bucket(&bucket)
        .key("hello.txt")
        .send()
        .await
        .expect("head_object");
    assert_eq!(head.content_length(), Some(body.len() as i64));
    assert_eq!(head.content_type(), Some("text/plain"));

    let got = s3
        .get_object()
        .bucket(&bucket)
        .key("hello.txt")
        .send()
        .await
        .expect("get_object");
    let bytes = got.body.collect().await.expect("collect").to_vec();
    assert_eq!(bytes, body);

    s3.delete_object()
        .bucket(&bucket)
        .key("hello.txt")
        .send()
        .await
        .expect("delete_object");

    let err = s3
        .head_object()
        .bucket(&bucket)
        .key("hello.txt")
        .send()
        .await
        .expect_err("head after delete");
    let err_str = format!("{err:?}");
    assert!(
        err_str.contains("NotFound") || err_str.contains("404"),
        "unexpected post-delete error: {err_str}"
    );
}

#[tokio::test]
async fn copy_object_within_bucket() {
    let bucket = unique_bucket("s3cp");
    create_bucket_via_rest(&bucket, "private").await;
    let s3 = s3_client(&bucket);

    s3.put_object()
        .bucket(&bucket)
        .key("orig.bin")
        .body(byte_stream(b"data"))
        .send()
        .await
        .expect("put_object");

    s3.copy_object()
        .bucket(&bucket)
        .key("dup.bin")
        .copy_source(format!("{bucket}/orig.bin"))
        .send()
        .await
        .expect("copy_object");

    let got = s3
        .get_object()
        .bucket(&bucket)
        .key("dup.bin")
        .send()
        .await
        .expect("get_object");
    let bytes = got.body.collect().await.expect("collect").to_vec();
    assert_eq!(bytes, b"data");
}

#[tokio::test]
async fn list_objects_v2_with_prefix() {
    let bucket = unique_bucket("s3ls");
    create_bucket_via_rest(&bucket, "private").await;
    let s3 = s3_client(&bucket);

    for k in ["avatars/a.png", "avatars/b.png", "other.txt"] {
        s3.put_object()
            .bucket(&bucket)
            .key(k)
            .body(byte_stream(b"x"))
            .send()
            .await
            .unwrap_or_else(|_| panic!("put {k}"));
    }

    let resp = s3
        .list_objects_v2()
        .bucket(&bucket)
        .prefix("avatars/")
        .send()
        .await
        .expect("list_objects_v2");

    let keys: Vec<&str> = resp.contents().iter().filter_map(|o| o.key()).collect();
    assert_eq!(keys, vec!["avatars/a.png", "avatars/b.png"]);
    assert_eq!(resp.key_count(), Some(2));
}

#[tokio::test]
async fn list_objects_v2_with_delimiter_rolls_up() {
    let bucket = unique_bucket("s3del");
    create_bucket_via_rest(&bucket, "private").await;
    let s3 = s3_client(&bucket);

    for k in ["a/1", "a/2", "b/1", "top.txt"] {
        s3.put_object()
            .bucket(&bucket)
            .key(k)
            .body(byte_stream(b"x"))
            .send()
            .await
            .unwrap_or_else(|_| panic!("put {k}"));
    }

    let resp = s3
        .list_objects_v2()
        .bucket(&bucket)
        .delimiter("/")
        .send()
        .await
        .expect("list_objects_v2 delim");

    let prefixes: Vec<&str> = resp
        .common_prefixes()
        .iter()
        .filter_map(|p| p.prefix())
        .collect();
    assert!(prefixes.contains(&"a/"));
    assert!(prefixes.contains(&"b/"));

    let keys: Vec<&str> = resp.contents().iter().filter_map(|o| o.key()).collect();
    assert_eq!(keys, vec!["top.txt"]);
}

#[tokio::test]
async fn list_buckets_returns_created_buckets() {
    let bucket = unique_bucket("s3lsb");
    create_bucket_via_rest(&bucket, "private").await;

    let s3 = s3_client("root");
    let resp = s3.list_buckets().send().await.expect("list_buckets");
    let names: Vec<&str> = resp.buckets().iter().filter_map(|b| b.name()).collect();
    assert!(names.contains(&bucket.as_str()));
}

#[tokio::test]
async fn create_and_delete_bucket_via_s3() {
    let bucket = unique_bucket("s3cb");
    let s3 = s3_client("root");

    s3.create_bucket()
        .bucket(&bucket)
        .send()
        .await
        .expect("create_bucket");

    s3.head_bucket()
        .bucket(&bucket)
        .send()
        .await
        .expect("head_bucket");

    s3.delete_bucket()
        .bucket(&bucket)
        .send()
        .await
        .expect("delete_bucket");
}
