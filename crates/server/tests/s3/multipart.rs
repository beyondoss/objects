use crate::common::{byte_stream, create_bucket_via_rest, s3_client, unique_bucket};

#[tokio::test]
async fn multipart_upload_lifecycle() {
    let bucket = unique_bucket("s3mp");
    create_bucket_via_rest(&bucket, "private").await;
    let s3 = s3_client(&bucket);

    let create = s3
        .create_multipart_upload()
        .bucket(&bucket)
        .key("big.bin")
        .content_type("application/octet-stream")
        .send()
        .await
        .expect("create_multipart_upload");
    let upload_id = create.upload_id().expect("upload_id").to_string();

    let part_one = s3
        .upload_part()
        .bucket(&bucket)
        .key("big.bin")
        .upload_id(&upload_id)
        .part_number(1)
        .body(byte_stream(b"hello "))
        .send()
        .await
        .expect("upload part 1");
    let etag1 = part_one.e_tag().expect("etag1").to_string();

    let part_two = s3
        .upload_part()
        .bucket(&bucket)
        .key("big.bin")
        .upload_id(&upload_id)
        .part_number(2)
        .body(byte_stream(b"world"))
        .send()
        .await
        .expect("upload part 2");
    let etag2 = part_two.e_tag().expect("etag2").to_string();

    let parts = s3
        .list_parts()
        .bucket(&bucket)
        .key("big.bin")
        .upload_id(&upload_id)
        .send()
        .await
        .expect("list_parts");
    assert_eq!(parts.parts().len(), 2);

    let uploads = s3
        .list_multipart_uploads()
        .bucket(&bucket)
        .send()
        .await
        .expect("list_multipart_uploads");
    let keys: Vec<&str> = uploads.uploads().iter().filter_map(|u| u.key()).collect();
    assert!(keys.contains(&"big.bin"));

    let completed = aws_sdk_s3::types::CompletedMultipartUpload::builder()
        .parts(
            aws_sdk_s3::types::CompletedPart::builder()
                .part_number(1)
                .e_tag(etag1)
                .build(),
        )
        .parts(
            aws_sdk_s3::types::CompletedPart::builder()
                .part_number(2)
                .e_tag(etag2)
                .build(),
        )
        .build();

    let complete = s3
        .complete_multipart_upload()
        .bucket(&bucket)
        .key("big.bin")
        .upload_id(&upload_id)
        .multipart_upload(completed)
        .send()
        .await
        .expect("complete_multipart_upload");
    let final_etag = complete.e_tag().expect("final etag").to_string();
    assert!(
        final_etag.ends_with("-2\""),
        "expected `…-2\"` etag, got {final_etag:?}"
    );

    let got = s3
        .get_object()
        .bucket(&bucket)
        .key("big.bin")
        .send()
        .await
        .expect("get assembled object");
    let bytes = got.body.collect().await.expect("collect").to_vec();
    assert_eq!(bytes, b"hello world");
}

#[tokio::test]
async fn abort_multipart_cleans_up() {
    let bucket = unique_bucket("s3mpa");
    create_bucket_via_rest(&bucket, "private").await;
    let s3 = s3_client(&bucket);

    let create = s3
        .create_multipart_upload()
        .bucket(&bucket)
        .key("partial.bin")
        .send()
        .await
        .expect("create_multipart_upload");
    let upload_id = create.upload_id().expect("upload_id").to_string();

    s3.upload_part()
        .bucket(&bucket)
        .key("partial.bin")
        .upload_id(&upload_id)
        .part_number(1)
        .body(byte_stream(b"a"))
        .send()
        .await
        .expect("upload part");

    s3.abort_multipart_upload()
        .bucket(&bucket)
        .key("partial.bin")
        .upload_id(&upload_id)
        .send()
        .await
        .expect("abort_multipart_upload");

    // List parts on the aborted upload should now return NoSuchUpload.
    let err = s3
        .list_parts()
        .bucket(&bucket)
        .key("partial.bin")
        .upload_id(&upload_id)
        .send()
        .await
        .expect_err("list_parts after abort");
    let err_str = format!("{err:?}");
    assert!(
        err_str.contains("NoSuchUpload") || err_str.contains("404"),
        "unexpected error after abort: {err_str}"
    );
}
