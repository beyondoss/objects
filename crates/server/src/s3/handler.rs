//! S3 trait implementation backed by `Storage` and `Index`.
//!
//! Every method on `S3` has a default that returns `NotImplemented`, so this
//! file only implements the operations we explicitly support (see
//! `DESIGN.md > "What we support"`). The struct holds an `AppState` clone —
//! same pattern as axum extractors on the `/v1` REST surface — so the two
//! protocol surfaces share storage, index, and the `publish` event hook.

use std::collections::{BTreeSet, HashMap};
use std::time::{Instant, SystemTime};

use axum::http::StatusCode;
use futures::TryStreamExt;
use s3s::dto::{
    AbortMultipartUploadInput, AbortMultipartUploadOutput, Bucket, Buckets, CommonPrefix,
    CommonPrefixList, CompleteMultipartUploadInput, CompleteMultipartUploadOutput, CopyObjectInput,
    CopyObjectOutput, CopyObjectResult, CopySource, CreateBucketInput, CreateBucketOutput,
    CreateMultipartUploadInput, CreateMultipartUploadOutput, DeleteBucketInput, DeleteBucketOutput,
    DeleteObjectInput, DeleteObjectOutput, ETag, GetObjectInput, GetObjectOutput, HeadBucketInput,
    HeadBucketOutput, HeadObjectInput, HeadObjectOutput, ListBucketsInput, ListBucketsOutput,
    ListMultipartUploadsInput, ListMultipartUploadsOutput, ListObjectsV2Input, ListObjectsV2Output,
    ListPartsInput, ListPartsOutput, MultipartUpload, MultipartUploadList, Object, ObjectList,
    Part, Parts, PutObjectInput, PutObjectOutput, StreamingBlob, Timestamp, UploadPartInput,
    UploadPartOutput,
};
use s3s::{S3, S3Error, S3Request, S3Response, S3Result, s3_error};
use tokio_util::io::StreamReader;

use beyond_objects_storage::{
    AccessLevel, CompletedPart as StorageCompletedPart, ObjectInfo, ObjectMeta, WriteCondition,
};

use crate::AppState;

use super::error::{from_api, from_index, from_storage};

const MAX_LIST_KEYS: usize = 1000;

#[derive(Clone)]
pub struct ObjectsS3 {
    pub(super) state: AppState,
}

#[async_trait::async_trait]
impl S3 for ObjectsS3 {
    // ---------- bucket ops ----------

    async fn list_buckets(
        &self,
        _req: S3Request<ListBucketsInput>,
    ) -> S3Result<S3Response<ListBucketsOutput>> {
        let metas = self
            .state
            .storage
            .list_buckets()
            .await
            .map_err(from_storage)?;
        let buckets: Vec<Bucket> = metas
            .into_iter()
            .map(|b| Bucket {
                name: Some(b.name),
                creation_date: None,
                bucket_region: None,
            })
            .collect();
        Ok(S3Response::new(ListBucketsOutput {
            buckets: Some(Buckets::from(buckets)),
            ..Default::default()
        }))
    }

    async fn create_bucket(
        &self,
        req: S3Request<CreateBucketInput>,
    ) -> S3Result<S3Response<CreateBucketOutput>> {
        let bucket = req.input.bucket;
        let access = req
            .input
            .acl
            .as_ref()
            .map(|acl| canned_acl_str_to_access(acl.as_str()))
            .transpose()?
            .unwrap_or(AccessLevel::Private);
        self.state
            .storage
            .create_bucket(&bucket, access)
            .await
            .map_err(from_storage)?;
        Ok(S3Response::new(CreateBucketOutput {
            location: Some(format!("/{bucket}")),
        }))
    }

    async fn delete_bucket(
        &self,
        req: S3Request<DeleteBucketInput>,
    ) -> S3Result<S3Response<DeleteBucketOutput>> {
        self.state
            .storage
            .delete_bucket(&req.input.bucket)
            .await
            .map_err(from_storage)?;
        Ok(S3Response::new(DeleteBucketOutput {}))
    }

    async fn head_bucket(
        &self,
        req: S3Request<HeadBucketInput>,
    ) -> S3Result<S3Response<HeadBucketOutput>> {
        self.state
            .storage
            .get_bucket(&req.input.bucket)
            .await
            .map_err(from_storage)?;
        Ok(S3Response::new(HeadBucketOutput::default()))
    }

    // ---------- object ops ----------

    async fn put_object(
        &self,
        req: S3Request<PutObjectInput>,
    ) -> S3Result<S3Response<PutObjectOutput>> {
        let input = req.input;
        let bucket = input.bucket;
        let key = input.key;

        let condition = match (&input.if_none_match, &input.if_match) {
            (Some(_), Some(_)) => {
                return Err(s3_error!(
                    InvalidArgument,
                    "If-None-Match and If-Match are mutually exclusive"
                ));
            }
            (Some(cond), None) if cond.is_any() => Some(WriteCondition::IfNoneMatch),
            (Some(_), None) => {
                return Err(s3_error!(
                    InvalidArgument,
                    "If-None-Match: only `*` is supported"
                ));
            }
            (None, Some(cond)) => match cond.as_etag() {
                Some(et) => Some(WriteCondition::IfMatch(format!("\"{}\"", et.value()))),
                None => Some(WriteCondition::IfMatch("\"*\"".into())),
            },
            (None, None) => None,
        };

        let access = input
            .acl
            .as_ref()
            .map(|acl| canned_acl_str_to_access(acl.as_str()))
            .transpose()?;

        let user_metadata: HashMap<String, String> = input
            .metadata
            .map(|m| m.into_iter().collect())
            .unwrap_or_default();

        let meta = ObjectMeta {
            content_type: input.content_type,
            access,
            user_metadata,
        };

        let body = input
            .body
            .ok_or_else(|| s3_error!(InvalidRequest, "PutObject requires a body"))?;
        let stream = body.map_err(std::io::Error::other);
        let mut reader = StreamReader::new(stream);

        let t = Instant::now();
        let (etag, size) = self
            .state
            .storage
            .write_object(&bucket, &key, &mut reader, meta, condition)
            .await
            .map_err(from_storage)?;
        self.state
            .metrics
            .storage_operation_seconds
            .with_label_values(&["write"])
            .observe(t.elapsed().as_secs_f64());
        self.state
            .metrics
            .bytes_uploaded_total
            .with_label_values(&[&bucket])
            .inc_by(size as f64);

        let idx = self.state.index.clone();
        let bucket_owned = bucket.clone();
        let key_owned = key.clone();
        tokio::task::spawn_blocking(move || idx.insert(&bucket_owned, &key_owned))
            .await
            .map_err(|e| s3_error!(InternalError, "index insert join: {e}"))?
            .map_err(from_index)?;

        self.state
            .publish(&self.state.config.base_url(), &bucket, &key);

        Ok(S3Response::new(PutObjectOutput {
            e_tag: Some(etag_to_dto(&etag)),
            ..Default::default()
        }))
    }

    async fn get_object(
        &self,
        req: S3Request<GetObjectInput>,
    ) -> S3Result<S3Response<GetObjectOutput>> {
        let authenticated = req.credentials.is_some();
        let input = req.input;
        let bucket = input.bucket;
        let key = input.key;

        let t = Instant::now();
        let (info, file) = self
            .state
            .storage
            .open_object(&bucket, &key)
            .await
            .map_err(from_storage)?;
        self.state
            .metrics
            .storage_operation_seconds
            .with_label_values(&["read"])
            .observe(t.elapsed().as_secs_f64());

        enforce_anonymous_visibility(authenticated, info.access)?;

        if let ConditionOutcome::NotModified = check_read_conditions(
            &info,
            input.if_match.as_ref().and_then(|c| c.as_etag()),
            input.if_none_match.as_ref().and_then(|c| c.as_etag()),
            input.if_modified_since.as_ref(),
            input.if_unmodified_since.as_ref(),
        )? {
            return Ok(S3Response::with_status(
                GetObjectOutput {
                    e_tag: Some(etag_to_dto(&info.etag)),
                    last_modified: Some(Timestamp::from(info.last_modified)),
                    ..Default::default()
                },
                StatusCode::NOT_MODIFIED,
            ));
        }

        let (start, end) = match input.range {
            Some(r) => {
                let bytes = r.check(info.size).map_err(|_| s3_error!(InvalidRange))?;
                (bytes.start, bytes.end) // exclusive end
            }
            None => (0, info.size),
        };
        let length = end.saturating_sub(start);
        self.state
            .metrics
            .bytes_downloaded_total
            .with_label_values(&[&bucket])
            .inc_by(length as f64);

        let body = if length == 0 {
            StreamingBlob::wrap(futures::stream::empty::<std::io::Result<bytes::Bytes>>())
        } else {
            let file_std = file.into_std().await;
            let data = tokio::task::spawn_blocking(move || -> std::io::Result<bytes::Bytes> {
                // SAFETY: Objects are write-once-by-rename (no in-place mutation ever
                // occurs). `delete_object` and `move_object` use `unlink`/`rename`; on
                // Linux and macOS these never invalidate a live `Mmap` — the inode is
                // reference-counted by the OS and persists until all fds and mappings
                // are released. `file_std` pins the inode through mmap creation;
                // dropping it afterward is safe because `Mmap` holds the reference
                // independently of the fd.
                let mmap = unsafe {
                    memmap2::MmapOptions::new()
                        .offset(start)
                        .len(length as usize)
                        .map(&file_std)?
                };
                drop(file_std);
                Ok(bytes::Bytes::from_owner(mmap))
            })
            .await
            .map_err(|e| s3_error!(InternalError, "read task: {e}"))?
            .map_err(|e| s3_error!(InternalError, "mmap: {e}"))?;
            StreamingBlob::wrap(futures::stream::once(async move {
                Ok::<_, std::io::Error>(data)
            }))
        };

        let content_range = if input.range.is_some() {
            Some(format!("bytes {}-{}/{}", start, end - 1, info.size))
        } else {
            None
        };

        Ok(S3Response::new(GetObjectOutput {
            body: Some(body),
            content_length: Some(length as i64),
            content_range,
            content_type: info.content_type,
            e_tag: Some(etag_to_dto(&info.etag)),
            last_modified: Some(Timestamp::from(info.last_modified)),
            metadata: metadata_dto(&info.user_metadata),
            accept_ranges: Some("bytes".into()),
            ..Default::default()
        }))
    }

    async fn head_object(
        &self,
        req: S3Request<HeadObjectInput>,
    ) -> S3Result<S3Response<HeadObjectOutput>> {
        let authenticated = req.credentials.is_some();
        let input = req.input;
        let t = Instant::now();
        let info = self
            .state
            .storage
            .head_object(&input.bucket, &input.key)
            .await
            .map_err(from_storage)?;
        self.state
            .metrics
            .storage_operation_seconds
            .with_label_values(&["head"])
            .observe(t.elapsed().as_secs_f64());

        enforce_anonymous_visibility(authenticated, info.access)?;

        if let ConditionOutcome::NotModified = check_read_conditions(
            &info,
            input.if_match.as_ref().and_then(|c| c.as_etag()),
            input.if_none_match.as_ref().and_then(|c| c.as_etag()),
            input.if_modified_since.as_ref(),
            input.if_unmodified_since.as_ref(),
        )? {
            return Ok(S3Response::with_status(
                HeadObjectOutput {
                    e_tag: Some(etag_to_dto(&info.etag)),
                    last_modified: Some(Timestamp::from(info.last_modified)),
                    ..Default::default()
                },
                StatusCode::NOT_MODIFIED,
            ));
        }

        Ok(S3Response::new(HeadObjectOutput {
            content_length: Some(info.size as i64),
            content_type: info.content_type,
            e_tag: Some(etag_to_dto(&info.etag)),
            last_modified: Some(Timestamp::from(info.last_modified)),
            metadata: metadata_dto(&info.user_metadata),
            accept_ranges: Some("bytes".into()),
            ..Default::default()
        }))
    }

    async fn delete_object(
        &self,
        req: S3Request<DeleteObjectInput>,
    ) -> S3Result<S3Response<DeleteObjectOutput>> {
        let bucket = req.input.bucket;
        let key = req.input.key;

        // Mirror the REST handler's failure ordering: storage first, then
        // index. Reconcile heals any gap on next startup.
        let t = Instant::now();
        let delete_result = self.state.storage.delete_object(&bucket, &key).await;
        self.state
            .metrics
            .storage_operation_seconds
            .with_label_values(&["delete"])
            .observe(t.elapsed().as_secs_f64());
        match delete_result {
            Ok(()) => {
                let idx = self.state.index.clone();
                let b = bucket.clone();
                let k = key.clone();
                tokio::task::spawn_blocking(move || idx.delete(&b, &k))
                    .await
                    .map_err(|e| s3_error!(InternalError, "index delete join: {e}"))?
                    .map_err(from_index)?;
            }
            Err(beyond_objects_storage::StorageError::NotFound { .. }) => {
                // S3 DeleteObject is idempotent — succeed silently.
                let idx = self.state.index.clone();
                let b = bucket.clone();
                let k = key.clone();
                match tokio::task::spawn_blocking(move || idx.delete(&b, &k)).await {
                    Err(e) => tracing::warn!(
                        error = %e, bucket = %bucket, key = %key,
                        "index cleanup task panicked on idempotent delete"
                    ),
                    Ok(Err(e)) => tracing::warn!(
                        error = %e, bucket = %bucket, key = %key,
                        "index cleanup failed on idempotent delete"
                    ),
                    Ok(Ok(())) => {}
                }
            }
            Err(e) => return Err(from_storage(e)),
        }
        Ok(S3Response::new(DeleteObjectOutput::default()))
    }

    async fn copy_object(
        &self,
        req: S3Request<CopyObjectInput>,
    ) -> S3Result<S3Response<CopyObjectOutput>> {
        let input = req.input;
        let dst_bucket = input.bucket;
        let dst_key = input.key;
        let (src_bucket, src_key) = match input.copy_source {
            CopySource::Bucket { bucket, key, .. } => (bucket.into_string(), key.into_string()),
            CopySource::AccessPoint { .. } => {
                return Err(s3_error!(
                    NotImplemented,
                    "access-point copy sources are not supported"
                ));
            }
        };

        let t = Instant::now();
        let etag = self
            .state
            .storage
            .copy_object(&src_bucket, &src_key, &dst_bucket, &dst_key)
            .await
            .map_err(from_storage)?;
        self.state
            .metrics
            .storage_operation_seconds
            .with_label_values(&["copy"])
            .observe(t.elapsed().as_secs_f64());

        let idx = self.state.index.clone();
        let b = dst_bucket.clone();
        let k = dst_key.clone();
        tokio::task::spawn_blocking(move || idx.insert(&b, &k))
            .await
            .map_err(|e| s3_error!(InternalError, "index insert join: {e}"))?
            .map_err(from_index)?;

        Ok(S3Response::new(CopyObjectOutput {
            copy_object_result: Some(CopyObjectResult {
                e_tag: Some(etag_to_dto(&etag)),
                last_modified: Some(Timestamp::from(SystemTime::now())),
                ..Default::default()
            }),
            ..Default::default()
        }))
    }

    async fn list_objects_v2(
        &self,
        req: S3Request<ListObjectsV2Input>,
    ) -> S3Result<S3Response<ListObjectsV2Output>> {
        let input = req.input;
        let bucket = input.bucket;
        let prefix = input.prefix.clone().unwrap_or_default();
        let limit = match input.max_keys {
            None => MAX_LIST_KEYS,
            Some(n) => {
                let n = u32::try_from(n)
                    .map_err(|_| s3_error!(InvalidArgument, "max-keys must be non-negative"))?;
                (n as usize).min(MAX_LIST_KEYS)
            }
        };
        let cursor = input.continuation_token.clone();
        let delimiter = input.delimiter.clone();

        let page = self
            .state
            .list_page(&bucket, &prefix, cursor.as_deref(), limit)
            .await
            .map_err(from_api)?;

        let (contents, common_prefixes) = match delimiter.as_deref() {
            Some(d) => collapse_with_delimiter(&prefix, d, page.items),
            None => {
                let contents: Vec<Object> = page
                    .items
                    .into_iter()
                    .map(|item| Object {
                        key: Some(item.key),
                        size: Some(item.info.size as i64),
                        e_tag: Some(etag_to_dto(&item.info.etag)),
                        last_modified: Some(Timestamp::from(item.info.last_modified)),
                        ..Default::default()
                    })
                    .collect();
                (contents, Vec::new())
            }
        };

        let key_count = (contents.len() + common_prefixes.len()) as i32;
        let is_truncated = page.next_cursor.is_some();
        let common_prefixes = if common_prefixes.is_empty() {
            None
        } else {
            Some(CommonPrefixList::from(common_prefixes))
        };

        Ok(S3Response::new(ListObjectsV2Output {
            name: Some(bucket),
            prefix: input.prefix,
            max_keys: Some(limit as i32),
            key_count: Some(key_count),
            continuation_token: cursor,
            next_continuation_token: page.next_cursor,
            is_truncated: Some(is_truncated),
            contents: Some(ObjectList::from(contents)),
            common_prefixes,
            delimiter,
            encoding_type: input.encoding_type,
            start_after: input.start_after,
            request_charged: None,
        }))
    }

    // ---------- multipart ops ----------

    async fn create_multipart_upload(
        &self,
        req: S3Request<CreateMultipartUploadInput>,
    ) -> S3Result<S3Response<CreateMultipartUploadOutput>> {
        let input = req.input;
        let bucket = input.bucket;
        let key = input.key;

        let access = input
            .acl
            .as_ref()
            .map(|acl| canned_acl_str_to_access(acl.as_str()))
            .transpose()?;
        let user_metadata: HashMap<String, String> = input
            .metadata
            .map(|m| m.into_iter().collect())
            .unwrap_or_default();
        let meta = ObjectMeta {
            content_type: input.content_type,
            access,
            user_metadata,
        };

        let t = Instant::now();
        let upload_id = self
            .state
            .storage
            .init_multipart(&bucket, &key, meta)
            .await
            .map_err(from_storage)?;
        self.state
            .metrics
            .storage_operation_seconds
            .with_label_values(&["initiate_multipart"])
            .observe(t.elapsed().as_secs_f64());
        self.state.metrics.multipart_uploads_active.inc();

        Ok(S3Response::new(CreateMultipartUploadOutput {
            bucket: Some(bucket),
            key: Some(key),
            upload_id: Some(upload_id),
            ..Default::default()
        }))
    }

    async fn upload_part(
        &self,
        req: S3Request<UploadPartInput>,
    ) -> S3Result<S3Response<UploadPartOutput>> {
        let input = req.input;
        let body = input
            .body
            .ok_or_else(|| s3_error!(InvalidRequest, "UploadPart requires a body"))?;
        let stream = body.map_err(std::io::Error::other);
        let mut reader = StreamReader::new(stream);

        let part_number = u32::try_from(input.part_number)
            .map_err(|_| s3_error!(InvalidArgument, "part number out of range"))?;

        let t = Instant::now();
        let etag = self
            .state
            .storage
            .write_part(&input.upload_id, part_number, &mut reader)
            .await
            .map_err(from_storage)?;
        self.state
            .metrics
            .storage_operation_seconds
            .with_label_values(&["upload_part"])
            .observe(t.elapsed().as_secs_f64());

        Ok(S3Response::new(UploadPartOutput {
            e_tag: Some(etag_to_dto(&etag)),
            ..Default::default()
        }))
    }

    async fn complete_multipart_upload(
        &self,
        req: S3Request<CompleteMultipartUploadInput>,
    ) -> S3Result<S3Response<CompleteMultipartUploadOutput>> {
        let input = req.input;
        let bucket = input.bucket;
        let key = input.key;
        let upload_id = input.upload_id;

        let parts_in: Vec<StorageCompletedPart> = input
            .multipart_upload
            .and_then(|m| m.parts)
            .ok_or_else(|| s3_error!(InvalidRequest, "missing parts list"))?
            .into_iter()
            .map(|p| {
                let number = p
                    .part_number
                    .ok_or_else(|| s3_error!(InvalidPart, "missing part_number"))?;
                let number = u32::try_from(number)
                    .map_err(|_| s3_error!(InvalidPart, "part_number out of range"))?;
                let etag = p
                    .e_tag
                    .ok_or_else(|| s3_error!(InvalidPart, "missing etag"))?;
                Ok::<_, S3Error>(StorageCompletedPart {
                    number,
                    etag: format!("\"{}\"", etag.value()),
                })
            })
            .collect::<S3Result<Vec<_>>>()?;

        let t = Instant::now();
        let (final_etag, _size) = self
            .state
            .storage
            .complete_multipart(&upload_id, &parts_in)
            .await
            .map_err(from_storage)?;
        self.state
            .metrics
            .storage_operation_seconds
            .with_label_values(&["complete_multipart"])
            .observe(t.elapsed().as_secs_f64());
        self.state.metrics.multipart_uploads_active.dec();
        self.state
            .metrics
            .multipart_uploads_total
            .with_label_values(&["completed"])
            .inc();

        let idx = self.state.index.clone();
        let b = bucket.clone();
        let k = key.clone();
        tokio::task::spawn_blocking(move || idx.insert(&b, &k))
            .await
            .map_err(|e| s3_error!(InternalError, "index insert join: {e}"))?
            .map_err(from_index)?;

        self.state
            .publish(&self.state.config.base_url(), &bucket, &key);

        Ok(S3Response::new(CompleteMultipartUploadOutput {
            bucket: Some(bucket),
            key: Some(key),
            e_tag: Some(etag_to_dto(&final_etag)),
            ..Default::default()
        }))
    }

    async fn abort_multipart_upload(
        &self,
        req: S3Request<AbortMultipartUploadInput>,
    ) -> S3Result<S3Response<AbortMultipartUploadOutput>> {
        let t = Instant::now();
        self.state
            .storage
            .abort_multipart(&req.input.upload_id)
            .await
            .map_err(from_storage)?;
        self.state
            .metrics
            .storage_operation_seconds
            .with_label_values(&["abort_multipart"])
            .observe(t.elapsed().as_secs_f64());
        self.state.metrics.multipart_uploads_active.dec();
        self.state
            .metrics
            .multipart_uploads_total
            .with_label_values(&["aborted"])
            .inc();
        Ok(S3Response::new(AbortMultipartUploadOutput::default()))
    }

    async fn list_parts(
        &self,
        req: S3Request<ListPartsInput>,
    ) -> S3Result<S3Response<ListPartsOutput>> {
        let input = req.input;
        let parts_in = self
            .state
            .storage
            .list_parts(&input.upload_id)
            .await
            .map_err(from_storage)?;

        let parts: Vec<Part> = parts_in
            .into_iter()
            .map(|p| Part {
                part_number: Some(p.number as i32),
                e_tag: Some(etag_to_dto(&p.etag)),
                size: Some(p.size as i64),
                last_modified: Some(Timestamp::from(p.last_modified)),
                ..Default::default()
            })
            .collect();

        Ok(S3Response::new(ListPartsOutput {
            bucket: Some(input.bucket),
            key: Some(input.key),
            upload_id: Some(input.upload_id),
            parts: Some(Parts::from(parts)),
            ..Default::default()
        }))
    }

    async fn list_multipart_uploads(
        &self,
        req: S3Request<ListMultipartUploadsInput>,
    ) -> S3Result<S3Response<ListMultipartUploadsOutput>> {
        let input = req.input;
        let prefix = input.prefix.as_deref();
        let infos = self
            .state
            .storage
            .list_multipart_uploads(&input.bucket, prefix)
            .await
            .map_err(from_storage)?;

        let uploads: Vec<MultipartUpload> = infos
            .into_iter()
            .map(|m| MultipartUpload {
                upload_id: Some(m.upload_id),
                key: Some(m.key),
                initiated: Some(Timestamp::from(m.init_time)),
                ..Default::default()
            })
            .collect();

        Ok(S3Response::new(ListMultipartUploadsOutput {
            bucket: Some(input.bucket),
            prefix: input.prefix,
            uploads: Some(MultipartUploadList::from(uploads)),
            ..Default::default()
        }))
    }
}

// ---------- helpers ----------

/// When the request had no SigV4 credentials, only allow access to public
/// objects — mirrors `routes/objects.rs::enforce_object_auth` for the REST
/// surface. `S3Access` already gated which *operations* anonymous requests
/// can reach (Get/Head/List); this enforces the *per-object* visibility.
fn enforce_anonymous_visibility(authenticated: bool, access: AccessLevel) -> S3Result<()> {
    if authenticated || access == AccessLevel::Public {
        Ok(())
    } else {
        Err(s3_error!(
            AccessDenied,
            "object is private; bearer token or SigV4 credentials required"
        ))
    }
}

fn canned_acl_str_to_access(acl: &str) -> S3Result<AccessLevel> {
    match acl {
        "private" => Ok(AccessLevel::Private),
        "public-read" => Ok(AccessLevel::Public),
        other => Err(s3_error!(
            InvalidArgument,
            "ACL `{other}` is not supported (only `private` and `public-read`)"
        )),
    }
}

fn etag_to_dto(quoted: &str) -> ETag {
    // Storage stores etags in their HTTP-quoted form: `"hex"` or `"hex-N"`. The
    // s3s ETag value drops the quotes — `as_strong()` returns the inner text.
    debug_assert!(
        quoted.starts_with('"') && quoted.ends_with('"'),
        "etag from storage must be double-quoted: {quoted:?}"
    );
    ETag::Strong(quoted.trim_matches('"').to_owned())
}

fn metadata_dto(map: &HashMap<String, String>) -> Option<s3s::dto::Metadata> {
    if map.is_empty() {
        None
    } else {
        Some(map.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
    }
}

enum ConditionOutcome {
    Pass,
    NotModified,
}

fn check_read_conditions(
    info: &ObjectInfo,
    if_match: Option<&ETag>,
    if_none_match: Option<&ETag>,
    if_modified_since: Option<&Timestamp>,
    if_unmodified_since: Option<&Timestamp>,
) -> S3Result<ConditionOutcome> {
    let actual_etag = info.etag.trim_matches('"');
    let actual_ts: Timestamp = info.last_modified.into();

    // 412 conditions — the request cannot proceed regardless of HTTP method.
    if let Some(et) = if_match
        && et.value() != actual_etag
    {
        return Err(s3_error!(PreconditionFailed, "If-Match etag mismatch"));
    }
    if let Some(t) = if_unmodified_since
        && actual_ts > *t
    {
        return Err(s3_error!(PreconditionFailed, "modified since"));
    }

    // 304 conditions — callers return Not Modified instead of streaming.
    if let Some(et) = if_none_match
        && et.value() == actual_etag
    {
        return Ok(ConditionOutcome::NotModified);
    }
    if let Some(t) = if_modified_since
        && actual_ts <= *t
    {
        return Ok(ConditionOutcome::NotModified);
    }

    Ok(ConditionOutcome::Pass)
}

/// Collapse keys at the next occurrence of `delimiter` after `prefix` into
/// `CommonPrefixes`. Keys without the delimiter beyond the prefix go into
/// `Contents`. Implements the only delimiter shape AWS clients actually use
/// (single-character `/`).
fn collapse_with_delimiter(
    prefix: &str,
    delimiter: &str,
    items: Vec<crate::ListItem>,
) -> (Vec<Object>, Vec<CommonPrefix>) {
    let mut contents = Vec::new();
    let mut seen_prefixes: BTreeSet<String> = BTreeSet::new();

    for item in items {
        let after_prefix = match item.key.strip_prefix(prefix) {
            Some(s) => s,
            None => {
                // Index bug or stale prefix — fall through as a regular item.
                contents.push(Object {
                    key: Some(item.key),
                    size: Some(item.info.size as i64),
                    e_tag: Some(etag_to_dto(&item.info.etag)),
                    last_modified: Some(Timestamp::from(item.info.last_modified)),
                    ..Default::default()
                });
                continue;
            }
        };

        match after_prefix.find(delimiter) {
            Some(idx) => {
                let group = &item.key[..prefix.len() + idx + delimiter.len()];
                seen_prefixes.insert(group.to_owned());
            }
            None => {
                contents.push(Object {
                    key: Some(item.key),
                    size: Some(item.info.size as i64),
                    e_tag: Some(etag_to_dto(&item.info.etag)),
                    last_modified: Some(Timestamp::from(item.info.last_modified)),
                    ..Default::default()
                });
            }
        }
    }

    let prefixes = seen_prefixes
        .into_iter()
        .map(|p| CommonPrefix { prefix: Some(p) })
        .collect();
    (contents, prefixes)
}
