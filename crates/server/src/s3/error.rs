//! Map storage / index / API errors to `S3Error` codes.
//!
//! Most mappings are direct; the only judgment call is collapsing both
//! `ObjectExists` and `EtagMismatch` to `PreconditionFailed`, which is what
//! every S3 client expects for a failed conditional write.

use beyond_objects_index::IndexError;
use beyond_objects_storage::StorageError;
use s3s::{S3Error, S3ErrorCode, s3_error};

pub(super) fn from_storage(e: StorageError) -> S3Error {
    match e {
        StorageError::NotFound { bucket, key } => {
            s3_error!(NoSuchKey, "{bucket}/{key} not found")
        }
        StorageError::BucketNotFound(b) => {
            s3_error!(NoSuchBucket, "{b} not found")
        }
        StorageError::BucketNotEmpty => s3_error!(BucketNotEmpty),
        StorageError::ObjectExists { bucket, key } => {
            s3_error!(PreconditionFailed, "{bucket}/{key} already exists")
        }
        StorageError::EtagMismatch => s3_error!(PreconditionFailed, "etag mismatch"),
        StorageError::InvalidKey(k) => s3_error!(InvalidArgument, "invalid key: {k}"),
        StorageError::InvalidValue(v) => s3_error!(InvalidArgument, "invalid value: {v}"),
        StorageError::UploadNotFound(id) => s3_error!(NoSuchUpload, "upload {id} not found"),
        StorageError::InvalidPart(msg) => s3_error!(InvalidPart, "{msg}"),
        StorageError::Xattr(msg) => {
            tracing::error!(error = %msg, "xattr failure during S3 op");
            S3Error::with_message(S3ErrorCode::InternalError, "xattr failure")
        }
        StorageError::Io(e) => {
            tracing::error!(error = %e, "io failure during S3 op");
            S3Error::internal_error(e)
        }
    }
}

pub(super) fn from_api(e: crate::error::ApiError) -> S3Error {
    use crate::error::ApiError;
    match e {
        ApiError::BucketNotFound(b) => s3_error!(NoSuchBucket, "{b} not found"),
        ApiError::ObjectNotFound { bucket, key } => {
            s3_error!(NoSuchKey, "{bucket}/{key} not found")
        }
        ApiError::Unauthorized | ApiError::Forbidden => s3_error!(AccessDenied),
        ApiError::ObjectExists => s3_error!(PreconditionFailed, "object already exists"),
        ApiError::EtagMismatch => s3_error!(PreconditionFailed, "etag mismatch"),
        ApiError::BucketNotEmpty => s3_error!(BucketNotEmpty),
        ApiError::InvalidKey(k) => s3_error!(InvalidArgument, "invalid key: {k}"),
        ApiError::BadRequest(m) => s3_error!(InvalidArgument, "{m}"),
        ApiError::UploadNotFound(id) => s3_error!(NoSuchUpload, "upload {id} not found"),
        ApiError::InvalidPart(m) => s3_error!(InvalidPart, "{m}"),
        ApiError::RangeNotSatisfiable => s3_error!(InvalidRange),
        ApiError::Internal(err) => {
            tracing::error!(error = %err, "internal error during S3 op");
            S3Error::internal_error(std::io::Error::other(err.to_string()))
        }
    }
}

pub(super) fn from_index(e: IndexError) -> S3Error {
    tracing::error!(error = %e, "index failure during S3 op");
    S3Error::with_message(S3ErrorCode::InternalError, "index failure")
}
