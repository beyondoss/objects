#[derive(Debug, thiserror::Error)]
#[must_use = "errors must be handled or explicitly ignored with `let _ =`"]
pub enum StorageError {
    #[error("object not found: {bucket}/{key}")]
    NotFound { bucket: String, key: String },

    #[error("bucket not found: {0}")]
    BucketNotFound(String),

    #[error("bucket not empty")]
    BucketNotEmpty,

    #[error("object already exists: {bucket}/{key}")]
    ObjectExists { bucket: String, key: String },

    #[error("etag mismatch")]
    EtagMismatch,

    #[error("invalid key: {0}")]
    InvalidKey(String),

    #[error("invalid value: {0}")]
    InvalidValue(String),

    #[error("xattr: {0}")]
    Xattr(String),

    #[error(transparent)]
    Io(#[from] std::io::Error),
}
