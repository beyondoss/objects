use std::collections::HashMap;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AccessLevel {
    Public,
    #[default]
    Private,
}

impl AccessLevel {
    pub fn as_str(self) -> &'static str {
        match self {
            AccessLevel::Public => "public",
            AccessLevel::Private => "private",
        }
    }
}

impl std::str::FromStr for AccessLevel {
    type Err = crate::StorageError;

    fn from_str(s: &str) -> crate::Result<Self> {
        match s {
            "public" => Ok(Self::Public),
            "private" => Ok(Self::Private),
            other => Err(crate::StorageError::InvalidValue(format!(
                "unknown access level: {other}"
            ))),
        }
    }
}

#[derive(Default)]
pub struct ObjectMeta {
    pub content_type: Option<String>,
    /// `None` → inherit from bucket at serve time.
    pub access: Option<AccessLevel>,
    pub user_metadata: HashMap<String, String>,
}

#[derive(Debug)]
pub struct ObjectInfo {
    pub size: u64,
    /// S3-style with surrounding quotes, e.g. `"\"d41d8cd...\""`.
    pub etag: String,
    pub last_modified: std::time::SystemTime,
    pub content_type: Option<String>,
    pub access: AccessLevel,
    pub user_metadata: HashMap<String, String>,
}

#[derive(Debug)]
pub struct BucketMeta {
    pub name: String,
    pub access: AccessLevel,
}

pub enum WriteCondition {
    /// `If-None-Match: *` — object must not exist. Returns `ObjectExists` if it does.
    IfNoneMatch,
    /// `If-Match: "<etag>"` — current etag must match. Returns `EtagMismatch` if not.
    IfMatch(String),
}

/// In-progress multipart upload.
#[derive(Debug)]
pub struct MultipartInfo {
    pub upload_id: String,
    pub bucket: String,
    pub key: String,
    pub init_time: std::time::SystemTime,
}

/// One uploaded part of an in-progress multipart upload.
#[derive(Debug, Clone)]
pub struct PartInfo {
    pub number: u32,
    /// Quoted hex MD5 of the part bytes (matches AWS's per-part ETag form).
    pub etag: String,
    pub size: u64,
    pub last_modified: std::time::SystemTime,
}

/// One completed-part assertion supplied to `complete_multipart`.
#[derive(Debug, Clone)]
pub struct CompletedPart {
    pub number: u32,
    /// Quoted hex MD5 the client received from `write_part` and is asserting.
    pub etag: String,
}
