use std::collections::HashMap;
use std::path::Path;

use crate::types::AccessLevel;
use crate::{Result, StorageError};

pub const ETAG: &str = "user.etag";
pub const CONTENT_TYPE: &str = "user.content-type";
pub const ACCESS: &str = "user.access";
pub const METADATA: &str = "user.metadata";

pub fn set(path: &Path, name: &str, value: &[u8]) -> Result<()> {
    xattr::set(path, name, value).map_err(|e| StorageError::Xattr(format!("{name}: {e}")))
}

pub fn get(path: &Path, name: &str) -> Result<Option<Vec<u8>>> {
    xattr::get(path, name).map_err(|e| StorageError::Xattr(format!("{name}: {e}")))
}

pub fn set_object(
    path: &Path,
    etag: &str,
    content_type: Option<&str>,
    access: AccessLevel,
    user_metadata: &HashMap<String, String>,
) -> Result<()> {
    set(path, ETAG, etag.as_bytes())?;
    if let Some(ct) = content_type {
        set(path, CONTENT_TYPE, ct.as_bytes())?;
    }
    set(path, ACCESS, access.as_str().as_bytes())?;
    if !user_metadata.is_empty() {
        let json = serde_json::to_vec(user_metadata)
            .map_err(|e| StorageError::Xattr(format!("metadata serialize: {e}")))?;
        set(path, METADATA, &json)?;
    }
    Ok(())
}

#[allow(clippy::type_complexity)]
pub fn read_object(
    path: &Path,
) -> Result<(String, Option<String>, AccessLevel, HashMap<String, String>)> {
    let etag = get(path, ETAG)?
        .map(|b| String::from_utf8(b).map_err(|e| StorageError::Xattr(format!("etag: {e}"))))
        .transpose()?
        .unwrap_or_default();
    let content_type = get(path, CONTENT_TYPE)?
        .map(|b| {
            String::from_utf8(b).map_err(|e| StorageError::Xattr(format!("content-type: {e}")))
        })
        .transpose()?;
    let access = get(path, ACCESS)?
        .map(|b| {
            String::from_utf8(b)
                .map_err(|e| StorageError::Xattr(format!("access: {e}")))?
                .parse::<AccessLevel>()
        })
        .transpose()?
        .unwrap_or_default();
    let user_metadata = get(path, METADATA)?
        .map(|b| {
            serde_json::from_slice(&b)
                .map_err(|e| StorageError::Xattr(format!("metadata deserialize: {e}")))
        })
        .transpose()?
        .unwrap_or_default();
    Ok((etag, content_type, access, user_metadata))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("obj");
        std::fs::write(&path, b"").unwrap();

        let mut meta = HashMap::new();
        meta.insert("x-custom".to_string(), "hello".to_string());

        set_object(
            &path,
            "\"abc123\"",
            Some("image/png"),
            AccessLevel::Public,
            &meta,
        )
        .unwrap();

        let (etag, ct, access, um) = read_object(&path).unwrap();
        assert_eq!(etag, "\"abc123\"");
        assert_eq!(ct.as_deref(), Some("image/png"));
        assert_eq!(access, AccessLevel::Public);
        assert_eq!(um.get("x-custom").map(String::as_str), Some("hello"));
    }

    #[test]
    fn empty_metadata_not_written() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("obj");
        std::fs::write(&path, b"").unwrap();

        set_object(
            &path,
            "\"etag\"",
            None,
            AccessLevel::Private,
            &HashMap::new(),
        )
        .unwrap();

        let (_, ct, access, um) = read_object(&path).unwrap();
        assert!(ct.is_none());
        assert_eq!(access, AccessLevel::Private);
        assert!(um.is_empty());
    }

    #[test]
    fn unknown_access_level_errors() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("obj");
        std::fs::write(&path, b"").unwrap();
        set(&path, ACCESS, b"readwrite").unwrap();
        assert!(read_object(&path).is_err());
    }
}
