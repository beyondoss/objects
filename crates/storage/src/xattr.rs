use std::collections::HashMap;
use std::ffi::CString;
use std::io;
use std::os::unix::io::RawFd;
use std::path::Path;

use crate::types::AccessLevel;
use crate::{Result, StorageError};

pub(crate) struct ObjectAttrs {
    pub etag: String,
    pub content_type: Option<String>,
    /// `None` when the object has no explicit access xattr — caller resolves by
    /// falling back to the bucket-level default.
    pub access: Option<AccessLevel>,
    pub user_metadata: HashMap<String, String>,
}

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
    access: Option<AccessLevel>,
    user_metadata: &HashMap<String, String>,
) -> Result<()> {
    set(path, ETAG, etag.as_bytes())?;
    if let Some(ct) = content_type {
        set(path, CONTENT_TYPE, ct.as_bytes())?;
    }
    if let Some(a) = access {
        set(path, ACCESS, a.as_str().as_bytes())?;
    }
    if !user_metadata.is_empty() {
        let json = serde_json::to_vec(user_metadata)
            .map_err(|e| StorageError::Xattr(format!("metadata serialize: {e}")))?;
        set(path, METADATA, &json)?;
    }
    Ok(())
}

pub fn read_object(path: &Path) -> Result<ObjectAttrs> {
    let etag = get(path, ETAG)?
        .map(|b| String::from_utf8(b).map_err(|e| StorageError::Xattr(format!("etag: {e}"))))
        .transpose()?
        .unwrap_or_default();
    let content_type = get(path, CONTENT_TYPE)?
        .map(|b| {
            String::from_utf8(b).map_err(|e| StorageError::Xattr(format!("content-type: {e}")))
        })
        .transpose()?;
    let access = read_access(path)?;
    let user_metadata = get(path, METADATA)?
        .map(|b| {
            serde_json::from_slice(&b)
                .map_err(|e| StorageError::Xattr(format!("metadata deserialize: {e}")))
        })
        .transpose()?
        .unwrap_or_default();
    Ok(ObjectAttrs {
        etag,
        content_type,
        access,
        user_metadata,
    })
}

/// Read all object xattrs from an open file descriptor.
///
/// Uses `fgetxattr` so the VFS name lookup done by `open()` is not repeated.
/// On network filesystems each path-based xattr call costs a round-trip;
/// fd-based reads operate on the already-resolved inode.
pub fn read_object_fd(fd: RawFd) -> Result<ObjectAttrs> {
    let etag = fgetxattr_str(fd, ETAG)?
        .map(|s| {
            if s.is_empty() {
                Err(StorageError::Xattr("etag xattr is empty".into()))
            } else {
                Ok(s)
            }
        })
        .transpose()?
        .unwrap_or_default();
    let content_type = fgetxattr_str(fd, CONTENT_TYPE)?;
    let access = fgetxattr_str(fd, ACCESS)?
        .map(|s| s.parse::<AccessLevel>())
        .transpose()?;
    let user_metadata = fgetxattr_str(fd, METADATA)?
        .map(|s| {
            serde_json::from_str(&s)
                .map_err(|e| StorageError::Xattr(format!("metadata deserialize: {e}")))
        })
        .transpose()?
        .unwrap_or_default();
    Ok(ObjectAttrs {
        etag,
        content_type,
        access,
        user_metadata,
    })
}

fn fgetxattr_str(fd: RawFd, name: &str) -> Result<Option<String>> {
    fgetxattr_bytes(fd, name)?
        .map(|b| {
            String::from_utf8(b)
                .map_err(|e| StorageError::Xattr(format!("{name}: invalid utf-8: {e}")))
        })
        .transpose()
}

fn fgetxattr_bytes(fd: RawFd, name: &str) -> Result<Option<Vec<u8>>> {
    let cname = CString::new(name).expect("xattr name has no null bytes");
    let mut buf = vec![0u8; 256];
    loop {
        let n = unsafe { fgetxattr_sys(fd, cname.as_ptr(), buf.as_mut_ptr().cast(), buf.len()) };
        if n >= 0 {
            buf.truncate(n as usize);
            return Ok(Some(buf));
        }
        let raw = io::Error::last_os_error().raw_os_error().unwrap_or(0);
        if is_no_attr(raw) {
            return Ok(None);
        }
        if raw == libc::ERANGE {
            // Buffer too small — probe for the required size and retry.
            let needed = unsafe { fgetxattr_sys(fd, cname.as_ptr(), std::ptr::null_mut(), 0) };
            if needed <= 0 {
                let raw2 = io::Error::last_os_error().raw_os_error().unwrap_or(0);
                if is_no_attr(raw2) {
                    return Ok(None);
                }
                return Err(StorageError::Xattr(format!(
                    "fgetxattr({name}): {}",
                    io::Error::from_raw_os_error(raw2)
                )));
            }
            buf = vec![0u8; needed as usize];
            continue;
        }
        return Err(StorageError::Xattr(format!(
            "fgetxattr({name}): {}",
            io::Error::from_raw_os_error(raw)
        )));
    }
}

#[cfg(target_os = "linux")]
unsafe fn fgetxattr_sys(
    fd: RawFd,
    name: *const libc::c_char,
    buf: *mut libc::c_void,
    size: usize,
) -> libc::ssize_t {
    unsafe { libc::fgetxattr(fd, name, buf, size) }
}

#[cfg(target_os = "macos")]
unsafe fn fgetxattr_sys(
    fd: RawFd,
    name: *const libc::c_char,
    buf: *mut libc::c_void,
    size: usize,
) -> libc::ssize_t {
    unsafe { libc::fgetxattr(fd, name, buf, size, 0, 0) }
}

#[cfg(target_os = "linux")]
fn is_no_attr(errno: i32) -> bool {
    errno == libc::ENODATA
}

#[cfg(target_os = "macos")]
fn is_no_attr(errno: i32) -> bool {
    errno == libc::ENOATTR
}

/// Read the access xattr at `path` (object or bucket directory). Returns `None`
/// when the xattr is absent.
pub fn read_access(path: &Path) -> Result<Option<AccessLevel>> {
    get(path, ACCESS)?
        .map(|b| {
            String::from_utf8(b)
                .map_err(|e| StorageError::Xattr(format!("access: {e}")))?
                .parse::<AccessLevel>()
        })
        .transpose()
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
            Some(AccessLevel::Public),
            &meta,
        )
        .unwrap();

        let attrs = read_object(&path).unwrap();
        assert_eq!(attrs.etag, "\"abc123\"");
        assert_eq!(attrs.content_type.as_deref(), Some("image/png"));
        assert_eq!(attrs.access, Some(AccessLevel::Public));
        assert_eq!(
            attrs.user_metadata.get("x-custom").map(String::as_str),
            Some("hello")
        );
    }

    #[test]
    fn absent_access_xattr() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("obj");
        std::fs::write(&path, b"").unwrap();

        set_object(&path, "\"etag\"", None, None, &HashMap::new()).unwrap();

        let attrs = read_object(&path).unwrap();
        assert!(attrs.content_type.is_none());
        assert_eq!(attrs.access, None);
        assert!(attrs.user_metadata.is_empty());
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
