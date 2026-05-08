//! `S3Access` impl: enforce per-bucket scoping on signed requests, allow
//! anonymous reads through to the operation handler so it can apply the
//! per-object public/private check.
//!
//! Authorization rules:
//!
//! - **Signed request** (`access_key` present): require
//!   `access_key == bucket || access_key == "root"`.
//! - **Anonymous request** (no credentials): allow only object reads
//!   (`GetObject`, `HeadObject`) and `ListObjectsV2`. The handler then
//!   refuses if the object's effective access is `private`.
//!
//! Cohesive with `middleware/auth.rs::verify`: same root-or-bucket rule, just
//! applied through s3s's auth-context surface instead of an axum middleware.

use async_trait::async_trait;
use s3s::S3Result;
use s3s::access::{S3Access, S3AccessContext};
use s3s::s3_error;

const ROOT_KEY_ID: &str = "root";

const ANONYMOUS_OPS: &[&str] = &["GetObject", "HeadObject", "ListObjectsV2"];

#[derive(Clone, Default)]
pub struct BucketScopedAccess;

#[async_trait]
impl S3Access for BucketScopedAccess {
    async fn check(&self, cx: &mut S3AccessContext<'_>) -> S3Result<()> {
        let op_name = cx.s3_op().name();
        let target_bucket = cx.s3_path().get_bucket_name();

        match cx.credentials() {
            Some(creds) => {
                if creds.access_key == ROOT_KEY_ID {
                    return Ok(());
                }
                match target_bucket {
                    Some(bucket) if creds.access_key == bucket => Ok(()),
                    Some(bucket) => Err(s3_error!(
                        AccessDenied,
                        "access key `{}` cannot access bucket `{}`",
                        creds.access_key,
                        bucket
                    )),
                    None => {
                        // Operations on the root path — only `root` is allowed.
                        Err(s3_error!(
                            AccessDenied,
                            "access key `{}` is not authorized for `{op_name}`",
                            creds.access_key
                        ))
                    }
                }
            }
            None => {
                if ANONYMOUS_OPS.contains(&op_name) {
                    Ok(())
                } else {
                    Err(s3_error!(
                        AccessDenied,
                        "anonymous requests are not allowed for `{op_name}`"
                    ))
                }
            }
        }
    }
}
