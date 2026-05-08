//! `S3Auth` impl: vend secret keys derived from our HMAC scheme so AWS SDKs
//! sign with the same string a REST client puts in `Authorization: Bearer …`.
//!
//! Mapping (cohesive with `middleware/auth.rs`):
//!
//! ```text
//! access_key_id == "root"   → secret = OBJECTS_ROOT_TOKEN
//! access_key_id == bucket   → secret = HMAC-SHA256(OBJECTS_ROOT_TOKEN, bucket)
//! ```
//!
//! The bucket-name derivation is identical to the bearer token a /v1 caller
//! uses; AWS SDKs then run SigV4 with that secret and `s3s` verifies it.
//! Authorization (does this access_key match the bucket on the URL?) lives
//! in `s3/access.rs`.

use std::sync::Arc;

use async_trait::async_trait;
use hmac::{Hmac, Mac};
use s3s::S3Result;
use s3s::auth::{S3Auth, SecretKey};
use s3s::s3_error;
use secrecy::{ExposeSecret, Secret};
use sha2::Sha256;

const ROOT_KEY_ID: &str = "root";

#[derive(Clone)]
pub struct HmacAuth {
    root_token: Arc<Secret<String>>,
}

impl HmacAuth {
    pub fn new(root_token: Secret<String>) -> Self {
        Self {
            root_token: Arc::new(root_token),
        }
    }
}

#[async_trait]
impl S3Auth for HmacAuth {
    async fn get_secret_key(&self, access_key: &str) -> S3Result<SecretKey> {
        let root = self.root_token.expose_secret();
        if access_key == ROOT_KEY_ID {
            return Ok(SecretKey::from(root.as_str()));
        }
        // Treat the access_key as a bucket name and recompute the bearer
        // token. We don't validate that the bucket exists here — that's an
        // authorization concern (handled in S3Access) or a "not found"
        // surfaced by the operation handler.
        let mut mac = Hmac::<Sha256>::new_from_slice(root.as_bytes())
            .map_err(|_| s3_error!(InvalidAccessKeyId, "root token is invalid"))?;
        mac.update(access_key.as_bytes());
        let derived = hex::encode(mac.finalize().into_bytes());
        Ok(SecretKey::from(derived))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn root_returns_root_token() {
        let auth = HmacAuth::new(Secret::new("rootsecret".into()));
        let secret = auth.get_secret_key("root").await.unwrap();
        assert_eq!(secret.expose(), "rootsecret");
    }

    #[tokio::test]
    async fn bucket_returns_hmac_derived_secret() {
        let auth = HmacAuth::new(Secret::new("rootsecret".into()));
        let secret = auth.get_secret_key("images").await.unwrap();

        let mut mac = Hmac::<Sha256>::new_from_slice(b"rootsecret").unwrap();
        mac.update(b"images");
        let expected = hex::encode(mac.finalize().into_bytes());
        assert_eq!(secret.expose(), expected);
    }
}
