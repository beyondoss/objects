//! Short-lived, key-scoped upload tokens for browser direct uploads.
//!
//! Format: `{exp_unix_secs}:{hmac_hex}`
//!
//! The HMAC covers `"{bucket}\n{key}\n{exp}"` keyed by `OBJECTS_ROOT_TOKEN`.
//! Bucket and key are not embedded in the token string — they are taken from the
//! request path and used to recompute the HMAC, so the token is meaningless
//! without both the root token (secret) and the intended upload destination
//! (provided by the request).
//!
//! The format is unambiguous from existing bucket tokens, which are plain 64-char
//! lowercase hex with no `:`.

use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::time::{SystemTime, UNIX_EPOCH};
use subtle::ConstantTimeEq;

type HmacSha256 = Hmac<Sha256>;

pub const MAX_TTL_SECS: u64 = 86_400;
pub const DEFAULT_TTL_SECS: u64 = 3_600;

/// Create an upload token scoped to `bucket`/`key` that expires after `ttl_secs`.
/// Returns `(token, expires_at_unix_secs)`.
pub fn create(root_token: &str, bucket: &str, key: &str, ttl_secs: u64) -> (String, u64) {
    let exp = now_secs() + ttl_secs;
    let sig = sign(root_token, bucket, key, exp);
    (format!("{exp}:{sig}"), exp)
}

/// Returns `true` iff `token` is a valid, unexpired upload token for `bucket`/`key`.
pub fn validate(root_token: &str, bucket: &str, key: &str, token: &str) -> bool {
    let Some((exp_str, presented_sig)) = token.split_once(':') else {
        return false;
    };
    let Ok(exp) = exp_str.parse::<u64>() else {
        return false;
    };
    if exp <= now_secs() {
        return false;
    }
    let expected_sig = sign(root_token, bucket, key, exp);
    bool::from(expected_sig.as_bytes().ct_eq(presented_sig.as_bytes()))
}

fn sign(root_token: &str, bucket: &str, key: &str, exp: u64) -> String {
    let mut mac =
        HmacSha256::new_from_slice(root_token.as_bytes()).expect("HMAC accepts any key length");
    mac.update(bucket.as_bytes());
    mac.update(b"\n");
    mac.update(key.as_bytes());
    mac.update(b"\n");
    mac.update(exp.to_string().as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

pub fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip() {
        let (token, exp) = create("secret", "photos", "avatar.png", 3600);
        assert!(validate("secret", "photos", "avatar.png", &token));
        assert!(exp > now_secs());
    }

    #[test]
    fn wrong_key_rejected() {
        let (token, _) = create("secret", "photos", "avatar.png", 3600);
        assert!(!validate("secret", "photos", "other.png", &token));
    }

    #[test]
    fn wrong_bucket_rejected() {
        let (token, _) = create("secret", "photos", "avatar.png", 3600);
        assert!(!validate("secret", "docs", "avatar.png", &token));
    }

    #[test]
    fn wrong_root_token_rejected() {
        let (token, _) = create("secret", "photos", "avatar.png", 3600);
        assert!(!validate("other-secret", "photos", "avatar.png", &token));
    }

    #[test]
    fn expired_token_rejected() {
        let exp = now_secs() - 1; // already expired
        let sig = sign("secret", "photos", "avatar.png", exp);
        let token = format!("{exp}:{sig}");
        assert!(!validate("secret", "photos", "avatar.png", &token));
    }

    #[test]
    fn plain_bucket_token_not_mistaken_for_upload_token() {
        // Bucket tokens have no ':' so split_once fails immediately.
        let bucket_token = "a".repeat(64);
        assert!(!validate("secret", "photos", "avatar.png", &bucket_token));
    }
}
