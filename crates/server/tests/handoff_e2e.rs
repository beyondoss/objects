//! Happy-path end-to-end handoff scenarios. Boots a real `beyond-objects`
//! binary (or two, across a real handoff) and uses real HTTP clients via the
//! shared harness.
//!
//! New scenarios should be one method call on `Harness` plus assertions.

mod handoff_harness;

use handoff_harness::{Harness, TEST_BUCKET, http_get, http_put};

/// **The load-bearing claim:** an object written before a handoff is readable
/// after the handoff, on the same TCP port, served by a different process.
///
/// Exercises:
/// - `Role::ColdStart` with `LISTEN_FDS` inheritance.
/// - One full Hello→Commit protocol on the real `Incumbent::serve` thread.
/// - `Index::persist` writing a real on-disk flush (defensive).
/// - `Role::Successor` opening the data dir, acquiring the (just-released)
///   flock, and serving the inherited listener.
/// - Writes work on the new process too (proves it didn't open read-only).
#[test]
fn data_survives_handoff() {
    let mut h = Harness::new();
    h.cold_start();

    let token = h.bucket_token(TEST_BUCKET);

    let status = http_put(
        h.http_addr(),
        &token,
        TEST_BUCKET,
        "survive-key",
        b"survive-value",
    );
    assert!((200..300).contains(&status), "PUT pre-handoff: {status}");
    let got = http_get(h.http_addr(), &token, TEST_BUCKET, "survive-key");
    assert_eq!(got.as_deref(), Some(&b"survive-value"[..]));

    let summary = h.handoff();
    assert!(summary.committed, "handoff must commit: {summary:?}");

    let got = http_get(h.http_addr(), &token, TEST_BUCKET, "survive-key");
    assert_eq!(
        got.as_deref(),
        Some(&b"survive-value"[..]),
        "value written to old must be readable on new"
    );

    // Write on new — proves the successor is fully online, not read-only.
    let status = http_put(
        h.http_addr(),
        &token,
        TEST_BUCKET,
        "post-key",
        b"post-value",
    );
    assert!((200..300).contains(&status), "PUT post-handoff: {status}");
    let got = http_get(h.http_addr(), &token, TEST_BUCKET, "post-key");
    assert_eq!(got.as_deref(), Some(&b"post-value"[..]));
}

/// Two handoffs in a row should both commit and preserve data through each.
/// Proves the flock dance is repeatable, not just a one-shot fluke.
#[test]
fn back_to_back_handoffs() {
    let mut h = Harness::new();
    h.cold_start();

    let token = h.bucket_token(TEST_BUCKET);

    let _ = http_put(h.http_addr(), &token, TEST_BUCKET, "v1-key", b"v1-value");

    let s1 = h.handoff();
    assert!(s1.committed, "first handoff: {s1:?}");

    let _ = http_put(h.http_addr(), &token, TEST_BUCKET, "v2-key", b"v2-value");

    let s2 = h.handoff();
    assert!(s2.committed, "second handoff: {s2:?}");

    // Both objects must be present on the post-second-handoff process.
    let v1 = http_get(h.http_addr(), &token, TEST_BUCKET, "v1-key");
    let v2 = http_get(h.http_addr(), &token, TEST_BUCKET, "v2-key");
    assert_eq!(v1.as_deref(), Some(&b"v1-value"[..]));
    assert_eq!(v2.as_deref(), Some(&b"v2-value"[..]));
}
