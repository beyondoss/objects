//! Conditional-write contracts. Fail on current main (B2/B12/B18).

use std::io::Cursor;
use std::sync::Arc;

use beyond_objects_storage::{AccessLevel, ObjectMeta, Storage, StorageError, WriteCondition};
use tempfile::TempDir;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn if_none_match_exactly_one_winner() {
    for _ in 0..24 {
        let dir = TempDir::new().unwrap();
        let store = Arc::new(Storage::new(dir.path()));
        store
            .create_bucket("bkt", AccessLevel::Private)
            .await
            .unwrap();
        let mut joins = Vec::new();
        for i in 0..32 {
            let s = Arc::clone(&store);
            joins.push(tokio::spawn(async move {
                s.write_object(
                    "bkt",
                    "only-once",
                    Cursor::new(format!("w{i}").into_bytes()),
                    ObjectMeta::default(),
                    Some(WriteCondition::IfNoneMatch),
                )
                .await
            }));
        }
        let mut oks = 0u32;
        for j in joins {
            match j.await.unwrap() {
                Ok(_) => oks += 1,
                Err(StorageError::ObjectExists { .. }) => {}
                Err(e) => panic!("unexpected {e}"),
            }
        }
        assert!(oks >= 1, "at least one create must succeed");
        if oks != 1 {
            panic!("If-None-Match is create-only: got {oks} successes");
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn if_match_is_compare_and_swap() {
    let dir = TempDir::new().unwrap();
    let store = Arc::new(Storage::new(dir.path()));
    store
        .create_bucket("bkt", AccessLevel::Private)
        .await
        .unwrap();
    let (etag, _) = store
        .write_object(
            "bkt",
            "k",
            Cursor::new(b"v0".to_vec()),
            ObjectMeta::default(),
            None,
        )
        .await
        .unwrap();

    let mut joins = Vec::new();
    for i in 0..12 {
        let s = Arc::clone(&store);
        let et = etag.clone();
        joins.push(tokio::spawn(async move {
            s.write_object(
                "bkt",
                "k",
                Cursor::new(format!("v{i}").into_bytes()),
                ObjectMeta::default(),
                Some(WriteCondition::IfMatch(et)),
            )
            .await
        }));
    }
    let mut oks = 0u32;
    for j in joins {
        if j.await.unwrap().is_ok() {
            oks += 1;
        }
    }
    assert_eq!(
        oks, 1,
        "If-Match is CAS: exactly one concurrent update may succeed, got {oks}"
    );
}

#[tokio::test]
async fn if_match_star_updates_existing() {
    let dir = TempDir::new().unwrap();
    let store = Storage::new(dir.path());
    store
        .create_bucket("bkt", AccessLevel::Private)
        .await
        .unwrap();
    store
        .write_object(
            "bkt",
            "k",
            Cursor::new(b"v0".to_vec()),
            ObjectMeta::default(),
            None,
        )
        .await
        .unwrap();
    store
        .write_object(
            "bkt",
            "k",
            Cursor::new(b"v1".to_vec()),
            ObjectMeta::default(),
            Some(WriteCondition::IfMatch("\"*\"".into())),
        )
        .await
        .expect("S3 If-Match: * must succeed when the object exists");
}
