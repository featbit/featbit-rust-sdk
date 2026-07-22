use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use super::super::{
    apply_message, connection_token, encode_number, streaming_url, ApplyResult, StatusTracker,
    SyncStatus,
};
use crate::options::FbOptionsBuilder;
use crate::store::SnapshotStore;

#[test]
fn encoded_numbers_match_protocol_map() {
    assert_eq!(encode_number(2, 3), "QQW");
    assert_eq!(encode_number(13, 2), "BS");
    assert_eq!(encode_number(987, 3), "UZX");
}

#[test]
fn token_contains_secret_without_padding() {
    let token = connection_token("abcdefghijk=").expect("valid secret should create token");
    assert!(token.contains("ab"));
    assert!(!token.contains('='));
    assert!(token.len() > "abcdefghijk".len());
}

#[test]
fn streaming_endpoint_preserves_base_path_and_encodes_token() {
    let options = FbOptionsBuilder::new("abcdefghijk")
        .streaming_url("wss://example.com/proxy/")
        .build()
        .expect("options should build");
    let url = streaming_url(&options).expect("URL should build");
    assert_eq!(url.path(), "/proxy/streaming");
    assert_eq!(
        url.query_pairs()
            .find(|(key, _)| key == "type")
            .map(|(_, value)| value),
        Some("server".into())
    );
}

#[test]
fn full_and_newer_patch_update_store() {
    let store = SnapshotStore::new();
    let status = StatusTracker::new(SyncStatus::Starting, false);
    let full = br#"{"messageType":"data-sync","data":{"eventType":"full","featureFlags":[{"key":"flag","updatedAt":1,"variations":[]}],"segments":[]}}"#;
    assert_eq!(apply_message(&store, &status, full), ApplyResult::Full);
    assert!(status.initialized());
    assert_eq!(store.version(), 1);

    let patch = br#"{"messageType":"data-sync","data":{"eventType":"patch","featureFlags":[{"key":"flag","updatedAt":2,"isArchived":true}],"segments":[]}}"#;
    assert_eq!(apply_message(&store, &status, patch), ApplyResult::Patch);
    assert_eq!(store.version(), 2);
    assert!(store.load().flags["flag"].is_archived);

    let conflict = br#"{"messageType":"data-sync","data":{"eventType":"patch","featureFlags":[{"key":"flag","updatedAt":2,"isArchived":false}],"segments":[]}}"#;
    assert_eq!(
        apply_message(&store, &status, conflict),
        ApplyResult::VersionConflict
    );
    assert_eq!(status.status(), SyncStatus::Stale);
    assert!(store.load().flags["flag"].is_archived);

    let stale = br#"{"messageType":"data-sync","data":{"eventType":"patch","featureFlags":[{"key":"flag","updatedAt":1,"isArchived":false}],"segments":[]}}"#;
    assert_eq!(apply_message(&store, &status, stale), ApplyResult::Patch);
    assert_eq!(status.status(), SyncStatus::Ready);
    assert!(store.load().flags["flag"].is_archived);
}

#[test]
fn initialization_wait_handles_ready_and_terminal_notifications() {
    for _ in 0..100 {
        let status = Arc::new(StatusTracker::new(SyncStatus::Starting, false));
        let setter_status = Arc::clone(&status);
        let setter = thread::spawn(move || setter_status.set(SyncStatus::Ready));
        assert!(status.wait_until_initialized(Duration::from_secs(1)));
        setter.join().expect("status setter should finish");
    }

    let terminal = StatusTracker::new(SyncStatus::Starting, false);
    terminal.set(SyncStatus::Closed);
    let started = Instant::now();
    assert!(!terminal.wait_until_initialized(Duration::from_secs(30)));
    assert!(started.elapsed() < Duration::from_secs(1));
}
