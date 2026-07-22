use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use chrono::Utc;

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

fn decode_number(encoded: &str) -> u64 {
    encoded.chars().fold(0, |number, character| {
        let digit = "QBWSPHDXZU"
            .find(character)
            .expect("encoded test number should use the protocol alphabet");
        number * 10 + u64::try_from(digit).expect("protocol digit should fit u64")
    })
}

#[test]
fn token_shape_preserves_the_secret_and_embeds_a_current_timestamp() {
    let secret = "abcdefghijk=";
    let trimmed_secret = secret.trim_end_matches('=');
    let earliest_timestamp = Utc::now().timestamp_millis();
    let token = connection_token(secret).expect("valid secret should create token");
    let latest_timestamp = Utc::now().timestamp_millis();

    let split = usize::try_from(decode_number(&token[..3])).expect("split should fit usize");
    let timestamp_length =
        usize::try_from(decode_number(&token[3..5])).expect("length should fit usize");
    assert!((2..trimmed_secret.len()).contains(&split));
    assert_eq!(token.len(), 5 + trimmed_secret.len() + timestamp_length);

    let prefix_end = 5 + split;
    let timestamp_end = prefix_end + timestamp_length;
    assert_eq!(&token[5..prefix_end], &trimmed_secret[..split]);
    assert_eq!(&token[timestamp_end..], &trimmed_secret[split..]);
    assert!(!token.contains('='));

    let timestamp = i64::try_from(decode_number(&token[prefix_end..timestamp_end]))
        .expect("timestamp should fit i64");
    assert!((earliest_timestamp..=latest_timestamp).contains(&timestamp));
}

#[test]
fn streaming_endpoint_handles_root_nested_and_trailing_slash_base_urls() {
    for (base, expected_path) in [
        ("wss://example.com", "/streaming"),
        ("wss://example.com/", "/streaming"),
        ("wss://example.com/proxy", "/proxy/streaming"),
        ("wss://example.com/proxy/", "/proxy/streaming"),
    ] {
        let options = FbOptionsBuilder::new("abcdefghijk")
            .streaming_url(base)
            .build()
            .expect("options should build");
        let url = streaming_url(&options).expect("URL should build");
        assert_eq!(url.path(), expected_path, "base URL: {base}");
        assert_eq!(
            url.query_pairs()
                .find(|(key, _)| key == "type")
                .map(|(_, value)| value),
            Some("server".into()),
            "base URL: {base}"
        );
        assert!(
            url.query_pairs()
                .find(|(key, _)| key == "token")
                .is_some_and(|(_, value)| !value.is_empty()),
            "base URL: {base}"
        );
    }
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

#[test]
fn unknown_messages_are_ignored_and_extra_fields_remain_forward_compatible() {
    let store = SnapshotStore::new();
    let status = StatusTracker::new(SyncStatus::Starting, false);
    let initial = br#"{"messageType":"data-sync","data":{"eventType":"full","featureFlags":[{"key":"kept","updatedAt":1}],"segments":[]}}"#;
    assert_eq!(apply_message(&store, &status, initial), ApplyResult::Full);

    let unknown_message = br#"{"messageType":"future-message","data":{"eventType":"full","featureFlags":[{"key":"replaced","updatedAt":2}],"segments":[]}}"#;
    assert_eq!(
        apply_message(&store, &status, unknown_message),
        ApplyResult::Ignored
    );
    let unknown_event = br#"{"messageType":"data-sync","data":{"eventType":"future-update","featureFlags":[{"key":"replaced","updatedAt":2}],"segments":[]}}"#;
    assert_eq!(
        apply_message(&store, &status, unknown_event),
        ApplyResult::Ignored
    );
    assert!(store.load().flags.contains_key("kept"));
    assert!(!store.load().flags.contains_key("replaced"));
    assert_eq!(store.version(), 1);

    let extra_fields = br#"{"futureEnvelopeField":true,"messageType":"data-sync","data":{"futureDataField":{"nested":true},"eventType":"patch","featureFlags":[{"key":"kept","updatedAt":2,"futureFlagField":"accepted"}],"segments":[]}}"#;
    assert_eq!(
        apply_message(&store, &status, extra_fields),
        ApplyResult::Patch
    );
    assert_eq!(store.version(), 2);
}

#[test]
fn an_empty_full_message_initializes_an_empty_store() {
    let store = SnapshotStore::new();
    let status = StatusTracker::new(SyncStatus::Starting, false);
    let empty_full =
        br#"{"messageType":"data-sync","data":{"eventType":"full","featureFlags":[],"segments":[]}}"#;

    assert_eq!(
        apply_message(&store, &status, empty_full),
        ApplyResult::Full
    );
    assert!(store.load().populated);
    assert_eq!(store.version(), 0);
    assert!(status.initialized());
    assert_eq!(status.status(), SyncStatus::Ready);
}
