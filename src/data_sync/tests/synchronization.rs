use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use futures_util::{SinkExt, StreamExt};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::time;
use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};
use tokio_tungstenite::tungstenite::protocol::{frame::coding::CloseCode, CloseFrame, Message};
use tokio_tungstenite::{accept_async, accept_hdr_async};

use super::super::{StatusTracker, SyncStatus, WebSocketDataSynchronizer};
use super::support::{accept_test_socket, next_test_text, serve_until_client_close};
use crate::client::FbClient;
use crate::model::{DataSet, FbUser, FeatureFlag};
use crate::options::FbOptionsBuilder;
use crate::store::SnapshotStore;
use crate::worker::WorkerWait;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn client_synchronizes_full_and_patch_over_websocket() {
    const FULL: &str = r#"{"messageType":"data-sync","data":{"eventType":"full","featureFlags":[{"id":"flag-id","key":"live-flag","updatedAt":1,"variationType":"boolean","variations":[{"id":"value","value":"true"}],"targetUsers":[],"rules":[],"isEnabled":true,"fallthrough":{"variations":[{"id":"value","rollout":[0,1],"exptRollout":0}]}}],"segments":[]}}"#;
    const PATCH: &str = r#"{"messageType":"data-sync","data":{"eventType":"patch","featureFlags":[{"id":"flag-id","key":"live-flag","updatedAt":2,"variationType":"boolean","variations":[{"id":"value","value":"false"}],"targetUsers":[],"rules":[],"isEnabled":true,"fallthrough":{"variations":[{"id":"value","rollout":[0,1],"exptRollout":0}]}}],"segments":[]}}"#;

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("test listener should bind");
    let address = listener
        .local_addr()
        .expect("listener should have an address");
    let (patch_sender, patch_receiver) = oneshot::channel::<()>();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("client should connect");
        let mut socket = accept_async(stream)
            .await
            .expect("WebSocket handshake should succeed");
        let request = socket
            .next()
            .await
            .expect("client should request data")
            .expect("data request should be valid");
        let request_text = request.into_text().expect("request should be text");
        assert!(request_text.contains("\"messageType\":\"data-sync\""));
        assert!(request_text.contains("\"timestamp\":0"));
        socket
            .send(Message::Text(FULL.into()))
            .await
            .expect("full data should send");

        patch_receiver.await.expect("test should request patch");
        socket
            .send(Message::Text(PATCH.into()))
            .await
            .expect("patch data should send");

        while let Some(message) = socket.next().await {
            match message.expect("client message should be valid") {
                Message::Close(_) => {
                    let _ignored = socket.close(None).await;
                    break;
                }
                Message::Ping(payload) => {
                    socket
                        .send(Message::Pong(payload))
                        .await
                        .expect("pong should send");
                }
                _ => {}
            }
        }
    });

    let options = FbOptionsBuilder::new("valid-environment-secret")
        .streaming_url(format!("ws://{address}"))
        .disable_events(true, false)
        .start_wait(Duration::from_secs(2))
        .connect_timeout(Duration::from_secs(1))
        .close_timeout(Duration::from_secs(1))
        .keep_alive_interval(Duration::from_mins(1))
        .build()
        .expect("options should build");
    let client = tokio::task::spawn_blocking(move || FbClient::with_options(options))
        .await
        .expect("client construction task should finish");
    let user = FbUser::builder("user-1").build();
    assert!(client.bool_variation("live-flag", &user, false));

    patch_sender.send(()).expect("server should still run");
    let mut patched = false;
    for _ in 0..50 {
        if !client.bool_variation("live-flag", &user, true) {
            patched = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(patched, "patch should become visible to evaluation");

    tokio::task::spawn_blocking(move || client.close())
        .await
        .expect("client close task should finish");
    server.await.expect("test server should stop");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn online_client_with_active_workers_closes_idempotently_from_multiple_threads() {
    const EMPTY_FULL: &str = r#"{"messageType":"data-sync","data":{"eventType":"full","featureFlags":[],"segments":[]}}"#;
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("test listener should bind");
    let address = listener
        .local_addr()
        .expect("listener should have an address");
    let server = tokio::spawn(async move {
        let mut socket = accept_test_socket(&listener).await;
        assert!(next_test_text(&mut socket)
            .await
            .contains("\"timestamp\":0"));
        socket
            .send(Message::Text(EMPTY_FULL.into()))
            .await
            .expect("empty full data should send");
        serve_until_client_close(&mut socket).await;
    });

    let options = FbOptionsBuilder::new("valid-environment-secret")
        .streaming_url(format!("ws://{address}"))
        .start_wait(Duration::from_secs(2))
        .connect_timeout(Duration::from_secs(1))
        .close_timeout(Duration::from_secs(1))
        .auto_flush_interval(Duration::from_mins(1))
        .keep_alive_interval(Duration::from_mins(1))
        .build()
        .expect("options should build");
    let client = tokio::task::spawn_blocking(move || FbClient::with_options(options))
        .await
        .expect("client construction task should finish");
    assert_eq!(client.status(), crate::ClientStatus::Ready);

    let barrier = Arc::new(Barrier::new(5));
    let closers = (0..4)
        .map(|_| {
            let client = client.clone();
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                barrier.wait();
                client.close();
            })
        })
        .collect::<Vec<_>>();
    barrier.wait();
    for closer in closers {
        closer.join().expect("client closer should finish");
    }

    assert_eq!(client.status(), crate::ClientStatus::Closed);
    assert!(!client.flush_and_wait(Duration::from_millis(10)));
    server.await.expect("test server should stop");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn websocket_handshake_rejection_retries_and_recovers() {
    const EMPTY_FULL: &str = r#"{"messageType":"data-sync","data":{"eventType":"full","featureFlags":[],"segments":[]}}"#;
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("test listener should bind");
    let address = listener
        .local_addr()
        .expect("listener should have an address");
    let server = tokio::spawn(async move {
        let (mut rejected, _peer) = listener
            .accept()
            .await
            .expect("first handshake should connect");
        let mut request = [0_u8; 2_048];
        let read = rejected
            .read(&mut request)
            .await
            .expect("handshake request should be readable");
        assert!(request[..read].starts_with(b"GET "));
        rejected
            .write_all(
                b"HTTP/1.1 401 Unauthorized\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
            )
            .await
            .expect("rejection should write");
        rejected.shutdown().await.expect("rejection should close");

        let mut socket = accept_test_socket(&listener).await;
        assert!(next_test_text(&mut socket)
            .await
            .contains("\"timestamp\":0"));
        socket
            .send(Message::Text(EMPTY_FULL.into()))
            .await
            .expect("recovery full data should send");
        serve_until_client_close(&mut socket).await;
    });

    let options = FbOptionsBuilder::new("valid-environment-secret")
        .streaming_url(format!("ws://{address}"))
        .disable_events(true, false)
        .start_wait(Duration::from_secs(2))
        .connect_timeout(Duration::from_secs(1))
        .close_timeout(Duration::from_secs(1))
        .reconnect_delays([Duration::from_millis(1)])
        .build()
        .expect("options should build");
    let client = tokio::task::spawn_blocking(move || FbClient::with_options(options))
        .await
        .expect("client construction task should finish");

    assert!(client.initialized());
    assert_eq!(client.status(), crate::ClientStatus::Ready);
    client.close();
    server.await.expect("test server should stop");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn initial_sync_uses_the_version_of_a_prepopulated_store() {
    const NEWER_FULL: &str = r#"{"messageType":"data-sync","data":{"eventType":"full","featureFlags":[{"key":"newer","updatedAt":38}],"segments":[]}}"#;
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("test listener should bind");
    let address = listener
        .local_addr()
        .expect("listener should have an address");
    let server = tokio::spawn(async move {
        let mut socket = accept_test_socket(&listener).await;
        let request = next_test_text(&mut socket).await;
        assert!(request.contains("\"timestamp\":37"), "request: {request}");
        socket
            .send(Message::Text(NEWER_FULL.into()))
            .await
            .expect("newer full data should send");
        serve_until_client_close(&mut socket).await;
    });

    let store = Arc::new(SnapshotStore::new());
    store.populate(&DataSet {
        event_type: "full".to_owned(),
        feature_flags: vec![FeatureFlag {
            key: "cached".to_owned(),
            updated_at: 37,
            ..FeatureFlag::default()
        }],
        ..DataSet::default()
    });
    let status = Arc::new(StatusTracker::new(SyncStatus::Starting, true));
    let options = FbOptionsBuilder::new("valid-environment-secret")
        .streaming_url(format!("ws://{address}"))
        .disable_events(true, false)
        .start_wait(Duration::from_secs(2))
        .connect_timeout(Duration::from_secs(1))
        .close_timeout(Duration::from_secs(1))
        .build()
        .expect("options should build");
    let synchronizer =
        WebSocketDataSynchronizer::start(options, Arc::clone(&store), Arc::clone(&status))
            .expect("synchronizer should start");

    for _ in 0..100 {
        if store.version() == 38 && status.status() == SyncStatus::Ready {
            break;
        }
        time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(store.version(), 38);
    assert_eq!(status.status(), SyncStatus::Ready);
    tokio::task::spawn_blocking(move || synchronizer.close())
        .await
        .expect("synchronizer close task should finish");
    server.await.expect("test server should stop");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn equal_version_conflict_requests_an_authoritative_full_snapshot() {
    const INITIAL: &str = r#"{"messageType":"data-sync","data":{"eventType":"full","featureFlags":[{"id":"flag-id","key":"collision-flag","updatedAt":5,"variationType":"boolean","variations":[{"id":"value","value":"true"}],"targetUsers":[],"rules":[],"isEnabled":true,"fallthrough":{"variations":[{"id":"value","rollout":[0,1],"exptRollout":0}]}}],"segments":[]}}"#;
    const FIRST_PATCH: &str = r#"{"messageType":"data-sync","data":{"eventType":"patch","featureFlags":[{"id":"flag-id","key":"collision-flag","updatedAt":6,"variationType":"boolean","variations":[{"id":"value","value":"false"}],"targetUsers":[],"rules":[],"isEnabled":true,"fallthrough":{"variations":[{"id":"value","rollout":[0,1],"exptRollout":0}]}}],"segments":[]}}"#;
    const CONFLICTING_PATCH: &str = r#"{"messageType":"data-sync","data":{"eventType":"patch","featureFlags":[{"id":"flag-id","key":"collision-flag","updatedAt":6,"variationType":"boolean","variations":[{"id":"value","value":"true"}],"targetUsers":[],"rules":[],"isEnabled":true,"fallthrough":{"variations":[{"id":"value","rollout":[0,1],"exptRollout":0}]}}],"segments":[]}}"#;
    const AUTHORITATIVE_FULL: &str = r#"{"messageType":"data-sync","data":{"eventType":"full","featureFlags":[{"id":"flag-id","key":"collision-flag","updatedAt":6,"variationType":"boolean","variations":[{"id":"value","value":"true"}],"targetUsers":[],"rules":[],"isEnabled":true,"fallthrough":{"variations":[{"id":"value","rollout":[0,1],"exptRollout":0}]}}],"segments":[]}}"#;

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("test listener should bind");
    let address = listener
        .local_addr()
        .expect("listener should have an address");
    let (first_patch_sender, first_patch_receiver) = oneshot::channel::<()>();
    let (conflict_sender, conflict_receiver) = oneshot::channel::<()>();
    let server = tokio::spawn(async move {
        let mut socket = accept_test_socket(&listener).await;
        assert!(next_test_text(&mut socket)
            .await
            .contains("\"timestamp\":0"));
        socket
            .send(Message::Text(INITIAL.into()))
            .await
            .expect("initial full data should send");

        first_patch_receiver
            .await
            .expect("first patch should be requested");
        socket
            .send(Message::Text(FIRST_PATCH.into()))
            .await
            .expect("first patch should send");

        conflict_receiver
            .await
            .expect("conflicting patch should be requested");
        socket
            .send(Message::Text(CONFLICTING_PATCH.into()))
            .await
            .expect("conflicting patch should send");
        let resync = time::timeout(Duration::from_secs(2), next_test_text(&mut socket))
            .await
            .expect("version conflict should request another data sync");
        assert!(resync.contains("\"timestamp\":0"));
        socket
            .send(Message::Text(AUTHORITATIVE_FULL.into()))
            .await
            .expect("authoritative full data should send");
        serve_until_client_close(&mut socket).await;
    });

    let options = FbOptionsBuilder::new("valid-environment-secret")
        .streaming_url(format!("ws://{address}"))
        .disable_events(true, false)
        .start_wait(Duration::from_secs(2))
        .connect_timeout(Duration::from_secs(1))
        .close_timeout(Duration::from_secs(1))
        .keep_alive_interval(Duration::from_mins(1))
        .build()
        .expect("options should build");
    let client = tokio::task::spawn_blocking(move || FbClient::with_options(options))
        .await
        .expect("client construction task should finish");
    let user = FbUser::builder("user-1").build();
    assert!(client.bool_variation("collision-flag", &user, false));

    first_patch_sender
        .send(())
        .expect("test server should receive first patch trigger");
    for _ in 0..100 {
        if !client.bool_variation("collision-flag", &user, true) {
            break;
        }
        time::sleep(Duration::from_millis(10)).await;
    }
    assert!(!client.bool_variation("collision-flag", &user, true));

    conflict_sender
        .send(())
        .expect("test server should receive conflict trigger");
    for _ in 0..100 {
        if client.bool_variation("collision-flag", &user, false) {
            break;
        }
        time::sleep(Duration::from_millis(10)).await;
    }
    assert!(client.bool_variation("collision-flag", &user, false));

    tokio::task::spawn_blocking(move || client.close())
        .await
        .expect("client close task should finish");
    server.await.expect("test server should stop");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn close_cancels_a_stalled_websocket_handshake() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("test listener should bind");
    let address = listener
        .local_addr()
        .expect("listener should have an address");
    let options = FbOptionsBuilder::new("valid-environment-secret")
        .streaming_url(format!("ws://{address}"))
        .disable_events(true, false)
        .start_wait(Duration::from_secs(30))
        .connect_timeout(Duration::from_secs(30))
        .close_timeout(Duration::from_millis(400))
        .build()
        .expect("options should build");
    let status = Arc::new(StatusTracker::new(SyncStatus::Starting, false));
    let synchronizer = Arc::new(
        WebSocketDataSynchronizer::start(
            options,
            Arc::new(SnapshotStore::new()),
            Arc::clone(&status),
        )
        .expect("synchronizer should start"),
    );

    let (_stalled_stream, _peer) = time::timeout(Duration::from_secs(2), listener.accept())
        .await
        .expect("client should connect before the test timeout")
        .expect("test listener should accept the client");
    let started = Instant::now();
    let closing = Arc::clone(&synchronizer);
    tokio::task::spawn_blocking(move || closing.close())
        .await
        .expect("close task should finish");

    assert!(started.elapsed() < Duration::from_secs(2));
    assert_eq!(status.status(), SyncStatus::Closed);
    assert_eq!(
        synchronizer.worker.wait(Duration::from_secs(1)),
        WorkerWait::Completed
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unrecoverable_close_is_terminal_and_disables_stale_evaluation() {
    const FULL: &str = r#"{"messageType":"data-sync","data":{"eventType":"full","featureFlags":[{"id":"flag-id","key":"terminal-flag","updatedAt":1,"variationType":"boolean","variations":[{"id":"value","value":"true"}],"targetUsers":[],"rules":[],"isEnabled":true,"fallthrough":{"variations":[{"id":"value","rollout":[0,1],"exptRollout":0}]}}],"segments":[]}}"#;

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("test listener should bind");
    let address = listener
        .local_addr()
        .expect("listener should have an address");
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("client should connect");
        let mut socket = accept_async(stream)
            .await
            .expect("WebSocket handshake should succeed");
        let _request = socket
            .next()
            .await
            .expect("client should request data")
            .expect("data request should be valid");
        socket
            .send(Message::Text(FULL.into()))
            .await
            .expect("full data should send");
        socket
            .send(Message::Close(Some(CloseFrame {
                code: CloseCode::from(4003),
                reason: "terminal".into(),
            })))
            .await
            .expect("terminal close should send");

        assert!(
            time::timeout(Duration::from_millis(300), listener.accept())
                .await
                .is_err(),
            "terminal close must not reconnect"
        );
    });

    let options = FbOptionsBuilder::new("valid-environment-secret")
        .streaming_url(format!("ws://{address}"))
        .disable_events(true, false)
        .start_wait(Duration::from_secs(2))
        .connect_timeout(Duration::from_secs(1))
        .close_timeout(Duration::from_secs(1))
        .keep_alive_interval(Duration::from_mins(1))
        .build()
        .expect("options should build");
    let client = tokio::task::spawn_blocking(move || FbClient::with_options(options))
        .await
        .expect("client construction task should finish");
    let user = FbUser::builder("user-1").build();

    for _ in 0..100 {
        if client.status() == crate::ClientStatus::Closed {
            break;
        }
        time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(client.status(), crate::ClientStatus::Closed);
    assert!(client.initialized());
    let detail = client.bool_variation_detail("terminal-flag", &user, false);
    assert!(!detail.value);
    assert_eq!(detail.kind, crate::ReasonKind::ClientNotReady);

    tokio::task::spawn_blocking(move || client.close())
        .await
        .expect("client close task should finish");
    server.await.expect("test server should stop");
}

// Tungstenite fixes the handshake callback's large error type; the test cannot narrow it.
#[allow(clippy::result_large_err)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn websocket_handshake_contains_protocol_path_query_and_user_agent() {
    const EMPTY_FULL: &str = r#"{"messageType":"data-sync","data":{"eventType":"full","featureFlags":[],"segments":[]}}"#;
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("test listener should bind");
    let address = listener
        .local_addr()
        .expect("listener should have an address");
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("client should connect");
        let callback = |request: &Request, response: Response| {
            assert_eq!(request.uri().path(), "/proxy/streaming");
            let query = request.uri().query().expect("query should be present");
            assert!(query.contains("type=server"));
            assert!(query.contains("token="));
            assert_eq!(
                request
                    .headers()
                    .get("user-agent")
                    .and_then(|value| value.to_str().ok()),
                Some(crate::user_agent().as_str())
            );
            Ok(response)
        };
        let mut socket = accept_hdr_async(stream, callback)
            .await
            .expect("WebSocket handshake should succeed");
        assert!(next_test_text(&mut socket)
            .await
            .contains("\"timestamp\":0"));
        socket
            .send(Message::Text(EMPTY_FULL.into()))
            .await
            .expect("empty full data should send");
        serve_until_client_close(&mut socket).await;
    });

    let options = FbOptionsBuilder::new("valid-environment-secret")
        .streaming_url(format!("ws://{address}/proxy/"))
        .disable_events(true, false)
        .start_wait(Duration::from_secs(1))
        .connect_timeout(Duration::from_millis(200))
        .close_timeout(Duration::from_secs(1))
        .build()
        .expect("options should build");
    let client = tokio::task::spawn_blocking(move || FbClient::with_options(options))
        .await
        .expect("client construction should finish");
    assert!(client.initialized());
    client.close();
    server.await.expect("test server should stop");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn configured_connect_timeout_retries_a_stalled_handshake() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("test listener should bind");
    let address = listener
        .local_addr()
        .expect("listener should have an address");
    let (retried_sender, retried_receiver) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (first, _) = listener
            .accept()
            .await
            .expect("first connection should arrive");
        let (second, _) = time::timeout(Duration::from_secs(1), listener.accept())
            .await
            .expect("connect timeout should trigger a retry")
            .expect("second connection should arrive");
        retried_sender
            .send(())
            .expect("retry observer should remain available");
        time::sleep(Duration::from_millis(250)).await;
        drop((first, second));
    });

    let options = FbOptionsBuilder::new("valid-environment-secret")
        .streaming_url(format!("ws://{address}"))
        .disable_events(true, false)
        .start_wait(Duration::from_millis(200))
        .connect_timeout(Duration::from_millis(50))
        .close_timeout(Duration::from_millis(200))
        .reconnect_delays([Duration::from_millis(1)])
        .build()
        .expect("options should build");
    let client = tokio::task::spawn_blocking(move || FbClient::with_options(options))
        .await
        .expect("client construction should finish");
    time::timeout(Duration::from_secs(1), retried_receiver)
        .await
        .expect("stalled handshake should time out and retry")
        .expect("retry assertion should complete");
    assert!(!client.initialized());
    assert_eq!(client.status(), crate::ClientStatus::NotReady);

    client.close();
    server.await.expect("test server should stop");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unrecoverable_close_before_initialization_stops_without_becoming_ready() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("test listener should bind");
    let address = listener
        .local_addr()
        .expect("listener should have an address");
    let server = tokio::spawn(async move {
        let mut socket = accept_test_socket(&listener).await;
        let _request = next_test_text(&mut socket).await;
        socket
            .send(Message::Close(Some(CloseFrame {
                code: CloseCode::from(4003),
                reason: "invalid environment".into(),
            })))
            .await
            .expect("terminal close should send");
        assert!(
            time::timeout(Duration::from_millis(300), listener.accept())
                .await
                .is_err(),
            "terminal close must not reconnect"
        );
    });

    let options = FbOptionsBuilder::new("valid-environment-secret")
        .streaming_url(format!("ws://{address}"))
        .disable_events(true, false)
        .start_wait(Duration::from_secs(1))
        .connect_timeout(Duration::from_millis(200))
        .close_timeout(Duration::from_secs(1))
        .build()
        .expect("options should build");
    let client = tokio::task::spawn_blocking(move || FbClient::with_options(options))
        .await
        .expect("client construction should finish");
    assert_eq!(client.status(), crate::ClientStatus::Closed);
    assert!(!client.initialized());
    let detail =
        client.bool_variation_detail("never-loaded", &FbUser::builder("user-1").build(), false);
    assert_eq!(detail.kind, crate::ReasonKind::ClientNotReady);

    client.close();
    server.await.expect("test server should stop");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn disconnected_ready_client_becomes_stale_and_evaluates_cached_data() {
    const FULL: &str = r#"{"messageType":"data-sync","data":{"eventType":"full","featureFlags":[{"id":"flag-id","key":"cached-flag","updatedAt":7,"variationType":"boolean","variations":[{"id":"value","value":"true"}],"isEnabled":true,"fallthrough":{"variations":[{"id":"value","rollout":[0,1]}]}}],"segments":[]}}"#;
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("test listener should bind");
    let address = listener
        .local_addr()
        .expect("listener should have an address");
    let (disconnected_sender, disconnected_receiver) = oneshot::channel();
    let (resume_sender, resume_receiver) = oneshot::channel();
    let server = tokio::spawn(async move {
        let mut first = accept_test_socket(&listener).await;
        let _request = next_test_text(&mut first).await;
        first
            .send(Message::Text(FULL.into()))
            .await
            .expect("initial full data should send");
        time::sleep(Duration::from_millis(20)).await;
        first
            .send(Message::Close(Some(CloseFrame {
                code: CloseCode::from(1011),
                reason: "temporary failure".into(),
            })))
            .await
            .expect("recoverable close should send");
        drop(first);
        disconnected_sender
            .send(())
            .expect("disconnect observer should remain available");

        resume_receiver
            .await
            .expect("test should allow reconnect handshake");
        let mut second = accept_test_socket(&listener).await;
        assert!(next_test_text(&mut second)
            .await
            .contains("\"timestamp\":7"));
        second
            .send(Message::Text(FULL.into()))
            .await
            .expect("resynchronized full data should send");
        serve_until_client_close(&mut second).await;
    });

    let options = FbOptionsBuilder::new("valid-environment-secret")
        .streaming_url(format!("ws://{address}"))
        .disable_events(true, false)
        .start_wait(Duration::from_secs(1))
        .connect_timeout(Duration::from_millis(200))
        .close_timeout(Duration::from_secs(1))
        .reconnect_delays([Duration::from_millis(1)])
        .build()
        .expect("options should build");
    let client = tokio::task::spawn_blocking(move || FbClient::with_options(options))
        .await
        .expect("client construction should finish");
    disconnected_receiver
        .await
        .expect("server should close the stable connection");
    for _ in 0..100 {
        if client.status() == crate::ClientStatus::Stale {
            break;
        }
        time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(client.status(), crate::ClientStatus::Stale);
    assert!(client.bool_variation("cached-flag", &FbUser::builder("user-1").build(), false));

    resume_sender
        .send(())
        .expect("reconnect server should remain available");
    for _ in 0..100 {
        if client.status() == crate::ClientStatus::Ready {
            break;
        }
        time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(client.status(), crate::ClientStatus::Ready);

    client.close();
    server.await.expect("test server should stop");
}
