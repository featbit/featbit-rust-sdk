const RECONNECT_FULL: &str = r#"{"messageType":"data-sync","data":{"eventType":"full","featureFlags":[{"id":"flag-id","key":"reconnected-flag","updatedAt":7,"variationType":"boolean","variations":[{"id":"value","value":"true"}],"targetUsers":[],"rules":[],"isEnabled":true,"fallthrough":{"variations":[{"id":"value","rollout":[0,1],"exptRollout":0}]}}],"segments":[]}}"#;
const NORMAL_CLOSE_PATCH: &str = r#"{"messageType":"data-sync","data":{"eventType":"patch","featureFlags":[{"id":"flag-id","key":"reconnected-flag","updatedAt":8,"variationType":"boolean","variations":[{"id":"value","value":"false"}],"targetUsers":[],"rules":[],"isEnabled":true,"fallthrough":{"variations":[{"id":"value","rollout":[0,1],"exptRollout":0}]}}],"segments":[]}}"#;

use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::time;
use tokio_tungstenite::tungstenite::protocol::{frame::coding::CloseCode, CloseFrame, Message};

use super::support::{accept_test_socket, next_test_text, serve_until_client_close};
use crate::client::FbClient;
use crate::model::FbUser;
use crate::options::FbOptionsBuilder;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn normal_server_close_reconnects_and_keeps_cached_data_available() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("test listener should bind");
    let address = listener
        .local_addr()
        .expect("listener should have an address");
    let (patch_sender, patch_receiver) = oneshot::channel::<()>();
    let server = tokio::spawn(async move {
        let mut first = accept_test_socket(&listener).await;
        assert!(next_test_text(&mut first).await.contains("\"timestamp\":0"));
        first
            .send(Message::Text(RECONNECT_FULL.into()))
            .await
            .expect("initial full data should send");
        first
            .send(Message::Close(Some(CloseFrame {
                code: CloseCode::Normal,
                reason: "load balancer rotation".into(),
            })))
            .await
            .expect("normal close should send");
        drop(first);

        let mut second = accept_test_socket(&listener).await;
        assert!(next_test_text(&mut second)
            .await
            .contains("\"timestamp\":7"));
        second
            .send(Message::Text(NORMAL_CLOSE_PATCH.into()))
            .await
            .expect("patch should send after a normal server close");
        patch_sender
            .send(())
            .expect("patch receiver should remain available");
        serve_until_client_close(&mut second).await;
    });

    let options = FbOptionsBuilder::new("valid-environment-secret")
        .streaming_url(format!("ws://{address}"))
        .disable_events(true, false)
        .start_wait(Duration::from_secs(2))
        .connect_timeout(Duration::from_secs(1))
        .close_timeout(Duration::from_secs(1))
        .keep_alive_interval(Duration::from_mins(1))
        .reconnect_delays([Duration::from_millis(1)])
        .build()
        .expect("options should build");
    let client = tokio::task::spawn_blocking(move || FbClient::with_options(options))
        .await
        .expect("client construction task should finish");
    time::timeout(Duration::from_secs(2), patch_receiver)
        .await
        .expect("normal close should reconnect")
        .expect("patch assertion should complete");

    let user = FbUser::builder("user-1").build();
    for _ in 0..100 {
        if !client.bool_variation("reconnected-flag", &user, true) {
            break;
        }
        time::sleep(Duration::from_millis(10)).await;
    }
    assert!(!client.bool_variation("reconnected-flag", &user, true));
    assert_ne!(client.status(), crate::ClientStatus::Closed);

    tokio::task::spawn_blocking(move || client.close())
        .await
        .expect("client close task should finish");
    server.await.expect("test server should stop");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn short_failed_connections_advance_the_reconnect_backoff() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("test listener should bind");
    let address = listener
        .local_addr()
        .expect("listener should have an address");
    let (third_sender, third_receiver) = oneshot::channel::<()>();
    let server = tokio::spawn(async move {
        for _ in 0..2 {
            let mut socket = accept_test_socket(&listener).await;
            let _request = next_test_text(&mut socket).await;
            socket
                .send(Message::Close(Some(CloseFrame {
                    code: CloseCode::from(1011),
                    reason: "short failure".into(),
                })))
                .await
                .expect("recoverable close should send");
            drop(socket);
        }

        assert!(
            time::timeout(Duration::from_millis(100), listener.accept())
                .await
                .is_err(),
            "the third connection must observe the second backoff delay"
        );
        let mut third = accept_test_socket(&listener).await;
        let _request = next_test_text(&mut third).await;
        third_sender
            .send(())
            .expect("third connection receiver should remain available");
        serve_until_client_close(&mut third).await;
    });

    let options = FbOptionsBuilder::new("valid-environment-secret")
        .streaming_url(format!("ws://{address}"))
        .disable_events(true, false)
        .start_wait(Duration::from_secs(1))
        .connect_timeout(Duration::from_millis(100))
        .close_timeout(Duration::from_secs(1))
        .keep_alive_interval(Duration::from_mins(1))
        .reconnect_delays([
            Duration::ZERO,
            Duration::from_millis(300),
            Duration::from_millis(300),
        ])
        .build()
        .expect("options should build");
    let client = tokio::task::spawn_blocking(move || FbClient::with_options(options))
        .await
        .expect("client construction task should finish");
    time::timeout(Duration::from_secs(2), third_receiver)
        .await
        .expect("third connection should arrive after backoff")
        .expect("third connection assertion should complete");

    tokio::task::spawn_blocking(move || client.close())
        .await
        .expect("client close task should finish");
    server.await.expect("test server should stop");
}

async fn run_reconnect_protocol_server(listener: TcpListener, version_sender: oneshot::Sender<()>) {
    let mut first = accept_test_socket(&listener).await;
    assert!(next_test_text(&mut first).await.contains("\"timestamp\":0"));
    first
        .send(Message::Text("not-json".into()))
        .await
        .expect("malformed test message should send");
    let ping = time::timeout(Duration::from_secs(1), async {
        loop {
            let text = next_test_text(&mut first).await;
            if text.contains("\"messageType\":\"ping\"") {
                break text;
            }
        }
    })
    .await
    .expect("application ping should arrive");
    assert_eq!(ping, "{\"messageType\":\"ping\",\"data\":{}}");
    first
        .send(Message::Text("x".repeat(2_048).into()))
        .await
        .expect("oversized message should reach the client");
    drop(first);

    let mut second = accept_test_socket(&listener).await;
    assert!(next_test_text(&mut second)
        .await
        .contains("\"timestamp\":0"));
    second
        .send(Message::Text(RECONNECT_FULL.into()))
        .await
        .expect("full data should send after reconnect");
    second
        .send(Message::Close(Some(CloseFrame {
            code: CloseCode::from(1011),
            reason: "retry".into(),
        })))
        .await
        .expect("recoverable close should send");
    drop(second);

    let mut third = accept_test_socket(&listener).await;
    assert!(next_test_text(&mut third).await.contains("\"timestamp\":7"));
    third
        .send(Message::Text(RECONNECT_FULL.into()))
        .await
        .expect("full data should restore ready state");
    version_sender
        .send(())
        .expect("version assertion receiver should remain available");
    while let Some(message) = third.next().await {
        match message.expect("client message should be valid") {
            Message::Close(_) => {
                let _ignored = third.close(None).await;
                break;
            }
            Message::Ping(payload) => {
                third
                    .send(Message::Pong(payload))
                    .await
                    .expect("pong should send");
            }
            _ => {}
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn malformed_ping_oversize_and_reconnect_follow_the_wire_protocol() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("test listener should bind");
    let address = listener
        .local_addr()
        .expect("listener should have an address");
    let (version_sender, version_receiver) = oneshot::channel::<()>();
    let server = tokio::spawn(run_reconnect_protocol_server(listener, version_sender));

    let options = FbOptionsBuilder::new("valid-environment-secret")
        .streaming_url(format!("ws://{address}"))
        .disable_events(true, false)
        .start_wait(Duration::from_secs(2))
        .connect_timeout(Duration::from_secs(1))
        .close_timeout(Duration::from_secs(1))
        .keep_alive_interval(Duration::from_millis(20))
        .reconnect_delays([Duration::from_millis(1)])
        .max_ws_message_size(1_024)
        .build()
        .expect("options should build");
    let client = tokio::task::spawn_blocking(move || FbClient::with_options(options))
        .await
        .expect("client construction task should finish");
    time::timeout(Duration::from_secs(2), version_receiver)
        .await
        .expect("versioned reconnect should occur")
        .expect("version assertion should complete");
    let user = FbUser::builder("user-1").build();
    assert!(client.bool_variation("reconnected-flag", &user, false));

    tokio::task::spawn_blocking(move || client.close())
        .await
        .expect("client close task should finish");
    server.await.expect("test server should stop");
}
