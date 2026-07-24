//! End-to-end coverage for WebSocket synchronization, evaluation, and HTTP event delivery.

use std::collections::HashMap;
use std::time::Duration;

use axum::body::Bytes;
use axum::extract::State;
use axum::http::header::{AUTHORIZATION, CONTENT_TYPE, USER_AGENT};
use axum::http::{HeaderMap, StatusCode};
use axum::routing::post;
use axum::Router;
use featbit_server_sdk::{ClientStatus, FbClient, FbOptions, FbUser, ReasonKind};
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time;
use tokio_tungstenite::accept_hdr_async;
use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};
use tokio_tungstenite::tungstenite::Message;

const FULL: &str = r#"{
    "messageType":"data-sync",
    "data":{"eventType":"full","featureFlags":[{
        "id":"e2e-id","key":"e2e-flag","updatedAt":1,"variationType":"boolean",
        "variations":[{"id":"enabled","value":"true"}],
        "targetUsers":[],"rules":[],"isEnabled":true,
        "fallthrough":{"includedInExpt":false,"variations":[
            {"id":"enabled","rollout":[0,1],"exptRollout":0}
        ]}
    }],"segments":[]}
}"#;

const PATCH: &str = r#"{
    "messageType":"data-sync",
    "data":{"eventType":"patch","featureFlags":[{
        "id":"e2e-id","key":"e2e-flag","updatedAt":2,"variationType":"boolean",
        "variations":[{"id":"disabled","value":"false"}],
        "targetUsers":[],"rules":[],"isEnabled":true,
        "fallthrough":{"includedInExpt":false,"variations":[
            {"id":"disabled","rollout":[0,1],"exptRollout":0}
        ]}
    }],"segments":[]}
}"#;

#[derive(Debug)]
struct CapturedEventRequest {
    headers: HeaderMap,
    body: Bytes,
}

#[derive(Clone)]
struct EventServerState {
    requests: mpsc::UnboundedSender<CapturedEventRequest>,
}

struct EventServer {
    url: String,
    requests: mpsc::UnboundedReceiver<CapturedEventRequest>,
    shutdown: oneshot::Sender<()>,
    task: JoinHandle<()>,
}

struct StreamingServer {
    url: String,
    send_patch: oneshot::Sender<()>,
    task: JoinHandle<()>,
}

async fn capture_events(
    State(state): State<EventServerState>,
    headers: HeaderMap,
    body: Bytes,
) -> StatusCode {
    let _ignored = state.requests.send(CapturedEventRequest { headers, body });
    StatusCode::ACCEPTED
}

async fn start_event_server() -> EventServer {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("event listener should bind");
    let address = listener
        .local_addr()
        .expect("event listener should have an address");
    let (request_sender, requests) = mpsc::unbounded_channel();
    let (shutdown, shutdown_receiver) = oneshot::channel();
    let router = Router::new()
        .route("/api/public/insight/track", post(capture_events))
        .with_state(EventServerState {
            requests: request_sender,
        });
    let task = tokio::spawn(async move {
        axum::serve(listener, router)
            .with_graceful_shutdown(async {
                let _ignored = shutdown_receiver.await;
            })
            .await
            .expect("event server should run");
    });
    EventServer {
        url: format!("http://{address}"),
        requests,
        shutdown,
        task,
    }
}

fn verify_handshake(request: &Request) {
    assert_eq!(request.uri().path(), "/streaming");
    let query = request
        .uri()
        .query()
        .expect("streaming query should be present");
    let parameters = url::form_urlencoded::parse(query.as_bytes())
        .into_owned()
        .collect::<HashMap<_, _>>();
    assert_eq!(parameters.get("type").map(String::as_str), Some("server"));
    assert!(parameters
        .get("token")
        .is_some_and(|token| !token.is_empty()));
    assert_eq!(
        request
            .headers()
            .get(USER_AGENT)
            .and_then(|value| value.to_str().ok()),
        Some(concat!(
            "featbit-rust-server-sdk/",
            env!("CARGO_PKG_VERSION")
        ))
    );
}

// Tungstenite fixes the handshake callback's large error type; the test cannot narrow it.
#[allow(clippy::result_large_err)]
async fn run_streaming_server(listener: TcpListener, send_patch: oneshot::Receiver<()>) {
    let (stream, _) = listener.accept().await.expect("SDK should connect");
    let mut socket = accept_hdr_async(stream, |request: &Request, response: Response| {
        verify_handshake(request);
        Ok(response)
    })
    .await
    .expect("WebSocket handshake should succeed");
    let request = socket
        .next()
        .await
        .expect("SDK should request data")
        .expect("data-sync request should be valid")
        .into_text()
        .expect("data-sync request should be text");
    let request: Value =
        serde_json::from_str(&request).expect("data-sync request should contain JSON");
    assert_eq!(request["messageType"], "data-sync");
    assert_eq!(request["data"]["timestamp"], 0);
    socket
        .send(Message::Text(FULL.into()))
        .await
        .expect("full data should send");

    send_patch.await.expect("test should request the patch");
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
}

async fn start_streaming_server() -> StreamingServer {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("streaming listener should bind");
    let address = listener
        .local_addr()
        .expect("streaming listener should have an address");
    let (send_patch, patch_receiver) = oneshot::channel();
    let task = tokio::spawn(run_streaming_server(listener, patch_receiver));
    StreamingServer {
        url: format!("ws://{address}"),
        send_patch,
        task,
    }
}

async fn wait_for_raw_value(client: &FbClient, user: &FbUser, expected: &str) {
    time::timeout(Duration::from_secs(2), async {
        loop {
            let matches = client
                .all_variations(user)
                .iter()
                .any(|detail| detail.key == "e2e-flag" && detail.value == expected);
            if matches {
                return;
            }
            time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("patched value should become visible");
}

fn assert_event_request(request: &CapturedEventRequest) {
    assert_eq!(
        request
            .headers
            .get(AUTHORIZATION)
            .and_then(|value| value.to_str().ok()),
        Some("e2e-environment-secret")
    );
    assert_eq!(
        request
            .headers
            .get(USER_AGENT)
            .and_then(|value| value.to_str().ok()),
        Some(concat!(
            "featbit-rust-server-sdk/",
            env!("CARGO_PKG_VERSION")
        ))
    );
    assert!(request
        .headers
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.starts_with("application/json")));

    let events: Value =
        serde_json::from_slice(&request.body).expect("event request should contain JSON");
    let events = events.as_array().expect("event batch should be an array");
    assert_eq!(events.len(), 3);
    assert_eq!(events[0]["variations"][0]["variation"]["value"], "true");
    assert_eq!(events[1]["variations"][0]["variation"]["value"], "false");
    assert_eq!(events[2]["metrics"][0]["eventName"], "e2e-conversion");
    assert!(events
        .iter()
        .all(|event| event["user"]["keyId"] == "e2e-user"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn full_client_sync_evaluation_patch_and_event_delivery() {
    let mut events = start_event_server().await;
    let streaming = start_streaming_server().await;
    let options = FbOptions::builder("e2e-environment-secret")
        .streaming_url(&streaming.url)
        .event_url(&events.url)
        .start_wait(Duration::from_secs(2))
        .connect_timeout(Duration::from_secs(1))
        .close_timeout(Duration::from_secs(1))
        .keep_alive_interval(Duration::from_mins(1))
        .auto_flush_interval(Duration::from_mins(1))
        .flush_timeout(Duration::from_secs(2))
        .event_request_timeout(Duration::from_secs(1))
        .max_events_per_request(10)
        .build()
        .expect("e2e options should build");
    let client = tokio::task::spawn_blocking(move || FbClient::with_options(options))
        .await
        .expect("client construction should finish");
    let user = FbUser::builder("e2e-user").name("E2E User").build();

    assert_eq!(client.status(), ClientStatus::Ready);
    let initial = client.bool_variation_detail("e2e-flag", &user, false);
    assert!(initial.value);
    assert_eq!(initial.kind, ReasonKind::Fallthrough);
    streaming
        .send_patch
        .send(())
        .expect("streaming server should still run");
    wait_for_raw_value(&client, &user, "false").await;
    assert!(!client.bool_variation("e2e-flag", &user, true));
    assert!(client.track_metric_event(&user, "e2e-conversion", 1.0));

    let flushing = client.clone();
    assert!(
        tokio::task::spawn_blocking(move || flushing.flush_and_wait(Duration::from_secs(2)))
            .await
            .expect("flush task should finish")
    );
    let request = time::timeout(Duration::from_secs(2), events.requests.recv())
        .await
        .expect("event request should arrive")
        .expect("event server should remain available");
    assert_event_request(&request);

    let closing = client.clone();
    tokio::task::spawn_blocking(move || closing.close())
        .await
        .expect("close task should finish");
    streaming
        .task
        .await
        .expect("streaming server should stop cleanly");
    let _ignored = events.shutdown.send(());
    events.task.await.expect("event server should stop cleanly");
}
