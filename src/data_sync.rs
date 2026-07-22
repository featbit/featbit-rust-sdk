use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant as StdInstant};

use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use parking_lot::{Condvar, Mutex};
use rand::Rng;
use tokio::net::TcpStream;
use tokio::sync::watch;
use tokio::time::{self, Instant};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::protocol::{Message, WebSocketConfig};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};
use url::Url;

use crate::model::DataSyncEnvelope;
use crate::options::FbOptions;
use crate::store::SnapshotStore;
use crate::user_agent;
use crate::worker::{WorkerThread, WorkerWait};

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub(crate) enum SyncStatus {
    Starting = 0,
    Ready = 1,
    Stale = 2,
    Closed = 3,
}

#[derive(Debug)]
pub(crate) struct StatusTracker {
    status: AtomicU8,
    initialized: AtomicBool,
    wait_lock: Mutex<()>,
    wait_condition: Condvar,
}

impl StatusTracker {
    pub(crate) fn new(status: SyncStatus, initialized: bool) -> Self {
        Self {
            status: AtomicU8::new(status as u8),
            initialized: AtomicBool::new(initialized),
            wait_lock: Mutex::new(()),
            wait_condition: Condvar::new(),
        }
    }

    pub(crate) fn status(&self) -> SyncStatus {
        match self.status.load(Ordering::Acquire) {
            1 => SyncStatus::Ready,
            2 => SyncStatus::Stale,
            3 => SyncStatus::Closed,
            _ => SyncStatus::Starting,
        }
    }

    pub(crate) fn initialized(&self) -> bool {
        self.initialized.load(Ordering::Acquire)
    }

    pub(crate) fn set(&self, status: SyncStatus) {
        let _guard = self.wait_lock.lock();
        if status == SyncStatus::Ready {
            self.initialized.store(true, Ordering::Release);
        }
        let previous = self.status.swap(status as u8, Ordering::AcqRel);
        if previous != status as u8 || status == SyncStatus::Ready {
            self.wait_condition.notify_all();
        }
    }

    pub(crate) fn wait_until_initialized(&self, timeout: Duration) -> bool {
        if self.initialized() {
            return true;
        }

        let now = StdInstant::now();
        let deadline = now.checked_add(timeout).unwrap_or(now);
        let mut guard = self.wait_lock.lock();
        while !self.initialized() && self.status() != SyncStatus::Closed {
            let Some(remaining) = deadline.checked_duration_since(StdInstant::now()) else {
                break;
            };
            if remaining.is_zero()
                || self
                    .wait_condition
                    .wait_for(&mut guard, remaining)
                    .timed_out()
            {
                break;
            }
        }
        self.initialized()
    }
}

#[derive(Debug)]
pub(crate) struct WebSocketDataSynchronizer {
    stop: watch::Sender<bool>,
    worker: WorkerThread,
    status: Arc<StatusTracker>,
    close_timeout: Duration,
    closed: AtomicBool,
}

impl WebSocketDataSynchronizer {
    pub(crate) fn start(
        options: FbOptions,
        store: Arc<SnapshotStore>,
        status: Arc<StatusTracker>,
    ) -> Option<Self> {
        let (stop, stop_receiver) = watch::channel(false);
        let worker_status = Arc::clone(&status);
        let close_timeout = options.close_timeout;
        let worker = WorkerThread::spawn("data synchronizer", move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build();
            match runtime {
                Ok(runtime) => runtime.block_on(run_sync_loop(
                    options,
                    store,
                    Arc::clone(&worker_status),
                    stop_receiver,
                )),
                Err(error) => {
                    log::error!("failed to create FeatBit WebSocket runtime: {error}");
                    worker_status.set(SyncStatus::Closed);
                }
            }
        });

        match worker {
            Ok(worker) => Some(Self {
                stop,
                worker,
                status,
                close_timeout,
                closed: AtomicBool::new(false),
            }),
            Err(error) => {
                log::error!("failed to start FeatBit WebSocket worker: {error}");
                status.set(SyncStatus::Closed);
                None
            }
        }
    }

    pub(crate) fn close(&self) {
        if self.closed.swap(true, Ordering::AcqRel) {
            let _ignored = self.worker.wait(Duration::ZERO);
            return;
        }
        let _ignored = self.stop.send(true);
        match self.worker.wait(self.close_timeout) {
            WorkerWait::Completed => {}
            WorkerWait::Panicked => {
                log::warn!("FeatBit WebSocket worker stopped after a panic");
            }
            WorkerWait::TimedOut => {
                log::warn!("FeatBit WebSocket worker did not close within the configured timeout");
            }
        }
        self.status.set(SyncStatus::Closed);
    }
}

impl Drop for WebSocketDataSynchronizer {
    fn drop(&mut self) {
        self.close();
    }
}

async fn run_sync_loop(
    options: FbOptions,
    store: Arc<SnapshotStore>,
    status: Arc<StatusTracker>,
    mut stop: watch::Receiver<bool>,
) {
    log::info!("starting FeatBit WebSocket data synchronizer");
    let mut retry_attempt = 0_usize;

    loop {
        if *stop.borrow() {
            break;
        }
        if status.initialized() {
            status.set(SyncStatus::Stale);
        }

        let connection = tokio::select! {
            biased;
            () = wait_for_stop(&mut stop) => break,
            connection = connect(&options) => connection,
        };

        match connection {
            Ok(mut socket) => {
                let connected_at = Instant::now();
                log::debug!("FeatBit WebSocket connected");
                match send_data_sync(&mut socket, store.version(), &options, &mut stop).await {
                    SocketSend::Stopped => break,
                    SocketSend::Failed(error) => {
                        log::debug!("failed to send FeatBit data-sync request: {error}");
                    }
                    SocketSend::Sent => {
                        let end =
                            run_connection(&mut socket, &options, &store, &status, &mut stop).await;
                        // A completed handshake is not a healthy session: an accept-and-drop peer
                        // must advance the backoff instead of creating a zero-delay reconnect loop.
                        // Reset only after valid data arrived and the connection survived long
                        // enough to cover one configured keepalive period.
                        if status.status() == SyncStatus::Ready
                            && connected_at.elapsed() >= options.keep_alive_interval
                        {
                            retry_attempt = 0;
                        }
                        match end {
                            ConnectionEnd::Stopped => break,
                            ConnectionEnd::Terminal(code) => {
                                log::error!(
                                    "FeatBit WebSocket closed without reconnecting (code {code})"
                                );
                                status.set(SyncStatus::Closed);
                                return;
                            }
                            ConnectionEnd::Reconnect => {}
                        }
                    }
                }
            }
            Err(error) => {
                log::debug!("FeatBit WebSocket connection failed: {error}");
            }
        }

        if status.initialized() {
            status.set(SyncStatus::Stale);
        }
        let delay = reconnect_delay(&options, retry_attempt);
        retry_attempt = retry_attempt.saturating_add(1);
        log::debug!("reconnecting FeatBit WebSocket after {delay:?}");
        tokio::select! {
            biased;
            () = wait_for_stop(&mut stop) => break,
            () = time::sleep(delay) => {}
        }
    }

    status.set(SyncStatus::Closed);
    log::info!("FeatBit WebSocket data synchronizer stopped");
}

async fn connect(options: &FbOptions) -> Result<Socket, String> {
    let url = streaming_url(options)?;
    let mut request = url
        .as_str()
        .into_client_request()
        .map_err(|error| error.to_string())?;
    let header = HeaderValue::from_str(&user_agent()).map_err(|error| error.to_string())?;
    request.headers_mut().insert(
        tokio_tungstenite::tungstenite::http::header::USER_AGENT,
        header,
    );

    let config = WebSocketConfig::default()
        .max_message_size(Some(options.max_ws_message_size))
        .max_frame_size(Some(options.max_ws_message_size));
    let connection = tokio_tungstenite::connect_async_with_config(request, Some(config), false);
    match time::timeout(options.connect_timeout, connection).await {
        Ok(Ok((socket, _response))) => Ok(socket),
        Ok(Err(_error)) => Err("connection failed".to_owned()),
        Err(_) => Err("connection timed out".to_owned()),
    }
}

async fn send_data_sync(
    socket: &mut Socket,
    version: i64,
    options: &FbOptions,
    stop: &mut watch::Receiver<bool>,
) -> SocketSend {
    let request = serde_json::json!({
        "messageType": "data-sync",
        "data": { "timestamp": version.max(0) }
    });
    send_socket_message(
        socket,
        Message::Text(request.to_string().into()),
        options.connect_timeout,
        stop,
    )
    .await
}

async fn run_connection(
    socket: &mut Socket,
    options: &FbOptions,
    store: &SnapshotStore,
    status: &StatusTracker,
    stop: &mut watch::Receiver<bool>,
) -> ConnectionEnd {
    let first_ping = Instant::now() + options.keep_alive_interval;
    let mut ping = time::interval_at(first_ping, options.keep_alive_interval);

    loop {
        tokio::select! {
            () = wait_for_stop(stop) => {
                let _ignored = time::timeout(
                    graceful_work_budget(options.close_timeout),
                    socket.close(None),
                )
                .await;
                return ConnectionEnd::Stopped;
            }
            _ = ping.tick() => {
                let message = Message::Text("{\"messageType\":\"ping\",\"data\":{}}".into());
                match send_socket_message(
                    socket,
                    message,
                    options.connect_timeout,
                    stop,
                ).await {
                    SocketSend::Sent => {}
                    SocketSend::Stopped => return ConnectionEnd::Stopped,
                    SocketSend::Failed(error) => {
                        log::debug!("failed to send FeatBit WebSocket ping: {error}");
                        return ConnectionEnd::Reconnect;
                    }
                }
            }
            incoming = socket.next() => {
                match incoming {
                    Some(Ok(Message::Text(text))) => {
                        if text.len() > options.max_ws_message_size {
                            log::warn!("discarding oversized FeatBit WebSocket message");
                            return ConnectionEnd::Reconnect;
                        }
                        apply_message(store, status, text.as_bytes());
                    }
                    Some(Ok(Message::Binary(bytes))) => {
                        if bytes.len() > options.max_ws_message_size {
                            log::warn!("discarding oversized FeatBit WebSocket message");
                            return ConnectionEnd::Reconnect;
                        }
                        apply_message(store, status, &bytes);
                    }
                    Some(Ok(Message::Ping(payload))) => {
                        match send_socket_message(
                            socket,
                            Message::Pong(payload),
                            options.connect_timeout,
                            stop,
                        ).await {
                            SocketSend::Sent => {}
                            SocketSend::Stopped => return ConnectionEnd::Stopped,
                            SocketSend::Failed(_) => return ConnectionEnd::Reconnect,
                        }
                    }
                    Some(Ok(Message::Pong(_) | Message::Frame(_))) => {}
                    Some(Ok(Message::Close(frame))) => {
                        let code = frame.map_or(1006, |frame| u16::from(frame.code));
                        return if code == 4003 {
                            ConnectionEnd::Terminal(code)
                        } else {
                            ConnectionEnd::Reconnect
                        };
                    }
                    Some(Err(error)) => {
                        log::debug!("FeatBit WebSocket receive failed: {error}");
                        return ConnectionEnd::Reconnect;
                    }
                    None => return ConnectionEnd::Reconnect,
                }
            }
        }
    }
}

fn apply_message(store: &SnapshotStore, status: &StatusTracker, payload: &[u8]) {
    let envelope = match serde_json::from_slice::<DataSyncEnvelope>(payload) {
        Ok(envelope) => envelope,
        Err(error) => {
            log::debug!("discarding malformed FeatBit WebSocket message: {error}");
            return;
        }
    };
    if envelope.message_type != "data-sync" {
        return;
    }

    match envelope.data.event_type.as_str() {
        "full" => store.populate(&envelope.data),
        "patch" => {
            store.patch(&envelope.data);
        }
        event_type => {
            log::debug!("ignoring unknown FeatBit data-sync event type {event_type:?}");
            return;
        }
    }
    log::debug!(
        "applied FeatBit {} data-sync at version {}",
        envelope.data.event_type,
        store.version()
    );
    status.set(SyncStatus::Ready);
}

#[derive(Debug, Eq, PartialEq)]
enum SocketSend {
    Sent,
    Stopped,
    Failed(String),
}

async fn send_socket_message(
    socket: &mut Socket,
    message: Message,
    timeout: Duration,
    stop: &mut watch::Receiver<bool>,
) -> SocketSend {
    tokio::select! {
        biased;
        () = wait_for_stop(stop) => SocketSend::Stopped,
        result = time::timeout(timeout, socket.send(message)) => match result {
            Ok(Ok(())) => SocketSend::Sent,
            Ok(Err(error)) => SocketSend::Failed(error.to_string()),
            Err(_) => SocketSend::Failed("WebSocket write timed out".to_owned()),
        },
    }
}

async fn wait_for_stop(stop: &mut watch::Receiver<bool>) {
    loop {
        if *stop.borrow() {
            return;
        }
        if stop.changed().await.is_err() {
            return;
        }
    }
}

fn graceful_work_budget(timeout: Duration) -> Duration {
    timeout.saturating_sub((timeout / 4).min(Duration::from_millis(100)))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ConnectionEnd {
    Stopped,
    Terminal(u16),
    Reconnect,
}

fn streaming_url(options: &FbOptions) -> Result<Url, String> {
    let token = connection_token(&options.env_secret)
        .ok_or_else(|| "environment secret cannot form a connection token".to_owned())?;
    let mut url = options.streaming_url.clone();
    let base_path = url.path().trim_end_matches('/');
    url.set_path(&format!("{base_path}/streaming"));
    url.set_query(None);
    url.set_fragment(None);
    url.query_pairs_mut()
        .append_pair("type", "server")
        .append_pair("token", &token);
    Ok(url)
}

fn connection_token(secret: &str) -> Option<String> {
    let trimmed = secret.trim_end_matches('=');
    if trimmed.len() < 3 || !trimmed.is_ascii() {
        return None;
    }

    let start = rand::rng().random_range(2..trimmed.len());
    let start_number = u64::try_from(start).ok()?;
    let timestamp = Utc::now().timestamp_millis().max(0);
    let timestamp_text = timestamp.to_string();
    let prefix = trimmed.get(..start)?;
    let suffix = trimmed.get(start..)?;

    Some(format!(
        "{}{}{prefix}{}{suffix}",
        encode_number(start_number, 3),
        encode_number(u64::try_from(timestamp_text.len()).ok()?, 2),
        encode_number(timestamp.cast_unsigned(), timestamp_text.len())
    ))
}

fn encode_number(number: u64, length: usize) -> String {
    const MAP: [char; 10] = ['Q', 'B', 'W', 'S', 'P', 'H', 'D', 'X', 'Z', 'U'];
    let padded = format!("{number:0length$}");
    let mut digits: Vec<char> = padded.chars().rev().take(length).collect();
    digits.reverse();
    digits
        .into_iter()
        .filter_map(|digit| digit.to_digit(10))
        .filter_map(|digit| MAP.get(digit as usize).copied())
        .collect()
}

fn reconnect_delay(options: &FbOptions, attempt: usize) -> Duration {
    let length = options.reconnect_delays.len();
    if length == 0 {
        return Duration::from_secs(1);
    }
    let index = attempt % length;
    let base = options
        .reconnect_delays
        .get(index)
        .copied()
        .unwrap_or(Duration::from_secs(1));
    if base.is_zero() {
        return base;
    }
    let jitter_percent = rand::rng().random_range(80..=120);
    base.checked_mul(jitter_percent)
        .and_then(|duration| duration.checked_div(100))
        .unwrap_or(base)
}

#[cfg(test)]
mod tests {
    use std::thread;
    use std::time::Duration;

    use tokio::net::TcpListener;
    use tokio::sync::oneshot;
    use tokio_tungstenite::accept_async;
    use tokio_tungstenite::tungstenite::protocol::{frame::coding::CloseCode, CloseFrame};

    use super::*;
    use crate::client::FbClient;
    use crate::model::FbUser;
    use crate::options::FbOptionsBuilder;

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
        apply_message(&store, &status, full);
        assert!(status.initialized());
        assert_eq!(store.version(), 1);

        let patch = br#"{"messageType":"data-sync","data":{"eventType":"patch","featureFlags":[{"key":"flag","updatedAt":2,"isArchived":true}],"segments":[]}}"#;
        apply_message(&store, &status, patch);
        assert_eq!(store.version(), 2);
        assert!(store.load().flags["flag"].is_archived);

        let stale = br#"{"messageType":"data-sync","data":{"eventType":"patch","featureFlags":[{"key":"flag","updatedAt":1,"isArchived":false}],"segments":[]}}"#;
        apply_message(&store, &status, stale);
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
        let started = StdInstant::now();
        assert!(!terminal.wait_until_initialized(Duration::from_secs(30)));
        assert!(started.elapsed() < Duration::from_secs(1));
    }

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
            .disable_events(true)
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
    async fn close_cancels_a_stalled_websocket_handshake() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("test listener should bind");
        let address = listener
            .local_addr()
            .expect("listener should have an address");
        let options = FbOptionsBuilder::new("valid-environment-secret")
            .streaming_url(format!("ws://{address}"))
            .disable_events(true)
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
        let started = StdInstant::now();
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
            .disable_events(true)
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

    const RECONNECT_FULL: &str = r#"{"messageType":"data-sync","data":{"eventType":"full","featureFlags":[{"id":"flag-id","key":"reconnected-flag","updatedAt":7,"variationType":"boolean","variations":[{"id":"value","value":"true"}],"targetUsers":[],"rules":[],"isEnabled":true,"fallthrough":{"variations":[{"id":"value","rollout":[0,1],"exptRollout":0}]}}],"segments":[]}}"#;
    const NORMAL_CLOSE_PATCH: &str = r#"{"messageType":"data-sync","data":{"eventType":"patch","featureFlags":[{"id":"flag-id","key":"reconnected-flag","updatedAt":8,"variationType":"boolean","variations":[{"id":"value","value":"false"}],"targetUsers":[],"rules":[],"isEnabled":true,"fallthrough":{"variations":[{"id":"value","rollout":[0,1],"exptRollout":0}]}}],"segments":[]}}"#;

    type TestSocket = WebSocketStream<TcpStream>;

    async fn accept_test_socket(listener: &TcpListener) -> TestSocket {
        let (stream, _) = time::timeout(Duration::from_secs(2), listener.accept())
            .await
            .expect("client should connect before the test timeout")
            .expect("test listener should accept the client");
        accept_async(stream)
            .await
            .expect("test WebSocket handshake should succeed")
    }

    async fn next_test_text(socket: &mut TestSocket) -> String {
        socket
            .next()
            .await
            .expect("client should send a message")
            .expect("client message should be valid")
            .into_text()
            .expect("client message should be text")
            .to_string()
    }

    async fn serve_until_client_close(socket: &mut TestSocket) {
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
            .disable_events(true)
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
            .disable_events(true)
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

    async fn run_reconnect_protocol_server(
        listener: TcpListener,
        version_sender: oneshot::Sender<()>,
    ) {
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
            .disable_events(true)
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
}
