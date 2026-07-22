use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio::sync::watch;
use tokio::time::{self, Instant};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::protocol::{Message, WebSocketConfig};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

use crate::options::FbOptions;
use crate::store::SnapshotStore;
use crate::user_agent;

use super::protocol::{apply_message, reconnect_delay, streaming_url, ApplyResult};
use super::{StatusTracker, SyncStatus};

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

pub(super) async fn run_sync_loop(
    options: FbOptions,
    store: Arc<SnapshotStore>,
    status: Arc<StatusTracker>,
    mut stop: watch::Receiver<bool>,
) {
    log::info!("starting FeatBit WebSocket data synchronizer");
    let mut retry_attempt = 0_usize;
    let mut force_full_sync = false;

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
                let requested_version = if force_full_sync { 0 } else { store.version() };
                match send_data_sync(&mut socket, requested_version, &options, &mut stop).await {
                    SocketSend::Stopped => break,
                    SocketSend::Failed(error) => {
                        log::debug!("failed to send FeatBit data-sync request: {error}");
                    }
                    SocketSend::Sent => {
                        let end = run_connection(
                            &mut socket,
                            &options,
                            &store,
                            &status,
                            &mut stop,
                            force_full_sync,
                        )
                        .await;
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
                            ConnectionEnd::Reconnect => force_full_sync = false,
                            ConnectionEnd::Resync => force_full_sync = true,
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
    mut awaiting_full_sync: bool,
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
                        return reconnect_end(awaiting_full_sync);
                    }
                }
            }
            incoming = socket.next() => {
                match incoming {
                    Some(Ok(Message::Text(text))) => {
                        if text.len() > options.max_ws_message_size {
                            log::warn!("discarding oversized FeatBit WebSocket message");
                            return reconnect_end(awaiting_full_sync);
                        }
                        if let Some(end) = handle_apply_result(
                            apply_message(store, status, text.as_bytes()),
                            socket,
                            options,
                            status,
                            stop,
                            &mut awaiting_full_sync,
                        ).await {
                            return end;
                        }
                    }
                    Some(Ok(Message::Binary(bytes))) => {
                        if bytes.len() > options.max_ws_message_size {
                            log::warn!("discarding oversized FeatBit WebSocket message");
                            return reconnect_end(awaiting_full_sync);
                        }
                        if let Some(end) = handle_apply_result(
                            apply_message(store, status, &bytes),
                            socket,
                            options,
                            status,
                            stop,
                            &mut awaiting_full_sync,
                        ).await {
                            return end;
                        }
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
                            SocketSend::Failed(_) => return reconnect_end(awaiting_full_sync),
                        }
                    }
                    Some(Ok(Message::Pong(_) | Message::Frame(_))) => {}
                    Some(Ok(Message::Close(frame))) => {
                        let code = frame.map_or(1006, |frame| u16::from(frame.code));
                        return if code == 4003 {
                            ConnectionEnd::Terminal(code)
                        } else {
                            reconnect_end(awaiting_full_sync)
                        };
                    }
                    Some(Err(error)) => {
                        log::debug!("FeatBit WebSocket receive failed: {error}");
                        return reconnect_end(awaiting_full_sync);
                    }
                    None => return reconnect_end(awaiting_full_sync),
                }
            }
        }
    }
}

async fn handle_apply_result(
    result: ApplyResult,
    socket: &mut Socket,
    options: &FbOptions,
    status: &StatusTracker,
    stop: &mut watch::Receiver<bool>,
    awaiting_full_sync: &mut bool,
) -> Option<ConnectionEnd> {
    // A patch cannot prove that the equal-version collision was resolved. Keep serving the
    // immutable last-known snapshot as stale until the requested authoritative full arrives.
    if *awaiting_full_sync && result != ApplyResult::Full {
        status.set(SyncStatus::Stale);
    }
    match result {
        ApplyResult::Full => {
            *awaiting_full_sync = false;
            None
        }
        ApplyResult::VersionConflict if !*awaiting_full_sync => {
            log::warn!(
                "detected conflicting FeatBit patch objects with the same version; requesting a full data sync"
            );
            match send_data_sync(socket, 0, options, stop).await {
                SocketSend::Sent => {
                    *awaiting_full_sync = true;
                    None
                }
                SocketSend::Stopped => Some(ConnectionEnd::Stopped),
                SocketSend::Failed(error) => {
                    log::debug!("failed to request a full FeatBit data sync: {error}");
                    Some(ConnectionEnd::Resync)
                }
            }
        }
        ApplyResult::Patch | ApplyResult::Ignored | ApplyResult::VersionConflict => None,
    }
}

const fn reconnect_end(awaiting_full_sync: bool) -> ConnectionEnd {
    if awaiting_full_sync {
        ConnectionEnd::Resync
    } else {
        ConnectionEnd::Reconnect
    }
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
    Resync,
}
