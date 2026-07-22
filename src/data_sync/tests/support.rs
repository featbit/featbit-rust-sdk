use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time;
use tokio_tungstenite::tungstenite::protocol::Message;
use tokio_tungstenite::{accept_async, WebSocketStream};

pub(super) type TestSocket = WebSocketStream<TcpStream>;

pub(super) async fn accept_test_socket(listener: &TcpListener) -> TestSocket {
    let (stream, _) = time::timeout(Duration::from_secs(2), listener.accept())
        .await
        .expect("client should connect before the test timeout")
        .expect("test listener should accept the client");
    accept_async(stream)
        .await
        .expect("test WebSocket handshake should succeed")
}

pub(super) async fn next_test_text(socket: &mut TestSocket) -> String {
    socket
        .next()
        .await
        .expect("client should send a message")
        .expect("client message should be valid")
        .into_text()
        .expect("client message should be text")
        .to_string()
}

pub(super) async fn serve_until_client_close(socket: &mut TestSocket) {
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
