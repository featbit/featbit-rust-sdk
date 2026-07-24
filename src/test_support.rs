use std::io::{Read, Write};
use std::net::TcpListener;
use std::thread;
use std::time::Duration;

use crossbeam_channel::{bounded, Receiver};

pub(crate) fn read_request_body(stream: &mut std::net::TcpStream) -> Vec<u8> {
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .expect("test stream should configure a timeout");
    let mut request = Vec::new();
    let mut buffer = [0_u8; 2_048];
    loop {
        let read = stream
            .read(&mut buffer)
            .expect("request should be readable");
        assert!(read > 0, "request ended before its body was complete");
        request.extend_from_slice(&buffer[..read]);
        let Some(header_end) = request.windows(4).position(|part| part == b"\r\n\r\n") else {
            continue;
        };
        let body_start = header_end + 4;
        let headers =
            std::str::from_utf8(&request[..header_end]).expect("request headers should be UTF-8");
        let content_length = headers
            .lines()
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse::<usize>().ok())
                    .flatten()
            })
            .expect("request should include content-length");
        if request.len() >= body_start + content_length {
            return request[body_start..body_start + content_length].to_vec();
        }
    }
}

pub(crate) fn scripted_http_server(
    statuses: impl IntoIterator<Item = u16>,
) -> (String, Receiver<Vec<u8>>, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("test listener should bind");
    let address = listener
        .local_addr()
        .expect("test listener should have an address");
    let statuses = statuses.into_iter().collect::<Vec<_>>();
    let (body_sender, bodies) = bounded(statuses.len());
    let server = thread::spawn(move || {
        for status in statuses {
            let (mut stream, _peer) = listener.accept().expect("event request should connect");
            let body = read_request_body(&mut stream);
            body_sender
                .send(body)
                .expect("test body receiver should remain available");
            write!(
                stream,
                "HTTP/1.1 {status} Test\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
            )
            .expect("test response should write");
            stream.flush().expect("test response should flush");
        }
    });
    (format!("http://{address}"), bodies, server)
}

pub(crate) fn disconnect_then_http_server(
    final_status: u16,
) -> (String, Receiver<Vec<u8>>, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("test listener should bind");
    let address = listener
        .local_addr()
        .expect("test listener should have an address");
    let (body_sender, bodies) = bounded(2);
    let server = thread::spawn(move || {
        let (mut disconnected, _peer) = listener
            .accept()
            .expect("first event request should connect");
        body_sender
            .send(read_request_body(&mut disconnected))
            .expect("test body receiver should remain available");
        drop(disconnected);

        let (mut successful, _peer) = listener
            .accept()
            .expect("retried event request should connect");
        body_sender
            .send(read_request_body(&mut successful))
            .expect("test body receiver should remain available");
        write!(
            successful,
            "HTTP/1.1 {final_status} Test\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
        )
        .expect("test response should write");
        successful.flush().expect("test response should flush");
    });
    (format!("http://{address}"), bodies, server)
}
