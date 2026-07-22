use std::io::ErrorKind;
use std::net::TcpListener;
use std::sync::Barrier;
use std::thread;

use mockito::Matcher;
use serde_json::Value;
use url::Url;

use super::*;
use crate::test_support::scripted_http_server;

fn test_user() -> FbUser {
    FbUser::builder("u1")
        .name("Ada")
        .custom("country", "cn")
        .build()
}

#[test]
fn evaluation_wire_shape_matches_featbit() {
    let tracked = FbEvaluationEvent::new("checkout", "on-id", "true", true);
    let event = PayloadEvent::evaluation(&test_user(), &tracked);
    let value = serde_json::to_value(event).expect("event should serialize");
    assert_eq!(value["user"]["keyId"], "u1");
    assert_eq!(value["variations"][0]["featureFlagKey"], "checkout");
    assert_eq!(value["variations"][0]["variation"]["id"], "on-id");
    assert_eq!(value["variations"][0]["sendToExperiment"], true);
}

#[test]
fn public_evaluation_event_debug_redacts_the_raw_value() {
    let event = FbEvaluationEvent::new("checkout", "on-id", "private-variation-value", false);
    let debug = format!("{event:?}");
    assert!(debug.contains("[REDACTED]"));
    assert!(!debug.contains("private-variation-value"));
}

#[test]
fn metric_wire_shape_identifies_rust_sdk() {
    let event = PayloadEvent::metric(&test_user(), "purchased", 12.5);
    let value = serde_json::to_value(event).expect("event should serialize");
    assert_eq!(value["metrics"][0]["appType"], "rust-server-side");
    assert_eq!(value["metrics"][0]["route"], "index/metric");
    assert_eq!(value["metrics"][0]["type"], "CustomEvent");
    assert!(matches!(value, Value::Object(_)));
}

#[test]
fn endpoint_preserves_base_path() {
    let base = Url::parse("https://example.com/proxy/").expect("URL should parse");
    assert_eq!(
        event_endpoint(&base).as_str(),
        "https://example.com/proxy/api/public/insight/track"
    );
}

#[test]
fn processor_posts_authorized_event_batch() {
    let mut server = mockito::Server::new();
    let request = server
        .mock("POST", "/api/public/insight/track")
        .match_header("authorization", "valid-secret")
        .match_header("user-agent", "featbit-rust-server-sdk/0.1.0")
        .match_header("content-type", "application/json; charset=utf-8")
        .match_body(Matcher::Regex(
            ".*\\\"eventName\\\":\\\"purchase\\\".*".into(),
        ))
        .with_status(202)
        .expect(1)
        .create();
    let options = crate::options::FbOptionsBuilder::new("valid-secret")
        .event_url(server.url())
        .auto_flush_interval(Duration::from_mins(1))
        .build()
        .expect("options should build");
    let processor = EventProcessor::new(&options);

    assert!(processor.record_metric(&test_user(), "purchase", 42.0));
    assert!(processor.flush_and_wait(Duration::from_secs(2)));
    processor.close();
    assert!(!processor.flush_and_wait(Duration::from_secs(2)));
    request.assert();
}

#[test]
fn full_queue_and_stalled_http_request_do_not_block_concurrent_close() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("test listener should bind");
    listener
        .set_nonblocking(true)
        .expect("test listener should become nonblocking");
    let address = listener
        .local_addr()
        .expect("test listener should have an address");
    let (accepted_sender, accepted_receiver) = bounded(1);
    let (server_stop_sender, server_stop_receiver) = bounded(1);
    let server = thread::spawn(move || loop {
        match listener.accept() {
            Ok((_stream, _peer)) => {
                let _ignored = accepted_sender.send(());
                let _ignored = server_stop_receiver.recv_timeout(Duration::from_secs(5));
                break;
            }
            Err(error) if error.kind() == ErrorKind::WouldBlock => {
                if server_stop_receiver.try_recv().is_ok() {
                    break;
                }
                thread::yield_now();
            }
            Err(error) => panic!("test listener failed: {error}"),
        }
    });

    let options = crate::options::FbOptionsBuilder::new("valid-secret")
        .event_url(format!("http://{address}"))
        .auto_flush_interval(Duration::from_mins(1))
        .flush_timeout(Duration::from_millis(400))
        .event_request_timeout(Duration::from_secs(30))
        .max_events_in_queue(1)
        .max_events_per_request(1)
        .max_send_event_attempts(1)
        .build()
        .expect("options should build");
    let processor = EventProcessor::new(&options);
    let EventProcessor::Active(inner) = &processor else {
        panic!("event processor should start");
    };

    assert!(processor.record_metric(&test_user(), "first", 1.0));
    accepted_receiver
        .recv_timeout(Duration::from_secs(2))
        .expect("event request should reach the test server");
    assert!(processor.record_metric(&test_user(), "queued", 2.0));
    assert!(!processor.record_metric(&test_user(), "dropped", 3.0));

    let barrier = Arc::new(Barrier::new(5));
    let mut closers = Vec::new();
    let started = Instant::now();
    for _ in 0..4 {
        let inner = Arc::clone(inner);
        let barrier = Arc::clone(&barrier);
        closers.push(thread::spawn(move || {
            barrier.wait();
            inner.close();
        }));
    }
    barrier.wait();
    for closer in closers {
        closer.join().expect("close thread should not panic");
    }

    assert!(started.elapsed() < Duration::from_secs(2));
    assert!(inner.closed.load(Ordering::Acquire));
    assert!(inner.sender.load_full().is_none());
    assert_eq!(
        inner.worker.wait(Duration::from_secs(1)),
        WorkerWait::Completed
    );

    let _ignored = server_stop_sender.send(());
    server.join().expect("test server should stop");
}

#[test]
fn processor_splits_batches_at_the_configured_request_limit() {
    let (event_url, bodies, server) = scripted_http_server([202, 202]);
    let options = crate::options::FbOptionsBuilder::new("valid-secret")
        .event_url(event_url)
        .auto_flush_interval(Duration::from_mins(1))
        .max_events_per_request(2)
        .build()
        .expect("options should build");
    let processor = EventProcessor::new(&options);

    for index in 0..3 {
        assert!(processor.record_metric(&test_user(), &format!("event-{index}"), 1.0));
    }
    assert!(processor.flush_and_wait(Duration::from_secs(2)));
    processor.close();
    server.join().expect("test server should stop");

    let first: Value = serde_json::from_slice(
        &bodies
            .recv_timeout(Duration::from_secs(1))
            .expect("first batch should arrive"),
    )
    .expect("first batch should be JSON");
    let second: Value = serde_json::from_slice(
        &bodies
            .recv_timeout(Duration::from_secs(1))
            .expect("second batch should arrive"),
    )
    .expect("second batch should be JSON");
    assert_eq!(first.as_array().map(Vec::len), Some(2));
    assert_eq!(second.as_array().map(Vec::len), Some(1));
}

#[test]
fn graceful_close_drains_every_accepted_event() {
    let (event_url, bodies, server) = scripted_http_server([202, 202]);
    let options = crate::options::FbOptionsBuilder::new("valid-secret")
        .event_url(event_url)
        .auto_flush_interval(Duration::from_mins(1))
        .flush_timeout(Duration::from_secs(2))
        .max_events_per_request(2)
        .build()
        .expect("options should build");
    let processor = EventProcessor::new(&options);

    for index in 0..3 {
        assert!(processor.record_metric(&test_user(), &format!("close-{index}"), 1.0));
    }
    processor.close();
    server.join().expect("test server should stop");

    let delivered = (0..2)
        .map(|_| {
            let body = bodies
                .recv_timeout(Duration::from_secs(1))
                .expect("close should deliver both batches");
            serde_json::from_slice::<Value>(&body)
                .expect("batch should be JSON")
                .as_array()
                .map_or(0, Vec::len)
        })
        .sum::<usize>();
    assert_eq!(delivered, 3);
}

#[test]
fn recoverable_failures_retry_and_unrecoverable_status_stops_delivery() {
    let (retry_url, retry_bodies, retry_server) = scripted_http_server([500, 202]);
    let retry_options = crate::options::FbOptionsBuilder::new("valid-secret")
        .event_url(retry_url)
        .auto_flush_interval(Duration::from_mins(1))
        .max_events_per_request(1)
        .max_send_event_attempts(2)
        .send_event_retry_interval(Duration::from_millis(1))
        .build()
        .expect("retry options should build");
    let retry_processor = EventProcessor::new(&retry_options);
    assert!(retry_processor.record_metric(&test_user(), "retry", 1.0));
    assert!(retry_processor.flush_and_wait(Duration::from_secs(2)));
    retry_processor.close();
    retry_server.join().expect("retry server should stop");
    assert!(retry_bodies.recv_timeout(Duration::from_secs(1)).is_ok());
    assert!(retry_bodies.recv_timeout(Duration::from_secs(1)).is_ok());

    let (fatal_url, fatal_bodies, fatal_server) = scripted_http_server([401]);
    let fatal_options = crate::options::FbOptionsBuilder::new("valid-secret")
        .event_url(fatal_url)
        .auto_flush_interval(Duration::from_mins(1))
        .max_events_per_request(1)
        .build()
        .expect("fatal options should build");
    let fatal_processor = EventProcessor::new(&fatal_options);
    assert!(fatal_processor.record_metric(&test_user(), "fatal", 1.0));
    assert!(!fatal_processor.flush_and_wait(Duration::from_secs(2)));
    assert!(!fatal_processor.record_metric(&test_user(), "discarded", 2.0));
    assert!(!fatal_processor.flush_and_wait(Duration::from_secs(2)));
    fatal_processor.close();
    fatal_server.join().expect("fatal server should stop");
    assert!(fatal_bodies.recv_timeout(Duration::from_secs(1)).is_ok());
    assert!(fatal_bodies.try_recv().is_err());

    let (failed_url, failed_bodies, failed_server) = scripted_http_server([500, 500]);
    let failed_options = crate::options::FbOptionsBuilder::new("valid-secret")
        .event_url(failed_url)
        .auto_flush_interval(Duration::from_mins(1))
        .max_events_per_request(1)
        .max_send_event_attempts(2)
        .send_event_retry_interval(Duration::from_millis(1))
        .build()
        .expect("retry options should build");
    let failed_processor = EventProcessor::new(&failed_options);
    assert!(failed_processor.record_metric(&test_user(), "failed", 1.0));
    assert!(!failed_processor.flush_and_wait(Duration::from_secs(2)));
    failed_processor.close();
    failed_server.join().expect("failed server should stop");
    assert!(failed_bodies.recv_timeout(Duration::from_secs(1)).is_ok());
    assert!(failed_bodies.recv_timeout(Duration::from_secs(1)).is_ok());

    assert!(is_recoverable(StatusCode::BAD_REQUEST));
    assert!(is_recoverable(StatusCode::REQUEST_TIMEOUT));
    assert!(is_recoverable(StatusCode::TOO_MANY_REQUESTS));
    assert!(is_recoverable(StatusCode::INTERNAL_SERVER_ERROR));
    assert!(!is_recoverable(StatusCode::UNAUTHORIZED));
    assert!(!is_recoverable(StatusCode::NOT_FOUND));
}
