use std::io::{ErrorKind, Read, Write};
use std::net::TcpListener;
use std::sync::Barrier;
use std::thread;

use mockito::Matcher;
use serde_json::Value;
use url::Url;

use super::*;
use crate::test_support::{disconnect_then_http_server, scripted_http_server};

fn test_user() -> FbUser {
    FbUser::builder("u1")
        .name("Ada")
        .custom("country", "cn")
        .build()
}

fn normalize_timestamps(value: &mut Value) {
    match value {
        Value::Array(values) => values.iter_mut().for_each(normalize_timestamps),
        Value::Object(fields) => {
            for (key, value) in fields {
                if key == "timestamp" {
                    *value = Value::from(0);
                } else {
                    normalize_timestamps(value);
                }
            }
        }
        _ => {}
    }
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
fn single_multi_and_mixed_event_serialization_matches_featbit_wire_fixtures() {
    let user_one = FbUser::builder("u1-Id")
        .name("u1-name")
        .custom("country", "us")
        .custom("custom", "value")
        .build();
    let user_two = FbUser::builder("u2-Id")
        .name("u2-name")
        .custom("age", "10")
        .build();
    let eval_one = PayloadEvent::evaluation(
        &user_one,
        &FbEvaluationEvent::new("hello", "v1Id", "v1", true),
    );
    let eval_two = PayloadEvent::evaluation(
        &user_two,
        &FbEvaluationEvent::new("hello", "v2Id", "v2", false),
    );
    let metric_one = PayloadEvent::metric(&user_one, "click-button", 1.5);
    let metric_two = PayloadEvent::metric(&user_two, "click-button", 32.5);

    let mut single_eval = serde_json::to_value(&eval_one).expect("evaluation should serialize");
    normalize_timestamps(&mut single_eval);
    assert_eq!(
        single_eval,
        serde_json::json!({
            "user": {
                "keyId": "u1-Id",
                "name": "u1-name",
                "customizedProperties": [
                    {"name": "country", "value": "us"},
                    {"name": "custom", "value": "value"}
                ]
            },
            "variations": [{
                "featureFlagKey": "hello",
                "variation": {"id": "v1Id", "value": "v1"},
                "timestamp": 0,
                "sendToExperiment": true
            }]
        })
    );

    let mut evaluation_batch =
        serde_json::to_value([eval_one.clone(), eval_two]).expect("batch should serialize");
    normalize_timestamps(&mut evaluation_batch);
    assert_eq!(evaluation_batch.as_array().map(Vec::len), Some(2));
    assert_eq!(evaluation_batch[1]["user"]["keyId"], "u2-Id");
    assert_eq!(
        evaluation_batch[1]["variations"][0]["variation"]["value"],
        "v2"
    );
    assert_eq!(
        evaluation_batch[1]["variations"][0]["sendToExperiment"],
        false
    );

    let mut metric_batch = serde_json::to_value([metric_one.clone(), metric_two])
        .expect("metric batch should serialize");
    normalize_timestamps(&mut metric_batch);
    assert_eq!(metric_batch.as_array().map(Vec::len), Some(2));
    assert_eq!(metric_batch[0]["metrics"][0]["appType"], "rust-server-side");
    assert_eq!(metric_batch[0]["metrics"][0]["route"], "index/metric");
    assert_eq!(metric_batch[0]["metrics"][0]["type"], "CustomEvent");
    assert_eq!(metric_batch[1]["metrics"][0]["numericValue"], 32.5);

    let mut mixed =
        serde_json::to_value([eval_one, metric_one]).expect("mixed batch should serialize");
    normalize_timestamps(&mut mixed);
    assert_eq!(mixed.as_array().map(Vec::len), Some(2));
    assert!(mixed[0].get("variations").is_some());
    assert!(mixed[1].get("metrics").is_some());
}

#[test]
fn endpoint_handles_root_nested_and_trailing_slash_base_urls() {
    for (base, expected) in [
        (
            "https://example.com",
            "https://example.com/api/public/insight/track",
        ),
        (
            "https://example.com/",
            "https://example.com/api/public/insight/track",
        ),
        (
            "https://example.com/proxy",
            "https://example.com/proxy/api/public/insight/track",
        ),
        (
            "https://example.com/proxy/",
            "https://example.com/proxy/api/public/insight/track",
        ),
    ] {
        let base = Url::parse(base).expect("URL should parse");
        assert_eq!(event_endpoint(&base).as_str(), expected);
    }
}

#[test]
fn queue_overflow_warning_state_is_suppressed_until_the_queue_recovers() {
    let capacity_exceeded = AtomicBool::new(false);

    assert!(should_log_event_queue_overflow(&capacity_exceeded));
    assert!(!should_log_event_queue_overflow(&capacity_exceeded));
    mark_event_queue_available(&capacity_exceeded);
    assert!(should_log_event_queue_overflow(&capacity_exceeded));
}

#[test]
fn processor_posts_authorized_event_batch() {
    let mut server = mockito::Server::new();
    let user_agent = crate::user_agent();
    let request = server
        .mock("POST", "/api/public/insight/track")
        .match_header("authorization", "valid-secret")
        .match_header("user-agent", user_agent.as_str())
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
fn active_processor_flushes_an_empty_buffer() {
    let options = crate::options::FbOptionsBuilder::new("valid-secret")
        .auto_flush_interval(Duration::from_mins(1))
        .build()
        .expect("options should build");
    let processor = EventProcessor::new(&options);

    assert!(processor.flush_and_wait(Duration::from_secs(1)));
    processor.flush();
    processor.close();
}

#[test]
fn invalid_event_fields_are_rejected_before_enqueue() {
    let options = crate::options::FbOptionsBuilder::new("valid-secret")
        .auto_flush_interval(Duration::from_mins(1))
        .build()
        .expect("options should build");
    let processor = EventProcessor::new(&options);
    let valid_user = test_user();
    let empty_user = FbUser::builder("").build();

    assert!(!processor.record_evaluation(
        &valid_user,
        &FbEvaluationEvent::new("", "variation", "value", false)
    ));
    assert!(!processor.record_evaluation(
        &valid_user,
        &FbEvaluationEvent::new("flag", " ", "value", false)
    ));
    assert!(!processor.record_evaluation(
        &empty_user,
        &FbEvaluationEvent::new("flag", "variation", "value", false)
    ));
    assert!(!processor.record_metric(&valid_user, "", 1.0));
    assert!(!processor.record_metric(&valid_user, "metric", f64::NAN));
    assert!(!processor.record_metric(&valid_user, "metric", f64::INFINITY));
    assert!(!processor.record_metric(&empty_user, "metric", 1.0));
    assert!(processor.flush_and_wait(Duration::from_secs(1)));
    processor.close();
}

#[test]
fn auto_flush_interval_delivers_without_an_explicit_flush() {
    let (event_url, bodies, server) = scripted_http_server([202]);
    let options = crate::options::FbOptionsBuilder::new("valid-secret")
        .event_url(event_url)
        .auto_flush_interval(Duration::from_millis(20))
        .build()
        .expect("options should build");
    let processor = EventProcessor::new(&options);

    assert!(processor.record_metric(&test_user(), "interval", 1.0));
    let body = bodies
        .recv_timeout(Duration::from_secs(2))
        .expect("automatic interval should flush the event");
    let events: Value = serde_json::from_slice(&body).expect("batch should be JSON");
    assert_eq!(events.as_array().map(Vec::len), Some(1));
    assert_eq!(events[0]["metrics"][0]["eventName"], "interval");

    processor.close();
    server.join().expect("event server should stop");
}

#[test]
fn batch_threshold_delivers_without_an_explicit_flush() {
    let (event_url, bodies, server) = scripted_http_server([202]);
    let options = crate::options::FbOptionsBuilder::new("valid-secret")
        .event_url(event_url)
        .auto_flush_interval(Duration::from_mins(1))
        .max_events_per_request(2)
        .build()
        .expect("options should build");
    let processor = EventProcessor::new(&options);

    assert!(processor.record_metric(&test_user(), "threshold-1", 1.0));
    assert!(processor.record_metric(&test_user(), "threshold-2", 2.0));
    let body = bodies
        .recv_timeout(Duration::from_secs(2))
        .expect("batch threshold should flush both events");
    let events: Value = serde_json::from_slice(&body).expect("batch should be JSON");
    assert_eq!(events.as_array().map(Vec::len), Some(2));

    processor.close();
    server.join().expect("event server should stop");
}

#[test]
fn event_request_timeout_is_bounded_and_reported_as_a_failed_flush() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("test listener should bind");
    let address = listener
        .local_addr()
        .expect("test listener should have an address");
    let server = thread::spawn(move || {
        let (mut stream, _peer) = listener.accept().expect("event request should connect");
        let mut buffer = [0_u8; 2_048];
        let _read = stream
            .read(&mut buffer)
            .expect("request should be readable");
        thread::sleep(Duration::from_millis(250));
    });
    let options = crate::options::FbOptionsBuilder::new("valid-secret")
        .event_url(format!("http://{address}"))
        .auto_flush_interval(Duration::from_mins(1))
        .event_request_timeout(Duration::from_millis(50))
        .max_send_event_attempts(1)
        .build()
        .expect("options should build");
    let processor = EventProcessor::new(&options);

    assert!(processor.record_metric(&test_user(), "timeout", 1.0));
    let started = Instant::now();
    assert!(!processor.flush_and_wait(Duration::from_secs(1)));
    assert!(started.elapsed() < Duration::from_millis(500));

    processor.close();
    server.join().expect("event server should stop");
}

#[test]
fn caller_flush_timeout_returns_while_the_http_request_continues() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("test listener should bind");
    let address = listener
        .local_addr()
        .expect("test listener should have an address");
    let server = thread::spawn(move || {
        let (mut stream, _peer) = listener.accept().expect("event request should connect");
        let mut buffer = [0_u8; 2_048];
        let _read = stream
            .read(&mut buffer)
            .expect("request should be readable");
        thread::sleep(Duration::from_millis(150));
        write!(
            stream,
            "HTTP/1.1 202 Accepted\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
        )
        .expect("response should write");
        stream.flush().expect("response should flush");
    });
    let options = crate::options::FbOptionsBuilder::new("valid-secret")
        .event_url(format!("http://{address}"))
        .auto_flush_interval(Duration::from_mins(1))
        .event_request_timeout(Duration::from_secs(1))
        .flush_timeout(Duration::from_millis(500))
        .max_send_event_attempts(1)
        .build()
        .expect("options should build");
    let processor = EventProcessor::new(&options);

    assert!(processor.record_metric(&test_user(), "slow-success", 1.0));
    let started = Instant::now();
    assert!(!processor.flush_and_wait(Duration::from_millis(20)));
    assert!(started.elapsed() < Duration::from_millis(100));

    server.join().expect("event server should stop");
    processor.close();
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

#[test]
fn connection_level_delivery_failure_retries_the_unchanged_batch() {
    let (event_url, bodies, server) = disconnect_then_http_server(202);
    let options = crate::options::FbOptionsBuilder::new("valid-secret")
        .event_url(event_url)
        .auto_flush_interval(Duration::from_mins(1))
        .max_send_event_attempts(2)
        .send_event_retry_interval(Duration::from_millis(1))
        .build()
        .expect("options should build");
    let processor = EventProcessor::new(&options);

    assert!(processor.record_metric(&test_user(), "transport-retry", 7.0));
    assert!(processor.flush_and_wait(Duration::from_secs(2)));
    processor.close();
    server.join().expect("event server should stop");

    let first = bodies
        .recv_timeout(Duration::from_secs(1))
        .expect("first request body should be captured");
    let retry = bodies
        .recv_timeout(Duration::from_secs(1))
        .expect("retried request body should be captured");
    assert_eq!(first, retry);
}
