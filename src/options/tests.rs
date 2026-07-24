use super::*;

const EMPTY_BOOTSTRAP: &str = r#"{
        "messageType":"data-sync",
        "data":{"eventType":"full","featureFlags":[],"segments":[]}
    }"#;

#[test]
fn defaults_match_the_featbit_server_contract() {
    let options = FbOptionsBuilder::new("valid-secret")
        .build()
        .expect("default options should build");

    assert_eq!(options.streaming_url.as_str(), "ws://localhost:5100/");
    assert_eq!(options.event_url.as_str(), "http://localhost:5100/");
    assert_eq!(options.start_wait, Duration::from_secs(5));
    assert_eq!(options.connect_timeout, Duration::from_secs(3));
    assert_eq!(options.close_timeout, Duration::from_secs(2));
    assert_eq!(options.keep_alive_interval, Duration::from_secs(15));
    assert_eq!(options.auto_flush_interval, Duration::from_secs(5));
    assert_eq!(options.flush_timeout, Duration::from_secs(5));
    assert_eq!(options.event_request_timeout, Duration::from_secs(2));
    assert_eq!(options.max_events_in_queue, 10_000);
    assert_eq!(options.max_event_queue_size_bytes, 64 * 1024 * 1024);
    assert_eq!(options.max_events_per_request, 50);
    assert_eq!(options.max_send_event_attempts, 2);
    assert_eq!(
        options.send_event_retry_interval,
        Duration::from_millis(200)
    );
    assert_eq!(options.max_ws_message_size, 1024 * 1024);
    assert_eq!(
        options.reconnect_delays.as_ref(),
        [
            Duration::ZERO,
            Duration::from_secs(1),
            Duration::from_secs(2),
            Duration::from_secs(3),
            Duration::from_secs(5),
            Duration::from_secs(8),
            Duration::from_secs(13),
            Duration::from_secs(21),
            Duration::from_secs(34),
            Duration::from_secs(55),
        ]
    );
    assert!(!options.offline);
    assert!(!options.disable_events);
    assert!(options.allow_track);
    assert!(!options.events_disabled());
    assert!(options.track_allowed());
    assert!(options.evaluation_observer.is_none());
    assert!(options.bootstrap.is_none());
}

#[test]
fn event_mode_configuration_keeps_disable_and_track_decisions_independent() {
    for (disable, allow_track) in [(false, true), (false, false), (true, true), (true, false)] {
        let options = FbOptionsBuilder::new("valid-secret")
            .disable_events(disable, allow_track)
            .build()
            .expect("event mode should build");
        assert_eq!(options.events_disabled(), disable);
        assert_eq!(options.track_allowed(), allow_track);
    }
}

#[test]
fn debug_output_redacts_credentials_and_bootstrap_data() {
    let builder = FbOptionsBuilder::new("do-not-log-this-secret")
        .streaming_url(
            "wss://url-user:url-password@example.com/private-stream-path?token=query-secret",
        )
        .event_url("https://example.com/private-event-path?api_key=event-secret")
        .offline(true)
        .bootstrap_json(EMPTY_BOOTSTRAP);
    let builder_debug = format!("{builder:?}");
    assert!(builder_debug.contains("[REDACTED]"));
    assert!(!builder_debug.contains("do-not-log-this-secret"));
    assert!(!builder_debug.contains("messageType"));
    for secret in [
        "url-user",
        "url-password",
        "private-stream-path",
        "query-secret",
        "private-event-path",
        "event-secret",
    ] {
        assert!(!builder_debug.contains(secret));
    }

    assert!(matches!(
        builder.build(),
        Err(ConfigError::InvalidUrl {
            field: "streaming_url",
            ..
        })
    ));

    let options = FbOptionsBuilder::new("do-not-log-this-secret")
        .streaming_url("wss://example.com/private-stream-path")
        .event_url("https://example.com/private-event-path")
        .offline(true)
        .bootstrap_json(EMPTY_BOOTSTRAP)
        .build()
        .expect("bootstrap options should build");
    let options_debug = format!("{options:?}");
    assert!(options_debug.contains("[REDACTED]"));
    assert!(options_debug.contains("has_bootstrap: true"));
    assert!(!options_debug.contains("do-not-log-this-secret"));
    assert!(!options_debug.contains("messageType"));
    assert!(!options_debug.contains("private-stream-path"));
    assert!(!options_debug.contains("private-event-path"));
}

#[test]
fn invalid_configuration_returns_typed_errors() {
    assert!(matches!(
        FbOptionsBuilder::new("x").build(),
        Err(ConfigError::InvalidEnvironmentSecret)
    ));
    assert!(matches!(
        FbOptionsBuilder::new("valid-secret")
            .streaming_url("https://example.com")
            .build(),
        Err(ConfigError::InvalidUrlScheme {
            field: "streaming_url",
            expected: "ws or wss"
        })
    ));
    assert!(matches!(
        FbOptionsBuilder::new("valid-secret")
            .bootstrap_json(EMPTY_BOOTSTRAP)
            .build(),
        Err(ConfigError::BootstrapRequiresOffline)
    ));
    assert!(matches!(
        FbOptionsBuilder::new("valid-secret")
            .offline(true)
            .bootstrap_json("{}")
            .build(),
        Err(ConfigError::InvalidBootstrap(_))
    ));
    assert!(matches!(
        FbOptionsBuilder::new("valid-secret")
            .event_url("https://user:password@example.com")
            .build(),
        Err(ConfigError::InvalidUrl {
            field: "event_url",
            ..
        })
    ));
    assert!(matches!(
        FbOptionsBuilder::new("valid-secret")
            .streaming_url("wss://example.com?token=secret")
            .build(),
        Err(ConfigError::InvalidUrl {
            field: "streaming_url",
            ..
        })
    ));
    assert!(matches!(
        FbOptionsBuilder::new("valid-secret")
            .reconnect_delays([Duration::ZERO])
            .build(),
        Err(ConfigError::InvalidDuration {
            field: "reconnect_delays",
            ..
        })
    ));
}

#[test]
fn validation_rejects_every_non_progressing_duration_and_capacity() {
    type BuilderUpdate = fn(FbOptionsBuilder) -> FbOptionsBuilder;
    let zero_durations: [(&str, BuilderUpdate); 7] = [
        ("start_wait", |builder| builder.start_wait(Duration::ZERO)),
        ("connect_timeout", |builder| {
            builder.connect_timeout(Duration::ZERO)
        }),
        ("close_timeout", |builder| {
            builder.close_timeout(Duration::ZERO)
        }),
        ("keep_alive_interval", |builder| {
            builder.keep_alive_interval(Duration::ZERO)
        }),
        ("auto_flush_interval", |builder| {
            builder.auto_flush_interval(Duration::ZERO)
        }),
        ("flush_timeout", |builder| {
            builder.flush_timeout(Duration::ZERO)
        }),
        ("event_request_timeout", |builder| {
            builder.event_request_timeout(Duration::ZERO)
        }),
    ];
    for (expected_field, update) in zero_durations {
        let error = update(FbOptionsBuilder::new("valid-secret"))
            .build()
            .expect_err("zero duration should be rejected");
        assert!(matches!(
            error,
            ConfigError::InvalidDuration { field, .. } if field == expected_field
        ));
    }

    let zero_capacities: [(&str, BuilderUpdate); 5] = [
        ("max_events_in_queue", |builder| {
            builder.max_events_in_queue(0)
        }),
        ("max_event_queue_size_bytes", |builder| {
            builder.max_event_queue_size_bytes(0)
        }),
        ("max_events_per_request", |builder| {
            builder.max_events_per_request(0)
        }),
        ("max_send_event_attempts", |builder| {
            builder.max_send_event_attempts(0)
        }),
        ("max_ws_message_size", |builder| {
            builder.max_ws_message_size(0)
        }),
    ];
    for (expected_field, update) in zero_capacities {
        let error = update(FbOptionsBuilder::new("valid-secret"))
            .build()
            .expect_err("zero capacity should be rejected");
        assert!(matches!(
            error,
            ConfigError::InvalidCapacity { field, .. } if field == expected_field
        ));
    }
}

#[test]
fn validation_rejects_invalid_secret_url_and_limit_relationships() {
    for secret in ["", "ab", "abc def", "密钥"] {
        assert!(matches!(
            FbOptionsBuilder::new(secret).build(),
            Err(ConfigError::InvalidEnvironmentSecret)
        ));
    }

    for builder in [
        FbOptionsBuilder::new("valid-secret").event_url("ws://example.com"),
        FbOptionsBuilder::new("valid-secret").streaming_url("wss://"),
        FbOptionsBuilder::new("valid-secret").event_url("https://example.com/path#fragment"),
    ] {
        assert!(builder.build().is_err());
    }

    assert!(matches!(
        FbOptionsBuilder::new("valid-secret")
            .start_wait(Duration::from_secs(1))
            .connect_timeout(Duration::from_secs(2))
            .build(),
        Err(ConfigError::InvalidDuration {
            field: "start_wait",
            ..
        })
    ));
    assert!(matches!(
        FbOptionsBuilder::new("valid-secret")
            .reconnect_delays([])
            .build(),
        Err(ConfigError::EmptyReconnectDelays)
    ));
    assert!(matches!(
        FbOptionsBuilder::new("valid-secret")
            .reconnect_delays([Duration::ZERO, Duration::ZERO])
            .build(),
        Err(ConfigError::InvalidDuration {
            field: "reconnect_delays",
            ..
        })
    ));

    let too_long = Duration::from_hours(8_784);
    assert!(matches!(
        FbOptionsBuilder::new("valid-secret")
            .start_wait(too_long)
            .connect_timeout(Duration::from_secs(1))
            .build(),
        Err(ConfigError::InvalidDuration {
            field: "start_wait",
            ..
        })
    ));
    assert!(matches!(
        FbOptionsBuilder::new("valid-secret")
            .max_events_in_queue(1_000_001)
            .build(),
        Err(ConfigError::InvalidCapacity {
            field: "max_events_in_queue",
            ..
        })
    ));
    assert!(matches!(
        FbOptionsBuilder::new("valid-secret")
            .max_event_queue_size_bytes(1024 * 1024 * 1024 + 1)
            .build(),
        Err(ConfigError::InvalidCapacity {
            field: "max_event_queue_size_bytes",
            ..
        })
    ));
}
