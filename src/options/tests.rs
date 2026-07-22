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
    assert_eq!(options.max_events_per_request, 50);
    assert_eq!(options.max_send_event_attempts, 2);
    assert_eq!(
        options.send_event_retry_interval,
        Duration::from_millis(200)
    );
    assert_eq!(options.max_ws_message_size, 1024 * 1024);
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
