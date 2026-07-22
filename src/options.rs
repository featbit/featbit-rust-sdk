use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use url::Url;

use crate::error::ConfigError;
use crate::model::DataSyncEnvelope;

const DEFAULT_STREAMING_URL: &str = "ws://localhost:5100";
const DEFAULT_EVENT_URL: &str = "http://localhost:5100";
const MAX_CONFIG_DURATION: Duration = Duration::from_hours(8_760);

const DEFAULT_RECONNECT_DELAYS: [Duration; 10] = [
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
];

/// Immutable configuration for [`crate::FbClient`] and [`crate::FeatBitProvider`].
///
/// Construct options with [`FbOptionsBuilder`]. Cloning this value is inexpensive and never
/// exposes the environment secret through `Debug` output.
#[derive(Clone)]
pub struct FbOptions {
    pub(crate) env_secret: Arc<str>,
    pub(crate) streaming_url: Url,
    pub(crate) event_url: Url,
    pub(crate) start_wait: Duration,
    pub(crate) offline: bool,
    pub(crate) disable_events: bool,
    pub(crate) connect_timeout: Duration,
    pub(crate) close_timeout: Duration,
    pub(crate) keep_alive_interval: Duration,
    pub(crate) reconnect_delays: Arc<[Duration]>,
    pub(crate) auto_flush_interval: Duration,
    pub(crate) flush_timeout: Duration,
    pub(crate) event_request_timeout: Duration,
    pub(crate) max_events_in_queue: usize,
    pub(crate) max_events_per_request: usize,
    pub(crate) max_send_event_attempts: usize,
    pub(crate) send_event_retry_interval: Duration,
    pub(crate) max_ws_message_size: usize,
    pub(crate) bootstrap: Option<Arc<DataSyncEnvelope>>,
}

impl FbOptions {
    /// Starts building options for an environment secret.
    #[must_use]
    pub fn builder(env_secret: impl Into<String>) -> FbOptionsBuilder {
        FbOptionsBuilder::new(env_secret)
    }

    /// Returns the configured WebSocket streaming base URL.
    #[must_use]
    pub fn streaming_url(&self) -> &Url {
        &self.streaming_url
    }

    /// Returns the configured event service base URL.
    #[must_use]
    pub fn event_url(&self) -> &Url {
        &self.event_url
    }

    /// Returns how long client construction waits for initial data.
    #[must_use]
    pub const fn start_wait(&self) -> Duration {
        self.start_wait
    }

    /// Returns whether all remote communication is disabled.
    #[must_use]
    pub const fn offline(&self) -> bool {
        self.offline
    }

    /// Returns whether analytics event collection is disabled.
    #[must_use]
    pub const fn events_disabled(&self) -> bool {
        self.disable_events
    }
}

impl fmt::Debug for FbOptions {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FbOptions")
            .field("env_secret", &"[REDACTED]")
            .field("streaming_url", &UrlSummary(&self.streaming_url))
            .field("event_url", &UrlSummary(&self.event_url))
            .field("start_wait", &self.start_wait)
            .field("offline", &self.offline)
            .field("disable_events", &self.disable_events)
            .field("connect_timeout", &self.connect_timeout)
            .field("close_timeout", &self.close_timeout)
            .field("keep_alive_interval", &self.keep_alive_interval)
            .field("reconnect_delays", &self.reconnect_delays)
            .field("auto_flush_interval", &self.auto_flush_interval)
            .field("flush_timeout", &self.flush_timeout)
            .field("event_request_timeout", &self.event_request_timeout)
            .field("max_events_in_queue", &self.max_events_in_queue)
            .field("max_events_per_request", &self.max_events_per_request)
            .field("max_send_event_attempts", &self.max_send_event_attempts)
            .field("send_event_retry_interval", &self.send_event_retry_interval)
            .field("max_ws_message_size", &self.max_ws_message_size)
            .field("has_bootstrap", &self.bootstrap.is_some())
            .finish()
    }
}

/// Builder for validated, immutable [`FbOptions`].
#[derive(Clone)]
pub struct FbOptionsBuilder {
    env_secret: String,
    streaming_url: String,
    event_url: String,
    start_wait: Duration,
    offline: bool,
    disable_events: bool,
    connect_timeout: Duration,
    close_timeout: Duration,
    keep_alive_interval: Duration,
    reconnect_delays: Vec<Duration>,
    auto_flush_interval: Duration,
    flush_timeout: Duration,
    event_request_timeout: Duration,
    max_events_in_queue: usize,
    max_events_per_request: usize,
    max_send_event_attempts: usize,
    send_event_retry_interval: Duration,
    max_ws_message_size: usize,
    bootstrap_json: Option<String>,
}

impl fmt::Debug for FbOptionsBuilder {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FbOptionsBuilder")
            .field("env_secret", &"[REDACTED]")
            .field("streaming_url", &UrlInputSummary(&self.streaming_url))
            .field("event_url", &UrlInputSummary(&self.event_url))
            .field("start_wait", &self.start_wait)
            .field("offline", &self.offline)
            .field("disable_events", &self.disable_events)
            .field("connect_timeout", &self.connect_timeout)
            .field("close_timeout", &self.close_timeout)
            .field("keep_alive_interval", &self.keep_alive_interval)
            .field("reconnect_delays", &self.reconnect_delays)
            .field("auto_flush_interval", &self.auto_flush_interval)
            .field("flush_timeout", &self.flush_timeout)
            .field("event_request_timeout", &self.event_request_timeout)
            .field("max_events_in_queue", &self.max_events_in_queue)
            .field("max_events_per_request", &self.max_events_per_request)
            .field("max_send_event_attempts", &self.max_send_event_attempts)
            .field("send_event_retry_interval", &self.send_event_retry_interval)
            .field("max_ws_message_size", &self.max_ws_message_size)
            .field("has_bootstrap", &self.bootstrap_json.is_some())
            .finish()
    }
}

impl FbOptionsBuilder {
    /// Creates a builder with FeatBit-compatible defaults.
    #[must_use]
    pub fn new(env_secret: impl Into<String>) -> Self {
        Self {
            env_secret: env_secret.into(),
            streaming_url: DEFAULT_STREAMING_URL.to_owned(),
            event_url: DEFAULT_EVENT_URL.to_owned(),
            start_wait: Duration::from_secs(5),
            offline: false,
            disable_events: false,
            connect_timeout: Duration::from_secs(3),
            close_timeout: Duration::from_secs(2),
            keep_alive_interval: Duration::from_secs(15),
            reconnect_delays: DEFAULT_RECONNECT_DELAYS.to_vec(),
            auto_flush_interval: Duration::from_secs(5),
            flush_timeout: Duration::from_secs(5),
            event_request_timeout: Duration::from_secs(2),
            max_events_in_queue: 10_000,
            max_events_per_request: 50,
            max_send_event_attempts: 2,
            send_event_retry_interval: Duration::from_millis(200),
            max_ws_message_size: 1024 * 1024,
            bootstrap_json: None,
        }
    }

    /// Sets the WebSocket streaming base URL.
    ///
    /// A reverse-proxy base path is supported. Credentials, query parameters, and fragments are
    /// rejected because the SDK constructs authentication and protocol query parameters itself.
    #[must_use]
    pub fn streaming_url(mut self, url: impl Into<String>) -> Self {
        self.streaming_url = url.into();
        self
    }

    /// Sets the HTTP event service base URL.
    ///
    /// A reverse-proxy base path is supported. Credentials, query parameters, and fragments are
    /// rejected; configure endpoint authentication outside the URL.
    #[must_use]
    pub fn event_url(mut self, url: impl Into<String>) -> Self {
        self.event_url = url.into();
        self
    }

    /// Sets the maximum wait for initial synchronized data during client construction.
    #[must_use]
    pub const fn start_wait(mut self, timeout: Duration) -> Self {
        self.start_wait = timeout;
        self
    }

    /// Enables or disables offline mode.
    #[must_use]
    pub const fn offline(mut self, offline: bool) -> Self {
        self.offline = offline;
        self
    }

    /// Enables or disables analytics event collection.
    #[must_use]
    pub const fn disable_events(mut self, disable: bool) -> Self {
        self.disable_events = disable;
        self
    }

    /// Sets the WebSocket connection timeout.
    #[must_use]
    pub const fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }

    /// Sets the graceful worker and WebSocket close timeout.
    #[must_use]
    pub const fn close_timeout(mut self, timeout: Duration) -> Self {
        self.close_timeout = timeout;
        self
    }

    /// Sets the WebSocket ping interval.
    #[must_use]
    pub const fn keep_alive_interval(mut self, interval: Duration) -> Self {
        self.keep_alive_interval = interval;
        self
    }

    /// Sets reconnect delays. The sequence repeats after its last item.
    #[must_use]
    pub fn reconnect_delays(mut self, delays: impl IntoIterator<Item = Duration>) -> Self {
        self.reconnect_delays = delays.into_iter().collect();
        self
    }

    /// Sets the automatic analytics flush interval.
    #[must_use]
    pub const fn auto_flush_interval(mut self, interval: Duration) -> Self {
        self.auto_flush_interval = interval;
        self
    }

    /// Sets the maximum wait used by explicit flush and close operations.
    #[must_use]
    pub const fn flush_timeout(mut self, timeout: Duration) -> Self {
        self.flush_timeout = timeout;
        self
    }

    /// Sets the total timeout for one event HTTP attempt.
    #[must_use]
    pub const fn event_request_timeout(mut self, timeout: Duration) -> Self {
        self.event_request_timeout = timeout;
        self
    }

    /// Sets the bounded event queue capacity.
    #[must_use]
    pub const fn max_events_in_queue(mut self, capacity: usize) -> Self {
        self.max_events_in_queue = capacity;
        self
    }

    /// Sets the maximum number of events in one HTTP request.
    #[must_use]
    pub const fn max_events_per_request(mut self, capacity: usize) -> Self {
        self.max_events_per_request = capacity;
        self
    }

    /// Sets the maximum number of HTTP attempts for an event batch.
    #[must_use]
    pub const fn max_send_event_attempts(mut self, attempts: usize) -> Self {
        self.max_send_event_attempts = attempts;
        self
    }

    /// Sets the delay between HTTP event retries.
    #[must_use]
    pub const fn send_event_retry_interval(mut self, interval: Duration) -> Self {
        self.send_event_retry_interval = interval;
        self
    }

    /// Sets the maximum accepted WebSocket message size in bytes.
    #[must_use]
    pub const fn max_ws_message_size(mut self, bytes: usize) -> Self {
        self.max_ws_message_size = bytes;
        self
    }

    /// Supplies a `FeatBit` full data-sync envelope for offline evaluation.
    #[must_use]
    pub fn bootstrap_json(mut self, json: impl Into<String>) -> Self {
        self.bootstrap_json = Some(json.into());
        self
    }

    /// Validates all fields and builds immutable options.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError`] when a URL, duration, capacity, secret, retry policy, or bootstrap
    /// payload is invalid.
    pub fn build(self) -> Result<FbOptions, ConfigError> {
        self.validate_secret()?;
        let (streaming_url, event_url) = self.validated_urls()?;
        self.validate_runtime_limits()?;
        let bootstrap = self.validated_bootstrap()?;

        Ok(FbOptions {
            env_secret: Arc::from(self.env_secret),
            streaming_url,
            event_url,
            start_wait: self.start_wait,
            offline: self.offline,
            disable_events: self.disable_events,
            connect_timeout: self.connect_timeout,
            close_timeout: self.close_timeout,
            keep_alive_interval: self.keep_alive_interval,
            reconnect_delays: Arc::from(self.reconnect_delays),
            auto_flush_interval: self.auto_flush_interval,
            flush_timeout: self.flush_timeout,
            event_request_timeout: self.event_request_timeout,
            max_events_in_queue: self.max_events_in_queue,
            max_events_per_request: self.max_events_per_request,
            max_send_event_attempts: self.max_send_event_attempts,
            send_event_retry_interval: self.send_event_retry_interval,
            max_ws_message_size: self.max_ws_message_size,
            bootstrap,
        })
    }

    fn validate_secret(&self) -> Result<(), ConfigError> {
        let trimmed_secret = self.env_secret.trim_end_matches('=');
        if trimmed_secret.len() < 3
            || !trimmed_secret.is_ascii()
            || !trimmed_secret
                .bytes()
                .all(|byte| (0x21..=0x7e).contains(&byte))
        {
            return Err(ConfigError::InvalidEnvironmentSecret);
        }
        Ok(())
    }

    fn validated_urls(&self) -> Result<(Url, Url), ConfigError> {
        let streaming_url = parse_url("streaming_url", &self.streaming_url)?;
        if !matches!(streaming_url.scheme(), "ws" | "wss") {
            return Err(ConfigError::InvalidUrlScheme {
                field: "streaming_url",
                expected: "ws or wss",
            });
        }

        let event_url = parse_url("event_url", &self.event_url)?;
        if !matches!(event_url.scheme(), "http" | "https") {
            return Err(ConfigError::InvalidUrlScheme {
                field: "event_url",
                expected: "http or https",
            });
        }
        Ok((streaming_url, event_url))
    }

    fn validate_runtime_limits(&self) -> Result<(), ConfigError> {
        validate_nonzero_duration("start_wait", self.start_wait)?;
        validate_nonzero_duration("connect_timeout", self.connect_timeout)?;
        validate_nonzero_duration("close_timeout", self.close_timeout)?;
        validate_nonzero_duration("keep_alive_interval", self.keep_alive_interval)?;
        validate_nonzero_duration("auto_flush_interval", self.auto_flush_interval)?;
        validate_nonzero_duration("flush_timeout", self.flush_timeout)?;
        validate_nonzero_duration("event_request_timeout", self.event_request_timeout)?;
        if self.send_event_retry_interval > MAX_CONFIG_DURATION {
            return Err(ConfigError::InvalidDuration {
                field: "send_event_retry_interval",
                message: "must not exceed 365 days",
            });
        }
        if self.start_wait < self.connect_timeout {
            return Err(ConfigError::InvalidDuration {
                field: "start_wait",
                message: "must be greater than or equal to connect_timeout",
            });
        }
        if self.reconnect_delays.is_empty() {
            return Err(ConfigError::EmptyReconnectDelays);
        }
        if self.reconnect_delays.iter().all(Duration::is_zero) {
            return Err(ConfigError::InvalidDuration {
                field: "reconnect_delays",
                message: "must contain at least one non-zero delay",
            });
        }
        if self
            .reconnect_delays
            .iter()
            .any(|delay| *delay > MAX_CONFIG_DURATION)
        {
            return Err(ConfigError::InvalidDuration {
                field: "reconnect_delays",
                message: "individual delays must not exceed 365 days",
            });
        }

        validate_capacity("max_events_in_queue", self.max_events_in_queue, 1_000_000)?;
        validate_capacity(
            "max_events_per_request",
            self.max_events_per_request,
            10_000,
        )?;
        validate_capacity("max_send_event_attempts", self.max_send_event_attempts, 100)?;
        validate_capacity(
            "max_ws_message_size",
            self.max_ws_message_size,
            64 * 1024 * 1024,
        )?;
        Ok(())
    }

    fn validated_bootstrap(&self) -> Result<Option<Arc<DataSyncEnvelope>>, ConfigError> {
        if self.bootstrap_json.is_some() && !self.offline {
            return Err(ConfigError::BootstrapRequiresOffline);
        }
        let Some(json) = self.bootstrap_json.as_deref() else {
            return Ok(None);
        };
        let envelope = serde_json::from_str::<DataSyncEnvelope>(json)
            .map_err(|error| ConfigError::InvalidBootstrap(error.to_string()))?;
        if envelope.message_type != "data-sync" || envelope.data.event_type != "full" {
            return Err(ConfigError::InvalidBootstrap(
                "offline bootstrap must be a full data-sync envelope".to_owned(),
            ));
        }
        Ok(Some(Arc::new(envelope)))
    }
}

fn parse_url(field: &'static str, value: &str) -> Result<Url, ConfigError> {
    Url::parse(value)
        .map_err(|error| ConfigError::InvalidUrl {
            field,
            message: error.to_string(),
        })
        .and_then(|url| {
            let validation_message = if url.host_str().is_none() {
                Some("URL must contain a host")
            } else if !url.username().is_empty() || url.password().is_some() {
                Some("URL must not contain credentials")
            } else if url.query().is_some() {
                Some("URL must not contain a query")
            } else if url.fragment().is_some() {
                Some("URL must not contain a fragment")
            } else {
                None
            };
            if let Some(message) = validation_message {
                Err(ConfigError::InvalidUrl {
                    field,
                    message: message.to_owned(),
                })
            } else {
                Ok(url)
            }
        })
}

struct UrlSummary<'a>(&'a Url);

impl fmt::Debug for UrlSummary<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Url")
            .field("scheme", &self.0.scheme())
            .field("host", &self.0.host_str())
            .field("port", &self.0.port())
            .field("has_base_path", &!matches!(self.0.path(), "" | "/"))
            .field(
                "has_credentials",
                &(!self.0.username().is_empty() || self.0.password().is_some()),
            )
            .field("has_query", &self.0.query().is_some())
            .field("has_fragment", &self.0.fragment().is_some())
            .finish()
    }
}

struct UrlInputSummary<'a>(&'a str);

impl fmt::Debug for UrlInputSummary<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match Url::parse(self.0) {
            Ok(url) => UrlSummary(&url).fmt(formatter),
            Err(_) => formatter.write_str("[INVALID URL REDACTED]"),
        }
    }
}

fn validate_nonzero_duration(field: &'static str, duration: Duration) -> Result<(), ConfigError> {
    if duration.is_zero() {
        Err(ConfigError::InvalidDuration {
            field,
            message: "must be greater than zero",
        })
    } else if duration > MAX_CONFIG_DURATION {
        Err(ConfigError::InvalidDuration {
            field,
            message: "must not exceed 365 days",
        })
    } else {
        Ok(())
    }
}

const fn validate_capacity(
    field: &'static str,
    value: usize,
    maximum: usize,
) -> Result<(), ConfigError> {
    if value == 0 {
        Err(ConfigError::InvalidCapacity {
            field,
            message: "must be greater than zero",
        })
    } else if value > maximum {
        Err(ConfigError::InvalidCapacity {
            field,
            message: "exceeds the supported safety limit",
        })
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
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
        assert!(options.bootstrap.is_none());
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
}
