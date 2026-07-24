use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use url::Url;

use crate::model::DataSyncEnvelope;
use crate::observation::EvaluationObserver;

#[cfg(test)]
use crate::error::ConfigError;

mod validation;

const DEFAULT_STREAMING_URL: &str = "ws://localhost:5100";
const DEFAULT_EVENT_URL: &str = "http://localhost:5100";
// Leaves room for a default 50-event batch containing unusually large variation values while
// keeping total retained analytics memory finite.
const DEFAULT_MAX_EVENT_QUEUE_SIZE_BYTES: usize = 64 * 1024 * 1024;
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

/// Immutable configuration for [`crate::FbClient`].
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
    pub(crate) allow_track: bool,
    pub(crate) evaluation_observer: Option<Arc<dyn EvaluationObserver>>,
    pub(crate) connect_timeout: Duration,
    pub(crate) close_timeout: Duration,
    pub(crate) keep_alive_interval: Duration,
    pub(crate) reconnect_delays: Arc<[Duration]>,
    pub(crate) auto_flush_interval: Duration,
    pub(crate) flush_timeout: Duration,
    pub(crate) event_request_timeout: Duration,
    pub(crate) max_events_in_queue: usize,
    pub(crate) max_event_queue_size_bytes: usize,
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

    /// Returns whether automatic evaluation-event collection is disabled.
    #[must_use]
    pub const fn events_disabled(&self) -> bool {
        self.disable_events
    }

    /// Returns whether explicit evaluation and metric tracking calls are allowed.
    #[must_use]
    pub const fn track_allowed(&self) -> bool {
        self.allow_track
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
            .field("allow_track", &self.allow_track)
            .field(
                "has_evaluation_observer",
                &self.evaluation_observer.is_some(),
            )
            .field("connect_timeout", &self.connect_timeout)
            .field("close_timeout", &self.close_timeout)
            .field("keep_alive_interval", &self.keep_alive_interval)
            .field("reconnect_delays", &self.reconnect_delays)
            .field("auto_flush_interval", &self.auto_flush_interval)
            .field("flush_timeout", &self.flush_timeout)
            .field("event_request_timeout", &self.event_request_timeout)
            .field("max_events_in_queue", &self.max_events_in_queue)
            .field(
                "max_event_queue_size_bytes",
                &self.max_event_queue_size_bytes,
            )
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
    allow_track: bool,
    evaluation_observer: Option<Arc<dyn EvaluationObserver>>,
    connect_timeout: Duration,
    close_timeout: Duration,
    keep_alive_interval: Duration,
    reconnect_delays: Vec<Duration>,
    auto_flush_interval: Duration,
    flush_timeout: Duration,
    event_request_timeout: Duration,
    max_events_in_queue: usize,
    max_event_queue_size_bytes: usize,
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
            .field("allow_track", &self.allow_track)
            .field(
                "has_evaluation_observer",
                &self.evaluation_observer.is_some(),
            )
            .field("connect_timeout", &self.connect_timeout)
            .field("close_timeout", &self.close_timeout)
            .field("keep_alive_interval", &self.keep_alive_interval)
            .field("reconnect_delays", &self.reconnect_delays)
            .field("auto_flush_interval", &self.auto_flush_interval)
            .field("flush_timeout", &self.flush_timeout)
            .field("event_request_timeout", &self.event_request_timeout)
            .field("max_events_in_queue", &self.max_events_in_queue)
            .field(
                "max_event_queue_size_bytes",
                &self.max_event_queue_size_bytes,
            )
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
            allow_track: true,
            evaluation_observer: None,
            connect_timeout: Duration::from_secs(3),
            close_timeout: Duration::from_secs(2),
            keep_alive_interval: Duration::from_secs(15),
            reconnect_delays: DEFAULT_RECONNECT_DELAYS.to_vec(),
            auto_flush_interval: Duration::from_secs(5),
            flush_timeout: Duration::from_secs(5),
            event_request_timeout: Duration::from_secs(2),
            max_events_in_queue: 10_000,
            max_event_queue_size_bytes: DEFAULT_MAX_EVENT_QUEUE_SIZE_BYTES,
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

    /// Configures automatic evaluation events and explicit tracking calls.
    ///
    /// `disable` controls automatic evaluation events. `allow_track` independently controls
    /// explicit calls to [`crate::FbClient::track_eval_event`] and
    /// [`crate::FbClient::track_metric_event`]. The event processor is completely disabled only
    /// when `disable` is `true` and `allow_track` is `false`.
    ///
    /// The default is `disable = false, allow_track = true`.
    #[must_use]
    pub const fn disable_events(mut self, disable: bool, allow_track: bool) -> Self {
        self.disable_events = disable;
        self.allow_track = allow_track;
        self
    }

    /// Registers a transport-neutral observer for evaluation attempts.
    ///
    /// The observer is independent from `FeatBit` analytics configuration and is therefore called
    /// even when event delivery is disabled. It runs synchronously on the evaluation thread and
    /// must return promptly without performing blocking network I/O.
    #[must_use]
    pub fn evaluation_observer(mut self, observer: impl EvaluationObserver + 'static) -> Self {
        self.evaluation_observer = Some(Arc::new(observer));
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

    /// Sets the approximate retained-memory budget for pending event payloads, in bytes.
    ///
    /// The budget covers event data in the queue, the worker batch, and an event batch being sent.
    /// An event that would exceed the remaining budget is dropped whole; the SDK never truncates
    /// user attributes or variation values. JSON serialization can temporarily require additional
    /// memory while a batch is sent.
    #[must_use]
    pub const fn max_event_queue_size_bytes(mut self, bytes: usize) -> Self {
        self.max_event_queue_size_bytes = bytes;
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

#[cfg(test)]
mod tests;
