use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use arc_swap::ArcSwapOption;
use chrono::Utc;
use crossbeam_channel::{bounded, Receiver, Sender, TryRecvError, TrySendError};
use reqwest::Client;
use reqwest::StatusCode;
use serde::Serialize;
use tokio::runtime::{Builder as RuntimeBuilder, Runtime};
use tokio::sync::watch;
use url::Url;

use crate::model::FbUser;
use crate::options::FbOptions;
use crate::user_agent;
use crate::worker::{WorkerThread, WorkerWait};

const MAX_PUBLIC_WAIT: Duration = Duration::from_hours(8_760);

/// A snapshot of a successful flag evaluation that can be delivered explicitly.
///
/// Detail-returning variation methods produce this value in
/// [`crate::EvaluationDetail::evaluation_event`]. It preserves the selected raw variation and
/// experiment attribution decision even if flag data changes before the application records the
/// exposure.
#[derive(Clone, Eq, PartialEq)]
pub struct FbEvaluationEvent {
    flag_key: String,
    variation_id: String,
    variation_value: String,
    timestamp: SystemTime,
    send_to_experiment: bool,
}

impl FbEvaluationEvent {
    /// Creates an explicit evaluation event using the current time.
    ///
    /// Prefer the event returned by a detail variation method when reporting a real SDK evaluation;
    /// it carries the exact variation and experiment decision selected at evaluation time.
    #[must_use]
    pub fn new(
        flag_key: impl Into<String>,
        variation_id: impl Into<String>,
        variation_value: impl Into<String>,
        send_to_experiment: bool,
    ) -> Self {
        Self {
            flag_key: flag_key.into(),
            variation_id: variation_id.into(),
            variation_value: variation_value.into(),
            timestamp: SystemTime::now(),
            send_to_experiment,
        }
    }

    /// Returns the evaluated flag key.
    #[must_use]
    pub fn flag_key(&self) -> &str {
        &self.flag_key
    }

    /// Returns the selected variation ID.
    #[must_use]
    pub fn variation_id(&self) -> &str {
        &self.variation_id
    }

    /// Returns the raw selected variation value.
    #[must_use]
    pub fn variation_value(&self) -> &str {
        &self.variation_value
    }

    /// Returns when the evaluation occurred.
    #[must_use]
    pub const fn timestamp(&self) -> SystemTime {
        self.timestamp
    }

    /// Returns whether this exposure is eligible for experiment attribution.
    #[must_use]
    pub const fn send_to_experiment(&self) -> bool {
        self.send_to_experiment
    }
}

impl fmt::Debug for FbEvaluationEvent {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FbEvaluationEvent")
            .field("flag_key", &self.flag_key)
            .field("variation_id", &self.variation_id)
            .field("variation_value", &"[REDACTED]")
            .field("timestamp", &self.timestamp)
            .field("send_to_experiment", &self.send_to_experiment)
            .finish()
    }
}

#[derive(Debug)]
pub(crate) enum EventProcessor {
    Disabled,
    Active(Arc<EventProcessorInner>),
}

impl EventProcessor {
    pub(crate) fn new(options: &FbOptions) -> Self {
        if options.offline || (options.disable_events && !options.allow_track) {
            return Self::Disabled;
        }

        let (sender, receiver) = bounded(options.max_events_in_queue);
        let (shutdown_sender, shutdown_receiver) = bounded(2);
        let (abort_sender, abort_receiver) = watch::channel(false);
        let delivery_stopped = Arc::new(AtomicBool::new(false));
        let worker_delivery_stopped = Arc::clone(&delivery_stopped);
        let worker_config = EventWorkerConfig::from_options(options);
        let worker = WorkerThread::spawn("event processor", move || {
            let runtime = RuntimeBuilder::new_current_thread().enable_all().build();
            match runtime {
                Ok(runtime) => EventWorker::new(worker_config, worker_delivery_stopped).run(
                    &runtime,
                    &receiver,
                    &shutdown_receiver,
                    abort_receiver,
                ),
                Err(error) => {
                    worker_delivery_stopped.store(true, Ordering::Release);
                    log::error!("failed to start FeatBit event runtime: {error}");
                }
            }
        });

        match worker {
            Ok(worker) => Self::Active(Arc::new(EventProcessorInner {
                sender: ArcSwapOption::from(Some(Arc::new(sender))),
                shutdown_sender,
                abort_sender,
                closed: AtomicBool::new(false),
                capacity_exceeded: AtomicBool::new(false),
                delivery_stopped,
                worker,
                flush_timeout: options.flush_timeout,
            })),
            Err(error) => {
                log::error!("failed to start FeatBit event processor: {error}");
                Self::Disabled
            }
        }
    }

    pub(crate) fn record_evaluation(&self, user: &FbUser, event: &FbEvaluationEvent) -> bool {
        if user.key().is_empty()
            || event.flag_key.trim().is_empty()
            || event.variation_id.trim().is_empty()
        {
            log::debug!("discarding invalid FeatBit evaluation event");
            return false;
        }
        self.record(PayloadEvent::evaluation(user, event))
    }

    pub(crate) fn record_metric(
        &self,
        user: &FbUser,
        event_name: &str,
        numeric_value: f64,
    ) -> bool {
        if user.key().is_empty() || event_name.trim().is_empty() || !numeric_value.is_finite() {
            log::debug!("discarding invalid FeatBit metric event");
            return false;
        }
        self.record(PayloadEvent::metric(user, event_name, numeric_value))
    }

    fn record(&self, event: PayloadEvent) -> bool {
        let Self::Active(inner) = self else {
            return false;
        };
        if inner.closed.load(Ordering::Acquire) || inner.delivery_stopped.load(Ordering::Acquire) {
            return false;
        }

        let Some(sender) = inner.sender.load_full() else {
            return false;
        };
        match sender.try_send(EventMessage::Payload(event)) {
            Ok(()) => {
                inner.capacity_exceeded.store(false, Ordering::Release);
                true
            }
            Err(TrySendError::Full(_)) => {
                if !inner.capacity_exceeded.swap(true, Ordering::AcqRel) {
                    log::warn!(
                        "FeatBit events are being produced faster than they can be processed; events will be dropped"
                    );
                }
                false
            }
            Err(TrySendError::Disconnected(_)) => false,
        }
    }

    pub(crate) fn flush(&self) {
        let Self::Active(inner) = self else {
            return;
        };
        if !inner.closed.load(Ordering::Acquire) {
            if let Some(sender) = inner.sender.load_full() {
                let _ignored = sender.try_send(EventMessage::Flush(None));
            }
        }
    }

    pub(crate) fn flush_and_wait(&self, timeout: Duration) -> bool {
        let Self::Active(inner) = self else {
            return true;
        };
        if inner.closed.load(Ordering::Acquire) {
            return false;
        }

        let timeout = timeout.min(MAX_PUBLIC_WAIT);
        let deadline = Instant::now()
            .checked_add(timeout)
            .unwrap_or_else(Instant::now);
        let (reply_sender, reply_receiver) = bounded(1);
        let Some(sender) = inner.sender.load_full() else {
            return false;
        };
        if sender
            .send_timeout(EventMessage::Flush(Some(reply_sender)), timeout)
            .is_err()
        {
            return false;
        }
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .unwrap_or(Duration::ZERO);
        reply_receiver.recv_timeout(remaining).unwrap_or(false)
    }

    pub(crate) fn close(&self) {
        let Self::Active(inner) = self else {
            return;
        };
        inner.close();
    }
}

#[derive(Debug)]
pub(crate) struct EventProcessorInner {
    sender: ArcSwapOption<Sender<EventMessage>>,
    shutdown_sender: Sender<Shutdown>,
    abort_sender: watch::Sender<bool>,
    closed: AtomicBool,
    capacity_exceeded: AtomicBool,
    delivery_stopped: Arc<AtomicBool>,
    worker: WorkerThread,
    flush_timeout: Duration,
}

impl EventProcessorInner {
    fn close(&self) {
        if self.closed.swap(true, Ordering::AcqRel) {
            let _ignored = self.worker.wait(Duration::ZERO);
            return;
        }

        let timeout = self.flush_timeout.min(MAX_PUBLIC_WAIT);
        let abort_budget = (timeout / 4).min(Duration::from_millis(100));
        let graceful_budget = timeout.saturating_sub(abort_budget);

        self.sender.store(None);
        let _ignored = self.shutdown_sender.try_send(Shutdown::Graceful);

        match self.worker.wait(graceful_budget) {
            WorkerWait::Completed => return,
            WorkerWait::Panicked => {
                log::warn!("FeatBit event processor stopped after a worker panic");
                return;
            }
            WorkerWait::TimedOut => {}
        }

        let _ignored = self.abort_sender.send(true);
        let _ignored = self.shutdown_sender.try_send(Shutdown::Abort);
        match self.worker.wait(abort_budget) {
            WorkerWait::Completed => {
                log::warn!(
                    "FeatBit event processor exceeded its graceful flush budget and was cancelled"
                );
            }
            WorkerWait::Panicked => {
                log::warn!("FeatBit event processor stopped after a worker panic");
            }
            WorkerWait::TimedOut => {
                log::warn!("FeatBit event processor did not close within the configured timeout");
            }
        }
    }
}

impl Drop for EventProcessorInner {
    fn drop(&mut self) {
        self.close();
    }
}

#[derive(Debug)]
enum EventMessage {
    Payload(PayloadEvent),
    Flush(Option<Sender<bool>>),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Shutdown {
    Graceful,
    Abort,
}

#[derive(Clone, Debug, Serialize)]
#[serde(untagged)]
enum PayloadEvent {
    Evaluation(EvaluationPayload),
    Metric(MetricPayload),
}

impl PayloadEvent {
    fn evaluation(user: &FbUser, event: &FbEvaluationEvent) -> Self {
        Self::Evaluation(EvaluationPayload {
            user: EventUser::from(user),
            variations: vec![EvaluationVariation {
                feature_flag_key: event.flag_key.clone(),
                variation: EventVariation {
                    id: event.variation_id.clone(),
                    value: event.variation_value.clone(),
                },
                timestamp: unix_millis(event.timestamp),
                send_to_experiment: event.send_to_experiment,
            }],
        })
    }

    fn metric(user: &FbUser, event_name: &str, numeric_value: f64) -> Self {
        Self::Metric(MetricPayload {
            user: EventUser::from(user),
            metrics: vec![Metric {
                app_type: "rust-server-side",
                route: "index/metric",
                event_type: "CustomEvent",
                event_name: event_name.to_owned(),
                numeric_value,
                timestamp: Utc::now().timestamp_millis(),
            }],
        })
    }
}

fn unix_millis(timestamp: SystemTime) -> i64 {
    match timestamp.duration_since(UNIX_EPOCH) {
        Ok(duration) => i64::try_from(duration.as_millis()).unwrap_or(i64::MAX),
        Err(error) => -i64::try_from(error.duration().as_millis()).unwrap_or(i64::MAX),
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct EventUser {
    key_id: String,
    name: String,
    customized_properties: Vec<CustomizedProperty>,
}

impl From<&FbUser> for EventUser {
    fn from(user: &FbUser) -> Self {
        Self {
            key_id: user.key().to_owned(),
            name: user.name().to_owned(),
            customized_properties: user
                .custom()
                .iter()
                .map(|(name, value)| CustomizedProperty {
                    name: name.clone(),
                    value: value.clone(),
                })
                .collect(),
        }
    }
}

#[derive(Clone, Debug, Serialize)]
struct CustomizedProperty {
    name: String,
    value: String,
}

#[derive(Clone, Debug, Serialize)]
struct EvaluationPayload {
    user: EventUser,
    variations: Vec<EvaluationVariation>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct EvaluationVariation {
    feature_flag_key: String,
    variation: EventVariation,
    timestamp: i64,
    send_to_experiment: bool,
}

#[derive(Clone, Debug, Serialize)]
struct EventVariation {
    id: String,
    value: String,
}

#[derive(Clone, Debug, Serialize)]
struct MetricPayload {
    user: EventUser,
    metrics: Vec<Metric>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct Metric {
    app_type: &'static str,
    route: &'static str,
    #[serde(rename = "type")]
    event_type: &'static str,
    event_name: String,
    numeric_value: f64,
    timestamp: i64,
}

#[derive(Clone, Debug)]
struct EventWorkerConfig {
    endpoint: Url,
    env_secret: Arc<str>,
    auto_flush_interval: Duration,
    request_timeout: Duration,
    max_events_per_request: usize,
    max_attempts: usize,
    retry_interval: Duration,
}

impl EventWorkerConfig {
    fn from_options(options: &FbOptions) -> Self {
        Self {
            endpoint: event_endpoint(&options.event_url),
            env_secret: Arc::clone(&options.env_secret),
            auto_flush_interval: options.auto_flush_interval,
            request_timeout: options.event_request_timeout,
            max_events_per_request: options.max_events_per_request,
            max_attempts: options.max_send_event_attempts,
            retry_interval: options.send_event_retry_interval,
        }
    }
}

struct EventWorker {
    config: EventWorkerConfig,
    client: Option<Client>,
    buffer: Vec<PayloadEvent>,
    delivery_stopped: bool,
    shared_delivery_stopped: Arc<AtomicBool>,
    unreported_delivery_failure: bool,
    last_flush: Instant,
}

impl EventWorker {
    fn new(config: EventWorkerConfig, shared_delivery_stopped: Arc<AtomicBool>) -> Self {
        let client = Client::builder()
            .connect_timeout(config.request_timeout.min(Duration::from_secs(1)))
            .timeout(config.request_timeout)
            .build()
            .map_err(|error| {
                log::error!("failed to build FeatBit event HTTP client: {error}");
                error
            })
            .ok();
        Self {
            config,
            client,
            buffer: Vec::new(),
            delivery_stopped: false,
            shared_delivery_stopped,
            unreported_delivery_failure: false,
            last_flush: Instant::now(),
        }
    }

    fn run(
        mut self,
        runtime: &Runtime,
        receiver: &Receiver<EventMessage>,
        shutdown: &Receiver<Shutdown>,
        mut abort: watch::Receiver<bool>,
    ) {
        log::debug!("FeatBit event processor started");
        let mut closing = false;

        'worker: loop {
            match shutdown.try_recv() {
                Ok(Shutdown::Graceful) => closing = true,
                Ok(Shutdown::Abort) | Err(TryRecvError::Disconnected) => break,
                Err(TryRecvError::Empty) => {}
            }

            if closing {
                crossbeam_channel::select! {
                    recv(shutdown) -> message => match message {
                        Ok(Shutdown::Graceful) => {}
                        Ok(Shutdown::Abort) | Err(_) => break 'worker,
                    },
                    recv(receiver) -> message => if let Ok(message) = message {
                        if !self.process(message, runtime, &mut abort) {
                            break 'worker;
                        }
                    } else {
                        let _completed = self.flush(runtime, &mut abort);
                        break 'worker;
                    },
                }
                continue;
            }

            let elapsed = self.last_flush.elapsed();
            let wait = self
                .config
                .auto_flush_interval
                .checked_sub(elapsed)
                .unwrap_or(Duration::ZERO);

            crossbeam_channel::select! {
                recv(shutdown) -> message => match message {
                    Ok(Shutdown::Graceful) => closing = true,
                    Ok(Shutdown::Abort) | Err(_) => break 'worker,
                },
                recv(receiver) -> message => if let Ok(message) = message {
                    if !self.process(message, runtime, &mut abort) {
                        break 'worker;
                    }
                } else {
                    let _completed = self.flush(runtime, &mut abort);
                    break 'worker;
                },
                default(wait) => {
                    let outcome = self.flush(runtime, &mut abort);
                    self.remember_delivery_failure(outcome);
                    if !outcome.keep_running {
                        break 'worker;
                    }
                },
            }
        }
        log::debug!("FeatBit event processor stopped");
    }

    fn process(
        &mut self,
        message: EventMessage,
        runtime: &Runtime,
        abort: &mut watch::Receiver<bool>,
    ) -> bool {
        match message {
            EventMessage::Payload(event) => {
                if !self.delivery_stopped {
                    self.buffer.push(event);
                    if self.buffer.len() >= self.config.max_events_per_request {
                        let outcome = self.flush(runtime, abort);
                        self.remember_delivery_failure(outcome);
                        return outcome.keep_running;
                    }
                }
                true
            }
            EventMessage::Flush(reply) => {
                let outcome = self.flush(runtime, abort);
                let delivered = outcome.delivered && !self.unreported_delivery_failure;
                self.unreported_delivery_failure = false;
                if let Some(reply) = reply {
                    let _ignored = reply.send(delivered);
                }
                outcome.keep_running
            }
        }
    }

    fn remember_delivery_failure(&mut self, outcome: FlushOutcome) {
        if !outcome.delivered {
            self.unreported_delivery_failure = true;
        }
    }

    fn flush(&mut self, runtime: &Runtime, abort: &mut watch::Receiver<bool>) -> FlushOutcome {
        self.last_flush = Instant::now();
        if self.delivery_stopped {
            self.buffer.clear();
            return FlushOutcome::failed();
        }
        if self.buffer.is_empty() {
            return FlushOutcome::delivered();
        }

        let events = std::mem::take(&mut self.buffer);
        log::debug!("flushing {} FeatBit events", events.len());
        let mut delivered = true;
        for chunk in events.chunks(self.config.max_events_per_request) {
            let payload = match serde_json::to_vec(chunk) {
                Ok(payload) => payload,
                Err(error) => {
                    log::warn!("failed to serialize FeatBit events: {error}");
                    delivered = false;
                    continue;
                }
            };
            match runtime.block_on(self.send(&payload, abort)) {
                Delivery::Fatal => {
                    self.delivery_stopped = true;
                    self.shared_delivery_stopped.store(true, Ordering::Release);
                    delivered = false;
                    break;
                }
                Delivery::Cancelled => return FlushOutcome::cancelled(),
                Delivery::Failed => delivered = false,
                Delivery::Succeeded => {}
            }
        }
        FlushOutcome {
            delivered,
            keep_running: true,
        }
    }

    async fn send(&self, payload: &[u8], abort: &mut watch::Receiver<bool>) -> Delivery {
        let Some(client) = &self.client else {
            return Delivery::Fatal;
        };

        for attempt in 0..self.config.max_attempts {
            if attempt > 0 {
                tokio::select! {
                    biased;
                    () = wait_for_abort(abort) => return Delivery::Cancelled,
                    () = tokio::time::sleep(self.config.retry_interval) => {}
                }
            }
            let request = client
                .post(self.config.endpoint.clone())
                .header(
                    reqwest::header::AUTHORIZATION,
                    self.config.env_secret.as_ref(),
                )
                .header(reqwest::header::USER_AGENT, user_agent())
                .header(
                    reqwest::header::CONTENT_TYPE,
                    "application/json; charset=utf-8",
                )
                .body(payload.to_vec())
                .send();
            let response = tokio::select! {
                biased;
                () = wait_for_abort(abort) => return Delivery::Cancelled,
                response = request => response,
            };

            match response {
                Ok(response) if response.status().is_success() => return Delivery::Succeeded,
                Ok(response) => {
                    let status = response.status();
                    log::warn!("FeatBit event request failed with HTTP status {status}");
                    if !is_recoverable(status) {
                        log::error!(
                            "FeatBit event delivery stopped after unrecoverable HTTP status {status}"
                        );
                        return Delivery::Fatal;
                    }
                }
                Err(error) => {
                    log::warn!(
                        "FeatBit event request failed ({})",
                        request_error_kind(&error)
                    );
                }
            }
        }

        log::warn!("FeatBit event delivery exhausted its configured retry attempts");
        Delivery::Failed
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct FlushOutcome {
    delivered: bool,
    keep_running: bool,
}

impl FlushOutcome {
    const fn delivered() -> Self {
        Self {
            delivered: true,
            keep_running: true,
        }
    }

    const fn failed() -> Self {
        Self {
            delivered: false,
            keep_running: true,
        }
    }

    const fn cancelled() -> Self {
        Self {
            delivered: false,
            keep_running: false,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Delivery {
    Succeeded,
    Failed,
    Fatal,
    Cancelled,
}

async fn wait_for_abort(abort: &mut watch::Receiver<bool>) {
    loop {
        if *abort.borrow() {
            return;
        }
        if abort.changed().await.is_err() {
            return;
        }
    }
}

fn request_error_kind(error: &reqwest::Error) -> &'static str {
    if error.is_timeout() {
        "timeout"
    } else if error.is_connect() {
        "connection error"
    } else if error.is_body() {
        "request body error"
    } else {
        "transport error"
    }
}

fn is_recoverable(status: StatusCode) -> bool {
    if status.is_client_error() {
        matches!(
            status,
            StatusCode::BAD_REQUEST | StatusCode::REQUEST_TIMEOUT | StatusCode::TOO_MANY_REQUESTS
        )
    } else {
        true
    }
}

fn event_endpoint(base: &Url) -> Url {
    let mut endpoint = base.clone();
    let base_path = endpoint.path().trim_end_matches('/');
    endpoint.set_path(&format!("{base_path}/api/public/insight/track"));
    endpoint.set_query(None);
    endpoint.set_fragment(None);
    endpoint
}

#[cfg(test)]
mod tests {
    use std::io::ErrorKind;
    use std::net::TcpListener;
    use std::sync::Barrier;
    use std::thread;

    use mockito::Matcher;
    use serde_json::Value;

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
}
