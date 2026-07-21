use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use chrono::Utc;
use crossbeam_channel::{bounded, Receiver, RecvTimeoutError, Sender};
use parking_lot::Mutex;
use reqwest::blocking::Client;
use reqwest::StatusCode;
use serde::Serialize;
use url::Url;

use crate::model::{FbUser, Variation};
use crate::options::FbOptions;
use crate::user_agent;

const MAX_PUBLIC_WAIT: Duration = Duration::from_hours(8_760);

#[derive(Debug)]
pub(crate) enum EventProcessor {
    Disabled,
    Active(Arc<EventProcessorInner>),
}

impl EventProcessor {
    pub(crate) fn new(options: &FbOptions) -> Self {
        if options.offline || options.disable_events {
            return Self::Disabled;
        }

        let (sender, receiver) = bounded(options.max_events_in_queue);
        let worker_config = EventWorkerConfig::from_options(options);
        let worker = thread::Builder::new()
            .name("featbit-event-processor".to_owned())
            .spawn(move || EventWorker::new(worker_config).run(&receiver));

        match worker {
            Ok(worker) => Self::Active(Arc::new(EventProcessorInner {
                sender,
                closed: AtomicBool::new(false),
                capacity_exceeded: AtomicBool::new(false),
                worker: Mutex::new(Some(worker)),
                flush_timeout: options.flush_timeout,
            })),
            Err(error) => {
                log::error!("failed to start FeatBit event processor: {error}");
                Self::Disabled
            }
        }
    }

    pub(crate) fn record_evaluation(
        &self,
        user: &FbUser,
        flag_key: &str,
        variation: &Variation,
        send_to_experiment: bool,
    ) -> bool {
        self.record(PayloadEvent::evaluation(
            user,
            flag_key,
            variation,
            send_to_experiment,
        ))
    }

    pub(crate) fn record_metric(
        &self,
        user: &FbUser,
        event_name: &str,
        numeric_value: f64,
    ) -> bool {
        if event_name.trim().is_empty() || !numeric_value.is_finite() {
            log::debug!("discarding invalid FeatBit metric event");
            return false;
        }
        self.record(PayloadEvent::metric(user, event_name, numeric_value))
    }

    fn record(&self, event: PayloadEvent) -> bool {
        let Self::Active(inner) = self else {
            return false;
        };
        if inner.closed.load(Ordering::Acquire) {
            return false;
        }

        if inner.sender.try_send(EventMessage::Payload(event)).is_ok() {
            inner.capacity_exceeded.store(false, Ordering::Release);
            true
        } else {
            if !inner.capacity_exceeded.swap(true, Ordering::AcqRel) {
                log::warn!(
                    "FeatBit events are being produced faster than they can be processed; events will be dropped"
                );
            }
            false
        }
    }

    pub(crate) fn flush(&self) {
        let Self::Active(inner) = self else {
            return;
        };
        if !inner.closed.load(Ordering::Acquire) {
            let _ignored = inner.sender.try_send(EventMessage::Flush(None));
        }
    }

    pub(crate) fn flush_and_wait(&self, timeout: Duration) -> bool {
        let Self::Active(inner) = self else {
            return true;
        };
        if inner.closed.load(Ordering::Acquire) {
            return true;
        }

        let timeout = timeout.min(MAX_PUBLIC_WAIT);
        let deadline = Instant::now()
            .checked_add(timeout)
            .unwrap_or_else(Instant::now);
        let (reply_sender, reply_receiver) = bounded(1);
        if inner
            .sender
            .send_timeout(EventMessage::Flush(Some(reply_sender)), timeout)
            .is_err()
        {
            return false;
        }
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .unwrap_or(Duration::ZERO);
        reply_receiver.recv_timeout(remaining).is_ok()
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
    sender: Sender<EventMessage>,
    closed: AtomicBool,
    capacity_exceeded: AtomicBool,
    worker: Mutex<Option<JoinHandle<()>>>,
    flush_timeout: Duration,
}

impl EventProcessorInner {
    fn close(&self) {
        if self.closed.swap(true, Ordering::AcqRel) {
            return;
        }

        let timeout = self.flush_timeout.min(MAX_PUBLIC_WAIT);
        let deadline = Instant::now()
            .checked_add(timeout)
            .unwrap_or_else(Instant::now);
        let (reply_sender, reply_receiver) = bounded(1);
        let sent = self
            .sender
            .send_timeout(EventMessage::Close(reply_sender), timeout);
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .unwrap_or(Duration::ZERO);
        let completed = sent.is_ok() && reply_receiver.recv_timeout(remaining).is_ok();

        let worker = self.worker.lock().take();
        if completed {
            if let Some(worker) = worker {
                if worker.join().is_err() {
                    log::warn!("FeatBit event processor stopped after a worker panic");
                }
            }
        } else {
            log::warn!("FeatBit event processor did not close within the configured timeout");
            drop(worker);
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
    Flush(Option<Sender<()>>),
    Close(Sender<()>),
}

#[derive(Clone, Debug, Serialize)]
#[serde(untagged)]
enum PayloadEvent {
    Evaluation(EvaluationPayload),
    Metric(MetricPayload),
}

impl PayloadEvent {
    fn evaluation(
        user: &FbUser,
        flag_key: &str,
        variation: &Variation,
        send_to_experiment: bool,
    ) -> Self {
        Self::Evaluation(EvaluationPayload {
            user: EventUser::from(user),
            variations: vec![EvaluationVariation {
                feature_flag_key: flag_key.to_owned(),
                variation: EventVariation {
                    id: variation.id.clone(),
                    value: variation.value.clone(),
                },
                timestamp: Utc::now().timestamp_millis(),
                send_to_experiment,
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
    last_flush: Instant,
}

impl EventWorker {
    fn new(config: EventWorkerConfig) -> Self {
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
            last_flush: Instant::now(),
        }
    }

    fn run(mut self, receiver: &Receiver<EventMessage>) {
        log::debug!("FeatBit event processor started");
        loop {
            let elapsed = self.last_flush.elapsed();
            let wait = self
                .config
                .auto_flush_interval
                .checked_sub(elapsed)
                .unwrap_or(Duration::ZERO);
            match receiver.recv_timeout(wait) {
                Ok(EventMessage::Payload(event)) => {
                    if !self.delivery_stopped {
                        self.buffer.push(event);
                        if self.buffer.len() >= self.config.max_events_per_request {
                            self.flush();
                        }
                    }
                }
                Ok(EventMessage::Flush(reply)) => {
                    self.flush();
                    if let Some(reply) = reply {
                        let _ignored = reply.send(());
                    }
                }
                Ok(EventMessage::Close(reply)) => {
                    self.flush();
                    let _ignored = reply.send(());
                    break;
                }
                Err(RecvTimeoutError::Timeout) => self.flush(),
                Err(RecvTimeoutError::Disconnected) => {
                    self.flush();
                    break;
                }
            }
        }
        log::debug!("FeatBit event processor stopped");
    }

    fn flush(&mut self) {
        self.last_flush = Instant::now();
        if self.buffer.is_empty() || self.delivery_stopped {
            self.buffer.clear();
            return;
        }

        let events = std::mem::take(&mut self.buffer);
        log::debug!("flushing {} FeatBit events", events.len());
        for chunk in events.chunks(self.config.max_events_per_request) {
            let payload = match serde_json::to_vec(chunk) {
                Ok(payload) => payload,
                Err(error) => {
                    log::warn!("failed to serialize FeatBit events: {error}");
                    continue;
                }
            };
            if self.send(&payload) == Delivery::Fatal {
                self.delivery_stopped = true;
                break;
            }
        }
    }

    fn send(&self, payload: &[u8]) -> Delivery {
        let Some(client) = &self.client else {
            return Delivery::Fatal;
        };

        for attempt in 0..self.config.max_attempts {
            if attempt > 0 {
                thread::sleep(self.config.retry_interval);
            }
            let response = client
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
                    log::warn!("FeatBit event request failed: {error}");
                }
            }
        }

        log::warn!("FeatBit event delivery exhausted its configured retry attempts");
        Delivery::Failed
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Delivery {
    Succeeded,
    Failed,
    Fatal,
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
    use mockito::Matcher;
    use serde_json::Value;

    use super::*;

    fn test_user() -> FbUser {
        FbUser::builder("u1")
            .name("Ada")
            .custom("country", "cn")
            .build()
    }

    #[test]
    fn evaluation_wire_shape_matches_featbit() {
        let event = PayloadEvent::evaluation(
            &test_user(),
            "checkout",
            &Variation {
                id: "on-id".to_owned(),
                value: "true".to_owned(),
            },
            true,
        );
        let value = serde_json::to_value(event).expect("event should serialize");
        assert_eq!(value["user"]["keyId"], "u1");
        assert_eq!(value["variations"][0]["featureFlagKey"], "checkout");
        assert_eq!(value["variations"][0]["variation"]["id"], "on-id");
        assert_eq!(value["variations"][0]["sendToExperiment"], true);
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
        request.assert();
    }
}
