use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use crossbeam_channel::{Receiver, TryRecvError};
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION, CONTENT_TYPE, USER_AGENT};
use reqwest::{Client, StatusCode};
use tokio::runtime::Runtime;
use tokio::sync::watch;
use url::Url;

use crate::options::FbOptions;
use crate::user_agent;

use super::{EventMessage, PendingEvent, Shutdown};

#[derive(Clone, Debug)]
pub(super) struct EventWorkerConfig {
    endpoint: Url,
    env_secret: Arc<str>,
    auto_flush_interval: Duration,
    request_timeout: Duration,
    max_events_per_request: usize,
    max_attempts: usize,
    retry_interval: Duration,
}

impl EventWorkerConfig {
    pub(super) fn from_options(options: &FbOptions) -> Self {
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

pub(super) struct EventWorker {
    config: EventWorkerConfig,
    client: Option<Client>,
    buffer: Vec<PendingEvent>,
    delivery_stopped: bool,
    shared_delivery_stopped: Arc<AtomicBool>,
    unreported_delivery_failure: bool,
    last_flush: Instant,
}

impl EventWorker {
    pub(super) fn new(config: EventWorkerConfig, shared_delivery_stopped: Arc<AtomicBool>) -> Self {
        let client = build_http_client(&config);
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

    pub(super) fn run(
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
            EventMessage::Payload(mut event) => {
                event.mark_dequeued();
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

        let mut events = std::mem::take(&mut self.buffer);
        log::debug!("flushing {} FeatBit events", events.len());
        let outcome = self.deliver_events(&events, runtime, abort);
        events.clear();
        self.buffer = events;
        outcome
    }

    fn deliver_events(
        &mut self,
        events: &[PendingEvent],
        runtime: &Runtime,
        abort: &mut watch::Receiver<bool>,
    ) -> FlushOutcome {
        let mut delivered = true;
        for chunk in events.chunks(self.config.max_events_per_request) {
            let payload = match serialize_events(chunk) {
                Ok(payload) => payload,
                Err(error) => {
                    log::warn!("failed to serialize FeatBit events: {error}");
                    delivered = false;
                    continue;
                }
            };
            match runtime.block_on(self.send(payload, abort)) {
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

    async fn send(&self, payload: Bytes, abort: &mut watch::Receiver<bool>) -> Delivery {
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
                .body(payload.clone())
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

fn serialize_events(events: &[PendingEvent]) -> serde_json::Result<Bytes> {
    let estimated_size = events.iter().fold(2_usize, |size, event| {
        size.saturating_add(event.retained_bytes())
    });
    let mut payload = Vec::with_capacity(estimated_size);
    serde_json::to_writer(&mut payload, events)?;
    Ok(Bytes::from(payload))
}

fn build_http_client(config: &EventWorkerConfig) -> Option<Client> {
    let Ok(mut authorization) = HeaderValue::from_bytes(config.env_secret.as_bytes()) else {
        log::error!("failed to build FeatBit event authorization header");
        return None;
    };
    authorization.set_sensitive(true);
    let Ok(agent) = HeaderValue::from_bytes(user_agent().as_bytes()) else {
        log::error!("failed to build FeatBit event user-agent header");
        return None;
    };
    let mut headers = HeaderMap::with_capacity(3);
    headers.insert(AUTHORIZATION, authorization);
    headers.insert(USER_AGENT, agent);
    headers.insert(
        CONTENT_TYPE,
        HeaderValue::from_static("application/json; charset=utf-8"),
    );

    Client::builder()
        .connect_timeout(config.request_timeout.min(Duration::from_secs(1)))
        .timeout(config.request_timeout)
        .default_headers(headers)
        .build()
        .map_err(|error| {
            log::error!("failed to build FeatBit event HTTP client: {error}");
            error
        })
        .ok()
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

pub(super) fn is_recoverable(status: StatusCode) -> bool {
    if status.is_client_error() {
        matches!(
            status,
            StatusCode::BAD_REQUEST | StatusCode::REQUEST_TIMEOUT | StatusCode::TOO_MANY_REQUESTS
        )
    } else {
        true
    }
}

pub(super) fn event_endpoint(base: &Url) -> Url {
    let mut endpoint = base.clone();
    let base_path = endpoint.path().trim_end_matches('/');
    endpoint.set_path(&format!("{base_path}/api/public/insight/track"));
    endpoint.set_query(None);
    endpoint.set_fragment(None);
    endpoint
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicBool;
    use std::sync::Arc;
    use std::time::Duration;

    use reqwest::StatusCode;
    use tokio::runtime::Builder as RuntimeBuilder;
    use tokio::sync::watch;

    use super::{is_recoverable, EventWorker, EventWorkerConfig, FlushOutcome};
    use crate::events::payload::PayloadEvent;
    use crate::events::{EventCapacity, PendingEvent};
    use crate::model::FbUser;
    use crate::options::FbOptionsBuilder;
    use crate::test_support::scripted_http_server;

    #[test]
    fn http_delivery_classification_matches_featbit_retry_contract() {
        for recoverable in [
            StatusCode::BAD_REQUEST,
            StatusCode::REQUEST_TIMEOUT,
            StatusCode::TOO_MANY_REQUESTS,
            StatusCode::INTERNAL_SERVER_ERROR,
        ] {
            assert!(is_recoverable(recoverable));
        }
        for fatal in [StatusCode::UNAUTHORIZED, StatusCode::NOT_FOUND] {
            assert!(!is_recoverable(fatal));
        }
    }

    #[test]
    fn successful_flush_reuses_the_worker_batch_allocation() {
        let (event_url, _bodies, server) = scripted_http_server([202, 202]);
        let options = FbOptionsBuilder::new("valid-secret")
            .event_url(event_url)
            .auto_flush_interval(Duration::from_mins(1))
            .build()
            .expect("options should build");
        let config = EventWorkerConfig::from_options(&options);
        let mut worker = EventWorker::new(config, Arc::new(AtomicBool::new(false)));
        worker.buffer = Vec::with_capacity(8);
        worker.buffer.push(pending_metric("first"));
        let original_capacity = worker.buffer.capacity();
        let original_allocation = worker.buffer.as_ptr();
        let runtime = RuntimeBuilder::new_current_thread()
            .enable_all()
            .build()
            .expect("event runtime should build");
        let (_abort_sender, mut abort) = watch::channel(false);

        assert_eq!(
            worker.flush(&runtime, &mut abort),
            FlushOutcome::delivered()
        );
        assert!(worker.buffer.is_empty());
        assert_eq!(worker.buffer.capacity(), original_capacity);
        assert_eq!(worker.buffer.as_ptr(), original_allocation);

        worker.buffer.push(pending_metric("second"));
        assert_eq!(
            worker.flush(&runtime, &mut abort),
            FlushOutcome::delivered()
        );
        assert!(worker.buffer.is_empty());
        assert_eq!(worker.buffer.capacity(), original_capacity);
        assert_eq!(worker.buffer.as_ptr(), original_allocation);
        server.join().expect("event server should stop");
    }

    #[test]
    fn cancelled_flush_reuses_the_worker_batch_allocation() {
        let options = FbOptionsBuilder::new("valid-secret")
            .event_url("http://127.0.0.1:9")
            .auto_flush_interval(Duration::from_mins(1))
            .build()
            .expect("options should build");
        let config = EventWorkerConfig::from_options(&options);
        let mut worker = EventWorker::new(config, Arc::new(AtomicBool::new(false)));
        worker.buffer = Vec::with_capacity(8);
        worker.buffer.push(pending_metric("cancelled"));
        let original_capacity = worker.buffer.capacity();
        let original_allocation = worker.buffer.as_ptr();
        let runtime = RuntimeBuilder::new_current_thread()
            .enable_all()
            .build()
            .expect("event runtime should build");
        let (_abort_sender, mut abort) = watch::channel(true);

        assert_eq!(
            worker.flush(&runtime, &mut abort),
            FlushOutcome::cancelled()
        );
        assert!(worker.buffer.is_empty());
        assert_eq!(worker.buffer.capacity(), original_capacity);
        assert_eq!(worker.buffer.as_ptr(), original_allocation);
    }

    fn pending_metric(event_name: &str) -> PendingEvent {
        let capacity = Arc::new(EventCapacity::new(1, 1_024));
        let admission = capacity
            .try_reserve(1_024)
            .expect("test event capacity should be available");
        let mut event = PendingEvent {
            payload: PayloadEvent::metric(
                &FbUser::builder("buffer-reuse-user").build(),
                event_name,
                1.0,
            ),
            admission,
        };
        event.mark_dequeued();
        event
    }
}
