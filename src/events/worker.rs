use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crossbeam_channel::{Receiver, TryRecvError};
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
    use reqwest::StatusCode;

    use super::is_recoverable;

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
}
