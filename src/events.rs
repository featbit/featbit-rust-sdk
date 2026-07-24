mod payload;
mod worker;

#[cfg(test)]
mod tests;

use std::mem::size_of;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use arc_swap::ArcSwapOption;
use crossbeam_channel::{bounded, Sender, TrySendError};
use tokio::runtime::Builder as RuntimeBuilder;
use tokio::sync::watch;

use crate::model::FbUser;
use crate::options::FbOptions;
use crate::worker::{WorkerThread, WorkerWait};

use payload::PayloadEvent;
use worker::{EventWorker, EventWorkerConfig};

pub use payload::FbEvaluationEvent;

#[cfg(test)]
use reqwest::StatusCode;
#[cfg(test)]
use worker::{event_endpoint, is_recoverable};

const MAX_PUBLIC_WAIT: Duration = Duration::from_hours(8_760);

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
        let capacity = Arc::new(EventCapacity::new(
            options.max_events_in_queue,
            options.max_event_queue_size_bytes,
        ));
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
                capacity,
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
        self.record_evaluation_at(
            user,
            event.flag_key(),
            event.variation_id(),
            event.variation_value(),
            event.timestamp(),
            event.send_to_experiment(),
        )
    }

    pub(crate) fn record_evaluation_at(
        &self,
        user: &FbUser,
        flag_key: &str,
        variation_id: &str,
        variation_value: &str,
        timestamp: SystemTime,
        send_to_experiment: bool,
    ) -> bool {
        if user.key().is_empty() || flag_key.trim().is_empty() || variation_id.trim().is_empty() {
            log::debug!("discarding invalid FeatBit evaluation event");
            return false;
        }
        if !self.is_accepting() {
            return false;
        }
        let retained_bytes =
            PayloadEvent::estimated_evaluation_size(user, flag_key, variation_id, variation_value)
                .saturating_add(size_of::<EventAdmission>());
        self.record(retained_bytes, || {
            PayloadEvent::evaluation_at(
                user,
                flag_key,
                variation_id,
                variation_value,
                timestamp,
                send_to_experiment,
            )
        })
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
        if !self.is_accepting() {
            return false;
        }
        let retained_bytes = PayloadEvent::estimated_metric_size(user, event_name)
            .saturating_add(size_of::<EventAdmission>());
        self.record(retained_bytes, || {
            PayloadEvent::metric(user, event_name, numeric_value)
        })
    }

    fn record(&self, retained_bytes: usize, create_event: impl FnOnce() -> PayloadEvent) -> bool {
        let Self::Active(inner) = self else {
            return false;
        };
        if inner.closed.load(Ordering::Acquire) || inner.delivery_stopped.load(Ordering::Acquire) {
            return false;
        }

        let Some(sender) = inner.sender.load_full() else {
            return false;
        };
        if sender.is_full() {
            log_event_queue_overflow(&inner.capacity_exceeded);
            return false;
        }
        let Some(admission) = inner.capacity.try_reserve(retained_bytes) else {
            log_event_queue_overflow(&inner.capacity_exceeded);
            return false;
        };
        let event = PendingEvent {
            payload: create_event(),
            admission,
        };
        match sender.try_send(EventMessage::Payload(event)) {
            Ok(()) => {
                mark_event_queue_available(&inner.capacity_exceeded);
                true
            }
            Err(TrySendError::Full(_)) => {
                log_event_queue_overflow(&inner.capacity_exceeded);
                false
            }
            Err(TrySendError::Disconnected(_)) => false,
        }
    }

    pub(crate) fn is_accepting(&self) -> bool {
        let Self::Active(inner) = self else {
            return false;
        };
        !inner.closed.load(Ordering::Acquire) && !inner.delivery_stopped.load(Ordering::Acquire)
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

fn mark_event_queue_available(capacity_exceeded: &AtomicBool) {
    capacity_exceeded.store(false, Ordering::Release);
}

fn should_log_event_queue_overflow(capacity_exceeded: &AtomicBool) -> bool {
    !capacity_exceeded.swap(true, Ordering::AcqRel)
}

fn log_event_queue_overflow(capacity_exceeded: &AtomicBool) {
    if should_log_event_queue_overflow(capacity_exceeded) {
        log::warn!(
            "FeatBit event capacity is exhausted; events will be dropped until capacity recovers"
        );
    }
}

#[derive(Debug)]
pub(crate) struct EventProcessorInner {
    sender: ArcSwapOption<Sender<EventMessage>>,
    shutdown_sender: Sender<Shutdown>,
    abort_sender: watch::Sender<bool>,
    closed: AtomicBool,
    capacity_exceeded: AtomicBool,
    capacity: Arc<EventCapacity>,
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
    Payload(PendingEvent),
    Flush(Option<Sender<bool>>),
}

#[derive(Debug)]
struct PendingEvent {
    payload: PayloadEvent,
    admission: EventAdmission,
}

impl PendingEvent {
    fn mark_dequeued(&mut self) {
        self.admission.mark_dequeued();
    }
}

impl serde::Serialize for PendingEvent {
    fn serialize<Serializer>(
        &self,
        serializer: Serializer,
    ) -> Result<Serializer::Ok, Serializer::Error>
    where
        Serializer: serde::Serializer,
    {
        serde::Serialize::serialize(&self.payload, serializer)
    }
}

#[derive(Debug)]
struct EventCapacity {
    max_queued_events: usize,
    max_retained_bytes: usize,
    queued_events: AtomicUsize,
    retained_bytes: AtomicUsize,
}

impl EventCapacity {
    const fn new(max_queued_events: usize, max_retained_bytes: usize) -> Self {
        Self {
            max_queued_events,
            max_retained_bytes,
            queued_events: AtomicUsize::new(0),
            retained_bytes: AtomicUsize::new(0),
        }
    }

    fn try_reserve(self: &Arc<Self>, retained_bytes: usize) -> Option<EventAdmission> {
        if !try_reserve_counter(&self.queued_events, 1, self.max_queued_events) {
            return None;
        }
        if !try_reserve_counter(
            &self.retained_bytes,
            retained_bytes,
            self.max_retained_bytes,
        ) {
            self.queued_events.fetch_sub(1, Ordering::Relaxed);
            return None;
        }
        Some(EventAdmission {
            capacity: Arc::clone(self),
            retained_bytes,
            occupies_queue_slot: true,
        })
    }
}

fn try_reserve_counter(counter: &AtomicUsize, amount: usize, maximum: usize) -> bool {
    let mut current = counter.load(Ordering::Relaxed);
    loop {
        let Some(next) = current.checked_add(amount) else {
            return false;
        };
        if next > maximum {
            return false;
        }
        match counter.compare_exchange_weak(current, next, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => return true,
            Err(observed) => current = observed,
        }
    }
}

#[derive(Debug)]
struct EventAdmission {
    capacity: Arc<EventCapacity>,
    retained_bytes: usize,
    occupies_queue_slot: bool,
}

impl EventAdmission {
    fn mark_dequeued(&mut self) {
        if self.occupies_queue_slot {
            self.occupies_queue_slot = false;
            self.capacity.queued_events.fetch_sub(1, Ordering::Relaxed);
        }
    }
}

impl Drop for EventAdmission {
    fn drop(&mut self) {
        if self.occupies_queue_slot {
            self.capacity.queued_events.fetch_sub(1, Ordering::Relaxed);
        }
        self.capacity
            .retained_bytes
            .fetch_sub(self.retained_bytes, Ordering::Relaxed);
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Shutdown {
    Graceful,
    Abort,
}
