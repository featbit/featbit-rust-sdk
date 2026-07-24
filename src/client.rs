use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use std::{fmt, fmt::Formatter};

use crate::data_sync::{StatusTracker, SyncStatus, WebSocketDataSynchronizer};
use crate::error::ConfigError;
use crate::evaluation::Evaluator;
use crate::events::{EventProcessor, FbEvaluationEvent};
use crate::model::FbUser;
use crate::options::{FbOptions, FbOptionsBuilder};
use crate::store::SnapshotStore;

mod evaluation;

pub use evaluation::{EvaluationError, RawEvaluation};

/// Current lifecycle/readiness state of an [`FbClient`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ClientStatus {
    /// Initial data has not been received yet.
    NotReady,
    /// The local snapshot is synchronized and ready for evaluation.
    Ready,
    /// A previously synchronized snapshot is usable but may no longer be current.
    Stale,
    /// The client was closed or encountered an unrecoverable synchronization failure.
    Closed,
}

/// Why a direct `FeatBit` variation method returned its value.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ReasonKind {
    /// The client has not received data or was already closed.
    ClientNotReady,
    /// The flag is disabled and returned its disabled variation.
    Off,
    /// The flag reached its fallthrough configuration.
    Fallthrough,
    /// The user was directly targeted.
    TargetMatch,
    /// A targeting rule matched.
    RuleMatch,
    /// The variation could not be converted to the requested type.
    WrongType,
    /// Evaluation failed, for example because the flag was not found or malformed.
    Error,
}

/// A value plus `FeatBit` evaluation diagnostics.
///
/// Equality compares the resolved value and diagnostics but intentionally ignores the captured
/// event, whose timestamp differs for otherwise identical evaluations.
#[derive(Clone, Debug)]
pub struct EvaluationDetail<T> {
    /// The evaluated flag key.
    pub key: String,
    /// The high-level evaluation reason.
    pub kind: ReasonKind,
    /// A human-readable, non-sensitive reason.
    pub reason: String,
    /// The evaluated value or caller-provided fallback.
    pub value: T,
    /// The selected variation ID, or an empty string when a fallback was used.
    pub variation_id: String,
    /// An immutable event that can be recorded later with [`FbClient::track_eval_event`].
    ///
    /// This is `None` when evaluation returned a fallback or when the detail came from the
    /// inspection-only [`FbClient::all_variations`] API.
    pub evaluation_event: Option<FbEvaluationEvent>,
}

impl<T: PartialEq> PartialEq for EvaluationDetail<T> {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
            && self.kind == other.kind
            && self.reason == other.reason
            && self.value == other.value
            && self.variation_id == other.variation_id
    }
}

impl<T> EvaluationDetail<T> {
    fn fallback(key: &str, kind: ReasonKind, reason: impl Into<String>, value: T) -> Self {
        Self {
            key: key.to_owned(),
            kind,
            reason: reason.into(),
            value,
            variation_id: String::new(),
            evaluation_event: None,
        }
    }
}

/// Thread-safe `FeatBit` server-side client.
///
/// Clone this handle freely; all clones share one snapshot, synchronizer, and event processor.
/// Value-only variation methods never panic and return their `default` argument on failure.
#[derive(Clone)]
pub struct FbClient {
    inner: Arc<ClientInner>,
}

impl fmt::Debug for FbClient {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FbClient")
            .field("initialized", &self.initialized())
            .field("status", &self.status())
            .finish_non_exhaustive()
    }
}

impl FbClient {
    /// Builds default options and starts a client.
    ///
    /// This only returns an error for invalid local configuration. Network and initialization
    /// failures are reflected by [`Self::status`] and cause variation calls to return fallbacks.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError`] when the environment secret or a default option is invalid.
    pub fn new(env_secret: impl Into<String>) -> Result<Self, ConfigError> {
        let options = FbOptionsBuilder::new(env_secret).build()?;
        Ok(Self::with_options(options))
    }

    /// Starts a client from validated options.
    #[must_use]
    pub fn with_options(options: FbOptions) -> Self {
        let offline = options.offline;
        let start_wait = options.start_wait;
        let store = Arc::new(SnapshotStore::new());
        let status = if offline {
            if let Some(bootstrap) = &options.bootstrap {
                store.populate(&bootstrap.data);
            }
            Arc::new(StatusTracker::new(SyncStatus::Ready, true))
        } else {
            Arc::new(StatusTracker::new(SyncStatus::Starting, false))
        };

        let event_processor = EventProcessor::new(&options);
        let synchronizer = if offline {
            None
        } else {
            WebSocketDataSynchronizer::start(
                options.clone(),
                Arc::clone(&store),
                Arc::clone(&status),
            )
        };

        let inner = Arc::new(ClientInner {
            options,
            store,
            status: Arc::clone(&status),
            synchronizer,
            event_processor,
            closed: AtomicBool::new(false),
        });
        let client = Self { inner };

        if !offline {
            log::info!("waiting up to {start_wait:?} for initial FeatBit data");
            if status.wait_until_initialized(start_wait) {
                log::info!("FeatBit client initialized");
            } else {
                log::warn!(
                    "FeatBit client did not initialize within {start_wait:?}; background retries continue"
                );
            }
        }
        client
    }

    /// Returns whether at least one valid data set has been published.
    #[must_use]
    pub fn initialized(&self) -> bool {
        self.inner.status.initialized()
    }

    /// Returns the current lifecycle/readiness state.
    #[must_use]
    pub fn status(&self) -> ClientStatus {
        match self.inner.status.status() {
            SyncStatus::Starting => ClientStatus::NotReady,
            SyncStatus::Ready => ClientStatus::Ready,
            SyncStatus::Stale => ClientStatus::Stale,
            SyncStatus::Closed => ClientStatus::Closed,
        }
    }

    /// Returns the immutable options used by this client.
    #[must_use]
    pub fn options(&self) -> &FbOptions {
        &self.inner.options
    }

    /// Records a custom metric with numeric value `1.0`.
    pub fn track(&self, user: &FbUser, event_name: &str) {
        let _accepted = self.track_metric_event(user, event_name, 1.0);
    }

    /// Records a custom metric without blocking on network I/O.
    pub fn track_value(&self, user: &FbUser, event_name: &str, numeric_value: f64) {
        let _accepted = self.track_metric_event(user, event_name, numeric_value);
    }

    /// Records a previously captured evaluation event without blocking on network I/O.
    ///
    /// This is available when the `allow_track` argument to
    /// [`FbOptionsBuilder::disable_events`] is `true`. It returns `false` when explicit tracking is
    /// not allowed, the client is not operational, the event is invalid, or the bounded queue is
    /// full. Use the event carried by a detail variation result to preserve the exact variation and
    /// experiment decision selected at evaluation time. Calling this while automatic evaluation
    /// events are enabled records a second event for the same evaluation.
    #[must_use]
    pub fn track_eval_event(&self, user: &FbUser, event: &FbEvaluationEvent) -> bool {
        self.tracking_available() && self.inner.event_processor.record_evaluation(user, event)
    }

    /// Re-evaluates `flag_key` against the current snapshot and records that evaluation event.
    ///
    /// This convenience is useful after an integration returns a detail type that cannot carry
    /// [`FbEvaluationEvent`]. Call it promptly after the original resolution. If an intervening flag
    /// update must not change the reported variation, use a direct detail method and pass its
    /// captured event to [`Self::track_eval_event`] instead. Calling this while automatic evaluation
    /// events are enabled records an additional event.
    #[must_use]
    pub fn track_eval_event_for_flag(&self, user: &FbUser, flag_key: &str) -> bool {
        if !self.tracking_available() {
            return false;
        }
        let snapshot = self.inner.store.load();
        let Ok(result) = Evaluator::evaluate(&snapshot, flag_key, user) else {
            return false;
        };
        let event = FbEvaluationEvent::new(
            flag_key,
            result.variation.id.as_str(),
            result.variation.value.as_str(),
            result.send_to_experiment,
        );
        self.inner.event_processor.record_evaluation(user, &event)
    }

    /// Records a custom metric event without blocking on network I/O.
    ///
    /// This is available when the `allow_track` argument to
    /// [`FbOptionsBuilder::disable_events`] is `true`. It returns `false` when explicit tracking is
    /// not allowed, the client is not operational, the event is invalid, or the bounded queue is
    /// full.
    #[must_use]
    pub fn track_metric_event(&self, user: &FbUser, event_name: &str, numeric_value: f64) -> bool {
        self.tracking_available()
            && self
                .inner
                .event_processor
                .record_metric(user, event_name, numeric_value)
    }

    /// Requests a non-blocking event flush.
    pub fn flush(&self) {
        self.inner.event_processor.flush();
    }

    /// Flushes events and waits up to `timeout` for delivery.
    ///
    /// Returns `true` only when every event covered by this flush was delivered successfully (or
    /// no event processor is active). A timeout, exhausted retry sequence, unrecoverable response,
    /// stopped worker, or concurrent shutdown returns `false`.
    #[must_use]
    pub fn flush_and_wait(&self, timeout: Duration) -> bool {
        self.inner.event_processor.flush_and_wait(timeout)
    }

    /// Stops synchronization and performs a bounded final event flush.
    ///
    /// Calling this more than once is safe. All future variation calls return their fallback.
    pub fn close(&self) {
        self.inner.close();
    }

    fn evaluation_available(&self) -> bool {
        !self.inner.closed.load(Ordering::Acquire)
            && self.initialized()
            && matches!(
                self.inner.status.status(),
                SyncStatus::Ready | SyncStatus::Stale
            )
    }

    fn tracking_available(&self) -> bool {
        self.inner.options.allow_track && self.evaluation_available()
    }
}

#[derive(Debug)]
struct ClientInner {
    options: FbOptions,
    store: Arc<SnapshotStore>,
    status: Arc<StatusTracker>,
    synchronizer: Option<WebSocketDataSynchronizer>,
    event_processor: EventProcessor,
    closed: AtomicBool,
}

impl ClientInner {
    fn close(&self) {
        let first_close = !self.closed.swap(true, Ordering::AcqRel);
        if first_close {
            log::info!("closing FeatBit client");
        }
        if let Some(synchronizer) = &self.synchronizer {
            synchronizer.close();
        } else {
            self.status.set(SyncStatus::Closed);
        }
        self.event_processor.close();
        if first_close {
            log::info!("FeatBit client closed");
        }
    }
}

impl Drop for ClientInner {
    fn drop(&mut self) {
        self.close();
    }
}

#[cfg(test)]
mod tests;
