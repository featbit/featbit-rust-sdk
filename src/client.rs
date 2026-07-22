use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use std::{fmt, fmt::Formatter};

use crate::data_sync::{StatusTracker, SyncStatus, WebSocketDataSynchronizer};
use crate::error::ConfigError;
use crate::evaluation::{EvalError, EvalReason, EvalResult, Evaluator};
use crate::events::{EventProcessor, FbEvaluationEvent};
use crate::model::FbUser;
use crate::observation::{
    EvaluationObservation, EvaluationObservationError, EvaluationObservationReason,
};
use crate::options::{FbOptions, FbOptionsBuilder};
use crate::store::SnapshotStore;

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

    /// Evaluates a boolean flag, returning `default` on every failure.
    #[must_use]
    pub fn bool_variation(&self, key: &str, user: &FbUser, default: bool) -> bool {
        self.bool_variation_detail(key, user, default).value
    }

    /// Evaluates a boolean flag with diagnostics.
    #[must_use]
    pub fn bool_variation_detail(
        &self,
        key: &str,
        user: &FbUser,
        default: bool,
    ) -> EvaluationDetail<bool> {
        self.evaluate_typed(key, user, default, parse_bool)
    }

    /// Evaluates a signed 64-bit integer flag, returning `default` on every failure.
    #[must_use]
    pub fn int_variation(&self, key: &str, user: &FbUser, default: i64) -> i64 {
        self.int_variation_detail(key, user, default).value
    }

    /// Evaluates a signed 64-bit integer flag with diagnostics.
    #[must_use]
    pub fn int_variation_detail(
        &self,
        key: &str,
        user: &FbUser,
        default: i64,
    ) -> EvaluationDetail<i64> {
        self.evaluate_typed(key, user, default, |value| {
            value
                .parse()
                .map_err(|_| EvaluationObservationError::TypeMismatch)
        })
    }

    /// Evaluates a 64-bit floating-point flag, returning `default` on every failure.
    #[must_use]
    pub fn float_variation(&self, key: &str, user: &FbUser, default: f64) -> f64 {
        self.float_variation_detail(key, user, default).value
    }

    /// Evaluates a 64-bit floating-point flag with diagnostics.
    #[must_use]
    pub fn float_variation_detail(
        &self,
        key: &str,
        user: &FbUser,
        default: f64,
    ) -> EvaluationDetail<f64> {
        self.evaluate_typed(key, user, default, |value| {
            value
                .parse::<f64>()
                .ok()
                .filter(|number| number.is_finite())
                .ok_or(EvaluationObservationError::TypeMismatch)
        })
    }

    /// Evaluates a string flag, returning `default` on every failure.
    #[must_use]
    pub fn string_variation(&self, key: &str, user: &FbUser, default: &str) -> String {
        self.string_variation_detail(key, user, default).value
    }

    /// Evaluates a string flag with diagnostics.
    #[must_use]
    pub fn string_variation_detail(
        &self,
        key: &str,
        user: &FbUser,
        default: &str,
    ) -> EvaluationDetail<String> {
        self.evaluate_typed(key, user, default.to_owned(), |value| Ok(value.to_owned()))
    }

    /// Evaluates a JSON flag, returning `default` on every failure.
    #[must_use]
    pub fn json_variation(
        &self,
        key: &str,
        user: &FbUser,
        default: serde_json::Value,
    ) -> serde_json::Value {
        self.json_variation_detail(key, user, default).value
    }

    /// Evaluates a JSON flag with diagnostics.
    #[must_use]
    pub fn json_variation_detail(
        &self,
        key: &str,
        user: &FbUser,
        default: serde_json::Value,
    ) -> EvaluationDetail<serde_json::Value> {
        self.evaluate_typed(key, user, default, |value| {
            serde_json::from_str(value).map_err(|_| EvaluationObservationError::ParseError)
        })
    }

    /// Evaluates every currently known, non-archived flag as its raw string variation.
    ///
    /// This inspection API does not emit evaluation events. Results are sorted by flag key.
    #[must_use]
    pub fn all_variations(&self, user: &FbUser) -> Vec<EvaluationDetail<String>> {
        if !self.evaluation_available() {
            return Vec::new();
        }
        let snapshot = self.inner.store.load();
        let mut keys: Vec<&str> = snapshot
            .flags
            .iter()
            .filter(|(_, flag)| !flag.is_archived)
            .map(|(key, _)| key.as_str())
            .collect();
        keys.sort_unstable();
        keys.into_iter()
            .filter_map(|key| {
                Evaluator::evaluate(&snapshot, key, user)
                    .ok()
                    .map(|result| string_detail_from_result(key, result))
            })
            .collect()
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
    /// This convenience is useful after an `OpenFeature` resolution, whose standard detail type
    /// cannot carry [`FbEvaluationEvent`]. Call it promptly after the original resolution. If an
    /// intervening flag update must not change the reported variation, use a direct detail method
    /// and pass its captured event to [`Self::track_eval_event`] instead. Calling this while
    /// automatic evaluation events are enabled records an additional event.
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
            result.variation.id,
            result.variation.value,
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

    fn evaluate_typed<T>(
        &self,
        key: &str,
        user: &FbUser,
        default: T,
        converter: impl FnOnce(&str) -> Result<T, EvaluationObservationError>,
    ) -> EvaluationDetail<T> {
        match self.evaluate_raw(key, user) {
            Ok((result, event)) => {
                let (kind, reason) = reason_detail(&result.reason);
                match converter(&result.variation.value) {
                    Ok(value) => {
                        self.complete_evaluation(user, &result, &event);
                        EvaluationDetail {
                            key: key.to_owned(),
                            kind,
                            reason,
                            value,
                            variation_id: result.variation.id,
                            evaluation_event: Some(event),
                        }
                    }
                    Err(error) => {
                        self.observe_error(key, Some(user.key()), error);
                        let (kind, reason) = match error {
                            EvaluationObservationError::ParseError => {
                                (ReasonKind::Error, "parse error")
                            }
                            _ => (ReasonKind::WrongType, "type mismatch"),
                        };
                        EvaluationDetail::fallback(key, kind, reason, default)
                    }
                }
            }
            Err(error) => {
                let (kind, reason) = match error {
                    ClientEvaluationError::NotReady => {
                        (ReasonKind::ClientNotReady, "client not ready")
                    }
                    ClientEvaluationError::InvalidContext => {
                        (ReasonKind::Error, "targeting key is missing")
                    }
                    ClientEvaluationError::FlagNotFound => (ReasonKind::Error, "flag not found"),
                    ClientEvaluationError::MalformedFlag => (ReasonKind::Error, "malformed flag"),
                };
                EvaluationDetail::fallback(key, kind, reason, default)
            }
        }
    }

    pub(crate) fn evaluate_raw(
        &self,
        key: &str,
        user: &FbUser,
    ) -> Result<(EvalResult, FbEvaluationEvent), ClientEvaluationError> {
        if !self.evaluation_available() {
            self.observe_error(
                key,
                (!user.key().is_empty()).then_some(user.key()),
                EvaluationObservationError::ProviderNotReady,
            );
            return Err(ClientEvaluationError::NotReady);
        }
        let snapshot = self.inner.store.load();
        let result = match Evaluator::evaluate(&snapshot, key, user) {
            Ok(result) => result,
            Err(error) => {
                let client_error = ClientEvaluationError::from(error);
                self.observe_error(
                    key,
                    (!user.key().is_empty()).then_some(user.key()),
                    observation_error(client_error),
                );
                return Err(client_error);
            }
        };
        let event = FbEvaluationEvent::new(
            key,
            &result.variation.id,
            &result.variation.value,
            result.send_to_experiment,
        );
        Ok((result, event))
    }

    pub(crate) fn complete_evaluation(
        &self,
        user: &FbUser,
        result: &EvalResult,
        event: &FbEvaluationEvent,
    ) {
        if !self.inner.options.disable_events {
            let _accepted = self.inner.event_processor.record_evaluation(user, event);
        }
        let Some(observer) = &self.inner.options.evaluation_observer else {
            return;
        };
        let observation = EvaluationObservation::success(
            event.timestamp(),
            event.flag_key(),
            user.key(),
            event.variation_id(),
            event.variation_value(),
            observation_reason(&result.reason),
            event.send_to_experiment(),
        );
        observer.on_evaluation(&observation);
    }

    pub(crate) fn observe_error(
        &self,
        key: &str,
        context_key: Option<&str>,
        error: EvaluationObservationError,
    ) {
        let Some(observer) = &self.inner.options.evaluation_observer else {
            return;
        };
        let observation = EvaluationObservation::error(SystemTime::now(), key, context_key, error);
        observer.on_evaluation(&observation);
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ClientEvaluationError {
    NotReady,
    InvalidContext,
    FlagNotFound,
    MalformedFlag,
}

impl From<EvalError> for ClientEvaluationError {
    fn from(error: EvalError) -> Self {
        match error {
            EvalError::InvalidContext => Self::InvalidContext,
            EvalError::FlagNotFound => Self::FlagNotFound,
            EvalError::MalformedFlag => Self::MalformedFlag,
        }
    }
}

fn string_detail_from_result(key: &str, result: EvalResult) -> EvaluationDetail<String> {
    let (kind, reason) = reason_detail(&result.reason);
    EvaluationDetail {
        key: key.to_owned(),
        kind,
        reason,
        value: result.variation.value,
        variation_id: result.variation.id,
        evaluation_event: None,
    }
}

fn observation_reason(reason: &EvalReason) -> EvaluationObservationReason {
    match reason {
        EvalReason::Off => EvaluationObservationReason::Disabled,
        EvalReason::TargetMatch | EvalReason::RuleMatch { split: false, .. } => {
            EvaluationObservationReason::TargetingMatch
        }
        EvalReason::RuleMatch { split: true, .. } | EvalReason::Fallthrough { split: true } => {
            EvaluationObservationReason::Split
        }
        EvalReason::Fallthrough { split: false } => EvaluationObservationReason::Default,
    }
}

fn observation_error(error: ClientEvaluationError) -> EvaluationObservationError {
    match error {
        ClientEvaluationError::NotReady => EvaluationObservationError::ProviderNotReady,
        ClientEvaluationError::InvalidContext => EvaluationObservationError::TargetingKeyMissing,
        ClientEvaluationError::FlagNotFound => EvaluationObservationError::FlagNotFound,
        ClientEvaluationError::MalformedFlag => EvaluationObservationError::ParseError,
    }
}

fn reason_detail(reason: &EvalReason) -> (ReasonKind, String) {
    match reason {
        EvalReason::Off => (ReasonKind::Off, "flag off".to_owned()),
        EvalReason::TargetMatch => (ReasonKind::TargetMatch, "target match".to_owned()),
        EvalReason::RuleMatch { name, .. } => (ReasonKind::RuleMatch, format!("match rule {name}")),
        EvalReason::Fallthrough { .. } => (
            ReasonKind::Fallthrough,
            "fall through targets and rules".to_owned(),
        ),
    }
}

fn parse_bool(value: &str) -> Result<bool, EvaluationObservationError> {
    if value.eq_ignore_ascii_case("true") {
        Ok(true)
    } else if value.eq_ignore_ascii_case("false") {
        Ok(false)
    } else {
        Err(EvaluationObservationError::TypeMismatch)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::thread;

    use super::*;
    use crate::model::DataSyncEnvelope;
    use crate::observation::EvaluationObserver;
    use crate::test_support::scripted_http_server;

    #[derive(Clone, Default)]
    struct RecordingObserver {
        observations: Arc<Mutex<Vec<EvaluationObservation>>>,
    }

    impl EvaluationObserver for RecordingObserver {
        fn on_evaluation(&self, observation: &EvaluationObservation) {
            self.observations
                .lock()
                .expect("test observer lock should remain available")
                .push(observation.clone());
        }
    }

    const READY_BOOTSTRAP: &str = r#"{
        "messageType":"data-sync",
        "data":{"eventType":"full","featureFlags":[{
            "id":"flag-id","key":"enabled","updatedAt":1,"variationType":"boolean",
            "variations":[{"id":"value","value":"true"}],
            "targetUsers":[],"rules":[],"isEnabled":true,
            "fallthrough":{"includedInExpt":false,"variations":[
                {"id":"value","rollout":[0,1],"exptRollout":0}
            ]}
        },{
            "id":"invalid-flag-id","key":"invalid-bool","updatedAt":1,"variationType":"boolean",
            "variations":[{"id":"invalid-value","value":"not-a-boolean"}],
            "targetUsers":[],"rules":[],"isEnabled":true,
            "fallthrough":{"includedInExpt":false,"variations":[
                {"id":"invalid-value","rollout":[0,1],"exptRollout":0}
            ]}
        }],"segments":[]}
    }"#;

    const UPDATED_BOOTSTRAP: &str = r#"{
        "messageType":"data-sync",
        "data":{"eventType":"full","featureFlags":[{
            "id":"flag-id","key":"enabled","updatedAt":2,"variationType":"boolean",
            "variations":[{"id":"value","value":"false"}],
            "targetUsers":[],"rules":[],"isEnabled":true,
            "fallthrough":{"includedInExpt":false,"variations":[
                {"id":"value","rollout":[0,1],"exptRollout":0}
            ]}
        }],"segments":[]}
    }"#;

    #[test]
    fn public_client_is_send_and_sync() {
        const fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<FbClient>();
    }

    #[test]
    fn uninitialized_client_returns_fallback_without_panicking() {
        let options = FbOptionsBuilder::new("valid-secret")
            .streaming_url("ws://127.0.0.1:9")
            .start_wait(Duration::from_millis(2))
            .connect_timeout(Duration::from_millis(1))
            .build()
            .expect("options should build");
        let client = FbClient::with_options(options);
        let user = FbUser::builder("user").build();
        let detail = client.bool_variation_detail("missing", &user, true);
        assert!(detail.value);
        assert_eq!(detail.kind, ReasonKind::ClientNotReady);
        client.close();
    }

    #[test]
    fn evaluation_and_idempotent_close_are_thread_safe() {
        let options = FbOptionsBuilder::new("valid-secret")
            .offline(true)
            .disable_events(true, false)
            .bootstrap_json(READY_BOOTSTRAP)
            .build()
            .expect("offline options should build");
        let client = FbClient::with_options(options);
        let updated = serde_json::from_str::<DataSyncEnvelope>(UPDATED_BOOTSTRAP)
            .expect("updated data should parse")
            .data;

        let writer_store = Arc::clone(&client.inner.store);
        let writer = thread::spawn(move || {
            for _ in 0..1_000 {
                writer_store.populate(&updated);
            }
        });
        let readers = (0..4)
            .map(|index| {
                let reader = client.clone();
                thread::spawn(move || {
                    let user = FbUser::builder(format!("user-{index}")).build();
                    for _ in 0..1_000 {
                        let detail = reader.bool_variation_detail("enabled", &user, false);
                        assert_eq!(detail.kind, ReasonKind::Fallthrough);
                        assert_eq!(detail.variation_id, "value");
                    }
                })
            })
            .collect::<Vec<_>>();

        writer.join().expect("writer should finish");
        for reader in readers {
            reader.join().expect("reader should finish");
        }

        let closers = (0..4)
            .map(|_| {
                let closer = client.clone();
                thread::spawn(move || closer.close())
            })
            .collect::<Vec<_>>();
        for closer in closers {
            closer.join().expect("close should finish");
        }
        assert_eq!(client.status(), ClientStatus::Closed);
    }

    #[test]
    fn terminal_sync_status_never_evaluates_a_previously_ready_snapshot() {
        let options = FbOptionsBuilder::new("valid-secret")
            .offline(true)
            .disable_events(true, false)
            .bootstrap_json(READY_BOOTSTRAP)
            .build()
            .expect("offline options should build");
        let client = FbClient::with_options(options);
        let user = FbUser::builder("user").build();
        assert!(client.bool_variation("enabled", &user, false));
        assert!(client.initialized());

        client.inner.status.set(SyncStatus::Closed);

        let detail = client.bool_variation_detail("enabled", &user, false);
        assert!(!detail.value);
        assert_eq!(detail.kind, ReasonKind::ClientNotReady);
        assert_eq!(client.status(), ClientStatus::Closed);
        assert!(client.initialized(), "initialized records prior readiness");
        assert!(client.all_variations(&user).is_empty());
        client.close();
    }

    #[test]
    fn event_modes_control_automatic_and_explicit_delivery() {
        for (disable, allow_track, expected_evaluations, expected_metrics) in [
            (false, true, 2, 1),
            (false, false, 1, 0),
            (true, true, 1, 1),
            (true, false, 0, 0),
        ] {
            let expected_total = expected_evaluations + expected_metrics;
            let statuses = if expected_total == 0 {
                Vec::new()
            } else {
                vec![202]
            };
            let (event_url, bodies, server) = scripted_http_server(statuses);
            let options = FbOptionsBuilder::new("valid-secret")
                .streaming_url("ws://127.0.0.1:9")
                .event_url(event_url)
                .start_wait(Duration::from_millis(2))
                .connect_timeout(Duration::from_millis(1))
                .close_timeout(Duration::from_millis(200))
                .auto_flush_interval(Duration::from_mins(1))
                .flush_timeout(Duration::from_secs(2))
                .disable_events(disable, allow_track)
                .build()
                .expect("online options should build");
            let client = FbClient::with_options(options);
            let data = serde_json::from_str::<DataSyncEnvelope>(READY_BOOTSTRAP)
                .expect("bootstrap should parse")
                .data;
            client.inner.store.populate(&data);
            client.inner.status.set(SyncStatus::Ready);

            let user = FbUser::builder("user").build();
            let detail = client.bool_variation_detail("enabled", &user, false);
            let evaluation_event = detail
                .evaluation_event
                .as_ref()
                .expect("successful detail should retain its evaluation event");
            assert_eq!(
                client.track_eval_event(&user, evaluation_event),
                allow_track
            );
            assert_eq!(
                client.track_metric_event(&user, "converted", 1.0),
                allow_track
            );
            assert_eq!(
                matches!(&client.inner.event_processor, EventProcessor::Disabled),
                disable && !allow_track
            );
            assert!(client.flush_and_wait(Duration::from_secs(2)));
            client.close();
            server.join().expect("event server should stop");

            if expected_total == 0 {
                assert!(bodies.try_recv().is_err());
                continue;
            }
            let body = bodies
                .recv_timeout(Duration::from_secs(1))
                .expect("configured events should be delivered");
            let events = serde_json::from_slice::<serde_json::Value>(&body)
                .expect("event batch should be JSON");
            let events = events.as_array().expect("event batch should be an array");
            assert_eq!(events.len(), expected_total);
            assert_eq!(
                events
                    .iter()
                    .filter(|event| event.get("variations").is_some())
                    .count(),
                expected_evaluations
            );
            assert_eq!(
                events
                    .iter()
                    .filter(|event| event.get("metrics").is_some())
                    .count(),
                expected_metrics
            );
        }
    }

    #[test]
    fn observer_is_independent_from_featbit_event_delivery() {
        let observer = RecordingObserver::default();
        let observations = Arc::clone(&observer.observations);
        let options = FbOptionsBuilder::new("valid-secret")
            .offline(true)
            .disable_events(true, false)
            .evaluation_observer(observer)
            .bootstrap_json(READY_BOOTSTRAP)
            .build()
            .expect("offline options should build");
        let client = FbClient::with_options(options);
        let user = FbUser::builder("private-user-key").build();

        let detail = client.bool_variation_detail("enabled", &user, false);
        assert!(detail.value);
        assert!(!client.track_metric_event(&user, "disabled", 1.0));
        assert!(!client.track_eval_event(
            &user,
            detail
                .evaluation_event
                .as_ref()
                .expect("successful evaluation should retain an event")
        ));
        let _fallback = client.bool_variation("missing", &user, false);
        let mismatch = client.bool_variation_detail("invalid-bool", &user, true);
        assert!(mismatch.value);
        assert_eq!(mismatch.kind, ReasonKind::WrongType);
        assert!(mismatch.evaluation_event.is_none());

        let observations = observations
            .lock()
            .expect("test observer lock should remain available");
        assert_eq!(observations.len(), 3);
        assert_eq!(
            observations[0].reason(),
            EvaluationObservationReason::Default
        );
        assert_eq!(observations[0].variation_id(), Some("value"));
        assert_eq!(
            observations[1].error_type(),
            Some(EvaluationObservationError::FlagNotFound)
        );
        assert_eq!(
            observations[2].error_type(),
            Some(EvaluationObservationError::TypeMismatch)
        );
        assert!(!format!("{:?}", observations[0]).contains("private-user-key"));
        client.close();
    }
}
