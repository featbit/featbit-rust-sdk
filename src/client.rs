use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use std::{fmt, fmt::Formatter};

use crate::data_sync::{StatusTracker, SyncStatus, WebSocketDataSynchronizer};
use crate::error::ConfigError;
use crate::evaluation::{EvalError, EvalReason, EvalResult, Evaluator};
use crate::events::EventProcessor;
use crate::model::FbUser;
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
#[derive(Clone, Debug, PartialEq)]
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
}

impl<T> EvaluationDetail<T> {
    fn fallback(key: &str, kind: ReasonKind, reason: impl Into<String>, value: T) -> Self {
        Self {
            key: key.to_owned(),
            kind,
            reason: reason.into(),
            value,
            variation_id: String::new(),
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
                match bootstrap.data.event_type.as_str() {
                    "full" => store.populate(&bootstrap.data),
                    "patch" => {
                        store.patch(&bootstrap.data);
                    }
                    _ => {}
                }
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
        self.evaluate_typed(key, user, default, |value| value.parse().ok())
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
        self.evaluate_typed(key, user, default.to_owned(), |value| {
            Some(value.to_owned())
        })
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
        self.evaluate_typed(key, user, default, |value| serde_json::from_str(value).ok())
    }

    /// Evaluates every currently known, non-archived flag as its raw string variation.
    ///
    /// This inspection API does not emit evaluation events. Results are sorted by flag key.
    #[must_use]
    pub fn all_variations(&self, user: &FbUser) -> Vec<EvaluationDetail<String>> {
        if self.inner.closed.load(Ordering::Acquire) || !self.initialized() {
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
        self.track_value(user, event_name, 1.0);
    }

    /// Records a custom metric without blocking on network I/O.
    pub fn track_value(&self, user: &FbUser, event_name: &str, numeric_value: f64) {
        if !self.inner.closed.load(Ordering::Acquire) {
            self.inner
                .event_processor
                .record_metric(user, event_name, numeric_value);
        }
    }

    /// Requests a non-blocking event flush.
    pub fn flush(&self) {
        self.inner.event_processor.flush();
    }

    /// Flushes events and waits up to `timeout` for delivery attempts to finish.
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
        converter: impl FnOnce(&str) -> Option<T>,
    ) -> EvaluationDetail<T> {
        match self.evaluate_raw(key, user) {
            Ok(result) => {
                let (kind, reason) = reason_detail(&result.reason);
                match converter(&result.variation.value) {
                    Some(value) => EvaluationDetail {
                        key: key.to_owned(),
                        kind,
                        reason,
                        value,
                        variation_id: result.variation.id,
                    },
                    None => EvaluationDetail::fallback(
                        key,
                        ReasonKind::WrongType,
                        "type mismatch",
                        default,
                    ),
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
    ) -> Result<EvalResult, ClientEvaluationError> {
        if self.inner.closed.load(Ordering::Acquire) || !self.initialized() {
            return Err(ClientEvaluationError::NotReady);
        }
        let snapshot = self.inner.store.load();
        let result =
            Evaluator::evaluate(&snapshot, key, user).map_err(ClientEvaluationError::from)?;
        self.inner.event_processor.record_evaluation(
            user,
            key,
            &result.variation,
            result.send_to_experiment,
        );
        Ok(result)
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
        if self.closed.swap(true, Ordering::AcqRel) {
            return;
        }
        log::info!("closing FeatBit client");
        if let Some(synchronizer) = &self.synchronizer {
            synchronizer.close();
        } else {
            self.status.set(SyncStatus::Closed);
        }
        self.event_processor.close();
        log::info!("FeatBit client closed");
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

fn parse_bool(value: &str) -> Option<bool> {
    if value.eq_ignore_ascii_case("true") {
        Some(true)
    } else if value.eq_ignore_ascii_case("false") {
        Some(false)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use std::thread;

    use super::*;
    use crate::model::DataSyncEnvelope;

    const READY_BOOTSTRAP: &str = r#"{
        "messageType":"data-sync",
        "data":{"eventType":"full","featureFlags":[{
            "id":"flag-id","key":"enabled","updatedAt":1,"variationType":"boolean",
            "variations":[{"id":"value","value":"true"}],
            "targetUsers":[],"rules":[],"isEnabled":true,
            "fallthrough":{"includedInExpt":false,"variations":[
                {"id":"value","rollout":[0,1],"exptRollout":0}
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
            .disable_events(true)
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
}
