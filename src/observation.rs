use std::fmt;
use std::time::SystemTime;

/// Receives a read-only notification after a flag evaluation attempt.
///
/// Observers are independent from `FeatBit` analytics delivery: they are called even when
/// automatic evaluation events or all `FeatBit` events are disabled. Implementations must return
/// promptly, must not perform blocking network I/O, and must not retain sensitive data unless the
/// application explicitly requires it.
pub trait EvaluationObserver: Send + Sync {
    /// Observes one evaluation attempt.
    fn on_evaluation(&self, observation: &EvaluationObservation);
}

/// A normalized reason suitable for observability adapters.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum EvaluationObservationReason {
    /// The flag was disabled.
    Disabled,
    /// A direct target or targeting rule matched.
    TargetingMatch,
    /// A percentage rollout selected the variation.
    Split,
    /// The ordinary fallthrough selected the variation.
    Default,
    /// Evaluation did not produce a variation.
    Error,
}

impl EvaluationObservationReason {
    /// Returns the corresponding OpenTelemetry feature-flag reason value.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::TargetingMatch => "targeting_match",
            Self::Split => "split",
            Self::Default => "default",
            Self::Error => "error",
        }
    }
}

/// A normalized evaluation failure suitable for observability adapters.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum EvaluationObservationError {
    /// The provider was not ready or had already closed.
    ProviderNotReady,
    /// The targeting key was missing.
    TargetingKeyMissing,
    /// The requested flag did not exist.
    FlagNotFound,
    /// Remote flag data could not be evaluated safely.
    ParseError,
    /// The selected variation could not be converted to the requested type.
    TypeMismatch,
}

impl EvaluationObservationError {
    /// Returns the corresponding OpenTelemetry feature-flag error type.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ProviderNotReady => "provider_not_ready",
            Self::TargetingKeyMissing => "targeting_key_missing",
            Self::FlagNotFound => "flag_not_found",
            Self::ParseError => "parse_error",
            Self::TypeMismatch => "type_mismatch",
        }
    }
}

/// An immutable, transport-neutral view of one flag evaluation attempt.
///
/// The targeting key and raw variation value are available so an explicitly configured adapter can
/// opt into them, but the built-in `Debug` implementation redacts both fields.
#[derive(Clone)]
pub struct EvaluationObservation {
    timestamp: SystemTime,
    flag_key: String,
    context_key: Option<String>,
    variation_id: Option<String>,
    variation_value: Option<String>,
    reason: EvaluationObservationReason,
    error: Option<EvaluationObservationError>,
    send_to_experiment: bool,
}

impl EvaluationObservation {
    pub(crate) fn success(
        timestamp: SystemTime,
        flag_key: &str,
        context_key: &str,
        variation_id: &str,
        variation_value: &str,
        reason: EvaluationObservationReason,
        send_to_experiment: bool,
    ) -> Self {
        Self {
            timestamp,
            flag_key: flag_key.to_owned(),
            context_key: Some(context_key.to_owned()),
            variation_id: Some(variation_id.to_owned()),
            variation_value: Some(variation_value.to_owned()),
            reason,
            error: None,
            send_to_experiment,
        }
    }

    pub(crate) fn error(
        timestamp: SystemTime,
        flag_key: &str,
        context_key: Option<&str>,
        error: EvaluationObservationError,
    ) -> Self {
        Self {
            timestamp,
            flag_key: flag_key.to_owned(),
            context_key: context_key.map(str::to_owned),
            variation_id: None,
            variation_value: None,
            reason: EvaluationObservationReason::Error,
            error: Some(error),
            send_to_experiment: false,
        }
    }

    /// Returns when the evaluation was observed.
    #[must_use]
    pub const fn timestamp(&self) -> SystemTime {
        self.timestamp
    }

    /// Returns the requested flag key.
    #[must_use]
    pub fn flag_key(&self) -> &str {
        &self.flag_key
    }

    /// Returns the targeting key, when one was supplied.
    ///
    /// This can identify an application subject. Observability adapters should exclude it by
    /// default and require an explicit privacy decision before exporting it.
    #[must_use]
    pub fn context_key(&self) -> Option<&str> {
        self.context_key.as_deref()
    }

    /// Returns the selected variation ID, or `None` when evaluation failed.
    #[must_use]
    pub fn variation_id(&self) -> Option<&str> {
        self.variation_id.as_deref()
    }

    /// Returns the raw selected variation value, or `None` when evaluation failed.
    ///
    /// Values can be sensitive or large. Observability adapters should exclude them by default.
    #[must_use]
    pub fn variation_value(&self) -> Option<&str> {
        self.variation_value.as_deref()
    }

    /// Returns the normalized evaluation reason.
    #[must_use]
    pub const fn reason(&self) -> EvaluationObservationReason {
        self.reason
    }

    /// Returns the normalized failure, or `None` when a variation was selected.
    #[must_use]
    pub const fn error_type(&self) -> Option<EvaluationObservationError> {
        self.error
    }

    /// Returns whether this exposure is eligible for `FeatBit` experiment attribution.
    #[must_use]
    pub const fn send_to_experiment(&self) -> bool {
        self.send_to_experiment
    }
}

impl fmt::Debug for EvaluationObservation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EvaluationObservation")
            .field("timestamp", &self.timestamp)
            .field("flag_key", &self.flag_key)
            .field(
                "context_key",
                &self.context_key.as_ref().map(|_| "[REDACTED]"),
            )
            .field("variation_id", &self.variation_id)
            .field("has_variation_value", &self.variation_value.is_some())
            .field("reason", &self.reason)
            .field("error", &self.error)
            .field("send_to_experiment", &self.send_to_experiment)
            .finish()
    }
}
