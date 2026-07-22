use std::fmt;
use std::time::{SystemTime, UNIX_EPOCH};

use chrono::Utc;
use serde::Serialize;

use crate::model::FbUser;

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

#[derive(Clone, Debug, Serialize)]
#[serde(untagged)]
pub(super) enum PayloadEvent {
    Evaluation(EvaluationPayload),
    Metric(MetricPayload),
}

impl PayloadEvent {
    pub(super) fn evaluation(user: &FbUser, event: &FbEvaluationEvent) -> Self {
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

    pub(super) fn metric(user: &FbUser, event_name: &str, numeric_value: f64) -> Self {
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
pub(super) struct EvaluationPayload {
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
pub(super) struct MetricPayload {
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
