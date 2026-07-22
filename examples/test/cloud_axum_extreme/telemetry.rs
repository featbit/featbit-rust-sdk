use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

use opentelemetry::logs::{AnyValue, LogRecord, Logger, Severity};
use opentelemetry::Key;

use super::api::TestFlag;
use super::{failure, TestResult, OTEL_EVENT_NAME, OTEL_TARGET};

#[derive(Clone, Default)]
pub(super) struct TestOtelLogger {
    aggregate: Arc<Mutex<OtelAggregate>>,
}

impl TestOtelLogger {
    pub(super) fn validate(
        &self,
        flag: &TestFlag,
        minimum_events: usize,
    ) -> TestResult<OtelReport> {
        let aggregate = match self.aggregate.lock() {
            Ok(aggregate) => aggregate,
            Err(poisoned) => poisoned.into_inner(),
        };
        if aggregate.invalid_schema != 0 {
            return Err(failure(format!(
                "OpenTelemetry emitted {} records with an invalid semantic shape",
                aggregate.invalid_schema
            )));
        }
        if aggregate.privacy_violations != 0 {
            return Err(failure(
                "OpenTelemetry default attributes exposed a context ID or raw variation value",
            ));
        }
        if aggregate.events < minimum_events || aggregate.successes == 0 {
            return Err(failure(format!(
                "OpenTelemetry observed too few successful evaluations: total={}, minimum={minimum_events}",
                aggregate.events
            )));
        }
        if aggregate.errors == 0 || !aggregate.error_types.contains("flag_not_found") {
            return Err(failure(
                "OpenTelemetry did not emit the archived flag-not-found evaluation error",
            ));
        }
        if aggregate.flag_keys.len() != 1 || !aggregate.flag_keys.contains(&flag.key) {
            return Err(failure(
                "OpenTelemetry emitted an evaluation for an unexpected feature flag",
            ));
        }
        Ok(OtelReport {
            events: aggregate.events,
            errors: aggregate.errors,
        })
    }
}

impl Logger for TestOtelLogger {
    type LogRecord = TestOtelLogRecord;

    fn create_log_record(&self) -> Self::LogRecord {
        TestOtelLogRecord::default()
    }

    fn emit(&self, record: Self::LogRecord) {
        let mut aggregate = match self.aggregate.lock() {
            Ok(aggregate) => aggregate,
            Err(poisoned) => poisoned.into_inner(),
        };
        aggregate.events += 1;

        let event_name_valid = record.event_name == Some(OTEL_EVENT_NAME);
        let target_valid = record.target.as_deref() == Some(OTEL_TARGET);
        let timestamp_valid = record.timestamp.is_some();
        let severity_valid = record.severity == Some(Severity::Info)
            && record.severity_text == Some("INFO")
            && !record.body_set;
        let provider_valid =
            attribute_string(&record.attributes, "feature_flag.provider.name") == Some("FeatBit");
        let reason_valid =
            attribute_string(&record.attributes, "feature_flag.result.reason").is_some();
        let experiment_valid = matches!(
            record
                .attributes
                .get("featbit.evaluation.send_to_experiment"),
            Some(AnyValue::Boolean(_))
        );

        let flag_key = attribute_string(&record.attributes, "feature_flag.key");
        if let Some(flag_key) = flag_key {
            aggregate.flag_keys.insert(flag_key.to_owned());
        }
        if let Some(error_type) = attribute_string(&record.attributes, "error.type") {
            aggregate.errors += 1;
            aggregate.error_types.insert(error_type.to_owned());
        } else if record
            .attributes
            .contains_key("feature_flag.result.variant")
        {
            aggregate.successes += 1;
        } else {
            aggregate.invalid_schema += 1;
        }

        if record.attributes.contains_key("feature_flag.context.id")
            || record.attributes.contains_key("feature_flag.result.value")
        {
            aggregate.privacy_violations += 1;
        }
        if !(event_name_valid
            && target_valid
            && timestamp_valid
            && severity_valid
            && provider_valid
            && reason_valid
            && experiment_valid
            && flag_key.is_some())
        {
            aggregate.invalid_schema += 1;
        }
    }

    fn event_enabled(&self, _level: Severity, _target: &str, _name: Option<&str>) -> bool {
        true
    }
}

#[derive(Default)]
pub(super) struct TestOtelLogRecord {
    event_name: Option<&'static str>,
    target: Option<String>,
    timestamp: Option<SystemTime>,
    severity: Option<Severity>,
    severity_text: Option<&'static str>,
    body_set: bool,
    attributes: BTreeMap<String, AnyValue>,
}

impl LogRecord for TestOtelLogRecord {
    fn set_event_name(&mut self, name: &'static str) {
        self.event_name = Some(name);
    }

    fn set_target<T>(&mut self, target: T)
    where
        T: Into<Cow<'static, str>>,
    {
        self.target = Some(target.into().into_owned());
    }

    fn set_timestamp(&mut self, timestamp: SystemTime) {
        self.timestamp = Some(timestamp);
    }

    fn set_observed_timestamp(&mut self, _timestamp: SystemTime) {}

    fn set_severity_text(&mut self, text: &'static str) {
        self.severity_text = Some(text);
    }

    fn set_severity_number(&mut self, number: Severity) {
        self.severity = Some(number);
    }

    fn set_body(&mut self, _body: AnyValue) {
        self.body_set = true;
    }

    fn add_attributes<I, K, V>(&mut self, attributes: I)
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<Key>,
        V: Into<AnyValue>,
    {
        for (key, value) in attributes {
            self.add_attribute(key, value);
        }
    }

    fn add_attribute<K, V>(&mut self, key: K, value: V)
    where
        K: Into<Key>,
        V: Into<AnyValue>,
    {
        let key = key.into();
        self.attributes
            .insert(key.as_str().to_owned(), value.into());
    }
}

fn attribute_string<'a>(attributes: &'a BTreeMap<String, AnyValue>, key: &str) -> Option<&'a str> {
    match attributes.get(key) {
        Some(AnyValue::String(value)) => Some(value.as_str()),
        _ => None,
    }
}

#[derive(Default)]
struct OtelAggregate {
    events: usize,
    successes: usize,
    errors: usize,
    invalid_schema: usize,
    privacy_violations: usize,
    flag_keys: BTreeSet<String>,
    error_types: BTreeSet<String>,
}

pub(super) struct OtelReport {
    pub(super) events: usize,
    pub(super) errors: usize,
}
