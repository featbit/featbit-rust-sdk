//! OpenTelemetry evaluation-event integration for `featbit-server-sdk`.
//!
//! The adapter emits the OpenTelemetry semantic event `feature_flag.evaluation` through a logger
//! supplied by the application. It never configures a global provider or exporter, and it never
//! sends or duplicates `FeatBit` analytics events.

#![forbid(unsafe_code)]

use std::fmt;

use featbit_server_sdk::{EvaluationObservation, EvaluationObserver};
use opentelemetry::logs::{LogRecord, Logger, Severity};

const EVENT_NAME: &str = "feature_flag.evaluation";
const INSTRUMENTATION_TARGET: &str = "featbit-server-sdk";

/// Emits `feature_flag.evaluation` events through an application-owned OpenTelemetry logger.
///
/// Variation values and context identifiers are excluded by default because they can be sensitive
/// or high-cardinality. The variation ID, normalized reason, provider name, and any standardized
/// error type are always included. Configure the supplied logger with a batch processor so exporter
/// I/O never runs on a flag-evaluation thread.
pub struct OpenTelemetryEvaluationObserver<L> {
    logger: L,
    include_context_id: bool,
    include_value: bool,
}

impl<L> OpenTelemetryEvaluationObserver<L> {
    /// Creates an observer backed by `logger` with privacy-sensitive attributes disabled.
    #[must_use]
    pub const fn new(logger: L) -> Self {
        Self {
            logger,
            include_context_id: false,
            include_value: false,
        }
    }

    /// Enables or disables exporting the targeting key as `feature_flag.context.id`.
    ///
    /// The targeting key can identify an application subject and is disabled by default.
    #[must_use]
    pub const fn with_context_id(mut self, include: bool) -> Self {
        self.include_context_id = include;
        self
    }

    /// Enables or disables exporting the raw variation as `feature_flag.result.value`.
    ///
    /// Values can be sensitive or large and are disabled by default. The variation ID remains
    /// available as `feature_flag.result.variant`.
    #[must_use]
    pub const fn with_value(mut self, include: bool) -> Self {
        self.include_value = include;
        self
    }
}

impl<L> EvaluationObserver for OpenTelemetryEvaluationObserver<L>
where
    L: Logger + Send + Sync,
{
    fn on_evaluation(&self, observation: &EvaluationObservation) {
        if !self
            .logger
            .event_enabled(Severity::Info, INSTRUMENTATION_TARGET, Some(EVENT_NAME))
        {
            return;
        }
        let mut record = self.logger.create_log_record();
        record.set_event_name(EVENT_NAME);
        record.set_target(INSTRUMENTATION_TARGET);
        record.set_timestamp(observation.timestamp());
        record.set_severity_number(Severity::Info);
        record.set_severity_text("INFO");
        record.add_attribute("feature_flag.key", observation.flag_key().to_owned());
        record.add_attribute("feature_flag.provider.name", "FeatBit");
        record.add_attribute("feature_flag.result.reason", observation.reason().as_str());
        record.add_attribute(
            "featbit.evaluation.send_to_experiment",
            observation.send_to_experiment(),
        );

        if let Some(variation_id) = observation.variation_id() {
            record.add_attribute("feature_flag.result.variant", variation_id.to_owned());
        }
        if let Some(error) = observation.error_type() {
            record.add_attribute("error.type", error.as_str());
        }
        if self.include_context_id {
            if let Some(context_key) = observation.context_key() {
                record.add_attribute("feature_flag.context.id", context_key.to_owned());
            }
        }
        if self.include_value {
            if let Some(value) = observation.variation_value() {
                record.add_attribute("feature_flag.result.value", value.to_owned());
            }
        }

        self.logger.emit(record);
    }
}

impl<L> fmt::Debug for OpenTelemetryEvaluationObserver<L> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OpenTelemetryEvaluationObserver")
            .field("include_context_id", &self.include_context_id)
            .field("include_value", &self.include_value)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;
    use std::collections::BTreeMap;
    use std::sync::{Arc, Mutex};
    use std::time::SystemTime;

    use featbit_server_sdk::{FbClient, FbOptions, FbUser};
    use opentelemetry::logs::{AnyValue, LogRecord, Logger, Severity};
    use opentelemetry::Key;

    use super::*;

    const READY_BOOTSTRAP: &str = r#"{
        "messageType":"data-sync",
        "data":{"eventType":"full","featureFlags":[{
            "id":"flag-id","key":"enabled","updatedAt":1,"variationType":"boolean",
            "variations":[{"id":"on","value":"true"}],
            "targetUsers":[],"rules":[],"isEnabled":true,
            "fallthrough":{"includedInExpt":false,"variations":[
                {"id":"on","rollout":[0,1],"exptRollout":0}
            ]}
        },{
            "id":"invalid-flag-id","key":"invalid-bool","updatedAt":1,"variationType":"boolean",
            "variations":[{"id":"invalid","value":"not-a-boolean"}],
            "targetUsers":[],"rules":[],"isEnabled":true,
            "fallthrough":{"includedInExpt":false,"variations":[
                {"id":"invalid","rollout":[0,1],"exptRollout":0}
            ]}
        }],"segments":[]}
    }"#;

    #[derive(Clone, Default)]
    struct TestLogger {
        records: Arc<Mutex<Vec<TestLogRecord>>>,
    }

    impl Logger for TestLogger {
        type LogRecord = TestLogRecord;

        fn create_log_record(&self) -> Self::LogRecord {
            TestLogRecord::default()
        }

        fn emit(&self, record: Self::LogRecord) {
            self.records
                .lock()
                .expect("test logger lock should remain available")
                .push(record);
        }

        fn event_enabled(&self, _level: Severity, _target: &str, _name: Option<&str>) -> bool {
            true
        }
    }

    #[derive(Default)]
    struct TestLogRecord {
        event_name: Option<&'static str>,
        timestamp: Option<SystemTime>,
        severity: Option<Severity>,
        attributes: BTreeMap<String, AnyValue>,
    }

    impl LogRecord for TestLogRecord {
        fn set_event_name(&mut self, name: &'static str) {
            self.event_name = Some(name);
        }

        fn set_target<T>(&mut self, _target: T)
        where
            T: Into<Cow<'static, str>>,
        {
        }

        fn set_timestamp(&mut self, timestamp: SystemTime) {
            self.timestamp = Some(timestamp);
        }

        fn set_observed_timestamp(&mut self, _timestamp: SystemTime) {}

        fn set_severity_text(&mut self, _text: &'static str) {}

        fn set_severity_number(&mut self, number: Severity) {
            self.severity = Some(number);
        }

        fn set_body(&mut self, _body: AnyValue) {}

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

    #[test]
    fn emits_semantic_event_without_sensitive_attributes_by_default() {
        let logger = TestLogger::default();
        let records = Arc::clone(&logger.records);
        let observer = OpenTelemetryEvaluationObserver::new(logger);
        let options = FbOptions::builder("valid-secret")
            .offline(true)
            .disable_events(true, false)
            .evaluation_observer(observer)
            .bootstrap_json(READY_BOOTSTRAP)
            .build()
            .expect("offline options should build");
        let client = FbClient::with_options(options);
        let user = FbUser::builder("private-user-key").build();

        assert!(client.bool_variation("enabled", &user, false));
        assert!(!client.bool_variation("missing", &user, false));
        assert!(client.bool_variation("invalid-bool", &user, true));

        let records = records
            .lock()
            .expect("test logger lock should remain available");
        assert_eq!(records.len(), 3);
        let record = &records[0];
        assert_eq!(record.event_name, Some(EVENT_NAME));
        assert_eq!(record.severity, Some(Severity::Info));
        assert!(record.timestamp.is_some());
        assert_eq!(
            record.attributes.get("feature_flag.key"),
            Some(&AnyValue::from("enabled".to_owned()))
        );
        assert_eq!(
            record.attributes.get("feature_flag.result.variant"),
            Some(&AnyValue::from("on".to_owned()))
        );
        assert!(!record.attributes.contains_key("feature_flag.context.id"));
        assert!(!record.attributes.contains_key("feature_flag.result.value"));
        assert_eq!(
            records[1].attributes.get("error.type"),
            Some(&AnyValue::from("flag_not_found"))
        );
        assert_eq!(
            records[1].attributes.get("feature_flag.result.reason"),
            Some(&AnyValue::from("error"))
        );
        assert_eq!(
            records[2].attributes.get("error.type"),
            Some(&AnyValue::from("type_mismatch"))
        );
        assert!(!records[2]
            .attributes
            .contains_key("feature_flag.result.variant"));
        client.close();
    }
}
