use std::time::SystemTime;

use crate::evaluation::{EvalError, EvalReason, EvalResult, EvaluationReason, Evaluator};
use crate::events::FbEvaluationEvent;
use crate::model::FbUser;
use crate::observation::{
    EvaluationObservation, EvaluationObservationError, EvaluationObservationReason,
};

use super::{EvaluationDetail, FbClient, ReasonKind};

/// A successful `FeatBit` evaluation before its string value is converted to an application type.
///
/// This advanced result is intended for adapters that must preserve `FeatBit` metadata and decide
/// whether a converted value is valid before recording the evaluation. Calling
/// [`FbClient::evaluate_raw`] does not emit an automatic evaluation event or a success observation;
/// call [`FbClient::complete_raw_evaluation`] after conversion succeeds.
#[derive(Clone, Debug)]
pub struct RawEvaluation {
    flag_id: String,
    flag_type: String,
    reason: EvaluationReason,
    event: FbEvaluationEvent,
}

#[derive(Clone, Copy)]
struct SuccessfulEvaluation<'a> {
    timestamp: SystemTime,
    key: &'a str,
    context_key: &'a str,
    variation_id: &'a str,
    variation_value: &'a str,
    reason: EvaluationObservationReason,
    send_to_experiment: bool,
}

impl RawEvaluation {
    /// Returns the evaluated flag key.
    #[must_use]
    pub fn flag_key(&self) -> &str {
        self.event.flag_key()
    }

    /// Returns the internal `FeatBit` flag ID.
    #[must_use]
    pub fn flag_id(&self) -> &str {
        &self.flag_id
    }

    /// Returns the `FeatBit` variation type recorded with the flag.
    #[must_use]
    pub fn flag_type(&self) -> &str {
        &self.flag_type
    }

    /// Returns the selected variation ID.
    #[must_use]
    pub fn variation_id(&self) -> &str {
        self.event.variation_id()
    }

    /// Returns the selected variation's unconverted string value.
    #[must_use]
    pub fn value(&self) -> &str {
        self.event.variation_value()
    }

    /// Returns why `FeatBit` selected the variation.
    #[must_use]
    pub const fn reason(&self) -> &EvaluationReason {
        &self.reason
    }

    /// Returns the immutable event snapshot captured with this evaluation.
    #[must_use]
    pub const fn evaluation_event(&self) -> &FbEvaluationEvent {
        &self.event
    }
}

/// A typed failure returned by [`FbClient::evaluate_raw`].
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[non_exhaustive]
pub enum EvaluationError {
    /// The client has not initialized, is unavailable, or has closed.
    #[error("client not ready")]
    ClientNotReady,
    /// The `FeatBit` user has no targeting key.
    #[error("targeting key is missing")]
    TargetingKeyMissing,
    /// The requested flag does not exist or is archived.
    #[error("flag not found")]
    FlagNotFound,
    /// Remote flag data cannot produce a valid variation.
    #[error("malformed flag")]
    MalformedFlag,
}

impl FbClient {
    /// Evaluates a boolean flag, returning `default` on every failure.
    #[must_use]
    pub fn bool_variation(&self, key: &str, user: &FbUser, default: bool) -> bool {
        self.evaluate_typed_value(key, user, || default, parse_bool)
    }

    /// Evaluates a boolean flag with diagnostics.
    #[must_use]
    pub fn bool_variation_detail(
        &self,
        key: &str,
        user: &FbUser,
        default: bool,
    ) -> EvaluationDetail<bool> {
        self.evaluate_typed_detail(key, user, || default, parse_bool)
    }

    /// Evaluates a signed 64-bit integer flag, returning `default` on every failure.
    #[must_use]
    pub fn int_variation(&self, key: &str, user: &FbUser, default: i64) -> i64 {
        self.evaluate_typed_value(key, user, || default, parse_int)
    }

    /// Evaluates a signed 64-bit integer flag with diagnostics.
    #[must_use]
    pub fn int_variation_detail(
        &self,
        key: &str,
        user: &FbUser,
        default: i64,
    ) -> EvaluationDetail<i64> {
        self.evaluate_typed_detail(key, user, || default, parse_int)
    }

    /// Evaluates a 64-bit floating-point flag, returning `default` on every failure.
    #[must_use]
    pub fn float_variation(&self, key: &str, user: &FbUser, default: f64) -> f64 {
        self.evaluate_typed_value(key, user, || default, parse_float)
    }

    /// Evaluates a 64-bit floating-point flag with diagnostics.
    #[must_use]
    pub fn float_variation_detail(
        &self,
        key: &str,
        user: &FbUser,
        default: f64,
    ) -> EvaluationDetail<f64> {
        self.evaluate_typed_detail(key, user, || default, parse_float)
    }

    /// Evaluates a string flag, returning `default` on every failure.
    #[must_use]
    pub fn string_variation(&self, key: &str, user: &FbUser, default: &str) -> String {
        self.evaluate_typed_value(
            key,
            user,
            || default.to_owned(),
            |value| Ok(value.to_owned()),
        )
    }

    /// Evaluates a string flag with diagnostics.
    #[must_use]
    pub fn string_variation_detail(
        &self,
        key: &str,
        user: &FbUser,
        default: &str,
    ) -> EvaluationDetail<String> {
        self.evaluate_typed_detail(
            key,
            user,
            || default.to_owned(),
            |value| Ok(value.to_owned()),
        )
    }

    /// Evaluates a JSON flag, returning `default` on every failure.
    #[must_use]
    pub fn json_variation(
        &self,
        key: &str,
        user: &FbUser,
        default: serde_json::Value,
    ) -> serde_json::Value {
        self.evaluate_typed_value(key, user, || default, parse_json)
    }

    /// Evaluates a JSON flag with diagnostics.
    #[must_use]
    pub fn json_variation_detail(
        &self,
        key: &str,
        user: &FbUser,
        default: serde_json::Value,
    ) -> EvaluationDetail<serde_json::Value> {
        self.evaluate_typed_detail(key, user, || default, parse_json)
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
            .map(|(key, _)| key.as_ref())
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

    fn evaluate_typed_value<T>(
        &self,
        key: &str,
        user: &FbUser,
        default: impl FnOnce() -> T,
        converter: impl FnOnce(&str) -> Result<T, EvaluationObservationError>,
    ) -> T {
        let evaluated = self.evaluate_and_then(key, user, |result| {
            let timestamp = self
                .should_complete_value_evaluation()
                .then(SystemTime::now);
            let value = converter(&result.variation.value)?;
            let observation = timestamp
                .and_then(|timestamp| self.complete_evaluation(user, key, result, timestamp));
            Ok((value, observation))
        });

        match evaluated {
            Ok(Ok((value, observation))) => {
                self.notify_successful_evaluation(observation);
                value
            }
            Ok(Err(error)) => {
                self.observe_evaluation_error(key, Some(user.key()), error);
                default()
            }
            Err(_) => default(),
        }
    }

    fn evaluate_typed_detail<T>(
        &self,
        key: &str,
        user: &FbUser,
        default: impl FnOnce() -> T,
        converter: impl FnOnce(&str) -> Result<T, EvaluationObservationError>,
    ) -> EvaluationDetail<T> {
        let evaluated = self.evaluate_and_then(key, user, |result| {
            let timestamp = SystemTime::now();
            let value = converter(&result.variation.value)?;
            let (kind, reason) = reason_detail(result.reason);
            let event = FbEvaluationEvent::at(
                key,
                result.variation.id.as_str(),
                result.variation.value.as_str(),
                timestamp,
                result.send_to_experiment,
            );
            Ok((
                EvaluationDetail {
                    key: key.to_owned(),
                    kind,
                    reason,
                    value,
                    variation_id: result.variation.id.clone(),
                    evaluation_event: Some(event),
                },
                observation_reason_borrowed(result.reason),
            ))
        });

        match evaluated {
            Ok(Ok((detail, observation_reason))) => {
                if let Some(event) = &detail.evaluation_event {
                    self.complete_captured_evaluation(user, event, observation_reason);
                }
                detail
            }
            Ok(Err(error)) => {
                self.observe_evaluation_error(key, Some(user.key()), error);
                let (kind, reason) = conversion_failure_detail(error);
                EvaluationDetail::fallback(key, kind, reason, default())
            }
            Err(error) => {
                let (kind, reason) = evaluation_failure_detail(error);
                EvaluationDetail::fallback(key, kind, reason, default())
            }
        }
    }

    /// Evaluates a flag without converting its string value or recording a successful evaluation.
    ///
    /// This advanced API lets an integration convert the result into its own type system before it
    /// calls [`Self::complete_raw_evaluation`]. Evaluation is local and does not perform I/O. A
    /// failure is reported to the configured evaluation observer before it is returned.
    ///
    /// # Errors
    ///
    /// Returns a typed [`EvaluationError`] when the client cannot evaluate the flag.
    pub fn evaluate_raw(&self, key: &str, user: &FbUser) -> Result<RawEvaluation, EvaluationError> {
        self.evaluate_and_then(key, user, |result| RawEvaluation {
            flag_id: result.flag_id.to_owned(),
            flag_type: result.flag_type.to_owned(),
            reason: result.reason.into_owned(),
            event: FbEvaluationEvent::at(
                key,
                result.variation.id.as_str(),
                result.variation.value.as_str(),
                SystemTime::now(),
                result.send_to_experiment,
            ),
        })
    }

    fn evaluate_and_then<R>(
        &self,
        key: &str,
        user: &FbUser,
        use_result: impl for<'snapshot> FnOnce(EvalResult<'snapshot>) -> R,
    ) -> Result<R, EvaluationError> {
        if !self.evaluation_available() {
            self.observe_evaluation_error(
                key,
                (!user.key().is_empty()).then_some(user.key()),
                EvaluationObservationError::ProviderNotReady,
            );
            return Err(EvaluationError::ClientNotReady);
        }
        let snapshot = self.inner.store.load();
        let result = match Evaluator::evaluate(&snapshot, key, user) {
            Ok(result) => result,
            Err(error) => {
                let evaluation_error = EvaluationError::from(error);
                self.observe_evaluation_error(
                    key,
                    (!user.key().is_empty()).then_some(user.key()),
                    observation_error(evaluation_error),
                );
                return Err(evaluation_error);
            }
        };
        Ok(use_result(result))
    }

    /// Records a successful raw evaluation after an integration has converted its value.
    ///
    /// This applies the same automatic analytics and observer behavior as a successful typed
    /// variation method. Call it at most once for a raw result; repeated calls can enqueue duplicate
    /// analytics events.
    pub fn complete_raw_evaluation(&self, user: &FbUser, evaluation: &RawEvaluation) {
        self.complete_captured_evaluation(
            user,
            &evaluation.event,
            observation_reason(&evaluation.reason),
        );
    }

    fn should_complete_value_evaluation(&self) -> bool {
        self.inner.options.evaluation_observer.is_some()
            || (!self.inner.options.disable_events && self.inner.event_processor.is_accepting())
    }

    fn complete_evaluation(
        &self,
        user: &FbUser,
        key: &str,
        result: EvalResult<'_>,
        timestamp: SystemTime,
    ) -> Option<EvaluationObservation> {
        if !self.inner.options.disable_events {
            let _accepted = self.inner.event_processor.record_evaluation_at(
                user,
                key,
                &result.variation.id,
                &result.variation.value,
                timestamp,
                result.send_to_experiment,
            );
        }
        self.successful_evaluation_observation(SuccessfulEvaluation {
            timestamp,
            key,
            context_key: user.key(),
            variation_id: &result.variation.id,
            variation_value: &result.variation.value,
            reason: observation_reason_borrowed(result.reason),
            send_to_experiment: result.send_to_experiment,
        })
    }

    fn complete_captured_evaluation(
        &self,
        user: &FbUser,
        event: &FbEvaluationEvent,
        reason: EvaluationObservationReason,
    ) {
        if !self.inner.options.disable_events {
            let _accepted = self.inner.event_processor.record_evaluation(user, event);
        }
        let observation = self.successful_evaluation_observation(SuccessfulEvaluation {
            timestamp: event.timestamp(),
            key: event.flag_key(),
            context_key: user.key(),
            variation_id: event.variation_id(),
            variation_value: event.variation_value(),
            reason,
            send_to_experiment: event.send_to_experiment(),
        });
        self.notify_successful_evaluation(observation);
    }

    fn successful_evaluation_observation(
        &self,
        evaluation: SuccessfulEvaluation<'_>,
    ) -> Option<EvaluationObservation> {
        self.inner.options.evaluation_observer.as_ref()?;
        Some(EvaluationObservation::success(
            evaluation.timestamp,
            evaluation.key,
            evaluation.context_key,
            evaluation.variation_id,
            evaluation.variation_value,
            evaluation.reason,
            evaluation.send_to_experiment,
        ))
    }

    fn notify_successful_evaluation(&self, observation: Option<EvaluationObservation>) {
        if let (Some(observer), Some(observation)) =
            (&self.inner.options.evaluation_observer, observation)
        {
            observer.on_evaluation(&observation);
        }
    }

    /// Reports an integration-side evaluation failure to the configured observer.
    ///
    /// This method performs no analytics tracking and is a no-op when no observer is configured.
    /// `context_key` should be omitted when conversion failed before a valid user was available.
    pub fn observe_evaluation_error(
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
}

impl From<EvalError> for EvaluationError {
    fn from(error: EvalError) -> Self {
        match error {
            EvalError::InvalidContext => Self::TargetingKeyMissing,
            EvalError::FlagNotFound => Self::FlagNotFound,
            EvalError::MalformedFlag => Self::MalformedFlag,
        }
    }
}

fn string_detail_from_result(key: &str, result: EvalResult<'_>) -> EvaluationDetail<String> {
    let (kind, reason) = reason_detail(result.reason);
    EvaluationDetail {
        key: key.to_owned(),
        kind,
        reason,
        value: result.variation.value.clone(),
        variation_id: result.variation.id.clone(),
        evaluation_event: None,
    }
}

fn observation_reason(reason: &EvaluationReason) -> EvaluationObservationReason {
    match reason {
        EvaluationReason::Off => EvaluationObservationReason::Disabled,
        EvaluationReason::TargetMatch | EvaluationReason::RuleMatch { split: false, .. } => {
            EvaluationObservationReason::TargetingMatch
        }
        EvaluationReason::RuleMatch { split: true, .. }
        | EvaluationReason::Fallthrough { split: true } => EvaluationObservationReason::Split,
        EvaluationReason::Fallthrough { split: false } => EvaluationObservationReason::Default,
    }
}

fn observation_reason_borrowed(reason: EvalReason<'_>) -> EvaluationObservationReason {
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

fn observation_error(error: EvaluationError) -> EvaluationObservationError {
    match error {
        EvaluationError::ClientNotReady => EvaluationObservationError::ProviderNotReady,
        EvaluationError::TargetingKeyMissing => EvaluationObservationError::TargetingKeyMissing,
        EvaluationError::FlagNotFound => EvaluationObservationError::FlagNotFound,
        EvaluationError::MalformedFlag => EvaluationObservationError::ParseError,
    }
}

fn reason_detail(reason: EvalReason<'_>) -> (ReasonKind, String) {
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

const fn conversion_failure_detail(
    error: EvaluationObservationError,
) -> (ReasonKind, &'static str) {
    match error {
        EvaluationObservationError::ParseError => (ReasonKind::Error, "parse error"),
        _ => (ReasonKind::WrongType, "type mismatch"),
    }
}

const fn evaluation_failure_detail(error: EvaluationError) -> (ReasonKind, &'static str) {
    match error {
        EvaluationError::ClientNotReady => (ReasonKind::ClientNotReady, "client not ready"),
        EvaluationError::TargetingKeyMissing => (ReasonKind::Error, "targeting key is missing"),
        EvaluationError::FlagNotFound => (ReasonKind::Error, "flag not found"),
        EvaluationError::MalformedFlag => (ReasonKind::Error, "malformed flag"),
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

fn parse_int(value: &str) -> Result<i64, EvaluationObservationError> {
    value
        .parse()
        .map_err(|_| EvaluationObservationError::TypeMismatch)
}

fn parse_float(value: &str) -> Result<f64, EvaluationObservationError> {
    value
        .parse::<f64>()
        .ok()
        .filter(|number| number.is_finite())
        .ok_or(EvaluationObservationError::TypeMismatch)
}

fn parse_json(value: &str) -> Result<serde_json::Value, EvaluationObservationError> {
    serde_json::from_str(value).map_err(|_| EvaluationObservationError::ParseError)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::FbClient;
    use crate::model::DataSyncEnvelope;
    use crate::{ClientStatus, EvaluationError, EvaluationReason, FbOptions, FbUser, ReasonKind};

    const TYPED_BOOTSTRAP: &str = r#"{
        "messageType":"data-sync",
        "data":{"eventType":"full","featureFlags":[
            {"id":"bool-id","key":"bool","updatedAt":1,"variationType":"boolean",
             "variations":[{"id":"bool-value","value":"TRUE"}],"isEnabled":true,
             "fallthrough":{"variations":[{"id":"bool-value","rollout":[0,1]}]}},
            {"id":"false-id","key":"false-bool","updatedAt":1,"variationType":"boolean",
             "variations":[{"id":"false-value","value":"FALSE"}],"isEnabled":true,
             "fallthrough":{"variations":[{"id":"false-value","rollout":[0,1]}]}},
            {"id":"int-id","key":"int","updatedAt":1,"variationType":"number",
             "variations":[{"id":"int-value","value":"123"}],"isEnabled":true,
             "fallthrough":{"variations":[{"id":"int-value","rollout":[0,1]}]}},
            {"id":"bad-int-id","key":"bad-int","updatedAt":1,"variationType":"number",
             "variations":[{"id":"bad-int-value","value":"123.4"}],"isEnabled":true,
             "fallthrough":{"variations":[{"id":"bad-int-value","rollout":[0,1]}]}},
            {"id":"bad-int-text-id","key":"bad-int-text","updatedAt":1,"variationType":"number",
             "variations":[{"id":"bad-int-text-value","value":"v123"}],"isEnabled":true,
             "fallthrough":{"variations":[{"id":"bad-int-text-value","rollout":[0,1]}]}},
            {"id":"float-id","key":"float","updatedAt":1,"variationType":"number",
             "variations":[{"id":"float-value","value":"123.45"}],"isEnabled":true,
             "fallthrough":{"variations":[{"id":"float-value","rollout":[0,1]}]}},
            {"id":"whole-float-id","key":"whole-float","updatedAt":1,"variationType":"number",
             "variations":[{"id":"whole-float-value","value":"123"}],"isEnabled":true,
             "fallthrough":{"variations":[{"id":"whole-float-value","rollout":[0,1]}]}},
            {"id":"double-id","key":"double","updatedAt":1,"variationType":"number",
             "variations":[{"id":"double-value","value":"123.456"}],"isEnabled":true,
             "fallthrough":{"variations":[{"id":"double-value","rollout":[0,1]}]}},
            {"id":"bad-float-id","key":"bad-float","updatedAt":1,"variationType":"number",
             "variations":[{"id":"bad-float-value","value":"NaN"}],"isEnabled":true,
             "fallthrough":{"variations":[{"id":"bad-float-value","rollout":[0,1]}]}},
            {"id":"bad-float-text-id","key":"bad-float-text","updatedAt":1,"variationType":"number",
             "variations":[{"id":"bad-float-text-value","value":"v123.4"}],"isEnabled":true,
             "fallthrough":{"variations":[{"id":"bad-float-text-value","rollout":[0,1]}]}},
            {"id":"json-id","key":"json","updatedAt":1,"variationType":"json",
             "variations":[{"id":"json-value","value":"{\"enabled\":true}"}],"isEnabled":true,
             "fallthrough":{"variations":[{"id":"json-value","rollout":[0,1]}]}}
        ],"segments":[]}}
    "#;

    fn typed_client() -> FbClient {
        let options = FbOptions::builder("secret")
            .offline(true)
            .bootstrap_json(TYPED_BOOTSTRAP)
            .build()
            .expect("typed bootstrap should be valid");
        FbClient::with_options(options)
    }

    #[test]
    fn direct_value_converters_match_dotnet_fixtures_and_fallback_contract() {
        let client = typed_client();
        let user = FbUser::builder("u1").build();

        assert!(client.bool_variation("bool", &user, false));
        assert!(!client.bool_variation("false-bool", &user, true));
        assert_eq!(client.int_variation("int", &user, 0), 123);
        assert_eq!(client.int_variation("bad-int", &user, 7), 7);
        assert_eq!(client.int_variation("bad-int-text", &user, 7), 7);
        assert_eq!(
            client.float_variation("float", &user, 0.0).to_bits(),
            123.45_f64.to_bits()
        );
        assert_eq!(
            client.float_variation("whole-float", &user, 0.0).to_bits(),
            123.0_f64.to_bits()
        );
        assert_eq!(
            client.float_variation("double", &user, 0.0).to_bits(),
            123.456_f64.to_bits()
        );
        assert_eq!(
            client.float_variation("bad-float", &user, 7.5).to_bits(),
            7.5_f64.to_bits()
        );
        assert_eq!(
            client
                .float_variation("bad-float-text", &user, 7.5)
                .to_bits(),
            7.5_f64.to_bits()
        );
        assert_eq!(client.string_variation("int", &user, "fallback"), "123");
        assert_eq!(
            client.json_variation("json", &user, json!({"fallback": true})),
            json!({"enabled": true})
        );

        let mismatch = client.int_variation_detail("bad-int", &user, 7);
        assert_eq!(mismatch.kind, ReasonKind::WrongType);
        assert_eq!(mismatch.reason, "type mismatch");
        assert!(mismatch.evaluation_event.is_none());
    }

    #[test]
    fn large_json_variations_preserve_value_and_retained_event_semantics() {
        let client = typed_client();
        let mut envelope = serde_json::from_str::<DataSyncEnvelope>(TYPED_BOOTSTRAP)
            .expect("typed bootstrap should parse");
        let large_text = "x".repeat(256 * 1024);
        let raw_value = serde_json::to_string(&json!({
            "payload": large_text,
            "nested": {
                "enabled": true,
                "values": [1, 2, 3, 4]
            }
        }))
        .expect("large JSON variation should serialize");
        let json_flag = envelope
            .data
            .feature_flags
            .iter_mut()
            .find(|flag| flag.key == "json")
            .expect("JSON flag should exist");
        json_flag.variations[0].value.clone_from(&raw_value);
        client.inner.store.populate(&envelope.data);
        let user = FbUser::builder("large-json-user").build();

        let value = client.json_variation("json", &user, json!({"fallback": true}));
        assert_eq!(value["payload"].as_str().map(str::len), Some(256 * 1024));
        assert_eq!(value["nested"]["enabled"], true);

        let detail = client.json_variation_detail("json", &user, json!({"fallback": true}));
        assert_eq!(
            detail.value["payload"].as_str().map(str::len),
            Some(256 * 1024)
        );
        assert_eq!(
            detail
                .evaluation_event
                .as_ref()
                .map(crate::FbEvaluationEvent::variation_value),
            Some(raw_value.as_str())
        );
    }

    #[test]
    fn empty_offline_store_is_ready_and_unknown_flags_use_fallbacks() {
        let options = FbOptions::builder("secret")
            .offline(true)
            .build()
            .expect("offline options should be valid");
        let client = FbClient::with_options(options);
        let user = FbUser::builder("u1").build();

        assert!(client.initialized());
        assert_eq!(client.status(), ClientStatus::Ready);
        let detail = client.string_variation_detail("missing", &user, "fallback");
        assert_eq!(detail.value, "fallback");
        assert_eq!(detail.kind, ReasonKind::Error);
        assert_eq!(detail.reason, "flag not found");
    }

    #[test]
    fn raw_evaluation_exposes_protocol_neutral_adapter_metadata() {
        let client = typed_client();
        let user = FbUser::builder("u1").build();

        let evaluation = client
            .evaluate_raw("bool", &user)
            .expect("known flag should evaluate");
        assert_eq!(evaluation.flag_key(), "bool");
        assert_eq!(evaluation.flag_id(), "bool-id");
        assert_eq!(evaluation.flag_type(), "boolean");
        assert_eq!(evaluation.variation_id(), "bool-value");
        assert_eq!(evaluation.value(), "TRUE");
        assert_eq!(
            evaluation.reason(),
            &EvaluationReason::Fallthrough { split: false }
        );
        assert_eq!(evaluation.evaluation_event().flag_key(), "bool");
        assert_eq!(evaluation.evaluation_event().variation_id(), "bool-value");

        client.complete_raw_evaluation(&user, &evaluation);
    }

    #[test]
    fn raw_evaluation_returns_stable_typed_errors() {
        let client = typed_client();
        let user = FbUser::builder("u1").build();
        let empty_user = FbUser::builder("").build();

        assert!(matches!(
            client.evaluate_raw("bool", &empty_user),
            Err(EvaluationError::TargetingKeyMissing)
        ));
        assert!(matches!(
            client.evaluate_raw("missing", &user),
            Err(EvaluationError::FlagNotFound)
        ));

        client.close();
        assert!(matches!(
            client.evaluate_raw("bool", &user),
            Err(EvaluationError::ClientNotReady)
        ));
    }
}
