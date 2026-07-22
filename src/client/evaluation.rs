use std::time::SystemTime;

use crate::evaluation::{EvalError, EvalReason, EvalResult, Evaluator};
use crate::events::FbEvaluationEvent;
use crate::model::FbUser;
use crate::observation::{
    EvaluationObservation, EvaluationObservationError, EvaluationObservationReason,
};

use super::{EvaluationDetail, FbClient, ReasonKind};

impl FbClient {
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
    use serde_json::json;

    use super::FbClient;
    use crate::{ClientStatus, FbOptions, FbUser, ReasonKind};

    const TYPED_BOOTSTRAP: &str = r#"{
        "messageType":"data-sync",
        "data":{"eventType":"full","featureFlags":[
            {"id":"bool-id","key":"bool","updatedAt":1,"variationType":"boolean",
             "variations":[{"id":"bool-value","value":"TRUE"}],"isEnabled":true,
             "fallthrough":{"variations":[{"id":"bool-value","rollout":[0,1]}]}},
            {"id":"int-id","key":"int","updatedAt":1,"variationType":"number",
             "variations":[{"id":"int-value","value":"123"}],"isEnabled":true,
             "fallthrough":{"variations":[{"id":"int-value","rollout":[0,1]}]}},
            {"id":"bad-int-id","key":"bad-int","updatedAt":1,"variationType":"number",
             "variations":[{"id":"bad-int-value","value":"123.4"}],"isEnabled":true,
             "fallthrough":{"variations":[{"id":"bad-int-value","rollout":[0,1]}]}},
            {"id":"float-id","key":"float","updatedAt":1,"variationType":"number",
             "variations":[{"id":"float-value","value":"123.45"}],"isEnabled":true,
             "fallthrough":{"variations":[{"id":"float-value","rollout":[0,1]}]}},
            {"id":"bad-float-id","key":"bad-float","updatedAt":1,"variationType":"number",
             "variations":[{"id":"bad-float-value","value":"NaN"}],"isEnabled":true,
             "fallthrough":{"variations":[{"id":"bad-float-value","rollout":[0,1]}]}},
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
        assert_eq!(client.int_variation("int", &user, 0), 123);
        assert_eq!(client.int_variation("bad-int", &user, 7), 7);
        assert_eq!(
            client.float_variation("float", &user, 0.0).to_bits(),
            123.45_f64.to_bits()
        );
        assert_eq!(
            client.float_variation("bad-float", &user, 7.5).to_bits(),
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
}
