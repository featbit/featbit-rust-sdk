use std::sync::{Arc, Mutex};

use open_feature::provider::FeatureProvider;

use super::*;
use crate::observation::{EvaluationObservation, EvaluationObserver};
use crate::options::FbOptionsBuilder;

const BOOTSTRAP: &str = r#"{
        "messageType":"data-sync",
        "data":{"eventType":"full","featureFlags":[{
            "id":"flag-id","key":"enabled","updatedAt":1,"variationType":"boolean",
            "variations":[{"id":"on","value":"true"},{"id":"off","value":"false"}],
            "targetUsers":[],"rules":[],"isEnabled":true,"disabledVariationId":"off",
            "fallthrough":{"includedInExpt":false,"variations":[{"id":"on","rollout":[0,1],"exptRollout":0}]}
        },{
            "id":"invalid-flag-id","key":"invalid-bool","updatedAt":1,"variationType":"boolean",
            "variations":[{"id":"invalid","value":"not-a-boolean"}],
            "targetUsers":[],"rules":[],"isEnabled":true,
            "fallthrough":{"includedInExpt":false,"variations":[{"id":"invalid","rollout":[0,1],"exptRollout":0}]}
        }],"segments":[]}
    }"#;

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

fn provider() -> FeatBitProvider {
    let options = FbOptionsBuilder::new("valid-secret")
        .offline(true)
        .bootstrap_json(BOOTSTRAP)
        .build()
        .expect("offline options should build");
    FeatBitProvider::new(options)
}

fn provider_with_typed_values() -> FeatBitProvider {
    fn flag(key: &str, variation_type: &str, value: &str) -> serde_json::Value {
        serde_json::json!({
            "id": format!("{key}-id"),
            "key": key,
            "updatedAt": 1,
            "variationType": variation_type,
            "variations": [{"id": "value", "value": value}],
            "targetUsers": [],
            "rules": [],
            "isEnabled": true,
            "fallthrough": {
                "includedInExpt": false,
                "variations": [{"id": "value", "rollout": [0, 1], "exptRollout": 0}]
            }
        })
    }

    let bootstrap = serde_json::json!({
        "messageType": "data-sync",
        "data": {
            "eventType": "full",
            "featureFlags": [
                flag("integer", "number", "42"),
                flag("float", "number", "1.5"),
                flag("text", "string", "hello"),
                flag("object", "json", r#"{"enabled":true,"count":2}"#)
            ],
            "segments": []
        }
    });
    let options = FbOptionsBuilder::new("valid-secret")
        .offline(true)
        .bootstrap_json(bootstrap.to_string())
        .build()
        .expect("typed bootstrap options should build");
    FeatBitProvider::new(options)
}

#[tokio::test]
async fn resolves_boolean_through_open_feature_trait() {
    let provider = provider();
    let context = EvaluationContext::default().with_targeting_key("user-1");
    let details = provider
        .resolve_bool_value("enabled", &context)
        .await
        .expect("flag should resolve");
    assert!(details.value);
    assert_eq!(details.variant.as_deref(), Some("on"));
    assert_eq!(details.reason, Some(EvaluationReason::Default));
    assert_eq!(provider.status(), ProviderStatus::Ready);
}

#[tokio::test]
async fn missing_targeting_key_is_typed_error() {
    let error = provider()
        .resolve_bool_value("enabled", &EvaluationContext::default())
        .await
        .expect_err("missing key should fail");
    assert_eq!(error.code, EvaluationErrorCode::TargetingKeyMissing);
}

#[tokio::test]
async fn observer_records_open_feature_context_and_conversion_errors() {
    let observer = RecordingObserver::default();
    let observations = Arc::clone(&observer.observations);
    let options = FbOptionsBuilder::new("valid-secret")
        .offline(true)
        .evaluation_observer(observer)
        .bootstrap_json(BOOTSTRAP)
        .build()
        .expect("offline options should build");
    let provider = FeatBitProvider::new(options);

    let missing = provider
        .resolve_bool_value("enabled", &EvaluationContext::default())
        .await
        .expect_err("missing targeting key should fail");
    assert_eq!(missing.code, EvaluationErrorCode::TargetingKeyMissing);

    let context = EvaluationContext::default().with_targeting_key("user-1");
    let mismatch = provider
        .resolve_bool_value("invalid-bool", &context)
        .await
        .expect_err("invalid boolean should fail conversion");
    assert_eq!(mismatch.code, EvaluationErrorCode::TypeMismatch);

    let observations = observations
        .lock()
        .expect("test observer lock should remain available");
    assert_eq!(observations.len(), 2);
    assert_eq!(
        observations[0].error_type(),
        Some(EvaluationObservationError::TargetingKeyMissing)
    );
    assert_eq!(observations[0].context_key(), None);
    assert_eq!(
        observations[1].error_type(),
        Some(EvaluationObservationError::TypeMismatch)
    );
    assert_eq!(observations[1].context_key(), Some("user-1"));
}

#[test]
fn provider_tracking_extensions_use_open_feature_context_validation() {
    let provider = provider();
    let missing_context = EvaluationContext::default();
    let evaluation_error = provider
        .track_eval_event_for_flag(&missing_context, "enabled")
        .expect_err("manual evaluation tracking requires a targeting key");
    assert_eq!(
        evaluation_error.code,
        EvaluationErrorCode::TargetingKeyMissing
    );
    let metric_error = provider
        .track_metric_event(&missing_context, "checkout-completed", 1.0)
        .expect_err("metric tracking requires a targeting key");
    assert_eq!(metric_error.code, EvaluationErrorCode::TargetingKeyMissing);

    let context = EvaluationContext::default().with_targeting_key("user-1");
    assert_eq!(
        provider.track_eval_event_for_flag(&context, "enabled"),
        Ok(false),
        "offline mode does not start FeatBit event delivery"
    );
    assert_eq!(
        provider.track_metric_event(&context, "checkout-completed", 1.0),
        Ok(false),
        "offline mode does not start FeatBit event delivery"
    );
}

#[tokio::test]
async fn resolves_all_open_feature_value_types() {
    let provider = provider_with_typed_values();
    let context = EvaluationContext::default().with_targeting_key("user-1");

    assert_eq!(
        provider
            .resolve_int_value("integer", &context)
            .await
            .expect("integer should resolve")
            .value,
        42
    );
    assert_eq!(
        provider
            .resolve_float_value("float", &context)
            .await
            .expect("float should resolve")
            .value
            .to_bits(),
        1.5_f64.to_bits()
    );
    assert_eq!(
        provider
            .resolve_string_value("text", &context)
            .await
            .expect("string should resolve")
            .value,
        "hello"
    );

    let object = provider
        .resolve_struct_value("object", &context)
        .await
        .expect("object should resolve")
        .value;
    assert!(matches!(
        object.fields.get("enabled"),
        Some(Value::Bool(true))
    ));
    assert!(matches!(object.fields.get("count"), Some(Value::Int(2))));
}

#[tokio::test]
async fn incompatible_value_returns_standard_type_error() {
    let context = EvaluationContext::default().with_targeting_key("user-1");
    let error = provider_with_typed_values()
        .resolve_bool_value("text", &context)
        .await
        .expect_err("a string flag cannot resolve as boolean");
    assert_eq!(error.code, EvaluationErrorCode::TypeMismatch);
}

#[tokio::test]
async fn closed_provider_reports_error_and_rejects_resolution() {
    let provider = provider();
    provider.client().close();
    assert_eq!(provider.status(), ProviderStatus::Error);

    let context = EvaluationContext::default().with_targeting_key("user-1");
    let error = provider
        .resolve_bool_value("enabled", &context)
        .await
        .expect_err("closed provider must reject resolution");
    assert_eq!(error.code, EvaluationErrorCode::ProviderNotReady);
}
