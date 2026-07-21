use open_feature::provider::{
    FeatureProvider, ProviderMetadata, ProviderStatus, ResolutionDetails,
};
use open_feature::{
    async_trait, EvaluationContext, EvaluationContextFieldValue, EvaluationError,
    EvaluationErrorCode, EvaluationReason, EvaluationResult, FlagMetadata, StructValue, Value,
};

use crate::client::{ClientEvaluationError, ClientStatus, FbClient};
use crate::evaluation::{EvalReason, EvalResult};
use crate::model::FbUser;
use crate::options::FbOptions;

/// `FeatBit`'s direct implementation of the official `OpenFeature` Rust provider interface.
///
/// The provider owns a cloneable [`FbClient`] handle. Register it with
/// `OpenFeature::set_provider`; use [`Self::client`] for FeatBit-specific tracking, flush, and
/// explicit shutdown operations.
#[derive(Clone, Debug)]
pub struct FeatBitProvider {
    client: FbClient,
    metadata: ProviderMetadata,
}

impl FeatBitProvider {
    /// Starts a provider from validated `FeatBit` options.
    #[must_use]
    pub fn new(options: FbOptions) -> Self {
        Self::from_client(FbClient::with_options(options))
    }

    /// Wraps an existing client, sharing its lifecycle and local data snapshot.
    #[must_use]
    pub fn from_client(client: FbClient) -> Self {
        Self {
            client,
            metadata: ProviderMetadata::new("FeatBit Rust Server SDK"),
        }
    }

    /// Returns the underlying `FeatBit` client for tracking, flush, status, and close operations.
    #[must_use]
    pub const fn client(&self) -> &FbClient {
        &self.client
    }

    fn resolve<T>(
        &self,
        flag_key: &str,
        context: &EvaluationContext,
        convert: impl FnOnce(&str) -> Result<T, EvaluationErrorCode>,
    ) -> EvaluationResult<ResolutionDetails<T>> {
        let user = user_from_context(context)?;
        let evaluated = self
            .client
            .evaluate_raw(flag_key, &user)
            .map_err(map_client_error)?;
        let value = convert(&evaluated.variation.value).map_err(|code| {
            evaluation_error(code, format!("flag {flag_key:?} has an incompatible value"))
        })?;
        Ok(resolution_details(evaluated, value))
    }
}

#[async_trait]
impl FeatureProvider for FeatBitProvider {
    fn metadata(&self) -> &ProviderMetadata {
        &self.metadata
    }

    fn status(&self) -> ProviderStatus {
        match self.client.status() {
            ClientStatus::NotReady => ProviderStatus::NotReady,
            ClientStatus::Ready => ProviderStatus::Ready,
            ClientStatus::Stale => ProviderStatus::STALE,
            ClientStatus::Closed => ProviderStatus::Error,
        }
    }

    async fn resolve_bool_value(
        &self,
        flag_key: &str,
        evaluation_context: &EvaluationContext,
    ) -> EvaluationResult<ResolutionDetails<bool>> {
        self.resolve(flag_key, evaluation_context, |value| {
            if value.eq_ignore_ascii_case("true") {
                Ok(true)
            } else if value.eq_ignore_ascii_case("false") {
                Ok(false)
            } else {
                Err(EvaluationErrorCode::TypeMismatch)
            }
        })
    }

    async fn resolve_int_value(
        &self,
        flag_key: &str,
        evaluation_context: &EvaluationContext,
    ) -> EvaluationResult<ResolutionDetails<i64>> {
        self.resolve(flag_key, evaluation_context, |value| {
            value
                .parse::<i64>()
                .map_err(|_| EvaluationErrorCode::TypeMismatch)
        })
    }

    async fn resolve_float_value(
        &self,
        flag_key: &str,
        evaluation_context: &EvaluationContext,
    ) -> EvaluationResult<ResolutionDetails<f64>> {
        self.resolve(flag_key, evaluation_context, |value| {
            value
                .parse::<f64>()
                .ok()
                .filter(|number| number.is_finite())
                .ok_or(EvaluationErrorCode::TypeMismatch)
        })
    }

    async fn resolve_string_value(
        &self,
        flag_key: &str,
        evaluation_context: &EvaluationContext,
    ) -> EvaluationResult<ResolutionDetails<String>> {
        self.resolve(flag_key, evaluation_context, |value| Ok(value.to_owned()))
    }

    async fn resolve_struct_value(
        &self,
        flag_key: &str,
        evaluation_context: &EvaluationContext,
    ) -> EvaluationResult<ResolutionDetails<StructValue>> {
        self.resolve(flag_key, evaluation_context, |value| {
            let json = serde_json::from_str::<serde_json::Value>(value)
                .map_err(|_| EvaluationErrorCode::ParseError)?;
            json_to_struct(&json).ok_or(EvaluationErrorCode::TypeMismatch)
        })
    }
}

fn user_from_context(context: &EvaluationContext) -> EvaluationResult<FbUser> {
    let key = context
        .targeting_key
        .as_deref()
        .filter(|key| !key.is_empty())
        .ok_or_else(|| {
            evaluation_error(
                EvaluationErrorCode::TargetingKeyMissing,
                "FeatBit evaluation requires a targeting key",
            )
        })?;

    let mut name = String::new();
    let mut custom = Vec::with_capacity(context.custom_fields.len());
    for (field, value) in &context.custom_fields {
        let Some(text) = context_value_to_string(value) else {
            log::debug!("ignoring unsupported structured OpenFeature context field {field:?}");
            continue;
        };
        if field == "name" {
            name = text;
        } else {
            custom.push((field.clone(), text));
        }
    }
    custom.sort_unstable_by(|left, right| left.0.cmp(&right.0));

    let mut builder = FbUser::builder(key).name(name);
    for (field, value) in custom {
        builder = builder.custom(field, value);
    }
    Ok(builder.build())
}

fn context_value_to_string(value: &EvaluationContextFieldValue) -> Option<String> {
    match value {
        EvaluationContextFieldValue::Bool(value) => Some(value.to_string()),
        EvaluationContextFieldValue::Int(value) => Some(value.to_string()),
        EvaluationContextFieldValue::Float(value) if value.is_finite() => Some(value.to_string()),
        EvaluationContextFieldValue::Float(_) => None,
        EvaluationContextFieldValue::String(value) => Some(value.clone()),
        EvaluationContextFieldValue::DateTime(value) => value
            .format(&time::format_description::well_known::Rfc3339)
            .ok(),
        EvaluationContextFieldValue::Struct(value) => value
            .as_ref()
            .downcast_ref::<serde_json::Value>()
            .and_then(|json| serde_json::to_string(json).ok())
            .or_else(|| {
                value
                    .as_ref()
                    .downcast_ref::<StructValue>()
                    .and_then(open_feature_struct_to_json)
                    .and_then(|json| serde_json::to_string(&json).ok())
            }),
    }
}

fn resolution_details<T>(evaluated: EvalResult, value: T) -> ResolutionDetails<T> {
    let reason = match &evaluated.reason {
        EvalReason::Off => EvaluationReason::Disabled,
        EvalReason::TargetMatch | EvalReason::RuleMatch { split: false, .. } => {
            EvaluationReason::TargetingMatch
        }
        EvalReason::RuleMatch { split: true, .. } | EvalReason::Fallthrough { split: true } => {
            EvaluationReason::Split
        }
        EvalReason::Fallthrough { split: false } => EvaluationReason::Default,
    };
    let metadata = FlagMetadata::default()
        .with_value("flagId", evaluated.flag_id)
        .with_value("variationType", evaluated.flag_type);
    ResolutionDetails {
        value,
        variant: Some(evaluated.variation.id),
        reason: Some(reason),
        flag_metadata: Some(metadata),
    }
}

fn map_client_error(error: ClientEvaluationError) -> EvaluationError {
    match error {
        ClientEvaluationError::NotReady => evaluation_error(
            EvaluationErrorCode::ProviderNotReady,
            "FeatBit provider is not ready",
        ),
        ClientEvaluationError::InvalidContext => evaluation_error(
            EvaluationErrorCode::TargetingKeyMissing,
            "FeatBit evaluation requires a targeting key",
        ),
        ClientEvaluationError::FlagNotFound => evaluation_error(
            EvaluationErrorCode::FlagNotFound,
            "FeatBit flag was not found",
        ),
        ClientEvaluationError::MalformedFlag => evaluation_error(
            EvaluationErrorCode::ParseError,
            "FeatBit flag data is malformed",
        ),
    }
}

fn evaluation_error(code: EvaluationErrorCode, message: impl Into<String>) -> EvaluationError {
    EvaluationError {
        code,
        message: Some(message.into()),
    }
}

fn json_to_struct(value: &serde_json::Value) -> Option<StructValue> {
    let serde_json::Value::Object(fields) = value else {
        return None;
    };
    let mut result = StructValue::default();
    for (key, value) in fields {
        result
            .fields
            .insert(key.clone(), json_to_open_feature(value)?);
    }
    Some(result)
}

fn json_to_open_feature(value: &serde_json::Value) -> Option<Value> {
    match value {
        serde_json::Value::Null => None,
        serde_json::Value::Bool(value) => Some(Value::Bool(*value)),
        serde_json::Value::Number(value) => value.as_i64().map(Value::Int).or_else(|| {
            value
                .as_f64()
                .filter(|number| number.is_finite())
                .map(Value::Float)
        }),
        serde_json::Value::String(value) => Some(Value::String(value.clone())),
        serde_json::Value::Array(values) => values
            .iter()
            .map(json_to_open_feature)
            .collect::<Option<Vec<_>>>()
            .map(Value::Array),
        serde_json::Value::Object(_) => json_to_struct(value).map(Value::Struct),
    }
}

fn open_feature_struct_to_json(value: &StructValue) -> Option<serde_json::Value> {
    let mut object = serde_json::Map::with_capacity(value.fields.len());
    for (key, value) in &value.fields {
        object.insert(key.clone(), open_feature_value_to_json(value)?);
    }
    Some(serde_json::Value::Object(object))
}

fn open_feature_value_to_json(value: &Value) -> Option<serde_json::Value> {
    match value {
        Value::Bool(value) => Some((*value).into()),
        Value::Int(value) => Some((*value).into()),
        Value::Float(value) if value.is_finite() => {
            serde_json::Number::from_f64(*value).map(serde_json::Value::Number)
        }
        Value::Float(_) => None,
        Value::String(value) => Some(value.clone().into()),
        Value::Array(values) => values
            .iter()
            .map(open_feature_value_to_json)
            .collect::<Option<Vec<_>>>()
            .map(serde_json::Value::Array),
        Value::Struct(value) => open_feature_struct_to_json(value),
    }
}

#[cfg(test)]
mod tests {
    use open_feature::provider::FeatureProvider;

    use super::*;
    use crate::options::FbOptionsBuilder;

    const BOOTSTRAP: &str = r#"{
        "messageType":"data-sync",
        "data":{"eventType":"full","featureFlags":[{
            "id":"flag-id","key":"enabled","updatedAt":1,"variationType":"boolean",
            "variations":[{"id":"on","value":"true"},{"id":"off","value":"false"}],
            "targetUsers":[],"rules":[],"isEnabled":true,"disabledVariationId":"off",
            "fallthrough":{"includedInExpt":false,"variations":[{"id":"on","rollout":[0,1],"exptRollout":0}]}
        }],"segments":[]}
    }"#;

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
}
