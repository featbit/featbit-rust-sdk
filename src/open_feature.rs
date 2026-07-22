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
use crate::observation::EvaluationObservationError;
use crate::options::FbOptions;

/// `FeatBit`'s direct implementation of the official `OpenFeature` Rust provider interface.
///
/// The provider owns a cloneable [`FbClient`] handle. Register it with
/// `OpenFeature::set_provider`; use the provider tracking extensions with an
/// [`EvaluationContext`], and [`Self::client`] for flush, status, and explicit shutdown operations.
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

    /// Re-evaluates `flag_key` for an `OpenFeature` context and records the evaluation event.
    ///
    /// `OpenFeature` 0.3 does not define a tracking API, so this provider-specific extension lets
    /// applications keep flag resolution and event attribution on the same
    /// [`EvaluationContext`]. It returns `Ok(false)` when `FeatBit` event delivery is unavailable or
    /// the bounded queue is full. Call it promptly after the corresponding `OpenFeature` resolution;
    /// an intervening flag update can change the variation selected by the re-evaluation.
    ///
    /// # Errors
    ///
    /// Returns the standard `OpenFeature` targeting-key error when `evaluation_context` has no
    /// non-empty targeting key.
    pub fn track_eval_event_for_flag(
        &self,
        evaluation_context: &EvaluationContext,
        flag_key: &str,
    ) -> EvaluationResult<bool> {
        let user = user_from_context(evaluation_context)?;
        Ok(self.client.track_eval_event_for_flag(&user, flag_key))
    }

    /// Records a `FeatBit` custom metric for an `OpenFeature` context without blocking on network
    /// I/O.
    ///
    /// `OpenFeature` 0.3 does not standardize custom metric events. This provider-specific extension
    /// uses the same context mapping as flag resolution and returns `Ok(false)` when `FeatBit` event
    /// delivery is unavailable, the event is invalid, or the bounded queue is full.
    ///
    /// # Errors
    ///
    /// Returns the standard `OpenFeature` targeting-key error when `evaluation_context` has no
    /// non-empty targeting key.
    pub fn track_metric_event(
        &self,
        evaluation_context: &EvaluationContext,
        event_name: &str,
        numeric_value: f64,
    ) -> EvaluationResult<bool> {
        let user = user_from_context(evaluation_context)?;
        Ok(self
            .client
            .track_metric_event(&user, event_name, numeric_value))
    }

    fn resolve<T>(
        &self,
        flag_key: &str,
        context: &EvaluationContext,
        convert: impl FnOnce(&str) -> Result<T, EvaluationErrorCode>,
    ) -> EvaluationResult<ResolutionDetails<T>> {
        let user = match user_from_context(context) {
            Ok(user) => user,
            Err(error) => {
                self.client.observe_error(
                    flag_key,
                    None,
                    EvaluationObservationError::TargetingKeyMissing,
                );
                return Err(error);
            }
        };
        let (evaluated, event) = self
            .client
            .evaluate_raw(flag_key, &user)
            .map_err(map_client_error)?;
        let value = match convert(&evaluated.variation.value) {
            Ok(value) => value,
            Err(code) => {
                let observation_error = if code == EvaluationErrorCode::TypeMismatch {
                    EvaluationObservationError::TypeMismatch
                } else {
                    EvaluationObservationError::ParseError
                };
                self.client
                    .observe_error(flag_key, Some(user.key()), observation_error);
                return Err(evaluation_error(
                    code,
                    format!("flag {flag_key:?} has an incompatible value"),
                ));
            }
        };
        self.client.complete_evaluation(&user, &evaluated, &event);
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
mod tests;
