use std::collections::BTreeMap;

use chrono::DateTime;
use serde::{de, Deserialize, Deserializer};

/// A user/subject used for `FeatBit` targeting and event attribution.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct FbUser {
    key: String,
    name: String,
    custom: BTreeMap<String, String>,
}

impl FbUser {
    /// Starts building a user with a stable targeting key.
    #[must_use]
    pub fn builder(key: impl Into<String>) -> FbUserBuilder {
        FbUserBuilder::new(key)
    }

    /// Returns the stable targeting key.
    #[must_use]
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Returns the optional display name, or an empty string when unset.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns custom string attributes in stable key order.
    #[must_use]
    pub fn custom(&self) -> &BTreeMap<String, String> {
        &self.custom
    }

    pub(crate) fn value_of(&self, property: &str) -> &str {
        match property {
            "keyId" => &self.key,
            "name" => &self.name,
            _ => self.custom.get(property).map_or("", String::as_str),
        }
    }
}

/// Builder for an immutable [`FbUser`].
#[derive(Clone, Debug, Default)]
pub struct FbUserBuilder {
    key: String,
    name: String,
    custom: BTreeMap<String, String>,
}

impl FbUserBuilder {
    /// Creates a user builder with the required targeting key.
    #[must_use]
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            ..Self::default()
        }
    }

    /// Sets the display name.
    #[must_use]
    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.name = name.into();
        self
    }

    /// Adds or replaces a custom targeting attribute.
    #[must_use]
    pub fn custom(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.custom.insert(key.into(), value.into());
        self
    }

    /// Builds the user. Empty keys are retained and safely rejected at evaluation time.
    #[must_use]
    pub fn build(self) -> FbUser {
        FbUser {
            key: self.key,
            name: self.name,
            custom: self.custom,
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DataSyncEnvelope {
    #[serde(default)]
    pub(crate) message_type: String,
    pub(crate) data: DataSet,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DataSet {
    #[serde(default)]
    pub(crate) event_type: String,
    #[serde(default)]
    pub(crate) feature_flags: Vec<FeatureFlag>,
    #[serde(default)]
    pub(crate) segments: Vec<Segment>,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct FeatureFlag {
    #[serde(default)]
    pub(crate) id: String,
    #[serde(default)]
    pub(crate) key: String,
    #[serde(default, deserialize_with = "deserialize_version")]
    pub(crate) updated_at: i64,
    #[serde(default)]
    pub(crate) variation_type: String,
    #[serde(default)]
    pub(crate) variations: Vec<Variation>,
    #[serde(default)]
    pub(crate) target_users: Vec<TargetUser>,
    #[serde(default)]
    pub(crate) rules: Vec<TargetRule>,
    #[serde(default)]
    pub(crate) is_enabled: bool,
    #[serde(default)]
    pub(crate) disabled_variation_id: String,
    #[serde(default)]
    pub(crate) fallthrough: Fallthrough,
    #[serde(default)]
    pub(crate) expt_include_all_targets: bool,
    #[serde(default)]
    pub(crate) is_archived: bool,
}

impl FeatureFlag {
    pub(crate) fn variation(&self, id: &str) -> Option<&Variation> {
        self.variations.iter().find(|variation| variation.id == id)
    }
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Variation {
    #[serde(default)]
    pub(crate) id: String,
    #[serde(default)]
    pub(crate) value: String,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TargetUser {
    #[serde(default)]
    pub(crate) key_ids: Vec<String>,
    #[serde(default)]
    pub(crate) variation_id: String,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TargetRule {
    #[serde(default)]
    pub(crate) name: String,
    #[serde(default)]
    pub(crate) dispatch_key: Option<String>,
    #[serde(default)]
    pub(crate) included_in_expt: bool,
    #[serde(default)]
    pub(crate) conditions: Vec<Condition>,
    #[serde(default)]
    pub(crate) variations: Vec<RolloutVariation>,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Fallthrough {
    #[serde(default)]
    pub(crate) dispatch_key: Option<String>,
    #[serde(default)]
    pub(crate) included_in_expt: bool,
    #[serde(default)]
    pub(crate) variations: Vec<RolloutVariation>,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RolloutVariation {
    #[serde(default)]
    pub(crate) id: String,
    #[serde(default)]
    pub(crate) rollout: Vec<f64>,
    #[serde(default)]
    pub(crate) expt_rollout: f64,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Condition {
    #[serde(default)]
    pub(crate) property: String,
    #[serde(default)]
    pub(crate) op: String,
    #[serde(default)]
    pub(crate) value: String,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Segment {
    #[serde(default)]
    pub(crate) id: String,
    #[serde(default, deserialize_with = "deserialize_version")]
    pub(crate) updated_at: i64,
    #[serde(default)]
    pub(crate) included: Vec<String>,
    #[serde(default)]
    pub(crate) excluded: Vec<String>,
    #[serde(default)]
    pub(crate) rules: Vec<MatchRule>,
    #[serde(default)]
    pub(crate) is_archived: bool,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct MatchRule {
    #[serde(default)]
    pub(crate) conditions: Vec<Condition>,
}

fn deserialize_version<'de, D>(deserializer: D) -> Result<i64, D::Error>
where
    D: Deserializer<'de>,
{
    let value = serde_json::Value::deserialize(deserializer)?;
    match value {
        serde_json::Value::String(text) => DateTime::parse_from_rfc3339(&text)
            .map(|timestamp| timestamp.timestamp_millis())
            .map_err(de::Error::custom),
        serde_json::Value::Number(number) => number
            .as_i64()
            .ok_or_else(|| de::Error::custom("updatedAt must fit in a signed 64-bit integer")),
        _ => Err(de::Error::custom(
            "updatedAt must be an RFC 3339 string or integer",
        )),
    }
}

#[cfg(test)]
mod tests;
