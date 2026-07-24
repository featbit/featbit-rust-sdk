use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use regex::Regex;

use crate::model::{Condition, FeatureFlag, Segment, Variation};

pub(crate) const IS_IN_SEGMENT: &str = "User is in segment";
pub(crate) const IS_NOT_IN_SEGMENT: &str = "User is not in segment";

#[derive(Clone, Debug, Default)]
pub(crate) struct PreparedSnapshot {
    pub(crate) flags: Arc<HashMap<Arc<str>, Arc<PreparedFlag>>>,
    pub(crate) segments: Arc<HashMap<Arc<str>, Arc<PreparedSegment>>>,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct PreparedFlag {
    variation_indices: HashMap<String, usize>,
    target_variations: HashMap<String, String>,
    rules: Arc<[PreparedRule]>,
}

impl PreparedFlag {
    pub(crate) fn new(flag: &FeatureFlag) -> Self {
        let mut variation_indices = HashMap::with_capacity(flag.variations.len());
        for (index, variation) in flag.variations.iter().enumerate() {
            variation_indices
                .entry(variation.id.clone())
                .or_insert(index);
        }

        let target_count = flag
            .target_users
            .iter()
            .map(|target| target.key_ids.len())
            .fold(0_usize, usize::saturating_add);
        let mut target_variations = HashMap::with_capacity(target_count);
        for target in &flag.target_users {
            for key in &target.key_ids {
                target_variations
                    .entry(key.clone())
                    .or_insert_with(|| target.variation_id.clone());
            }
        }

        Self {
            variation_indices,
            target_variations,
            rules: flag.rules.iter().map(PreparedRule::new).collect(),
        }
    }

    pub(crate) fn variation<'a>(&self, flag: &'a FeatureFlag, id: &str) -> Option<&'a Variation> {
        self.variation_indices
            .get(id)
            .and_then(|index| flag.variations.get(*index))
            .filter(|variation| variation.id == id)
            .or_else(|| flag.variation(id))
    }

    pub(crate) fn target_variation(&self, user_key: &str) -> Option<&str> {
        self.target_variations.get(user_key).map(String::as_str)
    }

    pub(crate) fn rule(&self, index: usize) -> Option<&PreparedRule> {
        self.rules.get(index)
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct PreparedSegment {
    included: HashSet<String>,
    excluded: HashSet<String>,
    rules: Arc<[PreparedRule]>,
}

impl PreparedSegment {
    pub(crate) fn new(segment: &Segment) -> Self {
        Self {
            included: segment.included.iter().cloned().collect(),
            excluded: segment.excluded.iter().cloned().collect(),
            rules: segment.rules.iter().map(PreparedRule::new).collect(),
        }
    }

    pub(crate) fn includes(&self, user_key: &str) -> bool {
        self.included.contains(user_key)
    }

    pub(crate) fn excludes(&self, user_key: &str) -> bool {
        self.excluded.contains(user_key)
    }

    pub(crate) fn rule(&self, index: usize) -> Option<&PreparedRule> {
        self.rules.get(index)
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct PreparedRule {
    conditions: Arc<[PreparedCondition]>,
}

impl PreparedRule {
    fn new(rule: &impl RuleConditions) -> Self {
        Self {
            conditions: rule
                .conditions()
                .iter()
                .map(PreparedCondition::new)
                .collect(),
        }
    }

    pub(crate) fn condition(&self, index: usize) -> Option<&PreparedCondition> {
        self.conditions.get(index)
    }
}

trait RuleConditions {
    fn conditions(&self) -> &[Condition];
}

impl RuleConditions for crate::model::TargetRule {
    fn conditions(&self) -> &[Condition] {
        &self.conditions
    }
}

impl RuleConditions for crate::model::MatchRule {
    fn conditions(&self) -> &[Condition] {
        &self.conditions
    }
}

#[derive(Clone, Debug)]
pub(crate) enum PreparedCondition {
    None,
    Numeric(Option<f64>),
    Regex(Option<Regex>),
    StringSet(Option<Arc<HashSet<String>>>),
    SegmentIds(PreparedSegmentIds),
}

impl PreparedCondition {
    fn new(condition: &Condition) -> Self {
        if matches!(
            condition.property.as_str(),
            IS_IN_SEGMENT | IS_NOT_IN_SEGMENT
        ) {
            let ids = match serde_json::from_str::<Option<Vec<String>>>(&condition.value) {
                Ok(Some(ids)) => PreparedSegmentIds::Valid(ids.into()),
                Ok(None) => PreparedSegmentIds::Valid(Arc::from([])),
                Err(_) => PreparedSegmentIds::Invalid,
            };
            return Self::SegmentIds(ids);
        }

        match condition.op.as_str() {
            "LessThan" | "LessEqualThan" | "BiggerThan" | "BiggerEqualThan" => Self::Numeric(
                condition
                    .value
                    .parse::<f64>()
                    .ok()
                    .filter(|number| number.is_finite()),
            ),
            "MatchRegex" | "NotMatchRegex" => Self::Regex(Regex::new(&condition.value).ok()),
            "IsOneOf" | "NotOneOf" => Self::StringSet(
                serde_json::from_str::<Vec<String>>(&condition.value)
                    .ok()
                    .map(|values| Arc::new(values.into_iter().collect())),
            ),
            _ => Self::None,
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) enum PreparedSegmentIds {
    Valid(Arc<[String]>),
    Invalid,
}
