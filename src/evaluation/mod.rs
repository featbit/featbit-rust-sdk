use std::fmt;

use md5::{Digest, Md5};
use regex::Regex;

use crate::model::{
    Condition, FbUser, FeatureFlag, RolloutVariation, Segment, TargetRule, Variation,
};
use crate::store::DataSnapshot;

const IS_IN_SEGMENT: &str = "User is in segment";
const IS_NOT_IN_SEGMENT: &str = "User is not in segment";

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum EvalError {
    InvalidContext,
    FlagNotFound,
    MalformedFlag,
}

impl fmt::Display for EvalError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self {
            Self::InvalidContext => "targeting key is missing",
            Self::FlagNotFound => "flag not found",
            Self::MalformedFlag => "malformed flag",
        };
        formatter.write_str(message)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum EvalReason {
    Off,
    TargetMatch,
    RuleMatch { name: String, split: bool },
    Fallthrough { split: bool },
}

#[derive(Clone, Debug)]
pub(crate) struct EvalResult {
    pub(crate) flag_id: String,
    pub(crate) flag_type: String,
    pub(crate) variation: Variation,
    pub(crate) reason: EvalReason,
    pub(crate) send_to_experiment: bool,
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct Evaluator;

impl Evaluator {
    pub(crate) fn evaluate(
        snapshot: &DataSnapshot,
        flag_key: &str,
        user: &FbUser,
    ) -> Result<EvalResult, EvalError> {
        if user.key().is_empty() {
            return Err(EvalError::InvalidContext);
        }

        let flag = snapshot
            .flags
            .get(flag_key)
            .filter(|flag| !flag.is_archived)
            .ok_or(EvalError::FlagNotFound)?;

        Self::evaluate_flag(snapshot, flag, user)
    }

    fn evaluate_flag(
        snapshot: &DataSnapshot,
        flag: &FeatureFlag,
        user: &FbUser,
    ) -> Result<EvalResult, EvalError> {
        if !flag.is_enabled {
            let variation = flag
                .variation(&flag.disabled_variation_id)
                .ok_or(EvalError::MalformedFlag)?;
            return Ok(Self::result(flag, variation, EvalReason::Off, false));
        }

        if let Some(target) = flag
            .target_users
            .iter()
            .find(|target| target.key_ids.iter().any(|key| key == user.key()))
        {
            let variation = flag
                .variation(&target.variation_id)
                .ok_or(EvalError::MalformedFlag)?;
            return Ok(Self::result(
                flag,
                variation,
                EvalReason::TargetMatch,
                flag.expt_include_all_targets,
            ));
        }

        for rule in &flag.rules {
            if !Self::rule_matches(snapshot, rule, user) {
                continue;
            }

            let dispatch_key = Self::dispatch_key(flag, rule.dispatch_key.as_deref(), user);
            let rollout = Self::select_rollout(&rule.variations, &dispatch_key)
                .ok_or(EvalError::MalformedFlag)?;
            let variation = flag
                .variation(&rollout.id)
                .ok_or(EvalError::MalformedFlag)?;
            let send_to_experiment = Self::should_send_to_experiment(
                flag.expt_include_all_targets,
                rule.included_in_expt,
                &dispatch_key,
                rollout,
            );
            return Ok(Self::result(
                flag,
                variation,
                EvalReason::RuleMatch {
                    name: rule.name.clone(),
                    split: is_percentage_split(&rule.variations),
                },
                send_to_experiment,
            ));
        }

        let dispatch_key = Self::dispatch_key(flag, flag.fallthrough.dispatch_key.as_deref(), user);
        let rollout = Self::select_rollout(&flag.fallthrough.variations, &dispatch_key)
            .ok_or(EvalError::MalformedFlag)?;
        let variation = flag
            .variation(&rollout.id)
            .ok_or(EvalError::MalformedFlag)?;
        let send_to_experiment = Self::should_send_to_experiment(
            flag.expt_include_all_targets,
            flag.fallthrough.included_in_expt,
            &dispatch_key,
            rollout,
        );
        Ok(Self::result(
            flag,
            variation,
            EvalReason::Fallthrough {
                split: is_percentage_split(&flag.fallthrough.variations),
            },
            send_to_experiment,
        ))
    }

    fn result(
        flag: &FeatureFlag,
        variation: &Variation,
        reason: EvalReason,
        send_to_experiment: bool,
    ) -> EvalResult {
        EvalResult {
            flag_id: flag.id.clone(),
            flag_type: flag.variation_type.clone(),
            variation: variation.clone(),
            reason,
            send_to_experiment,
        }
    }

    fn dispatch_key(flag: &FeatureFlag, property: Option<&str>, user: &FbUser) -> String {
        let value = property
            .filter(|property| !property.trim().is_empty())
            .map_or_else(|| user.key(), |property| user.value_of(property));
        format!("{}{value}", flag.key)
    }

    fn select_rollout<'a>(
        rollouts: &'a [RolloutVariation],
        dispatch_key: &str,
    ) -> Option<&'a RolloutVariation> {
        rollouts
            .iter()
            .find(|rollout| is_in_rollout(dispatch_key, &rollout.rollout))
    }

    fn rule_matches(snapshot: &DataSnapshot, rule: &TargetRule, user: &FbUser) -> bool {
        rule.conditions
            .iter()
            .all(|condition| match condition.property.as_str() {
                IS_IN_SEGMENT => Self::matches_any_segment(snapshot, condition, user),
                IS_NOT_IN_SEGMENT => !Self::matches_any_segment(snapshot, condition, user),
                _ => condition_matches(condition, user),
            })
    }

    fn matches_any_segment(snapshot: &DataSnapshot, condition: &Condition, user: &FbUser) -> bool {
        let Ok(segment_ids) = serde_json::from_str::<Vec<String>>(&condition.value) else {
            return false;
        };

        segment_ids.iter().any(|segment_id| {
            snapshot
                .segments
                .get(segment_id)
                .filter(|segment| !segment.is_archived)
                .is_some_and(|segment| segment_matches(segment, user))
        })
    }

    fn should_send_to_experiment(
        include_all_targets: bool,
        rule_in_experiment: bool,
        dispatch_key: &str,
        rollout: &RolloutVariation,
    ) -> bool {
        if include_all_targets {
            return true;
        }
        if !rule_in_experiment {
            return false;
        }

        let [lower, upper] = rollout.rollout.as_slice() else {
            return false;
        };
        let dispatch_rollout = upper - lower;
        if rollout.expt_rollout == 0.0
            || dispatch_rollout == 0.0
            || !rollout.expt_rollout.is_finite()
            || !dispatch_rollout.is_finite()
        {
            return false;
        }

        let experiment_upper = (rollout.expt_rollout / dispatch_rollout).min(1.0);
        if experiment_upper <= 0.0 {
            return false;
        }
        is_in_rollout(&format!("expt{dispatch_key}"), &[0.0, experiment_upper])
    }
}

pub(crate) fn condition_matches(condition: &Condition, user: &FbUser) -> bool {
    let user_value = user.value_of(&condition.property);
    operator_matches(user_value, &condition.op, &condition.value)
}

fn operator_matches(user_value: &str, operator: &str, rule_value: &str) -> bool {
    match operator {
        "LessThan" => numeric_compare(user_value, rule_value, |left, right| left < right),
        "LessEqualThan" => numeric_compare(user_value, rule_value, |left, right| left <= right),
        "BiggerThan" => numeric_compare(user_value, rule_value, |left, right| left > right),
        "BiggerEqualThan" => numeric_compare(user_value, rule_value, |left, right| left >= right),
        "Equal" => user_value == rule_value,
        "NotEqual" => user_value != rule_value,
        "Contains" => user_value.contains(rule_value),
        "NotContain" => !user_value.contains(rule_value),
        "StartsWith" => user_value.starts_with(rule_value),
        "EndsWith" => user_value.ends_with(rule_value),
        "MatchRegex" => Regex::new(rule_value).is_ok_and(|regex| regex.is_match(user_value)),
        "NotMatchRegex" => Regex::new(rule_value).is_ok_and(|regex| !regex.is_match(user_value)),
        "IsOneOf" => {
            string_list(rule_value).is_some_and(|values| values.iter().any(|v| v == user_value))
        }
        "NotOneOf" => {
            string_list(rule_value).is_some_and(|values| values.iter().all(|v| v != user_value))
        }
        "IsTrue" => user_value.eq_ignore_ascii_case("true"),
        "IsFalse" => user_value.eq_ignore_ascii_case("false"),
        _ => false,
    }
}

fn numeric_compare(
    user_value: &str,
    rule_value: &str,
    compare: impl FnOnce(f64, f64) -> bool,
) -> bool {
    let (Ok(user_number), Ok(rule_number)) = (user_value.parse::<f64>(), rule_value.parse::<f64>())
    else {
        return false;
    };
    user_number.is_finite() && rule_number.is_finite() && compare(user_number, rule_number)
}

fn string_list(value: &str) -> Option<Vec<String>> {
    serde_json::from_str(value).ok()
}

fn segment_matches(segment: &Segment, user: &FbUser) -> bool {
    if segment.excluded.iter().any(|key| key == user.key()) {
        return false;
    }
    if segment.included.iter().any(|key| key == user.key()) {
        return true;
    }
    segment.rules.iter().any(|rule| {
        rule.conditions
            .iter()
            .all(|condition| condition_matches(condition, user))
    })
}

pub(crate) fn rollout_of_key(key: &str) -> f64 {
    let digest = Md5::digest(key.as_bytes());
    let first_four = [digest[0], digest[1], digest[2], digest[3]];
    let signed = i32::from_le_bytes(first_four);
    ((f64::from(signed)) / f64::from(i32::MIN)).abs()
}

pub(crate) fn is_in_rollout(key: &str, rollout: &[f64]) -> bool {
    let [min, max] = rollout else {
        return false;
    };
    if !min.is_finite() || !max.is_finite() || min > max {
        return false;
    }
    if *min == 0.0 && 1.0 - max < 1e-5 {
        return true;
    }
    if *min == 0.0 && *max == 0.0 {
        return false;
    }
    let value = rollout_of_key(key);
    value >= *min && value <= *max
}

fn is_percentage_split(variations: &[RolloutVariation]) -> bool {
    if variations.len() > 1 {
        return true;
    }
    variations.first().is_some_and(|variation| {
        !matches!(variation.rollout.as_slice(), [min, max] if *min == 0.0 && 1.0 - max < 1e-5)
    })
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use super::*;
    use crate::model::{Fallthrough, Segment, TargetUser};

    fn variation(id: &str, value: &str) -> Variation {
        Variation {
            id: id.to_owned(),
            value: value.to_owned(),
        }
    }

    fn rollout(id: &str) -> RolloutVariation {
        RolloutVariation {
            id: id.to_owned(),
            rollout: vec![0.0, 1.0],
            expt_rollout: 0.0,
        }
    }

    fn basic_flag(key: &str) -> FeatureFlag {
        FeatureFlag {
            id: format!("{key}-id"),
            key: key.to_owned(),
            updated_at: 1,
            variation_type: "boolean".to_owned(),
            variations: vec![variation("true", "true"), variation("false", "false")],
            target_users: Vec::new(),
            rules: Vec::new(),
            is_enabled: true,
            disabled_variation_id: "false".to_owned(),
            fallthrough: Fallthrough {
                variations: vec![rollout("true")],
                ..Fallthrough::default()
            },
            expt_include_all_targets: false,
            is_archived: false,
        }
    }

    #[test]
    fn rollout_vectors_match_dotnet() {
        let fixtures: [(&str, f64); 3] = [
            ("test-value", 0.146_536_292_042_583_23),
            ("qKPKh1S3FolC", 0.910_591_969_266_533_9),
            (
                "3eacb184-2d79-49df-9ea7-edd4f10e4c6f",
                0.089_944_031_555_205_58,
            ),
        ];
        for (key, expected) in fixtures {
            assert_eq!(rollout_of_key(key).to_bits(), expected.to_bits());
        }
    }

    #[test]
    fn operators_match_featbit_names() {
        let fixtures = [
            ("10", "BiggerThan", "9", true),
            ("10", "LessEqualThan", "10", true),
            ("v1", "Equal", "v1", true),
            ("svip", "Contains", "vip", true),
            ("abc", "StartsWith", "ab", true),
            ("colour", "MatchRegex", "colou?r", true),
            ("a", "IsOneOf", "[\"a\",\"b\"]", true),
            ("TRue", "IsTrue", "", true),
            ("a", "unknown", "a", false),
        ];
        for (user, operator, rule, expected) in fixtures {
            assert_eq!(operator_matches(user, operator, rule), expected);
        }
    }

    #[test]
    fn malformed_negating_operators_are_non_matches() {
        assert!(!operator_matches("a", "NotMatchRegex", "[",));
        assert!(!operator_matches("a", "NotOneOf", "not-json"));
    }

    #[test]
    fn evaluation_order_is_off_target_rule_then_fallthrough() {
        let user = FbUser::builder("user-1").custom("tier", "pro").build();
        let mut off = basic_flag("off");
        off.is_enabled = false;
        off.target_users.push(TargetUser {
            key_ids: vec!["user-1".to_owned()],
            variation_id: "true".to_owned(),
        });

        let mut target = basic_flag("target");
        target.target_users.push(TargetUser {
            key_ids: vec!["user-1".to_owned()],
            variation_id: "false".to_owned(),
        });

        let mut rule = basic_flag("rule");
        rule.rules.push(TargetRule {
            name: "pro users".to_owned(),
            conditions: vec![Condition {
                property: "tier".to_owned(),
                op: "Equal".to_owned(),
                value: "pro".to_owned(),
            }],
            variations: vec![rollout("false")],
            ..TargetRule::default()
        });

        let snapshot = DataSnapshot {
            flags: [off, target, rule]
                .into_iter()
                .map(|flag| (flag.key.clone(), Arc::new(flag)))
                .collect::<HashMap<_, _>>(),
            populated: true,
            ..DataSnapshot::default()
        };

        let off_result = Evaluator::evaluate(&snapshot, "off", &user).expect("off should resolve");
        assert_eq!(off_result.variation.value, "false");
        assert_eq!(off_result.reason, EvalReason::Off);

        let target_result =
            Evaluator::evaluate(&snapshot, "target", &user).expect("target should resolve");
        assert_eq!(target_result.variation.value, "false");
        assert_eq!(target_result.reason, EvalReason::TargetMatch);

        let rule_result =
            Evaluator::evaluate(&snapshot, "rule", &user).expect("rule should resolve");
        assert_eq!(rule_result.variation.value, "false");
        assert!(matches!(rule_result.reason, EvalReason::RuleMatch { .. }));
    }

    #[test]
    fn segment_exclusion_precedes_inclusion_and_rules() {
        let user = FbUser::builder("user-1").custom("country", "cn").build();
        let segment = Segment {
            id: "segment-1".to_owned(),
            updated_at: 1,
            included: vec!["user-1".to_owned()],
            excluded: vec!["user-1".to_owned()],
            rules: Vec::new(),
            is_archived: false,
        };
        let mut flag = basic_flag("segment-flag");
        flag.rules.push(TargetRule {
            name: "segment rule".to_owned(),
            conditions: vec![Condition {
                property: IS_IN_SEGMENT.to_owned(),
                op: String::new(),
                value: "[\"segment-1\"]".to_owned(),
            }],
            variations: vec![rollout("false")],
            ..TargetRule::default()
        });
        let snapshot = DataSnapshot {
            flags: [(flag.key.clone(), Arc::new(flag))].into(),
            segments: [(segment.id.clone(), Arc::new(segment))].into(),
            populated: true,
            ..DataSnapshot::default()
        };

        let result = Evaluator::evaluate(&snapshot, "segment-flag", &user)
            .expect("fallthrough should resolve");
        assert_eq!(result.variation.value, "true");
        assert!(matches!(result.reason, EvalReason::Fallthrough { .. }));
    }
}
