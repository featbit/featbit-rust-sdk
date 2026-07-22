use std::fmt;

use md5::{Digest, Md5};
use regex::Regex;

use crate::model::{
    Condition, FbUser, FeatureFlag, RolloutVariation, Segment, TargetRule, Variation,
};
use crate::prepared::{
    PreparedCondition, PreparedFlag, PreparedRule, PreparedSegment, PreparedSegmentIds,
    IS_IN_SEGMENT, IS_NOT_IN_SEGMENT,
};
use crate::store::DataSnapshot;

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

        let prepared = snapshot.prepared.flags.get(flag_key).map(AsRef::as_ref);
        Self::evaluate_flag(snapshot, flag, prepared, user)
    }

    fn evaluate_flag(
        snapshot: &DataSnapshot,
        flag: &FeatureFlag,
        prepared: Option<&PreparedFlag>,
        user: &FbUser,
    ) -> Result<EvalResult, EvalError> {
        if !flag.is_enabled {
            let variation = Self::variation(flag, prepared, &flag.disabled_variation_id)
                .ok_or(EvalError::MalformedFlag)?;
            return Ok(Self::result(flag, variation, EvalReason::Off, false));
        }

        let target_variation = prepared
            .and_then(|prepared| prepared.target_variation(user.key()))
            .or_else(|| {
                flag.target_users
                    .iter()
                    .find(|target| target.key_ids.iter().any(|key| key == user.key()))
                    .map(|target| target.variation_id.as_str())
            });
        if let Some(variation_id) = target_variation {
            let variation =
                Self::variation(flag, prepared, variation_id).ok_or(EvalError::MalformedFlag)?;
            return Ok(Self::result(
                flag,
                variation,
                EvalReason::TargetMatch,
                flag.expt_include_all_targets,
            ));
        }

        for (rule_index, rule) in flag.rules.iter().enumerate() {
            let prepared_rule = prepared.and_then(|prepared| prepared.rule(rule_index));
            if !Self::rule_matches_prepared(snapshot, rule, prepared_rule, user) {
                continue;
            }

            let dispatch_key = Self::dispatch_key(flag, rule.dispatch_key.as_deref(), user);
            let rollout = Self::select_rollout(&rule.variations, &dispatch_key)
                .ok_or(EvalError::MalformedFlag)?;
            let variation =
                Self::variation(flag, prepared, &rollout.id).ok_or(EvalError::MalformedFlag)?;
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
        let variation =
            Self::variation(flag, prepared, &rollout.id).ok_or(EvalError::MalformedFlag)?;
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

    fn variation<'a>(
        flag: &'a FeatureFlag,
        prepared: Option<&PreparedFlag>,
        id: &str,
    ) -> Option<&'a Variation> {
        prepared.map_or_else(
            || flag.variation(id),
            |prepared| prepared.variation(flag, id),
        )
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

    #[cfg(test)]
    fn rule_matches(snapshot: &DataSnapshot, rule: &TargetRule, user: &FbUser) -> bool {
        Self::rule_matches_prepared(snapshot, rule, None, user)
    }

    fn rule_matches_prepared(
        snapshot: &DataSnapshot,
        rule: &TargetRule,
        prepared: Option<&PreparedRule>,
        user: &FbUser,
    ) -> bool {
        rule.conditions
            .iter()
            .enumerate()
            .all(|(index, condition)| match condition.property.as_str() {
                IS_IN_SEGMENT => {
                    Self::matches_any_segment(
                        snapshot,
                        condition,
                        prepared.and_then(|prepared| prepared.condition(index)),
                        user,
                    ) == SegmentMatch::Matched
                }
                IS_NOT_IN_SEGMENT => {
                    Self::matches_any_segment(
                        snapshot,
                        condition,
                        prepared.and_then(|prepared| prepared.condition(index)),
                        user,
                    ) == SegmentMatch::NotMatched
                }
                _ => condition_matches_prepared(
                    condition,
                    prepared.and_then(|prepared| prepared.condition(index)),
                    user,
                ),
            })
    }

    fn matches_any_segment(
        snapshot: &DataSnapshot,
        condition: &Condition,
        prepared: Option<&PreparedCondition>,
        user: &FbUser,
    ) -> SegmentMatch {
        if let Some(PreparedCondition::SegmentIds(segment_ids)) = prepared {
            return match segment_ids {
                PreparedSegmentIds::Valid(segment_ids) => {
                    Self::match_segment_ids(snapshot, segment_ids, user)
                }
                PreparedSegmentIds::Invalid => SegmentMatch::Invalid,
            };
        }

        let Ok(segment_ids) = serde_json::from_str::<Option<Vec<String>>>(&condition.value) else {
            return SegmentMatch::Invalid;
        };
        // The .NET SDK treats JSON null and an empty array as a valid non-match. Invalid JSON and
        // unresolved objects are separated so a negating condition cannot turn bad data into a
        // targeting match.
        let Some(segment_ids) = segment_ids else {
            return SegmentMatch::NotMatched;
        };
        Self::match_segment_ids(snapshot, &segment_ids, user)
    }

    fn match_segment_ids(
        snapshot: &DataSnapshot,
        segment_ids: &[String],
        user: &FbUser,
    ) -> SegmentMatch {
        for segment_id in segment_ids {
            let Some(segment) = snapshot
                .segments
                .get(segment_id)
                .filter(|segment| !segment.is_archived)
            else {
                return SegmentMatch::Invalid;
            };
            let prepared = snapshot
                .prepared
                .segments
                .get(segment_id)
                .map(AsRef::as_ref);
            if segment_matches(segment, prepared, user) {
                return SegmentMatch::Matched;
            }
        }
        SegmentMatch::NotMatched
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SegmentMatch {
    Matched,
    NotMatched,
    Invalid,
}

fn condition_matches_prepared(
    condition: &Condition,
    prepared: Option<&PreparedCondition>,
    user: &FbUser,
) -> bool {
    let user_value = user.value_of(&condition.property);
    operator_matches_prepared(user_value, &condition.op, &condition.value, prepared)
}

#[cfg(test)]
fn operator_matches(user_value: &str, operator: &str, rule_value: &str) -> bool {
    operator_matches_prepared(user_value, operator, rule_value, None)
}

fn operator_matches_prepared(
    user_value: &str,
    operator: &str,
    rule_value: &str,
    prepared: Option<&PreparedCondition>,
) -> bool {
    match operator {
        "LessThan" => {
            numeric_compare_prepared(user_value, rule_value, prepared, |left, right| left < right)
        }
        "LessEqualThan" => {
            numeric_compare_prepared(user_value, rule_value, prepared, |left, right| {
                left <= right
            })
        }
        "BiggerThan" => {
            numeric_compare_prepared(user_value, rule_value, prepared, |left, right| left > right)
        }
        "BiggerEqualThan" => {
            numeric_compare_prepared(user_value, rule_value, prepared, |left, right| {
                left >= right
            })
        }
        "Equal" => user_value == rule_value,
        "NotEqual" => user_value != rule_value,
        "Contains" => user_value.contains(rule_value),
        "NotContain" => !user_value.contains(rule_value),
        "StartsWith" => user_value.starts_with(rule_value),
        "EndsWith" => user_value.ends_with(rule_value),
        "MatchRegex" => regex_matches(user_value, rule_value, prepared, false),
        "NotMatchRegex" => regex_matches(user_value, rule_value, prepared, true),
        "IsOneOf" => string_set_matches(user_value, rule_value, prepared, false),
        "NotOneOf" => string_set_matches(user_value, rule_value, prepared, true),
        "IsTrue" => user_value.eq_ignore_ascii_case("true"),
        "IsFalse" => user_value.eq_ignore_ascii_case("false"),
        _ => false,
    }
}

fn numeric_compare_prepared(
    user_value: &str,
    rule_value: &str,
    prepared: Option<&PreparedCondition>,
    compare: impl FnOnce(f64, f64) -> bool,
) -> bool {
    match prepared {
        Some(PreparedCondition::Numeric(Some(rule_number))) => user_value
            .parse::<f64>()
            .ok()
            .filter(|number| number.is_finite())
            .is_some_and(|user_number| compare(user_number, *rule_number)),
        Some(PreparedCondition::Numeric(None)) => false,
        _ => numeric_compare(user_value, rule_value, compare),
    }
}

fn regex_matches(
    user_value: &str,
    rule_value: &str,
    prepared: Option<&PreparedCondition>,
    negate: bool,
) -> bool {
    let matched = match prepared {
        Some(PreparedCondition::Regex(Some(regex))) => regex.is_match(user_value),
        Some(PreparedCondition::Regex(None)) => return false,
        _ => {
            let Ok(regex) = Regex::new(rule_value) else {
                return false;
            };
            regex.is_match(user_value)
        }
    };
    matched != negate
}

fn string_set_matches(
    user_value: &str,
    rule_value: &str,
    prepared: Option<&PreparedCondition>,
    negate: bool,
) -> bool {
    let contains = match prepared {
        Some(PreparedCondition::StringSet(Some(values))) => values.contains(user_value),
        Some(PreparedCondition::StringSet(None)) => return false,
        _ => {
            let Some(values) = string_list(rule_value) else {
                return false;
            };
            values.iter().any(|value| value == user_value)
        }
    };
    contains != negate
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

fn segment_matches(segment: &Segment, prepared: Option<&PreparedSegment>, user: &FbUser) -> bool {
    let excluded = prepared.map_or_else(
        || segment.excluded.iter().any(|key| key == user.key()),
        |prepared| prepared.excludes(user.key()),
    );
    if excluded {
        return false;
    }
    let included = prepared.map_or_else(
        || segment.included.iter().any(|key| key == user.key()),
        |prepared| prepared.includes(user.key()),
    );
    if included {
        return true;
    }
    segment.rules.iter().enumerate().any(|(rule_index, rule)| {
        let prepared_rule = prepared.and_then(|prepared| prepared.rule(rule_index));
        rule.conditions
            .iter()
            .enumerate()
            .all(|(condition_index, condition)| {
                condition_matches_prepared(
                    condition,
                    prepared_rule.and_then(|prepared| prepared.condition(condition_index)),
                    user,
                )
            })
    })
}

pub(crate) fn rollout_of_key(key: &str) -> f64 {
    let digest = Md5::digest(key.as_bytes());
    let Some(first_four) = digest
        .get(..4)
        .and_then(|bytes| <&[u8; 4]>::try_from(bytes).ok())
    else {
        return 0.0;
    };
    let signed = i32::from_le_bytes(*first_four);
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
    use std::thread;

    use super::*;
    use crate::model::{DataSet, Fallthrough, MatchRule, Segment, TargetUser};
    use crate::store::SnapshotStore;

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

    #[test]
    fn segment_conditions_match_dotnet_for_all_valid_shapes() {
        let user = FbUser::builder("user-1").custom("country", "cn").build();
        let segments = [
            Segment {
                id: "included".to_owned(),
                updated_at: 1,
                included: vec!["user-1".to_owned()],
                ..Segment::default()
            },
            Segment {
                id: "excluded-wins".to_owned(),
                updated_at: 1,
                included: vec!["user-1".to_owned()],
                excluded: vec!["user-1".to_owned()],
                ..Segment::default()
            },
            Segment {
                id: "rule".to_owned(),
                updated_at: 1,
                rules: vec![MatchRule {
                    conditions: vec![Condition {
                        property: "country".to_owned(),
                        op: "Equal".to_owned(),
                        value: "cn".to_owned(),
                    }],
                }],
                ..Segment::default()
            },
            Segment {
                id: "empty-rule".to_owned(),
                updated_at: 1,
                rules: vec![MatchRule::default()],
                ..Segment::default()
            },
            Segment {
                id: "other".to_owned(),
                updated_at: 1,
                included: vec!["another-user".to_owned()],
                ..Segment::default()
            },
            Segment {
                id: "archived".to_owned(),
                updated_at: 1,
                included: vec!["user-1".to_owned()],
                is_archived: true,
                ..Segment::default()
            },
        ];
        let snapshot = DataSnapshot {
            segments: segments
                .into_iter()
                .map(|segment| (segment.id.clone(), Arc::new(segment)))
                .collect(),
            populated: true,
            ..DataSnapshot::default()
        };

        let fixtures = [
            (IS_IN_SEGMENT, r#"["included"]"#, true),
            (IS_NOT_IN_SEGMENT, r#"["included"]"#, false),
            (IS_IN_SEGMENT, r#"["excluded-wins"]"#, false),
            (IS_NOT_IN_SEGMENT, r#"["excluded-wins"]"#, true),
            (IS_IN_SEGMENT, r#"["rule"]"#, true),
            (IS_IN_SEGMENT, r#"["empty-rule"]"#, true),
            (IS_IN_SEGMENT, r#"["other","included"]"#, true),
            (IS_NOT_IN_SEGMENT, r#"["other","included"]"#, false),
            (IS_IN_SEGMENT, "[]", false),
            (IS_NOT_IN_SEGMENT, "[]", true),
            (IS_IN_SEGMENT, "null", false),
            (IS_NOT_IN_SEGMENT, "null", true),
            (IS_IN_SEGMENT, r#"["included","missing"]"#, true),
            (IS_NOT_IN_SEGMENT, r#"["included","missing"]"#, false),
        ];
        for (property, value, expected) in fixtures {
            let rule = TargetRule {
                conditions: vec![Condition {
                    property: property.to_owned(),
                    op: String::new(),
                    value: value.to_owned(),
                }],
                ..TargetRule::default()
            };
            assert_eq!(
                Evaluator::rule_matches(&snapshot, &rule, &user),
                expected,
                "unexpected result for {property} {value}"
            );
        }
    }

    #[test]
    fn invalid_segment_references_never_match_positive_or_negative_conditions() {
        let user = FbUser::builder("user-1").build();
        let snapshot = DataSnapshot {
            segments: [
                (
                    "archived".to_owned(),
                    Arc::new(Segment {
                        id: "archived".to_owned(),
                        updated_at: 1,
                        included: vec!["user-1".to_owned()],
                        is_archived: true,
                        ..Segment::default()
                    }),
                ),
                (
                    "included".to_owned(),
                    Arc::new(Segment {
                        id: "included".to_owned(),
                        updated_at: 1,
                        included: vec!["user-1".to_owned()],
                        ..Segment::default()
                    }),
                ),
            ]
            .into(),
            populated: true,
            ..DataSnapshot::default()
        };

        for property in [IS_IN_SEGMENT, IS_NOT_IN_SEGMENT] {
            for value in ["not-json", r#"["missing"]"#, r#"["archived"]"#] {
                let rule = TargetRule {
                    conditions: vec![Condition {
                        property: property.to_owned(),
                        op: String::new(),
                        value: value.to_owned(),
                    }],
                    ..TargetRule::default()
                };
                assert!(
                    !Evaluator::rule_matches(&snapshot, &rule, &user),
                    "invalid {property} {value} must be a non-match"
                );
            }
        }

        let missing_before_match = TargetRule {
            conditions: vec![Condition {
                property: IS_IN_SEGMENT.to_owned(),
                op: String::new(),
                value: r#"["missing","included"]"#.to_owned(),
            }],
            ..TargetRule::default()
        };
        assert!(!Evaluator::rule_matches(
            &snapshot,
            &missing_before_match,
            &user
        ));
    }

    #[test]
    fn preprocessed_snapshot_preserves_rule_and_segment_results() {
        let user = FbUser::builder("user-1")
            .custom("email", "ada@example.com")
            .custom("tier", "pro")
            .custom("age", "42")
            .build();
        let segment = Segment {
            id: "members".to_owned(),
            updated_at: 1,
            included: vec!["user-1".to_owned()],
            ..Segment::default()
        };
        let mut flag = basic_flag("prepared-rule");
        flag.rules.push(TargetRule {
            name: "prepared".to_owned(),
            conditions: vec![
                Condition {
                    property: "email".to_owned(),
                    op: "MatchRegex".to_owned(),
                    value: ".+@example\\.com".to_owned(),
                },
                Condition {
                    property: "tier".to_owned(),
                    op: "IsOneOf".to_owned(),
                    value: r#"["pro","enterprise"]"#.to_owned(),
                },
                Condition {
                    property: "age".to_owned(),
                    op: "BiggerEqualThan".to_owned(),
                    value: "18".to_owned(),
                },
                Condition {
                    property: IS_IN_SEGMENT.to_owned(),
                    op: String::new(),
                    value: r#"["members"]"#.to_owned(),
                },
            ],
            variations: vec![rollout("false")],
            ..TargetRule::default()
        });

        let raw = DataSnapshot {
            flags: [(flag.key.clone(), Arc::new(flag.clone()))].into(),
            segments: [(segment.id.clone(), Arc::new(segment.clone()))].into(),
            populated: true,
            ..DataSnapshot::default()
        };
        let store = SnapshotStore::new();
        store.populate(&DataSet {
            event_type: "full".to_owned(),
            feature_flags: vec![flag],
            segments: vec![segment],
        });
        let prepared = store.load();

        let raw_result = Evaluator::evaluate(&raw, "prepared-rule", &user)
            .expect("raw snapshot should evaluate");
        let prepared_result = Evaluator::evaluate(&prepared, "prepared-rule", &user)
            .expect("prepared snapshot should evaluate");
        assert_eq!(prepared_result.variation, raw_result.variation);
        assert_eq!(prepared_result.reason, raw_result.reason);
        assert_eq!(
            prepared_result.send_to_experiment,
            raw_result.send_to_experiment
        );
        assert_eq!(prepared_result.variation.value, "false");
    }

    #[test]
    fn concurrent_full_updates_never_mix_flags_and_segments() {
        fn data_set(suffix: &str, version: i64) -> DataSet {
            let segment_id = format!("segment-{suffix}");
            let mut flag = basic_flag("consistent");
            flag.updated_at = version;
            flag.rules.push(TargetRule {
                name: format!("rule-{suffix}"),
                conditions: vec![Condition {
                    property: IS_IN_SEGMENT.to_owned(),
                    op: String::new(),
                    value: serde_json::to_string(&[&segment_id])
                        .expect("segment IDs should serialize"),
                }],
                variations: vec![rollout("false")],
                ..TargetRule::default()
            });
            DataSet {
                event_type: "full".to_owned(),
                feature_flags: vec![flag],
                segments: vec![Segment {
                    id: segment_id,
                    updated_at: version,
                    included: vec!["user-1".to_owned()],
                    ..Segment::default()
                }],
            }
        }

        let first = data_set("a", 1);
        let second = data_set("b", 2);
        let store = Arc::new(SnapshotStore::new());
        store.populate(&first);
        let writer_store = Arc::clone(&store);
        let writer = thread::spawn(move || {
            for index in 0..2_000 {
                if index % 2 == 0 {
                    writer_store.populate(&first);
                } else {
                    writer_store.populate(&second);
                }
            }
        });
        let readers = (0..4)
            .map(|_| {
                let store = Arc::clone(&store);
                thread::spawn(move || {
                    let user = FbUser::builder("user-1").build();
                    for _ in 0..2_000 {
                        let snapshot = store.load();
                        let result = Evaluator::evaluate(&snapshot, "consistent", &user)
                            .expect("each complete snapshot should evaluate");
                        assert_eq!(result.variation.value, "false");
                        assert!(matches!(result.reason, EvalReason::RuleMatch { .. }));
                    }
                })
            })
            .collect::<Vec<_>>();

        writer.join().expect("snapshot writer should finish");
        for reader in readers {
            reader.join().expect("snapshot reader should finish");
        }
    }
}
