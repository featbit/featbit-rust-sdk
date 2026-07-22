use std::collections::HashMap;
use std::sync::Arc;
use std::thread;

use super::Evaluator;
use crate::evaluation::test_support::{basic_flag, rollout};
use crate::evaluation::{EvalError, EvalReason};
use crate::model::{Condition, DataSet, Fallthrough, FbUser, Segment, TargetRule, TargetUser};
use crate::prepared::IS_IN_SEGMENT;
use crate::store::{DataSnapshot, SnapshotStore};

#[test]
fn missing_archived_invalid_context_and_malformed_flags_return_typed_errors() {
    let user = FbUser::builder("u1").build();
    let mut archived = basic_flag("archived");
    archived.is_archived = true;
    let mut malformed_off = basic_flag("malformed-off");
    malformed_off.is_enabled = false;
    malformed_off.disabled_variation_id = "missing".to_owned();
    let mut malformed_fallthrough = basic_flag("malformed-fallthrough");
    malformed_fallthrough.fallthrough.variations.clear();
    let snapshot = DataSnapshot {
        flags: [archived, malformed_off, malformed_fallthrough]
            .into_iter()
            .map(|flag| (flag.key.clone(), Arc::new(flag)))
            .collect(),
        populated: true,
        ..DataSnapshot::default()
    };

    assert_eq!(
        Evaluator::evaluate(&snapshot, "missing", &user).unwrap_err(),
        EvalError::FlagNotFound
    );
    assert_eq!(
        Evaluator::evaluate(&snapshot, "archived", &user).unwrap_err(),
        EvalError::FlagNotFound
    );
    assert_eq!(
        Evaluator::evaluate(&snapshot, "malformed-off", &user).unwrap_err(),
        EvalError::MalformedFlag
    );
    assert_eq!(
        Evaluator::evaluate(&snapshot, "malformed-fallthrough", &user).unwrap_err(),
        EvalError::MalformedFlag
    );
    assert_eq!(
        Evaluator::evaluate(&snapshot, "malformed-off", &FbUser::builder("").build()).unwrap_err(),
        EvalError::InvalidContext
    );
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

    let rule_result = Evaluator::evaluate(&snapshot, "rule", &user).expect("rule should resolve");
    assert_eq!(rule_result.variation.value, "false");
    assert!(matches!(rule_result.reason, EvalReason::RuleMatch { .. }));
}

#[test]
fn rules_are_and_conditions_and_first_match_wins() {
    let user = FbUser::builder("u1")
        .custom("tier", "pro")
        .custom("country", "cn")
        .build();
    let mut flag = basic_flag("and-rule");
    flag.rules = vec![
        TargetRule {
            name: "partial".to_owned(),
            conditions: vec![
                Condition {
                    property: "tier".to_owned(),
                    op: "Equal".to_owned(),
                    value: "pro".to_owned(),
                },
                Condition {
                    property: "country".to_owned(),
                    op: "Equal".to_owned(),
                    value: "us".to_owned(),
                },
            ],
            variations: vec![rollout("false")],
            ..TargetRule::default()
        },
        TargetRule {
            name: "complete".to_owned(),
            conditions: vec![Condition {
                property: "country".to_owned(),
                op: "Equal".to_owned(),
                value: "cn".to_owned(),
            }],
            variations: vec![rollout("false")],
            ..TargetRule::default()
        },
    ];
    let snapshot = DataSnapshot {
        flags: [(flag.key.clone(), Arc::new(flag))].into(),
        populated: true,
        ..DataSnapshot::default()
    };

    let result = Evaluator::evaluate(&snapshot, "and-rule", &user).expect("rule should match");
    assert_eq!(result.variation.value, "false");
    assert!(matches!(
        result.reason,
        EvalReason::RuleMatch { ref name, .. } if name == "complete"
    ));
}

#[test]
fn experiment_delivery_matches_dotnet_target_rule_and_fallthrough_semantics() {
    let user = FbUser::builder("u1").custom("tier", "pro").build();

    let mut targeted = basic_flag("targeted");
    targeted.expt_include_all_targets = true;
    targeted.target_users.push(TargetUser {
        key_ids: vec!["u1".to_owned()],
        variation_id: "true".to_owned(),
    });

    let mut rule = basic_flag("rule-experiment");
    rule.rules.push(TargetRule {
        name: "not included".to_owned(),
        included_in_expt: false,
        conditions: vec![Condition {
            property: "tier".to_owned(),
            op: "Equal".to_owned(),
            value: "pro".to_owned(),
        }],
        variations: vec![crate::model::RolloutVariation {
            id: "true".to_owned(),
            rollout: vec![0.0, 1.0],
            expt_rollout: 1.0,
        }],
        ..TargetRule::default()
    });

    let mut fallthrough = basic_flag("fallthrough-experiment");
    fallthrough.fallthrough = Fallthrough {
        included_in_expt: true,
        variations: vec![crate::model::RolloutVariation {
            id: "true".to_owned(),
            rollout: vec![0.0, 1.0],
            expt_rollout: 1.0,
        }],
        ..Fallthrough::default()
    };

    let snapshot = DataSnapshot {
        flags: [targeted, rule, fallthrough]
            .into_iter()
            .map(|flag| (flag.key.clone(), Arc::new(flag)))
            .collect(),
        populated: true,
        ..DataSnapshot::default()
    };

    assert!(
        Evaluator::evaluate(&snapshot, "targeted", &user)
            .expect("target should resolve")
            .send_to_experiment
    );
    assert!(
        !Evaluator::evaluate(&snapshot, "rule-experiment", &user)
            .expect("rule should resolve")
            .send_to_experiment
    );
    assert!(
        Evaluator::evaluate(&snapshot, "fallthrough-experiment", &user)
            .expect("fallthrough should resolve")
            .send_to_experiment
    );
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

    let raw_result =
        Evaluator::evaluate(&raw, "prepared-rule", &user).expect("raw snapshot should evaluate");
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
                value: serde_json::to_string(&[&segment_id]).expect("segment IDs should serialize"),
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
