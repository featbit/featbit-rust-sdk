use std::sync::Arc;

use super::rule_matches_prepared;
use crate::model::{Condition, FbUser, MatchRule, Segment, TargetRule};
use crate::prepared::{IS_IN_SEGMENT, IS_NOT_IN_SEGMENT};
use crate::store::{test_snapshot_map, DataSnapshot};

fn rule_matches(snapshot: &DataSnapshot, rule: &TargetRule, user: &FbUser) -> bool {
    rule_matches_prepared(snapshot, rule, None, user)
}

#[test]
fn segment_exclusion_precedes_inclusion_and_rules() {
    let user = FbUser::builder("user-1").custom("country", "cn").build();
    let segment = Segment {
        id: "segment-1".to_owned(),
        updated_at: 1,
        included: vec!["user-1".to_owned()],
        excluded: vec!["user-1".to_owned()],
        rules: vec![MatchRule {
            conditions: vec![Condition {
                property: "country".to_owned(),
                op: "Equal".to_owned(),
                value: "cn".to_owned(),
            }],
        }],
        is_archived: false,
    };
    let condition = Condition {
        property: IS_IN_SEGMENT.to_owned(),
        op: String::new(),
        value: "[\"segment-1\"]".to_owned(),
    };
    let snapshot = DataSnapshot {
        segments: test_snapshot_map([(segment.id.clone(), Arc::new(segment))]),
        populated: true,
        ..DataSnapshot::default()
    };

    assert!(!rule_matches(
        &snapshot,
        &TargetRule {
            conditions: vec![condition],
            ..TargetRule::default()
        },
        &user
    ));
}

#[test]
fn direct_segment_rule_requires_every_condition() {
    let segment = Segment {
        id: "segment-rule".to_owned(),
        updated_at: 1,
        rules: vec![MatchRule {
            conditions: vec![
                Condition {
                    property: "age".to_owned(),
                    op: "Equal".to_owned(),
                    value: "10".to_owned(),
                },
                Condition {
                    property: "country".to_owned(),
                    op: "Equal".to_owned(),
                    value: "us".to_owned(),
                },
            ],
        }],
        ..Segment::default()
    };
    let snapshot = DataSnapshot {
        segments: test_snapshot_map([(segment.id.clone(), Arc::new(segment))]),
        populated: true,
        ..DataSnapshot::default()
    };
    let condition = |property: &str| TargetRule {
        conditions: vec![Condition {
            property: property.to_owned(),
            op: String::new(),
            value: "[\"segment-rule\"]".to_owned(),
        }],
        ..TargetRule::default()
    };

    let matching = FbUser::builder("u3")
        .custom("age", "10")
        .custom("country", "us")
        .build();
    let partial = FbUser::builder("u4")
        .custom("age", "10")
        .custom("country", "eu")
        .build();
    assert!(rule_matches(
        &snapshot,
        &condition(IS_IN_SEGMENT),
        &matching
    ));
    assert!(!rule_matches(
        &snapshot,
        &condition(IS_IN_SEGMENT),
        &partial
    ));
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
        segments: test_snapshot_map(
            segments
                .into_iter()
                .map(|segment| (segment.id.clone(), Arc::new(segment))),
        ),
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
            rule_matches(&snapshot, &rule, &user),
            expected,
            "unexpected result for {property} {value}"
        );
    }
}

#[test]
fn invalid_segment_references_never_match_positive_or_negative_conditions() {
    let user = FbUser::builder("user-1").build();
    let snapshot = DataSnapshot {
        segments: test_snapshot_map([
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
        ]),
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
                !rule_matches(&snapshot, &rule, &user),
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
    assert!(!rule_matches(&snapshot, &missing_before_match, &user));
}
