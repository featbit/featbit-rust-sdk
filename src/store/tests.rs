use std::sync::{Arc, Barrier};
use std::thread;

use super::*;
use crate::evaluation::test_support::{basic_flag, rollout};
use crate::evaluation::{EvalReason, Evaluator};
use crate::model::FbUser;
use crate::model::{Condition, TargetRule, Variation};
use crate::prepared::{PreparedCondition, IS_IN_SEGMENT};

fn flag(key: &str, updated_at: i64, is_archived: bool) -> FeatureFlag {
    FeatureFlag {
        key: key.to_owned(),
        updated_at,
        is_archived,
        ..FeatureFlag::default()
    }
}

#[test]
fn full_replace_and_patches_preserve_tombstone_versions() {
    let store = SnapshotStore::new();
    store.populate(&DataSet {
        event_type: "full".to_owned(),
        feature_flags: vec![flag("a", 1, false), flag("b", 1, false)],
        ..DataSet::default()
    });
    assert_eq!(store.load().flags.len(), 2);

    assert_eq!(
        store.patch(&DataSet {
            event_type: "patch".to_owned(),
            feature_flags: vec![flag("a", 2, true)],
            ..DataSet::default()
        }),
        PatchResult::Changed
    );
    assert_eq!(
        store.patch(&DataSet {
            event_type: "patch".to_owned(),
            feature_flags: vec![flag("a", 1, false)],
            ..DataSet::default()
        }),
        PatchResult::Unchanged
    );
    assert!(store.load().flags["a"].is_archived);
    assert_eq!(store.version(), 2);

    store.populate(&DataSet {
        event_type: "full".to_owned(),
        feature_flags: vec![flag("b", 3, false)],
        ..DataSet::default()
    });
    let snapshot = store.load();
    assert!(!snapshot.flags.contains_key("a"));
    assert_eq!(snapshot.flags.len(), 1);
    assert_eq!(snapshot.version, 3);
}

#[test]
fn concurrent_patches_cannot_overwrite_each_others_changes() {
    const WRITERS: usize = 16;
    let store = Arc::new(SnapshotStore::new());
    let barrier = Arc::new(Barrier::new(WRITERS + 1));
    let writers = (0..WRITERS)
        .map(|index| {
            let store = Arc::clone(&store);
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                let data = DataSet {
                    event_type: "patch".to_owned(),
                    feature_flags: vec![flag(
                        &format!("flag-{index}"),
                        i64::try_from(index).expect("test index fits i64") + 1,
                        false,
                    )],
                    ..DataSet::default()
                };
                barrier.wait();
                assert_eq!(store.patch(&data), PatchResult::Changed);
            })
        })
        .collect::<Vec<_>>();

    barrier.wait();
    for writer in writers {
        writer.join().expect("patch writer should finish");
    }
    let snapshot = store.load();
    assert_eq!(snapshot.flags.len(), WRITERS);
    for index in 0..WRITERS {
        let key = format!("flag-{index}");
        assert!(snapshot.flags.contains_key(key.as_str()));
    }
}

#[test]
fn publication_prepares_conditions_and_reuses_unchanged_indexes() {
    let store = SnapshotStore::new();
    let mut prepared_flag = flag("prepared", 1, false);
    prepared_flag.rules.push(TargetRule {
        conditions: vec![Condition {
            property: "email".to_owned(),
            op: "MatchRegex".to_owned(),
            value: ".+@example\\.com".to_owned(),
        }],
        ..TargetRule::default()
    });
    store.populate(&DataSet {
        event_type: "full".to_owned(),
        feature_flags: vec![prepared_flag],
        ..DataSet::default()
    });

    let initial = store.load();
    let prepared = Arc::clone(&initial.prepared.flags["prepared"]);
    assert!(matches!(
        prepared.rule(0).and_then(|rule| rule.condition(0)),
        Some(PreparedCondition::Regex(Some(_)))
    ));
    drop(initial);

    assert_eq!(
        store.patch(&DataSet {
            event_type: "patch".to_owned(),
            feature_flags: vec![flag("other", 2, false)],
            ..DataSet::default()
        }),
        PatchResult::Changed
    );
    let patched = store.load();
    assert!(Arc::ptr_eq(&prepared, &patched.prepared.flags["prepared"]));
}

const LARGE_STORE_OBJECTS: usize = 2_048;

fn large_store() -> SnapshotStore {
    let large_json = format!(r#"{{"payload":"{}"}}"#, "x".repeat(512 * 1024));
    let mut feature_flags = (0..LARGE_STORE_OBJECTS)
        .map(|index| {
            flag(
                &format!("flag-with-a-deliberately-long-key-{index}"),
                1,
                false,
            )
        })
        .collect::<Vec<_>>();
    feature_flags[0].variations.push(Variation {
        id: "large-json".to_owned(),
        value: large_json,
    });
    let segments = (0..LARGE_STORE_OBJECTS)
        .map(|index| Segment {
            id: format!("segment-with-a-deliberately-long-id-{index}"),
            updated_at: 1,
            ..Segment::default()
        })
        .collect::<Vec<_>>();
    let store = SnapshotStore::new();
    store.populate(&DataSet {
        event_type: "full".to_owned(),
        feature_flags,
        segments,
    });
    store
}

#[test]
fn flag_patch_copies_only_flag_maps_and_shares_large_values_and_keys() {
    let store = large_store();
    let initial = store.load();
    let (initial_flag_key, initial_large_flag) = initial
        .flags
        .get_key_value("flag-with-a-deliberately-long-key-0")
        .expect("large flag should exist");
    let (initial_prepared_key, initial_prepared_flag) = initial
        .prepared
        .flags
        .get_key_value("flag-with-a-deliberately-long-key-0")
        .expect("prepared large flag should exist");
    assert!(Arc::ptr_eq(initial_flag_key, initial_prepared_key));

    let changed_flag_key = format!(
        "flag-with-a-deliberately-long-key-{}",
        LARGE_STORE_OBJECTS - 1
    );
    assert_eq!(
        store.patch(&DataSet {
            event_type: "patch".to_owned(),
            feature_flags: vec![flag(&changed_flag_key, 2, false)],
            ..DataSet::default()
        }),
        PatchResult::Changed
    );
    let after_flag_patch = store.load();
    assert!(!Arc::ptr_eq(&initial.flags, &after_flag_patch.flags));
    assert!(!Arc::ptr_eq(
        &initial.prepared.flags,
        &after_flag_patch.prepared.flags
    ));
    assert!(Arc::ptr_eq(&initial.segments, &after_flag_patch.segments));
    assert!(Arc::ptr_eq(
        &initial.prepared.segments,
        &after_flag_patch.prepared.segments
    ));
    assert!(Arc::ptr_eq(
        initial_large_flag,
        &after_flag_patch.flags["flag-with-a-deliberately-long-key-0"]
    ));
    assert!(Arc::ptr_eq(
        initial_prepared_flag,
        &after_flag_patch.prepared.flags["flag-with-a-deliberately-long-key-0"]
    ));
    let (patched_flag_key, _) = after_flag_patch
        .flags
        .get_key_value("flag-with-a-deliberately-long-key-0")
        .expect("large flag should remain");
    assert!(Arc::ptr_eq(initial_flag_key, patched_flag_key));

    assert_eq!(
        store.patch(&DataSet {
            event_type: "patch".to_owned(),
            feature_flags: vec![flag(&changed_flag_key, 2, false)],
            ..DataSet::default()
        }),
        PatchResult::Unchanged
    );
    assert!(Arc::ptr_eq(&after_flag_patch, &store.load()));
}

#[test]
fn segment_patch_copies_only_segment_maps() {
    let store = large_store();
    let initial = store.load();
    let changed_segment_id = format!(
        "segment-with-a-deliberately-long-id-{}",
        LARGE_STORE_OBJECTS - 1
    );
    assert_eq!(
        store.patch(&DataSet {
            event_type: "patch".to_owned(),
            segments: vec![Segment {
                id: changed_segment_id,
                updated_at: 2,
                ..Segment::default()
            }],
            ..DataSet::default()
        }),
        PatchResult::Changed
    );
    let after_segment_patch = store.load();
    assert!(Arc::ptr_eq(&initial.flags, &after_segment_patch.flags));
    assert!(Arc::ptr_eq(
        &initial.prepared.flags,
        &after_segment_patch.prepared.flags
    ));
    assert!(!Arc::ptr_eq(
        &initial.segments,
        &after_segment_patch.segments
    ));
    assert!(!Arc::ptr_eq(
        &initial.prepared.segments,
        &after_segment_patch.prepared.segments
    ));
}

#[test]
fn first_empty_patch_publishes_without_copying_empty_maps() {
    let store = SnapshotStore::new();
    let empty = store.load();

    assert_eq!(
        store.patch(&DataSet {
            event_type: "patch".to_owned(),
            ..DataSet::default()
        }),
        PatchResult::Unchanged
    );
    let populated = store.load();
    assert!(populated.populated);
    assert!(Arc::ptr_eq(&empty.flags, &populated.flags));
    assert!(Arc::ptr_eq(&empty.segments, &populated.segments));
    assert!(Arc::ptr_eq(
        &empty.prepared.flags,
        &populated.prepared.flags
    ));
    assert!(Arc::ptr_eq(
        &empty.prepared.segments,
        &populated.prepared.segments
    ));
}

#[test]
fn equal_version_with_different_content_reports_a_conflict() {
    let store = SnapshotStore::new();
    store.populate(&DataSet {
        event_type: "full".to_owned(),
        feature_flags: vec![flag("same-version", 7, false)],
        ..DataSet::default()
    });
    let initial = store.load();

    assert_eq!(
        store.patch(&DataSet {
            event_type: "patch".to_owned(),
            feature_flags: vec![flag("same-version", 7, true)],
            ..DataSet::default()
        }),
        PatchResult::VersionConflict
    );
    let after_conflict = store.load();
    assert!(Arc::ptr_eq(&initial, &after_conflict));
    assert!(!after_conflict.flags["same-version"].is_archived);

    assert_eq!(
        store.patch(&DataSet {
            event_type: "patch".to_owned(),
            feature_flags: vec![flag("same-version", 7, false)],
            ..DataSet::default()
        }),
        PatchResult::Unchanged
    );
}

#[test]
fn empty_full_snapshot_is_populated_at_version_zero() {
    let store = SnapshotStore::new();
    assert!(!store.load().populated);

    store.populate(&DataSet {
        event_type: "full".to_owned(),
        ..DataSet::default()
    });

    let snapshot = store.load();
    assert!(snapshot.populated);
    assert!(snapshot.flags.is_empty());
    assert!(snapshot.segments.is_empty());
    assert_eq!(snapshot.version, 0);
    assert_eq!(store.version(), 0);
}

#[test]
fn full_bootstrap_populates_the_pinned_dotnet_flag_and_segment_fixtures() {
    let feature_flag: FeatureFlag =
        serde_json::from_str(include_str!("../model/fixtures/dotnet-one-flag.json"))
            .expect("pinned .NET feature flag should deserialize");
    let segment: Segment =
        serde_json::from_str(include_str!("../model/fixtures/dotnet-one-segment.json"))
            .expect("pinned .NET segment should deserialize");
    let expected_version = feature_flag.updated_at.max(segment.updated_at);
    let store = SnapshotStore::new();

    store.populate(&DataSet {
        event_type: "full".to_owned(),
        feature_flags: vec![feature_flag],
        segments: vec![segment],
    });

    let snapshot = store.load();
    assert!(snapshot.populated);
    assert_eq!(snapshot.flags.len(), 1);
    assert_eq!(snapshot.segments.len(), 1);
    assert_eq!(snapshot.flags["example"].key, "example");
    assert!(snapshot.segments["0779d76b-afc6-4886-ab65-af8c004273ad"]
        .included
        .contains(&"true-1".to_owned()));
    assert_eq!(snapshot.version, expected_version);
}

#[test]
fn segment_patches_preserve_newer_tombstones_and_report_conflicts() {
    let store = SnapshotStore::new();
    store.populate(&DataSet {
        event_type: "full".to_owned(),
        segments: vec![Segment {
            id: "segment".to_owned(),
            updated_at: 2,
            included: vec!["old-user".to_owned()],
            ..Segment::default()
        }],
        ..DataSet::default()
    });

    assert_eq!(
        store.patch(&DataSet {
            event_type: "patch".to_owned(),
            segments: vec![Segment {
                id: "segment".to_owned(),
                updated_at: 3,
                is_archived: true,
                ..Segment::default()
            }],
            ..DataSet::default()
        }),
        PatchResult::Changed
    );
    assert_eq!(store.version(), 3);
    assert!(store.load().segments["segment"].is_archived);

    assert_eq!(
        store.patch(&DataSet {
            event_type: "patch".to_owned(),
            segments: vec![Segment {
                id: "segment".to_owned(),
                updated_at: 2,
                included: vec!["resurrected".to_owned()],
                ..Segment::default()
            }],
            ..DataSet::default()
        }),
        PatchResult::Unchanged
    );
    assert!(store.load().segments["segment"].is_archived);

    assert_eq!(
        store.patch(&DataSet {
            event_type: "patch".to_owned(),
            segments: vec![Segment {
                id: "segment".to_owned(),
                updated_at: 3,
                is_archived: false,
                ..Segment::default()
            }],
            ..DataSet::default()
        }),
        PatchResult::VersionConflict
    );
    assert!(store.load().segments["segment"].is_archived);
}

#[test]
fn concurrent_evaluation_never_observes_a_partial_flag_and_segment_patch() {
    fn versioned_data(version: i64) -> DataSet {
        let segment_id = format!("segment-{version}");
        let mut flag = basic_flag("patch-consistent");
        flag.updated_at = version;
        flag.rules = vec![TargetRule {
            name: format!("rule-{version}"),
            conditions: vec![Condition {
                property: IS_IN_SEGMENT.to_owned(),
                op: String::new(),
                value: serde_json::to_string(&[&segment_id]).expect("segment ID should serialize"),
            }],
            variations: vec![rollout("false")],
            ..TargetRule::default()
        }];
        DataSet {
            event_type: "patch".to_owned(),
            feature_flags: vec![flag],
            segments: vec![Segment {
                id: segment_id,
                updated_at: version,
                included: vec!["user-1".to_owned()],
                ..Segment::default()
            }],
        }
    }

    let store = Arc::new(SnapshotStore::new());
    let initial = versioned_data(1);
    store.populate(&DataSet {
        event_type: "full".to_owned(),
        ..initial
    });

    let writer_store = Arc::clone(&store);
    let writer = thread::spawn(move || {
        for version in 2..=500 {
            assert_eq!(
                writer_store.patch(&versioned_data(version)),
                PatchResult::Changed
            );
        }
    });
    let readers = (0..4)
        .map(|_| {
            let store = Arc::clone(&store);
            thread::spawn(move || {
                let user = FbUser::builder("user-1").build();
                for _ in 0..2_000 {
                    let snapshot = store.load();
                    let result = Evaluator::evaluate(&snapshot, "patch-consistent", &user)
                        .expect("every atomically published patch should evaluate");
                    assert_eq!(result.variation.value, "false");
                    assert!(matches!(result.reason, EvalReason::RuleMatch { .. }));
                }
            })
        })
        .collect::<Vec<_>>();

    writer.join().expect("patch writer should finish");
    for reader in readers {
        reader.join().expect("evaluation reader should finish");
    }
}
