use std::sync::{Arc, Barrier};
use std::thread;

use super::*;
use crate::model::{Condition, TargetRule};
use crate::prepared::PreparedCondition;

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
        assert!(snapshot.flags.contains_key(&format!("flag-{index}")));
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

#[test]
fn equal_version_with_different_content_reports_a_conflict() {
    let store = SnapshotStore::new();
    store.populate(&DataSet {
        event_type: "full".to_owned(),
        feature_flags: vec![flag("same-version", 7, false)],
        ..DataSet::default()
    });

    assert_eq!(
        store.patch(&DataSet {
            event_type: "patch".to_owned(),
            feature_flags: vec![flag("same-version", 7, true)],
            ..DataSet::default()
        }),
        PatchResult::VersionConflict
    );
    assert!(!store.load().flags["same-version"].is_archived);

    assert_eq!(
        store.patch(&DataSet {
            event_type: "patch".to_owned(),
            feature_flags: vec![flag("same-version", 7, false)],
            ..DataSet::default()
        }),
        PatchResult::Unchanged
    );
}
