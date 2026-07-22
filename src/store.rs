use std::collections::HashMap;
use std::sync::Arc;

use arc_swap::ArcSwap;
use parking_lot::Mutex;

use crate::model::{DataSet, FeatureFlag, Segment};
use crate::prepared::{PreparedFlag, PreparedSegment, PreparedSnapshot};

#[derive(Clone, Debug, Default)]
pub(crate) struct DataSnapshot {
    pub(crate) flags: HashMap<String, Arc<FeatureFlag>>,
    pub(crate) segments: HashMap<String, Arc<Segment>>,
    pub(crate) prepared: PreparedSnapshot,
    pub(crate) version: i64,
    pub(crate) populated: bool,
}

#[derive(Debug, Default)]
pub(crate) struct SnapshotStore {
    snapshot: ArcSwap<DataSnapshot>,
    write_lock: Mutex<()>,
}

impl SnapshotStore {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn load(&self) -> Arc<DataSnapshot> {
        self.snapshot.load_full()
    }

    pub(crate) fn version(&self) -> i64 {
        self.snapshot.load().version
    }

    pub(crate) fn populate(&self, data: &DataSet) {
        let _write_guard = self.write_lock.lock();
        let mut flags = HashMap::with_capacity(data.feature_flags.len());
        let mut segments = HashMap::with_capacity(data.segments.len());
        let mut prepared = PreparedSnapshot {
            flags: HashMap::with_capacity(data.feature_flags.len()),
            segments: HashMap::with_capacity(data.segments.len()),
        };
        let mut version = 0;

        for flag in &data.feature_flags {
            if flag.key.is_empty() {
                continue;
            }
            version = version.max(flag.updated_at);
            prepared
                .flags
                .insert(flag.key.clone(), Arc::new(PreparedFlag::new(flag)));
            flags.insert(flag.key.clone(), Arc::new(flag.clone()));
        }
        for segment in &data.segments {
            if segment.id.is_empty() {
                continue;
            }
            version = version.max(segment.updated_at);
            prepared
                .segments
                .insert(segment.id.clone(), Arc::new(PreparedSegment::new(segment)));
            segments.insert(segment.id.clone(), Arc::new(segment.clone()));
        }

        self.snapshot.store(Arc::new(DataSnapshot {
            flags,
            segments,
            prepared,
            version,
            populated: true,
        }));
    }

    pub(crate) fn patch(&self, data: &DataSet) -> bool {
        let _write_guard = self.write_lock.lock();
        let current = self.load();
        let mut flags = current.flags.clone();
        let mut segments = current.segments.clone();
        let mut prepared = current.prepared.clone();
        let mut changed = false;

        for flag in &data.feature_flags {
            if flag.key.is_empty() {
                continue;
            }
            let should_replace = flags
                .get(&flag.key)
                .is_none_or(|existing| existing.updated_at < flag.updated_at);
            if should_replace {
                prepared
                    .flags
                    .insert(flag.key.clone(), Arc::new(PreparedFlag::new(flag)));
                flags.insert(flag.key.clone(), Arc::new(flag.clone()));
                changed = true;
            }
        }
        for segment in &data.segments {
            if segment.id.is_empty() {
                continue;
            }
            let should_replace = segments
                .get(&segment.id)
                .is_none_or(|existing| existing.updated_at < segment.updated_at);
            if should_replace {
                prepared
                    .segments
                    .insert(segment.id.clone(), Arc::new(PreparedSegment::new(segment)));
                segments.insert(segment.id.clone(), Arc::new(segment.clone()));
                changed = true;
            }
        }

        if changed || !current.populated {
            let version = flags
                .values()
                .map(|flag| flag.updated_at)
                .chain(segments.values().map(|segment| segment.updated_at))
                .max()
                .unwrap_or_default();
            self.snapshot.store(Arc::new(DataSnapshot {
                flags,
                segments,
                prepared,
                version,
                populated: true,
            }));
        }
        changed
    }
}

#[cfg(test)]
mod tests {
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

        assert!(store.patch(&DataSet {
            event_type: "patch".to_owned(),
            feature_flags: vec![flag("a", 2, true)],
            ..DataSet::default()
        }));
        assert!(!store.patch(&DataSet {
            event_type: "patch".to_owned(),
            feature_flags: vec![flag("a", 1, false)],
            ..DataSet::default()
        }));
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
                    assert!(store.patch(&data));
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

        assert!(store.patch(&DataSet {
            event_type: "patch".to_owned(),
            feature_flags: vec![flag("other", 2, false)],
            ..DataSet::default()
        }));
        let patched = store.load();
        assert!(Arc::ptr_eq(&prepared, &patched.prepared.flags["prepared"]));
    }
}
