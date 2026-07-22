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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PatchResult {
    Changed,
    Unchanged,
    VersionConflict,
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

    pub(crate) fn patch(&self, data: &DataSet) -> PatchResult {
        let _write_guard = self.write_lock.lock();
        let current = self.load();
        let mut flags = current.flags.clone();
        let mut segments = current.segments.clone();
        let mut prepared = current.prepared.clone();
        let mut changed = false;
        let mut version_conflict = false;

        for flag in &data.feature_flags {
            if flag.key.is_empty() {
                continue;
            }
            let should_replace = match flags.get(&flag.key) {
                None => true,
                Some(existing) if existing.updated_at < flag.updated_at => true,
                Some(existing)
                    if existing.updated_at == flag.updated_at && existing.as_ref() != flag =>
                {
                    version_conflict = true;
                    false
                }
                Some(_) => false,
            };
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
            let should_replace = match segments.get(&segment.id) {
                None => true,
                Some(existing) if existing.updated_at < segment.updated_at => true,
                Some(existing)
                    if existing.updated_at == segment.updated_at
                        && existing.as_ref() != segment =>
                {
                    version_conflict = true;
                    false
                }
                Some(_) => false,
            };
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
        if version_conflict {
            PatchResult::VersionConflict
        } else if changed {
            PatchResult::Changed
        } else {
            PatchResult::Unchanged
        }
    }
}
#[cfg(test)]
mod tests;
