use std::collections::HashMap;
use std::sync::Arc;

use arc_swap::ArcSwap;

use crate::model::{DataSet, FeatureFlag, Segment};

#[derive(Clone, Debug, Default)]
pub(crate) struct DataSnapshot {
    pub(crate) flags: HashMap<String, Arc<FeatureFlag>>,
    pub(crate) segments: HashMap<String, Arc<Segment>>,
    pub(crate) version: i64,
    pub(crate) populated: bool,
}

#[derive(Debug, Default)]
pub(crate) struct SnapshotStore {
    snapshot: ArcSwap<DataSnapshot>,
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
        let mut flags = HashMap::with_capacity(data.feature_flags.len());
        let mut segments = HashMap::with_capacity(data.segments.len());
        let mut version = 0;

        for flag in &data.feature_flags {
            if flag.key.is_empty() {
                continue;
            }
            version = version.max(flag.updated_at);
            flags.insert(flag.key.clone(), Arc::new(flag.clone()));
        }
        for segment in &data.segments {
            if segment.id.is_empty() {
                continue;
            }
            version = version.max(segment.updated_at);
            segments.insert(segment.id.clone(), Arc::new(segment.clone()));
        }

        self.snapshot.store(Arc::new(DataSnapshot {
            flags,
            segments,
            version,
            populated: true,
        }));
    }

    pub(crate) fn patch(&self, data: &DataSet) -> bool {
        let current = self.load();
        let mut flags = current.flags.clone();
        let mut segments = current.segments.clone();
        let mut changed = false;

        for flag in &data.feature_flags {
            if flag.key.is_empty() {
                continue;
            }
            let should_replace = flags
                .get(&flag.key)
                .is_none_or(|existing| existing.updated_at < flag.updated_at);
            if should_replace {
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
                version,
                populated: true,
            }));
        }
        changed
    }
}
