use crate::model::{Condition, FbUser, Segment, TargetRule};
use crate::prepared::{
    PreparedCondition, PreparedRule, PreparedSegment, PreparedSegmentIds, IS_IN_SEGMENT,
    IS_NOT_IN_SEGMENT,
};
use crate::store::DataSnapshot;

use super::operators::condition_matches_prepared;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SegmentMatch {
    Matched,
    NotMatched,
    Invalid,
}

pub(super) fn rule_matches_prepared(
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
                matches_any_segment(
                    snapshot,
                    condition,
                    prepared.and_then(|prepared| prepared.condition(index)),
                    user,
                ) == SegmentMatch::Matched
            }
            IS_NOT_IN_SEGMENT => {
                matches_any_segment(
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
                match_segment_ids(snapshot, segment_ids, user)
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
    match_segment_ids(snapshot, &segment_ids, user)
}

fn match_segment_ids(
    snapshot: &DataSnapshot,
    segment_ids: &[String],
    user: &FbUser,
) -> SegmentMatch {
    for segment_id in segment_ids {
        let Some(segment) = snapshot
            .segments
            .get(segment_id.as_str())
            .filter(|segment| !segment.is_archived)
        else {
            return SegmentMatch::Invalid;
        };
        let prepared = snapshot
            .prepared
            .segments
            .get(segment_id.as_str())
            .map(AsRef::as_ref);
        if segment_matches(segment, prepared, user) {
            return SegmentMatch::Matched;
        }
    }
    SegmentMatch::NotMatched
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
#[cfg(test)]
mod tests;
