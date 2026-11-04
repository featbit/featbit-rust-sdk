use crate::model::{FbUser, FeatureFlag, RolloutVariation, Variation};
use crate::prepared::PreparedFlag;
use crate::store::DataSnapshot;

use super::dispatch::{is_percentage_split, RolloutMatcher};
use super::segments::rule_matches_prepared;
use super::{EvalError, EvalReason, EvalResult};

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct Evaluator;

impl Evaluator {
    pub(crate) fn evaluate<'snapshot>(
        snapshot: &'snapshot DataSnapshot,
        flag_key: &str,
        user: &FbUser,
    ) -> Result<EvalResult<'snapshot>, EvalError> {
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

    fn evaluate_flag<'snapshot>(
        snapshot: &'snapshot DataSnapshot,
        flag: &'snapshot FeatureFlag,
        prepared: Option<&PreparedFlag>,
        user: &FbUser,
    ) -> Result<EvalResult<'snapshot>, EvalError> {
        if !flag.is_enabled {
            let variation = Self::variation(flag, prepared, &flag.disabled_variation_id)
                .ok_or(EvalError::MalformedFlag)?;
            return Ok(Self::result(flag, variation, EvalReason::Off));
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
            return Ok(Self::result(flag, variation, EvalReason::TargetMatch));
        }

        for (rule_index, rule) in flag.rules.iter().enumerate() {
            let prepared_rule = prepared.and_then(|prepared| prepared.rule(rule_index));
            if !rule_matches_prepared(snapshot, rule, prepared_rule, user) {
                continue;
            }

            let dispatch_key = Self::dispatch_key(flag, rule.dispatch_key.as_deref(), user);
            let rollout = Self::select_rollout(&rule.variations, &dispatch_key)
                .ok_or(EvalError::MalformedFlag)?;
            let variation =
                Self::variation(flag, prepared, &rollout.id).ok_or(EvalError::MalformedFlag)?;
            return Ok(Self::result(
                flag,
                variation,
                EvalReason::RuleMatch {
                    name: &rule.name,
                    split: is_percentage_split(&rule.variations),
                },
            ));
        }

        let dispatch_key = Self::dispatch_key(flag, flag.fallthrough.dispatch_key.as_deref(), user);
        let rollout = Self::select_rollout(&flag.fallthrough.variations, &dispatch_key)
            .ok_or(EvalError::MalformedFlag)?;
        let variation =
            Self::variation(flag, prepared, &rollout.id).ok_or(EvalError::MalformedFlag)?;
        Ok(Self::result(
            flag,
            variation,
            EvalReason::Fallthrough {
                split: is_percentage_split(&flag.fallthrough.variations),
            },
        ))
    }

    fn result<'snapshot>(
        flag: &'snapshot FeatureFlag,
        variation: &'snapshot Variation,
        reason: EvalReason<'snapshot>,
    ) -> EvalResult<'snapshot> {
        EvalResult {
            insights_enabled: flag.insights_enabled,
            flag_id: &flag.id,
            flag_type: &flag.variation_type,
            variation,
            reason,
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
        let mut matcher = RolloutMatcher::new(dispatch_key);
        rollouts
            .iter()
            .find(|rollout| matcher.matches(&rollout.rollout))
    }
}
#[cfg(test)]
mod tests;
