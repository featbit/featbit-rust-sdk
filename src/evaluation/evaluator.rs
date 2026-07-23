use crate::model::{FbUser, FeatureFlag, RolloutVariation, Variation};
use crate::prepared::PreparedFlag;
use crate::store::DataSnapshot;

use super::dispatch::{is_in_rollout, is_percentage_split};
use super::segments::rule_matches_prepared;
use super::{EvalError, EvalResult, EvaluationReason};

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
            return Ok(Self::result(flag, variation, EvaluationReason::Off, false));
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
                EvaluationReason::TargetMatch,
                flag.expt_include_all_targets,
            ));
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
            let send_to_experiment = Self::should_send_to_experiment(
                flag.expt_include_all_targets,
                rule.included_in_expt,
                &dispatch_key,
                rollout,
            );
            return Ok(Self::result(
                flag,
                variation,
                EvaluationReason::RuleMatch {
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
            EvaluationReason::Fallthrough {
                split: is_percentage_split(&flag.fallthrough.variations),
            },
            send_to_experiment,
        ))
    }

    fn result(
        flag: &FeatureFlag,
        variation: &Variation,
        reason: EvaluationReason,
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
#[cfg(test)]
mod tests;
