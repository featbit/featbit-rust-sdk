mod dispatch;
mod evaluator;
mod operators;
mod segments;

#[cfg(test)]
pub(crate) mod test_support;

use std::fmt;

use crate::model::Variation;

pub(crate) use evaluator::Evaluator;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum EvalError {
    InvalidContext,
    FlagNotFound,
    MalformedFlag,
}

impl fmt::Display for EvalError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self {
            Self::InvalidContext => "targeting key is missing",
            Self::FlagNotFound => "flag not found",
            Self::MalformedFlag => "malformed flag",
        };
        formatter.write_str(message)
    }
}

/// Why `FeatBit` selected a variation before converting its string value.
///
/// This protocol-level reason is exposed by [`crate::RawEvaluation`]. Most applications should use
/// the simpler [`crate::ReasonKind`] carried by typed detail methods.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum EvaluationReason {
    /// The flag is disabled and selected its configured disabled variation.
    Off,
    /// The user was directly targeted.
    TargetMatch,
    /// A targeting rule matched.
    ///
    /// `split` is `true` when the rule selected among percentage rollout ranges.
    RuleMatch {
        /// The configured rule name.
        name: String,
        /// Whether the rule selected among percentage rollout ranges.
        split: bool,
    },
    /// No target or rule matched, so the flag used its fallthrough rollout.
    ///
    /// `split` is `true` when the fallthrough selected among percentage rollout ranges.
    Fallthrough {
        /// Whether the fallthrough selected among percentage rollout ranges.
        split: bool,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum EvalReason<'a> {
    Off,
    TargetMatch,
    RuleMatch { name: &'a str, split: bool },
    Fallthrough { split: bool },
}

impl EvalReason<'_> {
    pub(crate) fn into_owned(self) -> EvaluationReason {
        match self {
            Self::Off => EvaluationReason::Off,
            Self::TargetMatch => EvaluationReason::TargetMatch,
            Self::RuleMatch { name, split } => EvaluationReason::RuleMatch {
                name: name.to_owned(),
                split,
            },
            Self::Fallthrough { split } => EvaluationReason::Fallthrough { split },
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct EvalResult<'a> {
    pub(crate) flag_id: &'a str,
    pub(crate) flag_type: &'a str,
    pub(crate) variation: &'a Variation,
    pub(crate) reason: EvalReason<'a>,
    pub(crate) send_to_experiment: bool,
}
