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

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum EvalReason {
    Off,
    TargetMatch,
    RuleMatch { name: String, split: bool },
    Fallthrough { split: bool },
}

#[derive(Clone, Debug)]
pub(crate) struct EvalResult {
    pub(crate) flag_id: String,
    pub(crate) flag_type: String,
    pub(crate) variation: Variation,
    pub(crate) reason: EvalReason,
    pub(crate) send_to_experiment: bool,
}
