use crate::model::{Fallthrough, FeatureFlag, RolloutVariation, Variation};

pub(crate) fn variation(id: &str, value: &str) -> Variation {
    Variation {
        id: id.to_owned(),
        value: value.to_owned(),
    }
}

pub(crate) fn rollout(id: &str) -> RolloutVariation {
    RolloutVariation {
        id: id.to_owned(),
        rollout: vec![0.0, 1.0],
        expt_rollout: 0.0,
    }
}

pub(crate) fn basic_flag(key: &str) -> FeatureFlag {
    FeatureFlag {
        id: format!("{key}-id"),
        key: key.to_owned(),
        updated_at: 1,
        variation_type: "boolean".to_owned(),
        variations: vec![variation("true", "true"), variation("false", "false")],
        target_users: Vec::new(),
        rules: Vec::new(),
        is_enabled: true,
        disabled_variation_id: "false".to_owned(),
        fallthrough: Fallthrough {
            variations: vec![rollout("true")],
            ..Fallthrough::default()
        },
        expt_include_all_targets: false,
        is_archived: false,
    }
}
