use reqwest::Client as HttpClient;
use serde_json::json;
use tokio::task::JoinSet;

use super::api::{random_uuid, RestApi, TestFlag};
use super::application::Probe;
use super::load::validate_resolution;
use super::scenario::call_evaluation;
use super::{failure, TestResult, CONCURRENT_UPDATE_BURST, ROLLOUT_REPEAT_USERS, ROLLOUT_USERS};

pub(super) async fn configure_targeting(api: &RestApi, flag: &TestFlag) -> TestResult<()> {
    let rule_id = random_uuid();
    let condition_id = random_uuid();
    api.patch_flag(
        flag,
        &json!([
            {
                "op": "replace",
                "path": "/targetUsers",
                "value": [{
                    "variationId": flag.on_variation,
                    "keyIds": ["direct-target-user"]
                }]
            },
            {
                "op": "replace",
                "path": "/rules",
                "value": [{
                    "id": rule_id,
                    "name": "country rule",
                    "dispatchKey": null,
                    "includedInExpt": false,
                    "conditions": [{
                        "id": condition_id,
                        "property": "country",
                        "op": "Equal",
                        "value": "CN"
                    }],
                    "variations": [{
                        "id": flag.on_variation,
                        "rollout": [0.0, 1.0],
                        "exptRollout": 0.0
                    }]
                }]
            },
            {
                "op": "replace",
                "path": "/fallthrough",
                "value": {
                    "dispatchKey": null,
                    "includedInExpt": false,
                    "variations": [{
                        "id": flag.off_variation,
                        "rollout": [0.0, 1.0],
                        "exptRollout": 0.0
                    }]
                }
            }
        ]),
    )
    .await
}

pub(super) async fn configure_split(api: &RestApi, flag: &TestFlag) -> TestResult<()> {
    api.patch_flag(
        flag,
        &json!([
            {"op": "replace", "path": "/targetUsers", "value": []},
            {"op": "replace", "path": "/rules", "value": []},
            {
                "op": "replace",
                "path": "/fallthrough",
                "value": {
                    "dispatchKey": null,
                    "includedInExpt": false,
                    "variations": [
                        {
                            "id": flag.on_variation,
                            "rollout": [0.0, 0.5],
                            "exptRollout": 0.0
                        },
                        {
                            "id": flag.off_variation,
                            "rollout": [0.5, 1.0],
                            "exptRollout": 0.0
                        }
                    ]
                }
            }
        ]),
    )
    .await
}

pub(super) async fn configure_single_variation(
    api: &RestApi,
    flag: &TestFlag,
    variation_id: &str,
) -> TestResult<()> {
    api.patch_flag(
        flag,
        &json!([
            {"op": "replace", "path": "/targetUsers", "value": []},
            {"op": "replace", "path": "/rules", "value": []},
            {
                "op": "replace",
                "path": "/fallthrough",
                "value": {
                    "dispatchKey": null,
                    "includedInExpt": false,
                    "variations": [{
                        "id": variation_id,
                        "rollout": [0.0, 1.0],
                        "exptRollout": 0.0
                    }]
                }
            }
        ]),
    )
    .await
}

pub(super) async fn configure_fallthrough_only(
    api: &RestApi,
    flag: &TestFlag,
    variation_id: &str,
) -> TestResult<()> {
    api.patch_flag(
        flag,
        &json!([{
            "op": "replace",
            "path": "/fallthrough",
            "value": {
                "dispatchKey": null,
                "includedInExpt": false,
                "variations": [{
                    "id": variation_id,
                    "rollout": [0.0, 1.0],
                    "exptRollout": 0.0
                }]
            }
        }]),
    )
    .await
}

pub(super) async fn run_concurrent_update_burst(
    api: &RestApi,
    flag: &TestFlag,
) -> TestResult<String> {
    let mut updates = JoinSet::new();
    for index in 0..CONCURRENT_UPDATE_BURST {
        let update_api = api.clone();
        let update_flag = flag.clone();
        updates.spawn(async move {
            let variation = if index % 2 == 0 {
                &update_flag.on_variation
            } else {
                &update_flag.off_variation
            };
            configure_fallthrough_only(&update_api, &update_flag, variation).await
        });
    }
    while let Some(result) = updates.join_next().await {
        result??;
    }

    let variation = api.current_fallthrough_variation(flag).await?;
    variation_value(flag, &variation)?;
    Ok(variation)
}

pub(super) fn variation_value(flag: &TestFlag, variation_id: &str) -> TestResult<bool> {
    if variation_id == flag.on_variation {
        Ok(true)
    } else if variation_id == flag.off_variation {
        Ok(false)
    } else {
        Err(failure(format!(
            "cloud returned unexpected variation {variation_id:?}"
        )))
    }
}

pub(super) async fn verify_rollout(
    http: &HttpClient,
    evaluation_url: &str,
    flag: &TestFlag,
) -> TestResult<(usize, usize, usize)> {
    let mut on = 0_usize;
    let mut off = 0_usize;
    for index in 0..ROLLOUT_USERS {
        let probe = Probe::user(format!("rollout-user-{index}"));
        let first = call_evaluation(http, evaluation_url, &probe).await?;
        validate_resolution(&first, flag)?;
        if first.value {
            on += 1;
        } else {
            off += 1;
        }
        if index < ROLLOUT_REPEAT_USERS {
            let second = call_evaluation(http, evaluation_url, &probe).await?;
            if first.value != second.value || first.variation_id != second.variation_id {
                return Err(failure("percentage rollout was not deterministic"));
            }
        }
    }
    if !(ROLLOUT_USERS * 35 / 100..=ROLLOUT_USERS * 65 / 100).contains(&on) {
        return Err(failure(format!(
            "50/50 rollout distribution was unexpectedly skewed: on={on}, off={off}"
        )));
    }
    Ok((on, off, ROLLOUT_USERS + ROLLOUT_REPEAT_USERS))
}
