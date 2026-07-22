use std::env;
use std::sync::Arc;

use reqwest::Url;

use super::{
    failure, TestResult, DEFAULT_EVALUATION_WORKERS, DEFAULT_REQUESTS_PER_WORKER,
    DEFAULT_UPDATE_COUNT, ROLLOUT_REPEAT_USERS, ROLLOUT_USERS,
};

pub(super) struct TestConfig {
    pub(super) streaming_url: String,
    pub(super) event_url: String,
    pub(super) api_url: Url,
    pub(super) environment_secret: String,
    pub(super) access_token: Arc<str>,
    pub(super) project_id: String,
    pub(super) environment_id: String,
    pub(super) disable_events: bool,
    pub(super) evaluation_workers: usize,
    pub(super) requests_per_worker: usize,
    pub(super) update_count: usize,
}

impl TestConfig {
    pub(super) fn from_environment() -> TestResult<Self> {
        let environment_id = required_environment("FEATBIT_ENVIRONMENT_ID")?;
        let acknowledgement = required_environment("FEATBIT_TEST_ALLOW_REMOTE_MUTATIONS")?;
        if acknowledgement != environment_id {
            return Err(failure(
                "FEATBIT_TEST_ALLOW_REMOTE_MUTATIONS must exactly equal FEATBIT_ENVIRONMENT_ID",
            ));
        }

        let project_id = required_environment("FEATBIT_PROJECT_ID")?;
        if !is_uuid_like(&project_id) || !is_uuid_like(&environment_id) {
            return Err(failure("project and environment IDs must be UUIDs"));
        }

        let api_url = Url::parse(&required_environment("FEATBIT_API_URL")?)?;
        if api_url.scheme() != "https" || api_url.cannot_be_a_base() {
            return Err(failure("FEATBIT_API_URL must be an HTTPS base URL"));
        }

        let access_token = required_environment("FEATBIT_ACCESS_TOKEN")?;
        if !access_token.starts_with("api-") {
            return Err(failure("FEATBIT_ACCESS_TOKEN must be an API access token"));
        }

        let evaluation_workers = bounded_usize(
            "FEATBIT_TEST_EVALUATION_WORKERS",
            DEFAULT_EVALUATION_WORKERS,
            1,
            64,
        )?;
        let requests_per_worker = bounded_usize(
            "FEATBIT_TEST_REQUESTS_PER_WORKER",
            DEFAULT_REQUESTS_PER_WORKER,
            1,
            5_000,
        )?;
        let update_count =
            bounded_usize("FEATBIT_TEST_UPDATE_COUNT", DEFAULT_UPDATE_COUNT, 1, 250)?;
        let disable_events = optional_bool("FEATBIT_TEST_DISABLE_EVENTS", true)?;
        let maximum_load_evaluations =
            evaluation_workers.saturating_mul(requests_per_worker.saturating_mul(3).min(5_000));
        if !disable_events
            && maximum_load_evaluations.saturating_add(ROLLOUT_USERS + ROLLOUT_REPEAT_USERS) > 2_000
        {
            return Err(failure(
                "analytics-enabled cloud runs are capped at 2,000 planned evaluations",
            ));
        }

        Ok(Self {
            streaming_url: required_environment("FEATBIT_STREAMING_URL")?,
            event_url: required_environment("FEATBIT_EVENT_URL")?,
            api_url,
            environment_secret: required_environment("FEATBIT_ENV_SECRET")?,
            access_token: Arc::from(access_token),
            project_id,
            environment_id,
            disable_events,
            evaluation_workers,
            requests_per_worker,
            update_count,
        })
    }
}

fn required_environment(name: &str) -> TestResult<String> {
    env::var(name).map_err(|_| failure(format!("required environment variable {name} is missing")))
}

fn bounded_usize(name: &str, default: usize, minimum: usize, maximum: usize) -> TestResult<usize> {
    let value = match env::var(name) {
        Ok(text) => text
            .parse::<usize>()
            .map_err(|_| failure(format!("{name} must be an integer")))?,
        Err(env::VarError::NotPresent) => default,
        Err(error) => return Err(error.into()),
    };
    if !(minimum..=maximum).contains(&value) {
        return Err(failure(format!(
            "{name} must be between {minimum} and {maximum}"
        )));
    }
    Ok(value)
}
fn optional_bool(name: &str, default: bool) -> TestResult<bool> {
    match env::var(name) {
        Ok(text) if text.eq_ignore_ascii_case("true") || text == "1" => Ok(true),
        Ok(text) if text.eq_ignore_ascii_case("false") || text == "0" => Ok(false),
        Ok(_) => Err(failure(format!("{name} must be true, false, 1, or 0"))),
        Err(env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(error.into()),
    }
}

fn is_uuid_like(value: &str) -> bool {
    value.len() == 36
        && value.bytes().enumerate().all(|(index, byte)| match index {
            8 | 13 | 18 | 23 => byte == b'-',
            _ => byte.is_ascii_hexdigit(),
        })
}
