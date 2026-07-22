use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use featbit_server_sdk::SDK_VERSION;
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE, USER_AGENT};
use reqwest::{Client as HttpClient, Method, Url};
use serde_json::{json, Value};

use super::config::TestConfig;
use super::{failure, TestResult, FLAG_PREFIX};

#[derive(Clone)]
pub(super) struct RestApi {
    client: HttpClient,
    base_url: Url,
    access_token: Arc<str>,
    project_id: Arc<str>,
    environment_id: Arc<str>,
}

impl RestApi {
    pub(super) fn new(config: &TestConfig) -> TestResult<Self> {
        let client = HttpClient::builder()
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(15))
            .build()?;
        Ok(Self {
            client,
            base_url: config.api_url.clone(),
            access_token: Arc::clone(&config.access_token),
            project_id: Arc::from(config.project_id.as_str()),
            environment_id: Arc::from(config.environment_id.as_str()),
        })
    }

    pub(super) async fn verify_scope(&self) -> TestResult<()> {
        let path = format!(
            "api/v1/projects/{}/envs/{}",
            self.project_id, self.environment_id
        );
        let response = self.request(Method::GET, &path, None).await?;
        let id = response
            .get("data")
            .and_then(|data| data.get("id"))
            .and_then(Value::as_str)
            .ok_or_else(|| failure("scope verification response did not contain an environment"))?;
        if id != self.environment_id.as_ref() {
            return Err(failure("scope verification returned another environment"));
        }
        Ok(())
    }

    pub(super) async fn create_flag(&self, flag: &TestFlag) -> TestResult<()> {
        flag.ensure_scoped()?;
        let path = format!("api/v1/envs/{}/feature-flags", self.environment_id);
        let body = json!({
            "name": format!("Codex Rust SDK P0/P1 test {}", flag.suffix),
            "key": flag.key,
            "description": "Bounded Axum/OpenFeature synchronization test; safe to archive",
            "isEnabled": true,
            "variationType": "boolean",
            "variations": [
                {"id": flag.on_variation, "name": "On", "value": "true"},
                {"id": flag.off_variation, "name": "Off", "value": "false"}
            ],
            "enabledVariationId": flag.on_variation,
            "disabledVariationId": flag.off_variation,
            "tags": ["codex-rust-sdk-extreme-test"]
        });
        let response = self.request(Method::POST, &path, Some(&body)).await?;
        let data = response
            .get("data")
            .ok_or_else(|| failure("create response did not contain flag data"))?;
        let returned_key = data.get("key").and_then(Value::as_str).unwrap_or_default();
        let returned_environment = data
            .get("envId")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if returned_key != flag.key || returned_environment != self.environment_id.as_ref() {
            return Err(failure(
                "created flag was not returned in the selected environment",
            ));
        }
        Ok(())
    }

    pub(super) async fn patch_flag(&self, flag: &TestFlag, operations: &Value) -> TestResult<()> {
        flag.ensure_scoped()?;
        let request_path = format!(
            "api/v1/envs/{}/feature-flags/{}",
            self.environment_id, flag.key
        );
        self.request(Method::PATCH, &request_path, Some(operations))
            .await?;
        Ok(())
    }

    pub(super) async fn current_fallthrough_variation(
        &self,
        flag: &TestFlag,
    ) -> TestResult<String> {
        flag.ensure_scoped()?;
        let request_path = format!(
            "api/v1/envs/{}/feature-flags/{}",
            self.environment_id, flag.key
        );
        let response = self.request(Method::GET, &request_path, None).await?;
        let variation_id = response
            .get("data")
            .and_then(|data| data.get("fallthrough"))
            .and_then(|fallthrough| fallthrough.get("variations"))
            .and_then(Value::as_array)
            .and_then(|variations| variations.first())
            .and_then(|variation| variation.get("id"))
            .and_then(Value::as_str)
            .filter(|id| !id.is_empty())
            .ok_or_else(|| failure("flag GET response had no fallthrough variation"))?;
        Ok(variation_id.to_owned())
    }

    pub(super) async fn archive_flag(&self, flag: &TestFlag) -> TestResult<()> {
        self.patch_flag(
            flag,
            &json!([{"op": "replace", "path": "/isArchived", "value": true}]),
        )
        .await
    }

    async fn request(&self, method: Method, path: &str, body: Option<&Value>) -> TestResult<Value> {
        let url = self.base_url.join(path)?;
        if url.origin() != self.base_url.origin() {
            return Err(failure("refusing to send a REST request to another origin"));
        }

        let mut request = self
            .client
            .request(method, url)
            .header(AUTHORIZATION, self.access_token.as_ref())
            .header(
                USER_AGENT,
                format!("featbit-rust-sdk-cloud-test/{SDK_VERSION}"),
            );
        if let Some(body) = body {
            request = request
                .header(CONTENT_TYPE, "application/json")
                .body(serde_json::to_vec(body)?);
        }

        let response = request.send().await?;
        let status = response.status();
        let bytes = response.bytes().await?;
        if !status.is_success() {
            return Err(failure(format!(
                "FeatBit REST request failed with HTTP {status}; response body omitted"
            )));
        }
        let value: Value = serde_json::from_slice(&bytes)?;
        if value.get("success").and_then(Value::as_bool) == Some(false) {
            return Err(failure("FeatBit REST response reported failure"));
        }
        Ok(value)
    }
}

#[derive(Clone)]
pub(super) struct TestFlag {
    pub(super) key: String,
    pub(super) suffix: String,
    pub(super) on_variation: String,
    pub(super) off_variation: String,
}

impl TestFlag {
    pub(super) fn new() -> TestResult<Self> {
        let timestamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
        let suffix = format!("{timestamp}-{}", std::process::id());
        Ok(Self {
            key: format!("{FLAG_PREFIX}{suffix}"),
            suffix,
            on_variation: random_uuid(),
            off_variation: random_uuid(),
        })
    }

    fn ensure_scoped(&self) -> TestResult<()> {
        if !self.key.starts_with(FLAG_PREFIX)
            || !self
                .key
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        {
            return Err(failure("refusing to mutate a flag outside the test prefix"));
        }
        Ok(())
    }
}

pub(super) fn random_uuid() -> String {
    let value = rand::random::<u128>();
    format!(
        "{:08x}-{:04x}-{:04x}-{:04x}-{:012x}",
        (value >> 96) & 0xffff_ffff,
        (value >> 80) & 0xffff,
        (value >> 64) & 0xffff,
        (value >> 48) & 0xffff,
        value & 0xffff_ffff_ffff
    )
}
