//! Runs a bounded, destructive `FeatBit` Cloud synchronization test through Axum and
//! `OpenFeature`.
//!
//! This example creates and archives one uniquely named feature flag in the explicitly selected
//! environment. It refuses to mutate remote state unless `FEATBIT_TEST_ALLOW_REMOTE_MUTATIONS`
//! exactly equals `FEATBIT_ENVIRONMENT_ID`. Credentials are read from environment variables and
//! are never persisted or printed.

use std::collections::BTreeMap;
use std::env;
use std::error::Error;
use std::io;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use featbit_server_sdk::{ClientStatus, FbOptions, FeatBitProvider, SDK_VERSION};
use open_feature::{Client as OpenFeatureClient, EvaluationContext, OpenFeature};
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE, USER_AGENT};
use reqwest::{Client as HttpClient, Method, Url};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::{JoinHandle, JoinSet};
use tokio::time;

type AnyError = Box<dyn Error + Send + Sync>;
type TestResult<T> = Result<T, AnyError>;

const FLAG_PREFIX: &str = "codex-rust-sdk-p0p1-";
const DEFAULT_EVALUATION_WORKERS: usize = 24;
const DEFAULT_REQUESTS_PER_WORKER: usize = 1_000;
const DEFAULT_UPDATE_COUNT: usize = 80;
const CONCURRENT_UPDATE_BURST: usize = 16;
const ROLLOUT_USERS: usize = 400;
const ROLLOUT_REPEAT_USERS: usize = 40;
const SYNC_TIMEOUT: Duration = Duration::from_secs(15);

#[tokio::main]
async fn main() -> TestResult<()> {
    let _ignored = env_logger::try_init();
    let config = TestConfig::from_environment()?;
    let api = RestApi::new(&config)?;
    api.verify_scope().await?;

    let flag = TestFlag::new()?;
    if let Err(error) = api.create_flag(&flag).await {
        let _cleanup = api.archive_flag(&flag).await;
        return Err(error);
    }
    println!("created scoped test flag {}", flag.key);

    let result = run_application(&config, &api, &flag).await;
    if result.is_err() {
        if let Err(error) = api.archive_flag(&flag).await {
            eprintln!("failed to archive test flag after scenario failure: {error}");
        }
    }

    let report = result?;
    println!(
        "cloud Axum/OpenFeature test passed: flag={}, updates={}, evaluations={}, maxLatencyMs={}, rolloutOn={}, rolloutOff={}, finalSyncMs={}",
        flag.key,
        report.updates,
        report.evaluations,
        report.max_latency.as_millis(),
        report.rollout_on,
        report.rollout_off,
        report.final_sync_latency.as_millis()
    );
    Ok(())
}

struct TestConfig {
    streaming_url: String,
    event_url: String,
    api_url: Url,
    environment_secret: String,
    access_token: Arc<str>,
    project_id: String,
    environment_id: String,
    disable_events: bool,
    evaluation_workers: usize,
    requests_per_worker: usize,
    update_count: usize,
}

impl TestConfig {
    fn from_environment() -> TestResult<Self> {
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

#[derive(Clone)]
struct RestApi {
    client: HttpClient,
    base_url: Url,
    access_token: Arc<str>,
    project_id: Arc<str>,
    environment_id: Arc<str>,
}

impl RestApi {
    fn new(config: &TestConfig) -> TestResult<Self> {
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

    async fn verify_scope(&self) -> TestResult<()> {
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

    async fn create_flag(&self, flag: &TestFlag) -> TestResult<()> {
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

    async fn patch_flag(&self, flag: &TestFlag, operations: &Value) -> TestResult<()> {
        flag.ensure_scoped()?;
        let request_path = format!(
            "api/v1/envs/{}/feature-flags/{}",
            self.environment_id, flag.key
        );
        self.request(Method::PATCH, &request_path, Some(operations))
            .await?;
        Ok(())
    }

    async fn current_fallthrough_variation(&self, flag: &TestFlag) -> TestResult<String> {
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

    async fn archive_flag(&self, flag: &TestFlag) -> TestResult<()> {
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
struct TestFlag {
    key: String,
    suffix: String,
    on_variation: String,
    off_variation: String,
}

impl TestFlag {
    fn new() -> TestResult<Self> {
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

fn random_uuid() -> String {
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

#[derive(Clone)]
struct AppState {
    flags: Arc<OpenFeatureClient>,
    provider: FeatBitProvider,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct EvaluationRequest {
    targeting_key: String,
    #[serde(default)]
    name: String,
    #[serde(default)]
    attributes: BTreeMap<String, String>,
    #[serde(default)]
    default_value: bool,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct EvaluationResponse {
    value: bool,
    variation_id: Option<String>,
    reason: String,
    used_fallback: bool,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct HealthResponse {
    status: &'static str,
    initialized: bool,
}

async fn evaluate_boolean(
    State(state): State<AppState>,
    Path(flag_key): Path<String>,
    Json(request): Json<EvaluationRequest>,
) -> Json<EvaluationResponse> {
    let EvaluationRequest {
        targeting_key,
        name,
        attributes,
        default_value,
    } = request;
    let mut context = EvaluationContext::default().with_targeting_key(targeting_key);
    context.add_custom_field("name", name);
    for (field, value) in attributes {
        if field != "name" {
            context.add_custom_field(field, value);
        }
    }

    let response = match state
        .flags
        .get_bool_details(&flag_key, Some(&context), None)
        .await
    {
        Ok(details) => EvaluationResponse {
            value: details.value,
            variation_id: details.variant,
            reason: details
                .reason
                .map_or_else(|| "UNKNOWN".to_owned(), |reason| reason.to_string()),
            used_fallback: false,
        },
        Err(error) => EvaluationResponse {
            value: default_value,
            variation_id: None,
            reason: error.code.to_string(),
            used_fallback: true,
        },
    };
    Json(response)
}

async fn readiness(State(state): State<AppState>) -> (StatusCode, Json<HealthResponse>) {
    let status = state.provider.client().status();
    let (http_status, text) = match status {
        ClientStatus::Ready => (StatusCode::OK, "ready"),
        ClientStatus::Stale => (StatusCode::OK, "stale"),
        ClientStatus::NotReady => (StatusCode::SERVICE_UNAVAILABLE, "not-ready"),
        ClientStatus::Closed => (StatusCode::SERVICE_UNAVAILABLE, "closed"),
        _ => (StatusCode::SERVICE_UNAVAILABLE, "unknown"),
    };
    (
        http_status,
        Json(HealthResponse {
            status: text,
            initialized: state.provider.client().initialized(),
        }),
    )
}

struct RunningApplication {
    address: SocketAddr,
    shutdown: Option<oneshot::Sender<()>>,
    task: JoinHandle<Result<(), io::Error>>,
}

impl RunningApplication {
    async fn start(state: AppState) -> TestResult<Self> {
        let router = Router::new()
            .route("/health/ready", get(readiness))
            .route("/api/flags/{flag_key}/evaluate", post(evaluate_boolean))
            .with_state(state);
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let (shutdown, receiver) = oneshot::channel();
        let task = tokio::spawn(async move {
            axum::serve(listener, router)
                .with_graceful_shutdown(async move {
                    let _ignored = receiver.await;
                })
                .await
        });
        Ok(Self {
            address,
            shutdown: Some(shutdown),
            task,
        })
    }

    async fn stop(mut self) -> TestResult<()> {
        if let Some(shutdown) = self.shutdown.take() {
            let _ignored = shutdown.send(());
        }
        match time::timeout(Duration::from_secs(5), self.task).await {
            Ok(Ok(Ok(()))) => Ok(()),
            Ok(Ok(Err(error))) => Err(error.into()),
            Ok(Err(error)) => Err(error.into()),
            Err(_) => Err(failure("Axum did not stop within five seconds")),
        }
    }

    fn evaluation_url(&self, flag: &TestFlag) -> String {
        format!("http://{}/api/flags/{}/evaluate", self.address, flag.key)
    }
}

#[derive(Clone)]
struct Probe {
    targeting_key: String,
    name: String,
    attributes: BTreeMap<String, String>,
}

impl Probe {
    fn user(targeting_key: impl Into<String>) -> Self {
        Self {
            targeting_key: targeting_key.into(),
            name: "Cloud test user".to_owned(),
            attributes: BTreeMap::new(),
        }
    }

    fn with_attribute(mut self, name: &str, value: &str) -> Self {
        self.attributes.insert(name.to_owned(), value.to_owned());
        self
    }

    fn request(&self) -> EvaluationRequest {
        EvaluationRequest {
            targeting_key: self.targeting_key.clone(),
            name: self.name.clone(),
            attributes: self.attributes.clone(),
            default_value: false,
        }
    }
}

#[derive(Debug)]
struct ScenarioReport {
    updates: usize,
    evaluations: usize,
    max_latency: Duration,
    rollout_on: usize,
    rollout_off: usize,
    final_sync_latency: Duration,
}

async fn run_application(
    config: &TestConfig,
    api: &RestApi,
    flag: &TestFlag,
) -> TestResult<ScenarioReport> {
    let options = FbOptions::builder(config.environment_secret.clone())
        .streaming_url(config.streaming_url.clone())
        .event_url(config.event_url.clone())
        .disable_events(config.disable_events)
        .start_wait(Duration::from_secs(10))
        .build()?;
    let provider = tokio::task::spawn_blocking(move || FeatBitProvider::new(options)).await?;
    if !provider.client().initialized()
        || !matches!(
            provider.client().status(),
            ClientStatus::Ready | ClientStatus::Stale
        )
    {
        provider.client().close();
        return Err(failure(
            "FeatBit provider did not initialize from the cloud",
        ));
    }
    let featbit = provider.client().clone();
    let flags = {
        let mut open_feature = OpenFeature::singleton_mut().await;
        open_feature.set_provider(provider.clone()).await;
        Arc::new(open_feature.create_client())
    };
    let application = RunningApplication::start(AppState { flags, provider }).await?;
    let evaluation_url = application.evaluation_url(flag);
    let http = HttpClient::builder()
        .connect_timeout(Duration::from_secs(2))
        .timeout(Duration::from_secs(5))
        .pool_max_idle_per_host(config.evaluation_workers)
        .build()?;

    let scenario = run_scenario(config, api, flag, &http, &evaluation_url).await;
    let application_stop = application.stop().await;
    OpenFeature::singleton_mut().await.shutdown().await;
    let client_close = tokio::task::spawn_blocking(move || featbit.close()).await;

    let report = scenario?;
    application_stop?;
    client_close?;
    Ok(report)
}

async fn run_scenario(
    config: &TestConfig,
    api: &RestApi,
    flag: &TestFlag,
    http: &HttpClient,
    evaluation_url: &str,
) -> TestResult<ScenarioReport> {
    let ordinary = Probe::user("ordinary-user");
    wait_for_value(http, evaluation_url, &ordinary, true, &flag.on_variation).await?;

    configure_targeting(api, flag).await?;
    wait_for_value(http, evaluation_url, &ordinary, false, &flag.off_variation).await?;
    wait_for_value(
        http,
        evaluation_url,
        &Probe::user("direct-target-user"),
        true,
        &flag.on_variation,
    )
    .await?;
    wait_for_value(
        http,
        evaluation_url,
        &Probe::user("rule-target-user").with_attribute("country", "CN"),
        true,
        &flag.on_variation,
    )
    .await?;

    configure_split(api, flag).await?;
    wait_for_reason(http, evaluation_url, &ordinary, "split").await?;
    let (rollout_on, rollout_off, rollout_evaluations) =
        verify_rollout(http, evaluation_url, flag).await?;

    let burst_variation = run_concurrent_update_burst(api, flag).await?;
    let burst_value = variation_value(flag, &burst_variation)?;
    wait_for_value(
        http,
        evaluation_url,
        &ordinary,
        burst_value,
        &burst_variation,
    )
    .await?;

    configure_single_variation(api, flag, &flag.on_variation).await?;
    wait_for_value(http, evaluation_url, &ordinary, true, &flag.on_variation).await?;
    let load = run_update_load(config, api, flag, http, evaluation_url).await?;

    let final_update = Instant::now();
    configure_single_variation(api, flag, &flag.on_variation).await?;
    wait_for_value(http, evaluation_url, &ordinary, true, &flag.on_variation).await?;
    let final_sync_latency = final_update.elapsed();

    api.archive_flag(flag).await?;
    wait_for_fallback(http, evaluation_url, &ordinary).await?;

    Ok(ScenarioReport {
        updates: config.update_count + CONCURRENT_UPDATE_BURST + 6,
        evaluations: load.evaluations + rollout_evaluations,
        max_latency: load.max_latency,
        rollout_on,
        rollout_off,
        final_sync_latency,
    })
}

async fn call_evaluation(
    http: &HttpClient,
    evaluation_url: &str,
    probe: &Probe,
) -> TestResult<EvaluationResponse> {
    let response = http
        .post(evaluation_url)
        .header(CONTENT_TYPE, "application/json")
        .body(serde_json::to_vec(&probe.request())?)
        .send()
        .await?;
    let status = response.status();
    if !status.is_success() {
        return Err(failure(format!("Axum evaluation returned HTTP {status}")));
    }
    Ok(serde_json::from_slice(&response.bytes().await?)?)
}

async fn wait_for_value(
    http: &HttpClient,
    evaluation_url: &str,
    probe: &Probe,
    expected_value: bool,
    expected_variation: &str,
) -> TestResult<Duration> {
    let started = Instant::now();
    loop {
        let response = call_evaluation(http, evaluation_url, probe).await?;
        if !response.used_fallback
            && response.value == expected_value
            && response.variation_id.as_deref() == Some(expected_variation)
        {
            return Ok(started.elapsed());
        }
        if started.elapsed() >= SYNC_TIMEOUT {
            return Err(failure(format!(
                "flag did not converge to variation {expected_variation} within {SYNC_TIMEOUT:?}"
            )));
        }
        time::sleep(Duration::from_millis(20)).await;
    }
}

async fn wait_for_reason(
    http: &HttpClient,
    evaluation_url: &str,
    probe: &Probe,
    expected_reason: &str,
) -> TestResult<()> {
    let started = Instant::now();
    loop {
        let response = call_evaluation(http, evaluation_url, probe).await?;
        if !response.used_fallback
            && response
                .reason
                .to_ascii_lowercase()
                .contains(expected_reason)
        {
            return Ok(());
        }
        if started.elapsed() >= SYNC_TIMEOUT {
            return Err(failure(format!(
                "flag did not report reason {expected_reason:?} within {SYNC_TIMEOUT:?}"
            )));
        }
        time::sleep(Duration::from_millis(20)).await;
    }
}

async fn wait_for_fallback(
    http: &HttpClient,
    evaluation_url: &str,
    probe: &Probe,
) -> TestResult<()> {
    let started = Instant::now();
    loop {
        if call_evaluation(http, evaluation_url, probe)
            .await?
            .used_fallback
        {
            return Ok(());
        }
        if started.elapsed() >= SYNC_TIMEOUT {
            return Err(failure(
                "archived flag remained evaluable after the synchronization timeout",
            ));
        }
        time::sleep(Duration::from_millis(20)).await;
    }
}

async fn configure_targeting(api: &RestApi, flag: &TestFlag) -> TestResult<()> {
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

async fn configure_split(api: &RestApi, flag: &TestFlag) -> TestResult<()> {
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

async fn configure_single_variation(
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

async fn configure_fallthrough_only(
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

async fn run_concurrent_update_burst(api: &RestApi, flag: &TestFlag) -> TestResult<String> {
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

fn variation_value(flag: &TestFlag, variation_id: &str) -> TestResult<bool> {
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

async fn verify_rollout(
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

#[derive(Default)]
struct LoadReport {
    evaluations: usize,
    max_latency: Duration,
}

async fn run_update_load(
    config: &TestConfig,
    api: &RestApi,
    flag: &TestFlag,
    http: &HttpClient,
    evaluation_url: &str,
) -> TestResult<LoadReport> {
    let updates_done = Arc::new(AtomicBool::new(false));
    let mut workers = JoinSet::new();
    for worker in 0..config.evaluation_workers {
        let worker_http = http.clone();
        let worker_url = evaluation_url.to_owned();
        let worker_flag = flag.clone();
        let done = Arc::clone(&updates_done);
        let request_count = config.requests_per_worker;
        workers.spawn(async move {
            run_evaluation_worker(
                worker,
                request_count,
                &worker_http,
                &worker_url,
                &worker_flag,
                &done,
            )
            .await
        });
    }

    let update_api = api.clone();
    let update_flag = flag.clone();
    let update_count = config.update_count;
    let updater = tokio::spawn(async move {
        for index in 0..update_count {
            let variation = if index % 2 == 0 {
                &update_flag.off_variation
            } else {
                &update_flag.on_variation
            };
            configure_single_variation(&update_api, &update_flag, variation).await?;
        }
        TestResult::Ok(())
    });

    let update_result = updater.await;
    updates_done.store(true, Ordering::Release);
    update_result??;

    let mut total = LoadReport::default();
    while let Some(result) = workers.join_next().await {
        let worker = result??;
        total.evaluations += worker.evaluations;
        total.max_latency = total.max_latency.max(worker.max_latency);
    }
    Ok(total)
}

async fn run_evaluation_worker(
    worker: usize,
    minimum_requests: usize,
    http: &HttpClient,
    evaluation_url: &str,
    flag: &TestFlag,
    updates_done: &AtomicBool,
) -> TestResult<LoadReport> {
    let maximum_requests = minimum_requests.saturating_mul(3).min(5_000);
    let mut report = LoadReport::default();
    for request in 0..maximum_requests {
        if request >= minimum_requests && updates_done.load(Ordering::Acquire) {
            break;
        }
        let probe = Probe::user(format!("load-user-{worker}-{request}"));
        let started = Instant::now();
        let response = call_evaluation(http, evaluation_url, &probe).await?;
        report.max_latency = report.max_latency.max(started.elapsed());
        validate_resolution(&response, flag)?;
        report.evaluations += 1;
    }
    Ok(report)
}

fn validate_resolution(response: &EvaluationResponse, flag: &TestFlag) -> TestResult<()> {
    if response.used_fallback {
        return Err(failure(format!(
            "OpenFeature unexpectedly used a fallback ({})",
            response.reason
        )));
    }
    let expected_variation = if response.value {
        flag.on_variation.as_str()
    } else {
        flag.off_variation.as_str()
    };
    if response.variation_id.as_deref() != Some(expected_variation) {
        return Err(failure("OpenFeature returned a value/variation mismatch"));
    }
    Ok(())
}

fn failure(message: impl Into<String>) -> AnyError {
    io::Error::other(message.into()).into()
}
