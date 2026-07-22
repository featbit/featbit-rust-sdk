use std::sync::Arc;
use std::time::{Duration, Instant};

use featbit_server_sdk::{ClientStatus, FbOptions, FbUser, FeatBitProvider};
use featbit_server_sdk_opentelemetry::OpenTelemetryEvaluationObserver;
use open_feature::{EvaluationContext, OpenFeature};
use reqwest::header::CONTENT_TYPE;
use reqwest::Client as HttpClient;
use tokio::time;

use super::api::{RestApi, TestFlag};
use super::application::{AppState, EvaluationResponse, Probe, RunningApplication};
use super::config::TestConfig;
use super::flag_config::{
    configure_single_variation, configure_split, configure_targeting, run_concurrent_update_burst,
    variation_value, verify_rollout,
};
use super::load::run_update_load;
use super::telemetry::TestOtelLogger;
use super::{failure, TestResult, CONCURRENT_UPDATE_BURST, EVENT_FLUSH_TIMEOUT, SYNC_TIMEOUT};

#[derive(Debug)]
pub(super) struct ScenarioReport {
    pub(super) updates: usize,
    pub(super) evaluations: usize,
    pub(super) max_latency: Duration,
    pub(super) rollout_on: usize,
    pub(super) rollout_off: usize,
    pub(super) final_sync_latency: Duration,
    pub(super) automatic_event_flush_latency: Duration,
    pub(super) explicit_events: usize,
    pub(super) event_flush_latency: Duration,
    pub(super) otel_events: usize,
    pub(super) otel_errors: usize,
}

pub(super) async fn run_application(
    config: &TestConfig,
    api: &RestApi,
    flag: &TestFlag,
) -> TestResult<ScenarioReport> {
    let otel_logger = TestOtelLogger::default();
    let otel_inspector = otel_logger.clone();
    let observer = OpenTelemetryEvaluationObserver::new(otel_logger);
    let options = FbOptions::builder(config.environment_secret.clone())
        .streaming_url(config.streaming_url.clone())
        .event_url(config.event_url.clone())
        .disable_events(config.disable_events, false)
        .evaluation_observer(observer)
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

    verify_tracking_is_disabled(&provider, flag)?;
    let featbit = provider.client().clone();
    let analytics_client = featbit.clone();
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

    let scenario = async {
        let mut report = run_scenario(config, api, flag, &http, &evaluation_url).await?;
        if !config.disable_events {
            let flush_client = analytics_client.clone();
            let flush_started = Instant::now();
            let delivered = tokio::task::spawn_blocking(move || {
                flush_client.flush_and_wait(EVENT_FLUSH_TIMEOUT)
            })
            .await?;
            report.automatic_event_flush_latency = flush_started.elapsed();
            if !delivered {
                return Err(failure(
                    "automatic evaluation events were not delivered to FeatBit",
                ));
            }
        }
        let event_probe = run_explicit_event_probe(config, flag).await?;
        api.archive_flag(flag).await?;
        wait_for_fallback(&http, &evaluation_url, &Probe::user("ordinary-user")).await?;
        let otel = otel_inspector.validate(flag, report.evaluations)?;
        report.explicit_events = event_probe.events;
        report.event_flush_latency = event_probe.flush_latency;
        report.otel_events = otel.events;
        report.otel_errors = otel.errors;
        TestResult::Ok(report)
    }
    .await;
    let application_stop = application.stop().await;
    OpenFeature::singleton_mut().await.shutdown().await;
    let client_close = tokio::task::spawn_blocking(move || featbit.close()).await;

    let report = scenario?;
    application_stop?;
    client_close?;
    Ok(report)
}

fn verify_tracking_is_disabled(provider: &FeatBitProvider, flag: &TestFlag) -> TestResult<()> {
    let user = FbUser::builder("disabled-tracking-user")
        .name("Disabled tracking probe")
        .build();
    let detail = provider
        .client()
        .bool_variation_detail(&flag.key, &user, false);
    let event = detail
        .evaluation_event
        .as_ref()
        .ok_or_else(|| failure("disabled tracking probe could not evaluate the live flag"))?;
    validate_direct_detail(detail.value, &detail.variation_id, flag)?;
    if provider.client().track_eval_event(&user, event) {
        return Err(failure(
            "track_eval_event was accepted even though allow_track is false",
        ));
    }
    if provider
        .client()
        .track_metric_event(&user, "disabled-tracking-metric", 1.0)
    {
        return Err(failure(
            "track_metric_event was accepted even though allow_track is false",
        ));
    }

    let context = EvaluationContext::default().with_targeting_key("disabled-tracking-user");
    if provider
        .track_eval_event_for_flag(&context, &flag.key)
        .map_err(|error| {
            failure(format!(
                "disabled OpenFeature evaluation tracking failed with {}",
                error.code
            ))
        })?
    {
        return Err(failure(
            "OpenFeature evaluation tracking was accepted while tracking is disabled",
        ));
    }
    if provider
        .track_metric_event(&context, "disabled-provider-metric", 1.0)
        .map_err(|error| {
            failure(format!(
                "disabled OpenFeature metric tracking failed with {}",
                error.code
            ))
        })?
    {
        return Err(failure(
            "OpenFeature metric tracking was accepted while tracking is disabled",
        ));
    }
    Ok(())
}

fn validate_direct_detail(value: bool, variation_id: &str, flag: &TestFlag) -> TestResult<()> {
    let expected_variation = if value {
        flag.on_variation.as_str()
    } else {
        flag.off_variation.as_str()
    };
    if variation_id != expected_variation {
        return Err(failure("direct detail returned a value/variation mismatch"));
    }
    Ok(())
}

struct EventProbeReport {
    events: usize,
    flush_latency: Duration,
}

async fn run_explicit_event_probe(
    config: &TestConfig,
    flag: &TestFlag,
) -> TestResult<EventProbeReport> {
    let options = FbOptions::builder(config.environment_secret.clone())
        .streaming_url(config.streaming_url.clone())
        .event_url(config.event_url.clone())
        .disable_events(true, true)
        .auto_flush_interval(Duration::from_secs(30))
        .flush_timeout(EVENT_FLUSH_TIMEOUT)
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
            "explicit event probe did not initialize from the cloud",
        ));
    }

    let result = async {
        let user = FbUser::builder("explicit-tracking-user")
            .name("Explicit tracking probe")
            .custom("testPhase", "explicit-events")
            .build();
        let detail = provider
            .client()
            .bool_variation_detail(&flag.key, &user, false);
        validate_direct_detail(detail.value, &detail.variation_id, flag)?;
        let event = detail
            .evaluation_event
            .as_ref()
            .ok_or_else(|| failure("successful detail did not retain an evaluation event"))?;
        if !provider.client().track_eval_event(&user, event) {
            return Err(failure("track_eval_event rejected the retained live event"));
        }
        if !provider
            .client()
            .track_metric_event(&user, "codex-rust-sdk-explicit-metric", 42.0)
        {
            return Err(failure("track_metric_event rejected the live metric"));
        }

        let context = EvaluationContext::default()
            .with_targeting_key("explicit-openfeature-user")
            .with_custom_field("name", "Explicit OpenFeature tracking probe");
        if !provider
            .track_eval_event_for_flag(&context, &flag.key)
            .map_err(|error| {
                failure(format!(
                    "OpenFeature evaluation tracking failed with {}",
                    error.code
                ))
            })?
        {
            return Err(failure(
                "OpenFeature tracking extension rejected the live evaluation event",
            ));
        }
        if !provider
            .track_metric_event(&context, "codex-rust-sdk-openfeature-metric", 7.0)
            .map_err(|error| {
                failure(format!(
                    "OpenFeature metric tracking failed with {}",
                    error.code
                ))
            })?
        {
            return Err(failure(
                "OpenFeature tracking extension rejected the live metric event",
            ));
        }

        let flush_client = provider.client().clone();
        let flush_started = Instant::now();
        let delivered =
            tokio::task::spawn_blocking(move || flush_client.flush_and_wait(EVENT_FLUSH_TIMEOUT))
                .await?;
        let flush_latency = flush_started.elapsed();
        if !delivered {
            return Err(failure(
                "explicit evaluation and metric events were not delivered to FeatBit",
            ));
        }
        Ok(EventProbeReport {
            events: 4,
            flush_latency,
        })
    }
    .await;

    let close_client = provider.client().clone();
    tokio::task::spawn_blocking(move || close_client.close()).await?;
    result
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

    Ok(ScenarioReport {
        updates: config.update_count + CONCURRENT_UPDATE_BURST + 6,
        evaluations: load.evaluations + rollout_evaluations,
        max_latency: load.max_latency,
        rollout_on,
        rollout_off,
        final_sync_latency,
        automatic_event_flush_latency: Duration::ZERO,
        explicit_events: 0,
        event_flush_latency: Duration::ZERO,
        otel_events: 0,
        otel_errors: 0,
    })
}

pub(super) async fn call_evaluation(
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
