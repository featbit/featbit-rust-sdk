use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use reqwest::Client as HttpClient;
use tokio::task::JoinSet;

use super::api::{RestApi, TestFlag};
use super::application::{EvaluationResponse, Probe};
use super::config::TestConfig;
use super::flag_config::configure_single_variation;
use super::scenario::call_evaluation;
use super::{failure, TestResult};

#[derive(Default)]
pub(super) struct LoadReport {
    pub(super) evaluations: usize,
    pub(super) max_latency: Duration,
}

pub(super) async fn run_update_load(
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

pub(super) fn validate_resolution(
    response: &EvaluationResponse,
    flag: &TestFlag,
) -> TestResult<()> {
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
