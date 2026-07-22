//! Runs a bounded, destructive `FeatBit` Cloud synchronization test through Axum and
//! `OpenFeature`.
//!
//! This example creates and archives one uniquely named feature flag in the explicitly selected
//! environment. It refuses to mutate remote state unless `FEATBIT_TEST_ALLOW_REMOTE_MUTATIONS`
//! exactly equals `FEATBIT_ENVIRONMENT_ID`. Credentials are read from environment variables and
//! are never persisted or printed.

use std::error::Error;
use std::io;
use std::time::Duration;

use api::{RestApi, TestFlag};
use config::TestConfig;
use scenario::run_application;

#[path = "cloud_axum_extreme/api.rs"]
mod api;
#[path = "cloud_axum_extreme/application.rs"]
mod application;
#[path = "cloud_axum_extreme/config.rs"]
mod config;
#[path = "cloud_axum_extreme/flag_config.rs"]
mod flag_config;
#[path = "cloud_axum_extreme/load.rs"]
mod load;
#[path = "cloud_axum_extreme/scenario.rs"]
mod scenario;
#[path = "cloud_axum_extreme/telemetry.rs"]
mod telemetry;

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
const EVENT_FLUSH_TIMEOUT: Duration = Duration::from_secs(10);
const OTEL_EVENT_NAME: &str = "feature_flag.evaluation";
const OTEL_TARGET: &str = "featbit-server-sdk";

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
        "cloud Axum/OpenFeature test passed: flag={}, updates={}, evaluations={}, maxLatencyMs={}, rolloutOn={}, rolloutOff={}, finalSyncMs={}, automaticEventFlushMs={}, explicitEvents={}, explicitEventFlushMs={}, otelEvents={}, otelErrors={}",
        flag.key,
        report.updates,
        report.evaluations,
        report.max_latency.as_millis(),
        report.rollout_on,
        report.rollout_off,
        report.final_sync_latency.as_millis(),
        report.automatic_event_flush_latency.as_millis(),
        report.explicit_events,
        report.event_flush_latency.as_millis(),
        report.otel_events,
        report.otel_errors
    );
    Ok(())
}

fn failure(message: impl Into<String>) -> AnyError {
    io::Error::other(message.into()).into()
}
