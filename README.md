# FeatBit Rust Server SDK

The FeatBit server-side SDK and [OpenFeature](https://openfeature.dev/) provider for Rust. It synchronizes feature-flag data over WebSocket, evaluates flags locally, and sends analytics events asynchronously.

The crate uses Rust edition 2021 and supports Rust 1.95.0 or newer. Register one `FeatBitProvider` per FeatBit environment and evaluate flags through an OpenFeature client shared for the process lifetime.

## Capabilities

- validated, immutable `FbOptions` configuration;
- full and incremental WebSocket data synchronization with reconnect and cached snapshots;
- local targeting, segment, rule, and deterministic percentage-rollout evaluation;
- bounded, non-blocking evaluation and custom-event processing;
- boolean, integer, float, string, and JSON/struct values;
- direct implementation of the OpenFeature Rust `FeatureProvider` trait;
- deferred/manual evaluation-event tracking and custom metric events;
- an optional OpenTelemetry semantic evaluation-event adapter;
- standard Rust `log` facade integration;
- thread-safe clients, bounded shutdown, and fallback-returning direct APIs.

## Installation

```toml
[dependencies]
featbit-server-sdk = "0.1"
open-feature = "0.3"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }

# Choose the logger used by your application. This one is only an example.
env_logger = "0.11"
```

The package name uses hyphens in `Cargo.toml` and is imported as `featbit_server_sdk` in Rust code.

## OpenFeature quick start

`FeatBitProvider` implements `open_feature::provider::FeatureProvider` directly; there is no second evaluation layer. OpenFeature is the recommended application-facing API.

```rust,no_run
use featbit_server_sdk::{FbOptions, FeatBitProvider};
use open_feature::{EvaluationContext, OpenFeature};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    env_logger::init();

    let options = FbOptions::builder(std::env::var("FEATBIT_ENV_SECRET")?)
        .streaming_url(std::env::var("FEATBIT_STREAMING_URL")?)
        .event_url(std::env::var("FEATBIT_EVENT_URL")?)
        .build()?;

    // Initial synchronization may wait for a bounded period, so initialize outside Tokio's
    // asynchronous worker threads.
    let provider =
        tokio::task::spawn_blocking(move || FeatBitProvider::new(options)).await?;
    let extensions = provider.clone();

    let client = {
        let mut api = OpenFeature::singleton_mut().await;
        api.set_provider(provider).await;
        api.create_client()
    };

    let context = EvaluationContext::default()
        .with_targeting_key("user-123")
        .with_custom_field("name", "Ada")
        .with_custom_field("country", "CN");
    let enabled = client
        .get_bool_value("new-checkout", Some(&context), None)
        .await
        .unwrap_or(false);

    if enabled {
        // Run the new code path.
    }

    OpenFeature::singleton_mut().await.shutdown().await;
    tokio::task::spawn_blocking(move || extensions.client().close()).await?;
    Ok(())
}
```

OpenFeature requires a non-empty targeting key. Provider failures use standard OpenFeature error codes such as `ProviderNotReady`, `FlagNotFound`, `TargetingKeyMissing`, `TypeMismatch`, and `ParseError`. Use `get_bool_details`, `get_int_details`, `get_float_details`, `get_string_details`, or `get_struct_details` when the reason and selected variant are needed.

## FeatBit-specific extensions

All flag resolution should use the OpenFeature client. OpenFeature 0.3 does not standardize custom
metric events, delivery-aware flush, readiness details, or explicit bounded close, so keep a clone of
`FeatBitProvider` only when the application needs those FeatBit-specific extensions:

```rust,no_run
use std::time::Duration;

use featbit_server_sdk::FeatBitProvider;
use open_feature::EvaluationContext;

# fn example(
#     provider: &FeatBitProvider,
#     context: &EvaluationContext,
# ) -> Result<(), open_feature::EvaluationError> {
let _accepted = provider.track_metric_event(context, "checkout-opened", 1.0)?;
let _delivered = provider
    .client()
    .flush_and_wait(Duration::from_secs(2));
# Ok(())
# }
```

The provider extensions use the same OpenFeature `EvaluationContext` mapping as flag resolution.
The direct variation methods remain available for compatibility and specialized integrations, but
README examples intentionally use OpenFeature for application-facing evaluation.

### Deferred evaluation and metric events

The recommended event configuration defers evaluation-event delivery until an OpenFeature
resolution becomes a real exposure. Disable automatic evaluation events, keep explicit tracking
available, and call the provider tracking extension only after exposure:

```rust,no_run
use std::time::Duration;

use featbit_server_sdk::{FbOptions, FeatBitProvider};
use open_feature::{EvaluationContext, OpenFeature};

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let options = FbOptions::builder("environment-secret")
    .disable_events(
        true, // disable automatic evaluation events for deferred exposure tracking
        true, // allow explicit evaluation and metric tracking
    )
    .build()?;
let provider = FeatBitProvider::new(options);
let extensions = provider.clone();
let client = {
    let mut api = OpenFeature::singleton_mut().await;
    api.set_provider(provider).await;
    api.create_client()
};
let context = EvaluationContext::default().with_targeting_key("user-123");

let detail = client
    .get_bool_details("new-checkout", Some(&context), None)
    .await?;
if user_was_exposed_to_the_result() {
    let _accepted = extensions
        .track_eval_event_for_flag(&context, &detail.flag_key)?;
}

let _accepted = extensions
    .track_metric_event(&context, "checkout-completed", 1.0)?;
let _delivered = extensions
    .client()
    .flush_and_wait(Duration::from_secs(2));
OpenFeature::singleton_mut().await.shutdown().await;
# extensions.client().close();
# Ok(())
# }
# fn user_was_exposed_to_the_result() -> bool { true }
```

`track_eval_event_for_flag` re-evaluates against the current snapshot because OpenFeature 0.3 details
cannot carry a provider-specific event token. Call it promptly after resolution; an intervening flag
update can change the variation recorded by the event.

`disable_events(disable, allow_track)` is the single event-mode setting:

| Configuration | Automatic evaluation events | Explicit evaluation/metric tracking |
| --- | --- | --- |
| `disable_events(false, true)` (SDK default) | enabled | allowed |
| `disable_events(false, false)` | enabled | rejected |
| `disable_events(true, true)` (recommended deferred mode) | disabled | allowed |
| `disable_events(true, false)` | disabled | rejected; event processor is not started |

## OpenTelemetry evaluation events

The separate `featbit-server-sdk-opentelemetry` crate implements `EvaluationObserver` without adding
OpenTelemetry dependencies to the core SDK. It emits the semantic event `feature_flag.evaluation`
through an application-owned OpenTelemetry logger. Configure that logger with a batch processor so
exporter I/O never runs on an evaluation thread:

```toml
[dependencies]
featbit-server-sdk = "0.1"
featbit-server-sdk-opentelemetry = "0.1"
opentelemetry = { version = "0.32", features = ["logs", "trace"] }
```

```rust,ignore
use featbit_server_sdk::{FbOptions, FeatBitProvider};
use featbit_server_sdk_opentelemetry::OpenTelemetryEvaluationObserver;
use open_feature::OpenFeature;
use opentelemetry::logs::LoggerProvider as _;

// The application owns and configures this provider and its OTLP exporter.
let logger = logger_provider.logger("featbit-server-sdk");
let observer = OpenTelemetryEvaluationObserver::new(logger);
let options = FbOptions::builder("environment-secret")
    .evaluation_observer(observer)
    .build()?;
let provider = FeatBitProvider::new(options);
OpenFeature::singleton_mut().await.set_provider(provider).await;
```

Context identifiers and raw variation values are excluded by default. They can be enabled explicitly
with `with_context_id(true)` and `with_value(true)`. The adapter remains active independently of
FeatBit analytics settings and never invokes `track_eval_event` or `track_metric_event`, so it cannot
duplicate events sent to the FeatBit server.

## Axum web application

[`examples/axum.rs`](examples/axum.rs) shows the recommended Axum integration pattern:

- construct and register one `FeatBitProvider` during application startup;
- share an OpenFeature `Client` in typed Axum `State`;
- build an OpenFeature `EvaluationContext` from each request and call `get_bool_details` inside the handler;
- keep the FeatBit provider handle only for readiness and lifecycle extensions;
- expose readiness without treating a reconnecting client as unable to serve cached flags;
- drain in-flight HTTP requests, then flush and close the SDK during graceful shutdown;
- bridge the SDK's `log` records into `tracing` and add Tower HTTP request tracing.

Set `FEATBIT_ENV_SECRET`, `FEATBIT_STREAMING_URL`, and `FEATBIT_EVENT_URL`, then run:

```text
cargo run --example axum
```

The example listens on `127.0.0.1:3000` by default. Override it with `AXUM_BIND_ADDRESS` and configure logging with `RUST_LOG`. Evaluate a boolean flag with:

```bash
curl --request POST http://127.0.0.1:3000/api/flags/new-checkout/evaluate \
  --header 'content-type: application/json' \
  --data '{"targetingKey":"user-123","name":"Ada","attributes":{"country":"CN"},"defaultValue":false}'
```

## Offline bootstrap

Offline mode performs no network I/O and can initialize from a FeatBit full data-sync envelope:

```rust,no_run
use featbit_server_sdk::{FbOptions, FeatBitProvider};
use open_feature::OpenFeature;

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let bootstrap = std::fs::read_to_string("featbit-bootstrap.json")?;
let options = FbOptions::builder("offline-placeholder")
    .offline(true)
    .disable_events(true, false)
    .bootstrap_json(bootstrap)
    .build()?;
let provider = FeatBitProvider::new(options);
OpenFeature::singleton_mut().await.set_provider(provider).await;
# Ok::<(), Box<dyn std::error::Error>>(())
# }
```

Bootstrap JSON is deliberately restricted to offline mode so static data cannot silently compete with the live synchronizer.

## Configuration and lifecycle

Defaults match the FeatBit .NET server SDK where practical: local endpoints, a 5-second initial wait, a 3-second connection timeout, 15-second keepalive, a 10,000-event queue, 50 events per request, and a 5-second auto-flush interval. Configure production deployments with `wss://` and `https://` endpoints. Endpoint base paths are supported, but credentials, query parameters, and fragments are rejected because the SDK supplies its own authentication and protocol query parameters.

The OpenFeature provider reports `NotReady`, `Ready`, `Stale`, or `Error`; `provider.client().status()` exposes the more specific FeatBit lifecycle state when operational health checks need it. Evaluation remains local and lock-free on the read path. Network failures are logged and retried in background workers. Event queues are bounded, so analytics can be dropped under sustained overload rather than delaying application requests.

The SDK uses the `log` facade and never installs a logger. Configure `env_logger`, `tracing-log`, or another logger in the application. Environment secrets are redacted from SDK `Debug` output and are never written to SDK logs.

## Development

The long-lived design and compatibility rules are in [`AGENTS.md`](AGENTS.md). Before submitting a change, run:

```text
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
cargo test --workspace --doc
```

The explicitly authorized, bounded FeatBit Cloud stress project is documented in
[`examples/test/README.md`](examples/test/README.md). It exercises live updates through a local
Axum application and the OpenFeature API without storing credentials in the repository.

## License

Licensed under the Apache License, Version 2.0. See [`LICENSE`](LICENSE).
