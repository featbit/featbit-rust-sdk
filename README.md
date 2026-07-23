# FeatBit RUST SDK (OpenFeature Compatible)

The FeatBit server-side SDK for Rust synchronizes feature flags in the background, evaluates them locally, and sends analytics events asynchronously. It uses Rust edition 2021 and supports Rust 1.95.0 or newer.

## Installation

```toml
[dependencies]
featbit-server-sdk = "0.1"

# The SDK uses the `log` facade. Your application chooses the logger.
env_logger = "0.11"
```

The package name uses hyphens in `Cargo.toml` and is imported as `featbit_server_sdk` in Rust code.

## Quick start

Create one `FbClient` per FeatBit environment and reuse it for the lifetime of the application:

```rust,no_run
use featbit_server_sdk::{FbClient, FbOptions, FbUser};

fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    env_logger::init();

    let options = FbOptions::builder(std::env::var("FEATBIT_ENV_SECRET")?)
        .streaming_url(std::env::var("FEATBIT_STREAMING_URL")?)
        .event_url(std::env::var("FEATBIT_EVENT_URL")?)
        .build()?;
    let client = FbClient::with_options(options);

    let user = FbUser::builder("user-123")
        .name("Ada")
        .custom("country", "CN")
        .build();
    let enabled = client.bool_variation("new-checkout", &user, false);

    if enabled {
        // Run the new code path.
    }

    client.close();
    Ok(())
}
```

Use `wss://` and `https://` endpoints in production. The default `ws://localhost:5100` and `http://localhost:5100` endpoints are intended for local development.

## FbClient best practices

- Create one client per FeatBit environment during application startup. `FbClient` is cheap to clone; every clone shares the same synchronized snapshot and background workers.
- Client construction can wait up to the configured `start_wait` for initial data. In async applications, construct it with `tokio::task::spawn_blocking` rather than blocking an async worker.
- Evaluation is synchronous, local, and performs no network I/O. Value-only methods always return the supplied fallback on failure.
- Use detail methods when you need the variation ID, reason, or an immutable event for deferred tracking.
- Treat both `Ready` and `Stale` as able to serve flags. `Stale` means the last synchronized snapshot remains available while the connection recovers.
- Call `close` after the application has drained in-flight work. Shutdown is bounded and idempotent; `flush_and_wait` is available when delivery confirmation is required.

| Flag type | Value-only method | Method with diagnostics |
| --- | --- | --- |
| Boolean | `bool_variation` | `bool_variation_detail` |
| Integer | `int_variation` | `int_variation_detail` |
| Float | `float_variation` | `float_variation_detail` |
| String | `string_variation` | `string_variation_detail` |
| JSON | `json_variation` | `json_variation_detail` |

### Deferred evaluation and metric events

For application-controlled exposure tracking, disable automatic evaluation events but keep explicit tracking enabled:

```rust,no_run
use featbit_server_sdk::FbOptions;

# fn example() -> Result<(), featbit_server_sdk::ConfigError> {
let options = FbOptions::builder("environment-secret")
    .disable_events(
        true, // disable automatic evaluation events
        true, // allow explicit evaluation and metric tracking
    )
    .build()?;
# let _ = options;
# Ok(())
# }
```

A successful detail result retains the exact evaluation event. Track it only after the result becomes a real exposure:

```rust,no_run
use featbit_server_sdk::{FbClient, FbUser};

# fn user_was_exposed() -> bool { true }
# fn example(client: &FbClient, user: &FbUser) {
let detail = client.bool_variation_detail("new-checkout", user, false);
if user_was_exposed() {
    if let Some(event) = detail.evaluation_event.as_ref() {
        let _accepted = client.track_eval_event(user, event);
    }
}

let _accepted = client.track_metric_event(user, "checkout-completed", 1.0);
# }
```

`disable_events(disable, allow_track)` controls event behavior:

| Configuration | Automatic evaluation events | Explicit evaluation/metric tracking |
| --- | --- | --- |
| `disable_events(false, true)` (SDK default) | enabled | allowed |
| `disable_events(false, false)` | enabled | rejected |
| `disable_events(true, true)` (recommended deferred mode) | disabled | allowed |
| `disable_events(true, false)` | disabled | rejected; event processor is not started |

## FbClient examples

### Console

[`examples/fbclient_console.rs`](examples/fbclient_console.rs) evaluates a boolean flag with `FbClient`, prints the variation and reason, and closes the client.

```text
cargo run --example fbclient_console
```

### Axum

[`examples/fbclient_axum.rs`](examples/fbclient_axum.rs) creates one client during startup, shares clones through typed Axum state, exposes readiness, and closes after in-flight requests drain.

```text
cargo run --example fbclient_axum
```

The Axum example listens on `127.0.0.1:3000` by default. Override it with `AXUM_BIND_ADDRESS` and configure logging with `RUST_LOG`.

```bash
curl --request POST http://127.0.0.1:3000/api/flags/new-checkout/evaluate \
  --header 'content-type: application/json' \
  --data '{"targetingKey":"user-123","name":"Ada","attributes":{"country":"CN"},"defaultValue":false}'
```

Readiness is available at `GET /health/ready`.

## OpenFeature

`FeatBitProvider` implements the official `open_feature::provider::FeatureProvider` interface. Applications evaluate flags through an OpenFeature `Client`; FeatBit continues to own synchronization, local evaluation, event delivery, and lifecycle.

Add the OpenFeature and Tokio dependencies:

```toml
[dependencies]
featbit-server-sdk = "0.1"
open-feature = "0.3"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

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
    let provider =
        tokio::task::spawn_blocking(move || FeatBitProvider::new(options)).await?;
    let featbit = provider.client().clone();

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
    tokio::task::spawn_blocking(move || featbit.close()).await?;
    Ok(())
}
```

OpenFeature requires a non-empty targeting key. Provider failures use standard codes such as `ProviderNotReady`, `FlagNotFound`, `TargetingKeyMissing`, `TypeMismatch`, and `ParseError`. Use detail methods when the reason and selected variant are needed.

### OpenFeature console example

[`examples/openfeature_console.rs`](examples/openfeature_console.rs) registers `FeatBitProvider`, creates an OpenFeature client, evaluates one boolean flag, and shuts down cleanly.

```text
cargo run --example openfeature_console
```

### OpenFeature Axum example

[`examples/openfeature_axum.rs`](examples/openfeature_axum.rs) shares an OpenFeature `Client` in typed Axum state and retains `FeatBitProvider` for readiness and shutdown operations. It exposes the same HTTP contract as the `FbClient` Axum example.

```text
cargo run --example openfeature_axum
```

OpenFeature 0.3 does not standardize custom metrics, delivery-aware flush, readiness details, or explicit bounded close. Keep a provider handle when these FeatBit-specific operations are required:

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

`FeatBitProvider::track_eval_event_for_flag` re-evaluates the current snapshot because OpenFeature details cannot carry the immutable FeatBit event. Call it promptly after resolution when explicit evaluation tracking is needed.

## OpenTelemetry evaluation events

The optional `featbit-server-sdk-opentelemetry` crate emits `feature_flag.evaluation` through an application-owned OpenTelemetry logger. It excludes context identifiers and raw variation values by default and remains independent of FeatBit analytics delivery.

See [`integrations/opentelemetry/README.md`](integrations/opentelemetry/README.md) for setup and privacy options.

## Offline bootstrap

Offline mode performs no network I/O and initializes `FbClient` from a FeatBit full data-sync envelope:

```rust,no_run
use featbit_server_sdk::{FbClient, FbOptions, FeatBitProvider};

# fn example() -> Result<(), Box<dyn std::error::Error>> {
let bootstrap = std::fs::read_to_string("featbit-bootstrap.json")?;
let options = FbOptions::builder("offline-placeholder")
    .offline(true)
    .disable_events(true, false)
    .bootstrap_json(bootstrap)
    .build()?;
let client = FbClient::with_options(options);

// Use the client directly, or expose the same snapshot through OpenFeature.
let provider = FeatBitProvider::from_client(client.clone());
# let _ = provider;
# Ok(())
# }
```

Bootstrap JSON is restricted to offline mode so static data cannot compete with the live synchronizer.

## Configuration and lifecycle

Defaults match the FeatBit .NET server SDK where practical: a 5-second initial wait, a 3-second connection timeout, 15-second keepalive, a 10,000-event queue, 50 events per request, and a 5-second auto-flush interval.

`FbClient` reports `NotReady`, `Ready`, `Stale`, or `Closed`; `FeatBitProvider` maps those states into OpenFeature provider status. Network failures are logged and retried by background workers. Event queues are bounded, so analytics can be dropped under sustained overload rather than delaying application requests.

The SDK uses the `log` facade and never installs a logger. Environment secrets are redacted from SDK `Debug` output and are never written to SDK logs.

## Development

The long-lived design and compatibility rules are in [`AGENTS.md`](AGENTS.md). Before submitting a change, run:

```text
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
cargo test --workspace --doc
```

The bounded FeatBit Cloud stress project is documented in [`examples/test/README.md`](examples/test/README.md).
The crates.io approval, versioning, prerelease, and recovery workflow is documented in [`RELEASING.md`](RELEASING.md).

## License

Licensed under the Apache License, Version 2.0. See [`LICENSE`](LICENSE).
