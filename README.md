# FeatBit Server-Side SDK for Rust

## Introduction

This is the Rust Server-Side SDK for the 100% open-source feature flag management platform
[FeatBit](https://github.com/featbit/featbit).

The SDK is designed for long-lived, multi-user systems such as web servers and backend
applications. Create one `FbClient` per FeatBit environment and reuse it for the lifetime of the
process. It is not intended for untrusted client-side, desktop, mobile, or embedded applications,
where an environment secret could be exposed.

OpenFeature support lives in the separate
[FeatBit OpenFeature Provider for Rust](https://github.com/featbit/openfeature-provider-rust-server)
repository.

When upgrading from `0.1.0-beta.1`, replace the core crate's `FeatBitProvider` import and
`open-feature` dependency with `featbit-openfeature-provider`. Direct `FbClient` APIs remain in this
repository.

## Data Synchronization

The SDK keeps feature flags and segments synchronized over WebSocket and evaluates them from an
immutable in-memory snapshot. Flag changes are pushed to the client; interrupted connections
reconnect automatically and request updates from the last known data version.

Evaluation is synchronous, local, and performs no network I/O. A previously initialized client
continues to evaluate its last snapshot with `ClientStatus::Stale` while the connection recovers.
See [Offline Mode](#offline-mode) when the application must use a static bootstrap without network
access.

## Get Started

### Installation

Add the current SDK version and a logger implementation to `Cargo.toml`:

```toml
[dependencies]
featbit-server-sdk = "0.1.0-beta.2"
env_logger = "0.11"
```

The Cargo package uses hyphens and is imported as `featbit_server_sdk` in Rust code.

### Prerequisite

Before using the SDK, obtain the environment secret and SDK URLs:

- [How to get the environment secret](https://docs.featbit.co/sdk/faq#how-to-get-the-environment-secret)
- [How to get the SDK URLs](https://docs.featbit.co/sdk/faq#how-to-get-the-sdk-urls)

Use `wss://` and `https://` endpoints in production. The default `ws://localhost:5100` and
`http://localhost:5100` endpoints are intended for local development.

### Quick Start

```rust,no_run
use featbit_server_sdk::{FbClient, FbOptions, FbUser};

fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    env_logger::init();

    let options = FbOptions::builder(std::env::var("FEATBIT_ENV_SECRET")?)
        .streaming_url(std::env::var("FEATBIT_STREAMING_URL")?)
        .event_url(std::env::var("FEATBIT_EVENT_URL")?)
        .build()?;
    let client = FbClient::with_options(options);

    if !client.initialized() {
        eprintln!("FbClient is not initialized; variation calls use fallbacks");
    }

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

### Examples

- [Console application](examples/fbclient_console.rs):
  `cargo run --example fbclient_console`
- [Axum application](examples/fbclient_axum.rs):
  `cargo run --example fbclient_axum`

The Axum example creates one client during startup, shares cloneable handles through application
state, exposes `GET /health/ready`, drains in-flight requests, and then closes the SDK.

## SDK

### FbClient

`FbClient` is the heart of the SDK. It owns data synchronization, local evaluation, analytics
delivery, and lifecycle state. Applications should instantiate a single client for each FeatBit
environment.

Cloning an `FbClient` is cheap: every clone shares the same snapshot and background workers.

#### Using Default Options

Defaults connect to a local FeatBit evaluation server:

```rust,no_run
use featbit_server_sdk::FbClient;

# fn example() -> Result<(), featbit_server_sdk::ConfigError> {
let client = FbClient::new("environment-secret")?;
# client.close();
# Ok(())
# }
```

#### Using Custom Options

```rust,no_run
use std::time::Duration;

use featbit_server_sdk::{FbClient, FbOptions};

# fn example() -> Result<(), featbit_server_sdk::ConfigError> {
let options = FbOptions::builder("environment-secret")
    .streaming_url("wss://app-eval.featbit.co")
    .event_url("https://app-eval.featbit.co")
    .start_wait(Duration::from_secs(3))
    .build()?;
let client = FbClient::with_options(options);
# client.close();
# Ok(())
# }
```

Construction can wait up to `start_wait` for initial data. In an async application, construct the
client with `tokio::task::spawn_blocking` so the bounded startup wait does not block an async worker.

#### Logging

The SDK uses Rust's standard [`log`](https://docs.rs/log) facade and never installs or replaces a
global logger. The application chooses the implementation:

```rust,no_run
fn main() {
    env_logger::init();
    // Construct FbClient after the logger is installed.
}
```

Set `RUST_LOG=featbit_server_sdk=debug` for lifecycle and delivery diagnostics. Environment secrets,
authorization headers, and complete user/event bodies are not logged.

### FbUser

`FbUser` describes the subject of an evaluation. `key` is mandatory and must uniquely identify the
user. `name` and custom string attributes can be referenced by targeting rules and included in
FeatBit analytics:

```rust
use featbit_server_sdk::FbUser;

let bob = FbUser::builder("a-unique-user-key")
    .name("Bob")
    .custom("age", "15")
    .custom("country", "FR")
    .build();

assert_eq!(bob.key(), "a-unique-user-key");
```

### Evaluating Flags

The SDK calculates flag values locally from the latest consistent snapshot. Each type has a
value-only method and a detail method:

| Flag type | Value-only method | Method with diagnostics |
| --- | --- | --- |
| Boolean | `bool_variation` | `bool_variation_detail` |
| Integer | `int_variation` | `int_variation_detail` |
| Float | `float_variation` | `float_variation_detail` |
| String | `string_variation` | `string_variation_detail` |
| JSON | `json_variation` | `json_variation_detail` |

Value-only methods always return the supplied fallback when the client is not ready, the flag does
not exist, remote data is malformed, the context is invalid, or the variation has the wrong type.
Application-facing evaluation methods do not panic.

```rust,no_run
use featbit_server_sdk::{FbClient, FbUser};

# fn example(client: &FbClient, user: &FbUser) {
let enabled = client.bool_variation("new-checkout", user, false);
let detail = client.bool_variation_detail("new-checkout", user, false);

println!(
    "value={}, variation={}, reason={}",
    detail.value, detail.variation_id, detail.reason
);
# let _ = enabled;
# }
```

`all_variations` returns a sorted, inspection-only snapshot of every known flag and does not emit
evaluation analytics.

### Status and Lifecycle

`FbClient::status` returns:

| Status | Meaning |
| --- | --- |
| `NotReady` | No valid data set has been received yet |
| `Ready` | The local snapshot is synchronized |
| `Stale` | The last snapshot is usable while synchronization recovers |
| `Closed` | The client was closed or synchronization stopped unrecoverably |

`flush` requests a non-blocking analytics flush. `flush_and_wait` waits up to a caller-provided
timeout and reports whether covered events were delivered. `close` is bounded and idempotent; call
it after the application has drained in-flight work.

### Offline Mode

Offline mode performs no network I/O. An optional FeatBit full data-sync envelope can initialize
the local snapshot:

```rust,no_run
use featbit_server_sdk::{FbClient, FbOptions};

# fn example() -> Result<(), Box<dyn std::error::Error>> {
let bootstrap = std::fs::read_to_string("featbit-bootstrap.json")?;
let options = FbOptions::builder("offline-placeholder")
    .offline(true)
    .disable_events(true, false)
    .bootstrap_json(bootstrap)
    .build()?;
let client = FbClient::with_options(options);
# client.close();
# Ok(())
# }
```

Bootstrap JSON is accepted only in offline mode so static data cannot compete with the live
synchronizer. An empty offline store is operational; unknown flags resolve to fallbacks.

### Disable Events Collection

`disable_events(disable, allow_track)` controls automatic evaluation analytics and explicit
tracking independently:

| Configuration | Automatic evaluation events | Explicit evaluation/metric tracking |
| --- | --- | --- |
| `disable_events(false, true)` (default) | enabled | allowed |
| `disable_events(false, false)` | enabled | rejected |
| `disable_events(true, true)` | disabled | allowed |
| `disable_events(true, false)` | disabled | rejected; no event worker |

For application-controlled exposure tracking, `disable_events(true, true)` is recommended.

### Experiments (A/B/n Testing)

Successful detail evaluations retain an immutable event snapshot. This lets the application record
the exact variation only after a real exposure, even if the flag changes in the meantime:

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

Evaluation and tracking calls enqueue into a bounded queue without waiting for network I/O.
Analytics may be dropped under sustained overload rather than delaying application requests.

### Integration Adapters

`evaluate_raw`, `complete_raw_evaluation`, and `observe_evaluation_error` form the transport-neutral
adapter boundary. They expose FeatBit reason, flag metadata, and the immutable event snapshot without
depending on an external standard. Most applications should use the typed variation methods.

The separate [OpenFeature provider](https://github.com/featbit/openfeature-provider-rust-server)
uses this boundary and does not maintain a second evaluation engine.

## OpenTelemetry Evaluation Events

The optional [`featbit-server-sdk-opentelemetry`](integrations/opentelemetry/README.md) crate emits
`feature_flag.evaluation` through an application-owned OpenTelemetry logger. It excludes context
identifiers and raw variation values by default and remains independent of FeatBit analytics
delivery.

## Supported Rust Versions

The crate uses Rust edition 2021 and has a minimum supported Rust version (MSRV) of 1.95.0. CI tests
Rust 1.95, 1.96, and 1.97. Stable Rust only is supported.

## Getting Support

- Ask FeatBit usage questions in [FeatBit Slack](https://join.slack.com/t/featbit/shared_invite/zt-1ew5e2vbb-x6Apan1xZOaYMnFzqZkGNQ).
- Report SDK bugs or request features in
  [featbit-rust-sdk issues](https://github.com/featbit/featbit-rust-sdk/issues/new).

## See Also

- [FeatBit documentation](https://docs.featbit.co/)
- [FeatBit OpenFeature Provider for Rust](https://github.com/featbit/openfeature-provider-rust-server)
- [OpenTelemetry adapter](integrations/opentelemetry/README.md)

## Development

The architecture and compatibility requirements are documented in [AGENTS.md](AGENTS.md). Run the
full quality gate before submitting a change:

```text
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
cargo test --workspace --doc
```

The crates.io approval, versioning, prerelease, and recovery workflow is documented in
[RELEASING.md](RELEASING.md).

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE).
