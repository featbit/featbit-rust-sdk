# FeatBit Rust SDK OpenTelemetry adapter

This crate emits OpenTelemetry `feature_flag.evaluation` semantic events for evaluations performed by
`featbit-server-sdk`. The application supplies and owns the OpenTelemetry logger, provider, OTLP
exporter, and shutdown lifecycle.

Configure the logger provider with a batch log processor. A simple/synchronous exporter may perform
network I/O on the application's flag-evaluation thread and is not suitable for production use.

```rust,ignore
use featbit_server_sdk::FbOptions;
use featbit_server_sdk_opentelemetry::OpenTelemetryEvaluationObserver;
use opentelemetry::logs::LoggerProvider as _;

let logger = logger_provider.logger("featbit-server-sdk");
let observer = OpenTelemetryEvaluationObserver::new(logger);
let options = FbOptions::builder("environment-secret")
    .evaluation_observer(observer)
    .build()?;
```

The adapter exports the flag key, provider name, variation ID, normalized reason, experiment-eligibility
boolean, and standardized error type. Targeting keys and raw variation values are excluded by default;
enable them only after making an explicit privacy and cardinality decision:

```rust,ignore
let observer = OpenTelemetryEvaluationObserver::new(logger)
    .with_context_id(true)
    .with_value(true);
```

This is an observability path only. FeatBit evaluation and metric events remain owned by the core SDK
and continue to use the FeatBit event endpoint.
