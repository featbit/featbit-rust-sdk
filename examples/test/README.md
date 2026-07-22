# FeatBit Cloud Axum/OpenFeature stress test

`cloud_axum_extreme` is an explicitly authorized, bounded end-to-end test. It creates one uniquely
prefixed boolean flag in the selected environment, exercises it through a local Axum server and
the official OpenFeature client, and archives the flag when the scenario finishes.

The test covers:

- initial WebSocket synchronization and live patch convergence;
- direct targeting, attribute rules, and deterministic 50/50 rollout;
- concurrent Axum requests whose handlers resolve exclusively through OpenFeature;
- configuration updates while evaluations are in flight;
- a concurrent update burst followed by REST-to-WebSocket final-state convergence;
- recovery from same-millisecond, equal-version patch collisions through an authoritative full
  data sync;
- `disable_events(..., false)` rejection of direct and OpenFeature explicit tracking;
- deferred `disable_events(true, true)` delivery through `track_eval_event`,
  `track_metric_event`, and both OpenFeature provider tracking extensions;
- delivery-aware explicit flush against the real FeatBit event endpoint;
- OpenTelemetry `feature_flag.evaluation` success/error events, required semantic attributes, and
  default exclusion of context IDs and raw variation values;
- archived-flag tombstone propagation.

## Safety

The REST client only calls the supplied project/environment scope and refuses to update keys that
do not start with `codex-rust-sdk-p0p1-`. Remote writes are disabled unless
`FEATBIT_TEST_ALLOW_REMOTE_MUTATIONS` exactly matches `FEATBIT_ENVIRONMENT_ID`. Credentials are
read from environment variables and are not persisted or printed.

Use a dedicated non-production environment. The default high-concurrency phase disables automatic
analytics so local evaluation load does not become event-ingestion load. Every run still sends four
bounded explicit test events (two evaluation and two metric events) to verify the event path.

## Run

Set these variables without committing their values:

```text
FEATBIT_STREAMING_URL=wss://your-evaluation-host
FEATBIT_EVENT_URL=https://your-evaluation-host
FEATBIT_API_URL=https://your-api-host
FEATBIT_ENV_SECRET=...
FEATBIT_ACCESS_TOKEN=api-...
FEATBIT_PROJECT_ID=...
FEATBIT_ENVIRONMENT_ID=...
FEATBIT_TEST_ALLOW_REMOTE_MUTATIONS=<same value as FEATBIT_ENVIRONMENT_ID>
```

Then run:

```text
cargo run --example cloud_axum_extreme
```

Optional bounds are `FEATBIT_TEST_EVALUATION_WORKERS` (1–64),
`FEATBIT_TEST_REQUESTS_PER_WORKER` (1–5,000), and `FEATBIT_TEST_UPDATE_COUNT` (1–250). Set
`FEATBIT_TEST_DISABLE_EVENTS=false` only for a deliberately small run; this enables automatic
evaluation analytics for the stress provider while explicit tracking remains disabled there.
Analytics-enabled runs are rejected when their planned rollout and load phases can exceed 2,000
evaluations. The separate four-event deferred-tracking probe always runs.
