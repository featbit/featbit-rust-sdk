# FeatBit Rust Server SDK Engineering Guide

This file defines the long-lived engineering constraints for this repository. It applies to every file in the repository unless a more specific `AGENTS.md` explicitly narrows a rule.

## Product intent

Build a production-grade, server-side FeatBit SDK for long-lived, multi-user services. The primary integration is an OpenFeature `FeatureProvider`; the crate may also expose an idiomatic FeatBit client for direct use. A process should normally create one provider/client per FeatBit environment and reuse it for the process lifetime.

The SDK must provide these capabilities as one coherent system:

- immutable, validated `FbOptions` user configuration;
- WebSocket-based full and patch data synchronization with automatic reconnect;
- a bounded, asynchronous event processor for evaluation and metric events;
- deterministic local feature-flag and segment evaluation;
- integration with the Rust `log` facade;
- thread-safe, low-latency evaluation and graceful, idempotent shutdown;
- direct conformance to the official OpenFeature Rust provider interface.

## Protocol authority and decision records

Before a major version or protocol change, verify observable behavior against the authoritative protocol and public-contract sources. The initial implementation recorded these source revisions on 2026-07-21:

- FeatBit .NET Server SDK, commit `974e2a7a557095b300e4e89da86df7d6fa894963`: <https://github.com/featbit/featbit-dotnet-sdk>
- OpenFeature Rust SDK 0.3.0, commit `abe65b862149347b7a08385c60f738eebafd444f`: <https://github.com/open-feature/rust-sdk>
- OpenFeature Rust getting-started guide: <https://openfeature.dev/docs/tutorials/getting-started/rust/>

FeatBit .NET defines the expected FeatBit behavior and wire format. OpenFeature defines the public provider contract. Rust API shape, runtime ownership, builder patterns, bounded event delivery, documentation, and quality gates are independent repository decisions defined by this guide and justified through tests and benchmarks.

Implement observable behavior and protocol semantics independently in idiomatic Rust. Record decisions here as requirements, rationale, and deterministic fixtures instead of depending on another SDK's internal implementation details.

## Rust compatibility

- Use Rust edition 2021.
- The minimum supported Rust version (MSRV) is 1.95.0.
- Support the latest stable Rust release and the two preceding minor releases. At project inception this is 1.95 through 1.97.
- Keep `package.rust-version` authoritative. Do not raise it incidentally through a dependency update. An MSRV increase requires a documented compatibility decision and changelog entry.
- Stable Rust only. Do not require nightly features.
- Put `#![forbid(unsafe_code)]` at the crate root. Unsafe code requires a separately reviewed architectural decision and must never be introduced as a local optimization.

## Architecture and design patterns

Use explicit component boundaries rather than a monolithic client:

```text
FbOptions / FbOptionsBuilder
            |
            v
     FbClient facade  <---->  FeatBitProvider (OpenFeature adapter)
       /     |     \
      v      v      v
data sync  evaluator  event processor
      |      |             |
      v      v             v
 atomic in-memory       HTTP event endpoint
 data snapshot
```

Apply these patterns consistently:

- **Builder + immutable configuration:** `FbOptionsBuilder` validates once and produces an immutable, cheaply clonable `FbOptions`. Never mutate an options value supplied by an application.
- **Facade:** `FbClient` owns lifecycle and offers safe direct evaluation, tracking, flush, status, and close operations.
- **Ports and adapters:** networking, event sending, clocks, and storage should have small internal seams that tests can replace without live services. Do not expose unnecessary implementation traits publicly.
- **Strategy:** evaluator operators and data/event transports are replaceable implementation strategies, not condition-heavy code in the facade.
- **Producer/consumer:** evaluation threads enqueue events without waiting for network I/O; one background consumer owns batching and delivery.
- **Immutable snapshot / copy-on-write:** readers load an `Arc` snapshot atomically. Full updates replace the snapshot; version-valid patches derive and atomically publish a new snapshot. Evaluation must not take a contended write lock.
- **Adapter:** `FeatBitProvider` maps OpenFeature types and reasons directly to the same evaluator used by `FbClient`; never maintain a second evaluation engine.

Suggested module ownership:

```text
src/options.rs             validated configuration and defaults
src/client.rs              public facade and lifecycle
src/model.rs               FeatBit wire/domain models and users
src/store.rs               immutable snapshot store
src/data_sync.rs           WebSocket protocol and reconnect loop
src/evaluation/            evaluator, operators, rollout algorithm
src/events.rs              queue, serialization, batching, HTTP sender
src/open_feature.rs        OpenFeature adapter and type mapping
src/error.rs               configuration and internal error taxonomy
```

Keep public exports intentional in `lib.rs`; implementation details remain `pub(crate)`.

## Public API and failure contract

Public application-facing operations must not panic. In particular, constructors, flag resolution, tracking, flushing, status checks, and shutdown must not unwind into application code because of malformed remote data, a poisoned resource, a closed channel, a failed worker, or a network error.

- Use `Result` for configuration errors and operations where the caller explicitly asks for diagnostic failure. A typed `Result` is the Rust equivalent of a safe error return, not an exception.
- Direct value-only variation methods always return the caller's fallback on not-ready, not-found, malformed data, wrong type, invalid context, or internal failure.
- Direct detail methods return the fallback plus a stable reason/error classification.
- OpenFeature provider methods return the standard `EvaluationError` required by its trait. Map errors precisely; do not invent string-only error protocols.
- Background failures update client/provider status and are logged. Recoverable failures retry; unrecoverable authentication/configuration failures stop the affected worker.
- Do not use `unwrap`, `expect`, indexing, unchecked slicing, `todo!`, `unimplemented!`, or deliberate `panic!` in non-test execution paths. If an invariant truly cannot be represented otherwise, document and test it before allowing a narrowly scoped exception.
- Do not use `catch_unwind` to hide routine bugs. Prevent panics through checked parsing and total state machines.
- Lock and channel failures must degrade safely. Prefer non-poisoning synchronization primitives where appropriate.
- All waits and I/O are bounded by configuration or shutdown timeouts. Evaluation itself performs no I/O and never blocks on a worker.
- Shutdown and flush are idempotent. Dropping the final client/provider handle performs a best-effort bounded shutdown; applications should still call `close` explicitly when they need a delivery guarantee.

## `FbOptions` contract

Match FeatBit .NET defaults unless Rust or OpenFeature semantics require a documented difference:

- start wait: 5 seconds;
- streaming URL: `ws://localhost:5100`;
- event URL: `http://localhost:5100`;
- WebSocket connect timeout: 3 seconds;
- close timeout: 2 seconds;
- ping interval: 15 seconds;
- reconnect delays: 0, 1, 2, 3, 5, 8, 13, 21, 34, 55 seconds, then repeat with jitter;
- event auto-flush interval: 5 seconds;
- event flush timeout: 5 seconds;
- event queue capacity: 10,000;
- maximum events per HTTP request: 50;
- maximum event send attempts: 2;
- event retry interval: 200 milliseconds.

`disable_events(disable, allow_track)` is the only event-mode configuration API. `disable` controls
automatic evaluation-event enqueueing; `allow_track` independently controls explicit evaluation and
metric tracking. The default is `(false, true)`. The event processor is completely disabled only for
`(true, false)` (or offline mode). README application guidance recommends `(true, true)` so exposure
events are deferred until the application explicitly tracks them.

Validate at build time:

- environment secret is non-empty and structurally usable by the connection-token algorithm;
- streaming URL uses `ws` or `wss`, and event URL uses `http` or `https`;
- production documentation recommends `wss`/`https`; plaintext localhost remains supported for development;
- durations and capacities are non-zero where zero would disable progress;
- start wait is not less than connect timeout;
- reconnect delay lists are non-empty;
- bootstrap JSON is valid and is only accepted in offline mode.

Secrets must be redacted from `Debug`, `Display`, errors, logs, and metrics.

## WebSocket data synchronizer

Preserve the FeatBit server protocol:

- connect to `streaming?type=server&token=<connection-token>` under the configured streaming base URL;
- send user agent `featbit-rust-server-sdk/<crate-version>`;
- after every connection, send `{"messageType":"data-sync","data":{"timestamp":<store-version>}}`;
- send timestamp `0` for an empty store, otherwise the maximum stored `updatedAt` Unix millisecond version;
- handle `messageType == "data-sync"` with `eventType == "full"` or `"patch"`;
- a full data set atomically replaces flags and segments;
- a patch only replaces the same object when its version is strictly newer; archived objects remain tombstones so an older update cannot resurrect them;
- mark the SDK initialized only after a valid full or patch data set is atomically published;
- periodically send `{"messageType":"ping","data":{}}`;
- reconnect on transport errors and abnormal close, re-requesting data from the local store version;
- do not reconnect after explicit shutdown or the FeatBit unrecoverable close status (4003).

Parsing is defensive. Ignore unknown message types and fields for forward compatibility. A malformed message is logged and discarded without modifying the current snapshot or killing the reconnect loop. Cap accepted message size to prevent unbounded allocation.

Client status mapping is stable:

- starting and never initialized -> `NotReady`;
- synchronized -> `Ready`;
- previously initialized but disconnected/reconnecting -> `Stale`;
- explicitly closed or unrecoverably stopped -> `Closed`/OpenFeature `Error`.

Offline mode performs no network calls. A valid bootstrap snapshot makes it immediately ready; an empty offline store is still operational but unknown flags resolve to fallbacks.

## Store and concurrency

- The client, provider, store, synchronizer, evaluator, and event processor must be `Send + Sync` where exposed across threads.
- Keep all shared ownership explicit with `Arc`; avoid global mutable SDK state.
- Evaluation reads one consistent snapshot for the whole evaluation. Never mix a flag from one snapshot with segments from another.
- Never hold a lock across `.await`, socket I/O, HTTP I/O, logging callbacks, or user code.
- Status and one-way lifecycle flags use atomics with documented ordering; composite state uses a small non-poisoning mutex.
- Patch frequency is expected to be much lower than evaluation frequency, so copy-on-write maps are preferred to read locks. Revisit only with benchmark evidence.
- Bounded channels are mandatory. Backpressure drops analytics rather than slowing application request threads. Log the transition to a full queue once, then suppress repetition until recovery.
- Add concurrency tests that evaluate while full and patch updates are published, close from multiple threads, and overflow the event queue.

## Evaluator compatibility

Use the FeatBit .NET evaluation order and semantics:

1. unknown/archived flag -> flag-not-found;
2. disabled flag -> configured disabled variation;
3. direct user target -> targeted variation;
4. first matching rule -> deterministic rollout variation;
5. fallthrough -> deterministic rollout variation.

Rules are an AND of conditions. Segment inclusion order is excluded user, included user, then any matching segment rule. A missing/malformed referenced segment is a non-match, never a panic.

Support the .NET operators exactly by wire name:

- numeric: `LessThan`, `LessEqualThan`, `BiggerThan`, `BiggerEqualThan`;
- ordinal string: `Equal`, `NotEqual`, `Contains`, `NotContain`, `StartsWith`, `EndsWith`;
- regular expression: `MatchRegex`, `NotMatchRegex`;
- collection: `IsOneOf`, `NotOneOf`;
- boolean text: `IsTrue`, `IsFalse`.

Unknown or malformed operators are non-matches. Invalid numbers, NaN, invalid regexes, and invalid JSON lists do not escape the evaluator.

Rollout compatibility is protocol-critical:

- hash UTF-8 dispatch keys with MD5;
- interpret the first four digest bytes as a little-endian signed 32-bit integer, matching common .NET `BitConverter.ToInt32` behavior;
- calculate `abs(value / i32::MIN)` and use the configured inclusive `[min, max]` range;
- `[0, 1]` always matches and `[0, 0]` never matches;
- the default dispatch key is `flagKey + userKey`; a configured dispatch property substitutes the user's property value;
- preserve the .NET `expt`-prefixed experiment sampling calculation and event `sendToExperiment` semantics.

Every compatibility rule needs deterministic fixtures, including cross-language rollout vectors.

## Event processor

Evaluation and `track` calls enqueue immutable payload events using a non-blocking bounded send. The background worker owns buffering, serialization, HTTP delivery, and retries.

- Successful detail evaluations retain an immutable event snapshot so an application can call
  `track_eval_event` later without re-reading a newer flag snapshot.
- `track_eval_event` and `track_metric_event` are available exactly when the `allow_track` argument
  of `disable_events` is true. They are no-ops when tracking is disallowed, offline mode, shutdown,
  or a full/closed event queue prevents delivery.
- OpenFeature users may explicitly track the current flag result through a provider-specific
  convenience that re-evaluates the current immutable snapshot; document that a retained direct
  detail event is required when the original result must be preserved across updates.

Preserve FeatBit .NET event wire shapes:

- evaluation payload contains `user`, one `variations` entry, `featureFlagKey`, `variation { id, value }`, Unix-millisecond `timestamp`, and `sendToExperiment`;
- metric payload contains `user`, one `metrics` entry, `appType = "rust-server-side"`, `route = "index/metric"`, `type = "CustomEvent"`, `eventName`, `numericValue`, and timestamp;
- user payload uses `keyId`, `name`, and `customizedProperties [{ name, value }]`;
- POST JSON batches to `api/public/insight/track` with `Authorization: <env-secret>` and the SDK user agent.

Flush on the configured interval, explicit request, queue pressure/batch threshold, and close. Split payloads by the configured maximum request size. Retry only recoverable errors: network failures, timeouts, 408, 429, 5xx, and FeatBit-compatible transient 400 behavior. Other 4xx responses stop delivery until the client is recreated. Never retry forever.

Do not log authorization headers or complete user/event bodies. Debug logging may include counts, status codes, durations, and redacted endpoints.

## Logging

Use the standard `log` facade so the application chooses `env_logger`, `tracing-log`, or another implementation. The SDK must not install or replace a global logger.

- `error`: unrecoverable worker/configuration state;
- `warn`: degraded behavior, queue overflow, exhausted retries, shutdown timeout;
- `info`: lifecycle transitions only;
- `debug`: reconnect attempts, sync versions, batch counts;
- `trace`: high-volume protocol diagnostics without secrets or personal data.

Avoid formatting/allocation when the log level is disabled for expensive diagnostics. Log once for repetitive state until that state recovers.

OpenTelemetry support belongs in a separate adapter crate. The core SDK exposes a synchronous,
transport-neutral evaluation observer and does not depend on OpenTelemetry, install global providers,
or configure exporters. The adapter emits `feature_flag.evaluation` semantic events through an
application-owned logger. Context identifiers and raw variation values are excluded by default and
require explicit opt-in. Observability events never replace or invoke FeatBit analytics tracking.

## OpenFeature adapter

`FeatBitProvider` is a first-class public type implementing the official `open_feature::provider::FeatureProvider` trait. Pin to a compatible `open-feature` release and treat trait changes as public API changes.

Context mapping:

- OpenFeature `targeting_key` -> FeatBit `keyId` and is required for targeted/rollout evaluation;
- custom field `name` -> FeatBit user name;
- primitive custom fields -> invariant string values in `customizedProperties`;
- datetime -> RFC 3339 string;
- supported structured fields -> stable JSON; unsupported `Any` values are ignored with debug logging, never downcast with an assumption.

Resolution mapping:

- flag off -> `EvaluationReason::Disabled`;
- direct target or rule match -> `EvaluationReason::TargetingMatch`;
- percentage rollout selection -> `EvaluationReason::Split` when applicable;
- ordinary fallthrough -> `EvaluationReason::Default`;
- variation ID -> OpenFeature `variant`;
- not initialized -> `ProviderNotReady`;
- unknown flag -> `FlagNotFound`;
- missing targeting key -> `TargetingKeyMissing`;
- malformed remote value -> `ParseError`;
- requested type mismatch -> `TypeMismatch`.

OpenFeature bool, integer, float, string, and struct resolutions all use the same string variation result. Struct resolution parses JSON recursively into OpenFeature `StructValue`; unsupported/null shapes return a typed error.

The provider status maps `NotReady`, `Ready`, `Stale`, and terminal closed state without masking stale data. Provider drop delegates to the client's best-effort idempotent shutdown.

## Dependencies and supply chain

- Prefer well-maintained, narrowly scoped crates with compatible MSRV and permissive licenses.
- Disable default features when they add unused runtimes, TLS stacks, mocks, or native dependencies.
- Use rustls by default for HTTP and WebSocket TLS to avoid an undeclared OpenSSL system dependency.
- Keep one async runtime (Tokio) and one HTTP stack (reqwest) unless an architectural decision documents otherwise.
- Exact application behavior belongs in this crate; dependencies must not determine FeatBit protocol semantics.
- Commit `Cargo.lock` for reproducible CI and examples even though the package is a library.
- Review `cargo tree -d`, security advisories, licenses, and MSRV before dependency upgrades.

## Documentation and compatibility

- All public types and methods need rustdoc with failure, fallback, lifecycle, and thread-safety behavior.
- README usage guidance must introduce direct `FbClient` flag evaluation and lifecycle best
  practices first, then document the OpenFeature client/provider integration. Link to compiling
  console and Axum examples for both paths. FeatBit-specific tracking, delivery-aware flush, status,
  and close examples may use direct client/provider extensions. Also show logging setup,
  offline/bootstrap mode, flush/close, and production TLS URLs.
- Examples must compile in CI and use placeholders, never real secrets.
- Use semantic versioning. Existing public names, defaults, event shapes, evaluation results, and reconnect behavior are compatibility surfaces.
- Unknown JSON fields must remain accepted. Removing accepted fields/operators or changing rollout results is a breaking change.

## Required development workflow

Before finishing a change, run from the repository root:

```text
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
cargo test --workspace --doc
```

Also compile with the declared MSRV in CI. Test the latest stable release plus the two preceding minor releases.

Testing layers:

- unit tests for config validation, token generation shape, model parsing, every operator, rollout vectors, reason/error mapping, and serialization;
- store tests for atomic full replacement, tombstones, stale patches, and snapshot consistency;
- deterministic WebSocket tests for initial sync, patch sync, ping, reconnect, malformed messages, close 4003, and shutdown;
- HTTP tests for batching, headers, retry classification, timeout, queue overflow, flush, and close;
- OpenFeature conformance-style tests for every supported type and status/error mapping;
- concurrency/stress tests suitable for normal CI; benchmarks for evaluator hot paths when performance changes.

Tests may use `unwrap`/`expect` when it makes an assertion clearer. Production code may not inherit that exception.

## Definition of done

A change is complete only when:

- behavior is covered at the appropriate test layer;
- public documentation and examples reflect the behavior;
- no secret or personal data is exposed in logs/errors;
- network and shutdown paths are bounded and cancelable;
- evaluation remains deterministic, local, and non-panicking;
- formatting, Clippy, tests, doc tests, and MSRV checks pass;
- deviations from the FeatBit .NET protocol, this repository's architecture rules, or the OpenFeature contract are explicitly documented.
