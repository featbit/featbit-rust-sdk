# FeatBit .NET-to-Rust SDK Test Parity Report

## Report metadata

| Field | Value |
| --- | --- |
| Last reviewed | 2026-07-23 |
| Rust baseline revision | `ea6b2a54d0c23a35c497500f114f6209038f0248`, plus the test changes recorded in this working tree |
| .NET SDK revision | [`974e2a7a557095b300e4e89da86df7d6fa894963`](https://github.com/featbit/featbit-dotnet-sdk/commit/974e2a7a557095b300e4e89da86df7d6fa894963) |
| .NET test source | [`tests/FeatBit.ServerSdk.Tests`](https://github.com/featbit/featbit-dotnet-sdk/tree/974e2a7a557095b300e4e89da86df7d6fa894963/tests/FeatBit.ServerSdk.Tests) |
| Rust test inventory | 89 unit tests: 88 in the core SDK and 1 in the OpenTelemetry adapter; 14 OpenFeature conformance tests live in the separate provider repository |
| .NET test inventory | 107 `[Fact]`/`[Theory]` methods, of which one is skipped |

Raw test counts are not directly comparable. A Rust table-driven test can execute every row from
one or more .NET xUnit theories, while xUnit reports each `InlineData` row separately. This report
compares observable behavior and fixture coverage instead of requiring one-to-one test names.

## Current result

No known behavior-level test from the pinned .NET suite remains missing. Every applicable gap found
in the previous audit has a Rust test. Tests for .NET-only helper classes are intentionally not
ported; equivalent Rust lifecycle behavior is tested through the SDK's public/component boundaries.

The added Rust-specific coverage also verifies contracts that the .NET suite cannot cover:
immutable snapshot consistency; the protocol-neutral raw evaluation adapter boundary; bounded worker
shutdown; queue overflow behavior; and Rust-specific event metadata.

OpenFeature status, reason, error, context, and recursive struct mapping moved to
[`openfeature-provider-rust-server`](https://github.com/featbit/openfeature-provider-rust-server)
when the repositories were separated on 2026-07-23.

## Dispatch algorithm compatibility

The protocol-critical .NET
[`DispatchAlgorithmTests`](https://github.com/featbit/featbit-dotnet-sdk/blob/974e2a7a557095b300e4e89da86df7d6fa894963/tests/FeatBit.ServerSdk.Tests/Evaluation/DispatchAlgorithmTests.cs)
are implemented by
`evaluation::dispatch::tests::rollout_of_key_matches_dotnet_dispatch_algorithm_vectors`.
The exact .NET fixtures are preserved:

| Dispatch key | Expected rollout value |
| --- | ---: |
| `test-value` | `0.14653629204258323` |
| `qKPKh1S3FolC` | `0.9105919692665339` |
| `3eacb184-2d79-49df-9ea7-edd4f10e4c6f` | `0.08994403155520558` |

The assertions compare the complete `f64` bit pattern, not an approximate tolerance. Related tests
also cover:

- inclusive rollout boundaries and invalid ranges;
- selection between multiple rollout variations;
- the default `flagKey + userKey` dispatch key;
- substitution by a configured user property;
- deterministic `expt`-prefixed experiment sampling;
- both `sendToExperiment = true` and `false` outcomes.

## Detailed evaluation output

Rust captures output from successful tests by default. Add `--show-output` to display the inputs and
results emitted by the evaluation compatibility tests:

```text
cargo test --workspace --all-features evaluation -- --show-output
```

Focused commands are also available:

```text
cargo test --workspace --all-features evaluation::dispatch -- --show-output
cargo test --workspace --all-features evaluation::evaluator -- --show-output
cargo test --workspace --all-features evaluation::operators -- --show-output
```

The output includes exact expected and actual rollout values, IEEE-754 bit patterns, dispatch-key
source, selected variation and reason, experiment threshold and decision, and every one of the 34
.NET condition-operator fixtures. Example fields are:

```text
dispatch key="test-value" expected=0.14653629204258323 actual=0.14653629204258323 expected_bits=0x3fc2c1b383000000 actual_bits=0x3fc2c1b383000000
evaluation dispatch_source=custom(bucket) dispatch_key="test-value" rollout=0.14653629204258323 variation=true reason=Fallthrough { split: true }
condition user="10" operator=BiggerThan rule="9" expected=true actual=true
```

## .NET parity matrix

| .NET test class | Status | Rust coverage |
| --- | --- | --- |
| `JsonBootstrapProviderTests` | Covered | Invalid JSON, offline restrictions, empty bootstrap behavior, and a full pinned flag-plus-segment population fixture. |
| `AtomicBooleanTests` | Not applicable | Rust uses standard atomics; public lifecycle concurrency and memory-visible transitions are tested. |
| `StatusManagerTests` | Covered at SDK boundary | Ready, stale, terminal state, notification, and waiter behavior are covered. The generic .NET callback helper has no Rust counterpart. |
| `WebSocketDataSynchronizerTests` | Covered | Initial full/patch sync, a pre-populated initial version, reconnect/version continuation, explicit stale evaluation, pre/post-initialization `4003`, conflict resync, and shutdown. |
| `ConditionMatcherTests` | Covered | All 34 exact .NET numeric, string, regex, collection, and boolean fixtures, plus malformed and unknown operators. |
| `DispatchAlgorithmTests` | Covered exactly | All three .NET keys use bit-for-bit `f64` equality. |
| `EvaluatorTests` | Covered | Not found, archived, disabled, target, rule, fallthrough, errors, reasons, variation IDs, rollout keys, and experiment decisions. |
| `RuleMatcherTests` | Covered | AND semantics, first match, segment conditions, and rollout result selection. |
| `SegmentMatcherTests` | Covered | Exclusion precedence, inclusion, rule matching, any-segment matching, archived/missing/malformed references, and positive/negative forms. |
| `AsyncEventTests` | Not applicable | Rust worker completion and bounded flush/close waits cover the lifecycle contract without a TaskCompletionSource-style helper. |
| `DefaultEventBufferTests` | Covered equivalently | Bounded capacity, drops, recovery/re-armed overflow warning state, and immutable retained evaluation-event snapshots. |
| `DefaultEventDispatcherTests` | Covered | Empty flush, add/send, threshold and interval flush, batching, fatal stop, graceful close, and draining. |
| `DefaultEventProcessorTests` | Covered | Valid/invalid record paths, overflow, explicit and timed-out flush, delivery failure, and concurrent idempotent close. |
| `DefaultEventSenderTests` | Covered | Headers, success, status classification, connection failure, request timeout, retries, exhaustion, fatal responses, and cancellation. |
| `DefaultEventSerializerTests` | Covered | Exact single evaluation plus multi-evaluation, multi-metric, and mixed batch fixtures. |
| `FbClientOfflineTests` | Covered | Readiness, empty-store fallbacks, bootstrap evaluation, no network/events, and close. |
| `FbClientTests` | Covered | Typed value/detail evaluation, automatic/manual tracking modes, retained snapshots, populated and sorted `all_variations`, no-event guarantee, and concurrent close. |
| `DeserializationTests` | Covered | Pinned .NET flag and segment JSON fixtures, unknown fields, and malformed `updatedAt` rejection. |
| `FbOptionsBuilderTests` | Covered | Exact defaults and reconnect sequence, event modes, redaction, URL/secret/duration/capacity/list/relationship validation, and bootstrap restrictions. |
| `BackoffAndJitterRetryPolicyTests` | Covered beyond .NET | Rust automatically asserts series cycling and jitter bounds; the corresponding .NET test is skipped. |
| `DefaultRetryPolicyTests` | Covered | Default and custom sequence cycling and retry behavior are covered. |
| `DefaultMemoryStoreTests` | Covered | Empty/full replacement, flag and segment patches, tombstones, stale/equal-version handling, concurrent writers, and snapshot consistency. |
| `ConnectionTokenTests` | Covered | Number encoding plus decoded token structure, secret reconstruction, split bounds, padding removal, timestamp validity, and endpoint integration. |
| `FbWebSocketTests` | Covered | Handshake metadata, send/receive, ping, malformed/oversized messages, normal and abnormal reconnect, terminal close, and shutdown. |
| `WebSocketsTransportTests` | Covered | Configured connect timeout, handshake rejection and recovery, data transport, and close cancellation. |
| `UriTests` | Not applicable | Generic `System.Uri` behavior belongs to .NET; SDK-owned root/nested and trailing-slash streaming/event endpoints are covered in Rust. |
| `ValueConverterTests` | Covered with intentional API difference | Exact boolean/integer/double/string/JSON/fallback rows are covered. Rust exposes `f64`, not a separate .NET-style `float32` API. |

## Rust-specific contract coverage

The following requirements are covered in addition to .NET parity:

- raw evaluation flag/variation metadata, protocol reason, typed error, completion, and observation
  behavior used by external adapters;
- atomic flag-and-segment publication during concurrent evaluation;
- online concurrent close with both WebSocket and event workers active;
- HTTP batch splitting, automatic flushes, transport retry, caller timeout, request timeout, and
  bounded cancellation;
- unknown WebSocket messages/fields and forward-compatible model fields;
- connection paths, query parameters, authorization, and SDK user-agent headers;
- secret, bootstrap, and variation-value redaction.

## Intentionally excluded tests

These are not missing SDK behavior:

- .NET helper implementation tests for `AtomicBoolean`, TaskCompletionSource-style `AsyncEvent`,
  callback internals, and concrete null-object types;
- null-reference inputs, which safe Rust references cannot represent; invalid values are tested
  instead;
- generic `System.Uri` behavior;
- a separate `float32` variation method that does not exist in the Rust public API;
- a static full connection-token string: the wire format deliberately includes current time and
  randomness, so the test decodes and validates every structural field instead;
- installation of a global test logger: the SDK must not replace the application's logger, while
  the overflow suppression/recovery state that controls warning emission is directly tested.

## Remaining scope outside this parity baseline

There are no known actionable gaps against the pinned .NET test suite or the audited Rust-specific
requirements. Future additions may include live-server end-to-end tests, TLS/proxy infrastructure
tests, fuzzing of protocol payloads, and evaluator benchmarks when performance-related code changes.
Those layers require separate infrastructure or a performance change and are not missing unit-test
parity.

## Verification

The recorded working tree passed all repository gates on 2026-07-22:

```text
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
cargo test --workspace --doc
cargo +1.95.0 test --workspace --all-features
cargo test --workspace --all-features evaluation -- --show-output
```

The normal and MSRV runs each passed all 89 tests in this repository. The separate provider's normal
and MSRV runs each passed all 14 OpenFeature tests. The detailed evaluation run passed the matching
tests and displayed the cross-language fixture details described above.

## Maintenance

Update this report when the pinned .NET revision, rollout behavior, event wire shape, WebSocket
protocol, raw adapter boundary, Rust test inventory, or any matrix status changes. When a new gap is
found, add the test first, then update this record. Document intentional protocol deviations in the
repository engineering guide.
