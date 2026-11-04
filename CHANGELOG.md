# Changelog

## 0.2.1 ¡ª 2026-11-04

- Remove `sendToExperiment` from FeatBit evaluation-event payloads and remove experiment sampling.
- Ignore server experiment metadata: `exptIncludeAllTargets`, rule/fallthrough `includedInExpt`,
  and rollout variation `exptRollout`. Ordinary targeting and percentage rollouts are unchanged.
- Remove the experiment attribute from the OpenTelemetry adapter.
- Update fixtures and retain compatibility tests for legacy server data and omitted event fields.

### API migration

This release intentionally removes public API despite the patch version:

- `FbEvaluationEvent::new` takes `(flag_key, variation_id, variation_value, insights_enabled)`;
  remove the previous `send_to_experiment` argument.
- Remove calls to `FbEvaluationEvent::send_to_experiment()` and
  `EvaluationObservation::send_to_experiment()`.
- The OpenTelemetry attribute `featbit.evaluation.send_to_experiment` is no longer emitted.

Core SDK and OpenTelemetry adapter versions are released together as 0.2.1.
