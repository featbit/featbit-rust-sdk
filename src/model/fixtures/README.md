# FeatBit .NET compatibility fixtures

These JSON files are copied from the FeatBit .NET Server SDK at commit
`974e2a7a557095b300e4e89da86df7d6fa894963`:

- `tests/FeatBit.ServerSdk.Tests/Model/one-flag.json`
- `tests/FeatBit.ServerSdk.Tests/Model/one-segment.json`

They are intentionally kept as cross-SDK wire fixtures. Changes to the Rust models must continue
to deserialize these files with the same observable field values. Unknown fields in the fixtures
also verify the SDK's forward-compatible parsing contract.
