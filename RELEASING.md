# Releasing to crates.io

The repository publishes two crates in one lockstep release:

- `featbit-server-sdk`
- `featbit-server-sdk-opentelemetry`

`.github/workflows/release.yml` runs for every non-bot push to `main`, including a
merged pull request. It verifies the selected commit and then waits at the
`crates-io-release` GitHub Environment. Nothing is uploaded until a required
reviewer approves that deployment.

The workflow publishes the core crate first, waits for it to appear in the
crates.io index, and then publishes the OpenTelemetry adapter. It writes the
selected version to both manifests and `Cargo.lock`, pushes that release commit
to `main`, creates `v<VERSION>`, and creates a GitHub Release.

## One-time setup

### 1. Bootstrap both crate names

crates.io can only configure Trusted Publishing after a crate exists. An owner
must therefore publish each crate once with a personal, narrowly scoped
crates.io token. From a clean, reviewed `main` checkout:

The initial bootstrap version is `0.1.0-beta.1`.

```text
cargo login

cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
cargo test --workspace --doc

cargo publish --dry-run -p featbit-server-sdk
cargo publish --locked -p featbit-server-sdk
```

Wait until the exact core version is visible:

```text
cargo info featbit-server-sdk@0.1.0-beta.1
```

Then publish the adapter:

```text
cargo publish --dry-run -p featbit-server-sdk-opentelemetry
cargo publish --locked -p featbit-server-sdk-opentelemetry
```

Published versions cannot be overwritten or deleted. A bad version can only be
yanked, so inspect the package and make sure the worktree contains no credentials
before this bootstrap.

### 2. Configure Trusted Publishing for both crates

On the crates.io settings page for **each** crate, add the same GitHub Trusted
Publisher:

| Field | Value |
| --- | --- |
| Repository owner | `featbit` |
| Repository name | `featbit-rust-sdk` |
| Workflow filename | `release.yml` |
| Environment name | `crates-io-release` |

The values are case-sensitive. Both crates need their own Trusted Publisher
entry. After a successful OIDC release, require Trusted Publishing for future
uploads if that option is enabled for the crate.

The workflow uses `rust-lang/crates-io-auth-action` to mint a short-lived token.
Do not add a long-lived crates.io token to GitHub Secrets.

### 3. Configure the approval gate

In GitHub, open **Settings → Environments**, create
`crates-io-release`, and configure:

1. Add the maintainers or maintainer team under **Required reviewers**.
2. Enable **Prevent self-review** when a second maintainer must approve.
3. Limit deployment branches to `main`.
4. Optionally disable administrator bypass.

The environment does not need a crates.io secret. The workflow requests only a
short-lived OIDC identity after the environment has been approved.

The publish job needs `contents: write` to push the generated version commit,
tag, and GitHub Release. The repository currently permits direct updates to
`main`. If branch protection or a repository ruleset is introduced, give this
release workflow a narrowly scoped bypass for its bot-authored
`chore(release): ...` commit, or change the version update to a release-PR model
before enabling the rule.

## Normal patch release

Every new commit on `main` starts a release run:

1. The workflow calculates the candidate version and runs all quality gates.
2. Open the run and review the planned version and commit.
3. Select **Review deployments**.
4. Approve `crates-io-release`.

With no explicit version, the workflow takes the newer of the manifest version
and latest completed workspace release, removes any prerelease suffix, and
increments the third component:

```text
0.1.0 -> 0.1.1
1.4.9 -> 1.4.10
2.0.0-beta.2 -> 2.0.1
```

Only approve the newest applicable run. The publish job rechecks that its source
is still the `main` head and fails before uploading if a newer commit arrived
while it was waiting.

## Explicit and prerelease versions

Choose **Actions → Release crates.io → Run workflow**, select `main`, and enter
an exact SemVer in `version`. Supported examples include:

```text
0.2.0
0.2.0-alpha.1
0.2.0-beta.1
0.2.0-beta.2
0.2.0-rc.1
```

The `v` prefix is only used for the Git tag and must not be entered in the
version field. Build metadata such as `1.2.3+build.4` is deliberately rejected
because Cargo ignores it for version precedence.

An explicit version must be newer than every version already published for the
workspace, except when resuming a partial release. To promote
`0.2.0-beta.2` to stable, explicitly request `0.2.0`; leaving the field empty
would perform the documented patch increment instead.

If an automatic run is already waiting for approval and a different explicit
version is required, cancel or reject that pending run before dispatching the
manual one.

## Failure recovery

The upload is idempotent per crate. If the core crate succeeds but the adapter
fails, the next full run detects that state, skips the existing core version,
and retries the missing adapter.

If the workflow has already pushed its `chore(release): v<VERSION> [skip ci]`
commit but later fails, use **Re-run all jobs** on that workflow. The planner
recognizes the untagged release commit and resumes the same version. Resolve a
partial release before merging more changes into `main`; this preserves the
guarantee that both crates and the tag describe the same source.

Do not delete or retarget a published version's Git tag. If a published package
is unusable, yank it and release a new version:

```text
cargo yank --version <VERSION> featbit-server-sdk
cargo yank --version <VERSION> featbit-server-sdk-opentelemetry
```
