#!/usr/bin/env python3
"""Plan and prepare lockstep FeatBit Rust SDK releases.

The script intentionally uses only the Python standard library so the release
workflow does not need to install another package manager or versioning tool.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import time
import tomllib
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from functools import total_ordering
from pathlib import Path
from typing import Callable, Mapping, Sequence


CORE_CRATE = "featbit-server-sdk"
ADAPTER_CRATE = "featbit-server-sdk-opentelemetry"
CRATES = (CORE_CRATE, ADAPTER_CRATE)
DEFAULT_REGISTRY_API = "https://crates.io"
USER_AGENT = "featbit-rust-sdk-release-workflow/1"

_SEMVER_PATTERN = re.compile(
    r"^(?P<major>0|[1-9][0-9]*)"
    r"\.(?P<minor>0|[1-9][0-9]*)"
    r"\.(?P<patch>0|[1-9][0-9]*)"
    r"(?:-(?P<prerelease>[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*))?$"
)
_RELEASE_SUBJECT_PATTERN = re.compile(
    r"^chore\(release\): v(?P<version>[^ ]+) \[skip ci\]$"
)


class ReleaseError(RuntimeError):
    """A safe, actionable release-planning failure."""


@total_ordering
@dataclass(frozen=True)
class Version:
    """A SemVer version without build metadata."""

    major: int
    minor: int
    patch: int
    prerelease: tuple[str, ...] = ()

    @classmethod
    def parse(cls, value: str) -> "Version":
        """Parse an exact SemVer value accepted by this release workflow."""

        match = _SEMVER_PATTERN.fullmatch(value.strip())
        if match is None:
            raise ReleaseError(
                f"invalid version {value!r}; expected SemVer such as "
                "1.2.3 or 1.2.3-beta.1 (without a leading 'v' or build metadata)"
            )

        prerelease_text = match.group("prerelease")
        prerelease = (
            tuple(prerelease_text.split(".")) if prerelease_text is not None else ()
        )
        for identifier in prerelease:
            if identifier.isdigit() and len(identifier) > 1 and identifier[0] == "0":
                raise ReleaseError(
                    f"invalid version {value!r}; numeric prerelease identifiers "
                    "must not contain leading zeroes"
                )

        return cls(
            major=int(match.group("major")),
            minor=int(match.group("minor")),
            patch=int(match.group("patch")),
            prerelease=prerelease,
        )

    @property
    def is_prerelease(self) -> bool:
        """Whether this version has a prerelease suffix."""

        return bool(self.prerelease)

    def next_patch(self) -> "Version":
        """Return the next stable patch version."""

        return Version(self.major, self.minor, self.patch + 1)

    def __str__(self) -> str:
        base = f"{self.major}.{self.minor}.{self.patch}"
        if self.prerelease:
            return f"{base}-{'.'.join(self.prerelease)}"
        return base

    def __lt__(self, other: object) -> bool:
        if not isinstance(other, Version):
            return NotImplemented

        left_base = (self.major, self.minor, self.patch)
        right_base = (other.major, other.minor, other.patch)
        if left_base != right_base:
            return left_base < right_base

        if not self.prerelease:
            return False
        if not other.prerelease:
            return True

        for left, right in zip(self.prerelease, other.prerelease):
            if left == right:
                continue
            left_numeric = left.isdigit()
            right_numeric = right.isdigit()
            if left_numeric and right_numeric:
                return int(left) < int(right)
            if left_numeric != right_numeric:
                return left_numeric
            return left < right

        return len(self.prerelease) < len(other.prerelease)


@dataclass(frozen=True)
class ReleasePlan:
    """The registry-aware plan for one lockstep workspace release."""

    version: Version
    core_published: bool
    adapter_published: bool


@dataclass(frozen=True)
class ReleaseSource:
    """The exact source commit to verify and publish."""

    source_sha: str
    resume: bool
    resume_version: Version | None = None


def read_manifest_version(path: Path) -> Version:
    """Read the package version from a Cargo manifest."""

    try:
        with path.open("rb") as manifest:
            value = tomllib.load(manifest)["package"]["version"]
    except (OSError, KeyError, TypeError, tomllib.TOMLDecodeError) as error:
        raise ReleaseError(f"cannot read package.version from {path}: {error}") from error
    if not isinstance(value, str):
        raise ReleaseError(f"package.version in {path} must be a string")
    return Version.parse(value)


def plan_release(
    manifest_version: Version,
    requested: str | None,
    published: Mapping[str, set[Version]],
    *,
    allow_complete: bool = False,
) -> ReleasePlan:
    """Choose and validate a release version against registry state."""

    core_versions = published.get(CORE_CRATE, set())
    adapter_versions = published.get(ADAPTER_CRATE, set())
    all_versions = core_versions | adapter_versions
    complete_versions = core_versions & adapter_versions

    newest = max(all_versions) if all_versions else None
    newest_complete = max(complete_versions) if complete_versions else None
    if (
        requested is None
        and newest is not None
        and newest not in complete_versions
        and (newest_complete is None or newest > newest_complete)
    ):
        missing = (
            ADAPTER_CRATE if newest in core_versions else CORE_CRATE
        )
        raise ReleaseError(
            f"latest release {newest} is incomplete; {missing} is missing. "
            "Re-run the failed workflow or request that exact version explicitly."
        )

    if requested is None:
        base = max(
            (candidate for candidate in (manifest_version, newest) if candidate is not None)
        )
        version = base.next_patch()
    else:
        version = Version.parse(requested)

    core_published = version in core_versions
    adapter_published = version in adapter_versions
    if core_published and adapter_published and not allow_complete:
        raise ReleaseError(
            f"version {version} is already published for both workspace crates"
        )

    if (
        requested is not None
        and not core_published
        and not adapter_published
        and newest is not None
        and version <= newest
    ):
        raise ReleaseError(
            f"requested version {version} must be newer than the latest published "
            f"workspace version {newest}"
        )

    return ReleasePlan(
        version=version,
        core_published=core_published,
        adapter_published=adapter_published,
    )


def _registry_api() -> str:
    return os.environ.get("CRATES_IO_API", DEFAULT_REGISTRY_API).rstrip("/")


def _request_json(path: str) -> object | None:
    url = f"{_registry_api()}{path}"
    request = urllib.request.Request(
        url,
        headers={
            "Accept": "application/json",
            "User-Agent": USER_AGENT,
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            return json.load(response)
    except urllib.error.HTTPError as error:
        if error.code == 404:
            return None
        raise ReleaseError(
            f"crates.io request failed with HTTP {error.code}: {url}"
        ) from error
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as error:
        raise ReleaseError(f"crates.io request failed for {url}: {error}") from error


def published_versions(crate_name: str) -> set[Version]:
    """Return every version crates.io reports for a crate, including yanked ones."""

    encoded_name = urllib.parse.quote(crate_name, safe="")
    payload = _request_json(f"/api/v1/crates/{encoded_name}")
    if payload is None:
        return set()
    if not isinstance(payload, dict) or not isinstance(payload.get("versions"), list):
        raise ReleaseError(f"unexpected crates.io response for {crate_name}")

    versions: set[Version] = set()
    for item in payload["versions"]:
        if not isinstance(item, dict) or not isinstance(item.get("num"), str):
            raise ReleaseError(f"unexpected crates.io version entry for {crate_name}")
        versions.add(Version.parse(item["num"]))
    return versions


def is_version_published(crate_name: str, version: Version) -> bool:
    """Check whether one exact crate version is visible through crates.io."""

    encoded_name = urllib.parse.quote(crate_name, safe="")
    encoded_version = urllib.parse.quote(str(version), safe="")
    return (
        _request_json(f"/api/v1/crates/{encoded_name}/{encoded_version}") is not None
    )


def wait_until_published(
    crate_name: str,
    version: Version,
    *,
    timeout_seconds: float,
    interval_seconds: float,
    check: Callable[[str, Version], bool] = is_version_published,
) -> None:
    """Wait for an uploaded version to become visible in the registry API."""

    deadline = time.monotonic() + timeout_seconds
    while True:
        if check(crate_name, version):
            print(f"{crate_name} {version} is visible on crates.io")
            return
        if time.monotonic() >= deadline:
            raise ReleaseError(
                f"timed out waiting for {crate_name} {version} to appear on crates.io"
            )
        print(f"waiting for {crate_name} {version} to appear on crates.io...")
        time.sleep(interval_seconds)


def _replace_package_version(path: Path, version: Version) -> None:
    raw = path.read_bytes()
    text = raw.decode("utf-8")
    package_match = re.search(
        r"(?ms)^\[package\][^\r\n]*(?:\r?\n)(.*?)(?=^\[|\Z)",
        text,
    )
    if package_match is None:
        raise ReleaseError(f"cannot find [package] in {path}")

    package_body = package_match.group(1)
    updated_body, count = re.subn(
        r'(?m)^(version\s*=\s*")[^"]+(".*)$',
        rf"\g<1>{version}\g<2>",
        package_body,
    )
    if count != 1:
        raise ReleaseError(
            f"expected exactly one package version in {path}, found {count}"
        )

    updated = (
        text[: package_match.start(1)]
        + updated_body
        + text[package_match.end(1) :]
    )
    path.write_bytes(updated.encode("utf-8"))


def _replace_adapter_dependency_version(path: Path, version: Version) -> None:
    raw = path.read_bytes()
    text = raw.decode("utf-8")
    updated, count = re.subn(
        r'(?m)^(featbit-server-sdk\s*=\s*\{[^\r\n]*\bversion\s*=\s*")[^"]+(".*)$',
        rf"\g<1>{version}\g<2>",
        text,
    )
    if count != 1:
        raise ReleaseError(
            f"expected exactly one {CORE_CRATE} dependency version in {path}, "
            f"found {count}"
        )
    path.write_bytes(updated.encode("utf-8"))


def _replace_lock_versions(path: Path, version: Version) -> None:
    raw = path.read_bytes()
    text = raw.decode("utf-8")
    package_pattern = re.compile(
        r"(?ms)^\[\[package\]\]\r?\n.*?(?=^\[\[package\]\]\r?\n|\Z)"
    )
    counts = {crate_name: 0 for crate_name in CRATES}

    def replace_block(match: re.Match[str]) -> str:
        block = match.group(0)
        for crate_name in CRATES:
            if re.search(
                rf'(?m)^name\s*=\s*"{re.escape(crate_name)}"\s*$',
                block,
            ):
                updated, count = re.subn(
                    r'(?m)^(version\s*=\s*")[^"]+(".*)$',
                    rf"\g<1>{version}\g<2>",
                    block,
                    count=1,
                )
                if count != 1:
                    raise ReleaseError(
                        f"cannot find version for {crate_name} in {path}"
                    )
                counts[crate_name] += 1
                return updated
        return block

    updated = package_pattern.sub(replace_block, text)
    unexpected = {
        crate_name: count for crate_name, count in counts.items() if count != 1
    }
    if unexpected:
        details = ", ".join(
            f"{crate_name}={count}" for crate_name, count in unexpected.items()
        )
        raise ReleaseError(f"unexpected workspace package counts in {path}: {details}")
    path.write_bytes(updated.encode("utf-8"))


def set_workspace_version(root: Path, version: Version) -> None:
    """Update both manifests, their path dependency, and Cargo.lock in lockstep."""

    root_manifest = root / "Cargo.toml"
    adapter_manifest = root / "integrations" / "opentelemetry" / "Cargo.toml"
    lock_file = root / "Cargo.lock"

    _replace_package_version(root_manifest, version)
    _replace_package_version(adapter_manifest, version)
    _replace_adapter_dependency_version(adapter_manifest, version)
    _replace_lock_versions(lock_file, version)

    actual_core = read_manifest_version(root_manifest)
    actual_adapter = read_manifest_version(adapter_manifest)
    if actual_core != version or actual_adapter != version:
        raise ReleaseError("workspace manifests did not retain the requested version")


def _git(root: Path, *arguments: str, check: bool = True) -> str:
    process = subprocess.run(
        ["git", *arguments],
        cwd=root,
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
    )
    if check and process.returncode != 0:
        detail = process.stderr.strip() or process.stdout.strip()
        raise ReleaseError(f"git {' '.join(arguments)} failed: {detail}")
    return process.stdout.strip()


def _release_version_from_subject(root: Path, commit: str) -> Version | None:
    subject = _git(root, "show", "-s", "--format=%s", commit)
    match = _RELEASE_SUBJECT_PATTERN.fullmatch(subject)
    if match is None:
        return None
    return Version.parse(match.group("version"))


def _tag_exists(root: Path, version: Version) -> bool:
    process = subprocess.run(
        [
            "git",
            "rev-parse",
            "--quiet",
            "--verify",
            f"refs/tags/v{version}^{{commit}}",
        ],
        cwd=root,
        check=False,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    return process.returncode == 0


def detect_release_source(root: Path, trigger_sha: str, git_ref: str) -> ReleaseSource:
    """Resolve a normal trigger or a retry after the release commit was pushed."""

    if git_ref != "refs/heads/main":
        raise ReleaseError(
            f"releases must run from refs/heads/main, not {git_ref or '<empty>'}"
        )

    _git(
        root,
        "fetch",
        "--no-tags",
        "origin",
        "+refs/heads/main:refs/remotes/origin/main",
    )
    _git(root, "fetch", "--tags", "origin")
    trigger = _git(root, "rev-parse", f"{trigger_sha}^{{commit}}")
    main = _git(root, "rev-parse", "origin/main^{commit}")

    if main == trigger:
        version = _release_version_from_subject(root, main)
        if version is not None and not _tag_exists(root, version):
            return ReleaseSource(main, True, version)
        return ReleaseSource(main, False)

    parent = _git(root, "rev-parse", f"{main}^", check=False)
    version = _release_version_from_subject(root, main)
    if parent == trigger and version is not None and not _tag_exists(root, version):
        return ReleaseSource(main, True, version)

    raise ReleaseError(
        "the triggering commit is no longer the main branch head; use the newest "
        "release run instead"
    )


def _write_github_outputs(path: str | None, values: Mapping[str, object]) -> None:
    if path is None:
        return
    with Path(path).open("a", encoding="utf-8", newline="\n") as output:
        for key, value in values.items():
            rendered = str(value).lower() if isinstance(value, bool) else str(value)
            if "\n" in rendered or "\r" in rendered:
                raise ReleaseError(f"GitHub output {key} contains a newline")
            output.write(f"{key}={rendered}\n")


def _append_summary(path: str | None, plan: ReleasePlan) -> None:
    if path is None:
        return
    with Path(path).open("a", encoding="utf-8", newline="\n") as summary:
        summary.write("### crates.io release candidate\n\n")
        summary.write(f"- Version: `v{plan.version}`\n")
        summary.write(
            f"- Channel: `{'prerelease' if plan.version.is_prerelease else 'stable'}`\n"
        )
        summary.write(
            f"- `{CORE_CRATE}` already published: `{str(plan.core_published).lower()}`\n"
        )
        summary.write(
            f"- `{ADAPTER_CRATE}` already published: "
            f"`{str(plan.adapter_published).lower()}`\n\n"
        )
        summary.write(
            "The crates.io upload remains blocked until the protected GitHub "
            "Environment is approved.\n"
        )


def _published_map() -> dict[str, set[Version]]:
    return {crate_name: published_versions(crate_name) for crate_name in CRATES}


def _add_common_root_argument(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--root",
        type=Path,
        default=Path(__file__).resolve().parents[1],
        help="repository root (defaults to the parent of scripts/)",
    )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    source_parser = subparsers.add_parser(
        "source", help="resolve the exact main-branch source commit"
    )
    _add_common_root_argument(source_parser)
    source_parser.add_argument("--trigger-sha", required=True)
    source_parser.add_argument("--git-ref", required=True)
    source_parser.add_argument("--github-output")

    plan_parser = subparsers.add_parser(
        "plan", help="calculate and validate the registry-aware release version"
    )
    _add_common_root_argument(plan_parser)
    plan_parser.add_argument(
        "--requested",
        default="",
        help="exact SemVer; an empty value increments the patch component",
    )
    plan_parser.add_argument("--expect", help="fail unless this version is selected")
    plan_parser.add_argument("--allow-complete", action="store_true")
    plan_parser.add_argument("--github-output")
    plan_parser.add_argument("--summary")

    set_parser = subparsers.add_parser(
        "set-version", help="write one version to both workspace crates"
    )
    _add_common_root_argument(set_parser)
    set_parser.add_argument("version")

    wait_parser = subparsers.add_parser(
        "wait", help="wait for an exact crate version to become visible"
    )
    wait_parser.add_argument("crate", choices=CRATES)
    wait_parser.add_argument("version")
    wait_parser.add_argument("--timeout", type=float, default=300)
    wait_parser.add_argument("--interval", type=float, default=5)

    return parser


def _run(arguments: Sequence[str]) -> int:
    parser = _build_parser()
    args = parser.parse_args(arguments)

    if args.command == "source":
        source = detect_release_source(
            args.root.resolve(),
            args.trigger_sha,
            args.git_ref,
        )
        outputs: dict[str, object] = {
            "source_sha": source.source_sha,
            "resume": source.resume,
            "resume_version": source.resume_version or "",
        }
        _write_github_outputs(args.github_output, outputs)
        print(json.dumps(outputs))
        return 0

    if args.command == "plan":
        manifest_version = read_manifest_version(args.root.resolve() / "Cargo.toml")
        requested = args.requested.strip() or None
        plan = plan_release(
            manifest_version,
            requested,
            _published_map(),
            allow_complete=args.allow_complete,
        )
        if args.expect is not None and plan.version != Version.parse(args.expect):
            raise ReleaseError(
                f"release plan changed from {args.expect} to {plan.version} while "
                "waiting for approval"
            )
        outputs = {
            "version": plan.version,
            "is_prerelease": plan.version.is_prerelease,
            "core_published": plan.core_published,
            "adapter_published": plan.adapter_published,
        }
        _write_github_outputs(args.github_output, outputs)
        _append_summary(args.summary, plan)
        print(json.dumps({key: str(value).lower() for key, value in outputs.items()}))
        return 0

    if args.command == "set-version":
        version = Version.parse(args.version)
        set_workspace_version(args.root.resolve(), version)
        print(f"workspace version set to {version}")
        return 0

    if args.command == "wait":
        version = Version.parse(args.version)
        wait_until_published(
            args.crate,
            version,
            timeout_seconds=args.timeout,
            interval_seconds=args.interval,
        )
        return 0

    parser.error(f"unsupported command: {args.command}")
    return 2


def main() -> int:
    """Run the release command-line interface."""

    try:
        return _run(sys.argv[1:])
    except (OSError, ReleaseError) as error:
        print(f"release error: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
