from __future__ import annotations

import sys
import subprocess
import tempfile
import unittest
from pathlib import Path


SCRIPTS_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SCRIPTS_DIR))

import release  # noqa: E402


class VersionTests(unittest.TestCase):
    def test_parses_stable_and_prerelease_versions(self) -> None:
        self.assertEqual(str(release.Version.parse("1.2.3")), "1.2.3")
        self.assertEqual(
            str(release.Version.parse("1.2.3-beta.11")), "1.2.3-beta.11"
        )
        self.assertTrue(release.Version.parse("2.0.0-rc.1").is_prerelease)

    def test_rejects_non_semver_and_ambiguous_metadata(self) -> None:
        invalid = (
            "v1.2.3",
            "1.2",
            "01.2.3",
            "1.2.3-beta.01",
            "1.2.3+build.1",
        )
        for value in invalid:
            with self.subTest(value=value):
                with self.assertRaises(release.ReleaseError):
                    release.Version.parse(value)

    def test_orders_prereleases_using_semver_rules(self) -> None:
        ordered = [
            "1.0.0-alpha",
            "1.0.0-alpha.1",
            "1.0.0-beta",
            "1.0.0-beta.2",
            "1.0.0-beta.11",
            "1.0.0-rc.1",
            "1.0.0",
        ]
        versions = [release.Version.parse(value) for value in ordered]
        self.assertEqual(sorted(reversed(versions)), versions)


class PlanTests(unittest.TestCase):
    def published(
        self,
        core: tuple[str, ...] = (),
        adapter: tuple[str, ...] = (),
    ) -> dict[str, set[release.Version]]:
        return {
            release.CORE_CRATE: {release.Version.parse(value) for value in core},
            release.ADAPTER_CRATE: {
                release.Version.parse(value) for value in adapter
            },
        }

    def test_default_increments_manifest_patch_for_first_release(self) -> None:
        plan = release.plan_release(
            release.Version.parse("0.1.0"),
            None,
            self.published(),
        )
        self.assertEqual(str(plan.version), "0.1.1")

    def test_default_increments_latest_registry_patch(self) -> None:
        plan = release.plan_release(
            release.Version.parse("0.1.0"),
            None,
            self.published(
                core=("0.1.0", "0.1.3"),
                adapter=("0.1.0", "0.1.3"),
            ),
        )
        self.assertEqual(str(plan.version), "0.1.4")

    def test_requested_prerelease_is_preserved(self) -> None:
        plan = release.plan_release(
            release.Version.parse("0.1.0"),
            "0.2.0-beta.1",
            self.published(core=("0.1.0",), adapter=("0.1.0",)),
        )
        self.assertEqual(str(plan.version), "0.2.0-beta.1")
        self.assertTrue(plan.version.is_prerelease)

    def test_rejects_a_requested_version_behind_the_registry(self) -> None:
        with self.assertRaisesRegex(release.ReleaseError, "must be newer"):
            release.plan_release(
                release.Version.parse("0.1.0"),
                "0.1.2",
                self.published(core=("0.1.3",), adapter=("0.1.3",)),
            )

    def test_fails_closed_for_an_incomplete_latest_release(self) -> None:
        with self.assertRaisesRegex(release.ReleaseError, "incomplete"):
            release.plan_release(
                release.Version.parse("0.1.0"),
                None,
                self.published(core=("0.1.1",), adapter=()),
            )

    def test_explicit_version_can_resume_a_partial_publish(self) -> None:
        plan = release.plan_release(
            release.Version.parse("0.1.1"),
            "0.1.1",
            self.published(core=("0.1.1",), adapter=()),
        )
        self.assertTrue(plan.core_published)
        self.assertFalse(plan.adapter_published)

    def test_complete_version_requires_explicit_retry_permission(self) -> None:
        published = self.published(core=("0.1.1",), adapter=("0.1.1",))
        with self.assertRaisesRegex(release.ReleaseError, "already published"):
            release.plan_release(
                release.Version.parse("0.1.1"),
                "0.1.1",
                published,
            )
        plan = release.plan_release(
            release.Version.parse("0.1.1"),
            "0.1.1",
            published,
            allow_complete=True,
        )
        self.assertTrue(plan.core_published)
        self.assertTrue(plan.adapter_published)


class WorkspaceVersionTests(unittest.TestCase):
    def test_updates_both_manifests_dependency_and_lock_file(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            adapter_dir = root / "integrations" / "opentelemetry"
            adapter_dir.mkdir(parents=True)
            (root / "Cargo.toml").write_text(
                '[package]\nname = "featbit-server-sdk"\nversion = "0.1.0"\n'
                '\n[dependencies]\nlog = "0.4"\n',
                encoding="utf-8",
            )
            (adapter_dir / "Cargo.toml").write_text(
                '[package]\nname = "featbit-server-sdk-opentelemetry"\n'
                'version = "0.1.0"\n\n[dependencies]\n'
                'featbit-server-sdk = { path = "../..", version = "0.1.0" }\n',
                encoding="utf-8",
            )
            (root / "Cargo.lock").write_text(
                'version = 4\n\n[[package]]\nname = "featbit-server-sdk"\n'
                'version = "0.1.0"\n\n[[package]]\n'
                'name = "featbit-server-sdk-opentelemetry"\n'
                'version = "0.1.0"\n',
                encoding="utf-8",
            )

            version = release.Version.parse("0.2.0-beta.1")
            release.set_workspace_version(root, version)

            self.assertEqual(
                release.read_manifest_version(root / "Cargo.toml"), version
            )
            self.assertEqual(
                release.read_manifest_version(adapter_dir / "Cargo.toml"), version
            )
            self.assertIn(
                'version = "0.2.0-beta.1"',
                (adapter_dir / "Cargo.toml").read_text(encoding="utf-8"),
            )
            lock_text = (root / "Cargo.lock").read_text(encoding="utf-8")
            self.assertEqual(lock_text.count('version = "0.2.0-beta.1"'), 2)


class ReleaseSourceTests(unittest.TestCase):
    def git(self, root: Path, *arguments: str) -> str:
        process = subprocess.run(
            ["git", *arguments],
            cwd=root,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
        )
        return process.stdout.strip()

    def test_detects_normal_source_and_untagged_release_retry(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            base = Path(temporary)
            remote = base / "remote.git"
            repository = base / "repository"
            self.git(base, "init", "--bare", str(remote))
            repository.mkdir()
            self.git(repository, "init", "-b", "main")
            self.git(repository, "config", "user.name", "Release Test")
            self.git(repository, "config", "user.email", "release@example.invalid")
            self.git(repository, "remote", "add", "origin", str(remote))
            (repository / "Cargo.toml").write_text(
                '[package]\nname = "test"\nversion = "0.1.0"\n',
                encoding="utf-8",
            )
            self.git(repository, "add", "Cargo.toml")
            self.git(repository, "commit", "-m", "feat: initial source")
            self.git(repository, "push", "-u", "origin", "main")
            trigger = self.git(repository, "rev-parse", "HEAD")

            source = release.detect_release_source(
                repository, trigger, "refs/heads/main"
            )
            self.assertEqual(source.source_sha, trigger)
            self.assertFalse(source.resume)

            (repository / "Cargo.toml").write_text(
                '[package]\nname = "test"\nversion = "0.1.1"\n',
                encoding="utf-8",
            )
            self.git(repository, "add", "Cargo.toml")
            self.git(
                repository,
                "commit",
                "-m",
                "chore(release): v0.1.1 [skip ci]",
            )
            self.git(repository, "push", "origin", "HEAD:main")
            release_commit = self.git(repository, "rev-parse", "HEAD")

            retry = release.detect_release_source(
                repository, trigger, "refs/heads/main"
            )
            self.assertEqual(retry.source_sha, release_commit)
            self.assertTrue(retry.resume)
            self.assertEqual(str(retry.resume_version), "0.1.1")

            self.git(repository, "tag", "v0.1.1")
            tagged = release.detect_release_source(
                repository, release_commit, "refs/heads/main"
            )
            self.assertFalse(tagged.resume)


class WaitTests(unittest.TestCase):
    def test_wait_returns_after_registry_visibility(self) -> None:
        responses = iter((False, False, True))

        def check(_crate: str, _version: release.Version) -> bool:
            return next(responses)

        release.wait_until_published(
            release.CORE_CRATE,
            release.Version.parse("1.0.0"),
            timeout_seconds=1,
            interval_seconds=0,
            check=check,
        )


if __name__ == "__main__":
    unittest.main()
