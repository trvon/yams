#!/usr/bin/env python3
"""Tests for fail-closed stable/experimental repository publication."""

from __future__ import annotations

import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[2]
MODULE_PATH = ROOT / "scripts" / "ci" / "publish_repository.py"
SPEC = importlib.util.spec_from_file_location("publish_repository", MODULE_PATH)
assert SPEC and SPEC.loader
publish_repository = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = publish_repository
SPEC.loader.exec_module(publish_repository)

PublicationError = publish_repository.PublicationError


class FakeStore:
    def __init__(
        self, existing: dict[str, bytes] | None = None, fail_on: str | None = None
    ):
        self.objects = dict(existing or {})
        self.fail_on = fail_on
        self.puts: list[str] = []

    def get(self, key: str) -> bytes | None:
        return self.objects.get(key)

    def put(self, key: str, source: Path, content_type: str) -> None:
        del content_type
        if key == self.fail_on:
            raise PublicationError(f"injected failure for {key}")
        self.objects[key] = source.read_bytes()
        self.puts.append(key)


class RepositoryPublicationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.root = Path(self.tempdir.name)
        for directory in publish_repository.REPOSITORY_DIRS:
            (self.root / directory).mkdir()
        (self.root / "aptrepo" / "pool").mkdir()
        (self.root / "aptrepo" / "dists").mkdir()
        (self.root / "aptrepo" / "pool" / "yams.deb").write_bytes(b"deb-payload")
        (self.root / "aptrepo" / "dists" / "Release").write_bytes(b"apt-metadata")
        (self.root / "yumrepo" / "yams.rpm").write_bytes(b"rpm-payload")
        (self.root / "yumrepo" / "repomd.xml").write_bytes(b"yum-metadata")
        (self.root / "archrepo" / "yams.pkg.tar.zst").write_bytes(b"arch-payload")
        (self.root / "archrepo" / "yams.db").write_bytes(b"arch-metadata")
        self.public_key = self.root / "public.key"
        self.public_key.write_bytes(b"public-key")

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def manifest(self, channel: str, run_id: int = 42) -> Path:
        path = self.root / "latest.json"
        source_ref = (
            "refs/tags/v1.2.3" if channel == "stable" else "refs/heads/experimental"
        )
        path.write_text(
            json.dumps(
                {
                    "channel": channel,
                    "release_workflow_run_id": run_id,
                    "release_workflow_run_attempt": 3,
                    "source_sha": "a" * 40,
                    "source_ref": source_ref,
                }
            ),
            encoding="utf-8",
        )
        return path

    def plan(self, channel: str, release_channel: str | None = None):
        return publish_repository.build_publication_plan(
            self.root,
            self.manifest(release_channel or channel),
            channel,
            42,
            3,
            self.public_key,
        )

    def test_stable_keys_remain_unprefixed(self) -> None:
        keys = [item.key for item in self.plan("stable")]
        self.assertIn("aptrepo/pool/yams.deb", keys)
        self.assertIn("yumrepo/yams.rpm", keys)
        self.assertIn("archrepo/yams.pkg.tar.zst", keys)
        self.assertIn("gpg.key", keys)
        self.assertEqual(keys[-1], "latest.json")
        self.assertFalse(any(key.startswith("experimental/") for key in keys))

    def test_experimental_keys_are_strictly_channel_prefixed(self) -> None:
        plan = self.plan("experimental", "nightly")
        allowed = (
            "experimental/aptrepo/",
            "experimental/yumrepo/",
            "experimental/archrepo/",
        )
        for item in plan:
            self.assertTrue(
                item.key == "experimental/latest.json" or item.key.startswith(allowed)
            )
        self.assertIn("experimental/aptrepo/gpg.key", [item.key for item in plan])
        self.assertEqual(plan[-1].key, "experimental/latest.json")

    def test_stable_and_experimental_plans_share_no_object_keys(self) -> None:
        stable_keys = {item.key for item in self.plan("stable")}
        experimental_keys = {item.key for item in self.plan("experimental", "nightly")}
        self.assertTrue(stable_keys.isdisjoint(experimental_keys))

    def test_missing_manifest_channel_is_rejected(self) -> None:
        manifest = self.manifest("nightly")
        data = json.loads(manifest.read_text(encoding="utf-8"))
        del data["channel"]
        manifest.write_text(json.dumps(data), encoding="utf-8")
        with self.assertRaisesRegex(PublicationError, "missing or invalid channel"):
            publish_repository.build_publication_plan(
                self.root, manifest, "experimental", 42, 3, self.public_key
            )

    def test_wrong_channel_is_rejected(self) -> None:
        with self.assertRaisesRegex(PublicationError, "does not match"):
            self.plan("stable", "nightly")

    def test_wrong_run_id_is_rejected(self) -> None:
        manifest = self.manifest("stable", run_id=41)
        with self.assertRaisesRegex(PublicationError, "does not match selected run"):
            publish_repository.build_publication_plan(
                self.root, manifest, "stable", 42, 3, self.public_key
            )

    def test_wrong_run_attempt_is_rejected(self) -> None:
        manifest = self.manifest("stable")
        with self.assertRaisesRegex(
            PublicationError, "does not match selected attempt"
        ):
            publish_repository.build_publication_plan(
                self.root, manifest, "stable", 42, 2, self.public_key
            )

    def test_experimental_manifest_with_nonexperimental_ref_is_rejected(self) -> None:
        manifest = self.manifest("nightly")
        data = json.loads(manifest.read_text(encoding="utf-8"))
        data["source_ref"] = "refs/heads/main"
        manifest.write_text(json.dumps(data), encoding="utf-8")
        with self.assertRaisesRegex(PublicationError, "refs/heads/experimental"):
            publish_repository.build_publication_plan(
                self.root, manifest, "experimental", 42, 3, self.public_key
            )

    def test_identical_immutable_put_is_idempotent(self) -> None:
        plan = self.plan("experimental", "weekly")
        package = next(item for item in plan if item.key.endswith("yams.deb"))
        store = FakeStore({package.key: package.source.read_bytes()})
        publish_repository.publish(plan, store)
        self.assertNotIn(package.key, store.puts)
        self.assertEqual(store.puts[-1], "experimental/latest.json")

    def test_conflicting_immutable_put_fails_before_metadata_and_latest(self) -> None:
        plan = self.plan("experimental", "nightly")
        package = next(item for item in plan if item.key.endswith("yams.deb"))
        store = FakeStore({package.key: b"different bytes"})
        with self.assertRaisesRegex(PublicationError, "immutable object conflict"):
            publish_repository.publish(plan, store)
        self.assertFalse(any("dists/" in key for key in store.puts))
        self.assertNotIn("experimental/latest.json", store.puts)

    def test_interrupted_metadata_publication_does_not_advance_latest(self) -> None:
        plan = self.plan("stable")
        metadata = next(item for item in plan if item.phase == "metadata")
        store = FakeStore(fail_on=metadata.key)
        with self.assertRaisesRegex(PublicationError, "injected failure"):
            publish_repository.publish(plan, store)
        self.assertNotIn("latest.json", store.puts)

    def test_interrupted_latest_write_recovers_on_idempotent_rerun(self) -> None:
        plan = self.plan("experimental", "nightly")
        latest_key = "experimental/latest.json"
        store = FakeStore(fail_on=latest_key)
        with self.assertRaisesRegex(PublicationError, "injected failure"):
            publish_repository.publish(plan, store)
        self.assertNotIn(latest_key, store.objects)

        store.fail_on = None
        store.puts.clear()
        publish_repository.publish(plan, store)
        self.assertEqual(store.puts[-1], latest_key)
        self.assertEqual(
            store.objects[latest_key], self.manifest("nightly").read_bytes()
        )

    def test_payloads_then_metadata_then_latest(self) -> None:
        plan = self.plan("stable")
        phases = [item.phase for item in plan]
        order = {"payload": 0, "metadata": 1, "latest": 2}
        self.assertEqual(phases, sorted(phases, key=lambda phase: order[phase]))
        store = FakeStore()
        publish_repository.publish(plan, store)
        self.assertEqual(store.puts[-1], "latest.json")

    def test_symlink_is_rejected(self) -> None:
        target = self.root / "outside"
        target.write_bytes(b"outside")
        symlink = self.root / "aptrepo" / "escape.deb"
        try:
            symlink.symlink_to(target)
        except OSError:
            self.skipTest("symlinks unavailable")
        with self.assertRaisesRegex(PublicationError, "unsafe symlink"):
            self.plan("stable")

    def test_wrangler_get_uses_remote_file_semantics(self) -> None:
        result = publish_repository.subprocess.CompletedProcess(
            args=[],
            returncode=1,
            stdout="",
            stderr="The specified key does not exist",
        )
        store = publish_repository.WranglerObjectStore(Path("/wrangler"), "bucket")
        with mock.patch.object(
            publish_repository.subprocess, "run", return_value=result
        ) as run:
            self.assertIsNone(store.get("experimental/aptrepo/pkg.deb"))

        command = run.call_args.args[0]
        self.assertEqual(
            command[:5],
            [
                "/wrangler",
                "r2",
                "object",
                "get",
                "bucket/experimental/aptrepo/pkg.deb",
            ],
        )
        self.assertIn("--remote", command)
        self.assertIn("--file", command)

    def test_wrangler_put_uses_noninteractive_remote_file_semantics(self) -> None:
        source = self.root / "payload.deb"
        source.write_bytes(b"payload")
        result = publish_repository.subprocess.CompletedProcess(args=[], returncode=0)
        store = publish_repository.WranglerObjectStore(Path("/wrangler"), "bucket")
        with mock.patch.object(
            publish_repository.subprocess, "run", return_value=result
        ) as run:
            store.put("aptrepo/payload.deb", source, "application/octet-stream")

        command = run.call_args.args[0]
        self.assertEqual(
            command[:5],
            [
                "/wrangler",
                "r2",
                "object",
                "put",
                "bucket/aptrepo/payload.deb",
            ],
        )
        self.assertIn("--remote", command)
        self.assertEqual(command[command.index("--file") + 1], str(source))
        self.assertEqual(
            command[command.index("--content-type") + 1],
            "application/octet-stream",
        )
        self.assertIn("--force", command)


if __name__ == "__main__":
    unittest.main()
