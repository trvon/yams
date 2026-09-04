#!/usr/bin/env python3
"""Unit tests for the release manifest generator."""

from __future__ import annotations

import hashlib
import importlib.util
import json
import tempfile
import unittest
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
MODULE_PATH = ROOT / "scripts/ci/generate_release_manifest.py"
SPEC = importlib.util.spec_from_file_location("generate_release_manifest", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
manifest_module = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(manifest_module)

FULL_SHA = "a" * 40


class ReleaseManifestTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.assets_dir = Path(self.temp_dir.name) / "assets"
        self.assets_dir.mkdir()
        self.asset = self.assets_dir / "yams-nightly-linux-x86_64.tar.gz"
        self.asset.write_bytes(b"release payload")
        self.digest = hashlib.sha256(self.asset.read_bytes()).hexdigest()
        self.checksums = self.assets_dir / "SHA256SUMS"
        self.checksums.write_text(
            f"{self.digest}  {self.asset.name}\n", encoding="utf-8"
        )

    def build(self, **overrides: object) -> dict[str, Any]:
        arguments: dict[str, object] = {
            "assets_dir": self.assets_dir,
            "checksums_path": self.checksums,
            "channel": "nightly",
            "version": "nightly-20260101-aaaaaaa",
            "tag": f"experimental-nightly-20260101-{FULL_SHA}",
            "source_sha": FULL_SHA,
            "source_ref": "refs/heads/experimental",
            "repository": "trvon/yams",
            "github_server_url": "https://github.com",
            "release_run_id": "123",
            "release_run_attempt": "2",
            "release_run_url": "https://github.com/trvon/yams/actions/runs/123/attempts/2",
            "tests_run_id": "99",
            "tests_run_attempt": "3",
            "tests_run_url": "https://github.com/trvon/yams/actions/runs/99/attempts/3",
            "ipc_protocol_version": 2,
            "p2p_protocol_version": 4,
            "published_at": "2026-01-01T00:00:00Z",
        }
        arguments.update(overrides)
        return manifest_module.build_manifest(**arguments)

    def test_manifest_records_immutable_provenance_and_asset_metadata(self) -> None:
        manifest = self.build()

        self.assertEqual(manifest["schema_version"], 1)
        self.assertEqual(manifest["source_sha"], FULL_SHA)
        self.assertEqual(manifest["source_ref"], "refs/heads/experimental")
        self.assertEqual(manifest["release_workflow_run_attempt"], 2)
        self.assertEqual(manifest["tests_workflow_run_id"], 99)
        self.assertEqual(manifest["tests_workflow_run_attempt"], 3)
        self.assertEqual(manifest["ipc_protocol_version"], 2)
        self.assertEqual(manifest["p2p_protocol_version"], 4)
        self.assertEqual(len(manifest["assets"]), 1)
        asset = manifest["assets"][0]
        self.assertEqual(asset["name"], self.asset.name)
        self.assertEqual(asset["filename"], self.asset.name)
        self.assertEqual(asset["size_bytes"], len(b"release payload"))
        self.assertEqual(asset["architecture"], "x86_64")
        self.assertEqual(asset["sha256"], self.digest)
        self.assertEqual(
            asset["url"],
            "https://github.com/trvon/yams/releases/download/"
            f"experimental-nightly-20260101-{FULL_SHA}/{self.asset.name}",
        )

    def test_stable_manifest_does_not_require_tests_run(self) -> None:
        manifest = self.build(
            channel="stable",
            version="1.2.3",
            tag="v1.2.3",
            source_ref="refs/tags/v1.2.3",
            tests_run_id="",
            tests_run_attempt="",
            tests_run_url="",
        )
        self.assertIsNone(manifest["tests_workflow_run_id"])
        self.assertIsNone(manifest["tests_workflow_run_url"])

    def test_rejects_non_full_source_sha(self) -> None:
        with self.assertRaisesRegex(ValueError, "full 40-character source SHA"):
            self.build(source_sha="abc1234")

    def test_rejects_missing_experimental_tests_run(self) -> None:
        with self.assertRaisesRegex(ValueError, "Tests workflow run"):
            self.build(tests_run_id="", tests_run_attempt="", tests_run_url="")

    def test_rejects_mutable_tests_attempt_url(self) -> None:
        with self.assertRaisesRegex(ValueError, "exact run attempt"):
            self.build(tests_run_url="https://github.com/trvon/yams/actions/runs/99")

    def test_rejects_unknown_or_ambiguous_architecture(self) -> None:
        unknown = self.assets_dir / "yams-nightly-linux-ppc64le.tar.gz"
        self.asset.rename(unknown)
        self.checksums.write_text(f"{self.digest}  {unknown.name}\n", encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "unknown architecture"):
            self.build()

        unknown.unlink()
        ambiguous = self.assets_dir / "yams-linux-x86_64-aarch64.tar.gz"
        ambiguous.write_bytes(b"release payload")
        self.checksums.write_text(
            f"{self.digest}  {ambiguous.name}\n", encoding="utf-8"
        )
        with self.assertRaisesRegex(ValueError, "ambiguous architecture"):
            self.build()

    def test_rejects_duplicate_checksum_names(self) -> None:
        self.checksums.write_text(
            f"{self.digest}  {self.asset.name}\n{self.digest}  {self.asset.name}\n",
            encoding="utf-8",
        )
        with self.assertRaisesRegex(ValueError, "duplicate asset name"):
            self.build()

    def test_rejects_checksum_mismatch_and_missing_assets(self) -> None:
        self.checksums.write_text(f"{'0' * 64}  {self.asset.name}\n", encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "checksum mismatch"):
            self.build()

        self.asset.unlink()
        with self.assertRaisesRegex(ValueError, "missing asset"):
            self.build()

    def test_rejects_unchecksummed_distributables_and_mutable_urls(self) -> None:
        extra = self.assets_dir / "yams-nightly-macos-arm64.zip"
        extra.write_bytes(b"extra")
        with self.assertRaisesRegex(ValueError, "missing from SHA256SUMS"):
            self.build()

        extra.unlink()
        with self.assertRaisesRegex(ValueError, "mutable release tag"):
            self.build(channel="stable", tag="latest")

    def test_write_manifest_is_stable_json(self) -> None:
        output = self.assets_dir / "latest.json"
        manifest_module.write_manifest(output, self.build())
        loaded = json.loads(output.read_text(encoding="utf-8"))
        self.assertEqual(loaded["schema_version"], 1)
        self.assertTrue(output.read_text(encoding="utf-8").endswith("\n"))


if __name__ == "__main__":
    unittest.main()
