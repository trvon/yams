#!/usr/bin/env python3
"""Tests for the configuration-authority policy scanner."""

from __future__ import annotations

import importlib.util
import tempfile
import unittest
from pathlib import Path

SCRIPT = Path(__file__).with_name("check_configuration_authority.py")
SPEC = importlib.util.spec_from_file_location("configuration_authority", SCRIPT)
assert SPEC and SPEC.loader
POLICY = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(POLICY)


class ConfigurationAuthorityPolicyTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        (self.root / "src").mkdir()
        self.env_allowlist = self.root / "environment.txt"
        self.reader_allowlist = self.root / "readers.txt"
        self.env_allowlist.write_text("YAMS_EXISTING\n", encoding="utf-8")
        self.reader_allowlist.write_text(
            "src/config.cpp:parseSimpleTomlFlat\n", encoding="utf-8"
        )

    def tearDown(self) -> None:
        self.temp.cleanup()

    def test_reviewed_environment_key_and_reader_pass(self) -> None:
        (self.root / "src" / "config.cpp").write_text(
            'const char* key = "YAMS_EXISTING";\n'
            "Config parseSimpleTomlFlat(const Path&) { return {}; }\n",
            encoding="utf-8",
        )
        self.assertEqual(
            POLICY.run(self.root, self.env_allowlist, self.reader_allowlist), 0
        )

    def test_new_environment_key_fails(self) -> None:
        (self.root / "src" / "feature.cpp").write_text(
            'auto value = getenv("YAMS_NEW_PRODUCT_KNOB");\n', encoding="utf-8"
        )
        self.assertEqual(
            POLICY.run(self.root, self.env_allowlist, self.reader_allowlist), 1
        )

    def test_new_toml_reader_fails(self) -> None:
        (self.root / "src" / "duplicate.cpp").write_text(
            "Config parseAnotherToml(const Path&) { return {}; }\n", encoding="utf-8"
        )
        self.assertEqual(
            POLICY.run(self.root, self.env_allowlist, self.reader_allowlist), 1
        )

    def test_stale_environment_key_fails(self) -> None:
        # Nothing in src/ mentions YAMS_EXISTING any more: the allowlist must shrink.
        (self.root / "src" / "config.cpp").write_text(
            "Config parseSimpleTomlFlat(const Path&) { return {}; }\n", encoding="utf-8"
        )
        self.assertEqual(
            POLICY.run(self.root, self.env_allowlist, self.reader_allowlist), 1
        )

    def test_stale_reader_entry_fails(self) -> None:
        (self.root / "src" / "config.cpp").write_text(
            'const char* key = "YAMS_EXISTING";\n', encoding="utf-8"
        )
        self.assertEqual(
            POLICY.run(self.root, self.env_allowlist, self.reader_allowlist), 1
        )

    def test_stale_entries_are_named(self) -> None:
        (self.root / "src" / "config.cpp").write_text("int x;\n", encoding="utf-8")
        stale_keys, stale_readers = POLICY.stale_entries(
            self.root, self.env_allowlist, self.reader_allowlist
        )
        self.assertEqual(stale_keys, ["YAMS_EXISTING"])
        self.assertEqual(stale_readers, ["src/config.cpp:parseSimpleTomlFlat"])


if __name__ == "__main__":
    unittest.main()
