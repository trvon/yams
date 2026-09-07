#!/usr/bin/env python3
"""Tests for the configuration key usage report."""

from __future__ import annotations

import importlib.util
import tempfile
import unittest
from pathlib import Path

SCRIPT = Path(__file__).with_name("report_config_key_usage.py")
SPEC = importlib.util.spec_from_file_location("report_config_key_usage", SCRIPT)
assert SPEC and SPEC.loader
REPORT = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(REPORT)


class ConfigKeyUsageReportTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        for rel in ("src", "tests/scripts", "tests/benchmarks", "docs", "plugins", "build"):
            (self.root / rel).mkdir(parents=True)
        (self.root / "src" / "knobs.cpp").write_text(
            'getenv("YAMS_ONLY_IN_SRC"); getenv("YAMS_BENCHED"); getenv("YAMS_DOCUMENTED");\n'
            'resolve("search.only_in_src"); resolve("search.tested");\n',
            encoding="utf-8",
        )
        (self.root / "tests" / "scripts" / "production_environment_keys.txt").write_text(
            "# allowlist\nYAMS_ONLY_IN_SRC\nYAMS_BENCHED\nYAMS_DOCUMENTED\nYAMS_STALE\n"
            "YAMS_BENCHED_LONGER\n",
            encoding="utf-8",
        )
        (self.root / "tests" / "benchmarks" / "arms.py").write_text(
            'ARMS = {"YAMS_BENCHED": "1", "YAMS_BENCHED_LONGER": "2"}\n', encoding="utf-8"
        )
        (self.root / "tests" / "unit_test.cpp").write_text(
            'cfg = "search.tested = 3";\n', encoding="utf-8"
        )
        (self.root / "docs" / "config.md").write_text("Set `YAMS_DOCUMENTED`.\n",
                                                        encoding="utf-8")
        # Build output must never count as a consumer.
        (self.root / "build" / "generated.cpp").write_text(
            'getenv("YAMS_ONLY_IN_SRC"); "search.only_in_src"\n', encoding="utf-8"
        )

    def tearDown(self) -> None:
        self.temp.cleanup()

    def rows(self) -> dict[str, dict]:
        env_keys = REPORT.read_allowlist(
            self.root / "tests" / "scripts" / "production_environment_keys.txt"
        )
        return {r["key"]: r for r in REPORT.build_report(
            self.root, env_keys, ["search.only_in_src", "search.tested"])}

    def test_verdicts(self) -> None:
        rows = self.rows()
        self.assertEqual(rows["YAMS_ONLY_IN_SRC"]["verdict"], "CANDIDATE")
        self.assertEqual(rows["YAMS_BENCHED"]["verdict"], "KEEP")
        self.assertEqual(rows["YAMS_DOCUMENTED"]["verdict"], "KEEP")
        self.assertEqual(rows["YAMS_STALE"]["verdict"], "NOT_IN_SRC")
        self.assertEqual(rows["search.only_in_src"]["verdict"], "CANDIDATE")
        self.assertEqual(rows["search.tested"]["verdict"], "KEEP")

    def test_counts_are_whole_token(self) -> None:
        rows = self.rows()
        # YAMS_BENCHED must not absorb YAMS_BENCHED_LONGER's bench reference.
        self.assertEqual(rows["YAMS_BENCHED"]["bench"], 1)
        self.assertEqual(rows["YAMS_BENCHED_LONGER"]["verdict"], "NOT_IN_SRC")

    def test_allowlist_and_build_output_are_not_consumers(self) -> None:
        rows = self.rows()
        self.assertEqual(rows["YAMS_ONLY_IN_SRC"]["test"], 0)
        self.assertEqual(rows["YAMS_ONLY_IN_SRC"]["src"], 1)

    def test_resolver_literal_extraction(self) -> None:
        resolver = self.root / "src" / "knobs.cpp"
        self.assertEqual(REPORT.resolver_toml_keys(resolver),
                         ["search.only_in_src", "search.tested"])


if __name__ == "__main__":
    unittest.main()
