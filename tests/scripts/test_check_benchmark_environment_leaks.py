#!/usr/bin/env python3
# Copyright (c) 2025 YAMS Contributors
# SPDX-License-Identifier: GPL-3.0-or-later

from __future__ import annotations

import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path

SCRIPT = Path(__file__).with_name("check_benchmark_environment_leaks.py")
SPEC = importlib.util.spec_from_file_location(
    "check_benchmark_environment_leaks", SCRIPT
)
assert SPEC and SPEC.loader
POLICY = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = POLICY
SPEC.loader.exec_module(POLICY)


class BenchmarkEnvironmentPolicyTest(unittest.TestCase):
    def test_rejects_benchmark_control_in_production_source(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "src" / "daemon" / "service.cpp"
            source.parent.mkdir(parents=True)
            source.write_text(
                'const char* value = std::getenv("YAMS_BENCH_PROFILE");\n',
                encoding="utf-8",
            )

            findings = POLICY.scan(root)

            self.assertEqual(len(findings), 1)
            self.assertEqual(findings[0].path, "src/daemon/service.cpp")
            self.assertEqual(findings[0].name, "YAMS_BENCH_PROFILE")

    def test_allows_benchmark_control_in_benchmark_binary(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "src" / "benchmarks" / "runner.cpp"
            source.parent.mkdir(parents=True)
            source.write_text(
                'const char* value = std::getenv("YAMS_BENCH_ITERS");\n',
                encoding="utf-8",
            )

            self.assertEqual(POLICY.scan(root), [])

    def test_ignores_non_source_files(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            document = root / "src" / "notes.md"
            document.parent.mkdir(parents=True)
            document.write_text("YAMS_BENCH_PROFILE\n", encoding="utf-8")

            self.assertEqual(POLICY.scan(root), [])


if __name__ == "__main__":
    unittest.main()
