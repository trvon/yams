#!/usr/bin/env python3
"""Unit tests for the raw environment mutation policy scanner."""

from __future__ import annotations

import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path

SCRIPT = Path(__file__).with_name("check_raw_environment_calls.py")
SPEC = importlib.util.spec_from_file_location("check_raw_environment_calls", SCRIPT)
assert SPEC and SPEC.loader
POLICY = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = POLICY
SPEC.loader.exec_module(POLICY)


class RawEnvironmentPolicyTests(unittest.TestCase):
    def scan_source(self, source: str, suffix: str = ".cpp"):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / "tests" / f"sample{suffix}"
            path.parent.mkdir(parents=True)
            path.write_text(source, encoding="utf-8")
            return POLICY.scan(root, ("tests",))

    def test_detects_supported_platform_apis_and_alias_references(self):
        findings = self.scan_source(
            """
            void mutate(char* entry) {
                setenv("A", "1", 1);
                unsetenv("A");
                putenv(entry);
                clearenv();
                _putenv("A=1");
                _putenv_s("A", "1");
                _wputenv(L"A=1");
                _wputenv_s(L"A", L"1");
                SetEnvironmentVariable("A", "1");
                SetEnvironmentVariableA("A", "1");
                SetEnvironmentVariableW(L"A", L"1");
                auto raw_mutator = &setenv;
            }
            """
        )
        self.assertEqual(
            {finding.name for finding in findings},
            {
                "setenv",
                "unsetenv",
                "putenv",
                "clearenv",
                "_putenv",
                "_putenv_s",
                "_wputenv",
                "_wputenv_s",
                "SetEnvironmentVariable",
                "SetEnvironmentVariableA",
                "SetEnvironmentVariableW",
            },
        )
        self.assertEqual(sum(finding.name == "setenv" for finding in findings), 2)

    def test_masks_comments_literals_and_unrelated_qualified_functions(self):
        findings = self.scan_source(
            r'''
            // setenv("A", "1", 1);
            /* unsetenv("A"); */
            const char* ordinary = "putenv(A=1)";
            const char* raw = R"tag(clearenv())tag";
            fake::setenv("A", "1", 1);
            '''
        )
        self.assertEqual(findings, [])

    def test_scans_objective_cpp_and_exempts_only_central_boundary(self):
        self.assertEqual(len(self.scan_source('void f() { clearenv(); }', ".mm")), 1)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            boundary = root / POLICY.CENTRAL_BOUNDARY
            boundary.parent.mkdir(parents=True)
            boundary.write_text('void f() { setenv("A", "1", 1); }', encoding="utf-8")
            self.assertEqual(POLICY.scan(root, ("src",)), [])

    def test_allowlist_requires_reasons_and_stale_entries_are_observable(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "tests" / "sample.cpp"
            source.parent.mkdir(parents=True)
            source.write_text('void f() { setenv("A", "1", 1); }', encoding="utf-8")
            finding = POLICY.scan(root, ("tests",))[0]
            allowlist_path = root / "allowlist.txt"
            allowlist_path.write_text(
                f"{finding.path}:{finding.line}:raw-env/{finding.name} # reviewed test\n",
                encoding="utf-8",
            )
            allowlist = POLICY.load_allowlist(allowlist_path)
            self.assertIn(finding.allowlist_key, allowlist)

            source.write_text("void f() {}\n", encoding="utf-8")
            live_keys = {item.allowlist_key for item in POLICY.scan(root, ("tests",))}
            self.assertEqual(set(allowlist) - live_keys, {finding.allowlist_key})

            allowlist_path.write_text(
                f"{finding.path}:{finding.line}:raw-env/{finding.name}\n", encoding="utf-8"
            )
            with self.assertRaises(ValueError):
                POLICY.load_allowlist(allowlist_path)


if __name__ == "__main__":
    unittest.main()
