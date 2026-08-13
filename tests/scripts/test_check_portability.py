#!/usr/bin/env python3
"""Unit tests for the cross-platform portability policy scanner."""

from __future__ import annotations

import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path

SCRIPT = Path(__file__).with_name("check_portability.py")
SPEC = importlib.util.spec_from_file_location("check_portability", SCRIPT)
assert SPEC and SPEC.loader
POLICY = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = POLICY
SPEC.loader.exec_module(POLICY)


class PortabilityPolicyTests(unittest.TestCase):
    def scan_source(self, source: str, suffix: str = ".cpp"):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / "tests" / f"sample{suffix}"
            path.parent.mkdir(parents=True)
            path.write_text(source, encoding="utf-8")
            return POLICY.scan(root, ("tests",))

    def test_detects_all_three_rule_classes(self):
        findings = self.scan_source(
            """
            void f(std::filesystem::path p) {
                std::jthread worker{[] {}};
                sqlite3_open(p.c_str(), nullptr);
                const char* v = std::getenv("YAMS_X");
            }
            """
        )
        self.assertEqual(
            {finding.rule for finding in findings},
            {"jthread", "path-cstr", "env-read"},
        )

    def test_masks_comments_literals_and_boundary_helpers(self):
        findings = self.scan_source(
            r"""
            // std::jthread worker;
            /* std::getenv("YAMS_X"); */
            const char* raw = "sqlite3_open(path.c_str())";
            auto a = yams::config::getenv_optional("YAMS_X");
            auto b = yams::config::getenv_nonempty("YAMS_X");
            """
        )
        self.assertEqual(findings, [])

    def test_digit_separators_do_not_hide_later_violations(self):
        findings = self.scan_source(
            """
            void f() {
                auto big = 1'000'000;
                std::jthread worker{[] {}};
                std::getenv("YAMS_X");
            }
            """
        )
        self.assertEqual(
            {finding.rule for finding in findings},
            {"jthread", "env-read"},
        )

    def test_path_cstr_detects_member_and_call_forms(self):
        findings = self.scan_source(
            """
            void f() {
                sqlite3_open(obj.path.c_str(), nullptr);
                sqlite3_open(getPath().c_str(), nullptr);
                sqlite3_open((path).c_str(), nullptr);
            }
            """
        )
        self.assertEqual(
            {finding.rule for finding in findings},
            {"path-cstr"},
        )
        self.assertEqual(len(findings), 3)

    def test_path_cstr_skips_string_conversion_but_flags_mixed_ternary(self):
        findings = self.scan_source(
            """
            void f() {
                sqlite3_open(path.string().c_str(), nullptr);
                sqlite3_open(flag ? safe.string().c_str() : path.c_str(), nullptr);
            }
            """
        )
        self.assertEqual(
            {finding.rule for finding in findings},
            {"path-cstr"},
        )
        self.assertEqual(len(findings), 1)

    def test_exempts_only_central_boundary(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            boundary = root / POLICY.CENTRAL_BOUNDARY
            boundary.parent.mkdir(parents=True)
            boundary.write_text('void f() { std::getenv("YAMS_X"); }', encoding="utf-8")
            self.assertEqual(POLICY.scan(root, ("src",)), [])

    def test_allowlist_requires_reasons_and_stale_entries_are_observable(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "tests" / "sample.cpp"
            source.parent.mkdir(parents=True)
            source.write_text('void f() { std::jthread w{[] {}}; }', encoding="utf-8")
            finding = POLICY.scan(root, ("tests",))[0]
            allowlist_path = root / "allowlist.txt"
            allowlist_path.write_text(
                f"{finding.path}:{finding.line}:portability/{finding.rule} # reviewed\n",
                encoding="utf-8",
            )
            allowlist = POLICY.load_allowlist(allowlist_path)
            self.assertIn(finding.allowlist_key, allowlist)

            source.write_text("void f() {}\n", encoding="utf-8")
            live_keys = {item.allowlist_key for item in POLICY.scan(root, ("tests",))}
            self.assertEqual(set(allowlist) - live_keys, {finding.allowlist_key})

            allowlist_path.write_text(
                f"{finding.path}:{finding.line}:portability/{finding.rule}\n",
                encoding="utf-8",
            )
            with self.assertRaises(ValueError):
                POLICY.load_allowlist(allowlist_path)


if __name__ == "__main__":
    unittest.main()
