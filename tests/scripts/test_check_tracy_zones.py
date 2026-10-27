#!/usr/bin/env python3
"""Tests for check_tracy_zones.py."""

from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import check_tracy_zones as checker  # noqa: E402


def findings(code: str) -> list[checker.Finding]:
    return checker.scan_text("x.cpp", code)


class ScopedZoneTest(unittest.TestCase):
    def test_zone_across_co_await_is_reported(self) -> None:
        code = """
awaitable<void> f() {
    YAMS_ZONE_SCOPED_N("f");
    work();
    co_await g();
}
"""
        result = findings(code)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].zone, "f")
        self.assertEqual(result[0].line, 3)
        self.assertEqual(result[0].suspend_line, 5)

    def test_zone_in_closed_block_is_fine(self) -> None:
        code = """
awaitable<void> f() {
    {
        YAMS_ZONE_SCOPED_N("sync");
        work();
    }
    co_await g();
}
"""
        self.assertEqual(findings(code), [])

    def test_loop_iteration_zone_spanning_wait_is_reported(self) -> None:
        code = """
awaitable<void> loop() {
    while (running) {
        YAMS_ZONE_SCOPED_N("loop");
        tick();
        co_await timer.async_wait(use_awaitable);
    }
}
"""
        self.assertEqual(len(findings(code)), 1)

    def test_co_yield_counts(self) -> None:
        code = """
generator<int> f() {
    ZoneScoped;
    co_yield 1;
}
"""
        self.assertEqual(len(findings(code)), 1)

    def test_nested_coroutine_lambda_is_skipped(self) -> None:
        code = """
void start() {
    YAMS_ZONE_SCOPED_N("start");
    co_spawn(exec, [self]() -> awaitable<void> {
        co_await self->loop();
    }, detached);
}
"""
        self.assertEqual(findings(code), [])

    def test_comment_and_string_mentions_are_ignored(self) -> None:
        code = """
awaitable<void> f() {
    YAMS_ZONE_SCOPED_N("f");
    // co_await later
    log("co_await in text");
}
awaitable<void> g() { co_await h(); }
"""
        self.assertEqual(findings(code), [])

    def test_zone_inside_callback_lambda_is_fine(self) -> None:
        code = """
awaitable<void> f() {
    auto r = co_await retry([&]() {
        YAMS_ZONE_SCOPED_N("db");
        return db.query();
    });
}
"""
        self.assertEqual(findings(code), [])


class SyncZoneTest(unittest.TestCase):
    def test_end_before_suspension_is_fine(self) -> None:
        code = """
awaitable<void> f() {
    YAMS_ZONE_SYNC_N(z, "plan");
    plan();
    YAMS_ZONE_END(z);
    co_await g();
}
"""
        self.assertEqual(findings(code), [])

    def test_missing_end_is_reported(self) -> None:
        code = """
awaitable<void> f() {
    YAMS_ZONE_SYNC_N(z, "plan");
    plan();
    co_await g();
}
"""
        result = findings(code)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].zone, "plan")

    def test_end_only_inside_branch_is_reported(self) -> None:
        code = """
awaitable<void> f() {
    YAMS_ZONE_SYNC_N(z, "plan");
    if (fast) {
        YAMS_ZONE_END(z);
    }
    co_await g();
}
"""
        self.assertEqual(len(findings(code)), 1)

    def test_early_return_before_end_is_fine(self) -> None:
        code = """
awaitable<int> f() {
    YAMS_ZONE_SYNC_N(z, "acquire");
    for (auto& c : pool) {
        if (c.free()) {
            co_return 1;
        }
    }
    YAMS_ZONE_END(z);
    co_return co_await create();
}
"""
        self.assertEqual(findings(code), [])


class ScanTest(unittest.TestCase):
    def test_scan_walks_tree_and_skips_profiling_header(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "src").mkdir()
            (root / "include" / "yams").mkdir(parents=True)
            (root / "src" / "a.cpp").write_text(
                'awaitable<void> f() {\n    YAMS_ZONE_SCOPED_N("a");\n    co_await g();\n}\n')
            (root / "include" / "yams" / "profiling.h").write_text(
                "#define YAMS_ZONE_SCOPED_N(name) ZoneNamedN(x, name, true)\n"
                "// co_await\n")
            result = checker.scan(root)
            self.assertEqual([f.path for f in result], ["src/a.cpp"])
            self.assertEqual(checker.main(["--root", str(root)]), 1)

    def test_clean_tree_exits_zero(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "src").mkdir()
            (root / "src" / "a.cpp").write_text("void f() { YAMS_ZONE_SCOPED_N(\"a\"); }\n")
            self.assertEqual(checker.main(["--root", str(root)]), 0)


if __name__ == "__main__":
    unittest.main()
