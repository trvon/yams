#!/usr/bin/env python3
# Copyright (c) 2026 YAMS Contributors
# SPDX-License-Identifier: GPL-3.0-or-later
"""Unit tests for scripts/ci/prune_prereleases.py selection logic."""

import datetime as dt
import importlib.util
import pathlib
import sys
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location("prune", ROOT / "scripts/ci/prune_prereleases.py")
prune = importlib.util.module_from_spec(spec)
sys.modules["prune"] = prune
spec.loader.exec_module(prune)

TODAY = dt.date(2026, 10, 27)
SHA = "8dd0ef1c337a3dcb1ef8e937d0e81cf5691fb620"


def release(rid, tag, published, draft=False):
    return {"id": rid, "tag_name": tag, "draft": draft, "prerelease": True,
            "published_at": published}


class SelectTest(unittest.TestCase):
    def test_keeps_newest_per_channel(self):
        releases = [
            release(1, f"experimental-nightly-20261001-{SHA}", "2026-10-01T08:00:00Z"),
            release(2, f"experimental-nightly-20261002-{SHA}", "2026-10-02T08:00:00Z"),
            release(3, "nightly-20260908-34b61b22", "2026-09-08T08:00:00Z"),
            release(4, "weekly-2026w40-abcdef1", "2026-09-28T08:00:00Z"),
            release(5, "weekly-2026w41-abcdef2", "2026-10-05T08:00:00Z"),
        ]
        tags = [r["tag_name"] for r in releases]
        ids, del_tags = prune.select(releases, tags, keep=1, today=TODAY)
        self.assertEqual(sorted(ids), [1, 3, 4])
        self.assertNotIn(f"experimental-nightly-20261002-{SHA}", del_tags)
        self.assertNotIn("weekly-2026w41-abcdef2", del_tags)
        self.assertIn("nightly-20260908-34b61b22", del_tags)

    def test_never_touches_stable_or_unknown_tags(self):
        releases = [
            release(1, "v0.21.1", "2026-10-27T00:00:00Z"),
            release(2, "v0.20.5", "2026-10-03T00:00:00Z"),
            release(3, "yams-v0.1.0", "2025-01-01T00:00:00Z"),
            release(4, "nightly-20250923", "2025-09-23T00:00:00Z"),
        ]
        tags = ["v0.21.0", "v0.21.1", "nightly-20250923", "random"]
        ids, del_tags = prune.select(releases, tags, keep=1, today=TODAY)
        self.assertEqual(ids, [])
        self.assertEqual(del_tags, [])

    def test_drafts_are_not_counted_or_deleted(self):
        releases = [
            release(1, f"experimental-nightly-20261003-{SHA}", None, draft=True),
            release(2, f"experimental-nightly-20261002-{SHA}", "2026-10-02T08:00:00Z"),
        ]
        ids, del_tags = prune.select(releases, [], keep=1, today=TODAY)
        self.assertEqual(ids, [])
        self.assertEqual(del_tags, [])

    def test_old_orphan_tags_deleted_recent_kept(self):
        tags = ["nightly-20251203-069c911", f"experimental-nightly-20261026-{SHA}"]
        ids, del_tags = prune.select([], tags, keep=3, today=TODAY)
        self.assertEqual(ids, [])
        # The 2026-10-26 tag may belong to a release still being created.
        self.assertEqual(del_tags, ["nightly-20251203-069c911"])

    def test_keep_must_be_positive(self):
        with self.assertRaises(ValueError):
            prune.select([], [], keep=0, today=TODAY)

    def test_invalid_dates_are_out_of_scope(self):
        self.assertIsNone(prune.channel_of("nightly-20261399-abcdef1"))
        self.assertIsNone(prune.channel_of("weekly-2026w60-abcdef1"))
        self.assertEqual(prune.channel_of("weekly-2026w41-abcdef1")[0], "weekly")


if __name__ == "__main__":
    unittest.main()
