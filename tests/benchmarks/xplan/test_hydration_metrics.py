#!/usr/bin/env python3
"""Contracts for hydration telemetry preserved by the xplan load pipeline."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

XPLAN_ROOT = Path(__file__).resolve().parent
if str(XPLAN_ROOT) not in sys.path:
    sys.path.insert(0, str(XPLAN_ROOT))

from model import ExperimentPlan  # noqa: E402
from workers.multi_client import _has_hydration_telemetry, _metrics_from_record  # noqa: E402


class HydrationMetricExtractionTests(unittest.TestCase):
    def test_search_grep_and_graph_timings_survive_metric_extraction(self) -> None:
        record = {
            "elapsed_seconds": 2.0,
            "total_searches": 4,
            "search_latency": {"p50_us": 20_000, "p95_us": 40_000},
            "search_server": {
                "timing_snippet_hydration_us": {
                    "count": 4,
                    "p50_us": 4_000,
                    "p95_us": 8_000,
                    "p99_us": 9_000,
                    "max_us": 10_000,
                },
                "timing_snippet_lookup_us": {"count": 4, "p95_us": 2_000},
                "timing_snippet_content_fetch_us": {"count": 4, "p95_us": 5_000},
                "timing_snippet_render_us": {"count": 4, "p95_us": 1_000},
                "snippet_timeout_hits": 1,
            },
            "grep_server": {
                "content_retrieval_ms": {"count": 3, "p95": 12},
                "worker_critical_total_ms": {"count": 3, "p95": 20},
                "match_render_us": {
                    "count": 3,
                    "p50": 700,
                    "p95": 1_500,
                    "p99": 1_900,
                    "max": 2_000,
                },
            },
            "graph_server": {
                "snippet_render_us": {
                    "count": 2,
                    "p50": 2_000,
                    "p95": 3_000,
                    "p99": 3_500,
                    "max": 4_000,
                },
            },
        }

        metrics = _metrics_from_record(record)

        self.assertEqual(metrics["search_snippet_hydration_samples"], 4.0)
        self.assertEqual(metrics["search_snippet_hydration_p50_ms"], 4.0)
        self.assertEqual(metrics["search_snippet_hydration_p95_ms"], 8.0)
        self.assertEqual(metrics["search_snippet_hydration_p99_ms"], 9.0)
        self.assertEqual(metrics["search_snippet_hydration_max_ms"], 10.0)
        self.assertEqual(metrics["search_snippet_lookup_p95_ms"], 2.0)
        self.assertEqual(metrics["search_snippet_content_fetch_p95_ms"], 5.0)
        self.assertEqual(metrics["search_snippet_render_p95_ms"], 1.0)
        self.assertEqual(metrics["search_snippet_timeout_hits"], 1.0)
        self.assertEqual(metrics["search_snippet_timeout_rate"], 0.25)
        self.assertEqual(metrics["grep_content_retrieval_p95_ms"], 12.0)
        self.assertEqual(metrics["grep_worker_critical_p95_ms"], 20.0)
        self.assertEqual(metrics["grep_match_render_p50_ms"], 0.7)
        self.assertEqual(metrics["grep_match_render_p95_ms"], 1.5)
        self.assertEqual(metrics["grep_match_render_p99_ms"], 1.9)
        self.assertEqual(metrics["grep_match_render_max_ms"], 2.0)
        self.assertEqual(metrics["grep_match_render_samples"], 3.0)
        self.assertEqual(metrics["graph_snippet_render_p50_ms"], 2.0)
        self.assertEqual(metrics["graph_snippet_render_p95_ms"], 3.0)
        self.assertEqual(metrics["graph_snippet_render_p99_ms"], 3.5)
        self.assertEqual(metrics["graph_snippet_render_max_ms"], 4.0)
        self.assertEqual(metrics["graph_snippet_render_samples"], 2.0)

    def test_missing_hydration_blocks_are_not_reported_as_telemetry(self) -> None:
        self.assertFalse(_has_hydration_telemetry({"search_server": {}}))


class HydrationPressurePlanTests(unittest.TestCase):
    def test_plan_has_hydrated_baseline_and_paths_only_control(self) -> None:
        plan = ExperimentPlan.load(
            XPLAN_ROOT / "plans" / "retrieval_hydration_pressure.json"
        )

        self.assertEqual(plan.baseline, "hydrated")
        self.assertEqual(plan.repeats, 3)
        self.assertEqual(plan.steps[0].worker, "retrieval_load")
        self.assertTrue(plan.fixed_params["profile_hydration_surfaces"])
        self.assertIn("grep_match_render_p95_ms", plan.steps[0].metrics)
        self.assertIn("graph_snippet_render_p95_ms", plan.steps[0].metrics)
        arms = {arm.name: arm.factors for arm in plan.arms}
        self.assertEqual(arms["hydrated"]["hydrate_snippets"], True)
        self.assertEqual(arms["paths_only_control"]["hydrate_snippets"], False)
        self.assertIn(
            "search_snippet_hydration_p95_ms", plan.steps[0].metrics
        )


if __name__ == "__main__":
    unittest.main()
