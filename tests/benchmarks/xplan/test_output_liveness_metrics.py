#!/usr/bin/env python3
"""Contracts for known-hit output liveness under daemon-generated load."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

XPLAN_ROOT = Path(__file__).resolve().parent
if str(XPLAN_ROOT) not in sys.path:
    sys.path.insert(0, str(XPLAN_ROOT))

from model import ExperimentPlan  # noqa: E402
from workers.multi_client import (  # noqa: E402
    _has_output_liveness_telemetry,
    _metrics_from_record,
)


class OutputLivenessMetricExtractionTests(unittest.TestCase):
    def test_known_hit_outcomes_survive_metric_extraction(self) -> None:
        record = {
            "elapsed_seconds": 2.0,
            "drain_metrics": {
                "drained": True,
                "elapsed_ms": 1_250,
                "minimum_documents": 42,
                "peak_post_ingest_queued": 3,
                "peak_post_ingest_inflight": 16,
                "peak_worker_active": 4,
                "peak_worker_queued": 7,
                "peak_deferred_queue_depth": 2,
                "final_snapshot": {"documents_total": 42},
            },
            "daemon_snapshot_final": {
                "post_commit_content_index_calls": 8,
                "post_commit_content_index_total_ms": 200,
                "post_commit_content_index_max_ms": 40,
            },
            "output_liveness": {
                "search": {
                    "attempts": 10,
                    "nonempty_successes": 7,
                    "empty_successes": 2,
                    "errors": 1,
                },
                "grep": {
                    "attempts": 8,
                    "nonempty_successes": 8,
                    "empty_successes": 0,
                    "errors": 0,
                },
                "graph": {
                    "attempts": 8,
                    "nonempty_successes": 6,
                    "empty_successes": 1,
                    "errors": 1,
                },
            },
        }

        metrics = _metrics_from_record(record)

        self.assertEqual(metrics["search_known_hit_attempts"], 10.0)
        self.assertEqual(metrics["search_known_hit_nonempty_rate"], 0.7)
        self.assertEqual(metrics["search_known_hit_empty_success_rate"], 0.2)
        self.assertEqual(metrics["search_known_hit_error_rate"], 0.1)
        self.assertEqual(metrics["grep_known_hit_nonempty_rate"], 1.0)
        self.assertEqual(metrics["graph_known_hit_empty_success_rate"], 0.125)
        self.assertEqual(metrics["known_hit_attempts"], 26.0)
        self.assertEqual(metrics["known_hit_nonempty_rate"], 21.0 / 26.0)
        self.assertEqual(metrics["known_hit_empty_success_rate"], 3.0 / 26.0)
        self.assertEqual(metrics["known_hit_error_rate"], 2.0 / 26.0)
        self.assertEqual(metrics["output_liveness_contract_pass"], 0.0)
        self.assertEqual(metrics["drain_elapsed_ms"], 1_250.0)
        self.assertEqual(metrics["workload_completion_ms"], 3_250.0)
        self.assertEqual(metrics["drain_post_ingest_queued_peak"], 3.0)
        self.assertEqual(metrics["drain_post_ingest_inflight_peak"], 16.0)
        self.assertEqual(metrics["drain_worker_active_peak"], 4.0)
        self.assertEqual(metrics["drain_worker_queued_peak"], 7.0)
        self.assertEqual(metrics["drain_deferred_queue_depth_peak"], 2.0)
        self.assertEqual(metrics["drain_minimum_documents"], 42.0)
        self.assertEqual(metrics["drain_final_documents"], 42.0)
        self.assertEqual(metrics["drain_document_completeness"], 1.0)
        self.assertEqual(metrics["drain_contract_pass"], 1.0)
        self.assertEqual(metrics["post_commit_content_index_calls"], 8.0)
        self.assertEqual(metrics["post_commit_content_index_total_ms"], 200.0)
        self.assertEqual(metrics["post_commit_content_index_avg_ms"], 25.0)
        self.assertEqual(metrics["post_commit_content_index_max_ms"], 40.0)
        self.assertTrue(_has_output_liveness_telemetry(record))

    def test_zero_empty_and_error_outcomes_pass_contract(self) -> None:
        record = {
            "output_liveness": {
                surface: {
                    "attempts": 4,
                    "nonempty_successes": 4,
                    "empty_successes": 0,
                    "errors": 0,
                }
                for surface in ("search", "grep", "graph")
            }
        }

        metrics = _metrics_from_record(record)

        self.assertEqual(metrics["output_liveness_contract_pass"], 1.0)

    def test_missing_surface_is_not_valid_telemetry(self) -> None:
        self.assertFalse(
            _has_output_liveness_telemetry(
                {
                    "output_liveness": {
                        "search": {},
                        "grep": {},
                    }
                }
            )
        )


class OutputLivenessLoadPlanTests(unittest.TestCase):
    def test_plan_generates_decision_grade_heavy_load(self) -> None:
        plan = ExperimentPlan.load(
            XPLAN_ROOT / "plans" / "retrieval_output_under_load.json"
        )

        self.assertEqual(plan.baseline, "control")
        self.assertEqual(plan.repeats, 3)
        self.assertEqual(plan.steps[0].worker, "retrieval_load")
        self.assertTrue(plan.fixed_params["profile_hydration_surfaces"])
        self.assertEqual(plan.fixed_params["search_query"], "architecture")
        self.assertEqual(plan.fixed_params["resource_sample_ms"], 50)
        self.assertIn("output_liveness_contract_pass", plan.steps[0].metrics)

        arms = {arm.name: arm.factors for arm in plan.arms}
        self.assertLess(
            arms["control"]["search_clients"],
            arms["heavy_read"]["search_clients"],
        )
        self.assertGreaterEqual(
            arms["heavy_read"]["mixed_ops_per_client"],
            200,
        )
        self.assertGreaterEqual(
            arms["heavy_write"]["mixed_ops_per_client"],
            200,
        )


if __name__ == "__main__":
    unittest.main()
