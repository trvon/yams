#!/usr/bin/env python3
"""Behavior contracts for xplan experiment identity and reporting."""

from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path

XPLAN_ROOT = Path(__file__).resolve().parent
if str(XPLAN_ROOT) not in sys.path:
    sys.path.insert(0, str(XPLAN_ROOT))

from analyze_query_class import load_by_type  # noqa: E402
from report import _baseline_row  # noqa: E402
from workers.retrieval_quality import (  # noqa: E402
    _merge_benchmark_env,
    parse_debug_jsonl,
)


class RetrievalQualityEnvironmentTests(unittest.TestCase):
    def test_query_class_loader_ignores_missing_or_directory_paths(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            self.assertEqual(load_by_type(Path(tmp)), {})
            self.assertEqual(load_by_type(Path(tmp) / "missing.jsonl"), {})

    def test_declared_params_override_ambient_and_arm_env_wins(self) -> None:
        merged = _merge_benchmark_env(
            ambient={
                "YAMS_BENCH_DATASET": "scifact",
                "YAMS_BENCH_CORPUS_SIZE": "2000",
                "UNRELATED": "kept",
            },
            declared_env={"YAMS_BENCH_CORPUS_SIZE": "500"},
            params={"dataset": "nfcorpus", "corpus_size": 0},
            param_env={
                "dataset": "YAMS_BENCH_DATASET",
                "corpus_size": "YAMS_BENCH_CORPUS_SIZE",
            },
        )

        self.assertEqual(merged["YAMS_BENCH_DATASET"], "nfcorpus")
        self.assertEqual(merged["YAMS_BENCH_CORPUS_SIZE"], "500")
        self.assertEqual(merged["UNRELATED"], "kept")

    def test_explicit_arm_config_cannot_be_shadowed_by_ambient_config_path(self) -> None:
        merged = _merge_benchmark_env(
            ambient={"YAMS_CONFIG_PATH": "/tmp/developer-config.toml"},
            declared_env={},
            params={},
            param_env={},
        )

        self.assertEqual(merged["YAMS_CONFIG_PATH"], "")

    def test_routing_budget_metrics_are_aggregated_from_query_debug(self) -> None:
        events = [
            {
                "search_type": "hybrid",
                "search_stats": {
                    "topology_weak_query_narrow_applied": "1",
                    "topology_route_available_count": "3",
                    "topology_route_boundary_score_margin": "0.25",
                    "topology_route_confidence_abstained": "0",
                    "topology_snapshot_cache_hit": "0",
                    "topology_weak_query_allowed_candidates": "8",
                    "topology_member_rerank_candidates": "32",
                    "topology_member_rerank_selected": "8",
                    "vector_search_candidate_budget": "8",
                    "vector_search_result_budget": "16",
                    "vector_search_distance_evaluation_budget": "40",
                    "topology_member_rerank_rows_visited_actual": "48",
                    "topology_member_rerank_distance_evaluations_actual": "40",
                    "vector_search_rows_visited_actual": "16",
                    "vector_search_exact_distance_evaluations_actual": "8",
                    "vector_search_ann_candidate_budget_actual": "16",
                    "topology_vector_scores_reused": "1",
                    "topology_vector_scores_reused_count": "12",
                },
            },
            {
                "search_type": "hybrid",
                "search_stats": {
                    "topology_weak_query_narrow_applied": "1",
                    "topology_route_available_count": "1",
                    "topology_route_boundary_score_margin": "0.05",
                    "topology_route_confidence_abstained": "1",
                    "topology_snapshot_cache_hit": "1",
                    "topology_weak_query_allowed_candidates": "4",
                    "topology_member_rerank_candidates": "16",
                    "topology_member_rerank_selected": "4",
                    "vector_search_candidate_budget": "4",
                    "vector_search_result_budget": "16",
                    "vector_search_distance_evaluation_budget": "24",
                    "topology_member_rerank_rows_visited_actual": "24",
                    "topology_member_rerank_distance_evaluations_actual": "16",
                    "vector_search_rows_visited_actual": "8",
                    "vector_search_exact_distance_evaluations_actual": "4",
                    "vector_search_ann_candidate_budget_actual": "8",
                    "topology_vector_scores_reused": "0",
                    "topology_vector_scores_reused_count": "0",
                },
            },
        ]
        with tempfile.TemporaryDirectory() as tmp:
            debug_path = Path(tmp) / "debug.jsonl"
            debug_path.write_text(
                "".join(json.dumps(event) + "\n" for event in events), encoding="utf-8"
            )
            metrics = parse_debug_jsonl(debug_path)["metrics"]

        self.assertEqual(metrics["topology_narrow_rate"], 1.0)
        self.assertEqual(metrics["topology_confidence_abstain_rate"], 0.5)
        self.assertEqual(metrics["topology_route_available_avg"], 2.0)
        self.assertAlmostEqual(metrics["topology_route_boundary_score_margin_avg"], 0.15)
        self.assertEqual(metrics["topology_snapshot_cache_hit_rate"], 0.5)
        self.assertEqual(metrics["topology_allowed_candidates_avg"], 6.0)
        self.assertEqual(metrics["topology_member_rerank_candidates_avg"], 24.0)
        self.assertEqual(metrics["vector_candidate_budget_avg"], 6.0)
        self.assertEqual(metrics["vector_result_budget_avg"], 16.0)
        self.assertEqual(metrics["vector_distance_evaluation_budget_avg"], 32.0)
        self.assertEqual(metrics["topology_member_rows_visited_actual_avg"], 36.0)
        self.assertEqual(metrics["topology_member_distance_evaluations_actual_avg"], 28.0)
        self.assertEqual(metrics["vector_rows_visited_actual_avg"], 12.0)
        self.assertEqual(metrics["vector_exact_distance_evaluations_actual_avg"], 6.0)
        self.assertEqual(metrics["vector_ann_candidate_budget_actual_avg"], 12.0)
        self.assertEqual(metrics["vector_total_rows_visited_actual_avg"], 48.0)
        self.assertEqual(metrics["vector_total_exact_distance_evaluations_actual_avg"], 34.0)
        self.assertEqual(metrics["topology_vector_scores_reuse_rate"], 0.5)

    def test_disabled_topology_zero_fills_vector_seed_metrics(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            debug_path = Path(tmp) / "debug.jsonl"
            debug_path.write_text(
                json.dumps(
                    {
                        "search_type": "hybrid",
                        "search_stats": {"topology_weak_query_skip_reason": "disabled"},
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            metrics = parse_debug_jsonl(debug_path)["metrics"]

        self.assertEqual(metrics["topology_vector_seeds_added_avg"], 0.0)
        self.assertEqual(metrics["topology_vector_seeds_nonzero_rate"], 0.0)


class ReportBaselineTests(unittest.TestCase):
    def test_declared_valid_baseline_wins_over_heuristic_names(self) -> None:
        rows = [
            {"arm": "baseline", "valid": True, "metrics": {"mrr": 0.1}},
            {"arm": "topo_off", "valid": True, "metrics": {"mrr": 0.7}},
        ]

        self.assertIs(_baseline_row(rows, "topo_off"), rows[1])

    def test_invalid_declared_baseline_withholds_deltas(self) -> None:
        rows = [
            {"arm": "topo_off", "valid": False, "metrics": {"mrr": 0.0}},
            {"arm": "topology", "valid": True, "metrics": {"mrr": 0.6}},
        ]

        self.assertIsNone(_baseline_row(rows, "topo_off"))


if __name__ == "__main__":
    unittest.main()
