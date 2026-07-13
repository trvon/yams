#!/usr/bin/env python3
"""Behavior contracts for xplan experiment identity and reporting."""

from __future__ import annotations

import json
import hashlib
import io
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path

XPLAN_ROOT = Path(__file__).resolve().parent
if str(XPLAN_ROOT) not in sys.path:
    sys.path.insert(0, str(XPLAN_ROOT))

from analyze_query_class import (  # noqa: E402
    compare_arms,
    load_by_type,
    paired_route_risk,
    shadow_route_risk,
)
from model import ExperimentPlan  # noqa: E402
from report import _baseline_row  # noqa: E402
from workers.multi_client import _clone_corpus_seed, _metrics_from_record  # noqa: E402
from workers.mixed_corpus import (  # noqa: E402
    analyze_mixed_cluster_overlap,
    analyze_mixed_corpus_debug,
    materialize_mixed_beir_manifest,
)
from workers.retrieval_quality import (  # noqa: E402
    _benchmark_command,
    _mark_shared_topology_seed_reuse,
    _merge_benchmark_env,
    _reset_measured_outputs,
    clone_benchmark_state,
    parse_debug_jsonl,
    require_shared_topology_construction_identity,
    require_steady_state,
)


class RetrievalQualityEnvironmentTests(unittest.TestCase):
    def test_retrieval_quality_uses_one_google_benchmark_iteration(self) -> None:
        command = _benchmark_command(Path("retrieval_quality_bench"), "BM_RetrievalQuality")
        self.assertEqual(
            command,
            [
                "retrieval_quality_bench",
                "--benchmark_filter=BM_RetrievalQuality",
                "--benchmark_min_time=1x",
            ],
        )

    def test_retrieval_quality_retry_clears_append_only_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            paths = [Path(tmp) / "debug.jsonl", Path(tmp) / "topology_clusters.json"]
            for path in paths:
                path.write_text("stale\n", encoding="utf-8")
            _reset_measured_outputs(paths)
            self.assertTrue(all(not path.exists() for path in paths))

    def test_shared_clone_reuses_primed_topology_inputs(self) -> None:
        env: dict[str, str] = {}
        _mark_shared_topology_seed_reuse(env)
        self.assertEqual(env["YAMS_BENCH_REUSE_SEEDED_TOPOLOGY_INPUTS"], "1")

    def test_resolved_plan_preserves_repeat_and_baseline_identity(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "plan.json"
            path.write_text(
                json.dumps(
                    {
                        "name": "identity",
                        "repeats": 3,
                        "baseline": "control",
                        "arms": [{"name": "control"}],
                        "steps": [{"worker": "retrieval_quality"}],
                    }
                ),
                encoding="utf-8",
            )
            plan = ExperimentPlan.load(path)
            resolved = plan.resolved_dict(stamp="test", repo_root=Path(tmp), git_sha=None)

            self.assertEqual(resolved["repeats"], 3)
            self.assertEqual(resolved["baseline"], "control")

            legacy_resolved = dict(resolved)
            legacy_resolved.pop("repeats")
            legacy_resolved.pop("baseline")
            legacy_path = Path(tmp) / "legacy_resolved.json"
            legacy_path.write_text(json.dumps(legacy_resolved), encoding="utf-8")
            replayed = ExperimentPlan.load(legacy_path)
            self.assertEqual(replayed.repeats, 3)
            self.assertEqual(replayed.baseline, "control")

    def test_benchmark_state_seed_is_cloned_into_an_isolated_directory(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "seed"
            destination = root / "arm" / "isolated_data"
            source.mkdir()
            (source / "state.db").write_text("seed", encoding="utf-8")
            destination.mkdir(parents=True)
            (destination / "stale").write_text("stale", encoding="utf-8")

            clone_benchmark_state(source, destination)

            self.assertEqual(
                (destination / "state.db").read_text(encoding="utf-8"), "seed"
            )
            self.assertFalse((destination / "stale").exists())

    def test_shared_topology_identity_pins_repeats_and_arms(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            identity = Path(tmp) / "topology_fingerprint.txt"
            self.assertIsNone(
                require_shared_topology_construction_identity(identity, {"same": 100})
            )
            self.assertEqual(identity.read_text(encoding="utf-8"), "same\n")
            self.assertIsNone(
                require_shared_topology_construction_identity(identity, {"same": 100})
            )
            self.assertIn(
                "identity mismatch",
                require_shared_topology_construction_identity(identity, {"different": 100})
                or "",
            )

    def test_query_class_comparison_uses_all_repeats(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            arm_a = root / "a"
            arm_b = root / "b"
            for repeat, returned in (("rep00", ["relevant"]), ("rep01", ["other", "relevant"])):
                for arm, docs in ((arm_a, []), (arm_b, returned)):
                    rep = arm / repeat
                    rep.mkdir(parents=True)
                    (rep / "debug.jsonl").write_text(
                        json.dumps(
                            {
                                "search_type": "hybrid",
                                "query": "same query",
                                "relevant_doc_ids": ["relevant"],
                                "returned_doc_ids": docs,
                            }
                        )
                        + "\n",
                        encoding="utf-8",
                    )

            output = io.StringIO()
            with redirect_stdout(output):
                compare_arms(arm_a, arm_b)

        self.assertIn("n=2", output.getvalue())
        self.assertIn("meanΔrr=0.7500", output.getvalue())

    def test_multi_client_corpus_seed_is_cloned_into_an_isolated_directory(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "seed"
            destination = root / "arm" / "corpus"
            source.mkdir()
            (source / "yams.db").write_bytes(b"symbol-rich-fixture")
            (source / "storage").mkdir()
            (source / "storage" / "object").write_text("content", encoding="utf-8")

            method = _clone_corpus_seed(source, destination)

            self.assertIn(method, {"clone", "copy"})
            self.assertEqual(
                (destination / "yams.db").read_bytes(), b"symbol-rich-fixture"
            )
            self.assertEqual(
                (destination / "storage" / "object").read_text(encoding="utf-8"),
                "content",
            )
            self.assertNotEqual(source, destination)

    def test_simeon_vector_attribution_disables_both_text_legs(self) -> None:
        plan = json.loads(
            (XPLAN_ROOT / "plans" / "search_simeon_ann_attribution_multicorp.json")
            .read_text(encoding="utf-8")
        )

        vector_arms = [
            arm for arm in plan["arms"] if arm["factors"]["retrieval"] == "vector_only"
        ]
        self.assertEqual(len(vector_arms), 2)
        for arm in vector_arms:
            self.assertEqual(arm["factors"].get("text"), "off")
            self.assertEqual(arm["factors"].get("simeon_text"), "off")

    def test_product_search_plans_use_the_topology_ann_default(self) -> None:
        product_plans = (
            "search_product_clean_baseline.json",
            "search_product_component_ablation.json",
            "search_product_nfcorpus_gate.json",
            "search_vector_weight_0_20_multicorp.json",
            "search_vector_weight_ablation.json",
            "search_rerank_blend_multicorp.json",
            "search_graph_vector_weight_ablation.json",
            "search_lexical_floor_vector_only_multicorp.json",
        )
        for filename in product_plans:
            with self.subTest(plan=filename):
                plan = json.loads(
                    (XPLAN_ROOT / "plans" / filename).read_text(encoding="utf-8")
                )
                env = plan["fixed"]["env"]
                self.assertEqual(env["YAMS_BENCH_TOPOLOGY_MODE"], "hybrid_assist")
                self.assertEqual(env["YAMS_BENCH_TOPOLOGY_VECTOR_POLICY"], "shadow")

        self.assertFalse(
            (XPLAN_ROOT / "plans" / "search_evidence_pipeline_multicorp.json").exists()
        )

    def test_generalized_memory_gate_is_a_lean_soar_construction_ablation(self) -> None:
        plan = json.loads(
            (XPLAN_ROOT / "plans" / "search_generalized_memory_topology_gate.json")
            .read_text(encoding="utf-8")
        )
        self.assertGreaterEqual(plan["repeats"], 3)
        self.assertEqual(plan["baseline"], "shadow_margin020_min1")
        self.assertEqual(
            plan["fixed"]["params"]["shared_warm_cache"],
            "generalized_memory_soar_boundary",
        )
        self.assertTrue(
            plan["fixed"]["params"]["require_topology_construction_stability"]
        )
        self.assertNotIn(
            "require_topology_construction_identity", plan["fixed"]["params"]
        )
        arms = {arm["name"]: arm for arm in plan["arms"]}
        self.assertEqual(
            set(arms),
            {
                "global_ann_c32",
                "shadow_margin020_min1",
                "narrow_soar_lambda1_ratio105",
            },
        )
        soar = arms["narrow_soar_lambda1_ratio105"]
        self.assertEqual(soar["params"]["topology_vector_policy"], "narrow")
        self.assertEqual(soar["params"]["topology_boundary_spill"], "1")
        self.assertEqual(
            soar["params"]["topology_boundary_spill_residual_penalty"], 1.0
        )
        self.assertEqual(
            soar["params"]["topology_boundary_spill_distance_ratio"], 1.05
        )
        self.assertEqual(soar["factors"]["ann_candidate_budget"], 32)

    def test_query_class_loader_ignores_missing_or_directory_paths(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            self.assertEqual(load_by_type(Path(tmp)), {})
            self.assertEqual(load_by_type(Path(tmp) / "missing.jsonl"), {})

    def test_paired_route_risk_counts_protected_global_candidates_lost_by_narrowing(
        self,
    ) -> None:
        def event(
            *, pre_fusion: list[str], returned: list[str], narrow: bool, exact: int
        ):
            relevant = ["nfcorpus__a", "nfcorpus__b"]
            return {
                "search_type": "hybrid",
                "query": "shared query",
                "relevant_doc_ids": relevant,
                "returned_doc_ids": returned,
                "relevant_decision_trace": {
                    "relevant_docs": [
                        {
                            "doc_id": doc,
                            "in_pre_fusion": doc in pre_fusion,
                            "component_top_hits": ["vector"]
                            if doc in pre_fusion
                            else [],
                        }
                        for doc in relevant
                    ]
                },
                "search_stats": {
                    "topology_weak_query_narrow_applied": "1" if narrow else "0",
                    "topology_route_boundary_score_margin": "0.42",
                    "topology_seed_coverage_count": "2",
                    "vector_search_exact_distance_evaluations_actual": str(exact),
                },
            }

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            baseline_path = root / "baseline.jsonl"
            routed_path = root / "routed.jsonl"
            baseline_path.write_text(
                json.dumps(
                    event(
                        pre_fusion=["nfcorpus__a", "nfcorpus__b"],
                        returned=["nfcorpus__a", "nfcorpus__b"],
                        narrow=False,
                        exact=20,
                    )
                )
                + "\n",
                encoding="utf-8",
            )
            routed_path.write_text(
                json.dumps(
                    event(
                        pre_fusion=["nfcorpus__a"],
                        returned=["irrelevant", "nfcorpus__a"],
                        narrow=True,
                        exact=8,
                    )
                )
                + "\n",
                encoding="utf-8",
            )

            summary = paired_route_risk(
                load_by_type(baseline_path).get("hybrid", []),
                load_by_type(routed_path).get("hybrid", []),
            )

        self.assertEqual(summary["all"]["calibration_queries"], 1)
        self.assertEqual(summary["all"]["protected_candidates"], 2)
        self.assertEqual(summary["all"]["missed_protected_candidates"], 1)
        self.assertEqual(summary["all"]["misses_per_thousand"], 500.0)
        self.assertEqual(summary["all"]["mean_exact_work_delta"], -12.0)
        self.assertEqual(summary["nfcorpus"], summary["all"])

    def test_shadow_route_risk_uses_projected_removals_without_a_routed_arm(self) -> None:
        event = {
            "search_type": "hybrid",
            "query": "shared query",
            "relevant_doc_ids": ["nfcorpus__a", "nfcorpus__b"],
            "returned_doc_ids": ["nfcorpus__a", "nfcorpus__b"],
            "relevant_decision_trace": {
                "relevant_docs": [
                    {
                        "doc_id": "nfcorpus__a",
                        "in_pre_fusion": True,
                        "component_top_hits": ["vector"],
                    },
                    {
                        "doc_id": "nfcorpus__b",
                        "in_pre_fusion": True,
                        "component_top_hits": ["vector"],
                    },
                ]
            },
            "search_stats": {
                "topology_shadow_evaluated": "1",
                "topology_shadow_proposed_action": "narrow",
                "topology_shadow_retained_candidate_doc_ids": "nfcorpus__a",
                "topology_shadow_removed_candidate_doc_ids": "nfcorpus__b\tirrelevant",
                "topology_route_boundary_score_margin": "0.42",
                "topology_seed_coverage_count": "2",
                "vector_search_exact_distance_evaluations_actual": "8",
            },
        }

        with tempfile.TemporaryDirectory() as tmp:
            debug_path = Path(tmp) / "debug.jsonl"
            debug_path.write_text(json.dumps(event) + "\n", encoding="utf-8")
            summary = shadow_route_risk(load_by_type(debug_path).get("hybrid", []))

        self.assertEqual(summary["all"]["calibration_queries"], 1)
        self.assertEqual(summary["all"]["protected_candidates"], 2)
        self.assertEqual(summary["all"]["missed_protected_candidates"], 1)
        self.assertEqual(summary["all"]["misses_per_thousand"], 500.0)
        self.assertEqual(summary["nfcorpus"], summary["all"])

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

    def test_explicit_arm_config_cannot_be_shadowed_by_ambient_config_path(
        self,
    ) -> None:
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
                    "vector_search_candidate_budget": "8",
                    "vector_search_result_budget": "16",
                    "vector_search_distance_evaluation_budget": "40",
                    "vector_search_rows_visited_actual": "16",
                    "vector_search_exact_distance_evaluations_actual": "8",
                    "vector_search_ann_candidate_budget_actual": "16",
                    "topology_vector_filter_applied": "1",
                    "topology_vector_filter_fallback": "0",
                    "topology_vector_filter_matched": "6",
                    "topology_vector_filter_removed": "10",
                    "topology_vector_allowed_set_ann_applied": "1",
                    "topology_vector_allowed_set_ann_fallback": "0",
                    "topology_vector_policy": "narrow",
                    "latency_ms": "30",
                    "topology_structure_candidate_count": "6",
                    "topology_structure_scale_agreement_mean": "0.8",
                    "topology_structure_overlap_support_mean": "0.6",
                    "topology_structure_persistence_support_mean": "0.9",
                    "topology_structure_cohesion_support_mean": "0.7",
                    "topology_structure_bridge_support_mean": "0.4",
                    "topology_structure_density_support_mean": "0.75",
                    "topology_construction_fingerprint": "fixed-topology",
                },
            },
            {
                "search_type": "hybrid",
                "search_stats": {
                    "topology_weak_query_narrow_applied": "0",
                    "topology_route_available_count": "1",
                    "topology_route_boundary_score_margin": "0.05",
                    "topology_route_confidence_abstained": "1",
                    "topology_snapshot_cache_hit": "1",
                    "topology_weak_query_allowed_candidates": "4",
                    "vector_search_candidate_budget": "4",
                    "vector_search_result_budget": "16",
                    "vector_search_distance_evaluation_budget": "24",
                    "vector_search_rows_visited_actual": "8",
                    "vector_search_exact_distance_evaluations_actual": "4",
                    "vector_search_ann_candidate_budget_actual": "8",
                    "topology_vector_filter_applied": "0",
                    "topology_vector_filter_fallback": "1",
                    "topology_vector_filter_matched": "0",
                    "topology_vector_filter_removed": "0",
                    "topology_vector_allowed_set_ann_applied": "0",
                    "topology_vector_allowed_set_ann_fallback": "1",
                    "topology_vector_policy": "narrow",
                    "latency_ms": "10",
                    "topology_structure_candidate_count": "2",
                    "topology_structure_scale_agreement_mean": "0.4",
                    "topology_structure_overlap_support_mean": "0.2",
                    "topology_structure_persistence_support_mean": "0.7",
                    "topology_structure_cohesion_support_mean": "0.5",
                    "topology_structure_bridge_support_mean": "0.2",
                    "topology_structure_density_support_mean": "0.65",
                    "topology_construction_fingerprint": "fixed-topology",
                },
            },
        ]
        with tempfile.TemporaryDirectory() as tmp:
            debug_path = Path(tmp) / "debug.jsonl"
            debug_path.write_text(
                "".join(json.dumps(event) + "\n" for event in events), encoding="utf-8"
            )
            metrics = parse_debug_jsonl(debug_path)["metrics"]

        self.assertEqual(metrics["topology_narrow_rate"], 0.5)
        self.assertEqual(metrics["topology_confidence_abstain_rate"], 0.5)
        self.assertEqual(metrics["topology_route_available_avg"], 2.0)
        self.assertAlmostEqual(
            metrics["topology_route_boundary_score_margin_avg"], 0.15
        )
        self.assertEqual(metrics["topology_snapshot_cache_hit_rate"], 0.5)
        self.assertEqual(metrics["topology_allowed_candidates_avg"], 6.0)
        self.assertEqual(metrics["topology_vector_filter_allowed_candidates_avg"], 8.0)
        self.assertEqual(metrics["topology_vector_filter_latency_ms_avg"], 30.0)
        self.assertEqual(metrics["topology_vector_abstain_latency_ms_avg"], 10.0)
        self.assertEqual(metrics["vector_candidate_budget_avg"], 6.0)
        self.assertEqual(metrics["vector_result_budget_avg"], 16.0)
        self.assertEqual(metrics["vector_distance_evaluation_budget_avg"], 32.0)
        self.assertEqual(metrics["vector_rows_visited_actual_avg"], 12.0)
        self.assertEqual(metrics["vector_exact_distance_evaluations_actual_avg"], 6.0)
        self.assertEqual(metrics["vector_ann_candidate_budget_actual_avg"], 12.0)
        self.assertEqual(metrics["vector_candidate_work_budget_avg"], 10.0)
        self.assertEqual(metrics["vector_total_rows_visited_actual_avg"], 12.0)
        self.assertEqual(metrics["vector_total_exact_distance_evaluations_actual_avg"], 6.0)
        self.assertEqual(metrics["topology_vector_filter_rate"], 0.5)
        self.assertEqual(metrics["topology_vector_filter_fallback_rate"], 0.5)
        self.assertEqual(metrics["topology_vector_filter_matched_avg"], 3.0)
        self.assertEqual(metrics["topology_vector_filter_removed_avg"], 5.0)
        self.assertEqual(metrics["topology_vector_allowed_set_ann_rate"], 0.5)
        self.assertEqual(metrics["topology_vector_allowed_set_ann_fallback_rate"], 0.5)
        self.assertEqual(metrics["topology_structure_candidate_count_avg"], 4.0)
        self.assertAlmostEqual(metrics["topology_structure_scale_agreement_avg"], 0.6)
        self.assertAlmostEqual(metrics["topology_structure_overlap_support_avg"], 0.4)
        self.assertAlmostEqual(metrics["topology_structure_persistence_support_avg"], 0.8)
        self.assertAlmostEqual(metrics["topology_structure_cohesion_support_avg"], 0.6)
        self.assertAlmostEqual(metrics["topology_structure_bridge_support_avg"], 0.3)
        self.assertAlmostEqual(metrics["topology_structure_density_support_avg"], 0.7)
        self.assertEqual(metrics["topology_construction_fingerprint_count"], 1.0)

    def test_shadow_projection_metrics_are_aggregated_without_counting_application(
        self,
    ) -> None:
        events = [
            {
                "search_type": "hybrid",
                "search_stats": {
                    "topology_shadow_evaluated": "1",
                    "topology_shadow_proposed_action": "narrow",
                    "topology_shadow_retained_candidates": "3",
                    "topology_shadow_removed_candidates": "5",
                    "topology_weak_query_applied": "0",
                },
            },
            {
                "search_type": "hybrid",
                "search_stats": {
                    "topology_shadow_evaluated": "1",
                    "topology_shadow_proposed_action": "global",
                    "topology_shadow_retained_candidates": "0",
                    "topology_shadow_removed_candidates": "0",
                    "topology_weak_query_applied": "0",
                },
            },
        ]
        with tempfile.TemporaryDirectory() as tmp:
            debug_path = Path(tmp) / "debug.jsonl"
            debug_path.write_text(
                "".join(json.dumps(event) + "\n" for event in events), encoding="utf-8"
            )
            metrics = parse_debug_jsonl(debug_path)["metrics"]

        self.assertEqual(metrics["topology_shadow_evaluation_rate"], 1.0)
        self.assertEqual(metrics["topology_shadow_narrow_proposal_rate"], 0.5)
        self.assertEqual(metrics["topology_shadow_retained_candidates_avg"], 1.5)
        self.assertEqual(metrics["topology_shadow_removed_candidates_avg"], 2.5)
        self.assertEqual(metrics["topology_applied"], 0.0)

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

    def test_debug_parser_exposes_readiness_and_candidate_oracles(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            debug_path = Path(tmp) / "debug.jsonl"
            debug_path.write_text(
                "".join(
                    json.dumps(event) + "\n"
                    for event in [
                        {
                            "search_type": "hybrid",
                            "relevant_doc_ids": ["d1", "d2"],
                            "returned_doc_ids": ["d1"],
                            "search_stats": {
                                "corpus_warming": "false",
                                "search_engine_ready": "true",
                                "vector_ready": "true",
                                "budget_short_query": "false",
                                "semantic_budget_vector_cap": "16",
                            },
                            "relevant_decision_trace": {
                                "any_relevant_in_pre_fusion": True,
                                "any_relevant_in_post_fusion": True,
                                "any_relevant_in_returned_topk": True,
                                "relevant_docs": [
                                    {
                                        "in_pre_fusion": True,
                                        "in_post_fusion": True,
                                        "in_returned_topk": True,
                                    },
                                    {
                                        "in_pre_fusion": True,
                                        "in_post_fusion": False,
                                        "in_returned_topk": False,
                                    },
                                ],
                                "stage_relevant_presence": {
                                    "text": {"any_relevant": True},
                                    "vector": {"any_relevant": False},
                                },
                            },
                        },
                        {
                            "search_type": "hybrid",
                            "relevant_doc_ids": ["d3"],
                            "returned_doc_ids": [],
                            "search_stats": {
                                "corpus_warming": "true",
                                "search_engine_ready": "false",
                                "vector_ready": "false",
                                "budget_short_query": "true",
                                "semantic_budget_vector_cap": "8",
                            },
                            "relevant_decision_trace": {
                                "any_relevant_in_pre_fusion": True,
                                "any_relevant_in_post_fusion": True,
                                "any_relevant_in_returned_topk": False,
                                "relevant_docs": [
                                    {
                                        "in_pre_fusion": True,
                                        "in_post_fusion": True,
                                        "in_returned_topk": False,
                                    }
                                ],
                                "stage_relevant_presence": {
                                    "text": {"any_relevant": False},
                                    "vector": {"any_relevant": True},
                                },
                            },
                        },
                    ]
                ),
                encoding="utf-8",
            )

            parsed = parse_debug_jsonl(debug_path, top_k=1)
            metrics = parsed["metrics"]

            self.assertEqual(metrics["steady_state_query_rate"], 0.5)
            self.assertEqual(metrics["corpus_warming_rate"], 0.5)
            self.assertEqual(metrics["short_query_budget_rate"], 0.5)
            self.assertEqual(metrics["effective_vector_cap_min"], 8.0)
            self.assertEqual(metrics["effective_vector_cap_max"], 16.0)
            self.assertEqual(metrics["candidate_pre_fusion_hit_rate"], 1.0)
            self.assertEqual(metrics["candidate_post_fusion_hit_rate"], 1.0)
            self.assertEqual(metrics["returned_trace_hit_rate"], 0.5)
            self.assertAlmostEqual(metrics["candidate_pre_fusion_recall"], 1.0)
            self.assertAlmostEqual(metrics["candidate_post_fusion_recall"], 0.75)
            self.assertAlmostEqual(metrics["returned_trace_recall"], 0.25)
            self.assertEqual(metrics["ranking_loss_query_rate"], 0.5)
            self.assertEqual(metrics["vector_unique_rescue_rate"], 0.5)
            self.assertAlmostEqual(metrics["recall_at_k_ceiling"], 0.75)
            self.assertAlmostEqual(
                metrics["ceiling_normalized_returned_recall"], 1.0 / 3.0
            )

            error = require_steady_state(debug_path, require_vector=True)
            self.assertIn("1/2 hybrid queries were not steady-state", error or "")

    def test_steady_state_validation_accepts_ready_text_only_queries(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            debug_path = Path(tmp) / "debug.jsonl"
            debug_path.write_text(
                json.dumps(
                    {
                        "search_type": "hybrid",
                        "search_stats": {
                            "corpus_warming": "false",
                            "search_engine_ready": "true",
                            "vector_ready": "false",
                        },
                    }
                )
                + "\n",
                encoding="utf-8",
            )

            self.assertIsNone(require_steady_state(debug_path, require_vector=False))

class MultiClientMetricTests(unittest.TestCase):
    def test_read_write_latency_and_pool_pressure_are_preserved(self) -> None:
        metrics = _metrics_from_record(
            {
                "elapsed_seconds": 2.0,
                "total_adds": 20,
                "total_searches": 60,
                "total_lists": 20,
                "add_latency": {"p50_us": 2000, "p95_us": 5000, "p99_us": 9000},
                "search_latency": {
                    "p50_us": 1000,
                    "p95_us": 3000,
                    "p99_us": 7000,
                },
                "list_latency": {"p50_us": 500, "p95_us": 1500, "p99_us": 2500},
                "resource_peaks": {
                    "peak_db_write_pool_waiting": 4,
                    "peak_db_read_pool_waiting": 7,
                    "peak_db_write_pool_slow_holders": 8,
                    "peak_db_read_pool_slow_holders": 3,
                    "peak_db_write_pool_max_holder_us": 950000,
                    "peak_db_read_pool_max_holder_us": 400000,
                    "peak_write_queue_depth_max": 11,
                    "peak_write_queue_capacity_rejections": 9,
                    "peak_write_queue_forced_over_capacity": 3,
                    "peak_metadata_wal_bytes": 5000,
                },
                "resource_baseline": {
                    "write_queue_depth_max": 5,
                    "write_queue_capacity_rejections": 2,
                    "write_queue_forced_over_capacity": 1,
                    "metadata_wal_bytes": 1200,
                    "db_write_pool_slow_holders": 5,
                    "db_read_pool_slow_holders": 1,
                    "db_write_pool_max_holder_us": 500000,
                    "db_read_pool_max_holder_us": 400000,
                },
            }
        )

        self.assertEqual(metrics["add_p95_ms"], 5.0)
        self.assertEqual(metrics["add_p99_ms"], 9.0)
        self.assertEqual(metrics["search_p99_ms"], 7.0)
        self.assertEqual(metrics["list_p50_ms"], 0.5)
        self.assertEqual(metrics["list_p95_ms"], 1.5)
        self.assertEqual(metrics["list_p99_ms"], 2.5)
        self.assertEqual(metrics["add_ops_per_s"], 10.0)
        self.assertEqual(metrics["search_ops_per_s"], 30.0)
        self.assertEqual(metrics["list_ops_per_s"], 10.0)
        self.assertEqual(metrics["total_ops_per_s"], 50.0)
        self.assertEqual(metrics["db_write_pool_waiting_peak"], 4.0)
        self.assertEqual(metrics["db_read_pool_waiting_peak"], 7.0)
        self.assertEqual(metrics["write_queue_depth_high_water_delta"], 6.0)
        self.assertEqual(metrics["write_queue_capacity_rejections_delta"], 7.0)
        self.assertEqual(metrics["write_queue_forced_over_capacity_delta"], 2.0)
        self.assertEqual(metrics["metadata_wal_growth_bytes"], 3800.0)
        self.assertEqual(metrics["db_write_pool_slow_holders_delta"], 3.0)
        self.assertEqual(metrics["db_read_pool_slow_holders_delta"], 2.0)
        self.assertEqual(metrics["db_write_pool_max_holder_high_water_ms"], 450.0)
        self.assertEqual(metrics["db_read_pool_max_holder_high_water_ms"], 0.0)

    def test_write_coordinator_pressure_is_preserved(self) -> None:
        metrics = _metrics_from_record(
            {
                "resource_peaks": {
                    "peak_post_ingest_queued": 47,
                    "peak_post_ingest_inflight": 5,
                    "peak_write_queue_depth": 1067,
                    "peak_write_in_flight": 1,
                    "peak_write_max_batch_apply_ms": 83,
                    "peak_write_max_batch_queue_wait_ms": 912,
                    "peak_write_max_batch_excess_queue_wait_ms": 712,
                    "max_pressure_level": 3,
                }
            }
        )

        self.assertEqual(metrics["backlog_peak"], 47.0)
        self.assertEqual(metrics["post_ingest_inflight_peak"], 5.0)
        self.assertEqual(metrics["write_queue_depth_peak"], 1067.0)
        self.assertEqual(metrics["write_in_flight_peak"], 1.0)
        self.assertEqual(metrics["write_max_batch_apply_ms_peak"], 83.0)
        self.assertEqual(metrics["write_max_batch_queue_wait_ms_peak"], 912.0)
        self.assertEqual(metrics["write_max_batch_excess_queue_wait_ms_peak"], 712.0)
        self.assertEqual(metrics["pressure_level_peak"], 3.0)

    def test_read_write_pressure_plan_uses_equal_operation_budgets(self) -> None:
        plan = json.loads(
            (XPLAN_ROOT / "plans" / "read_write_pressure.json").read_text(
                encoding="utf-8"
            )
        )

        self.assertEqual(plan["repeats"], 3)
        self.assertGreater(plan["fixed"]["params"]["mixed_ops_per_client"], 0)
        ratios = {
            arm["name"]: arm["factors"]["search_ratio"] for arm in plan["arms"]
        }
        self.assertEqual(
            ratios,
            {
                "write_heavy": 0.2,
                "balanced": 0.5,
                "read_heavy": 0.8,
                "read_only": 1.0,
            },
        )


class MixedCorpusTests(unittest.TestCase):
    @staticmethod
    def _write_beir_dataset(
        root: Path,
        *,
        documents: list[dict[str, str]],
        queries: list[dict[str, str]],
        qrels: list[tuple[str, str, int]],
    ) -> None:
        (root / "qrels").mkdir(parents=True)
        (root / "corpus.jsonl").write_text(
            "".join(json.dumps(document) + "\n" for document in documents),
            encoding="utf-8",
        )
        (root / "queries.jsonl").write_text(
            "".join(json.dumps(query) + "\n" for query in queries),
            encoding="utf-8",
        )
        (root / "qrels" / "test.tsv").write_text(
            "query-id\tcorpus-id\tscore\n"
            + "".join(
                f"{query}\t{document}\t{score}\n" for query, document, score in qrels
            ),
            encoding="utf-8",
        )

    def test_materializer_builds_one_deduplicated_namespaced_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            alpha = root / "alpha"
            beta = root / "beta"
            self._write_beir_dataset(
                alpha,
                documents=[
                    {"_id": "a1", "title": "Shared", "text": "same memory"},
                    {"_id": "a2", "title": "Alpha", "text": "alpha only"},
                ],
                queries=[{"_id": "qa", "text": "alpha memory"}],
                qrels=[("qa", "a1", 2), ("qa", "a2", 1)],
            )
            self._write_beir_dataset(
                beta,
                documents=[
                    {"_id": "b1", "title": "Shared", "text": "same memory"},
                    {"_id": "b2", "title": "Beta", "text": "beta only"},
                ],
                queries=[{"_id": "qb", "text": "beta memory"}],
                qrels=[("qb", "b1", 3), ("qb", "b2", 1)],
            )

            prepared = materialize_mixed_beir_manifest(
                {"alpha": alpha, "beta": beta}, root / "mixed"
            )
            manifest = json.loads(prepared.manifest_path.read_text(encoding="utf-8"))
            identity = json.loads(prepared.identity_path.read_text(encoding="utf-8"))

        self.assertEqual(manifest["name"], "mixed-beir-alpha-beta")
        self.assertEqual(len(manifest["documents"]), 3)
        self.assertEqual(
            {query["id"] for query in manifest["queries"]}, {"alpha__qa", "beta__qb"}
        )
        beta_shared = next(
            qrel
            for qrel in manifest["qrels"]
            if qrel["query_id"] == "beta__qb" and qrel["score"] == 3
        )
        self.assertEqual(beta_shared["doc_id"], "alpha__a1")
        self.assertEqual(identity["document_sources"]["alpha__a1"], ["alpha", "beta"])
        shared_hash = hashlib.sha256(b"Shared\n\nsame memory").hexdigest()
        self.assertEqual(
            identity["document_hash_sources"][shared_hash], ["alpha", "beta"]
        )
        self.assertEqual(identity["document_hashes"]["alpha__a1"], shared_hash)
        self.assertEqual(identity["query_order"], ["alpha__qa", "beta__qb"])

    def test_mixed_debug_metrics_report_each_source_and_cross_source_results(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            identity_path = root / "mixed_corpus_identity.json"
            identity_path.write_text(
                json.dumps(
                    {
                        "query_order": ["alpha__qa", "beta__qb"],
                        "query_sources": {"alpha__qa": "alpha", "beta__qb": "beta"},
                        "document_sources": {
                            "alpha__a1": ["alpha"],
                            "beta__b1": ["beta"],
                            "beta__b2": ["beta"],
                        },
                        "document_hash_sources": {
                            "hash-alpha": ["alpha"],
                            "hash-beta": ["beta"],
                            "hash-beta-2": ["beta"],
                        },
                        "document_hashes": {
                            "alpha__a1": "hash-alpha",
                            "beta__b1": "hash-beta",
                            "beta__b2": "hash-beta-2",
                        },
                    }
                ),
                encoding="utf-8",
            )
            debug_path = root / "debug.jsonl"
            debug_path.write_text(
                "".join(
                    json.dumps(event) + "\n"
                    for event in [
                        {
                            "query_index": 0,
                            "search_type": "hybrid",
                            "relevant_doc_ids": ["alpha__a1"],
                            "returned_doc_ids": ["beta__b1", "alpha__a1"],
                            "search_stats": {
                                "topology_weak_query_allowed_candidate_hashes": (
                                    "hash-beta\thash-alpha"
                                ),
                                "topology_weak_query_narrow_applied": "1",
                                "trace_post_fusion_top_doc_ids": (
                                    "beta__b1\talpha__a1"
                                ),
                            },
                            "relevant_decision_trace": {
                                "relevant_docs": [
                                    {
                                        "in_pre_fusion": True,
                                        "in_post_fusion": True,
                                        "in_returned_topk": True,
                                    }
                                ],
                                "stage_relevant_presence": {
                                    "text": {"any_relevant": True},
                                    "vector": {"any_relevant": False},
                                },
                            },
                        },
                        {
                            "query_index": 1,
                            "search_type": "hybrid",
                            "relevant_doc_ids": ["beta__b1", "beta__b2"],
                            "returned_doc_ids": ["beta__b1", "alpha__a1"],
                            "search_stats": {
                                "topology_weak_query_allowed_candidate_hashes": "hash-beta",
                                "topology_weak_query_narrow_applied": "1",
                                "trace_post_fusion_top_doc_ids": ("beta__b1\tbeta__b2"),
                            },
                            "relevant_decision_trace": {
                                "relevant_docs": [
                                    {
                                        "in_pre_fusion": True,
                                        "in_post_fusion": True,
                                        "in_returned_topk": True,
                                    },
                                    {
                                        "in_pre_fusion": False,
                                        "in_post_fusion": True,
                                        "in_returned_topk": False,
                                    },
                                ],
                                "stage_relevant_presence": {
                                    "text": {"any_relevant": False},
                                    "vector": {"any_relevant": True},
                                },
                            },
                        },
                    ]
                ),
                encoding="utf-8",
            )

            metrics = analyze_mixed_corpus_debug(debug_path, identity_path, top_k=2)

        self.assertEqual(metrics["mixed_query_count_alpha"], 1.0)
        self.assertEqual(metrics["mixed_query_count_beta"], 1.0)
        self.assertEqual(metrics["mixed_mrr_alpha"], 0.5)
        self.assertEqual(metrics["mixed_mrr_beta"], 1.0)
        self.assertEqual(metrics["mixed_recall_at_k_alpha"], 1.0)
        self.assertEqual(metrics["mixed_recall_at_k_beta"], 0.5)
        self.assertEqual(metrics["mixed_cross_source_result_rate"], 0.5)
        self.assertEqual(metrics["mixed_cross_source_top1_rate"], 0.5)
        self.assertEqual(metrics["mixed_source_macro_mrr"], 0.75)
        self.assertEqual(metrics["mixed_source_min_recall_at_k"], 0.5)
        self.assertEqual(metrics["mixed_candidate_pre_fusion_recall_alpha"], 1.0)
        self.assertEqual(metrics["mixed_candidate_pre_fusion_recall_beta"], 0.5)
        self.assertEqual(metrics["mixed_candidate_post_fusion_recall_beta"], 1.0)
        self.assertEqual(metrics["mixed_returned_trace_recall_beta"], 0.5)
        self.assertEqual(metrics["mixed_vector_unique_rescue_rate_alpha"], 0.0)
        self.assertEqual(metrics["mixed_vector_unique_rescue_rate_beta"], 1.0)
        self.assertAlmostEqual(metrics["mixed_topology_route_source_purity"], 2.0 / 3.0)
        self.assertAlmostEqual(
            metrics["mixed_topology_route_cross_source_rate"], 1.0 / 3.0
        )
        self.assertEqual(metrics["mixed_topology_route_source_purity_alpha"], 0.5)
        self.assertEqual(metrics["mixed_topology_route_source_purity_beta"], 1.0)
        self.assertEqual(metrics["mixed_post_fusion_cross_source_result_rate"], 0.25)
        self.assertAlmostEqual(
            metrics["mixed_topology_relevant_fragment_coverage"], 2.0 / 3.0
        )
        self.assertEqual(metrics["mixed_topology_relevant_fragment_hit_rate"], 1.0)
        self.assertEqual(
            metrics["mixed_topology_relevant_fragment_coverage_alpha"], 1.0
        )
        self.assertEqual(metrics["mixed_topology_relevant_fragment_coverage_beta"], 0.5)

    def test_route_oracle_counts_empty_routes_as_relevant_fragment_misses(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            identity_path = root / "mixed_corpus_identity.json"
            identity_path.write_text(
                json.dumps(
                    {
                        "query_order": ["alpha__hit", "alpha__miss"],
                        "query_sources": {
                            "alpha__hit": "alpha",
                            "alpha__miss": "alpha",
                        },
                        "document_sources": {
                            "alpha__a1": ["alpha"],
                            "alpha__a2": ["alpha"],
                        },
                        "document_hash_sources": {
                            "hash-a1": ["alpha"],
                            "hash-a2": ["alpha"],
                        },
                        "document_hashes": {
                            "alpha__a1": "hash-a1",
                            "alpha__a2": "hash-a2",
                        },
                    }
                ),
                encoding="utf-8",
            )
            debug_path = root / "debug.jsonl"
            debug_path.write_text(
                "".join(
                    json.dumps(event) + "\n"
                    for event in [
                        {
                            "query_index": 0,
                            "search_type": "hybrid",
                            "relevant_doc_ids": ["alpha__a1"],
                            "returned_doc_ids": ["alpha__a1"],
                            "search_stats": {
                                "topology_weak_query_allowed_candidate_hashes": "hash-a1"
                            },
                        },
                        {
                            "query_index": 1,
                            "search_type": "hybrid",
                            "relevant_doc_ids": ["alpha__a2"],
                            "returned_doc_ids": [],
                            "search_stats": {
                                "topology_weak_query_allowed_candidate_hashes": ""
                            },
                        },
                    ]
                ),
                encoding="utf-8",
            )

            metrics = analyze_mixed_corpus_debug(debug_path, identity_path, top_k=10)

        self.assertEqual(metrics["mixed_topology_relevant_fragment_coverage"], 0.5)
        self.assertEqual(metrics["mixed_topology_relevant_fragment_hit_rate"], 0.5)

    def test_cluster_overlap_reports_dataset_composition_without_duplicate_leakage(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            identity_path = root / "mixed_corpus_identity.json"
            identity_path.write_text(
                json.dumps(
                    {
                        "document_hash_sources": {
                            "a1": ["alpha"],
                            "a2": ["alpha"],
                            "b1": ["beta"],
                            "b2": ["beta"],
                            "shared": ["alpha", "beta"],
                        }
                    }
                ),
                encoding="utf-8",
            )
            snapshot_path = root / "topology_clusters.json"
            snapshot_path.write_text(
                json.dumps(
                    {
                        "algorithm": "connected_components_v1",
                        "snapshot_id": "snapshot-1",
                        "memberships": [
                            {
                                "document_hash": "a1",
                                "cluster_id": "mixed",
                                "overlap_cluster_ids": ["alpha"],
                            },
                            {"document_hash": "b1", "cluster_id": "mixed"},
                            {"document_hash": "a2", "cluster_id": "alpha"},
                            {"document_hash": "b2", "cluster_id": "beta"},
                            {"document_hash": "shared", "cluster_id": "duplicate"},
                        ],
                    }
                ),
                encoding="utf-8",
            )

            report = analyze_mixed_cluster_overlap(snapshot_path, identity_path)

        metrics = report["metrics"]
        self.assertEqual(metrics["mixed_cluster_count"], 4.0)
        self.assertEqual(metrics["mixed_cluster_analyzable_count"], 3.0)
        self.assertEqual(metrics["mixed_cluster_singleton_rate"], 0.75)
        self.assertEqual(metrics["mixed_cluster_intrinsic_shared_document_rate"], 0.2)
        self.assertEqual(metrics["mixed_cluster_shared_count"], 1.0)
        self.assertAlmostEqual(metrics["mixed_cluster_shared_rate"], 1.0 / 3.0)
        self.assertEqual(metrics["mixed_cluster_shared_document_rate"], 0.5)
        self.assertEqual(metrics["mixed_cluster_weighted_source_purity"], 0.75)
        self.assertEqual(metrics["mixed_cluster_weighted_source_entropy_bits"], 0.5)
        self.assertEqual(metrics["mixed_cluster_topology_overlap_membership_rate"], 0.2)
        self.assertEqual(metrics["mixed_cluster_count_alpha"], 2.0)
        self.assertEqual(metrics["mixed_cluster_count_beta"], 2.0)
        self.assertEqual(metrics["mixed_cluster_exclusive_count_alpha"], 1.0)
        self.assertEqual(metrics["mixed_cluster_exclusive_count_beta"], 1.0)
        self.assertEqual(metrics["mixed_cluster_cross_source_exposure_alpha"], 0.5)
        self.assertEqual(metrics["mixed_cluster_cross_source_exposure_beta"], 0.5)
        self.assertAlmostEqual(
            metrics["mixed_cluster_overlap_jaccard_alpha_beta"], 1.0 / 3.0
        )
        mixed = next(
            cluster
            for cluster in report["clusters"]
            if cluster["cluster_id"] == "mixed"
        )
        self.assertEqual(mixed["source_counts"], {"alpha": 1, "beta": 1})
        self.assertEqual(mixed["source_purity"], 0.5)


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
