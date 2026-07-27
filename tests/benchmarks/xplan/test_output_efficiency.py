#!/usr/bin/env python3
"""Contracts for the real-codebase retrieval output-efficiency worker."""

from __future__ import annotations

import json
import os
import sys
import tempfile
import unittest
from pathlib import Path

XPLAN_ROOT = Path(__file__).resolve().parent
if str(XPLAN_ROOT) not in sys.path:
    sys.path.insert(0, str(XPLAN_ROOT))

from workers.output_efficiency import (  # noqa: E402
    OutputTask,
    _absolute_executable_path,
    _command_environment,
    _dry_run_metrics,
    aggregate_output_records,
    analyze_output,
    build_command,
    build_shadow_grep_json,
    evaluate_efficiency_contract,
    load_tasks,
)
from model import ExperimentPlan  # noqa: E402


class OutputEfficiencyContractTests(unittest.TestCase):
    def test_analyze_output_uses_exact_utf8_bytes_and_marker_end(self) -> None:
        task = OutputTask(
            task_id="search-budget",
            surface="search",
            query="budget",
            expected_any=("include/yams/search/search_engine_config.h",),
            limit=5,
        )
        payload = "πreamble\ninclude/yams/search/search_engine_config.h\n"

        record = analyze_output(task, payload, latency_ms=12.5, exit_code=0)

        expected_end = len(payload[: payload.index("\n", payload.index("include/"))].encode("utf-8"))
        self.assertEqual(record.output_bytes, len(payload.encode("utf-8")))
        self.assertEqual(record.first_useful_byte, expected_end)
        self.assertTrue(record.useful)
        self.assertEqual(record.latency_ms, 12.5)

    def test_analyze_output_counts_repeated_normalized_lines_after_first(self) -> None:
        task = OutputTask(
            task_id="grep-budget",
            surface="grep",
            query="GraphContextBudget",
            expected_any=("graph_context_service.hpp",),
            limit=5,
        )
        payload = "header\nsame evidence\nsame   evidence\ngraph_context_service.hpp\n"

        record = analyze_output(task, payload, latency_ms=1.0, exit_code=0)

        self.assertEqual(record.duplicate_line_bytes, len("same evidence\n".encode("utf-8")))

    def test_analyze_output_reports_absolute_paths_outside_checkout(self) -> None:
        task = OutputTask(
            task_id="grep-scope",
            surface="grep",
            query="PostIngestQueue",
            expected_any=("src/daemon/components/PostIngestQueue.cpp",),
            limit=5,
        )
        payload = (
            "/workspace/yams/src/daemon/components/PostIngestQueue.cpp:"
            "PostIngestQueue\n"
            "/workspace/yams-copy/include/yams/PostIngestQueue.h:PostIngestQueue\n"
            "relative/include/yams/PostIngestQueue.h:PostIngestQueue\n"
            "#include <yams/daemon/components/PostIngestQueue.h>\n"
        )

        record = analyze_output(
            task,
            payload,
            latency_ms=1.0,
            exit_code=0,
            repo_root=Path("/workspace/yams"),
        )

        self.assertEqual(record.scope_path_count, 2)
        self.assertEqual(record.scope_leak_path_count, 1)

    def test_aggregate_reports_fixed_budget_recall_and_surface_metrics(self) -> None:
        search = OutputTask(
            task_id="search-hit",
            surface="search",
            query="search",
            expected_any=("target.cpp",),
            limit=5,
        )
        graph = OutputTask(
            task_id="graph-late",
            surface="graph",
            query="graph",
            expected_any=("target.hpp",),
            limit=5,
        )
        records = [
            analyze_output(search, "target.cpp\n", latency_ms=2.0, exit_code=0),
            analyze_output(
                graph,
                ("x" * 32) + "target.hpp\n",
                latency_ms=4.0,
                exit_code=0,
            ),
        ]

        metrics = aggregate_output_records(records, budgets=(16, 64))

        self.assertEqual(metrics["task_count"], 2.0)
        self.assertEqual(metrics["useful_hit_rate"], 1.0)
        self.assertEqual(metrics["useful_recall_at_16_bytes"], 0.5)
        self.assertEqual(metrics["useful_recall_at_64_bytes"], 1.0)
        self.assertEqual(metrics["search_useful_hit_rate"], 1.0)
        self.assertEqual(metrics["graph_useful_hit_rate"], 1.0)
        self.assertEqual(metrics["command_success_rate"], 1.0)
        self.assertEqual(metrics["scope_path_count"], 0.0)
        self.assertEqual(metrics["scope_leak_fraction"], 0.0)

    def test_dry_run_metrics_match_declared_live_schema(self) -> None:
        metrics = _dry_run_metrics(
            (1024,),
            {
                "profile": "agent_memory_aggressive_v1",
                "min_useful_hit_rate": 1.0,
            },
        )

        self.assertEqual(metrics["token_count_available"], 0.0)
        self.assertEqual(metrics["efficiency_contract_evaluated"], 0.0)
        self.assertEqual(metrics["grep_shadow_available"], 0.0)

    def test_efficiency_contract_reports_every_failed_gate(self) -> None:
        metrics = {
            "useful_hit_rate": 1.0,
            "useful_recall_at_1024_bytes": 0.75,
            "output_bytes_p95": 20_000.0,
            "duplicate_line_fraction": 0.08,
        }
        contract = {
            "profile": "agent_memory_aggressive_v1",
            "min_useful_hit_rate": 1.0,
            "min_useful_recall_at_1024_bytes": 1.0,
            "max_output_bytes_p95": 16_384.0,
            "max_duplicate_line_fraction": 0.10,
        }

        evaluation = evaluate_efficiency_contract(metrics, contract)

        self.assertEqual(evaluation.metrics["efficiency_contract_evaluated"], 1.0)
        self.assertEqual(evaluation.metrics["efficiency_contract_gate_count"], 4.0)
        self.assertEqual(evaluation.metrics["efficiency_contract_violation_count"], 2.0)
        self.assertEqual(evaluation.metrics["efficiency_contract_pass"], 0.0)
        self.assertEqual(
            {violation["metric"] for violation in evaluation.violations},
            {"useful_recall_at_1024_bytes", "output_bytes_p95"},
        )

    def test_shadow_grep_json_deduplicates_identity_and_preserves_total(self) -> None:
        payload = json.dumps(
            {
                "matches": [
                    {"file": "/repo/a.cpp", "line_number": 7, "line": " hit "},
                    {"file": "/repo/a.cpp", "line_number": 7, "line": "hit"},
                    {"file": "/repo/b.cpp", "line_number": 9, "line": "other"},
                ],
                "pattern": "hit",
                "total_matches": 3,
            },
            indent=2,
        )

        shadow = build_shadow_grep_json(payload)
        rendered = json.loads(shadow.payload)

        self.assertEqual(shadow.input_matches, 3)
        self.assertEqual(shadow.unique_matches, 2)
        self.assertEqual(shadow.emitted_matches, 2)
        self.assertEqual(shadow.identity_duplicates_removed, 1)
        self.assertEqual(rendered["total_matches"], 3)
        self.assertEqual(len(rendered["matches"]), 2)

    def test_shadow_grep_json_cap_is_applied_after_identity_dedup(self) -> None:
        payload = json.dumps(
            {
                "matches": [
                    {"file": "/repo/a.cpp", "line_number": 1, "line": "a"},
                    {"file": "/repo/a.cpp", "line_number": 1, "line": "a"},
                    {"file": "/repo/b.cpp", "line_number": 2, "line": "b"},
                    {"file": "/repo/c.cpp", "line_number": 3, "line": "c"},
                ],
                "total_matches": 4,
            }
        )

        shadow = build_shadow_grep_json(payload, max_matches=2)

        self.assertEqual(shadow.unique_matches, 3)
        self.assertEqual(shadow.emitted_matches, 2)
        self.assertEqual(shadow.cap_matches_dropped, 1)
        self.assertEqual(
            [match["file"] for match in json.loads(shadow.payload)["matches"]],
            ["/repo/a.cpp", "/repo/b.cpp"],
        )

    def test_command_environment_does_not_leak_binary_selector_to_product(self) -> None:
        previous = os.environ.get("YAMS_BENCH_YAMS_BINARY")
        os.environ["YAMS_BENCH_YAMS_BINARY"] = "/tmp/yams"
        try:
            environment = _command_environment({"NO_COLOR": "1"})
        finally:
            if previous is None:
                os.environ.pop("YAMS_BENCH_YAMS_BINARY", None)
            else:
                os.environ["YAMS_BENCH_YAMS_BINARY"] = previous

        self.assertNotIn("YAMS_BENCH_YAMS_BINARY", environment)
        self.assertEqual(environment["NO_COLOR"], "1")

    def test_absolute_executable_path_preserves_symlink_invocation_name(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            target = root / "yams-cli"
            target.touch()
            alias = root / "yams"
            alias.symlink_to(target.name)

            selected = _absolute_executable_path(str(alias), root)

        self.assertEqual(selected, alias)

    def test_load_tasks_rejects_duplicate_ids_and_unknown_surfaces(self) -> None:
        payload = {
            "schema_version": 1,
            "corpus": "yams",
            "tasks": [
                {
                    "id": "duplicate",
                    "surface": "search",
                    "query": "first",
                    "expected_any": ["src/search/search_engine.cpp"],
                },
                {
                    "id": "duplicate",
                    "surface": "unknown",
                    "query": "second",
                    "expected_any": ["src/search/search_engine.cpp"],
                },
            ],
        }
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "tasks.json"
            path.write_text(json.dumps(payload), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "duplicate task id"):
                load_tasks(path)

    def test_build_command_scopes_each_surface_to_the_yams_checkout(self) -> None:
        binary = Path("/tmp/yams")
        repo_root = Path("/workspace/yams")
        search = OutputTask(
            task_id="search",
            surface="search",
            query="SearchEngine",
            expected_any=("src/search/search_engine.cpp",),
            limit=7,
        )
        grep = OutputTask(
            task_id="grep",
            surface="grep",
            query="GraphContextBudget",
            expected_any=("graph_context_service.hpp",),
            limit=3,
        )
        graph = OutputTask(
            task_id="graph",
            surface="graph",
            query="PostIngestQueue",
            expected_any=("PostIngestQueue.cpp",),
            limit=4,
        )

        search_cmd = build_command(search, binary, repo_root, output_format="json")
        grep_cmd = build_command(grep, binary, repo_root, output_format="human")
        graph_cmd = build_command(graph, binary, repo_root, output_format="json")

        self.assertEqual(search_cmd[:3], [str(binary), "search", "SearchEngine"])
        self.assertIn("--cwd", search_cmd)
        self.assertIn("--json", search_cmd)
        self.assertNotIn("--no-streaming", search_cmd)
        self.assertIn(f"{repo_root}/**", grep_cmd)
        self.assertIn("--path", grep_cmd)
        self.assertNotIn("--cwd", grep_cmd)
        self.assertIn("-F", grep_cmd)
        self.assertNotIn("--no-streaming", grep_cmd)
        self.assertNotIn("--json", grep_cmd)
        self.assertEqual(graph_cmd[:4], [str(binary), "graph", "--explore", "PostIngestQueue"])
        self.assertIn("--max-files", graph_cmd)
        self.assertIn("--json", graph_cmd)


class YamsCodebaseOutputPlanTests(unittest.TestCase):
    def test_plan_uses_real_yams_manifest_for_human_and_json_outputs(self) -> None:
        plan = ExperimentPlan.load(
            XPLAN_ROOT / "plans" / "retrieval_output_efficiency_yams.json"
        )
        manifest = load_tasks(
            XPLAN_ROOT / "data" / "yams_codebase_output_tasks.json"
        )

        self.assertEqual(plan.baseline, "human")
        self.assertEqual(plan.repeats, 3)
        self.assertEqual(plan.steps[0].worker, "output_efficiency")
        self.assertEqual(plan.fixed_params["binary"], "yams")
        self.assertEqual(
            plan.fixed_params["byte_budgets"][:2],
            [256, 512],
        )
        contract = plan.fixed_params["efficiency_contract"]
        self.assertEqual(plan.fixed_params["shadow_grep_max_matches"], 20)
        self.assertEqual(contract["profile"], "agent_memory_aggressive_v1")
        self.assertEqual(contract["min_useful_hit_rate"], 1.0)
        self.assertEqual(contract["min_useful_recall_at_512_bytes"], 1.0)
        self.assertEqual(contract["max_output_bytes_p95"], 8192)
        self.assertEqual(contract["max_grep_duplicate_line_fraction"], 0.1)
        self.assertEqual(
            {arm.name for arm in plan.arms},
            {"human", "json"},
        )
        self.assertEqual(manifest.corpus, "yams_codebase")
        self.assertEqual(
            {task.surface for task in manifest.tasks},
            {"search", "grep", "graph"},
        )
        self.assertGreaterEqual(len(manifest.tasks), 6)


if __name__ == "__main__":
    unittest.main()
