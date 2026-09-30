"""Unit tests for the code_intelligence xplan worker."""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from model import Arm, ArmContext, ExperimentPlan, Step
from workers.base import WorkerContext
from workers.code_intelligence import (
    BrickModelIndex,
    CodeTask,
    _duplicate_normalized_line_bytes,
    _first_useful_byte,
    _normalized_payload,
    _scope_path_counts,
    load_code_tasks,
    run_code_intelligence,
)


class CodeIntelligenceWorkerTests(unittest.TestCase):
    def test_load_code_tasks_validates_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            manifest_path = Path(tmp) / "tasks.json"
            manifest_path.write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "corpus": "test_corpus",
                        "tasks": [
                            {
                                "id": "task_lookup",
                                "surface": "lookup",
                                "query": "TestSymbol",
                                "expected_any": ["test.hpp"],
                                "limit": 3,
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            manifest = load_code_tasks(manifest_path)
            self.assertEqual(manifest.corpus, "test_corpus")
            self.assertEqual(len(manifest.tasks), 1)
            self.assertEqual(manifest.tasks[0].task_id, "task_lookup")
            self.assertEqual(manifest.tasks[0].expected_any, ("test.hpp",))

    def test_load_code_tasks_rejects_missing_query(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            manifest_path = Path(tmp) / "tasks.json"
            manifest_path.write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "corpus": "test_corpus",
                        "tasks": [
                            {
                                "id": "task_lookup",
                                "surface": "lookup",
                                "query": "  ",
                                "expected_any": ["test.hpp"],
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            with self.assertRaises(ValueError):
                load_code_tasks(manifest_path)

    def test_brick_model_index_queries(self) -> None:
        data = {
            "nodes": [
                {
                    "id": "class:1",
                    "name": "MyClass",
                    "kind": "class",
                    "path": "src/my_class.h",
                    "line": 10,
                    "attributes": {"source.scope": "my::ns"},
                },
                {
                    "id": "function:2",
                    "name": "myFunc",
                    "kind": "function",
                    "path": "src/my_class.cpp",
                    "line": 20,
                },
            ],
            "edges": [
                {
                    "id": "edge:1",
                    "source": "class:1",
                    "target": "function:2",
                    "kind": "defines",
                },
                {
                    "id": "edge:2",
                    "source": "function:2",
                    "target": "class:1",
                    "kind": "calls",
                },
            ],
            "metrics": {
                "cycle_count": 0,
                "cyclic_node_count": 0,
                "unresolved_dependency_count": 0,
                "isolated_symbol_count": 0,
                "max_fan_in": 1,
                "max_fan_out": 1,
                "structural_health": {
                    "score": 95.0,
                    "acyclicity": 1.0,
                    "connectedness": 1.0,
                },
            },
        }
        index = BrickModelIndex(data)
        lookup_res = index.lookup("MyClass")
        self.assertIn("MyClass", lookup_res)
        self.assertIn("src/my_class.h:10", lookup_res)

        impact_res = index.impact("MyClass")
        self.assertIn("myFunc", impact_res)

        explore_res = index.explore("MyClass")
        self.assertIn("defines", explore_res)

        arch_res = index.architecture("cycles")
        self.assertIn("score: 95.0", arch_res)

    def test_scope_and_duplicate_helpers(self) -> None:
        payload = "line 1\nline 2\nline 1\n"
        dup_bytes = _duplicate_normalized_line_bytes(payload)
        self.assertGreater(dup_bytes, 0)

        norm = _normalized_payload("\x1b[31mhello\x1b[0m")
        self.assertEqual(norm, "hello")

        first = _first_useful_byte("prefix target suffix", ["target"])
        self.assertEqual(first, len("prefix target".encode("utf-8")))

    def test_dry_run_worker(self) -> None:
        plan_path = Path(__file__).parent / "plans" / "code_intelligence_foundation_ablation.json"
        plan = ExperimentPlan.load(plan_path)
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            arm_dir = tmp_path / "arm"
            arm_dir.mkdir()
            arm_ctx = ArmContext(
                plan=plan,
                arm=plan.arms[1],  # brick_foundation
                repo_root=tmp_path,
                build_dir=tmp_path,
                run_dir=tmp_path,
                arm_dir=arm_dir,
                stamp="test",
                dry_run=True,
            )
            wctx = WorkerContext(
                arm=arm_ctx,
                step=plan.steps[0],
                step_index=0,
                env={},
                params={"backend": "brick_foundation"},
            )
            res = run_code_intelligence(wctx)
            self.assertEqual(res.status, "ok")
            self.assertEqual(res.metrics["cycle_detection_supported"], 1.0)
            self.assertEqual(res.metrics["storage_footprint_mb"], 119.0)


if __name__ == "__main__":
    unittest.main()
