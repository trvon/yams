#!/usr/bin/env python3
"""Behavior contracts for xplan experiment identity and reporting."""

from __future__ import annotations

import hashlib
import io
import json
import math
import os
import signal
import subprocess
import sys
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

XPLAN_ROOT = Path(__file__).resolve().parent
if str(XPLAN_ROOT) not in sys.path:
    sys.path.insert(0, str(XPLAN_ROOT))

from analyze_query_class import (  # noqa: E402
    compare_arms,
    load_by_type,
    paired_candidate_rescue,
    paired_route_risk,
)
from model import ExperimentPlan  # noqa: E402
from report import _baseline_row  # noqa: E402
import artifacts as xplan_artifacts  # noqa: E402
import runner as xplan_runner  # noqa: E402
import summarize as xplan_summarize  # noqa: E402
import workers.retrieval_quality as retrieval_quality_worker  # noqa: E402
from artifacts import file_sha256, source_set_sha256  # noqa: E402
from workers.multi_client import _clone_corpus_seed, _metrics_from_record  # noqa: E402
from workers.base import WorkerResult  # noqa: E402
from workers.mixed_corpus import (  # noqa: E402
    analyze_mixed_cluster_overlap,
    analyze_mixed_corpus_debug,
    materialize_mixed_beir_manifest,
)
from workers.ingestion_e2e import (  # noqa: E402
    extract_metrics as extract_ingestion_metrics,
    ingestion_experiment_identity,
    require_ingestion_experiment_identity,
    validate_ingestion_contract,
)
from workers.retrieval_quality import (  # noqa: E402
    _benchmark_command,
    _disable_semantic_neighbor_backfill,
    _mark_shared_topology_seed_reuse,
    _merge_benchmark_env,
    _reset_measured_outputs,
    _retarget_benchmark_cache_engine,
    clone_benchmark_state,
    daemon_graph_construction_fingerprint,
    describe_process_failure,
    parse_benchmark_cache_metrics,
    parse_benchmark_setup_metrics,
    parse_daemon_graph_topology_fingerprint,
    parse_debug_jsonl,
    require_daemon_native_graph_inputs,
    require_shared_topology_construction_identity,
    require_steady_state,
)


class VectorBuildCompositionTests(unittest.TestCase):
    @staticmethod
    def _target_block(meson_source: str, target_name: str) -> str:
        start = meson_source.index(f"static_library('{target_name}'")
        end = meson_source.index("\n)", start)
        return meson_source[start:end]

    def test_optional_sqlite_vec_isa_flags_stay_in_vendor_translation_units(
        self,
    ) -> None:
        meson_source = (
            XPLAN_ROOT.parents[2] / "src" / "vector" / "meson.build"
        ).read_text(encoding="utf-8")

        vendor_start = meson_source.index(
            "sqlite_vec_cpp_lib = static_library('yams_sqlite_vec_cpp'"
        )
        vendor_end = meson_source.index("\n  )", vendor_start)
        vendor_target = meson_source[vendor_start:vendor_end]

        backend_start = meson_source.index(
            "sqlite_vec_backend_lib = static_library('yams_sqlite_vec_backend'"
        )
        backend_end = meson_source.index("\n)", backend_start)
        backend_target = meson_source[backend_start:backend_end]

        self.assertIn("vec_simd_cppargs", vendor_target)
        self.assertNotIn("vec_simd_cppargs", backend_target)
        self.assertNotIn("sqlite_vec_feature_cppargs", backend_target)

    def test_simeon_vector_and_retrieval_sources_are_separate_targets(self) -> None:
        meson_source = (
            XPLAN_ROOT.parents[2] / "src" / "vector" / "meson.build"
        ).read_text(encoding="utf-8")

        common_target = self._target_block(meson_source, "yams_simeon_common")
        vector_target = self._target_block(meson_source, "yams_simeon_vector")
        retrieval_target = self._target_block(meson_source, "yams_simeon_retrieval")

        self.assertIn("simeon_common_srcs", common_target)
        self.assertIn("simeon_vector_srcs", vector_target)
        self.assertNotIn("simeon_retrieval_srcs", vector_target)
        self.assertIn("simeon_retrieval_srcs", retrieval_target)
        self.assertNotIn("simeon_vector_srcs", retrieval_target)

    def test_public_vector_dependency_does_not_export_simeon_build_internals(self) -> None:
        meson_source = (
            XPLAN_ROOT.parents[2] / "src" / "vector" / "meson.build"
        ).read_text(encoding="utf-8")
        start = meson_source.index("yams_vector_dep = declare_dependency(")
        end = meson_source.index("\n)", start)
        public_dependency = meson_source[start:end]

        self.assertNotIn("simeon_inc", public_dependency)
        self.assertNotIn("simeon_simd_defines", public_dependency)

    def test_vector_database_uses_lifecycle_capability_without_raw_sqlite_access(
        self,
    ) -> None:
        source = (
            XPLAN_ROOT.parents[2] / "src" / "vector" / "vector_database.cpp"
        ).read_text(encoding="utf-8")

        self.assertIn("IEmbeddingLifecycleStore", source)
        self.assertNotIn("#include <sqlite3.h>", source)
        self.assertNotIn("getDbHandle()", source)
        self.assertNotIn("sqlite3_prepare", source)

    def test_public_simeon_backend_config_does_not_expose_vendor_configs(self) -> None:
        header = (
            XPLAN_ROOT.parents[2]
            / "include"
            / "yams"
            / "search"
            / "simeon_lexical_backend.h"
        ).read_text(encoding="utf-8")

        self.assertNotIn("#include <simeon/concept_mining.hpp>", header)
        self.assertNotIn("#include <simeon/fragment_geometry.hpp>", header)
        self.assertNotIn("simeon::ConceptConfig", header)
        self.assertNotIn("simeon::FragmentGeometryConfig", header)

    def test_retrieval_cache_readiness_requires_backend_validation(self) -> None:
        source = (
            XPLAN_ROOT.parents[2]
            / "tests"
            / "benchmarks"
            / "search"
            / "retrieval_quality_bench.cpp"
        ).read_text(encoding="utf-8")

        self.assertIn("bool validatedIndexReusable", source)
        self.assertIn("metadata.vectorIndexReady = validatedIndexReusable;", source)
        self.assertNotIn("persistedHnsw.reusable(", source)

    def test_engine_comparator_warms_each_backend_before_timing(self) -> None:
        source = (
            XPLAN_ROOT.parents[2]
            / "tests"
            / "benchmarks"
            / "vector_backend_engine_compare.cpp"
        ).read_text(encoding="utf-8")

        warmup = source.index("auto warmup = backend.searchSimilar(queries.front()")
        timed_loop = source.index("for (size_t i = 0; i < queries.size(); ++i)")
        self.assertLess(warmup, timed_loop)

    def test_vec0_non_persistence_contract_has_no_stale_positive_claims(self) -> None:
        coordinator = (
            XPLAN_ROOT.parents[2]
            / "include"
            / "yams"
            / "daemon"
            / "components"
            / "VectorIndexCoordinator.h"
        ).read_text(encoding="utf-8")
        benchmark = (
            XPLAN_ROOT.parents[2]
            / "tests"
            / "benchmarks"
            / "search"
            / "retrieval_quality_bench.cpp"
        ).read_text(encoding="utf-8")
        vector_tests = (
            XPLAN_ROOT.parents[2]
            / "tests"
            / "unit"
            / "vector"
            / "sqlite_vec_backend_comprehensive_catch2_test.cpp"
        ).read_text(encoding="utf-8")

        self.assertNotIn("persisted HNSW index", coordinator)
        self.assertNotIn("persisted HNSW is still unusable", benchmark)
        self.assertNotIn("search should work without rebuild", vector_tests)
        self.assertNotIn("[vec0_persist]", vector_tests)

    def test_vector_build_messages_do_not_overstate_simd_uplift(self) -> None:
        meson_source = (
            XPLAN_ROOT.parents[2] / "src" / "vector" / "meson.build"
        ).read_text(encoding="utf-8")

        self.assertNotIn("~33x faster", meson_source)


class RetrievalQualityEnvironmentTests(unittest.TestCase):
    def test_daemon_native_graph_rejects_benchmark_owned_topology_inputs(self) -> None:
        params = {"require_daemon_native_graph": True}

        self.assertIsNone(require_daemon_native_graph_inputs(params, {}))
        self.assertIn(
            "benchmark semantic-neighbor seeding",
            require_daemon_native_graph_inputs(
                params, {"YAMS_BENCH_SEED_SEMANTIC_NEIGHBORS": "1"}
            ),
        )
        self.assertIn(
            "shared warm cache",
            require_daemon_native_graph_inputs(
                {**params, "shared_warm_cache": "seed"}, {}
            ),
        )
        self.assertIn(
            "semantic-neighbor backfill",
            require_daemon_native_graph_inputs(
                params, {"YAMS_ENABLE_SEMANTIC_NEIGHBOR_BACKFILL": "0"}
            ),
        )

    def test_candidate_rescue_plan_uses_daemon_native_graph_inputs(self) -> None:
        plan = json.loads(
            (XPLAN_ROOT / "plans" / "search_candidate_rescue_graph.json").read_text(
                encoding="utf-8"
            )
        )
        env = plan["fixed"]["env"]
        params = plan["fixed"]["params"]

        self.assertTrue(params["require_daemon_native_graph"])
        self.assertNotIn("YAMS_BENCH_SEED_SEMANTIC_NEIGHBORS", env)
        self.assertNotIn("seed_semantic_neighbors", params)
        self.assertNotIn("shared_warm_cache", params)
        self.assertEqual(params["graph_vector_seed_probe"], 16)
        self.assertIn(
            "topology_vector_seeds_nonzero_rate",
            plan["validate"]["require_metrics"],
        )
        self.assertIn(
            "topology_path_seed_neighbors_rate",
            plan["validate"]["require_metrics"],
        )
        self.assertIsNone(require_daemon_native_graph_inputs(params, env))
        self.assertIn(
            "--yams-require-daemon-native-graph=true",
            _benchmark_command(Path("bench"), "BM_RetrievalQuality", params),
        )

    def test_retrieval_quality_imports_without_posix_resource_module(self) -> None:
        script = f"""
import builtins
import sys

real_import = builtins.__import__

def import_without_resource(name, *args, **kwargs):
    if name == "resource":
        raise ModuleNotFoundError("No module named 'resource'")
    return real_import(name, *args, **kwargs)

builtins.__import__ = import_without_resource
sys.path.insert(0, {str(XPLAN_ROOT)!r})
import workers.retrieval_quality
"""
        proc = subprocess.run(
            [sys.executable, "-c", script],
            capture_output=True,
            text=True,
            check=False,
        )

        self.assertEqual(proc.returncode, 0, proc.stderr)

    def test_binary_identity_hashes_executable_contents(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            binary = Path(tmp) / "bench"
            binary.write_bytes(b"retrieval-binary")
            self.assertEqual(
                file_sha256(binary), hashlib.sha256(b"retrieval-binary").hexdigest()
            )

    def test_source_identity_hashes_paths_and_contents(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            worker = root / "workers" / "quality.py"
            worker.parent.mkdir()
            worker.write_text("VERSION = 1\n", encoding="utf-8")
            plan = root / "plan.json"
            plan.write_text('{"name":"identity"}\n', encoding="utf-8")

            before = source_set_sha256(root, [worker, plan])
            worker.write_text("VERSION = 2\n", encoding="utf-8")
            after = source_set_sha256(root, [worker, plan])

            self.assertRegex(before, r"^[0-9a-f]{64}$")
            self.assertNotEqual(before, after)

    def test_dirty_git_source_snapshot_preserves_patch_and_identity(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            subprocess.run(["git", "init", "-q", str(root)], check=True)
            subprocess.run(
                ["git", "-C", str(root), "config", "user.email", "xplan@example.invalid"],
                check=True,
            )
            subprocess.run(
                ["git", "-C", str(root), "config", "user.name", "xplan-test"], check=True
            )
            source = root / "source.cpp"
            source.write_text("int value = 1;\n", encoding="utf-8")
            subprocess.run(["git", "-C", str(root), "add", "source.cpp"], check=True)
            subprocess.run(
                ["git", "-C", str(root), "commit", "-qm", "baseline"], check=True
            )
            source.write_text("int value = 2;\n", encoding="utf-8")
            run_dir = root / "artifacts"
            run_dir.mkdir()

            snapshot = xplan_artifacts.capture_git_source_snapshot(root, run_dir)

            self.assertTrue(snapshot["worktree_dirty"])
            self.assertRegex(snapshot["worktree_patch_sha256"], r"^[0-9a-f]{64}$")
            patch_path = run_dir / snapshot["worktree_patch_path"]
            self.assertTrue(patch_path.is_file())
            self.assertIn("+int value = 2;", patch_path.read_text(encoding="utf-8"))

    def test_binary_identity_rejects_mid_run_relink(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            binary = Path(tmp) / "bench"
            binary.write_bytes(b"first-binary")
            expected = file_sha256(binary)
            xplan_runner._require_binary_identity(binary, expected)

            binary.write_bytes(b"second-binary")
            with self.assertRaisesRegex(RuntimeError, "identity changed"):
                xplan_runner._require_binary_identity(binary, expected)

    def test_runner_resolves_and_binds_the_actual_quality_binary(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            plan_path = root / "plan.json"
            plan_path.write_text(
                json.dumps(
                    {
                        "name": "binary_identity",
                        "fixed": {"params": {}},
                        "arms": [{"name": "control"}],
                        "steps": [{"worker": "retrieval_quality"}],
                    }
                ),
                encoding="utf-8",
            )
            override = root / "explicit-bench"
            override.write_bytes(b"explicit")
            plan = ExperimentPlan.load(plan_path)

            actual = xplan_runner._resolve_retrieval_quality_binary(
                plan,
                repo_root=root,
                build_dir=root / "build",
                ambient={"YAMS_BENCH_BIN": str(override)},
            )

            self.assertEqual(actual, override.resolve())
            self.assertEqual(file_sha256(actual), hashlib.sha256(b"explicit").hexdigest())

    def test_runner_rejects_arm_specific_quality_binaries(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            plan_path = root / "plan.json"
            plan_path.write_text(
                json.dumps(
                    {
                        "name": "binary_identity",
                        "arms": [
                            {"name": "one", "params": {"binary": "one"}},
                            {"name": "two", "params": {"binary": "two"}},
                        ],
                        "steps": [{"worker": "retrieval_quality"}],
                    }
                ),
                encoding="utf-8",
            )
            plan = ExperimentPlan.load(plan_path)

            with self.assertRaisesRegex(ValueError, "one quality binary"):
                xplan_runner._resolve_retrieval_quality_binary(
                    plan,
                    repo_root=root,
                    build_dir=root / "build",
                    ambient={},
                )

    def test_report_replays_resolved_plan_instead_of_mutated_source(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "plan.json"
            source.write_text(
                json.dumps(
                    {
                        "name": "identity",
                        "repeats": 3,
                        "baseline": "original",
                        "arms": [{"name": "original"}],
                        "steps": [{"worker": "retrieval_quality"}],
                    }
                ),
                encoding="utf-8",
            )
            original = ExperimentPlan.load(source)
            run_dir = root / "run"
            run_dir.mkdir()
            (run_dir / "plan.resolved.json").write_text(
                json.dumps(
                    original.resolved_dict(stamp="test", repo_root=root, git_sha="abc")
                ),
                encoding="utf-8",
            )

            source.write_text(
                json.dumps(
                    {
                        "name": "identity",
                        "repeats": 1,
                        "baseline": "mutated",
                        "arms": [{"name": "mutated"}],
                        "steps": [{"worker": "retrieval_quality"}],
                    }
                ),
                encoding="utf-8",
            )

            replayed = xplan_runner.load_resolved_report_plan(run_dir)

            self.assertEqual(replayed.repeats, 3)
            self.assertEqual(replayed.baseline, "original")
            self.assertEqual([arm.name for arm in replayed.arms], ["original"])

    def test_run_rejects_nonempty_output_directory_before_reusing_seed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            plan_path = root / "plan.json"
            plan_path.write_text(
                json.dumps(
                    {
                        "name": "identity",
                        "arms": [{"name": "control"}],
                        "steps": [{"worker": "retrieval_quality"}],
                    }
                ),
                encoding="utf-8",
            )
            run_dir = root / "existing-run"
            stale_seed = run_dir / "shared_state" / "corpus" / "seed_ready"
            stale_seed.parent.mkdir(parents=True)
            stale_seed.write_text("ready\n", encoding="utf-8")

            stderr = io.StringIO()
            with redirect_stderr(stderr):
                exit_code = xplan_runner.cmd_run(
                    SimpleNamespace(
                        plan=str(plan_path),
                        arm=[],
                        stamp="",
                        build_dir="",
                        out_dir=str(run_dir),
                        dry_run=True,
                        continue_on_failure=False,
                        skip_summary=True,
                    )
                )

            self.assertEqual(exit_code, 2)
            self.assertIn("refusing to reuse nonempty xplan run directory", stderr.getvalue())
            self.assertEqual(stale_seed.read_text(encoding="utf-8"), "ready\n")
            self.assertFalse((run_dir / "plan.resolved.json").exists())

    def test_multi_repeat_metrics_keep_each_repeat_status_independent(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            plan_path = root / "plan.json"
            run_dir = root / "run"
            plan_path.write_text(
                json.dumps(
                    {
                        "name": "repeat_status",
                        "repeats": 2,
                        "arms": [{"name": "control"}],
                        "steps": [{"worker": "external_script"}],
                        "validate": {
                            "require_files": ["metrics.json"],
                            "require_metric_status": ["ok", "failed"],
                            "require_metrics": [],
                        },
                    }
                ),
                encoding="utf-8",
            )
            results = iter(
                [
                    WorkerResult(status="failed", exit_code=1, metrics={"probe": 1.0}),
                    WorkerResult(status="ok", exit_code=0, metrics={"probe": 2.0}),
                ]
            )

            def run_worker(_context):
                return next(results)

            source_snapshot = {
                "worktree_dirty": False,
                "worktree_patch_sha256": "",
                "source_snapshot_sha256": "test-snapshot",
            }
            with (
                patch.object(xplan_runner, "get_worker", return_value=run_worker),
                patch.object(
                    xplan_runner,
                    "capture_git_source_snapshot",
                    return_value=source_snapshot,
                ),
                patch.object(xplan_runner, "source_set_sha256", return_value="test-source"),
                redirect_stdout(io.StringIO()),
            ):
                exit_code = xplan_runner.cmd_run(
                    SimpleNamespace(
                        plan=str(plan_path),
                        arm=[],
                        stamp="",
                        build_dir="",
                        out_dir=str(run_dir),
                        dry_run=True,
                        continue_on_failure=False,
                        skip_summary=True,
                    )
                )

            self.assertEqual(exit_code, 1)
            rep00 = json.loads(
                (run_dir / "arms" / "control" / "rep00" / "metrics.json").read_text()
            )
            rep01 = json.loads(
                (run_dir / "arms" / "control" / "rep01" / "metrics.json").read_text()
            )
            self.assertEqual(rep00["status"], "failed")
            self.assertEqual(rep01["status"], "ok")

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

    def test_retrieval_quality_passes_typed_simeon_space_config(self) -> None:
        command = _benchmark_command(
            Path("retrieval_quality_bench"),
            "BM_RetrievalQuality",
            {
                "simeon_encoder_profile": "fixed_hash_384",
                "simeon_fragment_encoder_profile": "fixed_hash_384",
                "simeon_fragment_geometry": True,
                "simeon_bm25": True,
                "simeon_router": True,
            },
        )
        self.assertIn("--yams-simeon-encoder-profile=fixed_hash_384", command)
        self.assertIn(
            "--yams-simeon-fragment-encoder-profile=fixed_hash_384", command
        )
        self.assertIn("--yams-simeon-fragment-geometry=true", command)
        self.assertIn("--yams-simeon-bm25=true", command)
        self.assertIn("--yams-simeon-router=true", command)

    def test_retrieval_quality_retry_clears_append_only_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            paths = [Path(tmp) / "debug.jsonl", Path(tmp) / "topology_clusters.json"]
            for path in paths:
                path.write_text("stale\n", encoding="utf-8")
            _reset_measured_outputs(paths)
            self.assertTrue(all(not path.exists() for path in paths))

    def test_retrieval_quality_preserves_native_process_failure(self) -> None:
        output = """startup noise
[YAMS] dcheck failed
src/metadata/metadata_repository.cpp:1251: cachedExtractedCount_ <= cachedDocumentCount_
"""

        message = describe_process_failure(-signal.SIGABRT, output)

        self.assertIsNotNone(message)
        self.assertIn("SIGABRT", message or "")
        self.assertIn("metadata_repository.cpp:1251", message or "")
        self.assertIsNone(describe_process_failure(0, output))

    def test_shared_clone_reuses_primed_topology_inputs(self) -> None:
        env: dict[str, str] = {}
        _mark_shared_topology_seed_reuse(env)
        self.assertEqual(env["YAMS_BENCH_REUSE_SEEDED_TOPOLOGY_INPUTS"], "1")
        self.assertEqual(env["YAMS_ENABLE_SEMANTIC_NEIGHBOR_BACKFILL"], "0")

    def test_shared_seed_disables_background_topology_input_mutation(self) -> None:
        env: dict[str, str] = {}
        _disable_semantic_neighbor_backfill(env)
        self.assertEqual(env["YAMS_ENABLE_SEMANTIC_NEIGHBOR_BACKFILL"], "0")
        self.assertNotIn("YAMS_BENCH_REUSE_SEEDED_TOPOLOGY_INPUTS", env)

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

    def test_shared_seed_retargets_only_vector_index_identity(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            cache = Path(tmp)
            metadata_path = cache / "retrieval_bench_cache.json"
            metadata_path.write_text(
                json.dumps(
                    {
                        "dataset": "local-manifest",
                        "search_engine": "simeon_pq_adc",
                        "status": "primed",
                        "embedded_docs": 3985,
                        "vector_count": 12699,
                        "vector_index_ready": True,
                    }
                ),
                encoding="utf-8",
            )

            _retarget_benchmark_cache_engine(cache, "exact_scan")

            retargeted = json.loads(metadata_path.read_text(encoding="utf-8"))
            self.assertEqual(retargeted["search_engine"], "exact_scan")
            self.assertFalse(retargeted["vector_index_ready"])
            self.assertEqual(retargeted["status"], "primed")
            self.assertEqual(retargeted["embedded_docs"], 3985)
            self.assertEqual(retargeted["vector_count"], 12699)

    def test_shared_seed_preserves_ready_index_for_same_engine(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            cache = Path(tmp)
            metadata_path = cache / "retrieval_bench_cache.json"
            metadata_path.write_text(
                json.dumps(
                    {
                        "search_engine": "simeon_pq_adc",
                        "status": "primed",
                        "vector_index_ready": True,
                    }
                ),
                encoding="utf-8",
            )

            _retarget_benchmark_cache_engine(cache, "simeon_pq_adc")

            retargeted = json.loads(metadata_path.read_text(encoding="utf-8"))
            self.assertEqual(retargeted["search_engine"], "simeon_pq_adc")
            self.assertTrue(retargeted["vector_index_ready"])

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

    def test_daemon_graph_fingerprint_uses_canonical_persisted_shape(self) -> None:
        metrics = {
            "daemon_graph_canonicalized": 1.0,
            "daemon_graph_canonical_edges_generated": 32000.0,
            "daemon_graph_topology_document_nodes": 2000.0,
            "daemon_graph_topology_semantic_edges": 13914.0,
            "daemon_graph_topology_documents_with_neighbors": 2000.0,
            "daemon_graph_topology_isolated_documents": 0.0,
            "daemon_graph_semantic_edges_created": 64000.0,
        }

        fingerprint = daemon_graph_construction_fingerprint(metrics, "fixed-topology")

        self.assertEqual(
            json.loads(fingerprint),
            {
                "document_nodes": 2000,
                "semantic_edges": 13914,
                "documents_with_neighbors": 2000,
                "isolated_documents": 0,
                "topology_fingerprint": "fixed-topology",
            },
        )
        metrics["daemon_graph_semantic_edges_created"] = 59360.0
        metrics["daemon_graph_canonical_edges_generated"] = 33024.0
        self.assertEqual(
            daemon_graph_construction_fingerprint(metrics, "fixed-topology"), fingerprint
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
        self.assertEqual(len(vector_arms), 3)
        for arm in vector_arms:
            self.assertEqual(arm["factors"].get("text"), "off")
            self.assertEqual(arm["factors"].get("simeon_text"), "off")

    def test_vector_pq_coarse_routing_plan_has_truthful_oracle_and_transfer_gate(
        self,
    ) -> None:
        plan = json.loads(
            (XPLAN_ROOT / "plans" / "vector_pq_coarse_routing.json").read_text(
                encoding="utf-8"
            )
        )

        self.assertGreaterEqual(plan["repeats"], 3)
        self.assertIn("does not encode a product-promotion", plan["description"])
        self.assertEqual(plan["fixed"]["env"]["YAMS_BENCH_TOPOLOGY_MODE"], "disabled")
        self.assertEqual(plan["fixed"]["params"]["datasets"], ["scifact", "nfcorpus"])
        arms = {arm["name"]: arm for arm in plan["arms"]}
        self.assertEqual(
            set(arms),
            {"exact_scan_oracle", "simeon_pq_global", "vec0_global_ann"},
        )
        self.assertEqual(
            arms["exact_scan_oracle"]["params"]["vector_search_engine"],
            "exact_scan",
        )
        self.assertEqual(
            arms["simeon_pq_global"]["params"]["vector_search_engine"],
            "simeon_pq_adc",
        )
        self.assertEqual(
            arms["vec0_global_ann"]["params"]["vector_search_engine"],
            "vec0_l2",
        )
        self.assertEqual(
            arms["vec0_global_ann"]["params"]["vector_index_persistence_contract"],
            "unavailable",
        )
        self.assertGreaterEqual(
            int(
                arms["vec0_global_ann"]["env"][
                    "YAMS_BENCH_DAEMON_READY_TIMEOUT_MS"
                ]
            ),
            60_000,
        )
        for arm in arms.values():
            self.assertEqual(arm["factors"]["retrieval"], "product_hybrid")
        self.assertEqual(
            arms["simeon_pq_global"]["params"]["vector_work_observation_contract"],
            {
                "rows_visited": "required",
                "exact_distance_evaluations": "required",
                "ann_candidate_budget": "required",
            },
        )
        self.assertEqual(
            arms["exact_scan_oracle"]["params"]["vector_work_observation_contract"],
            {
                "rows_visited": "required",
                "exact_distance_evaluations": "required",
                "ann_candidate_budget": "unavailable",
            },
        )
        self.assertEqual(
            arms["vec0_global_ann"]["params"]["vector_work_observation_contract"],
            {
                "rows_visited": "unavailable",
                "exact_distance_evaluations": "unavailable",
                "ann_candidate_budget": "unavailable",
            },
        )
        metrics = set(plan["steps"][0]["metrics"])
        self.assertTrue(
            {
                "mrr",
                "recall_at_k",
                "mixed_source_min_ndcg_at_k",
                "mixed_mrr_scifact",
                "mixed_mrr_nfcorpus",
                "mixed_recall_at_k_scifact",
                "mixed_recall_at_k_nfcorpus",
                "mixed_ndcg_at_k_scifact",
                "mixed_ndcg_at_k_nfcorpus",
                "search_latency_ms_cold_first",
                "search_latency_ms_first_pass_first",
                "search_latency_ms_warmed_p50",
                "search_warmup_completed_queries",
                "search_latency_ms_p99",
                "search_throughput_qps",
                "vector_index_prepare_ms",
                "vector_index_persist_ms",
                "vector_database_bytes",
                "vector_wal_bytes",
                "vector_query_ready_after",
                "vector_rows_visited_observation_rate",
                "vector_exact_distance_evaluations_observation_rate",
            }.issubset(metrics)
        )
        summarized = set(plan["summarize"]["primary"])
        self.assertTrue(
            {
                "vector_total_rows_visited_actual_avg",
                "vector_total_exact_distance_evaluations_actual_avg",
                "vector_exact_oracle_recall_at_k",
                "vector_exact_oracle_gap_at_k",
                "vector_exact_oracle_paired_queries",
                "paired_query_mrr_delta",
                "paired_query_mrr_ci_low",
                "paired_query_mrr_ci_high",
                "search_latency_ms_p99",
                "search_throughput_qps",
                "vector_index_prepare_ms",
                "vector_index_persist_ms",
                "vector_database_bytes",
                "vector_wal_bytes",
                "vector_persisted_hnsw_nodes",
                "vector_persisted_vec0_rows",
                "vector_persisted_pq_codes",
            }.issubset(summarized)
        )
        self.assertEqual(
            plan["summarize"]["paired_vector_oracle"],
            {
                "arm": "exact_scan_oracle",
                "component": "vector",
                "top_k": 10,
                "identity_file": "datasets/mixed_beir/mixed_corpus_identity.json",
            },
        )
        self.assertEqual(
            plan["summarize"]["paired_query_analysis"]["metrics"],
            ["mrr", "recall_at_k", "ndcg_at_k"],
        )
        self.assertNotIn("promotion_gates", plan["summarize"])

    def test_vector_pq_crossover_uses_real_generalized_memory_corpora(self) -> None:
        plan = json.loads(
            (XPLAN_ROOT / "plans" / "vector_pq_crossover.json").read_text(
                encoding="utf-8"
            )
        )

        self.assertEqual(plan["name"], "vector_pq_scale_checkpoint")
        self.assertIn("not a crossover sweep", plan["description"].lower())
        self.assertGreaterEqual(plan["repeats"], 3)
        self.assertEqual(
            plan["fixed"]["params"]["datasets"],
            ["scifact", "nfcorpus", "fiqa"],
        )
        self.assertEqual(plan["fixed"]["params"]["documents_per_dataset"], 0)
        self.assertEqual(plan["fixed"]["env"]["YAMS_BENCH_TOPOLOGY_MODE"], "disabled")
        self.assertEqual(
            plan["arms"][0]["params"]["vector_search_engine"], "simeon_pq_adc"
        )
        self.assertEqual(plan["arms"][0]["factors"]["retrieval"], "product_hybrid")
        self.assertIn(
            "vector_adc_scoring_ms_avg", set(plan["steps"][0]["metrics"])
        )
        self.assertTrue(
            {
                "search_latency_ms_first_pass_first",
                "search_latency_ms_p99",
                "search_throughput_qps",
                "vector_index_prepare_ms",
                "vector_index_persist_ms",
                "vector_database_bytes",
                "vector_wal_bytes",
            }.issubset(set(plan["steps"][0]["metrics"]))
        )
        self.assertIn(
            "vector_total_rows_visited_actual_avg",
            set(plan["summarize"]["primary"]),
        )

    def test_vector_pq_rerank_quality_is_a_single_factor_multicorpus_gate(
        self,
    ) -> None:
        plan = json.loads(
            (XPLAN_ROOT / "plans" / "vector_pq_rerank_quality.json").read_text(
                encoding="utf-8"
            )
        )

        self.assertGreaterEqual(plan["repeats"], 3)
        self.assertEqual(plan["fixed"]["env"]["YAMS_BENCH_TOPOLOGY_MODE"], "disabled")
        self.assertEqual(plan["fixed"]["params"]["datasets"], ["scifact", "nfcorpus"])
        self.assertEqual(plan["baseline"], "simeon_rerank_2")
        arms = {arm["name"]: arm for arm in plan["arms"]}
        self.assertEqual(
            set(arms),
            {
                "simeon_rerank_1",
                "simeon_rerank_2",
                "simeon_rerank_4",
                "simeon_rerank_8",
                "exact_scan_oracle",
            },
        )
        for factor in (1, 2, 4, 8):
            arm = arms[f"simeon_rerank_{factor}"]
            self.assertEqual(arm["factors"]["retrieval"], "product_hybrid")
            self.assertEqual(arm["params"]["vector_search_engine"], "simeon_pq_adc")
            self.assertEqual(arm["params"]["simeon_pq_rerank_factor"], factor)
            self.assertEqual(arm["factors"]["rerank_factor"], str(factor))
            self.assertEqual(
                arm["params"]["vector_work_observation_contract"],
                {
                    "rows_visited": "required",
                    "exact_distance_evaluations": "required",
                    "ann_candidate_budget": "required",
                },
            )
        self.assertNotIn(
            "simeon_pq_rerank_factor", arms["exact_scan_oracle"]["params"]
        )
        self.assertEqual(
            arms["exact_scan_oracle"]["factors"]["retrieval"], "product_hybrid"
        )
        self.assertEqual(
            arms["exact_scan_oracle"]["params"]["vector_work_observation_contract"],
            {
                "rows_visited": "required",
                "exact_distance_evaluations": "required",
                "ann_candidate_budget": "unavailable",
            },
        )
        metrics = set(plan["steps"][0]["metrics"])
        self.assertTrue(
            {
                "mrr",
                "recall_at_k",
                "mixed_source_min_mrr",
                "mixed_source_min_recall_at_k",
                "mixed_source_min_ndcg_at_k",
                "mixed_mrr_scifact",
                "mixed_mrr_nfcorpus",
                "mixed_recall_at_k_scifact",
                "mixed_recall_at_k_nfcorpus",
                "mixed_ndcg_at_k_scifact",
                "mixed_ndcg_at_k_nfcorpus",
                "search_latency_ms_cold_first",
                "search_latency_ms_first_pass_first",
                "search_latency_ms_warmed_p50",
                "search_warmup_completed_queries",
                "search_latency_ms_p95",
                "search_latency_ms_p99",
                "search_throughput_qps",
                "vector_index_prepare_ms",
                "vector_index_persist_ms",
                "vector_database_bytes",
                "vector_wal_bytes",
                "vector_query_ready_after",
                "vector_total_exact_distance_evaluations_actual_avg",
                "vector_result_materialization_ms_avg",
                "vector_exact_rerank_ms_avg",
            }.issubset(metrics)
        )
        required = set(plan["validate"]["require_metrics"])
        self.assertTrue(
            {
                "mixed_mrr_scifact",
                "mixed_mrr_nfcorpus",
                "mixed_recall_at_k_scifact",
                "mixed_recall_at_k_nfcorpus",
                "mixed_ndcg_at_k_scifact",
                "mixed_ndcg_at_k_nfcorpus",
            }.issubset(required)
        )
        self.assertEqual(
            plan["summarize"]["paired_vector_oracle"],
            {
                "arm": "exact_scan_oracle",
                "component": "vector",
                "top_k": 10,
                "identity_file": "datasets/mixed_beir/mixed_corpus_identity.json",
            },
        )
        self.assertTrue(
            {
                "vector_exact_oracle_recall_at_k",
                "vector_exact_oracle_gap_at_k",
                "vector_exact_oracle_paired_queries",
                "search_latency_ms_p99",
                "search_throughput_qps",
                "vector_database_bytes",
            }.issubset(set(plan["summarize"]["primary"]))
        )
        self.assertEqual(
            plan["summarize"]["paired_query_analysis"]["metrics"],
            ["mrr", "recall_at_k", "ndcg_at_k"],
        )
        self.assertEqual(
            plan["summarize"]["promotion_gates"]["candidate_arms"],
            ["simeon_rerank_1", "simeon_rerank_4", "simeon_rerank_8"],
        )

    def test_vector_pq_plans_surface_restart_reuse_evidence(self) -> None:
        for filename in (
            "vector_pq_coarse_routing.json",
            "vector_pq_rerank_quality.json",
            "vector_pq_crossover.json",
        ):
            with self.subTest(plan=filename):
                plan = json.loads(
                    (XPLAN_ROOT / "plans" / filename).read_text(encoding="utf-8")
                )
                self.assertIn(
                    "vector_index_reusable_before",
                    set(plan["steps"][0]["metrics"]),
                )
                self.assertIn(
                    "vector_index_reusable_before",
                    set(plan["summarize"]["primary"]),
                )

    def test_paired_vector_oracle_summary_is_query_aligned_and_fail_closed(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            plan_path = root / "plan.json"
            plan_path.write_text(
                json.dumps(
                    {
                        "name": "paired-vector-oracle",
                        "repeats": 1,
                        "baseline": "pq",
                        "arms": [
                            {"name": "pq", "factors": {"vector_engine": "pq"}},
                            {
                                "name": "exact",
                                "factors": {"vector_engine": "exact_scan"},
                            },
                        ],
                        "steps": [{"worker": "retrieval_quality"}],
                        "summarize": {
                            "primary": ["vector_exact_oracle_recall_at_k"],
                            "paired_vector_oracle": {
                                "arm": "exact",
                                "component": "vector",
                                "top_k": 3,
                                "identity_file": (
                                    "datasets/mixed_beir/mixed_corpus_identity.json"
                                ),
                            },
                        },
                    }
                ),
                encoding="utf-8",
            )
            plan = ExperimentPlan.load(plan_path)
            self.assertEqual(plan.summarize.paired_vector_oracle.arm, "exact")
            self.assertEqual(plan.summarize.paired_vector_oracle.top_k, 3)
            self.assertEqual(
                plan.resolved_dict(stamp="test", repo_root=root, git_sha=None)[
                    "summarize"
                ]["paired_vector_oracle"]["component"],
                "vector",
            )

            run_dir = root / "run"
            identity_path = run_dir / "datasets" / "mixed_beir"
            identity_path.mkdir(parents=True)
            (identity_path / "mixed_corpus_identity.json").write_text(
                json.dumps(
                    {
                        "query_order": ["nfcorpus__q0", "scifact__q1"],
                        "query_sources": {
                            "nfcorpus__q0": "nfcorpus",
                            "scifact__q1": "scifact",
                        },
                    }
                ),
                encoding="utf-8",
            )

            def write_arm(name: str, vector_ids: list[list[str]]) -> None:
                arm_dir = run_dir / "arms" / name
                arm_dir.mkdir(parents=True, exist_ok=True)
                (arm_dir / "metrics.json").write_text(
                    json.dumps({"status": "ok", "metrics": {"mrr": 0.5}}),
                    encoding="utf-8",
                )
                (arm_dir / "validation.json").write_text(
                    json.dumps({"ok": True, "issues": []}), encoding="utf-8"
                )
                events = []
                for query_index, ids in enumerate(vector_ids):
                    events.append(
                        {
                            "query_index": query_index,
                            "search_type": "hybrid",
                            "search_stats": {
                                "trace_component_hits_json": json.dumps(
                                    {
                                        "vector": {
                                            "unique_top_doc_ids": ids,
                                        }
                                    }
                                )
                            },
                        }
                    )
                (arm_dir / "debug.jsonl").write_text(
                    "".join(json.dumps(event) + "\n" for event in events),
                    encoding="utf-8",
                )

            write_arm("exact", [["a", "b", "c"], ["d", "e", "f"]])
            write_arm("pq", [["a", "c", "x"], ["d", "y", "z"]])

            summary = xplan_summarize.write_summary(plan, run_dir, stamp="paired")
            rows = {row["arm"]: row for row in summary["arms"]}
            pq_metrics = rows["pq"]["metrics"]
            self.assertAlmostEqual(
                pq_metrics["vector_exact_oracle_recall_at_k"], 0.5
            )
            self.assertAlmostEqual(pq_metrics["vector_exact_oracle_gap_at_k"], 0.5)
            self.assertEqual(pq_metrics["vector_exact_oracle_paired_queries"], 2.0)
            self.assertEqual(
                pq_metrics["vector_exact_oracle_paired_observations"], 2.0
            )
            self.assertAlmostEqual(
                pq_metrics["vector_exact_oracle_recall_at_k_nfcorpus"], 2.0 / 3.0
            )
            self.assertAlmostEqual(
                pq_metrics["vector_exact_oracle_recall_at_k_scifact"], 1.0 / 3.0
            )
            self.assertEqual(
                rows["exact"]["metrics"]["vector_exact_oracle_recall_at_k"], 1.0
            )
            self.assertTrue((run_dir / "paired_vector_oracle.json").is_file())

            write_arm("pq", [["a", "c", "x"]])
            invalid = xplan_summarize.write_summary(plan, run_dir, stamp="missing")
            invalid_rows = {row["arm"]: row for row in invalid["arms"]}
            self.assertFalse(invalid_rows["pq"]["valid"])
            self.assertIn(
                "query indices",
                json.dumps(invalid_rows["pq"]["validation_issues"]),
            )

    def test_summary_validation_failure_is_a_runner_hard_failure(self) -> None:
        self.assertFalse(
            xplan_runner._summary_has_hard_failure(
                {"arm_count": 2, "valid_count": 2, "failed_count": 0}
            )
        )

    def test_retrieval_benchmark_emits_per_query_quality_samples(self) -> None:
        source = (
            XPLAN_ROOT.parents[2]
            / "tests"
            / "benchmarks"
            / "search"
            / "retrieval_quality_bench.cpp"
        ).read_text(encoding="utf-8")

        self.assertIn('debugEntry.extraFields["query_metrics"]', source)
        for metric in ("mrr", "recall_at_k", "ndcg_at_k"):
            self.assertIn(f'{{"{metric}", scoreSample.', source)

    def test_paired_query_uncertainty_drives_machine_promotion_gates(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            plan_path = root / "plan.json"
            plan_path.write_text(
                json.dumps(
                    {
                        "name": "paired-query-gate",
                        "repeats": 1,
                        "baseline": "baseline",
                        "arms": [{"name": "baseline"}, {"name": "target"}],
                        "steps": [{"worker": "retrieval_quality"}],
                        "summarize": {
                            "primary": ["paired_query_mrr_delta"],
                            "paired_query_analysis": {
                                "metrics": ["mrr", "recall_at_k", "ndcg_at_k"],
                                "confidence_level": 0.95,
                                "bootstrap_samples": 200,
                                "seed": 17,
                                "identity_file": (
                                    "datasets/mixed_beir/mixed_corpus_identity.json"
                                ),
                            },
                            "promotion_gates": {
                                "candidate_arms": ["target"],
                                "quality_metric": "mrr",
                                "quality_ci_lower_min": 0.0,
                                "per_source_metrics": [
                                    "mrr",
                                    "recall_at_k",
                                    "ndcg_at_k",
                                ],
                                "max_source_regression": 0.005,
                                "latency_relative_max": {
                                    "search_latency_ms_p50": 1.05,
                                    "search_latency_ms_p95": 1.10,
                                },
                            },
                        },
                    }
                ),
                encoding="utf-8",
            )
            plan = ExperimentPlan.load(plan_path)
            self.assertEqual(
                plan.summarize.paired_query_analysis.metrics,
                ["mrr", "recall_at_k", "ndcg_at_k"],
            )
            self.assertEqual(plan.summarize.promotion_gates.quality_metric, "mrr")

            run_dir = root / "run"
            identity_dir = run_dir / "datasets" / "mixed_beir"
            identity_dir.mkdir(parents=True)
            (identity_dir / "mixed_corpus_identity.json").write_text(
                json.dumps(
                    {
                        "query_order": ["nfcorpus__q0", "scifact__q1"],
                        "query_sources": {
                            "nfcorpus__q0": "nfcorpus",
                            "scifact__q1": "scifact",
                        },
                    }
                ),
                encoding="utf-8",
            )

            def write_arm(
                name: str,
                query_values: list[float],
                *,
                p50: float,
                p95: float,
            ) -> None:
                arm_dir = run_dir / "arms" / name
                arm_dir.mkdir(parents=True, exist_ok=True)
                (arm_dir / "metrics.json").write_text(
                    json.dumps(
                        {
                            "status": "ok",
                            "metrics": {
                                "search_latency_ms_p50": p50,
                                "search_latency_ms_p95": p95,
                            },
                        }
                    ),
                    encoding="utf-8",
                )
                (arm_dir / "validation.json").write_text(
                    json.dumps({"ok": True, "issues": []}), encoding="utf-8"
                )
                events = []
                for query_index, value in enumerate(query_values):
                    events.append(
                        {
                            "query_index": query_index,
                            "search_type": "hybrid",
                            "query_metrics": {
                                "mrr": value,
                                "recall_at_k": value,
                                "ndcg_at_k": value,
                            },
                            "search_stats": {"latency_ms": str(p50)},
                        }
                    )
                (arm_dir / "debug.jsonl").write_text(
                    "".join(json.dumps(event) + "\n" for event in events),
                    encoding="utf-8",
                )

            write_arm("baseline", [0.2, 0.3], p50=10.0, p95=20.0)
            write_arm("target", [0.3, 0.4], p50=10.4, p95=21.5)

            summary = xplan_summarize.write_summary(plan, run_dir, stamp="passing")
            rows = {row["arm"]: row for row in summary["arms"]}
            target = rows["target"]
            self.assertAlmostEqual(target["metrics"]["paired_query_mrr_delta"], 0.1)
            self.assertAlmostEqual(target["metrics"]["paired_query_mrr_ci_low"], 0.1)
            self.assertAlmostEqual(target["metrics"]["paired_query_mrr_ci_high"], 0.1)
            self.assertEqual(target["metrics"]["paired_query_mrr_query_count"], 2.0)
            self.assertAlmostEqual(
                target["metrics"]["paired_query_mrr_delta_nfcorpus"], 0.1
            )
            self.assertTrue(target["promotion"]["eligible"])
            self.assertTrue((run_dir / "paired_query_analysis.json").is_file())
            self.assertTrue((run_dir / "promotion_gates.json").is_file())

            write_arm("target", [0.3, 0.4], p50=10.4, p95=22.1)
            failed = xplan_summarize.write_summary(plan, run_dir, stamp="failing")
            failed_rows = {row["arm"]: row for row in failed["arms"]}
            self.assertFalse(failed_rows["target"]["promotion"]["eligible"])
            self.assertIn(
                "search_latency_ms_p95",
                json.dumps(failed_rows["target"]["promotion"]["reasons"]),
            )
        self.assertTrue(
            xplan_runner._summary_has_hard_failure(
                {"arm_count": 2, "valid_count": 1, "failed_count": 1}
            )
        )

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
            plan["fixed"]["params"]["datasets"],
            ["scifact", "nfcorpus", "fiqa"],
        )
        self.assertEqual(
            plan["fixed"]["params"]["shared_warm_cache"],
            "generalized_memory_soar_boundary",
        )
        self.assertTrue(
            plan["fixed"]["params"]["require_topology_construction_stability"]
        )
        self.assertEqual(
            plan["fixed"]["env"]["YAMS_VECTOR_SEARCH_ENGINE"], "simeon_pq_adc"
        )
        self.assertNotIn("YAMS_VECTOR_VEC0_PHSS_ENABLED", plan["fixed"]["env"])
        self.assertNotIn(
            "require_topology_construction_identity", plan["fixed"]["params"]
        )
        arms = {arm["name"]: arm for arm in plan["arms"]}
        self.assertEqual(
            set(arms),
            {
                "global_ann_c32",
                "shadow_margin020_min1",
                "shadow_exact_margin020_min1",
                "narrow_soar_lambda1_ratio105",
            },
        )
        self.assertEqual(
            arms["shadow_margin020_min1"]["params"][
                "topology_route_ann_candidate_limit"
            ],
            64,
        )
        self.assertEqual(
            arms["shadow_exact_margin020_min1"]["params"][
                "topology_route_ann_candidate_limit"
            ],
            0,
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
        required_metrics = set(plan["validate"]["require_metrics"])
        self.assertIn("vector_rows_visited_observation_rate", required_metrics)
        self.assertIn(
            "vector_exact_distance_evaluations_observation_rate", required_metrics
        )
        self.assertIn(
            "vector_ann_candidate_budget_observation_rate", required_metrics
        )
        self.assertIn("vector_total_rows_visited_actual_avg", required_metrics)
        self.assertIn(
            "vector_total_exact_distance_evaluations_actual_avg", required_metrics
        )
        self.assertIn("vector_ann_candidate_budget_actual_avg", required_metrics)

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
                    "topology_construction_fingerprint": "fixed-topology",
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
        self.assertEqual(
            summary["all"]["construction_fingerprint"], "fixed-topology"
        )
        self.assertEqual(summary["all"]["minimum_observed_route_margin"], 0.42)
        self.assertEqual(summary["all"]["minimum_observed_seed_hits"], 2)
        self.assertEqual(summary["nfcorpus"], summary["all"])

    def test_declared_params_override_ambient_and_arm_env_wins(self) -> None:
        merged = _merge_benchmark_env(
            ambient={
                "YAMS_BENCH_DATASET": "scifact",
                "YAMS_BENCH_CORPUS_SIZE": "2000",
                "YAMS_VECTOR_SEARCH_ENGINE": "ambient-engine",
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
        self.assertNotIn("YAMS_VECTOR_SEARCH_ENGINE", merged)
        self.assertEqual(merged["UNRELATED"], "kept")

    def test_boolean_params_use_numeric_environment_encoding(self) -> None:
        merged = _merge_benchmark_env(
            ambient={},
            declared_env={},
            params={"enabled": True, "disabled": False},
            param_env={
                "enabled": "YAMS_TEST_ENABLED",
                "disabled": "YAMS_TEST_DISABLED",
            },
        )

        self.assertEqual(merged["YAMS_TEST_ENABLED"], "1")
        self.assertEqual(merged["YAMS_TEST_DISABLED"], "0")

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
                "relevant_decision_trace": {
                    "relevant_docs": [
                        {"doc_id": "a", "component_top_hits": ["vector"]},
                        {"doc_id": "b", "component_top_hits": ["vector"]},
                    ]
                },
                "search_stats": {
                    "topology_weak_query_narrow_applied": "1",
                    "topology_route_narrow_proposed": "1",
                    "topology_route_admission_eligible": "1",
                    "topology_route_coordinate_space_alignment_status": "satisfied",
                    "topology_route_protected_fibers_represented_status": "satisfied",
                    "topology_route_certificate_saturates_protected_fibers_status": "satisfied",
                    "topology_route_protected_relation_coverage_status": "satisfied",
                    "topology_route_work_status": "satisfied",
                    "topology_route_cover_materialization_status": "satisfied",
                    "topology_shadow_evaluated": "1",
                    "topology_shadow_proposed_action": "narrow",
                    "topology_shadow_removed_candidate_doc_ids": "b\tirrelevant",
                    "topology_route_available_count": "3",
                    "topology_route_boundary_score_margin": "0.25",
                    "topology_route_ann_used": "1",
                    "topology_route_ann_candidates": "6",
                    "topology_route_ann_distance_evaluations": "4",
                    "topology_route_exact_representative_distance_evaluations": "3",
                    "topology_route_confidence_abstained": "0",
                    "topology_snapshot_cache_hit": "0",
                    "topology_weak_query_allowed_candidates": "8",
                    "vector_search_candidate_budget": "8",
                    "vector_search_result_budget": "16",
                    "vector_search_distance_evaluation_budget": "40",
                    "vector_search_rows_visited_actual": "16",
                    "vector_search_exact_distance_evaluations_actual": "8",
                    "vector_search_ann_candidate_budget_actual": "16",
                    "vector_search_candidate_index_cache_used": "1",
                    "vector_search_candidate_index_payload_bytes": "4096",
                    "vector_search_candidate_lookup_ns": "2000000",
                    "vector_search_candidate_projection_ns": "400000",
                    "vector_search_pq_lut_ns": "600000",
                    "vector_search_adc_scoring_ns": "800000",
                    "vector_search_topk_selection_ns": "1000000",
                    "vector_search_result_materialization_ns": "1200000",
                    "vector_search_exact_rerank_ns": "1400000",
                    "topology_vector_filter_applied": "1",
                    "topology_vector_filter_fallback": "0",
                    "topology_vector_filter_matched": "6",
                    "topology_vector_filter_removed": "10",
                    "topology_vector_allowed_set_ann_applied": "1",
                    "topology_vector_allowed_set_ann_fallback": "0",
                    "topology_vector_global_fill_count": "2",
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
                    "topology_route_narrow_proposed": "1",
                    "topology_route_admission_eligible": "0",
                    "topology_route_coordinate_space_alignment_status": "violated",
                    "topology_route_protected_fibers_represented_status": "violated",
                    "topology_route_certificate_saturates_protected_fibers_status": "violated",
                    "topology_route_available_count": "1",
                    "topology_route_boundary_score_margin": "0.05",
                    "topology_route_ann_used": "0",
                    "topology_route_ann_candidates": "0",
                    "topology_route_ann_distance_evaluations": "0",
                    "topology_route_exact_representative_distance_evaluations": "8",
                    "topology_route_confidence_abstained": "1",
                    "topology_snapshot_cache_hit": "1",
                    "topology_weak_query_allowed_candidates": "4",
                    "vector_search_candidate_budget": "4",
                    "vector_search_result_budget": "16",
                    "vector_search_distance_evaluation_budget": "24",
                    "vector_search_rows_visited_actual": "8",
                    "vector_search_exact_distance_evaluations_actual": "4",
                    "vector_search_ann_candidate_budget_actual": "8",
                    "vector_search_candidate_index_cache_used": "0",
                    "vector_search_candidate_index_payload_bytes": "0",
                    "vector_search_candidate_lookup_ns": "0",
                    "vector_search_candidate_projection_ns": "0",
                    "vector_search_pq_lut_ns": "0",
                    "vector_search_adc_scoring_ns": "0",
                    "vector_search_topk_selection_ns": "0",
                    "vector_search_result_materialization_ns": "0",
                    "vector_search_exact_rerank_ns": "0",
                    "topology_vector_filter_applied": "0",
                    "topology_vector_filter_fallback": "1",
                    "topology_vector_filter_matched": "0",
                    "topology_vector_filter_removed": "0",
                    "topology_vector_allowed_set_ann_applied": "0",
                    "topology_vector_allowed_set_ann_fallback": "1",
                    "topology_vector_global_fill_count": "0",
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
        self.assertEqual(metrics["topology_narrow_proposal_rate"], 1.0)
        self.assertEqual(metrics["topology_route_admission_rate"], 0.5)
        self.assertEqual(metrics["topology_coordinate_space_alignment_rate"], 0.5)
        self.assertEqual(
            metrics["topology_protected_fiber_representation_observation_rate"], 1.0
        )
        self.assertEqual(
            metrics["topology_protected_fiber_representation_satisfaction_rate"], 0.5
        )
        self.assertEqual(
            metrics["topology_certificate_fiber_saturation_observation_rate"], 1.0
        )
        self.assertEqual(
            metrics["topology_certificate_fiber_saturation_satisfaction_rate"], 0.5
        )
        self.assertEqual(metrics["topology_shadow_fiber_calibration_queries"], 1.0)
        self.assertEqual(metrics["topology_shadow_protected_fiber_candidates"], 2.0)
        self.assertEqual(
            metrics["topology_shadow_represented_protected_candidates"], 1.0
        )
        self.assertEqual(metrics["topology_shadow_unresolved_protected_fibers"], 1.0)
        self.assertEqual(
            metrics["topology_shadow_protected_fiber_representation_rate"], 0.5
        )
        self.assertEqual(metrics["topology_shadow_protected_fiber_unresolved_rate"], 0.5)
        self.assertEqual(metrics["topology_confidence_abstain_rate"], 0.5)
        self.assertEqual(metrics["topology_route_available_avg"], 2.0)
        self.assertAlmostEqual(
            metrics["topology_route_boundary_score_margin_avg"], 0.15
        )
        self.assertEqual(metrics["topology_route_ann_rate"], 0.5)
        self.assertEqual(metrics["topology_route_ann_candidates_avg"], 3.0)
        self.assertEqual(metrics["topology_route_ann_distance_evaluations_avg"], 2.0)
        self.assertEqual(
            metrics["topology_route_exact_representative_distance_evaluations_avg"], 5.5
        )
        self.assertEqual(metrics["topology_snapshot_cache_hit_rate"], 0.5)
        self.assertEqual(metrics["topology_allowed_candidates_avg"], 6.0)
        self.assertEqual(metrics["cross_rerank_apply_rate"], 0.0)
        self.assertEqual(metrics["topology_vector_filter_allowed_candidates_avg"], 8.0)
        self.assertEqual(metrics["topology_vector_filter_latency_ms_avg"], 30.0)
        self.assertEqual(metrics["topology_vector_abstain_latency_ms_avg"], 10.0)
        self.assertEqual(metrics["vector_candidate_budget_avg"], 6.0)
        self.assertEqual(metrics["vector_result_budget_avg"], 16.0)
        self.assertEqual(metrics["vector_distance_evaluation_budget_avg"], 32.0)
        self.assertEqual(metrics["vector_rows_visited_actual_avg"], 12.0)
        self.assertEqual(metrics["vector_exact_distance_evaluations_actual_avg"], 6.0)
        self.assertEqual(metrics["vector_ann_candidate_budget_actual_avg"], 12.0)
        self.assertEqual(metrics["vector_candidate_index_cache_rate"], 0.5)
        self.assertEqual(metrics["vector_candidate_index_payload_bytes_max"], 4096.0)
        self.assertEqual(metrics["vector_candidate_lookup_ms_avg"], 1.0)
        self.assertEqual(metrics["vector_candidate_projection_ms_avg"], 0.2)
        self.assertEqual(metrics["vector_pq_lut_ms_avg"], 0.3)
        self.assertEqual(metrics["vector_adc_scoring_ms_avg"], 0.4)
        self.assertEqual(metrics["vector_topk_selection_ms_avg"], 0.5)
        self.assertEqual(metrics["vector_result_materialization_ms_avg"], 0.6)
        self.assertEqual(metrics["vector_exact_rerank_ms_avg"], 0.7)
        self.assertEqual(metrics["vector_candidate_work_budget_avg"], 10.0)
        self.assertEqual(metrics["vector_total_rows_visited_actual_avg"], 12.0)
        self.assertEqual(metrics["vector_total_exact_distance_evaluations_actual_avg"], 6.0)
        self.assertEqual(metrics["topology_vector_filter_rate"], 0.5)
        self.assertEqual(metrics["topology_vector_filter_fallback_rate"], 0.5)
        self.assertEqual(metrics["topology_vector_filter_matched_avg"], 3.0)
        self.assertEqual(metrics["topology_vector_filter_removed_avg"], 5.0)
        self.assertEqual(metrics["topology_vector_allowed_set_ann_rate"], 0.5)
        self.assertEqual(metrics["topology_vector_allowed_set_ann_fallback_rate"], 0.5)
        self.assertEqual(metrics["topology_vector_global_fill_count_sum"], 2.0)
        self.assertEqual(metrics["topology_vector_global_fill_count_avg"], 1.0)
        self.assertEqual(metrics["topology_structure_candidate_count_avg"], 4.0)
        self.assertAlmostEqual(metrics["topology_structure_scale_agreement_avg"], 0.6)
        self.assertAlmostEqual(metrics["topology_structure_overlap_support_avg"], 0.4)
        self.assertAlmostEqual(metrics["topology_structure_persistence_support_avg"], 0.8)
        self.assertAlmostEqual(metrics["topology_structure_cohesion_support_avg"], 0.6)
        self.assertAlmostEqual(metrics["topology_structure_bridge_support_avg"], 0.3)
        self.assertAlmostEqual(metrics["topology_structure_density_support_avg"], 0.7)
        self.assertEqual(metrics["topology_construction_fingerprint_count"], 1.0)

    def test_candidate_rescue_metrics_measure_relevance_and_survival(self) -> None:
        events = [
            {
                "search_type": "hybrid",
                "relevant_doc_ids": ["relevant", "other"],
                "returned_doc_ids": ["relevant"],
                "relevant_decision_trace": {
                    "relevant_docs": [
                        {
                            "doc_id": "relevant",
                            "in_pre_fusion": True,
                            "in_post_fusion": True,
                        },
                        {
                            "doc_id": "other",
                            "in_pre_fusion": False,
                            "in_post_fusion": False,
                        },
                    ]
                },
                "search_stats": {
                    "topology_candidate_rescue_attempted": "1",
                    "topology_candidate_rescue_applied": "1",
                    "topology_candidate_rescue_added_candidates": "2",
                    "topology_candidate_rescue_novel_candidates": "1",
                    "topology_candidate_rescue_evidence_rescues": "1",
                    "topology_candidate_rescue_duplicate_candidates": "1",
                    "topology_candidate_rescue_added_candidate_hashes": "sha-relevant\tsha-irrelevant",
                    "topology_candidate_rescue_added_candidate_doc_ids": "relevant\tirrelevant",
                    "topology_candidate_rescue_novel_candidate_doc_ids": "irrelevant",
                    "topology_candidate_rescue_evidence_rescue_doc_ids": "relevant",
                    "topology_candidate_rescue_scored_candidate_doc_ids": "irrelevant",
                    "topology_candidate_rescue_exact_scored_candidate_doc_ids": "relevant\tother",
                    "topology_graph_relation_candidate_doc_ids": "relevant\tother",
                    "topology_graph_fetched_candidate_doc_ids": "relevant\tother",
                    "topology_graph_eligible_candidate_doc_ids": "relevant\tother",
                    "topology_weak_query_allowed_candidate_doc_ids": "relevant\tother",
                },
            },
            {
                "search_type": "hybrid",
                "relevant_doc_ids": ["shadow-relevant"],
                "returned_doc_ids": [],
                "relevant_decision_trace": {
                    "relevant_docs": [
                        {
                            "doc_id": "shadow-relevant",
                            "in_pre_fusion": False,
                            "in_post_fusion": False,
                        }
                    ]
                },
                "search_stats": {
                    "topology_candidate_rescue_attempted": "1",
                    "topology_candidate_rescue_applied": "0",
                    "topology_candidate_rescue_added_candidates": "1",
                    "topology_candidate_rescue_novel_candidates": "1",
                    "topology_candidate_rescue_evidence_rescues": "0",
                    "topology_candidate_rescue_duplicate_candidates": "0",
                    "topology_candidate_rescue_added_candidate_hashes": "sha-shadow-relevant",
                    "topology_candidate_rescue_added_candidate_doc_ids": "shadow-relevant",
                    "topology_candidate_rescue_novel_candidate_doc_ids": "shadow-relevant",
                    "topology_candidate_rescue_evidence_rescue_doc_ids": "",
                    "topology_candidate_rescue_scored_candidate_doc_ids": "shadow-relevant",
                    "topology_candidate_rescue_exact_scored_candidate_doc_ids": "shadow-relevant",
                    "topology_graph_relation_candidate_doc_ids": "shadow-relevant",
                    "topology_graph_fetched_candidate_doc_ids": "shadow-relevant",
                    "topology_graph_eligible_candidate_doc_ids": "shadow-relevant",
                    "topology_weak_query_allowed_candidate_doc_ids": "shadow-relevant",
                },
            },
        ]
        with tempfile.TemporaryDirectory() as tmp:
            debug_path = Path(tmp) / "debug.jsonl"
            debug_path.write_text(
                "".join(json.dumps(event) + "\n" for event in events), encoding="utf-8"
            )
            metrics = parse_debug_jsonl(debug_path)["metrics"]

        self.assertEqual(metrics["topology_candidate_rescue_attempt_rate"], 1.0)
        self.assertEqual(metrics["topology_candidate_rescue_apply_rate"], 0.5)
        self.assertEqual(metrics["topology_candidate_rescue_added_sum"], 3.0)
        self.assertEqual(metrics["topology_candidate_rescue_novel_sum"], 2.0)
        self.assertEqual(metrics["topology_candidate_rescue_evidence_rescue_sum"], 1.0)
        self.assertEqual(metrics["topology_candidate_rescue_duplicate_sum"], 1.0)
        self.assertEqual(metrics["topology_candidate_rescue_relevant_query_rate"], 1.0)
        self.assertEqual(metrics["topology_candidate_rescue_relevant_added"], 2.0)
        self.assertEqual(metrics["topology_candidate_rescue_relevant_novel"], 1.0)
        self.assertEqual(metrics["topology_candidate_rescue_relevant_evidence"], 1.0)
        self.assertEqual(
            metrics["topology_candidate_rescue_relevant_novel_query_rate"], 0.5
        )
        self.assertEqual(
            metrics["topology_candidate_rescue_relevant_evidence_query_rate"], 0.5
        )
        self.assertEqual(metrics["topology_candidate_rescue_relevant_reachable"], 3.0)
        self.assertEqual(
            metrics["topology_candidate_rescue_relevant_reachable_query_rate"], 1.0
        )
        self.assertAlmostEqual(
            metrics["topology_candidate_rescue_reachable_to_scored_rate"], 1.0 / 3.0
        )
        self.assertEqual(
            metrics["topology_candidate_rescue_baseline_miss_queries"], 2.0
        )
        self.assertEqual(
            metrics["topology_candidate_rescue_missing_relation_reachable_query_rate"],
            1.0,
        )
        self.assertEqual(
            metrics["topology_candidate_rescue_missing_fetched_query_rate"], 1.0
        )
        self.assertEqual(
            metrics["topology_candidate_rescue_missing_eligible_query_rate"], 1.0
        )
        self.assertEqual(
            metrics["topology_candidate_rescue_missing_selected_query_rate"], 1.0
        )
        self.assertEqual(
            metrics["topology_candidate_rescue_missing_scored_query_rate"], 0.5
        )
        self.assertEqual(
            metrics["topology_candidate_rescue_missing_exact_scored_query_rate"], 1.0
        )
        self.assertEqual(
            metrics["topology_candidate_rescue_post_fusion_survival_rate"], 0.5
        )
        self.assertEqual(
            metrics["topology_candidate_rescue_returned_survival_rate"], 0.5
        )
        self.assertEqual(
            metrics["topology_candidate_rescue_evidence_post_fusion_survival_rate"], 1.0
        )
        self.assertEqual(
            metrics["topology_candidate_rescue_evidence_returned_survival_rate"], 1.0
        )

    def test_candidate_rescue_stage_ledger_localizes_bounded_fetch_loss(self) -> None:
        event = {
            "search_type": "hybrid",
            "relevant_doc_ids": ["missing"],
            "returned_doc_ids": [],
            "relevant_decision_trace": {
                "relevant_docs": [
                    {
                        "doc_id": "missing",
                        "in_pre_fusion": False,
                        "in_post_fusion": False,
                    }
                ]
            },
            "search_stats": {
                "topology_graph_trace_collected": "1",
                "topology_graph_fetch_truncated_seed_count": "1",
                "topology_graph_relation_candidate_count": "3",
                "topology_graph_fetched_candidate_count": "2",
                "topology_graph_eligible_candidate_count": "1",
                "topology_graph_seed_unresolved_count": "0",
                "topology_graph_relation_unresolved_count": "0",
                "topology_graph_fetched_unresolved_count": "0",
                "topology_graph_eligible_unresolved_count": "0",
                "topology_graph_relation_candidate_doc_ids": "missing",
                "topology_graph_fetched_candidate_doc_ids": "",
                "topology_graph_eligible_candidate_doc_ids": "",
                "topology_weak_query_allowed_candidate_doc_ids": "",
                "topology_candidate_rescue_scored_candidate_doc_ids": "",
                "topology_candidate_rescue_exact_scored_candidate_doc_ids": "",
            },
        }
        with tempfile.TemporaryDirectory() as tmp:
            debug_path = Path(tmp) / "debug.jsonl"
            debug_path.write_text(json.dumps(event) + "\n", encoding="utf-8")
            metrics = parse_debug_jsonl(debug_path)["metrics"]

        self.assertEqual(
            metrics["topology_candidate_rescue_missing_relation_reachable_query_rate"],
            1.0,
        )
        self.assertEqual(
            metrics["topology_candidate_rescue_missing_fetched_query_rate"], 0.0
        )
        self.assertEqual(
            metrics["topology_candidate_rescue_missing_selected_query_rate"], 0.0
        )
        self.assertEqual(metrics["topology_graph_trace_query_rate"], 1.0)
        self.assertEqual(
            metrics["topology_graph_fetch_truncated_seed_count_sum"], 1.0
        )
        self.assertEqual(metrics["topology_graph_relation_candidate_count_avg"], 3.0)
        self.assertEqual(metrics["topology_graph_fetched_candidate_count_avg"], 2.0)
        self.assertEqual(metrics["topology_graph_eligible_candidate_count_avg"], 1.0)

    def test_query_class_loader_prefers_rescue_doc_ids_over_content_hashes(self) -> None:
        event = {
            "query": "query",
            "search_type": "hybrid",
            "relevant_doc_ids": ["relevant"],
            "returned_doc_ids": ["relevant"],
            "relevant_decision_trace": {
                "relevant_docs": [
                    {
                        "doc_id": "relevant",
                        "in_pre_fusion": True,
                        "in_post_fusion": True,
                        "in_returned_topk": True,
                    }
                ]
            },
            "search_stats": {
                "topology_candidate_rescue_added_candidate_hashes": "sha-relevant",
                "topology_candidate_rescue_added_candidate_doc_ids": "relevant",
                "topology_candidate_rescue_novel_candidate_doc_ids": "",
                "topology_candidate_rescue_evidence_rescue_doc_ids": "relevant",
                "topology_weak_query_allowed_candidate_doc_ids": "reachable\trelevant",
            },
        }
        with tempfile.TemporaryDirectory() as tmp:
            debug_path = Path(tmp) / "debug.jsonl"
            debug_path.write_text(json.dumps(event) + "\n", encoding="utf-8")
            records = load_by_type(debug_path).get("hybrid", [])

        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["candidate_rescue_added_doc_ids"], {"relevant"})
        self.assertEqual(records[0]["candidate_rescue_relevant_doc_ids"], {"relevant"})
        self.assertEqual(records[0]["candidate_rescue_novel_relevant_doc_ids"], set())
        self.assertEqual(
            records[0]["candidate_rescue_evidence_relevant_doc_ids"], {"relevant"}
        )
        self.assertEqual(
            records[0]["candidate_rescue_reachable_relevant_doc_ids"], {"relevant"}
        )

    def test_paired_candidate_rescue_checks_preservation_and_strict_improvement(
        self,
    ) -> None:
        baseline = [
            {
                "repeat": "rep00",
                "corpus": "scifact",
                "query": "query",
                "rr": 0.0,
                "pre_fusion_relevant": {"baseline-relevant"},
                "post_fusion_relevant": {"baseline-relevant"},
            }
        ]
        rescue = [
            {
                "repeat": "rep00",
                "corpus": "scifact",
                "query": "query",
                "rr": 1.0,
                "pre_fusion_relevant": {"baseline-relevant", "rescued-relevant"},
                "post_fusion_relevant": {"baseline-relevant", "rescued-relevant"},
                "returned_relevant": {"baseline-relevant", "rescued-relevant"},
                "candidate_rescue_relevant_doc_ids": {"rescued-relevant"},
                "candidate_rescue_novel_relevant_doc_ids": {"rescued-relevant"},
                "candidate_rescue_evidence_relevant_doc_ids": {
                    "baseline-relevant"
                },
                "candidate_rescue_reachable_relevant_doc_ids": {"rescued-relevant"},
                "candidate_rescue_attempted": True,
                "candidate_rescue_applied": True,
            }
        ]

        summary = paired_candidate_rescue(baseline, rescue)["all"]

        self.assertEqual(summary["paired_queries"], 1)
        self.assertEqual(summary["baseline_relevant_preservation_rate"], 1.0)
        self.assertEqual(
            summary["budgeted_baseline_relevant_preservation_rate"], 1.0
        )
        self.assertEqual(summary["missing_relevant_rescues"], 1)
        self.assertEqual(summary["reachable_missing_relevant_candidates"], 1)
        self.assertEqual(summary["reachable_to_scored_rescue_rate"], 1.0)
        self.assertEqual(summary["materialized_relevant_rescues"], 1)
        self.assertEqual(summary["strict_candidate_improvement_queries"], 1)
        self.assertEqual(summary["strict_budgeted_improvement_queries"], 1)
        self.assertEqual(summary["post_fusion_rescue_survival_rate"], 1.0)
        self.assertEqual(summary["returned_rescue_survival_rate"], 1.0)
        self.assertEqual(summary["existing_relevant_evidence_rescues"], 1)
        self.assertEqual(summary["post_fusion_relevant_evidence_rescues"], 1)
        self.assertEqual(summary["returned_relevant_evidence_rescues"], 1)
        self.assertEqual(summary["post_fusion_evidence_survival_rate"], 1.0)
        self.assertEqual(summary["returned_evidence_survival_rate"], 1.0)
        self.assertEqual(summary["mean_rr_delta"], 1.0)

    def test_unavailable_vector_work_is_not_reported_as_zero_observation(self) -> None:
        event = {
            "search_type": "hybrid",
            "search_stats": {
                "vector_search_rows_visited_status": "unavailable",
                "vector_search_exact_distance_evaluations_status": "unavailable",
                "vector_search_ann_candidate_budget_status": "unavailable",
                "vector_search_candidate_budget": "32",
            },
        }
        with tempfile.TemporaryDirectory() as tmp:
            debug_path = Path(tmp) / "debug.jsonl"
            debug_path.write_text(json.dumps(event) + "\n", encoding="utf-8")
            metrics = parse_debug_jsonl(debug_path)["metrics"]

        self.assertEqual(metrics["vector_candidate_budget_avg"], 32.0)
        self.assertEqual(metrics["vector_rows_visited_observation_rate"], 0.0)
        self.assertEqual(metrics["vector_exact_distance_evaluations_observation_rate"], 0.0)
        self.assertEqual(metrics["vector_ann_candidate_budget_observation_rate"], 0.0)
        self.assertNotIn("vector_rows_visited_actual_avg", metrics)
        self.assertNotIn("vector_exact_distance_evaluations_actual_avg", metrics)
        self.assertNotIn("vector_ann_candidate_budget_actual_avg", metrics)
        self.assertNotIn("vector_rows_visited_actual_sum", metrics)
        self.assertNotIn("vector_exact_distance_evaluations_actual_sum", metrics)
        self.assertNotIn("vector_ann_candidate_budget_actual_sum", metrics)

    def test_vector_work_contract_rejects_missing_required_actual_counters(self) -> None:
        error = retrieval_quality_worker.require_vector_work_observation_contract(
            {
                "vector_rows_visited_observation_rate": 0.0,
                "vector_exact_distance_evaluations_observation_rate": 1.0,
                "vector_exact_distance_evaluations_actual_avg": 4.0,
            },
            {
                "rows_visited": "required",
                "exact_distance_evaluations": "required",
            },
        )

        self.assertIsNotNone(error)
        self.assertIn("rows_visited", error or "")

    def test_vector_work_contract_accepts_explicit_unavailable_counters(self) -> None:
        error = retrieval_quality_worker.require_vector_work_observation_contract(
            {
                "vector_rows_visited_observation_rate": 0.0,
                "vector_exact_distance_evaluations_observation_rate": 0.0,
                "vector_ann_candidate_budget_observation_rate": 0.0,
            },
            {
                "rows_visited": "unavailable",
                "exact_distance_evaluations": "unavailable",
                "ann_candidate_budget": "unavailable",
            },
        )

        self.assertIsNone(error)

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
                    "topology_route_work_observed": "1",
                    "topology_route_work_rows_visited_actual": "3",
                    "topology_route_work_exact_distance_evaluations_actual": "2",
                    "topology_route_work_ann_candidate_budget_actual": "4",
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
                    "topology_route_work_observed": "0",
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
        self.assertEqual(metrics["topology_route_work_observation_rate"], 0.5)
        self.assertEqual(metrics["topology_route_work_rows_visited_actual_avg"], 3.0)
        self.assertEqual(
            metrics["topology_route_work_exact_distance_evaluations_actual_avg"], 2.0
        )
        self.assertEqual(
            metrics["topology_route_work_ann_candidate_budget_actual_avg"], 4.0
        )
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


class IngestionContractTests(unittest.TestCase):
    @staticmethod
    def _complete_result() -> dict[str, object]:
        return {
            "test_config": {
                "corpus_size": 10,
                "doc_size": 1000,
                "corpus_seed": 42,
                "corpus_fingerprint": "fnv1a64:abc123",
                "ingest_mode": "directory",
                "ingest_concurrency": 1,
                "post_ingest_coalesce_ms": 2,
                "embedding_model": "simeon-default",
                "embedding_backend": "simeon",
                "simeon_recipe": "stable",
                "kg_enabled": True,
            },
            "pipeline_status": {
                "complete": True,
                "expected_post": 10,
                "observed_post": 10,
                "expected_embed": 10,
                "observed_embed": 10,
                "expected_kg": 10,
                "observed_kg": 10,
                "enrichment_drained": True,
                "write_coordinator_flushed": True,
            },
            "stages": {
                "metadata_storage": {
                    "duration_ms": 12,
                    "count": 10,
                    "failures": 0,
                }
            },
            "phase_timings": {
                "admission_ms": 12,
                "storage_ready_ms": 25,
                "pipeline_drain_ms": 30,
                "enrichment_ready_ms": 42,
                "searchability_ready_ms": 45,
            },
            "queues": {
                "dropped_batches": 0,
                "max_post_ingest": 4,
            },
            "add_dispatch_metrics": {
                "samples": 10,
                "total_us": 6000,
                "max_us": 900,
                "avg_us": 600,
                "fingerprint_total_us": 2000,
                "fingerprint_max_us": 320,
                "fingerprint_avg_us": 200,
                "enqueue_total_us": 4000,
                "enqueue_max_us": 700,
                "enqueue_avg_us": 400,
            },
            "enrichment_status": {
                "kg": {"queued": 10, "consumed": 10, "dropped": 0},
                "symbol": {"queued": 2, "consumed": 2, "dropped": 0},
                "entity": {"queued": 0, "consumed": 0, "dropped": 0},
                "title": {"queued": 10, "consumed": 10, "dropped": 0},
            },
            "write_coordinator": {
                "batches_enqueued": 12,
                "batches_committed": 12,
                "ops_applied": 30,
                "commit_errors": 0,
                "max_queue_depth": 3,
                "capacity_rejections": 0,
                "forced_enqueues_over_capacity": 0,
                "max_batch_apply_ms": 4,
                "max_batch_queue_wait_ms": 2,
                "max_batch_excess_queue_wait_ms": 0,
                "hot_sources": [
                    {
                        "source": "doc_svc/versioning",
                        "batches": 4,
                        "ops": 8,
                        "errors": 0,
                        "max_queue_wait_ms": 6,
                    }
                ],
            },
            "metadata_insert_writer_metrics": {
                "submitted_items": 10,
                "completed_items": 10,
                "rejected_items": 0,
                "batches": 4,
                "batch_items": 10,
                "max_batch_size": 4,
                "avg_batch_size": 2.5,
                "queue_wait_samples": 10,
                "queue_wait_total_us": 500,
                "queue_wait_max_us": 90,
                "queue_wait_avg_us": 50,
                "batch_apply_samples": 4,
                "batch_apply_total_us": 1200,
                "batch_apply_max_us": 400,
                "batch_apply_avg_us": 300,
                "failed_batches": 0,
                "fallback_items": 0,
            },
            "metadata_insert_phase_timings": {
                "transaction_total": {
                    "calls": 4,
                    "total_us": 800,
                    "max_us": 250,
                    "avg_us": 200,
                }
            },
            "search_impact": [
                {
                    "name": "keyword",
                    "required_by_contract": True,
                    "skipped": False,
                    "ok": True,
                    "total_count": 10,
                    "returned_count": 10,
                    "wall_ms": 3,
                }
            ],
        }

    def test_ingestion_contract_requires_complete_lossless_searchable_pipeline(self) -> None:
        raw = self._complete_result()
        self.assertEqual(validate_ingestion_contract(raw, require_full_searchability=True), [])

        raw["queues"]["dropped_batches"] = 1  # type: ignore[index]
        raw["pipeline_status"]["complete"] = False  # type: ignore[index]
        raw["search_impact"][0]["total_count"] = 9  # type: ignore[index]

        errors = validate_ingestion_contract(raw, require_full_searchability=True)
        self.assertTrue(any("pipeline incomplete" in error for error in errors))
        self.assertTrue(any("dropped" in error for error in errors))
        self.assertTrue(any("searchable" in error for error in errors))

    def test_disabled_kg_stage_is_not_misclassified_as_data_loss(self) -> None:
        raw = self._complete_result()
        raw["test_config"]["kg_enabled"] = False  # type: ignore[index]
        raw["pipeline_status"]["expected_kg"] = 0  # type: ignore[index]
        raw["pipeline_status"]["observed_kg"] = 0  # type: ignore[index]
        raw["enrichment_status"]["kg"] = {  # type: ignore[index]
            "queued": 0,
            "consumed": 0,
            "dropped": 10,
        }

        self.assertEqual(validate_ingestion_contract(raw), [])

    def test_ingestion_contract_rejects_duplicate_pipeline_work(self) -> None:
        raw = self._complete_result()
        raw["pipeline_status"]["observed_post"] = 11  # type: ignore[index]
        raw["pipeline_status"]["observed_embed"] = 11  # type: ignore[index]

        errors = validate_ingestion_contract(raw)

        self.assertTrue(any("post duplicate work" in error for error in errors))
        self.assertTrue(any("embed duplicate work" in error for error in errors))

    def test_ingestion_metrics_export_phase_queue_and_write_coordinator_details(self) -> None:
        metrics = extract_ingestion_metrics(self._complete_result())

        self.assertEqual(metrics["admission_ms"], 12.0)
        self.assertEqual(metrics["storage_ready_ms"], 25.0)
        self.assertEqual(metrics["pipeline_drain_ms"], 30.0)
        self.assertEqual(metrics["searchability_ready_ms"], 45.0)
        self.assertEqual(metrics["queue_max_post_ingest"], 4.0)
        self.assertEqual(metrics["add_dispatch_avg_us"], 600.0)
        self.assertEqual(metrics["add_dispatch_fingerprint_avg_us"], 200.0)
        self.assertEqual(metrics["add_dispatch_enqueue_avg_us"], 400.0)
        self.assertEqual(metrics["write_coordinator_batches_committed"], 12.0)
        self.assertEqual(metrics["write_coordinator_max_batch_queue_wait_ms"], 2.0)
        self.assertEqual(metrics["metadata_insert_writer_avg_batch_size"], 2.5)
        self.assertEqual(metrics["metadata_insert_writer_queue_wait_avg_us"], 50.0)
        self.assertEqual(metrics["metadata_insert_writer_batch_apply_avg_us"], 300.0)
        self.assertEqual(
            metrics["metadata_insert_writer_connection_wait_estimated_total_us"],
            400.0,
        )
        self.assertEqual(
            metrics["metadata_insert_writer_connection_wait_estimated_avg_us"],
            100.0,
        )
        self.assertEqual(
            metrics["write_coordinator_source_doc_svc_versioning_max_queue_wait_ms"],
            6.0,
        )

    def test_ingestion_identity_is_stable_and_rejects_cross_repeat_drift(self) -> None:
        raw = self._complete_result()
        identity = ingestion_experiment_identity(raw)
        self.assertEqual(identity["corpus_fingerprint"], "fnv1a64:abc123")
        self.assertEqual(identity["post_ingest_coalesce_ms"], 2)
        self.assertNotIn("timestamp", identity)

        with tempfile.TemporaryDirectory() as tmp:
            identity_path = Path(tmp) / "identity.json"
            self.assertIsNone(require_ingestion_experiment_identity(identity_path, identity))
            self.assertIsNone(require_ingestion_experiment_identity(identity_path, identity))

            drifted = dict(identity)
            drifted["ingest_mode"] = "pipelined_single_file"
            error = require_ingestion_experiment_identity(identity_path, drifted)
            self.assertIsNotNone(error)
            self.assertIn("identity mismatch", error or "")

    def test_mixed_quality_seed_exports_one_honest_ingestion_observation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            debug_path = Path(tmp) / "prime_debug.jsonl"
            debug_path.write_text(
                json.dumps(
                    {
                        "event": "benchmark_setup_metrics",
                        "ingestion": {
                            "cold_performed": True,
                            "admission_ms": 11,
                            "storage_ready_ms": 25,
                            "pipeline_drain_ms": 14,
                            "searchability_ready_ms": 31,
                            "enrichment_ready_ms": 37,
                            "measurement_n": 1,
                        },
                        "vector_index": {
                            "prepared": True,
                            "persisted": True,
                            "reusable_before": False,
                            "reusable_after": False,
                            "query_ready_after": True,
                            "prepare_ms": 17,
                            "persist_ms": 3,
                            "reusable_probe_ms": 9,
                            "reusable_probe_count": 2,
                        },
                        "daemon_graph": {
                            "required": True,
                            "ready": True,
                            "source": "post_ingest_queue_embedding_service",
                            "canonicalized": True,
                            "canonical_edges_generated": 32000,
                            "canonical_topology_snapshot": "topology-test",
                            "canonical_topology_fingerprint": "fixed-topology",
                            "semantic_documents_processed": 2000,
                            "semantic_edges_created": 3792,
                            "semantic_update_errors": 0,
                            "topology_document_nodes": 2000,
                            "topology_semantic_edges": 3792,
                            "topology_documents_with_neighbors": 2000,
                            "topology_isolated_documents": 0,
                            "post_ingest_queued": 0,
                            "post_ingest_in_flight": 0,
                        },
                        "setup_total_ms": 41,
                    }
                )
                + "\n",
                encoding="utf-8",
            )

            metrics = parse_benchmark_setup_metrics(debug_path)
            self.assertEqual(
                parse_daemon_graph_topology_fingerprint(debug_path), "fixed-topology"
            )
            self.assertEqual(metrics["ingest_admission_ms"], 11.0)
            self.assertEqual(metrics["ingest_pipeline_drain_ms"], 14.0)
            self.assertEqual(metrics["ingest_enrichment_ready_ms"], 37.0)
            self.assertEqual(metrics["ingest_measurement_n"], 1.0)
            self.assertEqual(metrics["vector_index_prepare_ms"], 17.0)
            self.assertEqual(metrics["vector_index_persist_ms"], 3.0)
            self.assertEqual(metrics["vector_index_reusable_probe_ms"], 9.0)
            self.assertEqual(metrics["vector_index_reusable_probe_count"], 2.0)
            self.assertEqual(metrics["vector_index_prepared"], 1.0)
            self.assertEqual(metrics["vector_index_persisted"], 1.0)
            self.assertEqual(metrics["vector_index_reusable_before"], 0.0)
            self.assertEqual(metrics["vector_index_reusable_after"], 0.0)
            self.assertEqual(metrics["vector_query_ready_after"], 1.0)
            self.assertEqual(metrics["daemon_graph_required"], 1.0)
            self.assertEqual(metrics["daemon_graph_ready"], 1.0)
            self.assertEqual(metrics["daemon_graph_canonicalized"], 1.0)
            self.assertEqual(
                metrics["daemon_graph_canonical_edges_generated"], 32000.0
            )
            self.assertEqual(
                metrics["daemon_graph_semantic_documents_processed"], 2000.0
            )
            self.assertEqual(metrics["daemon_graph_semantic_edges_created"], 3792.0)
            self.assertEqual(metrics["daemon_graph_semantic_update_errors"], 0.0)
            self.assertEqual(metrics["daemon_graph_topology_document_nodes"], 2000.0)
            self.assertEqual(metrics["daemon_graph_topology_semantic_edges"], 3792.0)
            self.assertEqual(
                metrics["daemon_graph_topology_documents_with_neighbors"], 2000.0
            )
            self.assertEqual(metrics["daemon_graph_topology_isolated_documents"], 0.0)
            self.assertEqual(metrics["daemon_graph_post_ingest_queued"], 0.0)
            self.assertEqual(metrics["daemon_graph_post_ingest_in_flight"], 0.0)

    def test_vector_cache_metadata_exports_storage_and_index_footprint(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            metadata_path = Path(tmp) / "retrieval_bench_cache.json"
            metadata_path.write_text(
                json.dumps(
                    {
                        "vectors_db_bytes": 4096,
                        "vectors_wal_bytes": 512,
                        "persisted_hnsw_nodes": 11,
                        "persisted_vec0_rows": 12,
                        "persisted_pq_codes": 13,
                        "persisted_pq_meta": 1,
                    }
                ),
                encoding="utf-8",
            )

            metrics = parse_benchmark_cache_metrics(metadata_path)

            self.assertEqual(metrics["vector_database_bytes"], 4096.0)
            self.assertEqual(metrics["vector_wal_bytes"], 512.0)
            self.assertEqual(metrics["vector_persisted_hnsw_nodes"], 11.0)
            self.assertEqual(metrics["vector_persisted_vec0_rows"], 12.0)
            self.assertEqual(metrics["vector_persisted_pq_codes"], 13.0)
            self.assertEqual(metrics["vector_persisted_pq_meta_rows"], 1.0)

    def test_shared_seed_metrics_are_emitted_once_per_arm_not_once_per_repeat(
        self,
    ) -> None:
        self.assertTrue(
            retrieval_quality_worker.include_shared_seed_metrics(
                Path("/tmp/run/arms/control/rep00")
            )
        )
        self.assertFalse(
            retrieval_quality_worker.include_shared_seed_metrics(
                Path("/tmp/run/arms/control/rep01")
            )
        )
        self.assertTrue(
            retrieval_quality_worker.include_shared_seed_metrics(
                Path("/tmp/run/arms/control")
            )
        )

        aggregated = xplan_runner.aggregate_reps(
            [
                {"seed_ingest_admission_ms": 11.0},
                {"mrr": 0.4},
                {"mrr": 0.5},
            ]
        )
        self.assertEqual(aggregated["seed_ingest_admission_ms_n"], 1.0)

    def test_single_observation_delta_is_not_marked_as_beyond_noise(self) -> None:
        cell, detail = xplan_summarize._delta_cell(
            {"setup_ms": 11.0, "setup_ms_n": 1.0, "setup_ms_stdev": 0.0},
            {"setup_ms": 10.0, "setup_ms_n": 1.0, "setup_ms_stdev": 0.0},
            "setup_ms",
        )

        self.assertEqual(cell, "+1")
        self.assertIsNotNone(detail)
        self.assertIsNone((detail or {}).get("pooled_stdev"))

    def test_query_warmup_is_a_distinct_pass_not_first_sample_elision(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            debug_path = Path(tmp) / "debug.jsonl"
            debug_path.write_text(
                "".join(
                    json.dumps(event) + "\n"
                    for event in [
                        {
                            "event": "query_warmup_summary",
                            "search_type": "benchmark_warmup",
                            "first_query_latency_ms": 31.0,
                            "query_count": 2,
                            "completed_queries": 2,
                            "retried_queries": 0,
                            "retry_failed_queries": 0,
                        },
                        {
                            "query_index": 0,
                            "search_type": "hybrid",
                            "search_stats": {"latency_ms": "10"},
                        },
                        {
                            "query_index": 1,
                            "search_type": "hybrid",
                            "search_stats": {"latency_ms": "20"},
                        },
                    ]
                ),
                encoding="utf-8",
            )

            metrics = parse_debug_jsonl(debug_path)["metrics"]

        self.assertEqual(metrics["search_latency_ms_cold_first"], 31.0)
        self.assertEqual(metrics["search_latency_ms_first_pass_first"], 31.0)
        self.assertEqual(metrics["search_latency_ms_warmed_p50"], 15.0)
        self.assertEqual(metrics["search_latency_ms_p95"], 20.0)
        self.assertEqual(metrics["search_warmup_retried_queries"], 0.0)
        self.assertEqual(metrics["search_warmup_retry_failed_queries"], 0.0)
        self.assertEqual(metrics["search_latency_ms_p99"], 20.0)
        self.assertAlmostEqual(metrics["search_throughput_qps"], 2000.0 / 30.0)
        self.assertEqual(metrics["search_warmup_query_count"], 2.0)
        self.assertEqual(metrics["search_warmup_completed_queries"], 2.0)

    def test_warmup_retry_suppresses_invalid_cold_first_latency(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            debug_path = Path(tmp) / "debug.jsonl"
            debug_path.write_text(
                "\n".join(
                    [
                        json.dumps(
                            {
                                "event": "query_warmup_summary",
                                "first_query_latency_ms": 11.0,
                                "query_count": 1,
                                "completed_queries": 1,
                                "retried_queries": 1,
                                "retry_failed_queries": 0,
                            }
                        ),
                        json.dumps(
                            {
                                "query_index": 0,
                                "search_type": "hybrid",
                                "search_stats": {"latency_ms": "10"},
                            }
                        ),
                    ]
                )
                + "\n",
                encoding="utf-8",
            )

            metrics = parse_debug_jsonl(debug_path)["metrics"]

        self.assertEqual(metrics["search_warmup_retried_queries"], 1.0)
        self.assertEqual(metrics["search_warmup_retry_failed_queries"], 0.0)
        self.assertEqual(metrics["search_latency_ms_warmed_p50"], 10.0)
        self.assertNotIn("search_latency_ms_cold_first", metrics)
        self.assertNotIn("search_latency_ms_first_pass_first", metrics)

    def test_legacy_warmup_summary_does_not_impute_retry_telemetry(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            debug_path = Path(tmp) / "debug.jsonl"
            debug_path.write_text(
                "\n".join(
                    [
                        json.dumps(
                            {
                                "event": "query_warmup_summary",
                                "first_query_latency_ms": 31.0,
                                "query_count": 1,
                                "completed_queries": 1,
                            }
                        ),
                        json.dumps(
                            {
                                "query_index": 0,
                                "search_type": "hybrid",
                                "search_stats": {"latency_ms": "10"},
                            }
                        ),
                    ]
                )
                + "\n",
                encoding="utf-8",
            )

            metrics = parse_debug_jsonl(debug_path)["metrics"]

        self.assertNotIn("search_warmup_retried_queries", metrics)
        self.assertNotIn("search_warmup_retry_failed_queries", metrics)

    def test_mixed_query_trace_contract_rejects_missing_or_failed_queries(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            identity_path = root / "identity.json"
            identity_path.write_text(
                json.dumps(
                    {
                        "query_order": ["alpha__q1", "beta__q2"],
                        "query_sources": {
                            "alpha__q1": "alpha",
                            "beta__q2": "beta",
                        },
                        "query_texts": {
                            "alpha__q1": "alpha question",
                            "beta__q2": "beta question",
                        },
                    }
                ),
                encoding="utf-8",
            )
            debug_path = root / "debug.jsonl"
            debug_path.write_text(
                json.dumps(
                    {
                        "query_index": 0,
                        "query": "alpha question",
                        "search_type": "hybrid",
                        "search_stats": {"latency_ms": "4"},
                    }
                )
                + "\n",
                encoding="utf-8",
            )

            error = retrieval_quality_worker.require_complete_query_trace(
                debug_path, identity_path
            )

        self.assertIsNotNone(error)
        self.assertIn("missing query indices [1]", error or "")

    def test_mixed_query_trace_contract_accepts_one_success_per_expected_query(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            identity_path = root / "identity.json"
            identity_path.write_text(
                json.dumps(
                    {
                        "query_order": ["alpha__q1", "beta__q2"],
                        "query_sources": {
                            "alpha__q1": "alpha",
                            "beta__q2": "beta",
                        },
                        "query_texts": {
                            "alpha__q1": "alpha question",
                            "beta__q2": "beta question",
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
                            "event": "query_warmup_summary",
                            "search_type": "benchmark_warmup",
                            "query_count": 2,
                            "completed_queries": 2,
                            "first_query_latency_ms": 3,
                        },
                        *[
                            {
                            "query_index": index,
                            "query": ("alpha question" if index == 0 else "beta question"),
                            "search_type": "hybrid",
                            "search_stats": {"latency_ms": str(index + 1)},
                        }
                            for index in range(2)
                        ],
                        {
                            "event": "hybrid_summary",
                            "search_type": "benchmark_summary",
                        },
                    ]
                ),
                encoding="utf-8",
            )

            error = retrieval_quality_worker.require_complete_query_trace(
                debug_path, identity_path
            )

        self.assertIsNone(error)

    def test_mixed_query_trace_contract_rejects_swapped_query_text(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            identity_path = root / "identity.json"
            identity_path.write_text(
                json.dumps(
                    {
                        "query_order": ["alpha__q1", "beta__q2"],
                        "query_sources": {
                            "alpha__q1": "alpha",
                            "beta__q2": "beta",
                        },
                        "query_texts": {
                            "alpha__q1": "alpha question",
                            "beta__q2": "beta question",
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
                            "event": "query_warmup_summary",
                            "search_type": "benchmark_warmup",
                            "query_count": 2,
                            "completed_queries": 2,
                        },
                        {
                            "query_index": 0,
                            "query": "beta question",
                            "search_type": "hybrid",
                            "search_stats": {"latency_ms": "1"},
                        },
                        {
                            "query_index": 1,
                            "query": "alpha question",
                            "search_type": "hybrid",
                            "search_stats": {"latency_ms": "1"},
                        },
                        {
                            "event": "hybrid_summary",
                            "search_type": "benchmark_summary",
                        },
                    ]
                ),
                encoding="utf-8",
            )

            error = retrieval_quality_worker.require_complete_query_trace(
                debug_path, identity_path
            )

        self.assertIsNotNone(error)
        self.assertIn("query identity mismatch", error or "")

    def test_process_peak_rss_parser_uses_benchmark_self_observation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            debug_path = Path(tmp) / "debug.jsonl"
            debug_path.write_text(
                json.dumps(
                    {
                        "event": "process_resource_metrics",
                        "search_type": "benchmark_summary",
                        "process": {"peak_rss_mb": 321.5, "peak_rss_observed": True},
                    }
                )
                + "\n",
                encoding="utf-8",
            )

            metrics = retrieval_quality_worker.parse_process_resource_metrics(debug_path)

        self.assertEqual(metrics["proc_maxrss_mb"], 321.5)
        self.assertEqual(metrics["proc_maxrss_observation_rate"], 1.0)

    def test_required_vector_persistence_rejects_query_ready_only_state(self) -> None:
        error = retrieval_quality_worker.require_vector_index_persistence_contract(
            {
                "vector_query_ready_after": 1.0,
                "vector_index_reusable_after": 0.0,
                "vector_index_reusable_probe_count": 1.0,
            },
            "required",
        )

        self.assertIsNotNone(error)
        self.assertIn("persisted vector index is not reusable", error or "")

    def test_required_vector_persistence_rejects_in_run_rebuild_as_restart_evidence(
        self,
    ) -> None:
        error = retrieval_quality_worker.require_vector_index_persistence_contract(
            {
                "vector_query_ready_after": 1.0,
                "vector_index_reusable_before": 0.0,
                "vector_index_reusable_after": 1.0,
                "vector_index_reusable_probe_count": 1.0,
            },
            "required",
        )

        self.assertIsNotNone(error)
        self.assertIn("not reusable before setup", error or "")

    def test_vector_persistence_contract_rejects_missing_reuse_probe(self) -> None:
        error = retrieval_quality_worker.require_vector_index_persistence_contract(
            {
                "vector_query_ready_after": 1.0,
                "vector_index_reusable_before": 1.0,
                "vector_index_reusable_after": 1.0,
                "vector_index_reusable_probe_count": 0.0,
            },
            "required",
        )

        self.assertIsNotNone(error)
        self.assertIn("reusability probe", error or "")

    def test_required_process_rss_rejects_missing_benchmark_self_observation(self) -> None:
        error = retrieval_quality_worker.require_process_resource_contract(
            {"proc_maxrss_observation_rate": 0.0}, "required"
        )

        self.assertIsNotNone(error)
        self.assertIn("process peak RSS", error or "")

    def test_required_process_rss_accepts_observed_benchmark_self_metric(self) -> None:
        error = retrieval_quality_worker.require_process_resource_contract(
            {
                "proc_maxrss_mb": 321.5,
                "proc_maxrss_observation_rate": 1.0,
            },
            "required",
        )

        self.assertIsNone(error)

    def test_unavailable_vector_persistence_accepts_exact_scan(self) -> None:
        error = retrieval_quality_worker.require_vector_index_persistence_contract(
            {
                "vector_query_ready_after": 1.0,
                "vector_index_reusable_after": 0.0,
                "vector_index_reusable_probe_count": 1.0,
            },
            "unavailable",
        )

        self.assertIsNone(error)

    def test_ingestion_plans_are_decision_grade_and_keep_submission_paths_separate(self) -> None:
        plans_dir = XPLAN_ROOT / "plans"
        feature_plan = json.loads(
            (plans_dir / "ingest_pipeline.json").read_text(encoding="utf-8")
        )
        path_plan = json.loads(
            (plans_dir / "ingest_submission_path.json").read_text(encoding="utf-8")
        )

        self.assertEqual(feature_plan["repeats"], 3)
        self.assertEqual(feature_plan["fixed"]["params"]["ingest_mode"], "directory")
        self.assertEqual(feature_plan["fixed"]["params"]["post_ingest_coalesce_ms"], 2)
        self.assertTrue(feature_plan["fixed"]["params"]["require_complete_pipeline"])
        self.assertTrue(feature_plan["fixed"]["params"]["require_experiment_identity"])

        self.assertEqual(path_plan["repeats"], 3)
        modes = {arm["params"]["ingest_mode"] for arm in path_plan["arms"]}
        self.assertEqual(modes, {"directory", "pipelined_single_file"})

        quality_plan = json.loads(
            (plans_dir / "search_generalized_memory_topology_gate.json").read_text(
                encoding="utf-8"
            )
        )
        required = set(quality_plan["validate"]["require_metrics"])
        self.assertIn("seed_ingest_admission_ms", required)
        self.assertIn("seed_ingest_searchability_ready_ms", required)
        self.assertIn("seed_ingest_measurement_n", required)


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
        self.assertEqual(
            identity["query_texts"],
            {"alpha__qa": "alpha memory", "beta__qb": "beta memory"},
        )

    def test_materializer_excludes_unsearchable_empty_documents(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            alpha = root / "alpha"
            beta = root / "beta"
            self._write_beir_dataset(
                alpha,
                documents=[
                    {"_id": "a1", "title": "Alpha", "text": "searchable"},
                    {"_id": "empty", "title": "", "text": ""},
                ],
                queries=[{"_id": "qa", "text": "alpha"}],
                qrels=[("qa", "a1", 1)],
            )
            self._write_beir_dataset(
                beta,
                documents=[{"_id": "b1", "title": "Beta", "text": "searchable"}],
                queries=[{"_id": "qb", "text": "beta"}],
                qrels=[("qb", "b1", 1)],
            )

            prepared = materialize_mixed_beir_manifest(
                {"alpha": alpha, "beta": beta}, root / "mixed"
            )
            manifest = json.loads(prepared.manifest_path.read_text(encoding="utf-8"))
            identity = json.loads(prepared.identity_path.read_text(encoding="utf-8"))

        self.assertEqual(prepared.document_count, 2)
        self.assertNotIn("alpha__empty", {doc["id"] for doc in manifest["documents"]})
        self.assertEqual(identity["source_counts"]["alpha"]["empty_documents_skipped"], 1)

    def test_materializer_applies_a_disjoint_query_offset_per_source(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            datasets: dict[str, Path] = {}
            for source in ("alpha", "beta"):
                dataset = root / source
                datasets[source] = dataset
                self._write_beir_dataset(
                    dataset,
                    documents=[
                        {"_id": f"d{index}", "title": source, "text": f"doc {index}"}
                        for index in range(4)
                    ],
                    queries=[
                        {"_id": f"q{index}", "text": f"query {index}"}
                        for index in range(4)
                    ],
                    qrels=[(f"q{index}", f"d{index}", 1) for index in range(4)],
                )

            prepared = materialize_mixed_beir_manifest(
                datasets,
                root / "mixed",
                queries_per_dataset=2,
                query_offset_per_dataset=2,
            )
            manifest = json.loads(prepared.manifest_path.read_text(encoding="utf-8"))
            identity = json.loads(prepared.identity_path.read_text(encoding="utf-8"))

        self.assertEqual(
            {query["id"] for query in manifest["queries"]},
            {"alpha__q2", "alpha__q3", "beta__q2", "beta__q3"},
        )
        self.assertEqual(identity["spec"]["query_offset_per_dataset"], 2)

    def test_materializer_keeps_document_construction_fixed_across_query_splits(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            datasets: dict[str, Path] = {}
            for source in ("alpha", "beta"):
                dataset = root / source
                datasets[source] = dataset
                self._write_beir_dataset(
                    dataset,
                    documents=[
                        {"_id": f"d{index}", "title": source, "text": f"doc {index}"}
                        for index in range(4)
                    ],
                    queries=[
                        {"_id": f"q{index}", "text": f"query {index}"}
                        for index in range(4)
                    ],
                    qrels=[(f"q{index}", f"d{index}", 1) for index in range(4)],
                )

            calibration = materialize_mixed_beir_manifest(
                datasets,
                root / "calibration",
                documents_per_dataset=1,
                queries_per_dataset=2,
                query_offset_per_dataset=0,
                document_queries_per_dataset=4,
                document_query_offset_per_dataset=0,
            )
            evaluation = materialize_mixed_beir_manifest(
                datasets,
                root / "evaluation",
                documents_per_dataset=1,
                queries_per_dataset=2,
                query_offset_per_dataset=2,
                document_queries_per_dataset=4,
                document_query_offset_per_dataset=0,
            )
            calibration_manifest = json.loads(
                calibration.manifest_path.read_text(encoding="utf-8")
            )
            evaluation_manifest = json.loads(
                evaluation.manifest_path.read_text(encoding="utf-8")
            )
            evaluation_identity = json.loads(
                evaluation.identity_path.read_text(encoding="utf-8")
            )

        self.assertEqual(
            calibration_manifest["documents"], evaluation_manifest["documents"]
        )
        self.assertNotEqual(
            calibration_manifest["queries"], evaluation_manifest["queries"]
        )
        self.assertEqual(
            evaluation_identity["spec"]["document_queries_per_dataset"], 4
        )
        self.assertEqual(
            evaluation_identity["spec"]["document_query_offset_per_dataset"], 0
        )

    def test_materializer_identity_detects_same_size_same_mtime_source_changes(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            alpha = root / "alpha"
            beta = root / "beta"
            self._write_beir_dataset(
                alpha,
                documents=[{"_id": "a1", "title": "Alpha", "text": "one"}],
                queries=[{"_id": "qa", "text": "alpha"}],
                qrels=[("qa", "a1", 1)],
            )
            self._write_beir_dataset(
                beta,
                documents=[{"_id": "b1", "title": "Beta", "text": "stable"}],
                queries=[{"_id": "qb", "text": "beta"}],
                qrels=[("qb", "b1", 1)],
            )
            output = root / "mixed"
            first = materialize_mixed_beir_manifest(
                {"alpha": alpha, "beta": beta}, output
            )
            first_identity = json.loads(
                first.identity_path.read_text(encoding="utf-8")
            )

            corpus_path = alpha / "corpus.jsonl"
            before = corpus_path.stat()
            original = corpus_path.read_text(encoding="utf-8")
            changed = original.replace('"text": "one"', '"text": "two"')
            self.assertEqual(len(changed), len(original))
            corpus_path.write_text(changed, encoding="utf-8")
            os.utime(
                corpus_path,
                ns=(before.st_atime_ns, before.st_mtime_ns),
            )

            second = materialize_mixed_beir_manifest(
                {"alpha": alpha, "beta": beta}, output
            )
            second_identity = json.loads(
                second.identity_path.read_text(encoding="utf-8")
            )
            second_manifest = json.loads(
                second.manifest_path.read_text(encoding="utf-8")
            )

        self.assertNotEqual(first_identity["spec"], second_identity["spec"])
        alpha_document = next(
            row for row in second_manifest["documents"] if row["id"] == "alpha__a1"
        )
        self.assertEqual(alpha_document["text"], "two")

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
                        "query_qrels": {
                            "alpha__qa": {"alpha__a1": 1},
                            "beta__qb": {"beta__b1": 1, "beta__b2": 1},
                        },
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
        self.assertAlmostEqual(metrics["mixed_ndcg_at_k_alpha"], 1.0 / math.log2(3.0))
        self.assertAlmostEqual(
            metrics["mixed_ndcg_at_k_beta"],
            1.0 / (1.0 + 1.0 / math.log2(3.0)),
        )
        self.assertAlmostEqual(
            metrics["mixed_source_macro_ndcg_at_k"],
            (
                1.0 / math.log2(3.0)
                + 1.0 / (1.0 + 1.0 / math.log2(3.0))
            )
            / 2.0,
        )
        self.assertAlmostEqual(
            metrics["mixed_source_min_ndcg_at_k"],
            1.0 / (1.0 + 1.0 / math.log2(3.0)),
        )
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
