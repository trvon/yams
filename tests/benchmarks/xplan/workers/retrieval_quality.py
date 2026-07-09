"""Run retrieval_quality_bench and harvest quality/topology counters for xplan.

Internalizes topology expansion-arm presets formerly encoded only in
``topology_expansion_fusion_ablation.sh``:
  full64, medoid64, cap8, cap2, rerank_only
"""

from __future__ import annotations

import json
import math
import os
import re
import resource
import statistics
import sys
import time
from pathlib import Path
from typing import Any

from artifacts import write_json
from workers.ablation import apply_ablation
from workers.base import WorkerContext, WorkerResult
from workers.beir_data import (
    cache_dir as _beir_cache_dir,
    corpus_present as _beir_corpus_present,
    ensure_beir_dataset,
    is_beir_dataset,
)
from workers.util import maybe_meson_compile, resolve_binary, run_captured

_METRIC_RE = re.compile(
    r"^\s*(MRR \(Mean Reciprocal Rank\)|Recall@K|Precision@K|"
    r"nDCG@K \(Normalized DCG\)|MAP \(Mean Average Precision\)):\s+([0-9.]+)"
)
_SECTION_RE = re.compile(r"--- (.+) ---")
_GBENCH_COUNTER_RE = re.compile(
    r"\b(MRR|Recall@K|Precision@K|nDCG@K|MAP|MRR_keyword|MRR_grep)\s*[:=]\s*([0-9.eE+-]+)"
)

# Arm name → env for expansion/fusion/rerank ablations (was arm_env() in shell).
EXPANSION_PRESETS: dict[str, dict[str, str]] = {
    "full64": {
        "YAMS_BENCH_TOPOLOGY_MODE": "hybrid_assist",
        "YAMS_SEARCH_TOPOLOGY_MAX_DOCS": "64",
        "YAMS_BENCH_TOPOLOGY_MEDOID_ONLY_EXPANSION": "0",
    },
    "medoid64": {
        "YAMS_BENCH_TOPOLOGY_MODE": "hybrid_assist",
        "YAMS_SEARCH_TOPOLOGY_MAX_DOCS": "64",
        "YAMS_BENCH_TOPOLOGY_MEDOID_ONLY_EXPANSION": "1",
    },
    "cap8": {
        "YAMS_BENCH_TOPOLOGY_MODE": "hybrid_assist",
        "YAMS_SEARCH_TOPOLOGY_MAX_DOCS": "8",
        "YAMS_BENCH_TOPOLOGY_MEDOID_ONLY_EXPANSION": "0",
    },
    "cap2": {
        "YAMS_BENCH_TOPOLOGY_MODE": "hybrid_assist",
        "YAMS_SEARCH_TOPOLOGY_MAX_DOCS": "2",
        "YAMS_BENCH_TOPOLOGY_MEDOID_ONLY_EXPANSION": "0",
    },
    "rerank_only": {
        "YAMS_BENCH_TOPOLOGY_MODE": "rerank_only",
        "YAMS_SEARCH_TOPOLOGY_MAX_DOCS": "64",
        "YAMS_BENCH_TOPOLOGY_MEDOID_ONLY_EXPANSION": "0",
    },
}

_NON_VECTOR_SOURCES = frozenset(
    {"fts5", "keyphrase", "segment_keyphrase", "kg", "gliner", "header", "theme"}
)

def _truthy(val: Any) -> bool:
    return str(val).lower() in {"1", "true", "yes", "on"}


def _as_int(val: Any) -> int:
    try:
        return int(float(str(val)))
    except (TypeError, ValueError):
        return 0


def apply_expansion_preset(env: dict[str, str], arm: str | None) -> str | None:
    """Apply named expansion arm env. Returns preset name if applied."""
    if not arm:
        return None
    key = str(arm).strip()
    preset = EXPANSION_PRESETS.get(key)
    if not preset:
        return None
    for ek, ev in preset.items():
        # Explicit arm/plan env already on env wins only if preset not meant to own it.
        # Expansion arm always owns these three knobs.
        env[ek] = ev
    return key


def parse_quality_from_text(text: str) -> dict[str, float]:
    """Parse human-readable quality sections and gbench counter lines."""
    out: dict[str, float] = {}
    section: str | None = None
    key_map = {
        "MRR (Mean Reciprocal Rank)": "mrr",
        "Recall@K": "recall_at_k",
        "Precision@K": "precision_at_k",
        "nDCG@K (Normalized DCG)": "ndcg_at_k",
        "MAP (Mean Average Precision)": "map",
    }
    for line in text.splitlines():
        sm = _SECTION_RE.search(line)
        if sm:
            section = sm.group(1).strip().lower().replace(" ", "_")
            continue
        mm = _METRIC_RE.search(line)
        if mm and section:
            bare = key_map.get(mm.group(1), mm.group(1))
            out[f"{section}_{bare}"] = float(mm.group(2))
            out[bare] = float(mm.group(2))
            continue
        for name, val in _GBENCH_COUNTER_RE.findall(line):
            norm = name.lower().replace("@", "_at_").replace(" ", "_")
            try:
                out[norm] = float(val)
            except ValueError:
                pass
    return out


def parse_debug_jsonl(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {}
    hybrid = 0
    counters = {
        "load_attempted": 0,
        "load_succeeded": 0,
        "admitted": 0,
        "applied": 0,
        "added": 0,
        "duplicate": 0,
        "stale": 0,
        "routed_docs": 0,
        "routed_clusters": 0,
        "rejected": 0,
        "added_post_fusion": 0,
        "added_fusion_dropped": 0,
        "sidecar_candidates": 0,
    }
    added_vals: list[int] = []
    routed_docs: list[int] = []
    routed_clusters: list[int] = []
    sidecar_vals: list[int] = []
    skip_reasons: dict[str, int] = {}
    scoring_modes: dict[str, int] = {}
    routing_modes: dict[str, int] = {}
    medoid_flags: dict[str, int] = {}
    engine_variants: dict[str, int] = {}
    pipeline_variants: dict[str, int] = {}
    cert_seen = 0
    cert_ok = 0
    cert_errors: list[Any] = []
    hybrid_summary_metrics: dict[str, Any] = {}
    keyword_summary_metrics: dict[str, Any] = {}
    rerank_configured = 0
    rerank_applied = 0
    rerank_skip_reasons: dict[str, int] = {}
    rerank_blend_weights: list[float] = []
    fusion_source_mass: dict[str, float] = {}
    fusion_source_docs: dict[str, float] = {}
    latency_vals: list[float] = []
    # Mechanism counters (GraphNeighbors seed ANN / path labels).
    vector_seeds_added_vals: list[float] = []
    vector_seed_probe_vals: list[float] = []
    path_medoid = 0
    path_seed_neighbors = 0

    int_keys = {
        "added": "topology_weak_query_added_candidates",
        "duplicate": "topology_weak_query_duplicate_candidates",
        "stale": "topology_weak_query_stale_candidates",
        "routed_docs": "topology_weak_query_routed_docs",
        "routed_clusters": "topology_weak_query_routed_clusters",
        "rejected": "topology_weak_query_routes_rejected",
        "added_post_fusion": "topology_added_candidate_post_fusion_count",
        "added_fusion_dropped": "topology_added_candidate_fusion_dropped_count",
        "sidecar_candidates": "topology_sidecar_vector_candidates",
    }

    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(obj, dict):
            continue

        if obj.get("event") == "topology_construction_certificate":
            cert_seen += 1
            certificate = obj.get("certificate") or {}
            validation = certificate.get("validation") or {}
            if validation.get("ok") is True:
                cert_ok += 1
            else:
                cert_errors.append(validation.get("errors") or ["unknown certificate error"])
            continue

        if obj.get("event") == "hybrid_summary":
            hm = obj.get("hybrid_metrics")
            if isinstance(hm, dict):
                hybrid_summary_metrics = hm
            km = obj.get("keyword_metrics")
            if isinstance(km, dict):
                keyword_summary_metrics = km
            continue

        if obj.get("search_type") != "hybrid":
            continue
        hybrid += 1
        stats = obj.get("search_stats") or {}
        if not isinstance(stats, dict):
            continue

        try:
            lat = stats.get("latency_ms")
            if lat is not None and str(lat) != "":
                latency_vals.append(float(lat))
        except (TypeError, ValueError):
            pass

        if _truthy(stats.get("cross_rerank_configured")):
            rerank_configured += 1
            if _truthy(stats.get("trace_cross_rerank_applied")) or _truthy(
                stats.get("cross_rerank_applied")
            ):
                rerank_applied += 1
            rr_reason = str(stats.get("cross_rerank_skip_reason", "") or "")
            if rr_reason:
                rerank_skip_reasons[rr_reason] = rerank_skip_reasons.get(rr_reason, 0) + 1
            try:
                bw = stats.get("cross_rerank_blend_weight")
                if bw is not None and str(bw) != "":
                    rerank_blend_weights.append(float(bw))
            except (TypeError, ValueError):
                pass

        fusion_summary = stats.get("trace_fusion_source_summary_json")
        if fusion_summary:
            try:
                fs = json.loads(fusion_summary) if isinstance(fusion_summary, str) else fusion_summary
                sources = fs.get("sources", fs) if isinstance(fs, dict) else {}
                for sname, sinfo in (sources.items() if isinstance(sources, dict) else []):
                    if not isinstance(sinfo, dict):
                        continue
                    fusion_source_mass[sname] = fusion_source_mass.get(sname, 0.0) + float(
                        sinfo.get("final_score_mass", 0.0) or 0.0
                    )
                    fusion_source_docs[sname] = fusion_source_docs.get(sname, 0.0) + float(
                        sinfo.get("final_top_doc_count", 0.0) or 0.0
                    )
            except (ValueError, TypeError, AttributeError):
                pass

        if _truthy(stats.get("topology_weak_query_load_attempted")):
            counters["load_attempted"] += 1
        if _truthy(stats.get("topology_weak_query_load_succeeded")):
            counters["load_succeeded"] += 1
        if _truthy(stats.get("topology_artifact_admitted")):
            counters["admitted"] += 1
        if _truthy(stats.get("topology_weak_query_applied")):
            counters["applied"] += 1
        for dst, key in int_keys.items():
            v = _as_int(stats.get(key))
            counters[dst] += v
            if dst == "added":
                added_vals.append(v)
            elif dst == "routed_docs":
                routed_docs.append(v)
            elif dst == "routed_clusters":
                routed_clusters.append(v)
            elif dst == "sidecar_candidates":
                sidecar_vals.append(v)

        reason = str(stats.get("topology_weak_query_skip_reason", "") or "")
        skip_reasons[reason] = skip_reasons.get(reason, 0) + 1
        # Path labels are emitted as skip_reason even when expansion applied.
        if reason in ("graph_medoid_neighbors", "graph_medoid_anchor"):
            path_medoid += 1
        elif reason in ("graph_seed_neighbors", "graph_neighbors"):
            path_seed_neighbors += 1
        elif "medoid" in reason:
            path_medoid += 1
        elif "seed" in reason and "neighbor" in reason:
            path_seed_neighbors += 1

        try:
            vsa = stats.get("topology_vector_seeds_added")
            if vsa is not None and str(vsa) != "":
                vector_seeds_added_vals.append(float(vsa))
        except (TypeError, ValueError):
            pass
        try:
            vsp = stats.get("topology_vector_seed_probe")
            if vsp is not None and str(vsp) != "":
                vector_seed_probe_vals.append(float(vsp))
        except (TypeError, ValueError):
            pass

        mode = str(stats.get("topology_route_scoring_mode", "") or "")
        if mode:
            scoring_modes[mode] = scoring_modes.get(mode, 0) + 1
        rmode = str(stats.get("topology_routing_mode", "") or "")
        if rmode:
            routing_modes[rmode] = routing_modes.get(rmode, 0) + 1
        med = str(stats.get("topology_medoid_only_expansion", "") or "")
        if med:
            medoid_flags[med] = medoid_flags.get(med, 0) + 1
        for field, bucket in (
            ("search_engine_variant", engine_variants),
            ("search_pipeline_variant", pipeline_variants),
        ):
            val = str(stats.get(field) or obj.get(field) or "")
            if val:
                bucket[val] = bucket.get(val, 0) + 1

    metrics: dict[str, Any] = {
        "hybrid_queries": float(hybrid),
        "topology_load_attempted": float(counters["load_attempted"]),
        "topology_load_succeeded": float(counters["load_succeeded"]),
        "topology_admitted": float(counters["admitted"]),
        "topology_applied": float(counters["applied"]),
        "topology_added_sum": float(counters["added"]),
        "topology_duplicate_sum": float(counters["duplicate"]),
        "topology_stale_sum": float(counters["stale"]),
        "topology_routed_docs_sum": float(counters["routed_docs"]),
        "topology_routed_clusters_sum": float(counters["routed_clusters"]),
        "topology_rejected_sum": float(counters["rejected"]),
        "topology_added_post_fusion_sum": float(counters["added_post_fusion"]),
        "topology_added_fusion_dropped_sum": float(counters["added_fusion_dropped"]),
        "topology_sidecar_sum": float(counters["sidecar_candidates"]),
        "topology_cert_seen": float(cert_seen),
        "topology_cert_ok": float(cert_ok),
    }
    for mkey, mval in (hybrid_summary_metrics or {}).items():
        try:
            fv = float(mval)
        except (TypeError, ValueError):
            continue
        metrics[f"hybrid_{mkey}"] = fv
        metrics[mkey] = fv  # authoritative bare keys (fixes grep-clobber of stdout scrape)
    for mkey, mval in (keyword_summary_metrics or {}).items():
        try:
            metrics[f"keyword_{mkey}"] = float(mval)
        except (TypeError, ValueError):
            continue
    metrics["cross_rerank_configured_queries"] = float(rerank_configured)
    metrics["cross_rerank_applied_queries"] = float(rerank_applied)
    if rerank_configured:
        metrics["cross_rerank_apply_rate"] = float(rerank_applied) / float(rerank_configured)
    if rerank_blend_weights:
        metrics["cross_rerank_blend_weight"] = float(statistics.mean(rerank_blend_weights))
    if latency_vals:
        metrics["search_latency_ms_avg"] = float(statistics.mean(latency_vals))
        metrics["search_latency_ms_p50"] = float(statistics.median(latency_vals))
        sorted_lat = sorted(latency_vals)
        # Nearest-rank p95: ceil(0.95 * n)th sample (1-indexed).
        p95_idx = min(len(sorted_lat) - 1, max(0, math.ceil(0.95 * len(sorted_lat)) - 1))
        metrics["search_latency_ms_p95"] = float(sorted_lat[p95_idx])
    for sname, mass in fusion_source_mass.items():
        metrics[f"fusion_mass_{sname}"] = float(mass)
    for sname, docs in fusion_source_docs.items():
        metrics[f"fusion_docs_{sname}"] = float(docs)
    if added_vals:
        metrics["topology_added_avg"] = float(statistics.mean(added_vals))
        metrics["topology_added_max"] = float(max(added_vals))
    if routed_docs:
        metrics["topology_routed_docs_avg"] = float(statistics.mean(routed_docs))
    if routed_clusters:
        metrics["topology_routed_clusters_avg"] = float(statistics.mean(routed_clusters))
    if sidecar_vals:
        metrics["topology_sidecar_avg"] = float(statistics.mean(sidecar_vals))

    # Path / seed-ANN mechanism rates (for quality×cost loops).
    if hybrid > 0:
        metrics["topology_path_medoid_rate"] = float(path_medoid) / float(hybrid)
        metrics["topology_path_seed_neighbors_rate"] = float(path_seed_neighbors) / float(hybrid)
    if vector_seeds_added_vals:
        metrics["topology_vector_seeds_added_avg"] = float(
            statistics.mean(vector_seeds_added_vals)
        )
        metrics["topology_vector_seeds_added_max"] = float(max(vector_seeds_added_vals))
        metrics["topology_vector_seeds_nonzero_rate"] = float(
            sum(1 for v in vector_seeds_added_vals if v > 0)
        ) / float(len(vector_seeds_added_vals))
    if vector_seed_probe_vals:
        metrics["topology_vector_seed_probe_avg"] = float(statistics.mean(vector_seed_probe_vals))
        metrics["topology_vector_seed_probe_max"] = float(max(vector_seed_probe_vals))

    return {
        "metrics": metrics,
        "skip_reasons": skip_reasons,
        "rerank": {
            "configured": rerank_configured,
            "applied": rerank_applied,
            "skip_reasons": rerank_skip_reasons,
        },
        "scoring_modes": scoring_modes,
        "routing_modes": routing_modes,
        "medoid_flags": medoid_flags,
        "engine_variants": engine_variants,
        "pipeline_variants": pipeline_variants,
        "certificate": {
            "seen": cert_seen,
            "ok": cert_ok,
            "errors": cert_errors[:5],
        },
    }


def require_topology_certificate(
    *,
    source: str,
    seed_enabled: bool,
    debug_path: Path,
) -> str | None:
    """Return error message if certificate is required and missing/invalid."""
    if source == "vector" or source not in _NON_VECTOR_SOURCES:
        return None
    if not seed_enabled:
        return None
    if not debug_path.is_file():
        return f"missing topology certificate debug log for source={source}: {debug_path}"
    seen = 0
    bad: list[Any] = []
    for line in debug_path.read_text(encoding="utf-8", errors="replace").splitlines():
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        if event.get("event") != "topology_construction_certificate":
            continue
        seen += 1
        certificate = event.get("certificate") or {}
        validation = certificate.get("validation") or {}
        if validation.get("ok") is not True:
            bad.append(validation.get("errors") or ["unknown certificate validation error"])
    if seen == 0:
        return f"missing topology construction certificate for source={source}"
    if bad:
        return f"invalid topology construction certificate for source={source}: {bad[0]}"
    return None


def run_retrieval_quality(ctx: WorkerContext) -> WorkerResult:
    explicit = (
        ctx.params.get("binary")
        or os.environ.get("YAMS_TOPOLOGY_EXPANSION_BIN")
        or os.environ.get("YAMS_TOPOLOGY_ABLATION_BIN")
        or os.environ.get("YAMS_BENCH_BIN")
    )
    binary = resolve_binary(
        ctx.build_dir,
        "tests",
        "benchmarks",
        "retrieval_quality_bench",
        explicit=str(explicit) if explicit else None,
    )

    stdout_path = ctx.arm_dir / "stdout.log"
    stderr_path = ctx.arm_dir / "time.log"
    debug_path = ctx.arm_dir / "debug.jsonl"

    arm_factors = getattr(ctx.arm, "arm", None)
    arm_factors = getattr(arm_factors, "factors", None) or {}
    expansion_arm = (
        ctx.params.get("expansion_arm")
        or ctx.params.get("arm")
        or arm_factors.get("expansion_arm")
    )

    if ctx.dry_run:
        write_json(
            ctx.arm_dir / "step_dry_run.json",
            {
                "binary": str(binary),
                "params": ctx.params,
                "expansion_arm": expansion_arm,
                "expansion_preset": EXPANSION_PRESETS.get(str(expansion_arm or "")),
            },
        )
        stdout_path.write_text("# dry-run\n", encoding="utf-8")
        stderr_path.write_text("", encoding="utf-8")
        debug_path.write_text("", encoding="utf-8")
        # Zero-fill KPIs listed in plan step.metrics so dry-run validation passes.
        return WorkerResult(
            status="ok",
            exit_code=0,
            metrics={
                "mrr": 0.0,
                "recall_at_k": 0.0,
                "precision_at_k": 0.0,
                "ndcg_at_k": 0.0,
                "map": 0.0,
                "hybrid_queries": 0.0,
                "search_latency_ms_avg": 0.0,
                "search_latency_ms_p50": 0.0,
                "search_latency_ms_p95": 0.0,
                "topology_applied": 0.0,
                "topology_added_avg": 0.0,
                "topology_routed_docs_avg": 0.0,
                "topology_routed_clusters_avg": 0.0,
                "topology_cert_seen": 0.0,
                "topology_path_medoid_rate": 0.0,
                "topology_path_seed_neighbors_rate": 0.0,
                "topology_vector_seeds_added_avg": 0.0,
                "topology_vector_seeds_nonzero_rate": 0.0,
                "topology_vector_seed_probe_avg": 0.0,
            },
            attributes={
                "dry_run": True,
                "binary": str(binary),
                "expansion_arm": expansion_arm,
            },
            message="dry-run retrieval_quality",
        )

    # Skip mid-run rebuilds by default when the binary already exists (avoids
    # multi-arm/multi-rep link races). Force with YAMS_BENCH_FORCE_BUILD=1.
    skip_build = bool(ctx.params.get("skip_build")) or os.environ.get("YAMS_BENCH_SKIP_BUILD") == "1"
    force_build = bool(ctx.params.get("force_build")) or os.environ.get("YAMS_BENCH_FORCE_BUILD") == "1"
    maybe_meson_compile(
        ctx.repo_root,
        ctx.build_dir,
        "bench_retrieval_quality",
        skip=skip_build,
        binary=binary,
        force=force_build,
    )
    if not binary.exists():
        return WorkerResult(
            status="failed",
            exit_code=2,
            metrics={},
            attributes={"binary": str(binary)},
            message=f"retrieval_quality_bench missing: {binary}",
        )

    env = os.environ.copy()
    env.update(ctx.env)
    env.setdefault("YAMS_TEST_SAFE_SINGLE_INSTANCE", "1")
    env.setdefault("YAMS_SEARCH_STAGE_TRACE", "1")

    # Default topology scoring knobs (match former common_env).
    env.setdefault("YAMS_BENCH_TOPOLOGY_ROUTE_SCORING", "current")
    env.setdefault("YAMS_BENCH_TOPOLOGY_SPARSE_DENSE_ALPHA", "0.5")
    env.setdefault("YAMS_BENCH_TOPOLOGY_MIN_ROUTE_SCORE", "0")
    env.setdefault("YAMS_SEARCH_TOPOLOGY_MAX_CLUSTERS", "2")
    env.setdefault("YAMS_SEARCH_TOPOLOGY_MAX_DOCS_PER_CLUSTER", "0")

    param_env = {
        "corpus_size": "YAMS_BENCH_CORPUS_SIZE",
        "num_queries": "YAMS_BENCH_NUM_QUERIES",
        "topk": "YAMS_BENCH_TOPK",
        "dataset": "YAMS_BENCH_DATASET",
        "dataset_path": "YAMS_BENCH_DATASET_PATH",
        "topology_mode": "YAMS_BENCH_TOPOLOGY_MODE",
        "topology_engine": "YAMS_BENCH_TOPOLOGY_ENGINE",
        "topology_source": "YAMS_BENCH_TOPOLOGY_SOURCE",
        "feature_smoothing_hops": "YAMS_BENCH_TOPOLOGY_FEATURE_SMOOTHING_HOPS",
        "route_scoring": "YAMS_BENCH_TOPOLOGY_ROUTE_SCORING",
        "sparse_dense_alpha": "YAMS_BENCH_TOPOLOGY_SPARSE_DENSE_ALPHA",
        "min_route_score": "YAMS_BENCH_TOPOLOGY_MIN_ROUTE_SCORE",
        "medoid_only_expansion": "YAMS_BENCH_TOPOLOGY_MEDOID_ONLY_EXPANSION",
        "max_docs": "YAMS_SEARCH_TOPOLOGY_MAX_DOCS",
        "max_clusters": "YAMS_SEARCH_TOPOLOGY_MAX_CLUSTERS",
        "max_component_docs": "YAMS_BENCH_TOPOLOGY_MAX_COMPONENT_DOCS",
        "min_edge_score": "YAMS_TOPOLOGY_MIN_EDGE_SCORE",
        "expansion_source": "YAMS_BENCH_TOPOLOGY_EXPANSION",
        "seed_semantic_neighbors": "YAMS_BENCH_SEED_SEMANTIC_NEIGHBORS",
        "seed_semantic_topk": "YAMS_BENCH_SEED_SEMANTIC_TOPK",
        "seed_semantic_threshold": "YAMS_BENCH_SEED_SEMANTIC_THRESHOLD",
        "graph_neighbor_min_score": "YAMS_SEARCH_TOPOLOGY_GRAPH_NEIGHBOR_MIN_SCORE",
        "graph_neighbor_reciprocal_only": "YAMS_SEARCH_TOPOLOGY_GRAPH_NEIGHBOR_RECIPROCAL_ONLY",
        "hdbscan_min_points": "YAMS_BENCH_TOPOLOGY_HDBSCAN_MIN_POINTS",
        "hdbscan_min_cluster_size": "YAMS_BENCH_TOPOLOGY_HDBSCAN_MIN_CLUSTER_SIZE",
        "enable_reranking": "YAMS_SEARCH_ENABLE_RERANKING",
        "rerank_topk": "YAMS_SEARCH_RERANK_TOPK",
        "rerank_replace_scores": "YAMS_SEARCH_RERANK_REPLACE_SCORES",
        "rerank_weight": "YAMS_SEARCH_RERANK_WEIGHT",
        "embed_backend": "YAMS_EMBED_BACKEND",
        "search_engine_variant": "YAMS_SEARCH_ENGINE_VARIANT",
        "ingest_min_fraction": "YAMS_BENCH_INGEST_MIN_FRACTION",
        "progress_timeout_sec": "YAMS_BENCH_PROGRESS_TIMEOUT",
    }
    for pkey, ekey in param_env.items():
        if pkey in ctx.params and ekey not in env:
            env[ekey] = str(ctx.params[pkey])

    # Construction purity: mirror bench cert cap into TopologyManager build env.
    if "YAMS_BENCH_TOPOLOGY_MAX_COMPONENT_DOCS" in env and "YAMS_TOPOLOGY_MAX_COMPONENT_DOCS" not in env:
        env["YAMS_TOPOLOGY_MAX_COMPONENT_DOCS"] = env["YAMS_BENCH_TOPOLOGY_MAX_COMPONENT_DOCS"]
    if "YAMS_BENCH_TOPOLOGY_MIN_EDGE_SCORE" in env and "YAMS_TOPOLOGY_MIN_EDGE_SCORE" not in env:
        env["YAMS_TOPOLOGY_MIN_EDGE_SCORE"] = env["YAMS_BENCH_TOPOLOGY_MIN_EDGE_SCORE"]
    # Search expansion_source also as typed SearchEngine overlay.
    if "YAMS_BENCH_TOPOLOGY_EXPANSION" in env and "YAMS_SEARCH_TOPOLOGY_EXPANSION_SOURCE" not in env:
        env["YAMS_SEARCH_TOPOLOGY_EXPANSION_SOURCE"] = env["YAMS_BENCH_TOPOLOGY_EXPANSION"]

    # Factors often carry topology_source / expansion_arm.
    if "topology_source" in arm_factors and "YAMS_BENCH_TOPOLOGY_SOURCE" not in env:
        env["YAMS_BENCH_TOPOLOGY_SOURCE"] = str(arm_factors["topology_source"])

    # Shared ablation axes (component weights, topology, rerank, vectors, …).
    ablation = apply_ablation(env, factors=arm_factors, params=ctx.params)

    applied_preset = apply_expansion_preset(env, str(expansion_arm) if expansion_arm else None)
    if expansion_arm and not applied_preset and str(expansion_arm) not in EXPANSION_PRESETS:
        # Unknown named arm is only an error when it looks like a preset name.
        if re.fullmatch(r"[a-z0-9_]+", str(expansion_arm)):
            return WorkerResult(
                status="failed",
                exit_code=2,
                metrics={},
                message=(
                    f"unknown expansion_arm {expansion_arm!r}; "
                    f"known: {', '.join(sorted(EXPANSION_PRESETS))}"
                ),
            )

    # Quality plans default to BEIR scifact. Opt into synthetic only when explicitly set.
    env.setdefault("YAMS_BENCH_DATASET", str(ctx.params.get("dataset", "scifact")))
    env.setdefault("YAMS_BENCH_CORPUS_SIZE", str(ctx.params.get("corpus_size", 2000)))
    env.setdefault("YAMS_BENCH_NUM_QUERIES", str(ctx.params.get("num_queries", 50)))
    env.setdefault("YAMS_BENCH_TOPK", str(ctx.params.get("topk", 10)))
    env.setdefault("YAMS_BENCH_TOPOLOGY_ENGINE", str(ctx.params.get("topology_engine", "connected")))
    env.setdefault("YAMS_BENCH_TOPOLOGY_SOURCE", str(ctx.params.get("topology_source", "vector")))
    env["YAMS_BENCH_DEBUG_FILE"] = str(debug_path)

    # BEIR first-class: resolve cache path; auto-download when missing.
    dataset_name = env.get("YAMS_BENCH_DATASET", "scifact").lower()
    is_beir = is_beir_dataset(dataset_name)
    if is_beir and not env.get("YAMS_BENCH_DATASET_PATH"):
        auto = str(ctx.params.get("download_beir", "1")).lower() not in {
            "0",
            "false",
            "no",
            "off",
        }
        try:
            cache = ensure_beir_dataset(dataset_name, download=auto)
        except Exception as exc:  # noqa: BLE001
            return WorkerResult(
                status="failed",
                exit_code=2,
                metrics={},
                attributes={
                    "dataset": dataset_name,
                    "expected_path": str(_beir_cache_dir(dataset_name)),
                },
                message=str(exc),
            )
        if not _beir_corpus_present(cache):
            return WorkerResult(
                status="failed",
                exit_code=2,
                metrics={},
                attributes={"dataset": dataset_name, "expected_path": str(cache)},
                message=(
                    f"BEIR dataset {dataset_name!r} incomplete at {cache} "
                    "(need corpus.jsonl + queries.jsonl + qrels/test.tsv)"
                ),
            )
        env["YAMS_BENCH_DATASET_PATH"] = str(cache)

    # Ingest tolerance: BEIR corpora content-dedup so `indexed` never reaches corpusSize.
    env.setdefault("YAMS_BENCH_INGEST_MIN_FRACTION", "0.95" if is_beir else "1.0")
    env.setdefault("YAMS_BENCH_PROGRESS_TIMEOUT", "180")

    # Determinism defaults (C3): low-variance runs unless the plan overrides.
    env.setdefault("YAMS_HNSW_RANDOM_SEED", "42")
    env.setdefault("YAMS_HNSW_PARALLEL_BUILD_THRESHOLD", "0")
    env.setdefault("YAMS_SEARCH_WAIT_FOR_CONCEPTS", "1")

    # Non-vector sources typically need neighbor seeding for construction certs.
    source = env.get("YAMS_BENCH_TOPOLOGY_SOURCE", "vector")
    if source in _NON_VECTOR_SOURCES:
        env.setdefault("YAMS_BENCH_SEED_SEMANTIC_NEIGHBORS", "1")

    bench_filter = str(ctx.params.get("benchmark_filter") or "BM_RetrievalQuality")
    cmd = [str(binary), f"--benchmark_filter={bench_filter}"]

    timeout = ctx.step.timeout_sec or int(ctx.params.get("timeout_sec") or ctx.arm.plan.timeout_sec)
    # Let the bench's own ingest-stall logic (PROGRESS_TIMEOUT) fire before we kill the process.
    progress_timeout = int(env.get("YAMS_BENCH_PROGRESS_TIMEOUT", "0") or "0")
    timeout = max(timeout, progress_timeout + 120)
    ru_before = resource.getrusage(resource.RUSAGE_CHILDREN)
    wall_start = time.monotonic()
    try:
        proc = run_captured(
            cmd,
            cwd=ctx.repo_root,
            env=env,
            timeout=timeout,
            stdout_path=stdout_path,
            stderr_path=stderr_path,
        )
    except Exception as exc:  # noqa: BLE001
        return WorkerResult(
            status="failed",
            exit_code=124,
            metrics={},
            message=f"retrieval_quality timed out: {exc}",
        )
    wall_sec = time.monotonic() - wall_start
    ru_after = resource.getrusage(resource.RUSAGE_CHILDREN)
    # CPU time (utime+stime) is additive across children → reliable per-arm delta.
    proc_cpu_sec = (ru_after.ru_utime + ru_after.ru_stime) - (
        ru_before.ru_utime + ru_before.ru_stime
    )
    # ru_maxrss is a high-water mark (bytes on macOS, KiB on Linux); whole-process, coarse.
    rss_scale = 1.0 / (1024 * 1024) if sys.platform == "darwin" else 1.0 / 1024
    proc_maxrss_mb = float(ru_after.ru_maxrss) * rss_scale

    (ctx.arm_dir / "exit_code").write_text(str(proc.returncode) + "\n", encoding="utf-8")
    combined = (stdout_path.read_text(encoding="utf-8", errors="replace") if stdout_path.exists() else "")
    combined += "\n"
    combined += stderr_path.read_text(encoding="utf-8", errors="replace") if stderr_path.exists() else ""

    quality = parse_quality_from_text(combined)
    dbg = parse_debug_jsonl(debug_path)
    metrics: dict[str, Any] = {
        "mrr": float(quality.get("mrr", 0.0)),
        "recall_at_k": float(quality.get("recall_at_k", 0.0)),
        "precision_at_k": float(quality.get("precision_at_k", 0.0)),
        "ndcg_at_k": float(quality.get("ndcg_at_k", 0.0)),
        "map": float(quality.get("map", 0.0)),
    }
    metrics.update(quality)
    metrics.update(dbg.get("metrics") or {})
    metrics["proc_cpu_sec"] = float(proc_cpu_sec)
    metrics["proc_wall_sec"] = float(wall_sec)
    metrics["proc_maxrss_mb"] = float(proc_maxrss_mb)

    seed_enabled = _truthy(env.get("YAMS_BENCH_SEED_SEMANTIC_NEIGHBORS", "0"))
    cert_err = require_topology_certificate(
        source=source, seed_enabled=seed_enabled, debug_path=debug_path
    )
    require_cert = bool(ctx.params.get("require_topology_certificate"))
    if require_cert is False and "require_topology_certificate" in ctx.params:
        cert_err = None  # explicit opt-out
    # Auto-require for non-vector + seed (shell behavior).
    if cert_err and source in _NON_VECTOR_SOURCES and seed_enabled:
        write_json(
            ctx.arm_dir / "quality_parse.json",
            {"quality": quality, "debug": dbg, "certificate_error": cert_err},
        )
        return WorkerResult(
            status="failed",
            exit_code=proc.returncode if proc.returncode != 0 else 1,
            metrics=metrics,
            attributes={
                "binary": str(binary),
                "expansion_arm": applied_preset or expansion_arm,
                "topology_source": source,
                "certificate_error": cert_err,
            },
            message=cert_err,
            raw_path=str(debug_path) if debug_path.exists() else str(stdout_path),
        )

    status = "ok" if proc.returncode == 0 else "failed"
    write_json(
        ctx.arm_dir / "quality_parse.json",
        {
            "quality": quality,
            "debug": {
                k: v
                for k, v in dbg.items()
                if k != "metrics"
            },
            "expansion_arm": applied_preset or expansion_arm,
            "env_snapshot": {
                k: env.get(k)
                for k in (
                    "YAMS_BENCH_TOPOLOGY_MODE",
                    "YAMS_SEARCH_TOPOLOGY_MAX_DOCS",
                    "YAMS_BENCH_TOPOLOGY_MEDOID_ONLY_EXPANSION",
                    "YAMS_BENCH_TOPOLOGY_SOURCE",
                    "YAMS_BENCH_TOPOLOGY_ENGINE",
                    "YAMS_BENCH_SEED_SEMANTIC_NEIGHBORS",
                    "YAMS_SEARCH_ENGINE_VARIANT",
                )
            },
        },
    )

    return WorkerResult(
        status=status,
        exit_code=proc.returncode,
        metrics=metrics,
        attributes={
            "binary": str(binary),
            "debug_file": str(debug_path),
            "expansion_arm": applied_preset or expansion_arm,
            "topology_source": source,
            "ablation": ablation,
            "skip_reasons": dbg.get("skip_reasons"),
            "scoring_modes": dbg.get("scoring_modes"),
            "routing_modes": dbg.get("routing_modes"),
            "certificate": dbg.get("certificate"),
            "dataset": env.get("YAMS_BENCH_DATASET"),
            "corpus_size": env.get("YAMS_BENCH_CORPUS_SIZE"),
        },
        message="retrieval_quality completed" if proc.returncode == 0 else f"exit {proc.returncode}",
        raw_path=str(debug_path) if debug_path.exists() else str(stdout_path),
    )
