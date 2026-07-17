"""Run retrieval_quality_bench and harvest quality/topology counters for xplan.

Internalizes topology expansion-arm presets formerly encoded only in
``topology_expansion_fusion_ablation.sh``:
  full64, medoid64, cap8, cap2
"""

from __future__ import annotations

import json
import math
import os
import re
import resource
import shutil
import signal
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
from workers.mixed_corpus import (
    PreparedMixedCorpus,
    analyze_mixed_cluster_overlap,
    analyze_mixed_corpus_debug,
    materialize_mixed_beir_manifest,
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
}

_NON_VECTOR_SOURCES = frozenset(
    {"fts5", "keyphrase", "segment_keyphrase", "kg", "gliner", "header", "theme"}
)


def _benchmark_command(binary: Path, benchmark_filter: str) -> list[str]:
    """Run one heavy workload iteration; xplan owns isolated statistical repeats."""
    return [
        str(binary),
        f"--benchmark_filter={benchmark_filter}",
        "--benchmark_min_time=1x",
    ]


def _reset_measured_outputs(paths: list[Path]) -> None:
    """Make retries idempotent when a benchmark writes append-only diagnostics."""
    for path in paths:
        path.unlink(missing_ok=True)


def describe_process_failure(returncode: int, output: str) -> str | None:
    """Describe a failed native benchmark without flooding the xplan report."""
    if returncode == 0:
        return None

    if returncode < 0:
        signal_number = -returncode
        try:
            signal_name = signal.Signals(signal_number).name
        except ValueError:
            signal_name = f"signal {signal_number}"
        summary = (
            f"retrieval_quality benchmark terminated by {signal_name} "
            f"({returncode})"
        )
    else:
        summary = f"retrieval_quality benchmark exited with code {returncode}"

    lines = [line.strip() for line in output.splitlines() if line.strip()]
    if not lines:
        return summary

    tail = "\n".join(lines[-8:])
    if len(tail) > 1200:
        tail = "..." + tail[-1197:]
    return f"{summary}; last output:\n{tail}"


def _disable_semantic_neighbor_backfill(env: dict[str, str]) -> None:
    """Prevent the daemon from mutating benchmark topology inputs in the background."""
    env["YAMS_ENABLE_SEMANTIC_NEIGHBOR_BACKFILL"] = "0"


def _mark_shared_topology_seed_reuse(env: dict[str, str]) -> None:
    """Keep cloned topology inputs immutable while allowing per-arm reconstruction."""
    env["YAMS_BENCH_REUSE_SEEDED_TOPOLOGY_INPUTS"] = "1"
    _disable_semantic_neighbor_backfill(env)


def _truthy(val: Any) -> bool:
    return str(val).lower() in {"1", "true", "yes", "on"}


def _as_int(val: Any) -> int:
    try:
        return int(float(str(val)))
    except (TypeError, ValueError):
        return 0


def _mixed_dataset_names(raw: Any) -> list[str]:
    if raw is None:
        return []
    values = raw if isinstance(raw, list) else str(raw).split(",")
    names = [str(value).strip().lower() for value in values if str(value).strip()]
    if names and len(names) < 2:
        raise ValueError("mixed corpus benchmarks require at least two datasets")
    if len(set(names)) != len(names):
        raise ValueError("mixed corpus dataset names must be unique")
    return names


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


def parse_benchmark_setup_metrics(path: Path) -> dict[str, float]:
    """Read the structured cold-ingest/setup observation emitted by the benchmark."""
    if not path.is_file():
        return {}
    metrics: dict[str, float] = {}
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(obj, dict) or obj.get("event") != "benchmark_setup_metrics":
            continue
        ingestion = obj.get("ingestion") or {}
        if not isinstance(ingestion, dict):
            continue
        metrics = {
            "ingest_cold_performed": 1.0 if ingestion.get("cold_performed") else 0.0,
            "ingest_admission_ms": float(ingestion.get("admission_ms") or 0),
            "ingest_storage_ready_ms": float(ingestion.get("storage_ready_ms") or 0),
            "ingest_pipeline_drain_ms": float(ingestion.get("pipeline_drain_ms") or 0),
            "ingest_searchability_ready_ms": float(
                ingestion.get("searchability_ready_ms") or 0
            ),
            "ingest_enrichment_ready_ms": float(
                ingestion.get("enrichment_ready_ms") or 0
            ),
            "ingest_measurement_n": float(ingestion.get("measurement_n") or 0),
            "benchmark_setup_total_ms": float(obj.get("setup_total_ms") or 0),
        }
    return metrics


def parse_debug_jsonl(path: Path, *, top_k: int = 10) -> dict[str, Any]:
    if not path.is_file():
        return {}
    hybrid = 0
    counters = {
        "load_attempted": 0,
        "load_succeeded": 0,
        "admitted": 0,
        "applied": 0,
        "narrow_applied": 0,
        "confidence_abstained": 0,
        "snapshot_cache_hit": 0,
        "added": 0,
        "duplicate": 0,
        "stale": 0,
        "routed_docs": 0,
        "routed_clusters": 0,
        "route_available": 0,
        "rejected": 0,
        "allowed_candidates": 0,
        "vector_candidate_budget": 0,
        "vector_result_budget": 0,
        "vector_distance_evaluation_budget": 0,
        "vector_rows_visited_actual": 0,
        "vector_exact_distance_evaluations_actual": 0,
        "vector_ann_candidate_budget_actual": 0,
        "topology_vector_filter_applied_queries": 0,
        "topology_vector_filter_fallback_queries": 0,
        "topology_vector_filter_allowed_candidates": 0,
        "topology_vector_filter_matched": 0,
        "topology_vector_filter_removed": 0,
        "topology_vector_allowed_set_ann_applied_queries": 0,
        "topology_vector_allowed_set_ann_fallback_queries": 0,
        "topology_vector_global_fill_count": 0,
        "topology_shadow_evaluated_queries": 0,
        "topology_shadow_narrow_proposed_queries": 0,
        "topology_shadow_retained_candidates": 0,
        "topology_shadow_removed_candidates": 0,
        "route_representative_distance_evaluations": 0,
        "route_representative_count_max": 0,
        "route_ann_used_queries": 0,
        "route_ann_candidates": 0,
        "route_ann_distance_evaluations": 0,
        "route_exact_representative_distance_evaluations": 0,
    }
    added_vals: list[int] = []
    routed_docs: list[int] = []
    routed_clusters: list[int] = []
    route_available_vals: list[int] = []
    route_boundary_score_margin_vals: list[float] = []
    route_representative_distance_evaluation_vals: list[int] = []
    route_representative_count_max_vals: list[int] = []
    route_ann_candidate_vals: list[int] = []
    route_ann_distance_evaluation_vals: list[int] = []
    route_exact_representative_distance_evaluation_vals: list[int] = []
    structure_evidence_vals: dict[str, list[float]] = {
        "candidate_count": [],
        "scale_agreement": [],
        "overlap_support": [],
        "persistence_support": [],
        "cohesion_support": [],
        "bridge_support": [],
        "density_support": [],
    }
    allowed_candidate_vals: list[int] = []
    vector_candidate_budget_vals: list[int] = []
    vector_result_budget_vals: list[int] = []
    vector_distance_evaluation_budget_vals: list[int] = []
    vector_rows_visited_actual_vals: list[int] = []
    vector_exact_distance_evaluations_actual_vals: list[int] = []
    vector_ann_candidate_budget_actual_vals: list[int] = []
    vector_candidate_index_cache_queries = 0
    vector_candidate_index_payload_bytes_vals: list[int] = []
    vector_phase_ns_vals: dict[str, list[int]] = {
        "candidate_lookup": [],
        "candidate_projection": [],
        "pq_lut": [],
        "adc_scoring": [],
        "topk_selection": [],
        "result_materialization": [],
        "exact_rerank": [],
    }
    topology_vector_filter_matched_vals: list[int] = []
    topology_vector_filter_removed_vals: list[int] = []
    topology_vector_filter_allowed_candidate_vals: list[int] = []
    topology_vector_filter_latency_vals: list[float] = []
    topology_vector_abstain_latency_vals: list[float] = []
    topology_vector_global_fill_count_vals: list[int] = []
    skip_reasons: dict[str, int] = {}
    scoring_modes: dict[str, int] = {}
    routing_modes: dict[str, int] = {}
    construction_fingerprints: dict[str, int] = {}
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
    warming_queries = 0
    search_ready_queries = 0
    vector_ready_queries = 0
    steady_state_queries = 0
    short_query_budget_queries = 0
    effective_vector_caps: list[float] = []
    oracle_trace_queries = 0
    pre_fusion_hit_queries = 0
    post_fusion_hit_queries = 0
    returned_trace_hit_queries = 0
    ranking_loss_queries = 0
    text_relevant_queries = 0
    vector_relevant_queries = 0
    vector_unique_rescue_queries = 0
    pre_fusion_recall_sum = 0.0
    post_fusion_recall_sum = 0.0
    returned_trace_recall_sum = 0.0
    recall_at_k_ceiling_sum = 0.0

    int_keys = {
        "added": "topology_weak_query_added_candidates",
        "duplicate": "topology_weak_query_duplicate_candidates",
        "stale": "topology_weak_query_stale_candidates",
        "routed_docs": "topology_weak_query_routed_docs",
        "routed_clusters": "topology_weak_query_routed_clusters",
        "route_available": "topology_route_available_count",
        "rejected": "topology_weak_query_routes_rejected",
        "allowed_candidates": "topology_weak_query_allowed_candidates",
        "vector_candidate_budget": "vector_search_candidate_budget",
        "vector_result_budget": "vector_search_result_budget",
        "vector_distance_evaluation_budget": "vector_search_distance_evaluation_budget",
        "vector_rows_visited_actual": "vector_search_rows_visited_actual",
        "vector_exact_distance_evaluations_actual": (
            "vector_search_exact_distance_evaluations_actual"
        ),
        "vector_ann_candidate_budget_actual": "vector_search_ann_candidate_budget_actual",
        "topology_vector_filter_matched": "topology_vector_filter_matched",
        "topology_vector_filter_removed": "topology_vector_filter_removed",
        "topology_vector_global_fill_count": "topology_vector_global_fill_count",
        "topology_shadow_retained_candidates": "topology_shadow_retained_candidates",
        "topology_shadow_removed_candidates": "topology_shadow_removed_candidates",
        "route_representative_distance_evaluations": (
            "topology_route_representative_distance_evaluations"
        ),
        "route_representative_count_max": "topology_route_representative_count_max",
        "route_ann_candidates": "topology_route_ann_candidates",
        "route_ann_distance_evaluations": "topology_route_ann_distance_evaluations",
        "route_exact_representative_distance_evaluations": (
            "topology_route_exact_representative_distance_evaluations"
        ),
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
                cert_errors.append(
                    validation.get("errors") or ["unknown certificate error"]
                )
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

        warming = _truthy(stats.get("corpus_warming"))
        search_ready = _truthy(stats.get("search_engine_ready"))
        vector_ready = _truthy(stats.get("vector_ready"))
        if warming:
            warming_queries += 1
        if search_ready:
            search_ready_queries += 1
        if vector_ready:
            vector_ready_queries += 1
        if not warming and search_ready:
            steady_state_queries += 1
        if _truthy(stats.get("budget_short_query")):
            short_query_budget_queries += 1
        try:
            effective_cap = stats.get("semantic_budget_vector_cap")
            if effective_cap is not None and str(effective_cap) != "":
                effective_vector_caps.append(float(effective_cap))
        except (TypeError, ValueError):
            pass

        trace = obj.get("relevant_decision_trace") or {}
        relevant_docs = trace.get("relevant_docs") if isinstance(trace, dict) else None
        if isinstance(relevant_docs, list) and relevant_docs:
            oracle_trace_queries += 1
            pre_count = sum(bool(doc.get("in_pre_fusion")) for doc in relevant_docs)
            post_count = sum(bool(doc.get("in_post_fusion")) for doc in relevant_docs)
            returned_count = sum(bool(doc.get("in_returned_topk")) for doc in relevant_docs)
            relevant_count = len(relevant_docs)
            pre_hit = pre_count > 0
            post_hit = post_count > 0
            returned_hit = returned_count > 0
            pre_fusion_hit_queries += int(pre_hit)
            post_fusion_hit_queries += int(post_hit)
            returned_trace_hit_queries += int(returned_hit)
            ranking_loss_queries += int(pre_hit and not returned_hit)
            pre_fusion_recall_sum += float(pre_count) / float(relevant_count)
            post_fusion_recall_sum += float(post_count) / float(relevant_count)
            returned_trace_recall_sum += float(returned_count) / float(relevant_count)
            recall_at_k_ceiling_sum += float(min(max(top_k, 0), relevant_count)) / float(
                relevant_count
            )

            stage_presence = trace.get("stage_relevant_presence") or {}
            text_relevant = _truthy((stage_presence.get("text") or {}).get("any_relevant"))
            vector_relevant = _truthy(
                (stage_presence.get("vector") or {}).get("any_relevant")
            )
            text_relevant_queries += int(text_relevant)
            vector_relevant_queries += int(vector_relevant)
            vector_unique_rescue_queries += int(vector_relevant and not text_relevant)

        latency_value: float | None = None
        try:
            lat = stats.get("latency_ms")
            if lat is not None and str(lat) != "":
                latency_value = float(lat)
                latency_vals.append(latency_value)
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
                rerank_skip_reasons[rr_reason] = (
                    rerank_skip_reasons.get(rr_reason, 0) + 1
                )
            try:
                bw = stats.get("cross_rerank_blend_weight")
                if bw is not None and str(bw) != "":
                    rerank_blend_weights.append(float(bw))
            except (TypeError, ValueError):
                pass

        fusion_summary = stats.get("trace_fusion_source_summary_json")
        if fusion_summary:
            try:
                fs = (
                    json.loads(fusion_summary)
                    if isinstance(fusion_summary, str)
                    else fusion_summary
                )
                sources = fs.get("sources", fs) if isinstance(fs, dict) else {}
                for sname, sinfo in (
                    sources.items() if isinstance(sources, dict) else []
                ):
                    if not isinstance(sinfo, dict):
                        continue
                    fusion_source_mass[sname] = fusion_source_mass.get(
                        sname, 0.0
                    ) + float(sinfo.get("final_score_mass", 0.0) or 0.0)
                    fusion_source_docs[sname] = fusion_source_docs.get(
                        sname, 0.0
                    ) + float(sinfo.get("final_top_doc_count", 0.0) or 0.0)
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
        if _truthy(stats.get("topology_weak_query_narrow_applied")):
            counters["narrow_applied"] += 1
        if _truthy(stats.get("topology_route_confidence_abstained")):
            counters["confidence_abstained"] += 1
        if _truthy(stats.get("topology_snapshot_cache_hit")):
            counters["snapshot_cache_hit"] += 1
        topology_vector_filter_applied = _truthy(
            stats.get("topology_vector_filter_applied")
        )
        if topology_vector_filter_applied:
            counters["topology_vector_filter_applied_queries"] += 1
            allowed = _as_int(stats.get("topology_weak_query_allowed_candidates"))
            counters["topology_vector_filter_allowed_candidates"] += allowed
            topology_vector_filter_allowed_candidate_vals.append(allowed)
            if latency_value is not None:
                topology_vector_filter_latency_vals.append(latency_value)
        elif (
            stats.get("topology_vector_policy") == "narrow"
            and _truthy(stats.get("topology_route_confidence_abstained"))
            and latency_value is not None
        ):
            topology_vector_abstain_latency_vals.append(latency_value)
        if _truthy(stats.get("topology_vector_filter_fallback")):
            counters["topology_vector_filter_fallback_queries"] += 1
        if _truthy(stats.get("topology_vector_allowed_set_ann_applied")):
            counters["topology_vector_allowed_set_ann_applied_queries"] += 1
        if _truthy(stats.get("topology_vector_allowed_set_ann_fallback")):
            counters["topology_vector_allowed_set_ann_fallback_queries"] += 1
        if _truthy(stats.get("topology_route_ann_used")):
            counters["route_ann_used_queries"] += 1
        if _truthy(stats.get("vector_search_candidate_index_cache_used")):
            vector_candidate_index_cache_queries += 1
        vector_candidate_index_payload_bytes_vals.append(
            _as_int(stats.get("vector_search_candidate_index_payload_bytes"))
        )
        for phase_name in vector_phase_ns_vals:
            vector_phase_ns_vals[phase_name].append(
                _as_int(stats.get(f"vector_search_{phase_name}_ns"))
            )
        if _truthy(stats.get("topology_shadow_evaluated")):
            counters["topology_shadow_evaluated_queries"] += 1
            if stats.get("topology_shadow_proposed_action") == "narrow":
                counters["topology_shadow_narrow_proposed_queries"] += 1
        for dst, key in int_keys.items():
            v = _as_int(stats.get(key))
            counters[dst] += v
            if dst == "added":
                added_vals.append(v)
            elif dst == "routed_docs":
                routed_docs.append(v)
            elif dst == "routed_clusters":
                routed_clusters.append(v)
            elif dst == "route_available":
                route_available_vals.append(v)
            elif dst == "allowed_candidates":
                allowed_candidate_vals.append(v)
            elif dst == "vector_candidate_budget":
                vector_candidate_budget_vals.append(v)
            elif dst == "vector_result_budget":
                vector_result_budget_vals.append(v)
            elif dst == "vector_distance_evaluation_budget":
                vector_distance_evaluation_budget_vals.append(v)
            elif dst == "vector_rows_visited_actual":
                vector_rows_visited_actual_vals.append(v)
            elif dst == "vector_exact_distance_evaluations_actual":
                vector_exact_distance_evaluations_actual_vals.append(v)
            elif dst == "vector_ann_candidate_budget_actual":
                vector_ann_candidate_budget_actual_vals.append(v)
            elif dst == "topology_vector_filter_matched":
                topology_vector_filter_matched_vals.append(v)
            elif dst == "topology_vector_filter_removed":
                topology_vector_filter_removed_vals.append(v)
            elif dst == "topology_vector_global_fill_count":
                topology_vector_global_fill_count_vals.append(v)
            elif dst == "route_representative_distance_evaluations":
                route_representative_distance_evaluation_vals.append(v)
            elif dst == "route_representative_count_max":
                route_representative_count_max_vals.append(v)
            elif dst == "route_ann_candidates":
                route_ann_candidate_vals.append(v)
            elif dst == "route_ann_distance_evaluations":
                route_ann_distance_evaluation_vals.append(v)
            elif dst == "route_exact_representative_distance_evaluations":
                route_exact_representative_distance_evaluation_vals.append(v)

        try:
            margin = stats.get("topology_route_boundary_score_margin")
            if margin is not None and str(margin) != "":
                route_boundary_score_margin_vals.append(float(margin))
        except (TypeError, ValueError):
            pass

        for evidence_name, values in structure_evidence_vals.items():
            debug_name = f"topology_structure_{evidence_name}"
            if evidence_name != "candidate_count":
                debug_name += "_mean"
            try:
                value = stats.get(debug_name)
                if value is not None and str(value) != "":
                    values.append(float(value))
            except (TypeError, ValueError):
                pass

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
        fingerprint = str(stats.get("topology_construction_fingerprint", "") or "")
        if fingerprint:
            construction_fingerprints[fingerprint] = (
                construction_fingerprints.get(fingerprint, 0) + 1
            )

    metrics: dict[str, Any] = {
        "hybrid_queries": float(hybrid),
        "topology_load_attempted": float(counters["load_attempted"]),
        "topology_load_succeeded": float(counters["load_succeeded"]),
        "topology_admitted": float(counters["admitted"]),
        "topology_applied": float(counters["applied"]),
        "topology_narrow_applied": float(counters["narrow_applied"]),
        "topology_confidence_abstained": float(counters["confidence_abstained"]),
        "topology_snapshot_cache_hits": float(counters["snapshot_cache_hit"]),
        "topology_added_sum": float(counters["added"]),
        "topology_duplicate_sum": float(counters["duplicate"]),
        "topology_stale_sum": float(counters["stale"]),
        "topology_routed_docs_sum": float(counters["routed_docs"]),
        "topology_routed_clusters_sum": float(counters["routed_clusters"]),
        "topology_rejected_sum": float(counters["rejected"]),
        "topology_allowed_candidates_sum": float(counters["allowed_candidates"]),
        "topology_vector_filter_matched_sum": float(
            counters["topology_vector_filter_matched"]
        ),
        "topology_vector_filter_removed_sum": float(
            counters["topology_vector_filter_removed"]
        ),
        "topology_vector_global_fill_count_sum": float(
            counters["topology_vector_global_fill_count"]
        ),
        "topology_shadow_retained_candidates_sum": float(
            counters["topology_shadow_retained_candidates"]
        ),
        "topology_shadow_removed_candidates_sum": float(
            counters["topology_shadow_removed_candidates"]
        ),
        "vector_candidate_budget_sum": float(counters["vector_candidate_budget"]),
        "vector_result_budget_sum": float(counters["vector_result_budget"]),
        "vector_distance_evaluation_budget_sum": float(
            counters["vector_distance_evaluation_budget"]
        ),
        "vector_rows_visited_actual_sum": float(counters["vector_rows_visited_actual"]),
        "vector_exact_distance_evaluations_actual_sum": float(
            counters["vector_exact_distance_evaluations_actual"]
        ),
        "vector_ann_candidate_budget_actual_sum": float(
            counters["vector_ann_candidate_budget_actual"]
        ),
        "topology_cert_seen": float(cert_seen),
        "topology_cert_ok": float(cert_ok),
        "topology_construction_fingerprint_count": float(
            len(construction_fingerprints)
        ),
        "topology_route_representative_distance_evaluations_sum": float(
            counters["route_representative_distance_evaluations"]
        ),
        "topology_route_ann_candidates_sum": float(counters["route_ann_candidates"]),
        "topology_route_ann_distance_evaluations_sum": float(
            counters["route_ann_distance_evaluations"]
        ),
        "topology_route_exact_representative_distance_evaluations_sum": float(
            counters["route_exact_representative_distance_evaluations"]
        ),
    }
    for mkey, mval in (hybrid_summary_metrics or {}).items():
        try:
            fv = float(mval)
        except (TypeError, ValueError):
            continue
        metrics[f"hybrid_{mkey}"] = fv
        metrics[mkey] = (
            fv  # authoritative bare keys (fixes grep-clobber of stdout scrape)
        )
    for mkey, mval in (keyword_summary_metrics or {}).items():
        try:
            metrics[f"keyword_{mkey}"] = float(mval)
        except (TypeError, ValueError):
            continue
    metrics["cross_rerank_configured_queries"] = float(rerank_configured)
    metrics["cross_rerank_applied_queries"] = float(rerank_applied)
    if rerank_configured:
        metrics["cross_rerank_apply_rate"] = float(rerank_applied) / float(
            rerank_configured
        )
    if rerank_blend_weights:
        metrics["cross_rerank_blend_weight"] = float(
            statistics.mean(rerank_blend_weights)
        )
    if latency_vals:
        metrics["search_latency_ms_avg"] = float(statistics.mean(latency_vals))
        metrics["search_latency_ms_p50"] = float(statistics.median(latency_vals))
        sorted_lat = sorted(latency_vals)
        # Nearest-rank p95: ceil(0.95 * n)th sample (1-indexed).
        p95_idx = min(
            len(sorted_lat) - 1, max(0, math.ceil(0.95 * len(sorted_lat)) - 1)
        )
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
        metrics["topology_routed_clusters_avg"] = float(
            statistics.mean(routed_clusters)
        )
    if allowed_candidate_vals:
        metrics["topology_allowed_candidates_avg"] = float(
            statistics.mean(allowed_candidate_vals)
        )
    if vector_candidate_budget_vals:
        metrics["vector_candidate_budget_avg"] = float(
            statistics.mean(vector_candidate_budget_vals)
        )
    if vector_result_budget_vals:
        metrics["vector_result_budget_avg"] = float(
            statistics.mean(vector_result_budget_vals)
        )
    if vector_distance_evaluation_budget_vals:
        metrics["vector_distance_evaluation_budget_avg"] = float(
            statistics.mean(vector_distance_evaluation_budget_vals)
        )
    if vector_rows_visited_actual_vals:
        metrics["vector_rows_visited_actual_avg"] = float(
            statistics.mean(vector_rows_visited_actual_vals)
        )
    if vector_exact_distance_evaluations_actual_vals:
        metrics["vector_exact_distance_evaluations_actual_avg"] = float(
            statistics.mean(vector_exact_distance_evaluations_actual_vals)
        )
    if vector_ann_candidate_budget_actual_vals:
        metrics["vector_ann_candidate_budget_actual_avg"] = float(
            statistics.mean(vector_ann_candidate_budget_actual_vals)
        )
    if hybrid:
        metrics["vector_candidate_index_cache_rate"] = float(
            vector_candidate_index_cache_queries
        ) / float(hybrid)
    if vector_candidate_index_payload_bytes_vals:
        metrics["vector_candidate_index_payload_bytes_max"] = float(
            max(vector_candidate_index_payload_bytes_vals)
        )
    for phase_name, values in vector_phase_ns_vals.items():
        if values:
            metrics[f"vector_{phase_name}_ms_avg"] = float(
                statistics.mean(values)
            ) / 1_000_000.0
    if topology_vector_filter_matched_vals:
        metrics["topology_vector_filter_matched_avg"] = float(
            statistics.mean(topology_vector_filter_matched_vals)
        )
    if topology_vector_filter_removed_vals:
        metrics["topology_vector_filter_removed_avg"] = float(
            statistics.mean(topology_vector_filter_removed_vals)
        )
    metrics["topology_vector_global_fill_count_avg"] = (
        float(statistics.mean(topology_vector_global_fill_count_vals))
        if topology_vector_global_fill_count_vals
        else 0.0
    )
    metrics["topology_vector_filter_allowed_candidates_avg"] = (
        float(statistics.mean(topology_vector_filter_allowed_candidate_vals))
        if topology_vector_filter_allowed_candidate_vals
        else 0.0
    )
    metrics["topology_vector_filter_latency_ms_avg"] = (
        float(statistics.mean(topology_vector_filter_latency_vals))
        if topology_vector_filter_latency_vals
        else 0.0
    )
    metrics["topology_vector_abstain_latency_ms_avg"] = (
        float(statistics.mean(topology_vector_abstain_latency_vals))
        if topology_vector_abstain_latency_vals
        else 0.0
    )
    if route_available_vals:
        metrics["topology_route_available_avg"] = float(
            statistics.mean(route_available_vals)
        )
    if route_boundary_score_margin_vals:
        metrics["topology_route_boundary_score_margin_avg"] = float(
            statistics.mean(route_boundary_score_margin_vals)
        )
    if route_representative_distance_evaluation_vals:
        metrics["topology_route_representative_distance_evaluations_avg"] = float(
            statistics.mean(route_representative_distance_evaluation_vals)
        )
    if route_representative_count_max_vals:
        metrics["topology_route_representative_count_max"] = float(
            max(route_representative_count_max_vals)
        )
    metrics["topology_route_ann_candidates_avg"] = (
        float(statistics.mean(route_ann_candidate_vals)) if route_ann_candidate_vals else 0.0
    )
    metrics["topology_route_ann_distance_evaluations_avg"] = (
        float(statistics.mean(route_ann_distance_evaluation_vals))
        if route_ann_distance_evaluation_vals
        else 0.0
    )
    metrics["topology_route_exact_representative_distance_evaluations_avg"] = (
        float(statistics.mean(route_exact_representative_distance_evaluation_vals))
        if route_exact_representative_distance_evaluation_vals
        else 0.0
    )
    for evidence_name, values in structure_evidence_vals.items():
        metrics[f"topology_structure_{evidence_name}_avg"] = (
            float(statistics.mean(values)) if values else 0.0
        )
    if hybrid > 0:
        metrics["corpus_warming_rate"] = float(warming_queries) / float(hybrid)
        metrics["search_engine_ready_rate"] = float(search_ready_queries) / float(hybrid)
        metrics["vector_ready_rate"] = float(vector_ready_queries) / float(hybrid)
        metrics["steady_state_query_rate"] = float(steady_state_queries) / float(hybrid)
        metrics["short_query_budget_rate"] = float(short_query_budget_queries) / float(hybrid)
        metrics["vector_candidate_work_budget_avg"] = float(
            counters["vector_candidate_budget"]
            + counters["topology_vector_filter_allowed_candidates"]
        ) / float(hybrid)
        metrics["vector_total_rows_visited_actual_avg"] = float(
            counters["vector_rows_visited_actual"]
        ) / float(hybrid)
        metrics["vector_total_exact_distance_evaluations_actual_avg"] = float(
            counters["vector_exact_distance_evaluations_actual"]
        ) / float(hybrid)
        metrics["topology_vector_filter_rate"] = float(
            counters["topology_vector_filter_applied_queries"]
        ) / float(hybrid)
        metrics["topology_vector_filter_fallback_rate"] = float(
            counters["topology_vector_filter_fallback_queries"]
        ) / float(hybrid)
        metrics["topology_vector_allowed_set_ann_rate"] = float(
            counters["topology_vector_allowed_set_ann_applied_queries"]
        ) / float(hybrid)
        metrics["topology_vector_allowed_set_ann_fallback_rate"] = float(
            counters["topology_vector_allowed_set_ann_fallback_queries"]
        ) / float(hybrid)
        metrics["topology_route_ann_rate"] = float(
            counters["route_ann_used_queries"]
        ) / float(hybrid)
        metrics["topology_shadow_evaluation_rate"] = float(
            counters["topology_shadow_evaluated_queries"]
        ) / float(hybrid)
    shadow_evaluated = counters["topology_shadow_evaluated_queries"]
    if shadow_evaluated > 0:
        metrics["topology_shadow_narrow_proposal_rate"] = float(
            counters["topology_shadow_narrow_proposed_queries"]
        ) / float(shadow_evaluated)
        metrics["topology_shadow_retained_candidates_avg"] = float(
            counters["topology_shadow_retained_candidates"]
        ) / float(shadow_evaluated)
        metrics["topology_shadow_removed_candidates_avg"] = float(
            counters["topology_shadow_removed_candidates"]
        ) / float(shadow_evaluated)
    else:
        metrics["topology_shadow_narrow_proposal_rate"] = 0.0
        metrics["topology_shadow_retained_candidates_avg"] = 0.0
        metrics["topology_shadow_removed_candidates_avg"] = 0.0
    if effective_vector_caps:
        metrics["effective_vector_cap_min"] = float(min(effective_vector_caps))
        metrics["effective_vector_cap_max"] = float(max(effective_vector_caps))
        metrics["effective_vector_cap_avg"] = float(statistics.mean(effective_vector_caps))
    if oracle_trace_queries > 0:
        trace_count = float(oracle_trace_queries)
        returned_recall = returned_trace_recall_sum / trace_count
        recall_ceiling = recall_at_k_ceiling_sum / trace_count
        metrics["oracle_trace_coverage"] = trace_count / float(hybrid)
        metrics["candidate_pre_fusion_hit_rate"] = float(pre_fusion_hit_queries) / trace_count
        metrics["candidate_post_fusion_hit_rate"] = float(post_fusion_hit_queries) / trace_count
        metrics["returned_trace_hit_rate"] = float(returned_trace_hit_queries) / trace_count
        metrics["ranking_loss_query_rate"] = float(ranking_loss_queries) / trace_count
        metrics["text_relevant_query_rate"] = float(text_relevant_queries) / trace_count
        metrics["vector_relevant_query_rate"] = float(vector_relevant_queries) / trace_count
        metrics["vector_unique_rescue_rate"] = float(vector_unique_rescue_queries) / trace_count
        metrics["candidate_pre_fusion_recall"] = pre_fusion_recall_sum / trace_count
        metrics["candidate_post_fusion_recall"] = post_fusion_recall_sum / trace_count
        metrics["returned_trace_recall"] = returned_recall
        metrics["recall_at_k_ceiling"] = recall_ceiling
        metrics["ceiling_normalized_returned_recall"] = (
            returned_recall / recall_ceiling if recall_ceiling > 0.0 else 0.0
        )
    if hybrid > 0:
        metrics["topology_narrow_rate"] = float(counters["narrow_applied"]) / float(
            hybrid
        )
        metrics["topology_confidence_abstain_rate"] = float(
            counters["confidence_abstained"]
        ) / float(hybrid)
        metrics["topology_snapshot_cache_hit_rate"] = float(
            counters["snapshot_cache_hit"]
        ) / float(hybrid)

    # Path / seed-ANN mechanism rates (for quality×cost loops).
    if hybrid > 0:
        metrics["topology_path_medoid_rate"] = float(path_medoid) / float(hybrid)
        metrics["topology_path_seed_neighbors_rate"] = float(
            path_seed_neighbors
        ) / float(hybrid)
    if vector_seeds_added_vals:
        metrics["topology_vector_seeds_added_avg"] = float(
            statistics.mean(vector_seeds_added_vals)
        )
        metrics["topology_vector_seeds_added_max"] = float(max(vector_seeds_added_vals))
        metrics["topology_vector_seeds_nonzero_rate"] = float(
            sum(1 for v in vector_seeds_added_vals if v > 0)
        ) / float(len(vector_seeds_added_vals))
    else:
        metrics["topology_vector_seeds_added_avg"] = 0.0
        metrics["topology_vector_seeds_added_max"] = 0.0
        metrics["topology_vector_seeds_nonzero_rate"] = 0.0
    if vector_seed_probe_vals:
        metrics["topology_vector_seed_probe_avg"] = float(
            statistics.mean(vector_seed_probe_vals)
        )
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
        "construction_fingerprints": construction_fingerprints,
        "certificate": {
            "seen": cert_seen,
            "ok": cert_ok,
            "errors": cert_errors[:5],
        },
    }


def require_steady_state(path: Path, *, require_vector: bool) -> str | None:
    """Return an experiment-identity error when hybrid queries ran while warming."""
    if not path.is_file():
        return f"missing debug log for steady-state validation: {path}"
    hybrid = 0
    not_steady = 0
    warming = 0
    search_not_ready = 0
    vector_not_ready = 0
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(event, dict) or event.get("search_type") != "hybrid":
            continue
        hybrid += 1
        stats = event.get("search_stats") or {}
        is_warming = _truthy(stats.get("corpus_warming"))
        search_ready = _truthy(stats.get("search_engine_ready"))
        vector_ready = _truthy(stats.get("vector_ready"))
        warming += int(is_warming)
        search_not_ready += int(not search_ready)
        vector_not_ready += int(require_vector and not vector_ready)
        not_steady += int(
            is_warming or not search_ready or (require_vector and not vector_ready)
        )
    if hybrid == 0:
        return "steady-state validation found no hybrid query traces"
    if not_steady == 0:
        return None
    return (
        f"{not_steady}/{hybrid} hybrid queries were not steady-state "
        f"(warming={warming}, search_not_ready={search_not_ready}, "
        f"vector_not_ready={vector_not_ready}, require_vector={require_vector})"
    )


def require_shared_topology_construction_identity(
    identity_path: Path, fingerprints: dict[str, int]
) -> str | None:
    """Pin every repeat and arm to the first observed topology construction."""
    if len(fingerprints) != 1:
        return (
            "expected exactly one topology construction fingerprint, "
            f"observed {fingerprints}"
        )
    observed = next(iter(fingerprints))
    if identity_path.is_file():
        expected = identity_path.read_text(encoding="utf-8").strip()
        if expected == observed:
            return None
        return (
            "shared topology construction identity mismatch: "
            f"expected {expected!r}, observed {observed!r}"
        )
    identity_path.parent.mkdir(parents=True, exist_ok=True)
    identity_path.write_text(observed + "\n", encoding="utf-8")
    return None


def clone_benchmark_state(source: Path, destination: Path) -> None:
    """Clone an immutable prepared store into one isolated measured workload."""
    if not source.is_dir():
        raise ValueError(f"benchmark state seed is missing: {source}")
    if destination.exists():
        shutil.rmtree(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(source, destination, symlinks=True)


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
        return (
            f"missing topology certificate debug log for source={source}: {debug_path}"
        )
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
            bad.append(
                validation.get("errors") or ["unknown certificate validation error"]
            )
    if seen == 0:
        return f"missing topology construction certificate for source={source}"
    if bad:
        return (
            f"invalid topology construction certificate for source={source}: {bad[0]}"
        )
    return None


def _merge_benchmark_env(
    *,
    ambient: dict[str, str],
    declared_env: dict[str, str],
    params: dict[str, Any],
    param_env: dict[str, str],
) -> dict[str, str]:
    """Merge benchmark configuration with deterministic experiment precedence.

    Plan parameters override ambient shell state. Explicit fixed/arm/step env is
    authoritative when a plan intentionally expresses the same axis as an env var.
    """
    env = dict(ambient)
    for pkey, ekey in param_env.items():
        if pkey in params:
            env[ekey] = str(params[pkey])
    env.update(declared_env)
    # The benchmark writes a per-process config and DaemonHarness makes it
    # authoritative. Never let a developer's ambient config shadow that file.
    env["YAMS_CONFIG_PATH"] = ""
    return env


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
    topology_cluster_path = ctx.arm_dir / "topology_clusters.json"
    _reset_measured_outputs([debug_path, topology_cluster_path])

    arm_factors = getattr(ctx.arm, "arm", None)
    arm_factors = getattr(arm_factors, "factors", None) or {}
    expansion_arm = (
        ctx.params.get("expansion_arm")
        or ctx.params.get("arm")
        or arm_factors.get("expansion_arm")
    )

    try:
        mixed_dataset_names = _mixed_dataset_names(ctx.params.get("datasets"))
    except ValueError as exc:
        return WorkerResult(status="failed", exit_code=2, metrics={}, message=str(exc))

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
        write_json(topology_cluster_path, {"dry_run": True, "memberships": []})
        # Zero-fill KPIs listed in plan step.metrics so dry-run validation passes.
        dry_run_metrics = {
            f"mixed_{metric}_{dataset.replace('-', '_')}": 0.0
            for dataset in mixed_dataset_names
            for metric in (
                "query_count",
                "mrr",
                "recall_at_k",
                "hit_rate",
                "topology_route_source_purity",
                "topology_route_cross_source_rate",
                "topology_relevant_fragment_coverage",
                "topology_relevant_fragment_hit_rate",
            )
        }
        dry_sources = sorted(
            re.sub(r"[^a-z0-9]+", "_", dataset.lower()).strip("_")
            for dataset in mixed_dataset_names
        )
        for source in dry_sources:
            dry_run_metrics[f"mixed_cluster_count_{source}"] = 0.0
            dry_run_metrics[f"mixed_cluster_exclusive_count_{source}"] = 0.0
            dry_run_metrics[f"mixed_cluster_cross_source_exposure_{source}"] = 0.0
            for metric in (
                "candidate_pre_fusion_hit_rate",
                "candidate_pre_fusion_recall",
                "candidate_post_fusion_hit_rate",
                "candidate_post_fusion_recall",
                "returned_trace_hit_rate",
                "returned_trace_recall",
                "ranking_loss_query_rate",
                "vector_unique_rescue_rate",
            ):
                dry_run_metrics[f"mixed_{metric}_{source}"] = 0.0
        for left_index, left in enumerate(dry_sources):
            for right in dry_sources[left_index + 1 :]:
                dry_run_metrics[f"mixed_cluster_overlap_jaccard_{left}_{right}"] = 0.0
        # Plans can grow instrumentation without requiring a parallel dry-run registry.
        dry_run_metrics.update({name: 0.0 for name in ctx.step.metrics})
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
                "proc_maxrss_mb": 0.0,
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
                "topology_narrow_rate": 0.0,
                "topology_confidence_abstain_rate": 0.0,
                "topology_route_available_avg": 0.0,
                "topology_route_boundary_score_margin_avg": 0.0,
                "topology_route_representative_distance_evaluations_avg": 0.0,
                "topology_route_representative_count_max": 0.0,
                "topology_route_ann_rate": 0.0,
                "topology_route_ann_candidates_avg": 0.0,
                "topology_route_ann_distance_evaluations_avg": 0.0,
                "topology_route_exact_representative_distance_evaluations_avg": 0.0,
                "topology_construction_fingerprint_count": 0.0,
                "topology_structure_candidate_count_avg": 0.0,
                "topology_structure_scale_agreement_avg": 0.0,
                "topology_structure_overlap_support_avg": 0.0,
                "topology_structure_persistence_support_avg": 0.0,
                "topology_structure_cohesion_support_avg": 0.0,
                "topology_structure_bridge_support_avg": 0.0,
                "topology_structure_density_support_avg": 0.0,
                "topology_snapshot_cache_hit_rate": 0.0,
                "topology_allowed_candidates_avg": 0.0,
                "vector_candidate_budget_avg": 0.0,
                "vector_result_budget_avg": 0.0,
                "vector_candidate_work_budget_avg": 0.0,
                "vector_distance_evaluation_budget_avg": 0.0,
                "vector_rows_visited_actual_avg": 0.0,
                "vector_exact_distance_evaluations_actual_avg": 0.0,
                "vector_ann_candidate_budget_actual_avg": 0.0,
                "vector_candidate_index_cache_rate": 0.0,
                "vector_candidate_index_payload_bytes_max": 0.0,
                "vector_candidate_lookup_ms_avg": 0.0,
                "vector_candidate_projection_ms_avg": 0.0,
                "vector_pq_lut_ms_avg": 0.0,
                "vector_adc_scoring_ms_avg": 0.0,
                "vector_topk_selection_ms_avg": 0.0,
                "vector_result_materialization_ms_avg": 0.0,
                "vector_exact_rerank_ms_avg": 0.0,
                "vector_total_rows_visited_actual_avg": 0.0,
                "vector_total_exact_distance_evaluations_actual_avg": 0.0,
                "topology_vector_filter_rate": 0.0,
                "topology_vector_filter_fallback_rate": 0.0,
                "topology_vector_filter_matched_avg": 0.0,
                "topology_vector_filter_removed_avg": 0.0,
                "topology_vector_global_fill_count_avg": 0.0,
                "topology_vector_filter_allowed_candidates_avg": 0.0,
                "topology_vector_filter_latency_ms_avg": 0.0,
                "topology_vector_abstain_latency_ms_avg": 0.0,
                "topology_vector_allowed_set_ann_rate": 0.0,
                "topology_vector_allowed_set_ann_fallback_rate": 0.0,
                "topology_shadow_evaluation_rate": 0.0,
                "topology_shadow_narrow_proposal_rate": 0.0,
                "topology_shadow_retained_candidates_avg": 0.0,
                "topology_shadow_removed_candidates_avg": 0.0,
                "corpus_warming_rate": 0.0,
                "search_engine_ready_rate": 0.0,
                "vector_ready_rate": 0.0,
                "steady_state_query_rate": 0.0,
                "short_query_budget_rate": 0.0,
                "effective_vector_cap_min": 0.0,
                "effective_vector_cap_max": 0.0,
                "effective_vector_cap_avg": 0.0,
                "oracle_trace_coverage": 0.0,
                "candidate_pre_fusion_hit_rate": 0.0,
                "candidate_post_fusion_hit_rate": 0.0,
                "returned_trace_hit_rate": 0.0,
                "ranking_loss_query_rate": 0.0,
                "text_relevant_query_rate": 0.0,
                "vector_relevant_query_rate": 0.0,
                "vector_unique_rescue_rate": 0.0,
                "candidate_pre_fusion_recall": 0.0,
                "candidate_post_fusion_recall": 0.0,
                "returned_trace_recall": 0.0,
                "recall_at_k_ceiling": 0.0,
                "ceiling_normalized_returned_recall": 0.0,
                "mixed_cross_source_result_rate": 0.0,
                "mixed_cross_source_top1_rate": 0.0,
                "mixed_topology_route_source_purity": 0.0,
                "mixed_topology_route_cross_source_rate": 0.0,
                "mixed_topology_relevant_fragment_coverage": 0.0,
                "mixed_topology_relevant_fragment_hit_rate": 0.0,
                "mixed_post_fusion_cross_source_result_rate": 0.0,
                "mixed_post_fusion_cross_source_top1_rate": 0.0,
                "mixed_source_macro_mrr": 0.0,
                "mixed_source_min_mrr": 0.0,
                "mixed_source_macro_recall_at_k": 0.0,
                "mixed_source_min_recall_at_k": 0.0,
                "mixed_cluster_count": 0.0,
                "mixed_cluster_analyzable_count": 0.0,
                "mixed_cluster_singleton_rate": 0.0,
                "mixed_cluster_intrinsic_shared_document_rate": 0.0,
                "mixed_cluster_shared_count": 0.0,
                "mixed_cluster_shared_rate": 0.0,
                "mixed_cluster_shared_document_rate": 0.0,
                "mixed_cluster_weighted_source_purity": 0.0,
                "mixed_cluster_weighted_source_entropy_bits": 0.0,
                "mixed_cluster_topology_overlap_membership_rate": 0.0,
                **dry_run_metrics,
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
    skip_build = (
        bool(ctx.params.get("skip_build"))
        or os.environ.get("YAMS_BENCH_SKIP_BUILD") == "1"
    )
    force_build = (
        bool(ctx.params.get("force_build"))
        or os.environ.get("YAMS_BENCH_FORCE_BUILD") == "1"
    )
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

    mixed_corpus: PreparedMixedCorpus | None = None
    if mixed_dataset_names:
        dataset_roots: dict[str, Path] = {}
        try:
            for dataset in mixed_dataset_names:
                if not is_beir_dataset(dataset):
                    raise ValueError(f"unknown BEIR dataset in mixed corpus: {dataset}")
                dataset_roots[dataset] = ensure_beir_dataset(
                    dataset,
                    download=str(ctx.params.get("download_beir", "1")).lower()
                    not in {"0", "false", "no", "off"},
                )
            mixed_corpus = materialize_mixed_beir_manifest(
                dataset_roots,
                ctx.arm.run_dir / "datasets" / "mixed_beir",
                documents_per_dataset=_as_int(ctx.params.get("documents_per_dataset")),
                queries_per_dataset=_as_int(ctx.params.get("queries_per_dataset")),
            )
        except Exception as exc:  # noqa: BLE001
            return WorkerResult(
                status="failed",
                exit_code=2,
                metrics={},
                attributes={"datasets": mixed_dataset_names},
                message=f"failed to prepare mixed corpus: {exc}",
            )
        ctx.params["dataset"] = "local-manifest"
        ctx.params["dataset_path"] = str(mixed_corpus.manifest_path.parent)
        ctx.params["corpus_size"] = 0
        ctx.params["num_queries"] = 0

    param_env = {
        "corpus_size": "YAMS_BENCH_CORPUS_SIZE",
        "num_queries": "YAMS_BENCH_NUM_QUERIES",
        "topk": "YAMS_BENCH_TOPK",
        "dataset": "YAMS_BENCH_DATASET",
        "dataset_path": "YAMS_BENCH_DATASET_PATH",
        "topology_mode": "YAMS_BENCH_TOPOLOGY_MODE",
        "topology_vector_policy": "YAMS_BENCH_TOPOLOGY_VECTOR_POLICY",
        "topology_engine": "YAMS_BENCH_TOPOLOGY_ENGINE",
        "topology_routing_representatives": (
            "YAMS_BENCH_TOPOLOGY_ROUTING_REPRESENTATIVES"
        ),
        "topology_route_representative_limit": (
            "YAMS_BENCH_TOPOLOGY_ROUTE_REPRESENTATIVE_LIMIT"
        ),
        "topology_route_ann_candidate_limit": (
            "YAMS_BENCH_TOPOLOGY_ROUTE_ANN_CANDIDATE_LIMIT"
        ),
        "topology_source": "YAMS_BENCH_TOPOLOGY_SOURCE",
        "route_scoring": "YAMS_BENCH_TOPOLOGY_ROUTE_SCORING",
        "sparse_dense_alpha": "YAMS_BENCH_TOPOLOGY_SPARSE_DENSE_ALPHA",
        "min_route_score": "YAMS_BENCH_TOPOLOGY_MIN_ROUTE_SCORE",
        "min_clusters": "YAMS_BENCH_TOPOLOGY_MIN_CLUSTERS",
        "max_docs": "YAMS_BENCH_TOPOLOGY_MAX_DOCS",
        "max_clusters": "YAMS_BENCH_TOPOLOGY_MAX_CLUSTERS",
        "max_seed_documents": "YAMS_BENCH_TOPOLOGY_MAX_SEED_DOCUMENTS",
        "adaptive_probe_score_gap": "YAMS_BENCH_TOPOLOGY_ADAPTIVE_PROBE_SCORE_GAP",
        "narrow_min_boundary_margin": "YAMS_BENCH_TOPOLOGY_NARROW_MIN_BOUNDARY_MARGIN",
        "max_component_docs": "YAMS_BENCH_TOPOLOGY_MAX_COMPONENT_DOCS",
        "topology_boundary_spill": "YAMS_BENCH_TOPOLOGY_BOUNDARY_SPILL",
        "topology_boundary_spill_limit": "YAMS_BENCH_TOPOLOGY_BOUNDARY_SPILL_LIMIT",
        "topology_boundary_spill_distance_ratio": (
            "YAMS_BENCH_TOPOLOGY_BOUNDARY_SPILL_DISTANCE_RATIO"
        ),
        "topology_boundary_spill_residual_penalty": (
            "YAMS_BENCH_TOPOLOGY_BOUNDARY_SPILL_RESIDUAL_PENALTY"
        ),
        "min_edge_score": "YAMS_TOPOLOGY_MIN_EDGE_SCORE",
        "expansion_source": "YAMS_BENCH_TOPOLOGY_EXPANSION",
        "seed_semantic_neighbors": "YAMS_BENCH_SEED_SEMANTIC_NEIGHBORS",
        "seed_semantic_topk": "YAMS_BENCH_SEED_SEMANTIC_TOPK",
        "seed_semantic_threshold": "YAMS_BENCH_SEED_SEMANTIC_THRESHOLD",
        "graph_neighbor_min_score": "YAMS_SEARCH_TOPOLOGY_GRAPH_NEIGHBOR_MIN_SCORE",
        "graph_neighbor_reciprocal_only": "YAMS_SEARCH_TOPOLOGY_GRAPH_NEIGHBOR_RECIPROCAL_ONLY",
        "enable_reranking": "YAMS_SEARCH_ENABLE_RERANKING",
        "rerank_topk": "YAMS_SEARCH_RERANK_TOPK",
        "rerank_replace_scores": "YAMS_SEARCH_RERANK_REPLACE_SCORES",
        "rerank_weight": "YAMS_SEARCH_RERANK_WEIGHT",
        "embed_backend": "YAMS_EMBED_BACKEND",
        "vector_search_engine": "YAMS_VECTOR_SEARCH_ENGINE",
        "ingest_min_fraction": "YAMS_BENCH_INGEST_MIN_FRACTION",
        "progress_timeout_sec": "YAMS_BENCH_PROGRESS_TIMEOUT",
    }
    env = _merge_benchmark_env(
        ambient=os.environ,
        declared_env=ctx.env,
        params=ctx.params,
        param_env=param_env,
    )
    env.setdefault("YAMS_TEST_SAFE_SINGLE_INSTANCE", "1")
    env.setdefault("YAMS_SEARCH_STAGE_TRACE", "1")

    shared_warm_cache = str(ctx.params.get("shared_warm_cache") or "").strip()
    shared_state_root: Path | None = None
    if shared_warm_cache:
        safe_cache_name = re.sub(r"[^A-Za-z0-9_.-]+", "_", shared_warm_cache)
        shared_state_root = ctx.arm.run_dir / "shared_state" / safe_cache_name

    # Default topology scoring knobs (match former common_env).
    env.setdefault("YAMS_BENCH_TOPOLOGY_ROUTE_SCORING", "current")
    env.setdefault("YAMS_BENCH_TOPOLOGY_SPARSE_DENSE_ALPHA", "0.5")
    env.setdefault("YAMS_BENCH_TOPOLOGY_MIN_ROUTE_SCORE", "0")
    env.setdefault("YAMS_SEARCH_TOPOLOGY_MAX_CLUSTERS", "2")
    env.setdefault("YAMS_SEARCH_TOPOLOGY_MAX_DOCS_PER_CLUSTER", "0")

    # Construction purity: mirror bench cert cap into TopologyManager build env.
    if (
        "YAMS_BENCH_TOPOLOGY_MAX_COMPONENT_DOCS" in env
        and "YAMS_TOPOLOGY_MAX_COMPONENT_DOCS" not in env
    ):
        env["YAMS_TOPOLOGY_MAX_COMPONENT_DOCS"] = env[
            "YAMS_BENCH_TOPOLOGY_MAX_COMPONENT_DOCS"
        ]
    if (
        "YAMS_BENCH_TOPOLOGY_MIN_EDGE_SCORE" in env
        and "YAMS_TOPOLOGY_MIN_EDGE_SCORE" not in env
    ):
        env["YAMS_TOPOLOGY_MIN_EDGE_SCORE"] = env["YAMS_BENCH_TOPOLOGY_MIN_EDGE_SCORE"]
    # Search expansion_source also as typed SearchEngine overlay.
    if (
        "YAMS_BENCH_TOPOLOGY_EXPANSION" in env
        and "YAMS_SEARCH_TOPOLOGY_EXPANSION_SOURCE" not in env
    ):
        env["YAMS_SEARCH_TOPOLOGY_EXPANSION_SOURCE"] = env[
            "YAMS_BENCH_TOPOLOGY_EXPANSION"
        ]

    # Factors often carry topology_source / expansion_arm.
    if "topology_source" in arm_factors and "YAMS_BENCH_TOPOLOGY_SOURCE" not in env:
        env["YAMS_BENCH_TOPOLOGY_SOURCE"] = str(arm_factors["topology_source"])

    # Shared ablation axes (component weights, topology, rerank, vectors, …).
    ablation = apply_ablation(env, factors=arm_factors, params=ctx.params)

    applied_preset = apply_expansion_preset(
        env, str(expansion_arm) if expansion_arm else None
    )
    if (
        expansion_arm
        and not applied_preset
        and str(expansion_arm) not in EXPANSION_PRESETS
    ):
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
    env.setdefault(
        "YAMS_BENCH_TOPOLOGY_ENGINE",
        str(ctx.params.get("topology_engine", "connected")),
    )
    env.setdefault(
        "YAMS_BENCH_TOPOLOGY_SOURCE", str(ctx.params.get("topology_source", "vector"))
    )
    env["YAMS_BENCH_DEBUG_FILE"] = str(debug_path)
    env["YAMS_BENCH_TOPOLOGY_CLUSTER_OUTPUT"] = str(topology_cluster_path)

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
    cmd = _benchmark_command(binary, bench_filter)

    timeout = ctx.step.timeout_sec or int(
        ctx.params.get("timeout_sec") or ctx.arm.plan.timeout_sec
    )
    # Let the bench's own ingest-stall logic (PROGRESS_TIMEOUT) fire before we kill the process.
    progress_timeout = int(env.get("YAMS_BENCH_PROGRESS_TIMEOUT", "0") or "0")
    timeout = max(timeout, progress_timeout + 120)
    seed_setup_metrics: dict[str, float] = {}
    if shared_state_root is not None:
        seed_dir = shared_state_root / "seed"
        ready_marker = shared_state_root / "seed_ready"
        if not ready_marker.is_file():
            shared_state_root.mkdir(parents=True, exist_ok=True)
            prime_env = dict(env)
            if source == "vector":
                _disable_semantic_neighbor_backfill(prime_env)
            prime_env["YAMS_BENCH_WARM_CACHE_DIR"] = str(seed_dir)
            prime_env["YAMS_BENCH_DEBUG_FILE"] = str(shared_state_root / "prime_debug.jsonl")
            prime_env["YAMS_BENCH_TOPOLOGY_CLUSTER_OUTPUT"] = str(
                shared_state_root / "prime_topology_clusters.json"
            )
            try:
                prime = run_captured(
                    cmd,
                    cwd=ctx.repo_root,
                    env=prime_env,
                    timeout=timeout,
                    stdout_path=shared_state_root / "prime_stdout.log",
                    stderr_path=shared_state_root / "prime_time.log",
                )
            except Exception as exc:  # noqa: BLE001
                return WorkerResult(
                    status="failed",
                    exit_code=124,
                    metrics={},
                    message=f"retrieval_quality seed preparation timed out: {exc}",
                )
            if prime.returncode != 0:
                return WorkerResult(
                    status="failed",
                    exit_code=prime.returncode or 1,
                    metrics={},
                    message=(
                        "retrieval_quality seed preparation failed; see "
                        f"{shared_state_root / 'prime_stdout.log'}"
                    ),
                )
            ready_marker.write_text("ready\n", encoding="utf-8")
        prime_debug_path = shared_state_root / "prime_debug.jsonl"
        seed_setup_metrics = parse_benchmark_setup_metrics(prime_debug_path)
        if seed_setup_metrics:
            write_json(
                shared_state_root / "seed_ingestion_metrics.json",
                {"metrics": seed_setup_metrics},
            )
        isolated_data_dir = ctx.arm_dir / "isolated_data"
        try:
            clone_benchmark_state(seed_dir, isolated_data_dir)
        except (OSError, ValueError) as exc:
            return WorkerResult(
                status="failed",
                exit_code=2,
                metrics={},
                message=f"failed to clone benchmark state seed: {exc}",
            )
        env.pop("YAMS_BENCH_DATA_DIR", None)
        env["YAMS_BENCH_WARM_CACHE_DIR"] = str(isolated_data_dir)
        if source == "vector":
            _mark_shared_topology_seed_reuse(env)
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

    (ctx.arm_dir / "exit_code").write_text(
        str(proc.returncode) + "\n", encoding="utf-8"
    )
    combined = (
        stdout_path.read_text(encoding="utf-8", errors="replace")
        if stdout_path.exists()
        else ""
    )
    combined += "\n"
    combined += (
        stderr_path.read_text(encoding="utf-8", errors="replace")
        if stderr_path.exists()
        else ""
    )

    process_failure = describe_process_failure(proc.returncode, combined)
    if process_failure:
        return WorkerResult(
            status="failed",
            exit_code=proc.returncode,
            metrics={},
            attributes={
                "binary": str(binary),
                "debug_file": str(debug_path),
                "stdout_file": str(stdout_path),
                "stderr_file": str(stderr_path),
            },
            message=process_failure,
            raw_path=str(stderr_path if stderr_path.exists() else stdout_path),
        )

    quality = parse_quality_from_text(combined)
    dbg = parse_debug_jsonl(debug_path, top_k=_as_int(env.get("YAMS_BENCH_TOPK", "10")))
    metrics: dict[str, Any] = {
        "mrr": float(quality.get("mrr", 0.0)),
        "recall_at_k": float(quality.get("recall_at_k", 0.0)),
        "precision_at_k": float(quality.get("precision_at_k", 0.0)),
        "ndcg_at_k": float(quality.get("ndcg_at_k", 0.0)),
        "map": float(quality.get("map", 0.0)),
    }
    metrics.update(quality)
    metrics.update(dbg.get("metrics") or {})
    metrics.update(parse_benchmark_setup_metrics(debug_path))
    metrics.update({f"seed_{key}": value for key, value in seed_setup_metrics.items()})
    if mixed_corpus is not None:
        metrics.update(
            analyze_mixed_corpus_debug(
                debug_path,
                mixed_corpus.identity_path,
                top_k=_as_int(env.get("YAMS_BENCH_TOPK", "10")),
            )
        )
        cluster_report = analyze_mixed_cluster_overlap(
            topology_cluster_path, mixed_corpus.identity_path
        )
        metrics.update(cluster_report.get("metrics") or {})
        write_json(ctx.arm_dir / "mixed_cluster_overlap.json", cluster_report)
    metrics["proc_cpu_sec"] = float(proc_cpu_sec)
    metrics["proc_wall_sec"] = float(wall_sec)
    metrics["proc_maxrss_mb"] = float(proc_maxrss_mb)

    seed_enabled = _truthy(env.get("YAMS_BENCH_SEED_SEMANTIC_NEIGHBORS", "0"))
    require_steady = _truthy(
        ctx.params.get(
            "require_steady_state", env.get("YAMS_BENCH_REQUIRE_STEADY_STATE", "1")
        )
    )
    vectors_disabled = _truthy(env.get("YAMS_DISABLE_VECTORS", "0"))
    steady_state_error = (
        require_steady_state(debug_path, require_vector=not vectors_disabled)
        if require_steady
        else None
    )
    if steady_state_error:
        write_json(
            ctx.arm_dir / "quality_parse.json",
            {
                "quality": quality,
                "debug": dbg,
                "steady_state_error": steady_state_error,
            },
        )
        return WorkerResult(
            status="failed",
            exit_code=proc.returncode if proc.returncode != 0 else 1,
            metrics=metrics,
            attributes={
                "binary": str(binary),
                "topology_source": source,
                "steady_state_error": steady_state_error,
            },
            message=steady_state_error,
            raw_path=str(debug_path),
        )
    require_cross_arm_identity = _truthy(
        ctx.params.get("require_topology_construction_identity", False)
    )
    require_arm_stability = _truthy(
        ctx.params.get("require_topology_construction_stability", False)
    )
    if require_cross_arm_identity or require_arm_stability:
        fingerprints = dbg.get("construction_fingerprints") or {}
        identity_error = None
        if require_arm_stability:
            arm_identity_path = (
                ctx.arm.run_dir
                / "topology_construction_identity"
                / f"{ctx.arm.arm.safe_name}.txt"
            )
            if ctx.arm_dir.name == "rep00" or ctx.arm_dir.name == ctx.arm.arm.safe_name:
                arm_identity_path.unlink(missing_ok=True)
            identity_error = require_shared_topology_construction_identity(
                arm_identity_path,
                fingerprints,
            )
        elif shared_warm_cache:
            identity_error = require_shared_topology_construction_identity(
                ctx.arm.run_dir
                / "shared_state"
                / safe_cache_name
                / "topology_construction_fingerprint.txt",
                fingerprints,
            )
        elif len(fingerprints) != 1:
            identity_error = (
                "expected exactly one topology construction fingerprint, "
                f"observed {fingerprints}"
            )
        if identity_error:
            write_json(
                ctx.arm_dir / "quality_parse.json",
                {
                    "quality": quality,
                    "debug": dbg,
                    "topology_construction_identity_error": identity_error,
                },
            )
            return WorkerResult(
                status="failed",
                exit_code=proc.returncode if proc.returncode != 0 else 1,
                metrics=metrics,
                attributes={
                    "binary": str(binary),
                    "topology_construction_identity_error": identity_error,
                },
                message=identity_error,
                raw_path=str(debug_path),
            )
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
            "debug": {k: v for k, v in dbg.items() if k != "metrics"},
            "expansion_arm": applied_preset or expansion_arm,
            "env_snapshot": {
                k: env.get(k)
                for k in (
                    "YAMS_BENCH_TOPOLOGY_MODE",
                    "YAMS_SEARCH_TOPOLOGY_MAX_DOCS",
                    "YAMS_BENCH_TOPOLOGY_MEDOID_ONLY_EXPANSION",
                    "YAMS_BENCH_TOPOLOGY_SOURCE",
                    "YAMS_BENCH_TOPOLOGY_ENGINE",
                    "YAMS_BENCH_TOPOLOGY_ROUTING_REPRESENTATIVES",
                    "YAMS_BENCH_SEED_SEMANTIC_NEIGHBORS",
                    "YAMS_BENCH_REQUIRE_STEADY_STATE",
                    "YAMS_VECTOR_SEARCH_ENGINE",
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
            "topology_cluster_file": str(topology_cluster_path),
            "expansion_arm": applied_preset or expansion_arm,
            "topology_source": source,
            "ablation": ablation,
            "skip_reasons": dbg.get("skip_reasons"),
            "scoring_modes": dbg.get("scoring_modes"),
            "routing_modes": dbg.get("routing_modes"),
            "construction_fingerprints": dbg.get("construction_fingerprints"),
            "certificate": dbg.get("certificate"),
            "dataset": env.get("YAMS_BENCH_DATASET"),
            "corpus_size": env.get("YAMS_BENCH_CORPUS_SIZE"),
            "mixed_corpus": (
                {
                    "sources": list(mixed_corpus.sources),
                    "manifest": str(mixed_corpus.manifest_path),
                    "identity": str(mixed_corpus.identity_path),
                    "documents": mixed_corpus.document_count,
                    "queries": mixed_corpus.query_count,
                }
                if mixed_corpus is not None
                else None
            ),
        },
        message="retrieval_quality completed"
        if proc.returncode == 0
        else f"exit {proc.returncode}",
        raw_path=str(debug_path) if debug_path.exists() else str(stdout_path),
    )
