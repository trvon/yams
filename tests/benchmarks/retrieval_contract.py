"""Preflight checks for retrieval experiments; shared with optional local xplan."""

from collections.abc import Mapping
from typing import Any
import math


def validate_retrieval_configuration(
    env: Mapping[str, str], params: Mapping[str, Any], factors: Mapping[str, Any]
) -> list[str]:
    """Reject comparisons whose declared treatment cannot match effective execution.

    PHSS is a rerank pool, not an HNSW traversal-work bound. The computed raw k
    is a lower bound: weak-query fanout can increase it further at runtime.
    """
    issues: list[str] = []
    if "ann_candidate_budget" in factors and env.get("YAMS_VECTOR_VEC0_PHSS_ENABLED") == "1":
        document_limit = int(env.get("YAMS_VECTOR_MAX_RESULTS", "150"))
        aggregation = env.get("YAMS_SEARCH_CHUNK_AGGREGATION", "weighted_top_k_avg").lower()
        # Plans cannot override the typed aggregation top-k through an env knob.
        multiplier = 1 if aggregation == "max" else 3
        raw_limit = document_limit * multiplier
        configured = int(env.get("YAMS_VECTOR_VEC0_PHSS_CANDIDATES", "64"))
        effective = max(raw_limit, configured)
        if int(factors["ann_candidate_budget"]) != effective:
            issues.append(f"declared PHSS pool differs from effective minimum {effective} "
                          f"(raw chunk k={raw_limit}); use distinct effective budgets")

    if params.get("require_narrowing"):
        engine = params.get("vector_search_engine", env.get("YAMS_VECTOR_SEARCH_ENGINE", ""))
        if engine not in {"simeon_pq_adc", "simeon_pq"}:
            issues.append("fast narrowing requires simeon_pq_adc with observed work counters")
        # Artifact loading and held-out calibration generation are not implemented.
        # Do not accept a filename as evidence that the worker consumed calibration.
        issues.append("certified narrowing plans are parked until held-out, policy-bound "
                      "calibration generation and worker artifact loading are implemented; "
                      "hardcoded observation counts are not calibration")

    if params.get("latency_mode") == "product" and env.get("YAMS_SEARCH_STAGE_TRACE", "1") != "0":
        issues.append("product latency cannot include stage-trace counterfactual work")
    return issues


def validate_retrieval_metrics(metrics: Mapping[str, Any], params: Mapping[str, Any]) -> list[str]:
    """Require evidence that an advertised mechanism actually executed."""
    if params.get("require_shadow_evaluation"):
        for key in ("topology_shadow_evaluation_rate", "topology_candidate_rescue_attempt_rate",
                    "topology_route_work_observation_rate"):
            rate = float(metrics.get(key, 0.0))
            if not math.isfinite(rate) or not 0.0 < rate <= 1.0:
                return [f"shadow-cost arm lacks {key}; a load attempt or global fallback is not routed work"]
    if params.get("require_exact_shadow_control"):
        work = float(metrics.get("topology_candidate_rescue_exact_distance_evaluations_sum", 0.0))
        if not math.isfinite(work) or work <= 0.0:
            return ["traced shadow arm did not execute its exact control"]
    return []
