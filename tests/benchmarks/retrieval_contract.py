"""Preflight checks for retrieval experiments; shared with optional local xplan."""

from collections.abc import Mapping
from typing import Any
import math


def _enabled(value: Any, default: bool) -> bool:
    normalized = str(value).lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    return default


def _positive_integer(value: Any) -> int:
    number = int(value)
    if isinstance(value, bool) or number <= 0 or (isinstance(value, float) and number != value):
        raise ValueError("expected a positive integer")
    return number


def _finite_number(value: Any) -> float:
    try:
        return float(value) if not isinstance(value, bool) else math.nan
    except (ValueError, TypeError, OverflowError):
        return math.nan


def validate_retrieval_configuration(
    env: Mapping[str, str], params: Mapping[str, Any], factors: Mapping[str, Any]
) -> list[str]:
    """Reject comparisons whose declared treatment cannot match effective execution.

    PHSS is a rerank pool, not an HNSW traversal-work bound. The computed raw k
    is a lower bound: weak-query fanout can increase it further at runtime.
    """
    issues: list[str] = []
    if "ann_candidate_budget" in factors and _enabled(env.get("YAMS_VECTOR_VEC0_PHSS_ENABLED"), False):
        try:
            # Runtime scales defaults first, then applies individual result overrides.
            if "YAMS_VECTOR_MAX_RESULTS" in env:
                document_limit = _positive_integer(env["YAMS_VECTOR_MAX_RESULTS"])
            else:
                scale = float(env.get("YAMS_CANDIDATE_MULTIPLIER", "1"))
                document_limit = _positive_integer(int(150 * scale))
            aggregation = env.get("YAMS_SEARCH_CHUNK_AGGREGATION", "weighted_top_k_avg")
            # The C++ compatibility parser accepts MAX/max, not arbitrary mixed case.
            multiplier = 1 if aggregation in {"max", "MAX"} else 3
            raw_limit = document_limit * multiplier
            configured = _positive_integer(env.get("YAMS_VECTOR_VEC0_PHSS_CANDIDATES", "64"))
            effective = max(raw_limit, configured)
            if _positive_integer(factors["ann_candidate_budget"]) != effective:
                issues.append(f"declared PHSS pool differs from effective minimum {effective} "
                              f"(raw chunk k={raw_limit}); use distinct effective budgets")
        except (ValueError, TypeError, OverflowError):
            issues.append("invalid PHSS budget configuration; expected finite positive numeric values")

    if params.get("require_narrowing"):
        engine = params.get("vector_search_engine", env.get("YAMS_VECTOR_SEARCH_ENGINE", ""))
        if engine not in {"simeon_pq_adc", "simeon_pq"}:
            issues.append("fast narrowing requires simeon_pq_adc with observed work counters")
        # Artifact loading and held-out calibration generation are not implemented.
        # Do not accept a filename as evidence that the worker consumed calibration.
        issues.append("certified narrowing plans are parked until held-out, policy-bound "
                      "calibration generation and worker artifact loading are implemented; "
                      "hardcoded observation counts are not calibration")

    if params.get("latency_mode") == "product" and _enabled(env.get("YAMS_SEARCH_STAGE_TRACE"), True):
        issues.append("product latency cannot include stage-trace counterfactual work")
    return issues


def validate_retrieval_metrics(metrics: Mapping[str, Any], params: Mapping[str, Any]) -> list[str]:
    """Require evidence that an advertised mechanism actually executed."""
    if params.get("require_shadow_evaluation"):
        for key in ("topology_shadow_evaluation_rate", "topology_candidate_rescue_attempt_rate",
                    "topology_route_work_observation_rate"):
            rate = _finite_number(metrics.get(key, 0.0))
            if not math.isfinite(rate) or not 0.0 < rate <= 1.0:
                return [f"shadow-cost arm lacks {key}; a load attempt or global fallback is not routed work"]
    if params.get("require_exact_shadow_control"):
        work = _finite_number(metrics.get("topology_candidate_rescue_exact_distance_evaluations_sum", 0.0))
        if not math.isfinite(work) or work <= 0.0:
            return ["traced shadow arm did not execute its exact control"]
    return []
