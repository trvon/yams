"""Shared ablation axis → env mapping for all xplan workers.

Design: one-factor-off (or explicit factor values) maps to harness/product-test
env knobs. Search component ablations use weight=0 gates (search_engine.cpp
skips components when weight is 0) and require YAMS_ENABLE_ENV_OVERRIDES=1.

Factor names (on/off or 0/1 or enabled/disabled, or a number for weights):
  kg, vectors, gliner, topology, rerank, graph_rerank,
  text, vector, vector_weight, kg_weight, simeon_text, concept_boost,
  graph_rerank_weight, rerank_weight, search_type, seed_semantic_neighbors

Also accepts expansion_arm (delegates to EXPANSION_PRESETS in retrieval_quality).

No silent no-ops: every factor either maps to an env key that product or harness
code reads, or ``apply_ablation`` raises ``AblationError``. Factors whose env key
has no reader (and no typed-config route the bench can reach) are listed in
``UNSUPPORTED_FACTORS`` and rejected. Unknown factor keys and unparseable axis
values raise too, so a typo cannot quietly run the default engine.
"""

from __future__ import annotations

from typing import Any


class AblationError(ValueError):
    """A plan asked for an ablation the harness cannot honestly apply."""


# Factors that used to map to env vars nothing reads (verified by searching src/ and
# include/). SearchEngineConfig has typed fields for most, but no TOML key or bench flag
# routes them into retrieval_quality_bench, so those arms measured the identical default
# engine. Reject them instead of reporting fake deltas.
UNSUPPORTED_FACTORS: dict[str, str] = {
    "tiered": (
        "YAMS_SEARCH_TIERED_NARROW_VECTOR_SEARCH has no reader; "
        "SearchEngineConfig::tieredNarrowVectorSearch is not reachable from the bench"
    ),
    "graph_text": (
        "YAMS_SEARCH_GRAPH_TEXT_WEIGHT has no reader; "
        "SearchEngineConfig::graphTextWeight is not reachable from the bench"
    ),
    "graph_vector": (
        "YAMS_SEARCH_GRAPH_VECTOR_WEIGHT has no reader; "
        "SearchEngineConfig::graphVectorWeight is not reachable from the bench"
    ),
    "graph_vector_weight": (
        "YAMS_SEARCH_GRAPH_VECTOR_WEIGHT has no reader; "
        "SearchEngineConfig::graphVectorWeight is not reachable from the bench"
    ),
    "entity_vector": (
        "YAMS_SEARCH_WEAK_QUERY_ENTITY_VECTOR_FANOUT_MULTIPLIER has no reader; "
        "SearchEngineConfig::weakQueryEntityVectorFanoutMultiplier is not reachable "
        "from the bench"
    ),
}

# Component weight ablations: factor key → env var set to "0" when off.
WEIGHT_OFF_ENV: dict[str, str] = {
    "text": "YAMS_SEARCH_TEXT_WEIGHT",
    "simeon_text": "YAMS_SEARCH_SIMEON_TEXT_WEIGHT",
    "vector": "YAMS_SEARCH_VECTOR_WEIGHT",
    "vector_weight": "YAMS_SEARCH_VECTOR_WEIGHT",
    "kg_weight": "YAMS_SEARCH_KG_WEIGHT",
    "graph_rerank_weight": "YAMS_SEARCH_GRAPH_RERANK_WEIGHT",
    "rerank_weight": "YAMS_SEARCH_RERANK_WEIGHT",
    "concept_boost": "YAMS_SEARCH_CONCEPT_BOOST_WEIGHT",
}

# Toggle/enum axes applied directly by apply_ablation.
APPLIED_FACTOR_KEYS: frozenset[str] = frozenset(
    {
        "kg",
        "vectors",
        "gliner",
        "topology",
        "topology_mode",
        "rerank",
        "enable_reranking",
        "graph_rerank",
        "seed_semantic_neighbors",
        "search_type",
    }
)

# Factor keys that are not ablation axes: a worker consumes them as params/env or they are
# plan labels. Listed so a typo'd axis (``graph_txet``) fails loudly instead of no-op'ing.
PASSTHROUGH_FACTOR_KEYS: frozenset[str] = frozenset(
    {
        "ablate", "adaptive_routing", "ann_candidate_budget", "backend", "boundary_spill",
        "boundary_spill_distance_ratio", "boundary_spill_limit",
        "boundary_spill_residual_penalty", "bq_candidate_limit", "candidate_generation",
        "candidate_selection", "confidence_margin", "dataset", "docs_per_client",
        "expansion_arm", "expansion_output_budget", "expansion_source", "fault_kind",
        "feature_smoothing_hops", "final_window_rescue", "fusion_window_rescue",
        "graph_community_source", "graph_seed_ranking", "hydrate_snippets",
        "ingest_concurrency", "ingest_mode", "lane", "max_component_docs", "max_docs",
        "min_clusters", "min_edge_score", "min_route_score", "mixed_ops_per_client",
        "outer_maxsim", "output_format", "package", "post_ingest_batch_size", "profile",
        "relation_materialization_budget", "rerank_blend", "rerank_factor", "rerank_mode",
        "retrieval", "route_candidate_budget", "route_centroid_index", "route_index",
        "route_result_budget", "route_scoring", "scale", "search_clients", "search_ratio",
        "sgc_hops", "simeon_fragment_encoder", "sparse_dense_alpha", "topology_engine",
        "topology_rescue_selector", "topology_route_ann_candidate_limit", "topology_source",
        "topology_vector_policy", "vector_engine", "vector_result_budget", "vector_seed_probe",
    }
)

_ALL_AXIS_KEYS: frozenset[str] = (
    APPLIED_FACTOR_KEYS | frozenset(WEIGHT_OFF_ENV) | frozenset(UNSUPPORTED_FACTORS)
)


def _is_off(val: Any) -> bool:
    if val is None:
        return False
    if isinstance(val, bool):
        return not val
    s = str(val).strip().lower()
    return s in {"0", "off", "false", "no", "disabled", "disable", "none"}


def _is_on(val: Any) -> bool:
    if val is None:
        return False
    if isinstance(val, bool):
        return val
    s = str(val).strip().lower()
    return s in {"1", "on", "true", "yes", "enabled", "enable"}


def _norm_search_type(val: Any) -> str | None:
    if val is None:
        return None
    s = str(val).strip().lower()
    if s in {"hybrid", "keyword", "semantic", "grep"}:
        return s
    return None


def _toggle(key: str, val: Any) -> bool:
    """Return True for on, False for off; raise on anything else."""
    if _is_off(val):
        return False
    if _is_on(val):
        return True
    raise AblationError(f"ablation axis {key!r} has unrecognized on/off value {val!r}")


def _as_number(val: Any) -> float | None:
    if isinstance(val, bool) or val is None:
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _format_number(num: float) -> str:
    return repr(float(num)) if num != int(num) else str(int(num))


def validate_factor_keys(factors: dict[str, Any] | None, *, where: str = "factors") -> None:
    """Raise AblationError on factor keys the harness cannot honor."""
    for key in factors or {}:
        if key in UNSUPPORTED_FACTORS:
            raise AblationError(f"{where}: factor {key!r} is unsupported: {UNSUPPORTED_FACTORS[key]}")
        if key not in _ALL_AXIS_KEYS and key not in PASSTHROUGH_FACTOR_KEYS:
            raise AblationError(
                f"{where}: unknown factor {key!r}; not an ablation axis and not a known "
                "worker factor (typo? add it to PASSTHROUGH_FACTOR_KEYS if a worker consumes it)"
            )


def apply_ablation(
    env: dict[str, str],
    *,
    factors: dict[str, Any] | None = None,
    params: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Mutate env from ablation factors/params. Return applied ablation metadata.

    Raises AblationError for unknown/unsupported factor keys, unsupported ablation
    params, unparseable axis values, and numeric weights that contradict an explicit env.
    """
    factors = dict(factors or {})
    params = dict(params or {})
    validate_factor_keys(factors)
    # Params are plan parameters in general; only ablation-axis names overlay factors.
    axis_params = {k: v for k, v in params.items() if k in _ALL_AXIS_KEYS}
    validate_factor_keys(axis_params, where="params")
    axes: dict[str, Any] = {**factors, **axis_params}

    applied: dict[str, Any] = {"axes": {}, "weight_zeros": [], "flags": {}}
    needs_search_env = False

    # --- Product / harness feature flags ---
    if "kg" in axes:
        off = not _toggle("kg", axes["kg"])
        applied["axes"]["kg"] = "off" if off else "on"
        if off:
            env["YAMS_BENCH_DISABLE_KG"] = "1"
            applied["flags"]["YAMS_BENCH_DISABLE_KG"] = "1"

    if "vectors" in axes:
        off = not _toggle("vectors", axes["vectors"])
        applied["axes"]["vectors"] = "off" if off else "on"
        if off:
            env["YAMS_DISABLE_VECTORS"] = "1"
            applied["flags"]["YAMS_DISABLE_VECTORS"] = "1"

    if "gliner" in axes:
        off = not _toggle("gliner", axes["gliner"])
        applied["axes"]["gliner"] = "off" if off else "on"
        if off:
            env["YAMS_DISABLE_GLINER_TITLES"] = "1"
            applied["flags"]["YAMS_DISABLE_GLINER_TITLES"] = "1"

    if "topology" in axes or "topology_mode" in axes:
        raw = axes.get("topology_mode", axes.get("topology"))
        if _is_off(raw) or str(raw).lower() in {"disabled", "off", "0"}:
            env["YAMS_BENCH_TOPOLOGY_MODE"] = "disabled"
            applied["axes"]["topology"] = "disabled"
            applied["flags"]["YAMS_BENCH_TOPOLOGY_MODE"] = "disabled"
        elif raw is not None and str(raw).lower() not in {"on", "1", "true", "enabled"}:
            env["YAMS_BENCH_TOPOLOGY_MODE"] = str(raw)
            applied["axes"]["topology"] = str(raw)
            applied["flags"]["YAMS_BENCH_TOPOLOGY_MODE"] = str(raw)
        else:
            applied["axes"]["topology"] = "on"

    # enable_reranking is the plan factor used by simeon_rerank; alias of rerank.
    if "rerank" in axes or "enable_reranking" in axes:
        raw = axes.get("rerank", axes.get("enable_reranking"))
        off = not _toggle("rerank", raw)
        applied["axes"]["rerank"] = "off" if off else "on"
        env["YAMS_SEARCH_ENABLE_RERANKING"] = "0" if off else "1"
        applied["flags"]["YAMS_SEARCH_ENABLE_RERANKING"] = env["YAMS_SEARCH_ENABLE_RERANKING"]
        needs_search_env = True

    if "graph_rerank" in axes:
        off = not _toggle("graph_rerank", axes["graph_rerank"])
        applied["axes"]["graph_rerank"] = "off" if off else "on"
        env["YAMS_SEARCH_ENABLE_GRAPH_RERANK"] = "0" if off else "1"
        applied["flags"]["YAMS_SEARCH_ENABLE_GRAPH_RERANK"] = env["YAMS_SEARCH_ENABLE_GRAPH_RERANK"]
        needs_search_env = True

    if "seed_semantic_neighbors" in axes:
        on = _toggle("seed_semantic_neighbors", axes["seed_semantic_neighbors"])
        applied["axes"]["seed_semantic_neighbors"] = "on" if on else "off"
        env["YAMS_BENCH_SEED_SEMANTIC_NEIGHBORS"] = "1" if on else "0"

    if "search_type" in axes:
        st = _norm_search_type(axes["search_type"])
        if not st:
            raise AblationError(
                f"ablation axis 'search_type' has unsupported value {axes['search_type']!r}"
            )
        env["YAMS_BENCH_SEARCH_TYPE"] = st
        applied["axes"]["search_type"] = st
        applied["flags"]["YAMS_BENCH_SEARCH_TYPE"] = st

    # --- Search component weights (0 = ablated; other numbers set the weight) ---
    for factor_key, env_key in WEIGHT_OFF_ENV.items():
        if factor_key not in axes:
            continue
        raw = axes[factor_key]
        number = _as_number(raw)
        if number is not None:
            if number < 0:
                raise AblationError(f"ablation axis {factor_key!r} must be >= 0, got {raw!r}")
            existing = _as_number(env.get(env_key))
            if env_key in env and existing is not None and abs(existing - number) > 1e-9:
                raise AblationError(
                    f"ablation axis {factor_key}={raw!r} contradicts explicit "
                    f"{env_key}={env[env_key]!r}"
                )
            if env_key not in env:
                env[env_key] = _format_number(number)
            applied["flags"][env_key] = env[env_key]
            needs_search_env = True
            if number == 0:
                applied["weight_zeros"].append(factor_key)
                applied["axes"][factor_key] = "off"
            else:
                applied["axes"][factor_key] = _format_number(number)
        elif not _toggle(factor_key, raw):
            env[env_key] = "0"
            applied["weight_zeros"].append(factor_key)
            applied["axes"][factor_key] = "off"
            applied["flags"][env_key] = "0"
            needs_search_env = True
        else:
            applied["axes"][factor_key] = "on"

    if needs_search_env or applied["weight_zeros"]:
        env.setdefault("YAMS_ENABLE_ENV_OVERRIDES", "1")
        applied["flags"]["YAMS_ENABLE_ENV_OVERRIDES"] = env.get("YAMS_ENABLE_ENV_OVERRIDES")

    applied["label"] = "+".join(
        f"{k}={v}" for k, v in sorted(applied["axes"].items())
    ) or "baseline"
    return applied


def ablation_from_context(ctx: Any) -> tuple[dict[str, str], dict[str, Any]]:
    """Build env overlay + metadata from a WorkerContext-like object."""
    env: dict[str, str] = {}
    arm = getattr(ctx, "arm", None)
    arm_obj = getattr(arm, "arm", None) if arm is not None else None
    factors = getattr(arm_obj, "factors", None) or {}
    params = getattr(ctx, "params", None) or {}
    # Start from ctx.env then apply ablations so plan env is base.
    base = dict(getattr(ctx, "env", None) or {})
    meta = apply_ablation(base, factors=factors, params=params)
    return base, meta
