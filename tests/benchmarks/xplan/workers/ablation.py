"""Shared ablation axis → env mapping for all xplan workers.

Design: one-factor-off (or explicit factor values) maps to harness/product-test
env knobs. Search component ablations use weight=0 gates (search_engine.cpp
skips components when weight is 0) and require YAMS_ENABLE_ENV_OVERRIDES=1.

Factor names (on/off or 0/1 or enabled/disabled):
  kg, vectors, gliner, topology, rerank, graph_rerank,
  text, path, vector_weight, kg_weight, graph_text, graph_vector,
  entity_vector, metadata, tag, parallel_tiered,
  search_type, auto_repair, seed_semantic_neighbors

Also accepts expansion_arm (delegates to EXPANSION_PRESETS in retrieval_quality).
"""

from __future__ import annotations

from typing import Any


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


# Component weight ablations: factor key → env var set to "0" when off.
WEIGHT_OFF_ENV: dict[str, str] = {
    "text": "YAMS_SEARCH_TEXT_WEIGHT",
    "simeon_text": "YAMS_SEARCH_SIMEON_TEXT_WEIGHT",
    "vector": "YAMS_SEARCH_VECTOR_WEIGHT",
    "vector_weight": "YAMS_SEARCH_VECTOR_WEIGHT",
    "kg_weight": "YAMS_SEARCH_KG_WEIGHT",
    "graph_text": "YAMS_SEARCH_GRAPH_TEXT_WEIGHT",
    "graph_vector": "YAMS_SEARCH_GRAPH_VECTOR_WEIGHT",
    "graph_rerank_weight": "YAMS_SEARCH_GRAPH_RERANK_WEIGHT",
    "rerank_weight": "YAMS_SEARCH_RERANK_WEIGHT",
    "concept_boost": "YAMS_SEARCH_CONCEPT_BOOST_WEIGHT",
}


def apply_ablation(
    env: dict[str, str],
    *,
    factors: dict[str, Any] | None = None,
    params: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Mutate env from ablation factors/params. Return applied ablation metadata."""
    factors = dict(factors or {})
    params = dict(params or {})
    # Params overlay factors for explicit plan params.
    axes: dict[str, Any] = {**factors, **{k: v for k, v in params.items() if k in factors or k in WEIGHT_OFF_ENV or k in {
        "kg", "vectors", "gliner", "topology", "rerank", "graph_rerank", "search_type",
        "auto_repair", "seed_semantic_neighbors", "parallel_tiered", "topology_mode",
        "text", "vector", "vector_weight", "kg_weight", "graph_text", "graph_vector",
        "simeon_text", "entity_vector", "concept_boost", "graph_rerank_weight", "rerank_weight",
    }}}

    applied: dict[str, Any] = {"axes": {}, "weight_zeros": [], "flags": {}}
    needs_search_env = False

    # --- Product / harness feature flags ---
    if "kg" in axes:
        off = _is_off(axes["kg"])
        applied["axes"]["kg"] = "off" if off else "on"
        if off:
            env["YAMS_BENCH_DISABLE_KG"] = "1"
            applied["flags"]["YAMS_BENCH_DISABLE_KG"] = "1"

    if "vectors" in axes:
        off = _is_off(axes["vectors"])
        applied["axes"]["vectors"] = "off" if off else "on"
        if off:
            env["YAMS_DISABLE_VECTORS"] = "1"
            applied["flags"]["YAMS_DISABLE_VECTORS"] = "1"

    if "gliner" in axes:
        off = _is_off(axes["gliner"])
        applied["axes"]["gliner"] = "off" if off else "on"
        if off:
            env["YAMS_DISABLE_GLINER_TITLE_EXTRACTION"] = "1"
            applied["flags"]["YAMS_DISABLE_GLINER_TITLE_EXTRACTION"] = "1"

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
        off = _is_off(raw)
        applied["axes"]["rerank"] = "off" if off else "on"
        env["YAMS_SEARCH_ENABLE_RERANKING"] = "0" if off else "1"
        applied["flags"]["YAMS_SEARCH_ENABLE_RERANKING"] = env["YAMS_SEARCH_ENABLE_RERANKING"]
        needs_search_env = True

    if "graph_rerank" in axes:
        off = _is_off(axes["graph_rerank"])
        applied["axes"]["graph_rerank"] = "off" if off else "on"
        env["YAMS_SEARCH_ENABLE_GRAPH_RERANK"] = "0" if off else "1"
        applied["flags"]["YAMS_SEARCH_ENABLE_GRAPH_RERANK"] = env["YAMS_SEARCH_ENABLE_GRAPH_RERANK"]
        needs_search_env = True

    if "parallel_tiered" in axes:
        off = _is_off(axes["parallel_tiered"])
        applied["axes"]["parallel_tiered"] = "off" if off else "on"
        env["YAMS_SEARCH_ENABLE_TIERED_EXECUTION"] = "0" if off else "1"
        applied["flags"]["YAMS_SEARCH_ENABLE_TIERED_EXECUTION"] = env[
            "YAMS_SEARCH_ENABLE_TIERED_EXECUTION"
        ]
        needs_search_env = True

    if "seed_semantic_neighbors" in axes:
        on = _is_on(axes["seed_semantic_neighbors"]) and not _is_off(axes["seed_semantic_neighbors"])
        applied["axes"]["seed_semantic_neighbors"] = "on" if on else "off"
        env["YAMS_BENCH_SEED_SEMANTIC_NEIGHBORS"] = "1" if on else "0"

    if "search_type" in axes:
        st = _norm_search_type(axes["search_type"])
        if st:
            env["YAMS_BENCH_SEARCH_TYPE"] = st
            applied["axes"]["search_type"] = st
            applied["flags"]["YAMS_BENCH_SEARCH_TYPE"] = st

    # --- Search component weights (0 = ablated) ---
    for factor_key, env_key in WEIGHT_OFF_ENV.items():
        if factor_key not in axes:
            continue
        if _is_off(axes[factor_key]):
            env[env_key] = "0"
            applied["weight_zeros"].append(factor_key)
            applied["axes"][factor_key] = "off"
            applied["flags"][env_key] = "0"
            needs_search_env = True
        elif _is_on(axes[factor_key]):
            applied["axes"][factor_key] = "on"

    # entity_vector uses fanout multipliers; no dedicated weight env — use 0 multiplier if off.
    if "entity_vector" in axes and _is_off(axes["entity_vector"]):
        env["YAMS_SEARCH_WEAK_QUERY_ENTITY_VECTOR_FANOUT_MULTIPLIER"] = "0"
        applied["axes"]["entity_vector"] = "off"
        applied["weight_zeros"].append("entity_vector")
        needs_search_env = True

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
