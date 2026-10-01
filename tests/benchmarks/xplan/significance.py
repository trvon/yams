"""Welch's t-test and metric direction for xplan arm/run comparisons.

Replaces the old "delta exceeds pooled stdev" heuristic (which compared a mean difference
against a spread of individual runs, ignoring n) with a standard-error based test:

    se = sqrt(sd_a^2/n_a + sd_b^2/n_b)
    t  = (mean_a - mean_b) / se
    df = Welch-Satterthwaite

scipy is optional: without it the decision uses a two-sided 95% critical-value table
(rounded *down* in df, which is conservative); with it an exact p-value is reported too.
Inputs are sample (n-1) standard deviations as written by ``runner.aggregate_reps``.
"""

from __future__ import annotations

import math
from typing import Any

# Two-sided alpha=0.05 critical values of Student's t by degrees of freedom.
T_CRIT_95: dict[int, float] = {
    1: 12.706, 2: 4.303, 3: 3.182, 4: 2.776, 5: 2.571, 6: 2.447, 7: 2.365, 8: 2.306,
    9: 2.262, 10: 2.228, 11: 2.201, 12: 2.179, 13: 2.160, 14: 2.145, 15: 2.131,
    16: 2.120, 17: 2.110, 18: 2.101, 19: 2.093, 20: 2.086, 21: 2.080, 22: 2.074,
    23: 2.069, 24: 2.064, 25: 2.060, 26: 2.056, 27: 2.052, 28: 2.048, 29: 2.045,
    30: 2.042, 40: 2.021, 60: 2.000, 120: 1.980,
}
T_CRIT_INF = 1.960


def t_critical_95(df: float) -> float:
    """Two-sided 95% critical t for `df` (conservative: uses the table row at or below df)."""
    if df < 1:
        return T_CRIT_95[1]
    if math.isinf(df):
        return T_CRIT_INF
    usable = [k for k in T_CRIT_95 if k <= df]
    if not usable:
        return T_CRIT_95[1]
    row = max(usable)
    if row == 120 and df > 120:
        return T_CRIT_95[120] if df < 1000 else T_CRIT_INF
    return T_CRIT_95[row]


def welch(
    mean_a: float,
    sd_a: float,
    n_a: float,
    mean_b: float,
    sd_b: float,
    n_b: float,
) -> dict[str, Any]:
    """Welch's t-test of (a - b). `marker` is '*' significant, '~' noise, '' not testable."""
    delta = float(mean_a) - float(mean_b)
    out: dict[str, Any] = {
        "delta": delta,
        "se": None,
        "t": None,
        "df": None,
        "t_crit": None,
        "p_value": None,
        "significant": None,
        "marker": "",
    }
    if n_a < 2 or n_b < 2:
        return out  # variance unknown: no claim either way
    va = float(sd_a) ** 2 / float(n_a)
    vb = float(sd_b) ** 2 / float(n_b)
    se = math.sqrt(va + vb)
    out["se"] = se
    if se == 0.0:
        # Zero observed spread: any non-zero delta is deterministic, zero delta is a tie.
        out["significant"] = delta != 0.0
        out["t"] = math.inf if delta != 0.0 else 0.0
        out["df"] = float(n_a + n_b - 2)
        out["t_crit"] = t_critical_95(out["df"])
        out["marker"] = "*" if out["significant"] else "~"
        return out
    t = delta / se
    df = (va + vb) ** 2 / (
        (va**2) / (float(n_a) - 1.0) + (vb**2) / (float(n_b) - 1.0)
    )
    t_crit = t_critical_95(df)
    out.update({"t": t, "df": df, "t_crit": t_crit})
    out["significant"] = abs(t) > t_crit
    out["marker"] = "*" if out["significant"] else "~"
    try:  # optional exact p-value
        from scipy import stats  # type: ignore

        out["p_value"] = float(2.0 * stats.t.sf(abs(t), df))
    except Exception:  # noqa: BLE001 - scipy is optional
        pass
    return out


def welch_from_metrics(
    a: dict[str, Any], b: dict[str, Any], key: str
) -> dict[str, Any] | None:
    """Welch test of metric `key` from two aggregated metric dicts (mean/_stdev/_n)."""
    av, bv = a.get(key), b.get(key)
    if isinstance(av, bool) or isinstance(bv, bool):
        return None
    if not isinstance(av, (int, float)) or not isinstance(bv, (int, float)):
        return None
    a_n, b_n = a.get(f"{key}_n"), b.get(f"{key}_n")
    a_sd, b_sd = a.get(f"{key}_stdev"), b.get(f"{key}_stdev")
    if not all(isinstance(v, (int, float)) for v in (a_n, b_n, a_sd, b_sd)):
        return welch(float(av), 0.0, 1.0, float(bv), 0.0, 1.0)  # not testable
    return welch(float(av), float(a_sd), float(a_n), float(bv), float(b_sd), float(b_n))


# --- metric direction -------------------------------------------------------------------

HIGHER_BETTER = "higher"
LOWER_BETTER = "lower"
_NEUTRAL = "unknown"

# Rules are checked in order; first hit wins. Quality/throughput names are matched first so
# `useful_recall_at_512_bytes` is not read as a byte count; then cost-like (lower-better)
# tokens; then weaker higher-better hints.
_STRONG_HIGHER = ("mrr", "ndcg", "recall", "precision", "hit_rate", "useful", "_per_s", "qps")
_LOWER_TOKENS = (
    "latency", "_ms", "rss", "fail", "error", "reject", "timeout", "drop", "miss",
    "residual", "violation", "leak", "duplicate", "wall", "elapsed", "duration",
    "backlog", "queue", "wait", "stall", "lag", "bytes",
)
_WEAK_HIGHER = (
    "throughput", "pass", "success", "accuracy", "completeness", "coverage", "purity",
    "repaired",
)


def metric_direction(key: str) -> str:
    """'higher' | 'lower' | 'unknown' — which way is an improvement."""
    k = key.lower()
    if k == "map" or k.endswith("_map") or any(tok in k for tok in _STRONG_HIGHER):
        return HIGHER_BETTER
    if any(tok in k for tok in _LOWER_TOKENS):
        return LOWER_BETTER
    if any(tok in k for tok in _WEAK_HIGHER):
        return HIGHER_BETTER
    return _NEUTRAL


def is_regression(key: str, delta: float) -> bool:
    """True when `delta` (candidate - baseline) moves the wrong way for `key`."""
    direction = metric_direction(key)
    if direction == HIGHER_BETTER:
        return delta < 0
    if direction == LOWER_BETTER:
        return delta > 0
    return False
