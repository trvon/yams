"""Summarize multi-arm xplan runs into compact + granular engineering reports."""

from __future__ import annotations

import hashlib
import json
import math
import random
from pathlib import Path
from typing import Any

from artifacts import metrics_path, read_json
from model import ExperimentPlan
from report import write_report


def _find_baseline_row(
    plan: ExperimentPlan, rows: list[dict[str, Any]]
) -> dict[str, Any] | None:
    if not plan.baseline:
        return None
    for row in rows:
        if row.get("valid") and (
            row.get("arm") == plan.baseline or row.get("safe_name") == plan.baseline
        ):
            return row
    return None


def _delta_cell(
    arm_metrics: dict[str, Any], base_metrics: dict[str, Any], key: str
) -> tuple[str, dict[str, Any] | None]:
    """Return (rendered cell, structured delta) for one metric vs baseline.

    Marks the delta `~` when |delta| <= pooled stdev (within run-to-run noise, needs
    repeats>1), `*` when it clears noise, and bare when no variance is available (n<=1).
    """
    av = arm_metrics.get(key)
    bv = base_metrics.get(key)
    if not isinstance(av, (int, float)) or not isinstance(bv, (int, float)):
        return "", None
    delta = float(av) - float(bv)
    a_sd = arm_metrics.get(f"{key}_stdev")
    b_sd = base_metrics.get(f"{key}_stdev")
    a_n = arm_metrics.get(f"{key}_n")
    b_n = base_metrics.get(f"{key}_n")
    pooled = None
    if (
        isinstance(a_sd, (int, float))
        and isinstance(b_sd, (int, float))
        and isinstance(a_n, (int, float))
        and isinstance(b_n, (int, float))
        and float(a_n) > 1.0
        and float(b_n) > 1.0
    ):
        pooled = math.sqrt(float(a_sd) ** 2 + float(b_sd) ** 2)
    if pooled is None:
        mark = ""
    elif abs(delta) <= pooled:
        mark = " ~"
    else:
        mark = " *"
    cell = f"{delta:+.4g}{mark}"
    return cell, {
        "delta": delta,
        "pooled_stdev": pooled,
        "within_noise": (pooled is not None and abs(delta) <= pooled),
    }


def _metric_values(metrics_doc: dict[str, Any]) -> dict[str, Any]:
    inner = metrics_doc.get("metrics")
    if isinstance(inner, dict):
        return dict(inner)
    return {k: v for k, v in metrics_doc.items() if k not in {"status", "worker", "arm", "raw"}}


def _component_hits_by_query(
    debug_path: Path, *, component: str, top_k: int
) -> dict[int, tuple[str, ...]]:
    if not debug_path.is_file():
        raise ValueError(f"missing query trace: {debug_path}")

    hits_by_query: dict[int, tuple[str, ...]] = {}
    for line_number, line in enumerate(
        debug_path.read_text(encoding="utf-8", errors="replace").splitlines(), start=1
    ):
        if not line.strip():
            continue
        try:
            event = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ValueError(
                f"invalid JSON in {debug_path} at line {line_number}: {exc}"
            ) from exc
        if not isinstance(event, dict) or event.get("search_type") != "hybrid":
            continue
        query_index = event.get("query_index")
        if not isinstance(query_index, int) or query_index < 0:
            raise ValueError(
                f"hybrid trace in {debug_path} has invalid query_index at line {line_number}"
            )
        if query_index in hits_by_query:
            raise ValueError(
                f"hybrid trace in {debug_path} duplicates query index {query_index}"
            )

        stats = event.get("search_stats")
        if not isinstance(stats, dict):
            raise ValueError(
                f"hybrid trace in {debug_path} has no search_stats at query index {query_index}"
            )
        trace = stats.get("trace_component_hits_json")
        if isinstance(trace, str):
            try:
                trace = json.loads(trace)
            except json.JSONDecodeError as exc:
                raise ValueError(
                    f"component trace in {debug_path} is invalid at query index {query_index}: "
                    f"{exc}"
                ) from exc
        if not isinstance(trace, dict):
            raise ValueError(
                f"component trace in {debug_path} is missing at query index {query_index}"
            )

        component_trace = trace.get(component)
        if component_trace is None:
            raw_ids: list[Any] = []
        elif isinstance(component_trace, dict):
            value = component_trace.get("unique_top_doc_ids")
            if not isinstance(value, list):
                raise ValueError(
                    f"component {component!r} in {debug_path} has no document-id list "
                    f"at query index {query_index}"
                )
            raw_ids = value
        else:
            raise ValueError(
                f"component {component!r} in {debug_path} is invalid at query index "
                f"{query_index}"
            )

        unique_ids: list[str] = []
        seen: set[str] = set()
        for value in raw_ids:
            if not isinstance(value, str):
                raise ValueError(
                    f"component {component!r} in {debug_path} has a non-string document id "
                    f"at query index {query_index}"
                )
            if value not in seen:
                seen.add(value)
                unique_ids.append(value)
        hits_by_query[query_index] = tuple(unique_ids[:top_k])

    if not hits_by_query:
        raise ValueError(f"query trace has no measured hybrid queries: {debug_path}")
    return hits_by_query


def _query_sources_by_index(identity_path: Path) -> dict[int, str]:
    if not identity_path.is_file():
        raise ValueError(f"missing mixed-corpus identity: {identity_path}")
    try:
        identity = json.loads(identity_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"mixed-corpus identity is unreadable: {identity_path}: {exc}") from exc
    if not isinstance(identity, dict):
        raise ValueError(f"mixed-corpus identity must be an object: {identity_path}")
    query_order = identity.get("query_order")
    query_sources = identity.get("query_sources")
    if not isinstance(query_order, list) or not query_order:
        raise ValueError(f"mixed-corpus identity has no query_order: {identity_path}")
    if not isinstance(query_sources, dict):
        raise ValueError(f"mixed-corpus identity has no query_sources: {identity_path}")

    sources_by_index: dict[int, str] = {}
    for query_index, query_id in enumerate(query_order):
        if not isinstance(query_id, str) or not isinstance(query_sources.get(query_id), str):
            raise ValueError(
                f"mixed-corpus identity lacks a source for query index {query_index}: "
                f"{identity_path}"
            )
        sources_by_index[query_index] = str(query_sources[query_id])
    return sources_by_index


def _debug_paths_for_repeats(arm_dir: Path, repeats: int) -> list[Path]:
    if repeats <= 1:
        root_debug = arm_dir / "debug.jsonl"
        if root_debug.is_file() or not (arm_dir / "rep00" / "debug.jsonl").is_file():
            return [root_debug]
        return [arm_dir / "rep00" / "debug.jsonl"]
    return [arm_dir / f"rep{rep:02d}" / "debug.jsonl" for rep in range(repeats)]


def _paired_vector_oracle_metrics(
    *,
    target_dir: Path,
    oracle_dir: Path,
    repeats: int,
    component: str,
    top_k: int,
    sources_by_index: dict[int, str],
) -> dict[str, float]:
    expected_indices = set(sources_by_index)
    recalls: list[float] = []
    target_counts: list[float] = []
    oracle_counts: list[float] = []
    recalls_by_source: dict[str, list[float]] = {}

    target_paths = _debug_paths_for_repeats(target_dir, repeats)
    oracle_paths = _debug_paths_for_repeats(oracle_dir, repeats)
    for repeat, (target_path, oracle_path) in enumerate(
        zip(target_paths, oracle_paths, strict=True)
    ):
        target_hits = _component_hits_by_query(
            target_path, component=component, top_k=top_k
        )
        oracle_hits = _component_hits_by_query(
            oracle_path, component=component, top_k=top_k
        )
        for label, observed in (("target", set(target_hits)), ("oracle", set(oracle_hits))):
            if observed != expected_indices:
                missing = sorted(expected_indices - observed)
                unexpected = sorted(observed - expected_indices)
                raise ValueError(
                    f"{label} repeat {repeat} query indices do not match mixed-corpus identity "
                    f"(missing={missing}, unexpected={unexpected})"
                )

        for query_index in sorted(expected_indices):
            reference = oracle_hits[query_index]
            if not reference:
                raise ValueError(
                    f"oracle repeat {repeat} returned no {component!r} documents at query "
                    f"index {query_index}"
                )
            candidate = target_hits[query_index]
            overlap = len(set(candidate).intersection(reference))
            recall = float(overlap) / float(len(reference))
            recalls.append(recall)
            target_counts.append(float(len(candidate)))
            oracle_counts.append(float(len(reference)))
            recalls_by_source.setdefault(sources_by_index[query_index], []).append(recall)

    metrics = {
        "vector_exact_oracle_recall_at_k": math.fsum(recalls) / len(recalls),
        "vector_exact_oracle_gap_at_k": 1.0 - (math.fsum(recalls) / len(recalls)),
        "vector_exact_oracle_paired_queries": float(len(expected_indices)),
        "vector_exact_oracle_paired_observations": float(len(recalls)),
        "vector_exact_oracle_target_docs_at_k_avg": math.fsum(target_counts)
        / len(target_counts),
        "vector_exact_oracle_reference_docs_at_k_avg": math.fsum(oracle_counts)
        / len(oracle_counts),
    }
    for source, source_recalls in sorted(recalls_by_source.items()):
        source_recall = math.fsum(source_recalls) / len(source_recalls)
        metrics[f"vector_exact_oracle_recall_at_k_{source}"] = source_recall
        metrics[f"vector_exact_oracle_gap_at_k_{source}"] = 1.0 - source_recall
        metrics[f"vector_exact_oracle_paired_queries_{source}"] = float(
            sum(1 for value in sources_by_index.values() if value == source)
        )
    return metrics


def _invalidate_row(row: dict[str, Any], message: str) -> None:
    row["valid"] = False
    issues = row.setdefault("validation_issues", [])
    if not isinstance(issues, list):
        issues = []
        row["validation_issues"] = issues
    issues.append({"level": "error", "message": message})


def _apply_paired_vector_oracle(
    plan: ExperimentPlan, run_dir: Path, rows: list[dict[str, Any]]
) -> dict[str, Any] | None:
    spec = plan.summarize.paired_vector_oracle
    if spec is None:
        return None

    artifact: dict[str, Any] = {
        "ok": True,
        "spec": spec.to_dict(),
        "arms": {},
    }
    oracle_row = next(
        (
            row
            for row in rows
            if row.get("arm") == spec.arm or row.get("safe_name") == spec.arm
        ),
        None,
    )
    if oracle_row is None:
        error = f"paired vector oracle arm {spec.arm!r} is absent"
        for row in rows:
            _invalidate_row(row, error)
        artifact.update({"ok": False, "error": error})
    else:
        identity_path = run_dir / spec.identity_file
        try:
            sources_by_index = _query_sources_by_index(identity_path)
        except ValueError as exc:
            error = str(exc)
            for row in rows:
                _invalidate_row(row, error)
            artifact.update({"ok": False, "error": error})
        else:
            oracle_dir = run_dir / "arms" / str(oracle_row["safe_name"])
            if not oracle_row.get("valid"):
                error = f"paired vector oracle arm {spec.arm!r} failed arm validation"
                for row in rows:
                    _invalidate_row(row, error)
                artifact.update({"ok": False, "error": error})
            else:
                for row in rows:
                    arm_name = str(row.get("arm") or row.get("safe_name"))
                    if not row.get("valid"):
                        artifact["arms"][arm_name] = {
                            "ok": False,
                            "error": "arm failed validation before oracle pairing",
                        }
                        artifact["ok"] = False
                        continue
                    try:
                        metrics = _paired_vector_oracle_metrics(
                            target_dir=run_dir / "arms" / str(row["safe_name"]),
                            oracle_dir=oracle_dir,
                            repeats=plan.repeats,
                            component=spec.component,
                            top_k=spec.top_k,
                            sources_by_index=sources_by_index,
                        )
                    except ValueError as exc:
                        error = f"paired vector oracle contract failed for {arm_name}: {exc}"
                        _invalidate_row(row, error)
                        artifact["arms"][arm_name] = {"ok": False, "error": error}
                        artifact["ok"] = False
                        continue
                    row_metrics = row.setdefault("metrics", {})
                    if not isinstance(row_metrics, dict):
                        row_metrics = {}
                        row["metrics"] = row_metrics
                    row_metrics.update(metrics)
                    artifact["arms"][arm_name] = {"ok": True, "metrics": metrics}

    (run_dir / "paired_vector_oracle.json").write_text(
        json.dumps(artifact, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return artifact


def _query_metrics_by_index(
    debug_path: Path, *, metrics: list[str]
) -> dict[int, dict[str, float]]:
    if not debug_path.is_file():
        raise ValueError(f"missing query trace: {debug_path}")

    values_by_query: dict[int, dict[str, float]] = {}
    for line_number, line in enumerate(
        debug_path.read_text(encoding="utf-8", errors="replace").splitlines(), start=1
    ):
        if not line.strip():
            continue
        try:
            event = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ValueError(
                f"invalid JSON in {debug_path} at line {line_number}: {exc}"
            ) from exc
        if not isinstance(event, dict) or event.get("search_type") != "hybrid":
            continue
        query_index = event.get("query_index")
        if not isinstance(query_index, int) or query_index < 0:
            raise ValueError(
                f"hybrid trace in {debug_path} has invalid query_index at line {line_number}"
            )
        if query_index in values_by_query:
            raise ValueError(
                f"hybrid trace in {debug_path} duplicates query index {query_index}"
            )
        query_metrics = event.get("query_metrics")
        if not isinstance(query_metrics, dict):
            raise ValueError(
                f"hybrid trace in {debug_path} has no query_metrics at query index "
                f"{query_index}"
            )

        sample: dict[str, float] = {}
        for metric in metrics:
            value = query_metrics.get(metric)
            if not isinstance(value, (int, float)) or not math.isfinite(float(value)):
                raise ValueError(
                    f"hybrid trace in {debug_path} has no finite {metric!r} sample at "
                    f"query index {query_index}"
                )
            sample[metric] = float(value)
        values_by_query[query_index] = sample

    if not values_by_query:
        raise ValueError(f"query trace has no measured hybrid queries: {debug_path}")
    return values_by_query


def _percentile(sorted_values: list[float], probability: float) -> float:
    if not sorted_values:
        raise ValueError("percentile requires at least one value")
    if len(sorted_values) == 1:
        return sorted_values[0]
    position = max(0.0, min(1.0, probability)) * float(len(sorted_values) - 1)
    lower = int(math.floor(position))
    upper = int(math.ceil(position))
    if lower == upper:
        return sorted_values[lower]
    fraction = position - float(lower)
    return sorted_values[lower] * (1.0 - fraction) + sorted_values[upper] * fraction


def _paired_bootstrap_interval(
    query_deltas: list[float],
    *,
    confidence_level: float,
    bootstrap_samples: int,
    seed: int,
) -> tuple[float, float, float]:
    if not query_deltas:
        raise ValueError("paired bootstrap requires at least one query delta")
    point = math.fsum(query_deltas) / len(query_deltas)
    if len(query_deltas) == 1:
        return point, point, point

    rng = random.Random(seed)
    count = len(query_deltas)
    means = []
    for _ in range(bootstrap_samples):
        means.append(
            math.fsum(query_deltas[rng.randrange(count)] for _ in range(count)) / count
        )
    means.sort()
    tail = (1.0 - confidence_level) / 2.0
    return point, _percentile(means, tail), _percentile(means, 1.0 - tail)


def _derived_bootstrap_seed(seed: int, *labels: str) -> int:
    digest = hashlib.sha256(
        (str(seed) + "\0" + "\0".join(labels)).encode("utf-8")
    ).digest()
    return int.from_bytes(digest[:8], "little", signed=False)


def _paired_query_metrics(
    *,
    target_dir: Path,
    baseline_dir: Path,
    arm_name: str,
    repeats: int,
    metrics: list[str],
    confidence_level: float,
    bootstrap_samples: int,
    seed: int,
    sources_by_index: dict[int, str],
) -> dict[str, float]:
    expected_indices = set(sources_by_index)
    deltas: dict[str, dict[int, list[float]]] = {
        metric: {query_index: [] for query_index in expected_indices} for metric in metrics
    }

    target_paths = _debug_paths_for_repeats(target_dir, repeats)
    baseline_paths = _debug_paths_for_repeats(baseline_dir, repeats)
    for repeat, (target_path, baseline_path) in enumerate(
        zip(target_paths, baseline_paths, strict=True)
    ):
        target_samples = _query_metrics_by_index(target_path, metrics=metrics)
        baseline_samples = _query_metrics_by_index(baseline_path, metrics=metrics)
        for label, observed in (
            ("target", set(target_samples)),
            ("baseline", set(baseline_samples)),
        ):
            if observed != expected_indices:
                missing = sorted(expected_indices - observed)
                unexpected = sorted(observed - expected_indices)
                raise ValueError(
                    f"{label} repeat {repeat} query indices do not match mixed-corpus identity "
                    f"(missing={missing}, unexpected={unexpected})"
                )
        for metric in metrics:
            for query_index in expected_indices:
                deltas[metric][query_index].append(
                    target_samples[query_index][metric]
                    - baseline_samples[query_index][metric]
                )

    output: dict[str, float] = {}
    sources = sorted(set(sources_by_index.values()))
    for metric in metrics:
        query_deltas = {
            query_index: math.fsum(samples) / len(samples)
            for query_index, samples in deltas[metric].items()
        }
        all_values = [query_deltas[index] for index in sorted(query_deltas)]
        point, ci_low, ci_high = _paired_bootstrap_interval(
            all_values,
            confidence_level=confidence_level,
            bootstrap_samples=bootstrap_samples,
            seed=_derived_bootstrap_seed(seed, arm_name, metric, "all"),
        )
        prefix = f"paired_query_{metric}"
        output[f"{prefix}_delta"] = point
        output[f"{prefix}_ci_low"] = ci_low
        output[f"{prefix}_ci_high"] = ci_high
        output[f"{prefix}_query_count"] = float(len(all_values))
        output[f"{prefix}_paired_observations"] = float(
            sum(len(samples) for samples in deltas[metric].values())
        )

        for source in sources:
            source_values = [
                query_deltas[index]
                for index in sorted(query_deltas)
                if sources_by_index[index] == source
            ]
            source_point, source_low, source_high = _paired_bootstrap_interval(
                source_values,
                confidence_level=confidence_level,
                bootstrap_samples=bootstrap_samples,
                seed=_derived_bootstrap_seed(seed, arm_name, metric, source),
            )
            output[f"{prefix}_delta_{source}"] = source_point
            output[f"{prefix}_ci_low_{source}"] = source_low
            output[f"{prefix}_ci_high_{source}"] = source_high
            output[f"{prefix}_query_count_{source}"] = float(len(source_values))
    return output


def _apply_paired_query_analysis(
    plan: ExperimentPlan, run_dir: Path, rows: list[dict[str, Any]]
) -> dict[str, Any] | None:
    spec = plan.summarize.paired_query_analysis
    if spec is None:
        return None

    artifact: dict[str, Any] = {
        "ok": True,
        "baseline": plan.baseline,
        "spec": spec.to_dict(),
        "arms": {},
    }
    baseline_row = _find_baseline_row(plan, rows)
    if baseline_row is None:
        error = "paired query analysis requires a valid baseline arm"
        for row in rows:
            _invalidate_row(row, error)
        artifact.update({"ok": False, "error": error})
    else:
        try:
            sources_by_index = _query_sources_by_index(run_dir / spec.identity_file)
        except ValueError as exc:
            error = str(exc)
            for row in rows:
                _invalidate_row(row, error)
            artifact.update({"ok": False, "error": error})
        else:
            baseline_dir = run_dir / "arms" / str(baseline_row["safe_name"])
            for row in rows:
                arm_name = str(row.get("arm") or row.get("safe_name"))
                if not row.get("valid"):
                    artifact["arms"][arm_name] = {
                        "ok": False,
                        "error": "arm failed validation before paired query analysis",
                    }
                    artifact["ok"] = False
                    continue
                try:
                    metrics = _paired_query_metrics(
                        target_dir=run_dir / "arms" / str(row["safe_name"]),
                        baseline_dir=baseline_dir,
                        arm_name=arm_name,
                        repeats=plan.repeats,
                        metrics=spec.metrics,
                        confidence_level=spec.confidence_level,
                        bootstrap_samples=spec.bootstrap_samples,
                        seed=spec.seed,
                        sources_by_index=sources_by_index,
                    )
                except ValueError as exc:
                    error = f"paired query contract failed for {arm_name}: {exc}"
                    _invalidate_row(row, error)
                    artifact["arms"][arm_name] = {"ok": False, "error": error}
                    artifact["ok"] = False
                    continue
                row_metrics = row.setdefault("metrics", {})
                if not isinstance(row_metrics, dict):
                    row_metrics = {}
                    row["metrics"] = row_metrics
                row_metrics.update(metrics)
                artifact["arms"][arm_name] = {"ok": True, "metrics": metrics}

    (run_dir / "paired_query_analysis.json").write_text(
        json.dumps(artifact, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return artifact


def _apply_promotion_gates(
    plan: ExperimentPlan, run_dir: Path, rows: list[dict[str, Any]]
) -> dict[str, Any] | None:
    spec = plan.summarize.promotion_gates
    if spec is None:
        return None

    artifact: dict[str, Any] = {
        "ok": True,
        "baseline": plan.baseline,
        "spec": spec.to_dict(),
        "candidates": {},
    }
    baseline_row = _find_baseline_row(plan, rows)
    if baseline_row is None:
        artifact.update({"ok": False, "error": "promotion gates require a valid baseline arm"})
    else:
        baseline_metrics = baseline_row.get("metrics") or {}
        candidate_names = set(spec.candidate_arms)
        for row in rows:
            arm_name = str(row.get("arm") or row.get("safe_name"))
            if arm_name == baseline_row.get("arm"):
                row["promotion"] = {"eligible": False, "status": "baseline", "reasons": []}
                continue
            if arm_name not in candidate_names and str(row.get("safe_name")) not in candidate_names:
                row["promotion"] = {"eligible": False, "status": "excluded", "reasons": []}
                continue

            reasons: list[str] = []
            checks: dict[str, Any] = {}
            metrics = row.get("metrics") or {}
            if not row.get("valid"):
                reasons.append("arm is invalid; promotion evidence is incomplete")

            quality_key = f"paired_query_{spec.quality_metric}_ci_low"
            quality_value = metrics.get(quality_key)
            quality_pass = isinstance(quality_value, (int, float)) and float(
                quality_value
            ) > spec.quality_ci_lower_min
            checks[quality_key] = {
                "value": quality_value,
                "minimum_exclusive": spec.quality_ci_lower_min,
                "pass": quality_pass,
            }
            if not quality_pass:
                reasons.append(
                    f"{quality_key} must exceed {spec.quality_ci_lower_min}, observed "
                    f"{quality_value!r}"
                )

            for metric in spec.per_source_metrics:
                prefix = f"paired_query_{metric}_delta_"
                source_values = {
                    key.removeprefix(prefix): value
                    for key, value in metrics.items()
                    if key.startswith(prefix) and isinstance(value, (int, float))
                }
                if not source_values:
                    reasons.append(f"missing per-source paired query deltas for {metric}")
                    continue
                for source, value in sorted(source_values.items()):
                    passed = float(value) >= -spec.max_source_regression
                    check_key = f"{metric}:{source}"
                    checks[check_key] = {
                        "delta": float(value),
                        "minimum": -spec.max_source_regression,
                        "pass": passed,
                    }
                    if not passed:
                        reasons.append(
                            f"{metric} regressed by {-float(value):.6g} on {source}; "
                            f"maximum allowed is {spec.max_source_regression:.6g}"
                        )

            for metric, maximum_ratio in sorted(spec.latency_relative_max.items()):
                target_value = metrics.get(metric)
                baseline_value = baseline_metrics.get(metric)
                ratio = None
                if (
                    isinstance(target_value, (int, float))
                    and isinstance(baseline_value, (int, float))
                    and float(baseline_value) > 0.0
                ):
                    ratio = float(target_value) / float(baseline_value)
                passed = ratio is not None and ratio <= maximum_ratio
                checks[metric] = {
                    "target": target_value,
                    "baseline": baseline_value,
                    "ratio": ratio,
                    "maximum_ratio": maximum_ratio,
                    "pass": passed,
                }
                if not passed:
                    reasons.append(
                        f"{metric} ratio must be <= {maximum_ratio}, observed {ratio!r}"
                    )

            result = {
                "eligible": not reasons,
                "status": "pass" if not reasons else "fail",
                "reasons": reasons,
                "checks": checks,
            }
            row["promotion"] = result
            artifact["candidates"][arm_name] = result

    (run_dir / "promotion_gates.json").write_text(
        json.dumps(artifact, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return artifact


def collect_arm_rows(plan: ExperimentPlan, run_dir: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    arms_dir = run_dir / "arms"
    if not arms_dir.is_dir():
        return rows

    arm_meta_by_safe = {a.safe_name: a for a in plan.arms}

    for arm_dir in sorted(p for p in arms_dir.iterdir() if p.is_dir()):
        safe = arm_dir.name
        arm = arm_meta_by_safe.get(safe)
        row: dict[str, Any] = {
            "arm": arm.name if arm else safe,
            "safe_name": safe,
            "factors": arm.factors if arm else {},
            "status": "missing_metrics",
            "valid": False,
            "metrics": {},
        }

        mpath = metrics_path(arm_dir)
        if mpath.exists():
            try:
                doc = read_json(mpath)
                row["status"] = doc.get("status", "ok")
                row["metrics"] = _metric_values(doc if isinstance(doc, dict) else {})
                row["worker"] = doc.get("worker")
            except Exception as exc:  # noqa: BLE001
                row["status"] = f"parse_error:{exc}"

        validation_path = arm_dir / "validation.json"
        if validation_path.exists():
            try:
                v = read_json(validation_path)
                row["valid"] = bool(v.get("ok"))
                row["validation_issues"] = v.get("issues") or []
            except Exception:
                row["valid"] = False

        exit_path = arm_dir / "exit_code"
        if exit_path.exists():
            row["exit_code"] = exit_path.read_text(encoding="utf-8").strip()

        rows.append(row)
    return rows


def write_summary(
    plan: ExperimentPlan,
    run_dir: Path,
    *,
    stamp: str = "",
    git_sha: str | None = None,
    build_dir: str = "",
    dry_run: bool = False,
) -> dict[str, Any]:
    rows = collect_arm_rows(plan, run_dir)
    paired_vector_oracle = None
    paired_query_analysis = None
    promotion_gates = None
    if not dry_run:
        paired_vector_oracle = _apply_paired_vector_oracle(plan, run_dir, rows)
        paired_query_analysis = _apply_paired_query_analysis(plan, run_dir, rows)
        promotion_gates = _apply_promotion_gates(plan, run_dir, rows)
    primary = plan.summarize.primary or sorted(
        {
            key
            for row in rows
            for key in row.get("metrics", {})
            if isinstance(row.get("metrics"), dict)
        }
    )[:8]

    baseline_row = _find_baseline_row(plan, rows)
    base_metrics = (baseline_row.get("metrics") or {}) if baseline_row else {}
    if baseline_row is not None:
        for row in rows:
            deltas: dict[str, Any] = {}
            for key in primary:
                _, d = _delta_cell(row.get("metrics") or {}, base_metrics, key)
                if d is not None:
                    deltas[key] = d
            row["deltas_vs_baseline"] = deltas

    summary: dict[str, Any] = {
        "plan": plan.name,
        "run_dir": str(run_dir),
        "stamp": stamp,
        "primary": primary,
        "baseline": plan.baseline,
        "repeats": plan.repeats,
        "group_by": plan.summarize.group_by,
        "arms": rows,
        "arm_count": len(rows),
        "valid_count": sum(1 for r in rows if r.get("valid")),
        "failed_count": sum(1 for r in rows if not r.get("valid")),
    }
    if paired_vector_oracle is not None:
        summary["paired_vector_oracle"] = paired_vector_oracle
    if paired_query_analysis is not None:
        summary["paired_query_analysis"] = paired_query_analysis
    if promotion_gates is not None:
        summary["promotion_gates"] = promotion_gates

    (run_dir / "summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )

    lines = [
        f"# xplan summary: `{plan.name}`",
        "",
        f"- Run directory: `{run_dir}`",
        f"- Stamp: `{stamp}`",
        f"- Arms: {summary['arm_count']} (valid={summary['valid_count']}, "
        f"failed={summary['failed_count']})",
        f"- Full report: [`REPORT.md`](REPORT.md) · ablation: [`ablation.md`](ablation.md) · "
        f"[`metrics.csv`](metrics.csv)",
        "",
        "## Results",
        "",
    ]

    show_delta = baseline_row is not None
    headers = ["arm", "valid", "status"] + list(primary)
    if show_delta:
        headers += [f"Δ{k}" for k in primary]
    lines.append("| " + " | ".join(headers) + " |")
    lines.append("| " + " | ".join(["---"] * len(headers)) + " |")
    for row in rows:
        cells = [
            f"`{row['arm']}`",
            "yes" if row.get("valid") else "no",
            str(row.get("status")),
        ]
        metrics = row.get("metrics") or {}
        for key in primary:
            val = metrics.get(key, "")
            if isinstance(val, float):
                cells.append(f"{val:.4g}")
            else:
                cells.append(str(val))
        if show_delta:
            is_base = row is baseline_row
            for key in primary:
                if is_base:
                    cells.append("baseline")
                else:
                    cell, _ = _delta_cell(metrics, base_metrics, key)
                    cells.append(cell or "")
        lines.append("| " + " | ".join(cells) + " |")

    if plan.summarize.group_by:
        lines += ["", "## Group-by", ""]
        lines.append(
            "Group keys: " + ", ".join(f"`{k}`" for k in plan.summarize.group_by)
        )
        groups: dict[str, list[dict[str, Any]]] = {}
        for row in rows:
            factors = row.get("factors") or {}
            key = "|".join(
                f"{g}={factors.get(g, row.get('arm'))}" for g in plan.summarize.group_by
            )
            groups.setdefault(key, []).append(row)
        for key, group_rows in sorted(groups.items()):
            lines.append(f"- `{key}`: {len(group_rows)} arm(s)")

    lines += [
        "",
        "## Notes",
        "",
        "- `valid=no` means the arm failed plan validation (missing files/metrics or bad status).",
        "- Open `REPORT.md` for full metrics, factors, ablation deltas, and per-arm detail.",
    ]
    if show_delta:
        lines += [
            f"- Δ columns are vs baseline arm `{plan.baseline}` (repeats={plan.repeats}). "
            "`*` = delta exceeds pooled run-to-run stdev (real); `~` = within noise; "
            "no mark = single-run (n=1), variance unknown.",
        ]
        if plan.repeats <= 1:
            lines += [
                "- **Decision caution:** `repeats=1` — do not promote/kill a lever from bare Δ; "
                "re-run with `repeats>=3` (see `topology_optimize_v2`, "
                "`topology_vector_seed_ablation`, `topology_purity_validate`).",
            ]
    lines += [""]
    (run_dir / "summary.md").write_text("\n".join(lines), encoding="utf-8")

    # Granular engineering docs (always generated with summary).
    write_report(
        plan,
        run_dir,
        rows,
        stamp=stamp,
        git_sha=git_sha,
        build_dir=build_dir,
        dry_run=dry_run,
    )
    return summary
