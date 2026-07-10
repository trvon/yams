"""Summarize multi-arm xplan runs into compact + granular engineering reports."""

from __future__ import annotations

import json
import math
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
    pooled = None
    if isinstance(a_sd, (int, float)) and isinstance(b_sd, (int, float)):
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
