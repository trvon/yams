"""Granular engineering reports for xplan runs (generated into the artifact dir)."""

from __future__ import annotations

import csv
import json
import statistics
from pathlib import Path
from typing import Any

from artifacts import host_info, read_json
from model import ExperimentPlan


def _fmt(val: Any) -> str:
    if val is None or val == "":
        return ""
    if isinstance(val, float):
        if abs(val) >= 1000 or (0 < abs(val) < 0.01):
            return f"{val:.4g}"
        return f"{val:.4g}"
    if isinstance(val, bool):
        return "yes" if val else "no"
    return str(val)


def _all_metric_keys(rows: list[dict[str, Any]]) -> list[str]:
    keys: set[str] = set()
    for row in rows:
        metrics = row.get("metrics") or {}
        if isinstance(metrics, dict):
            keys.update(str(k) for k in metrics.keys())
    return sorted(keys)


def _baseline_row(rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    for name in ("baseline", "baseline_hybrid", "baseline_no_vectors"):
        for row in rows:
            if row.get("arm") == name or row.get("safe_name") == name:
                return row
    # Prefer arm that has factors empty or all "on"
    for row in rows:
        factors = row.get("factors") or {}
        if not factors:
            return row
        if all(str(v).lower() in {"on", "1", "true", "enabled", "hybrid"} for v in factors.values()):
            return row
    return rows[0] if rows else None


def _ablation_deltas(
    rows: list[dict[str, Any]], baseline: dict[str, Any] | None, keys: list[str]
) -> list[dict[str, Any]]:
    if not baseline or not keys:
        return []
    b_metrics = baseline.get("metrics") or {}
    out: list[dict[str, Any]] = []
    for row in rows:
        if row is baseline or row.get("arm") == baseline.get("arm"):
            continue
        m = row.get("metrics") or {}
        deltas: dict[str, Any] = {}
        for k in keys:
            bv, av = b_metrics.get(k), m.get(k)
            if isinstance(bv, (int, float)) and isinstance(av, (int, float)):
                abs_d = float(av) - float(bv)
                rel = (abs_d / float(bv)) if float(bv) != 0 else None
                deltas[k] = {"abs": abs_d, "rel": rel, "baseline": bv, "arm": av}
        out.append(
            {
                "arm": row.get("arm"),
                "factors": row.get("factors") or {},
                "valid": row.get("valid"),
                "status": row.get("status"),
                "deltas": deltas,
            }
        )
    return out


def _load_arm_extras(arm_dir: Path) -> dict[str, Any]:
    extras: dict[str, Any] = {"dir": str(arm_dir)}
    metrics_path = arm_dir / "metrics.json"
    if metrics_path.is_file():
        try:
            doc = read_json(metrics_path)
            extras["message"] = doc.get("message")
            extras["attributes"] = doc.get("attributes") or {}
            extras["steps"] = doc.get("steps") or []
        except Exception:
            pass
    for name in ("quality_parse.json", "validation.json", "arm.json", "mode_manifest.json"):
        p = arm_dir / name
        if p.is_file():
            try:
                extras[name.replace(".json", "")] = read_json(p)
            except Exception:
                extras[name.replace(".json", "")] = None
    # File inventory
    extras["files"] = sorted(p.name for p in arm_dir.iterdir() if p.is_file())
    return extras


def write_metrics_csv(path: Path, rows: list[dict[str, Any]], metric_keys: list[str]) -> None:
    fieldnames = ["arm", "safe_name", "valid", "status", "exit_code"] + metric_keys
    # Include factor columns
    factor_keys: set[str] = set()
    for row in rows:
        factor_keys.update((row.get("factors") or {}).keys())
    factor_cols = sorted(str(k) for k in factor_keys)
    fieldnames = (
        ["arm", "safe_name", "valid", "status", "exit_code"]
        + [f"factor_{k}" for k in factor_cols]
        + metric_keys
    )
    with path.open("w", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        for row in rows:
            rec: dict[str, Any] = {
                "arm": row.get("arm"),
                "safe_name": row.get("safe_name"),
                "valid": row.get("valid"),
                "status": row.get("status"),
                "exit_code": row.get("exit_code", ""),
            }
            factors = row.get("factors") or {}
            for k in factor_cols:
                rec[f"factor_{k}"] = factors.get(k, "")
            metrics = row.get("metrics") or {}
            for k in metric_keys:
                rec[k] = metrics.get(k, "")
            w.writerow(rec)


def write_report(
    plan: ExperimentPlan,
    run_dir: Path,
    rows: list[dict[str, Any]],
    *,
    stamp: str = "",
    git_sha: str | None = None,
    build_dir: str = "",
    dry_run: bool = False,
) -> dict[str, Any]:
    """Write REPORT.md, ablation.md, metrics.csv, report.json into run_dir."""
    primary = list(plan.summarize.primary) if plan.summarize.primary else []
    all_keys = _all_metric_keys(rows)
    if not primary:
        primary = all_keys[:10]
    # Prefer primary first, then remaining keys
    ordered_keys = list(primary) + [k for k in all_keys if k not in primary]

    baseline = _baseline_row(rows)
    deltas = _ablation_deltas(rows, baseline, primary or ordered_keys[:8])

    host = host_info()
    mode_manifest = {}
    mm = run_dir / "mode_manifest.json"
    if mm.is_file():
        try:
            mode_manifest = read_json(mm)
        except Exception:
            mode_manifest = {}

    arm_dirs = {
        p.name: p for p in (run_dir / "arms").iterdir() if p.is_dir()
    } if (run_dir / "arms").is_dir() else {}
    arm_details = []
    for row in rows:
        safe = row.get("safe_name") or row.get("arm")
        d = arm_dirs.get(str(safe))
        detail = dict(row)
        if d:
            detail["extras"] = _load_arm_extras(d)
        arm_details.append(detail)

    report: dict[str, Any] = {
        "plan": plan.name,
        "description": plan.description,
        "stamp": stamp,
        "run_dir": str(run_dir),
        "build_dir": build_dir,
        "git_sha": git_sha,
        "dry_run": dry_run,
        "host": host,
        "mode": plan.mode,
        "mode_manifest": mode_manifest,
        "primary_metrics": primary,
        "all_metric_keys": ordered_keys,
        "arm_count": len(rows),
        "valid_count": sum(1 for r in rows if r.get("valid")),
        "failed_count": sum(1 for r in rows if not r.get("valid")),
        "baseline_arm": (baseline or {}).get("arm"),
        "arms": rows,
        "ablation_deltas": deltas,
        "arm_details": arm_details,
        "group_by": plan.summarize.group_by,
    }

    # Numeric highlights for primary metrics across valid arms
    highlights: dict[str, Any] = {}
    for key in primary:
        vals = [
            float(r["metrics"][key])
            for r in rows
            if r.get("valid")
            and isinstance((r.get("metrics") or {}).get(key), (int, float))
        ]
        if not vals:
            continue
        highlights[key] = {
            "min": min(vals),
            "max": max(vals),
            "mean": statistics.mean(vals),
            "n": len(vals),
        }
    report["highlights"] = highlights

    (run_dir / "report.json").write_text(
        json.dumps(report, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8"
    )
    write_metrics_csv(run_dir / "metrics.csv", rows, ordered_keys)

    # --- REPORT.md ---
    lines: list[str] = [
        f"# xplan report: `{plan.name}`",
        "",
        "## Run",
        "",
        f"| Field | Value |",
        f"| --- | --- |",
        f"| Plan | `{plan.name}` |",
        f"| Stamp | `{stamp}` |",
        f"| Artifact | `{run_dir}` |",
        f"| Build | `{build_dir}` |",
        f"| Git | `{git_sha or 'unknown'}` |",
        f"| Mode | `{plan.mode}` |",
        f"| Dry-run | `{dry_run}` |",
        f"| Host | `{host.get('system')} {host.get('machine')} ({host.get('cpu_count')} cpus)` |",
        f"| Arms | {report['arm_count']} (valid={report['valid_count']}, failed={report['failed_count']}) |",
        f"| Baseline arm | `{report['baseline_arm'] or 'n/a'}` |",
        "",
    ]
    if plan.description:
        lines += ["## Description", "", plan.description.strip(), ""]

    lines += ["## Primary metrics", ""]
    headers = ["arm", "valid", "status"] + list(primary)
    lines.append("| " + " | ".join(headers) + " |")
    lines.append("| " + " | ".join(["---"] * len(headers)) + " |")
    for row in rows:
        cells = [
            f"`{row.get('arm')}`",
            "yes" if row.get("valid") else "no",
            str(row.get("status")),
        ]
        m = row.get("metrics") or {}
        for k in primary:
            cells.append(_fmt(m.get(k, "")))
        lines.append("| " + " | ".join(cells) + " |")
    lines.append("")

    if highlights:
        lines += ["## Highlights (valid arms)", ""]
        lines.append("| metric | min | mean | max | n |")
        lines.append("| --- | ---: | ---: | ---: | ---: |")
        for k, h in highlights.items():
            lines.append(
                f"| `{k}` | {_fmt(h['min'])} | {_fmt(h['mean'])} | {_fmt(h['max'])} | {h['n']} |"
            )
        lines.append("")

    if deltas:
        lines += [
            "## Ablation deltas vs baseline",
            "",
            f"Baseline: `{report['baseline_arm']}`. "
            "Relative delta = (arm − baseline) / baseline.",
            "",
        ]
        # One small table per primary metric
        for key in primary[:8]:
            lines.append(f"### `{key}`")
            lines.append("")
            lines.append("| arm | baseline | arm value | abs Δ | rel Δ |")
            lines.append("| --- | ---: | ---: | ---: | ---: |")
            for d in deltas:
                dd = (d.get("deltas") or {}).get(key)
                if not dd:
                    lines.append(f"| `{d['arm']}` |  |  |  |  |")
                    continue
                rel = dd.get("rel")
                rel_s = f"{rel*100:.2f}%" if isinstance(rel, float) else ""
                lines.append(
                    f"| `{d['arm']}` | {_fmt(dd.get('baseline'))} | {_fmt(dd.get('arm'))} | "
                    f"{_fmt(dd.get('abs'))} | {rel_s} |"
                )
            lines.append("")

    # Factors matrix
    factor_keys: set[str] = set()
    for row in rows:
        factor_keys.update((row.get("factors") or {}).keys())
    if factor_keys:
        fcols = sorted(factor_keys)
        lines += ["## Factors", ""]
        lines.append("| arm | " + " | ".join(fcols) + " |")
        lines.append("| --- | " + " | ".join(["---"] * len(fcols)) + " |")
        for row in rows:
            factors = row.get("factors") or {}
            cells = [f"`{row.get('arm')}`"] + [_fmt(factors.get(k, "")) for k in fcols]
            lines.append("| " + " | ".join(cells) + " |")
        lines.append("")

    # Full metrics
    if ordered_keys:
        lines += ["## All metrics", ""]
        # Split wide tables into chunks of 6 metrics
        chunk = 6
        for i in range(0, len(ordered_keys), chunk):
            keys = ordered_keys[i : i + chunk]
            lines.append("| arm | " + " | ".join(f"`{k}`" for k in keys) + " |")
            lines.append("| --- | " + " | ".join(["---:"] * len(keys)) + " |")
            for row in rows:
                m = row.get("metrics") or {}
                cells = [f"`{row.get('arm')}`"] + [_fmt(m.get(k, "")) for k in keys]
                lines.append("| " + " | ".join(cells) + " |")
            lines.append("")

    # Per-arm detail
    lines += ["## Per-arm detail", ""]
    for detail in arm_details:
        lines.append(f"### `{detail.get('arm')}`")
        lines.append("")
        lines.append(f"- status: `{detail.get('status')}` valid={detail.get('valid')}")
        lines.append(f"- exit_code: `{detail.get('exit_code', '')}`")
        factors = detail.get("factors") or {}
        if factors:
            lines.append(f"- factors: `{json.dumps(factors, sort_keys=True)}`")
        extras = detail.get("extras") or {}
        msg = extras.get("message")
        if msg:
            lines.append(f"- message: {msg}")
        attrs = extras.get("attributes") or {}
        abl = attrs.get("ablation") if isinstance(attrs, dict) else None
        if isinstance(abl, dict) and abl.get("label"):
            lines.append(f"- ablation: `{abl.get('label')}`")
            if abl.get("weight_zeros"):
                lines.append(f"- weight_zeros: `{abl.get('weight_zeros')}`")
        issues = detail.get("validation_issues") or []
        if issues:
            lines.append(f"- validation: `{json.dumps(issues)}`")
        files = extras.get("files") or []
        if files:
            lines.append(f"- files: {', '.join(f'`{f}`' for f in files[:20])}")
        lines.append("")

    lines += [
        "## Artifacts",
        "",
        f"- `REPORT.md` (this file)",
        f"- `summary.md` / `summary.json` (compact)",
        f"- `report.json` (full machine-readable)",
        f"- `metrics.csv`",
        f"- `ablation.md` (delta focus)",
        f"- `plan.resolved.json`, `mode_manifest.json`, `results.json`",
        f"- `arms/<arm>/metrics.json` (+ worker raw outputs)",
        "",
    ]
    (run_dir / "REPORT.md").write_text("\n".join(lines), encoding="utf-8")

    # --- ablation.md ---
    alines = [
        f"# Ablation: `{plan.name}`",
        "",
        f"Baseline arm: `{report['baseline_arm'] or 'n/a'}`",
        "",
    ]
    if not deltas:
        alines += ["No non-baseline arms to compare.", ""]
    else:
        alines += [
            "Relative Δ = (arm − baseline) / baseline. Negative means worse if higher-is-better.",
            "",
            "| arm | factors | " + " | ".join(f"{k} rel%" for k in primary[:6]) + " |",
            "| --- | --- | " + " | ".join(["---:"] * min(6, len(primary))) + " |",
        ]
        for d in deltas:
            fac = json.dumps(d.get("factors") or {}, sort_keys=True)
            cells = [f"`{d['arm']}`", f"`{fac}`"]
            for k in primary[:6]:
                dd = (d.get("deltas") or {}).get(k) or {}
                rel = dd.get("rel")
                cells.append(f"{rel*100:.2f}%" if isinstance(rel, float) else "")
            alines.append("| " + " | ".join(cells) + " |")
        alines.append("")
        # Ranking hints: for each primary metric, arms sorted by abs delta
        alines += ["## Ranking hints (largest absolute change first)", ""]
        for k in primary[:6]:
            ranked = []
            for d in deltas:
                dd = (d.get("deltas") or {}).get(k)
                if dd and isinstance(dd.get("abs"), (int, float)):
                    ranked.append((abs(float(dd["abs"])), d["arm"], dd))
            ranked.sort(reverse=True)
            if not ranked:
                continue
            alines.append(f"### `{k}`")
            alines.append("")
            for _, arm, dd in ranked[:8]:
                rel = dd.get("rel")
                rel_s = f"{rel*100:.2f}%" if isinstance(rel, float) else "n/a"
                alines.append(
                    f"- `{arm}`: abs={_fmt(dd.get('abs'))} rel={rel_s} "
                    f"(baseline={_fmt(dd.get('baseline'))} → arm={_fmt(dd.get('arm'))})"
                )
            alines.append("")
    (run_dir / "ablation.md").write_text("\n".join(alines), encoding="utf-8")

    return report


def compare_reports(a_dir: Path, b_dir: Path, out_path: Path | None = None) -> dict[str, Any]:
    """Compare two report.json / summary.json directories."""
    def load(d: Path) -> dict[str, Any]:
        for name in ("report.json", "summary.json"):
            p = d / name
            if p.is_file():
                return read_json(p)
        raise FileNotFoundError(f"no report.json/summary.json in {d}")

    a = load(a_dir)
    b = load(b_dir)
    a_arms = {r.get("arm"): r for r in a.get("arms", [])}
    b_arms = {r.get("arm"): r for r in b.get("arms", [])}
    common = sorted(set(a_arms) & set(b_arms))
    keys: set[str] = set()
    for name in common:
        keys.update((a_arms[name].get("metrics") or {}).keys())
        keys.update((b_arms[name].get("metrics") or {}).keys())
    keys_l = sorted(keys)

    comparison: dict[str, Any] = {
        "a": str(a_dir),
        "b": str(b_dir),
        "plan_a": a.get("plan"),
        "plan_b": b.get("plan"),
        "arms": [],
    }
    lines = [
        f"# xplan compare",
        "",
        f"- A: `{a_dir}`",
        f"- B: `{b_dir}`",
        "",
        "| arm | metric | A | B | abs Δ | rel Δ |",
        "| --- | --- | ---: | ---: | ---: | ---: |",
    ]
    for arm in common:
        ma = a_arms[arm].get("metrics") or {}
        mb = b_arms[arm].get("metrics") or {}
        arm_entry: dict[str, Any] = {"arm": arm, "metrics": {}}
        for k in keys_l:
            va, vb = ma.get(k), mb.get(k)
            if not (isinstance(va, (int, float)) and isinstance(vb, (int, float))):
                continue
            abs_d = float(vb) - float(va)
            rel = abs_d / float(va) if float(va) != 0 else None
            arm_entry["metrics"][k] = {"a": va, "b": vb, "abs": abs_d, "rel": rel}
            rel_s = f"{rel*100:.2f}%" if isinstance(rel, float) else ""
            lines.append(
                f"| `{arm}` | `{k}` | {_fmt(va)} | {_fmt(vb)} | {_fmt(abs_d)} | {rel_s} |"
            )
        comparison["arms"].append(arm_entry)
    lines.append("")
    only_a = sorted(set(a_arms) - set(b_arms))
    only_b = sorted(set(b_arms) - set(a_arms))
    if only_a:
        lines += ["## Only in A", "", ", ".join(f"`{x}`" for x in only_a), ""]
    if only_b:
        lines += ["## Only in B", "", ", ".join(f"`{x}`" for x in only_b), ""]

    text = "\n".join(lines)
    if out_path is None:
        out_path = b_dir / "compare.md"
    out_path.write_text(text, encoding="utf-8")
    (out_path.with_suffix(".json") if out_path.suffix else Path(str(out_path) + ".json")).write_text(
        json.dumps(comparison, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    # Prefer sibling compare.json next to compare.md
    out_path.with_name("compare.json").write_text(
        json.dumps(comparison, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return comparison
