"""Granular engineering reports for xplan runs (generated into the artifact dir)."""

from __future__ import annotations

import csv
import json
import statistics
from pathlib import Path
from typing import Any

from artifacts import host_info, read_json
from model import ExperimentPlan
from significance import is_regression, metric_direction, welch_from_metrics


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


def _baseline_row(
    rows: list[dict[str, Any]], declared_baseline: str | None
) -> dict[str, Any] | None:
    if not declared_baseline:
        return None
    for row in rows:
        if not row.get("valid"):
            continue
        if row.get("arm") == declared_baseline or row.get("safe_name") == declared_baseline:
            return row
    return None


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
                test = welch_from_metrics(m, b_metrics, k) or {}
                deltas[k] = {
                    "abs": abs_d,
                    "rel": rel,
                    "baseline": bv,
                    "arm": av,
                    "marker": test.get("marker", ""),
                    "significant": test.get("significant"),
                    "t": test.get("t"),
                    "df": test.get("df"),
                    "p_value": test.get("p_value"),
                }
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

    baseline = _baseline_row(rows, plan.baseline)
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
            "Relative delta = (arm − baseline) / baseline. "
            "`*` = Welch t-test significant at 95%; `~` = within noise; "
            "no mark = not testable (n<2).",
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
                mark = f" {dd['marker']}" if dd.get("marker") else ""
                lines.append(
                    f"| `{d['arm']}` | {_fmt(dd.get('baseline'))} | {_fmt(dd.get('arm'))} | "
                    f"{_fmt(dd.get('abs'))}{mark} | {rel_s} |"
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
        "- `REPORT.md` (this file)",
        "- `summary.md` / `summary.json` (compact)",
        "- `report.json` (full machine-readable)",
        "- `metrics.csv`",
        "- `ablation.md` (delta focus)",
        "- `run_manifest.json`, `plan.resolved.json`, `mode_manifest.json`, `results.json`",
        "- `arms/<arm>/metrics.json` (+ worker raw outputs)",
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


class CompareRefused(ValueError):
    """The two runs are not comparable (different plan/config/binary/corpus, or dry-run)."""

    def __init__(self, mismatches: list[str]):
        self.mismatches = list(mismatches)
        super().__init__("; ".join(self.mismatches))


_CORPUS_PARAM_KEYS = ("dataset", "dataset_path", "corpus_size", "num_queries", "topk")


def _load_report(d: Path) -> dict[str, Any]:
    for name in ("report.json", "summary.json"):
        p = d / name
        if p.is_file():
            return read_json(p)
    raise FileNotFoundError(f"no report.json/summary.json in {d}")


def _read_optional(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {}
    try:
        doc = read_json(path)
    except Exception:  # noqa: BLE001 - unreadable provenance is treated as unknown
        return {}
    return doc if isinstance(doc, dict) else {}


def run_identity(d: Path) -> dict[str, Any]:
    """What must match for two runs to be comparable: plan, config, binary, corpus."""
    report = _read_optional(d / "report.json") or _read_optional(d / "summary.json")
    resolved = _read_optional(d / "plan.resolved.json")
    manifest = _read_optional(d / "run_manifest.json")
    mode = _read_optional(d / "mode_manifest.json")
    fixed_params = ((resolved.get("fixed") or {}).get("params")) or {}
    corpus: dict[str, Any] = {}
    for arm in resolved.get("arms") or []:
        merged = {**fixed_params, **(arm.get("factors") or {}), **(arm.get("params") or {})}
        corpus[str(arm.get("name"))] = {
            k: merged[k] for k in _CORPUS_PARAM_KEYS if k in merged
        }
    effective = manifest.get("effective") or {}
    return {
        "plan": report.get("plan") or resolved.get("name"),
        "config_hash": manifest.get("config_hash") or resolved.get("bench_config_hash"),
        "binary_sha256": mode.get("retrieval_quality_binary_sha256"),
        "corpus": corpus,
        "dry_run": bool(effective.get("dry_run") or report.get("dry_run")),
    }


def identity_mismatches(a: dict[str, Any], b: dict[str, Any], common_arms: list[str]) -> list[str]:
    out: list[str] = []
    if a["dry_run"] or b["dry_run"]:
        which = "A" if a["dry_run"] else "B"
        out.append(f"run {which} is a dry-run: it has no measurements to compare")
    if a["plan"] != b["plan"]:
        out.append(f"plan differs: A={a['plan']!r} B={b['plan']!r}")
    if a["config_hash"] != b["config_hash"]:
        out.append(f"config hash differs: A={a['config_hash']!r} B={b['config_hash']!r}")
    if a["binary_sha256"] != b["binary_sha256"]:
        out.append(
            f"binary sha256 differs: A={a['binary_sha256']!r} B={b['binary_sha256']!r}"
        )
    for arm in common_arms:
        ca, cb = a["corpus"].get(arm), b["corpus"].get(arm)
        if ca != cb:
            out.append(f"corpus differs for arm {arm!r}: A={ca!r} B={cb!r}")
            break
    return out


def _compare_out_paths(a_dir: Path, b_dir: Path, out_path: Path | None) -> tuple[Path, Path]:
    """(markdown path, json path). Never defaults into either run directory."""
    if out_path is None:
        sibling = b_dir.parent / f"compare-{a_dir.name}-vs-{b_dir.name}"
        return sibling / "compare.md", sibling / "compare.json"
    if out_path.suffix:  # explicit file
        return out_path, out_path.with_name("compare.json")
    return out_path / "compare.md", out_path / "compare.json"


def compare_reports(
    a_dir: Path,
    b_dir: Path,
    out_path: Path | None = None,
    *,
    force: bool = False,
) -> dict[str, Any]:
    """Compare run A (baseline) with run B (candidate); B - A, Welch-tested per metric.

    Raises CompareRefused when the runs differ in plan, config hash, binary sha256 or corpus
    (or either is a dry-run) unless `force`. The returned dict carries `regressions`: the
    metrics that moved the wrong way with statistical significance.
    """
    a = _load_report(a_dir)
    b = _load_report(b_dir)
    a_arms = {r.get("arm"): r for r in a.get("arms", [])}
    b_arms = {r.get("arm"): r for r in b.get("arms", [])}
    common = sorted(set(a_arms) & set(b_arms))

    ident_a, ident_b = run_identity(a_dir), run_identity(b_dir)
    mismatches = identity_mismatches(ident_a, ident_b, common)
    if mismatches and not force:
        raise CompareRefused(mismatches)

    comparison: dict[str, Any] = {
        "a": str(a_dir),
        "b": str(b_dir),
        "plan_a": a.get("plan"),
        "plan_b": b.get("plan"),
        "identity_a": ident_a,
        "identity_b": ident_b,
        "identity_mismatches": mismatches,
        "forced": bool(force and mismatches),
        "arms": [],
        "regressions": [],
        "skipped_invalid_arms": [],
    }
    lines = [
        "# xplan compare",
        "",
        f"- A (baseline): `{a_dir}`",
        f"- B (candidate): `{b_dir}`",
        "- Δ = B − A. `*` = Welch t-test significant at 95%; `~` = within noise; "
        "no mark = not testable (n<2). `REGRESSION` = significant move in the worse direction.",
    ]
    if mismatches:
        lines += ["", "## WARNING: runs are not equivalent (--force)", ""]
        lines += [f"- {m}" for m in mismatches]
    lines += [
        "",
        "| arm | metric | A | B | abs Δ | rel Δ | better | verdict |",
        "| --- | --- | ---: | ---: | ---: | ---: | :---: | --- |",
    ]
    for arm in common:
        if not (a_arms[arm].get("valid", True) and b_arms[arm].get("valid", True)):
            comparison["skipped_invalid_arms"].append(arm)
            continue
        ma = a_arms[arm].get("metrics") or {}
        mb = b_arms[arm].get("metrics") or {}
        arm_entry: dict[str, Any] = {"arm": arm, "metrics": {}}
        for k in sorted(set(ma) | set(mb)):
            # Spread/count bookkeeping is input to the test, not a metric to compare.
            if k.endswith("_stdev") or k.endswith("_n"):
                continue
            va, vb = ma.get(k), mb.get(k)
            if isinstance(va, bool) or isinstance(vb, bool):
                continue
            if not (isinstance(va, (int, float)) and isinstance(vb, (int, float))):
                continue
            abs_d = float(vb) - float(va)
            rel = abs_d / float(va) if float(va) != 0 else None
            test = welch_from_metrics(mb, ma, k) or {}
            direction = metric_direction(k)
            regression = bool(test.get("significant")) and is_regression(k, abs_d)
            improvement = (
                bool(test.get("significant")) and not regression and direction != "unknown"
            )
            verdict = "REGRESSION" if regression else ("improved" if improvement else "")
            entry = {
                "a": va,
                "b": vb,
                "abs": abs_d,
                "rel": rel,
                "direction": direction,
                "marker": test.get("marker", ""),
                "significant": test.get("significant"),
                "t": test.get("t"),
                "df": test.get("df"),
                "p_value": test.get("p_value"),
                "regression": regression,
            }
            arm_entry["metrics"][k] = entry
            if regression:
                comparison["regressions"].append({"arm": arm, "metric": k, **entry})
            rel_s = f"{rel*100:.2f}%" if isinstance(rel, float) else ""
            mark = f" {entry['marker']}" if entry["marker"] else ""
            lines.append(
                f"| `{arm}` | `{k}` | {_fmt(va)} | {_fmt(vb)} | {_fmt(abs_d)}{mark} | "
                f"{rel_s} | {direction[0] if direction != 'unknown' else '?'} | {verdict} |"
            )
        comparison["arms"].append(arm_entry)
    lines.append("")
    only_a = sorted(set(a_arms) - set(b_arms))
    only_b = sorted(set(b_arms) - set(a_arms))
    if comparison["skipped_invalid_arms"]:
        lines += [
            "## Skipped (invalid in A or B)",
            "",
            ", ".join(f"`{x}`" for x in comparison["skipped_invalid_arms"]),
            "",
        ]
    if only_a:
        lines += ["## Only in A", "", ", ".join(f"`{x}`" for x in only_a), ""]
    if only_b:
        lines += ["## Only in B", "", ", ".join(f"`{x}`" for x in only_b), ""]
    if comparison["regressions"]:
        lines += ["## Regressions", ""]
        for r in comparison["regressions"]:
            lines.append(f"- `{r['arm']}` `{r['metric']}`: {_fmt(r['a'])} -> {_fmt(r['b'])}")
        lines.append("")

    md_path, json_path = _compare_out_paths(a_dir, b_dir, out_path)
    md_path.parent.mkdir(parents=True, exist_ok=True)
    md_path.write_text("\n".join(lines), encoding="utf-8")
    json_path.write_text(
        json.dumps(comparison, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8"
    )
    comparison["out_md"] = str(md_path)
    comparison["out_json"] = str(json_path)
    return comparison
