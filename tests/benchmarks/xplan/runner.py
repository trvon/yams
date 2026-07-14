#!/usr/bin/env python3
"""yams xplan — unified experiment plan runner for daemon-shaped benchmarks.

Usage:
  python3 tests/benchmarks/xplan/runner.py run plans/ingest_pipeline.yaml
  python3 tests/benchmarks/xplan/runner.py run tests/benchmarks/xplan/plans/ingest_pipeline.yaml \\
      --build-dir build/release --dry-run
  python3 tests/benchmarks/xplan/runner.py list-workers
  python3 tests/benchmarks/xplan/runner.py self-test
"""

from __future__ import annotations

import argparse
import json
import os
import statistics
import sys
import traceback
from dataclasses import replace
from pathlib import Path
from typing import Any

XPLAN_ROOT = Path(__file__).resolve().parent
if str(XPLAN_ROOT) not in sys.path:
    sys.path.insert(0, str(XPLAN_ROOT))

from artifacts import (  # noqa: E402
    arm_layout,
    ensure_dir,
    host_info,
    try_git_sha,
    utc_stamp,
    write_json,
    write_mode_manifest,
)
from model import ArmContext, ExperimentPlan  # noqa: E402
from summarize import write_summary  # noqa: E402
from validate import validate_arm  # noqa: E402
from workers import REGISTRY, get_worker  # noqa: E402
from workers.base import WorkerContext  # noqa: E402


def aggregate_reps(rep_metrics: list[dict[str, Any]]) -> dict[str, Any]:
    """Aggregate per-repeat metric dicts into mean + stdev + n for numeric keys.

    Numeric keys → mean at `{key}`, plus `{key}_stdev` (0.0 if n==1) and `{key}_n`.
    Non-numeric keys → last observed value. Single rep → values pass through unchanged
    (still emits `{key}_n=1`) so downstream code has a uniform shape.
    """
    if not rep_metrics:
        return {}
    if len(rep_metrics) == 1:
        return dict(rep_metrics[0])
    keys: list[str] = []
    seen: set[str] = set()
    for rm in rep_metrics:
        for k in rm:
            if k not in seen:
                seen.add(k)
                keys.append(k)
    out: dict[str, Any] = {}
    for key in keys:
        vals = [rm[key] for rm in rep_metrics if key in rm]
        numeric: list[float] = []
        for v in vals:
            if isinstance(v, bool):
                continue
            if isinstance(v, (int, float)):
                numeric.append(float(v))
        if numeric and len(numeric) == len(vals):
            out[key] = statistics.mean(numeric)
            out[f"{key}_stdev"] = statistics.pstdev(numeric) if len(numeric) > 1 else 0.0
            out[f"{key}_n"] = float(len(numeric))
        else:
            out[key] = vals[-1]
    return out


def repo_root_from(path: Path | None = None) -> Path:
    here = (path or XPLAN_ROOT).resolve()
    for candidate in [here, *here.parents]:
        if (candidate / "meson.build").exists() and (candidate / "tests" / "benchmarks").exists():
            return candidate
    # Fallback: xplan is tests/benchmarks/xplan → repo is parents[2]
    return XPLAN_ROOT.parents[2]


def resolve_plan_path(plan_arg: str, repo_root: Path) -> Path:
    p = Path(plan_arg)
    candidates = [
        p,
        Path.cwd() / p,
        repo_root / p,
        XPLAN_ROOT / p,
        XPLAN_ROOT / "plans" / p,
        XPLAN_ROOT / "plans" / f"{p.name}",
    ]
    if not p.suffix:
        # Prefer JSON (zero third-party deps); YAML when present + PyYAML installed.
        candidates.extend(
            [
                XPLAN_ROOT / "plans" / f"{p.name}.json",
                XPLAN_ROOT / "plans" / f"{p.name}.yaml",
                XPLAN_ROOT / "plans" / f"{p.name}.yml",
            ]
        )
    for c in candidates:
        if c.is_file():
            # If YAML chosen but unreadable without PyYAML, fall back to sibling JSON.
            if c.suffix.lower() in {".yaml", ".yml"}:
                try:
                    import yaml as _yaml  # noqa: F401
                except ImportError:
                    sibling = c.with_suffix(".json")
                    if sibling.is_file():
                        return sibling.resolve()
            return c.resolve()
    raise FileNotFoundError(f"plan not found: {plan_arg}")


def resolve_build_dir(repo_root: Path, plan: ExperimentPlan, override: str | None) -> Path:
    raw = override or os.environ.get("YAMS_BENCH_BUILD_DIR") or plan.build_dir
    path = Path(raw)
    if not path.is_absolute():
        path = repo_root / path
    return path.resolve()


def prepare_fresh_run_dir(run_dir: Path) -> None:
    """Create a new run directory, refusing to mix with existing artifacts."""
    if run_dir.exists():
        if not run_dir.is_dir():
            raise FileExistsError(f"xplan run path is not a directory: {run_dir}")
        if any(run_dir.iterdir()):
            raise FileExistsError(
                f"refusing to reuse nonempty xplan run directory: {run_dir}"
            )
    else:
        run_dir.mkdir(parents=True)


def load_resolved_report_plan(run_dir: Path) -> ExperimentPlan:
    """Load the immutable plan snapshot recorded by an xplan run."""
    plan_path = run_dir / "plan.resolved.json"
    if not plan_path.is_file():
        raise FileNotFoundError(f"missing plan.resolved.json in {run_dir}")
    return ExperimentPlan.load(plan_path)


def cmd_list_workers(_: argparse.Namespace) -> int:
    for name in sorted(REGISTRY):
        print(name)
    return 0


def cmd_list_plans(_: argparse.Namespace) -> int:
    plans_dir = XPLAN_ROOT / "plans"
    for path in sorted(plans_dir.glob("*.json")):
        try:
            plan = ExperimentPlan.load(path)
            print(
                f"{plan.name:22} {path.name:28} arms={len(plan.arms):2} steps={len(plan.steps)}"
            )
        except Exception as exc:  # noqa: BLE001
            print(f"{path.stem:22} {path.name:28} ERROR {exc}")
    return 0


def cmd_download_beir(args: argparse.Namespace) -> int:
    """Download BEIR datasets into ~/.cache/yams/benchmarks/<name>."""
    from workers.beir_data import BEIR_DATASETS, ensure_beir_dataset

    names = [n.strip().lower() for n in (args.datasets or []) if n.strip()]
    if not names:
        names = ["scifact", "nfcorpus"]
    if args.list:
        for n in sorted(BEIR_DATASETS):
            print(n)
        return 0
    rc = 0
    for name in names:
        try:
            path = ensure_beir_dataset(name, download=True, force=bool(args.force))
            print(f"ok  {name} -> {path}")
        except Exception as exc:  # noqa: BLE001
            print(f"fail {name}: {exc}", file=sys.stderr)
            rc = 1
    return rc


def cmd_self_test(_: argparse.Namespace) -> int:
    """Expand a built-in mini plan without executing heavy workers."""
    repo_root = repo_root_from()
    plan_path = XPLAN_ROOT / "plans" / "ingest_pipeline.json"
    if not plan_path.exists():
        print("missing ingest_pipeline.json", file=sys.stderr)
        return 2
    plan = ExperimentPlan.load(plan_path)
    assert plan.name == "ingest_pipeline"
    assert len(plan.arms) >= 5
    assert plan.steps[0].worker == "ingestion_e2e"

    # Cartesian expansion smoke via factors-only synthetic dict
    tmp = {
        "name": "factor_smoke",
        "factors": {"a": [1, 2], "b": ["x", "y"]},
        "steps": [{"worker": "ops_timeline", "allow_stub": True}],
    }
    tmp_path = XPLAN_ROOT / ".self_test_plan.json"
    try:
        tmp_path.write_text(json.dumps(tmp), encoding="utf-8")
        expanded = ExperimentPlan.load(tmp_path)
        assert len(expanded.arms) == 4
    finally:
        if tmp_path.exists():
            tmp_path.unlink()

    # Dry-run KPI plans (no daemon / no heavy binaries). Fresh stamp avoids
    # leftover arms from older plan shapes under a reused selftest directory.
    import shutil

    stamp_base = f"selftest-{utc_stamp()}"
    for plan_name in (
        "ingest_pipeline",
        "ingest_submission_path",
        "retrieval_load",
        "repair_ability",
        "ops_timeline",
        "daemon_ops_core",
        "topology_purity_validate",
        "topology_routing_budget_ablation",
        "simeon_rerank",
        "search_component_ablation",
        "subsystem_overhead",
    ):
        # Wipe prior selftest dirs for this plan name to keep summaries honest.
        art = repo_root / "build" / "benchmarks" / plan_name
        if art.is_dir():
            for child in art.iterdir():
                if child.is_dir() and child.name.startswith("selftest"):
                    shutil.rmtree(child, ignore_errors=True)
        ns = argparse.Namespace(
            plan=plan_name,
            build_dir="build/release",
            out_dir="",
            stamp=f"{stamp_base}-{plan_name}",
            dry_run=True,
            continue_on_failure=True,
            skip_summary=False,
            arm=[],
        )
        rc = cmd_run(ns)
        if rc != 0:
            print(f"self-test failed plan={plan_name} rc={rc}", file=sys.stderr)
            return rc
    print(f"self-test ok (repo={repo_root}, dry-run all KPI plans)")
    return 0


def _filter_arms(plan: ExperimentPlan, only: list[str]) -> None:
    if not only:
        return
    wanted = set(only)
    plan.arms = [a for a in plan.arms if a.name in wanted or a.safe_name in wanted]
    if not plan.arms:
        raise SystemExit(f"no arms matched filters: {only}")


def cmd_run(args: argparse.Namespace) -> int:
    repo_root = repo_root_from()
    plan_path = resolve_plan_path(args.plan, repo_root)
    plan = ExperimentPlan.load(plan_path)
    _filter_arms(plan, list(args.arm or []))

    stamp = args.stamp or utc_stamp()
    build_dir = resolve_build_dir(repo_root, plan, args.build_dir)
    if args.out_dir:
        run_dir = Path(args.out_dir)
        if not run_dir.is_absolute():
            run_dir = (repo_root / run_dir).resolve()
    else:
        root = Path(plan.artifact_root)
        if not root.is_absolute():
            root = repo_root / root
        run_dir = (root / plan.name / stamp).resolve()
    try:
        prepare_fresh_run_dir(run_dir)
    except FileExistsError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    ensure_dir(run_dir / "arms")

    git_sha = try_git_sha(repo_root)
    write_json(
        run_dir / "plan.resolved.json",
        plan.resolved_dict(stamp=stamp, repo_root=repo_root, git_sha=git_sha),
    )
    write_mode_manifest(
        run_dir / "mode_manifest.json",
        runner="xplan",
        plan_name=plan.name,
        mode=plan.mode,
        build_dir=str(build_dir),
        extra={"git_sha": git_sha, "host": host_info(), "dry_run": bool(args.dry_run)},
    )

    print(f"ARTIFACT={run_dir}", flush=True)
    print(f"plan={plan.name} arms={len(plan.arms)} build_dir={build_dir}", flush=True)

    # One pre-run compile for quality plans when binary is missing (workers skip
    # rebuilds if the binary already exists to avoid mid-run link races).
    if not args.dry_run:
        quality_bin = build_dir / "tests" / "benchmarks" / "retrieval_quality_bench"
        needs_quality = any(s.worker == "retrieval_quality" for s in plan.steps)
        if needs_quality and not quality_bin.is_file():
            from workers.util import maybe_meson_compile

            print(f"prebuild: meson compile -C {build_dir} bench_retrieval_quality", flush=True)
            maybe_meson_compile(
                repo_root,
                build_dir,
                "bench_retrieval_quality",
                binary=quality_bin,
                force=True,
            )

    arm_results: list[dict[str, Any]] = []
    any_hard_fail = False

    for arm in plan.arms:
        arm_dir = arm_layout(run_dir, arm.safe_name)
        write_mode_manifest(
            arm_dir / "mode_manifest.json",
            runner="xplan",
            plan_name=plan.name,
            mode=plan.mode,
            build_dir=str(build_dir),
            arm_name=arm.name,
            extra={"factors": arm.factors, "dry_run": bool(args.dry_run)},
        )
        write_json(
            arm_dir / "arm.json",
            {
                "name": arm.name,
                "safe_name": arm.safe_name,
                "factors": arm.factors,
                "env": arm.env,
                "params": arm.params,
            },
        )

        ctx_arm = ArmContext(
            plan=plan,
            arm=arm,
            repo_root=repo_root,
            build_dir=build_dir,
            run_dir=run_dir,
            arm_dir=arm_dir,
            stamp=stamp,
            dry_run=bool(args.dry_run),
        )

        print(f"==> arm={arm.name}", flush=True)
        step_results: list[dict[str, Any]] = []
        combined_metrics: dict[str, Any] = {}
        combined_attrs: dict[str, Any] = {"factors": arm.factors}
        overall_status = "ok"
        overall_exit = 0
        messages: list[str] = []

        try:
            repeats = max(1, plan.repeats)
            rep_metrics: list[dict[str, Any]] = []
            for rep in range(repeats):
                rep_dir = arm_dir if repeats == 1 else (arm_dir / f"rep{rep:02d}")
                if repeats > 1:
                    ensure_dir(rep_dir)
                rep_ctx = ctx_arm if repeats == 1 else replace(ctx_arm, arm_dir=rep_dir)
                rep_combined: dict[str, Any] = {}
                for idx, step in enumerate(plan.steps):
                    worker_fn = get_worker(step.worker)
                    wctx = WorkerContext(
                        arm=rep_ctx,
                        step=step,
                        step_index=idx,
                        env=rep_ctx.merged_env(step),
                        params=rep_ctx.merged_params(step),
                    )
                    result = worker_fn(wctx)
                    step_results.append(
                        {
                            "rep": rep,
                            "worker": step.worker,
                            "status": result.status,
                            "exit_code": result.exit_code,
                            "message": result.message,
                            "raw_path": result.raw_path,
                        }
                    )
                    rep_combined.update(result.metrics)
                    combined_attrs[f"step{idx}_{step.worker}"] = result.attributes
                    messages.append(result.message)
                    if result.status in {"failed", "skipped"} or result.exit_code not in (0,):
                        if result.status == "stub" and result.exit_code == 0:
                            pass
                        elif result.status == "stub":
                            overall_status = "stub"
                        else:
                            overall_status = "failed"
                            overall_exit = result.exit_code or 1
                    elif result.status == "stub" and overall_status == "ok":
                        overall_status = "stub"

                    # Persist per-step metrics snapshot for multi-step plans.
                    write_json(
                        rep_dir / f"step{idx:02d}_{step.worker}.metrics.json",
                        result.to_metrics_doc(worker=step.worker, arm=arm.name),
                    )
                rep_metrics.append(rep_combined)
                if repeats > 1:
                    write_json(
                        rep_dir / "metrics.json",
                        {"status": overall_status, "arm": arm.name, "rep": rep,
                         "metrics": rep_combined},
                    )

            combined_metrics = aggregate_reps(rep_metrics)
            combined_attrs["repeats"] = repeats

            metrics_doc = {
                "status": overall_status,
                "worker": "+".join(s.worker for s in plan.steps),
                "arm": arm.name,
                "message": " | ".join(m for m in messages if m),
                "metrics": combined_metrics,
                "attributes": combined_attrs,
                "steps": step_results,
                "exit_code": overall_exit,
            }
            # dry-run treats stub/ok as success for structure
            if args.dry_run and overall_status in {"ok", "stub"}:
                metrics_doc["status"] = "ok" if overall_status == "ok" else "stub"

            write_json(arm_dir / "metrics.json", metrics_doc)
            (arm_dir / "exit_code").write_text(str(overall_exit) + "\n", encoding="utf-8")

            # Multi-rep workers write under repNN/; promote logs to arm root so
            # plan.validate.require_files (stdout.log, …) still resolve.
            if repeats > 1:
                for name in ("stdout.log", "stderr.log", "debug.jsonl"):
                    dest = arm_dir / name
                    if dest.exists():
                        continue
                    for rep in range(repeats):
                        src = arm_dir / f"rep{rep:02d}" / name
                        if src.is_file():
                            dest.write_text(src.read_text(encoding="utf-8", errors="replace"),
                                            encoding="utf-8")
                            break
                    else:
                        if name == "stdout.log":
                            dest.write_text(
                                f"# multi-rep arm ({repeats} reps); see repNN/{name}\n",
                                encoding="utf-8",
                            )

            # Validation: allow stub status when any step allows stubs or dry-run.
            allow_stub = args.dry_run or any(s.allow_stub for s in plan.steps)
            # For dry-run, accept ok/stub
            require_status = list(plan.validate.require_metric_status)
            if allow_stub and "stub" not in require_status:
                # temporary override via copy of plan validate is handled in validate_arm
                pass
            validation = validate_arm(
                plan,
                arm.name,
                arm_dir,
                plan.steps,
                allow_stub_statuses=allow_stub or args.dry_run,
            )
            write_json(arm_dir / "validation.json", validation.to_dict())

            arm_results.append(
                {
                    "arm": arm.name,
                    "safe_name": arm.safe_name,
                    "status": overall_status,
                    "exit_code": overall_exit,
                    "valid": validation.ok,
                    "dir": str(arm_dir),
                }
            )
            if not validation.ok or overall_status == "failed":
                any_hard_fail = True
                print(
                    f"    arm={arm.name} status={overall_status} valid={validation.ok}",
                    flush=True,
                )
                if not plan.continue_on_arm_failure and not args.continue_on_failure:
                    break
            else:
                print(
                    f"    arm={arm.name} status={overall_status} valid={validation.ok}",
                    flush=True,
                )
        except Exception as exc:  # noqa: BLE001
            any_hard_fail = True
            tb = traceback.format_exc()
            (arm_dir / "stderr.log").write_text(tb, encoding="utf-8")
            write_json(
                arm_dir / "metrics.json",
                {
                    "status": "failed",
                    "worker": "xplan",
                    "arm": arm.name,
                    "message": str(exc),
                    "metrics": {},
                    "exit_code": 1,
                },
            )
            (arm_dir / "exit_code").write_text("1\n", encoding="utf-8")
            arm_results.append(
                {
                    "arm": arm.name,
                    "safe_name": arm.safe_name,
                    "status": "failed",
                    "exit_code": 1,
                    "valid": False,
                    "error": str(exc),
                    "dir": str(arm_dir),
                }
            )
            print(f"    arm={arm.name} ERROR: {exc}", flush=True)
            if not plan.continue_on_arm_failure and not args.continue_on_failure:
                break

    write_json(run_dir / "results.json", {"arms": arm_results})
    if not args.skip_summary:
        summary = write_summary(
            plan,
            run_dir,
            stamp=stamp,
            git_sha=git_sha,
            build_dir=str(build_dir),
            dry_run=bool(args.dry_run),
        )
        print(
            f"summary valid={summary['valid_count']}/{summary['arm_count']} "
            f"-> {run_dir / 'summary.md'} | REPORT={run_dir / 'REPORT.md'}",
            flush=True,
        )

    print(f"ARTIFACT={run_dir}", flush=True)
    return 1 if any_hard_fail else 0


def cmd_report(args: argparse.Namespace) -> int:
    """Regenerate REPORT.md / ablation.md / metrics.csv from an existing run dir."""
    run_dir = Path(args.run_dir).resolve()
    plan_path = run_dir / "plan.resolved.json"
    if not plan_path.is_file():
        print(f"missing plan.resolved.json in {run_dir}", file=sys.stderr)
        return 2
    resolved = json.loads(plan_path.read_text(encoding="utf-8"))
    plan = load_resolved_report_plan(run_dir)
    summary = write_summary(
        plan,
        run_dir,
        stamp=str(resolved.get("stamp") or args.stamp or run_dir.name),
        git_sha=resolved.get("git_sha"),
        build_dir=str(resolved.get("build_dir") or ""),
        dry_run=False,
    )
    print(f"REPORT={run_dir / 'REPORT.md'} arms={summary['arm_count']}", flush=True)
    return 0


def cmd_compare(args: argparse.Namespace) -> int:
    from report import compare_reports

    a = Path(args.a).resolve()
    b = Path(args.b).resolve()
    out = Path(args.out).resolve() if args.out else (b / "compare.md")
    compare_reports(a, b, out)
    print(f"COMPARE={out}", flush=True)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="xplan", description="YAMS unified experiment runner")
    sub = parser.add_subparsers(dest="command", required=True)

    run_p = sub.add_parser("run", help="Execute an experiment plan")
    run_p.add_argument("plan", help="Plan path or name under tests/benchmarks/xplan/plans/")
    run_p.add_argument("--build-dir", default="", help="Meson build directory")
    run_p.add_argument("--out-dir", default="", help="Override artifact run directory")
    run_p.add_argument("--stamp", default="", help="Run stamp (default: UTC timestamp)")
    run_p.add_argument(
        "--arm",
        action="append",
        default=[],
        help="Only run named arm (repeatable)",
    )
    run_p.add_argument(
        "--dry-run",
        action="store_true",
        help="Expand plan and exercise workers without heavy work",
    )
    run_p.add_argument(
        "--continue-on-failure",
        action="store_true",
        help="Continue remaining arms after a failure",
    )
    run_p.add_argument("--skip-summary", action="store_true")
    run_p.set_defaults(func=cmd_run)

    rp = sub.add_parser("report", help="Regenerate REPORT.md for an existing artifact dir")
    rp.add_argument("run_dir", help="Path to build/benchmarks/<plan>/<stamp>/")
    rp.add_argument("--stamp", default="", help="Override stamp label in report")
    rp.set_defaults(func=cmd_report)

    cp = sub.add_parser("compare", help="Compare two run artifact directories")
    cp.add_argument("a", help="Baseline run dir (A)")
    cp.add_argument("b", help="Candidate run dir (B)")
    cp.add_argument("--out", default="", help="Output compare.md path (default: B/compare.md)")
    cp.set_defaults(func=cmd_compare)

    lw = sub.add_parser("list-workers", help="List registered workers")
    lw.set_defaults(func=cmd_list_workers)

    lp = sub.add_parser("list-plans", help="List plan files under plans/")
    lp.set_defaults(func=cmd_list_plans)

    db = sub.add_parser(
        "download-beir",
        help="Download BEIR corpora into ~/.cache/yams/benchmarks/ (default: scifact nfcorpus)",
    )
    db.add_argument(
        "datasets",
        nargs="*",
        help="Dataset names (default: scifact nfcorpus). Use --list for known names.",
    )
    db.add_argument("--list", action="store_true", help="Print known BEIR dataset names")
    db.add_argument(
        "--force",
        action="store_true",
        help="Re-download even if corpus already present",
    )
    db.set_defaults(func=cmd_download_beir)

    st = sub.add_parser("self-test", help="Run runner self-checks (dry-run)")
    st.set_defaults(func=cmd_self_test)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
