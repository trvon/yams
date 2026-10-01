"""Validate arm artifacts against plan contracts."""

from __future__ import annotations

import functools
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from artifacts import metrics_path, read_json
from model import ExperimentPlan, Step


@dataclass
class ValidationIssue:
    level: str  # error | warning
    message: str


@dataclass
class ArmValidation:
    arm: str
    ok: bool
    issues: list[ValidationIssue] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "arm": self.arm,
            "ok": self.ok,
            "issues": [{"level": i.level, "message": i.message} for i in self.issues],
        }


def validate_arm(
    plan: ExperimentPlan,
    arm_name: str,
    arm_dir: Path,
    steps: list[Step],
    *,
    allow_stub_statuses: bool = False,
) -> ArmValidation:
    result = ArmValidation(arm=arm_name, ok=True)

    required_files = list(plan.validate.require_files)
    for rel in required_files:
        path = arm_dir / rel
        if not path.exists():
            result.ok = False
            result.issues.append(
                ValidationIssue("error", f"missing required file: {rel}")
            )

    metrics_file = metrics_path(arm_dir)
    if not metrics_file.exists():
        return result

    try:
        metrics = read_json(metrics_file)
    except Exception as exc:  # noqa: BLE001 - surface parse failures
        result.ok = False
        result.issues.append(ValidationIssue("error", f"metrics.json unreadable: {exc}"))
        return result

    if not isinstance(metrics, dict):
        result.ok = False
        result.issues.append(ValidationIssue("error", "metrics.json root must be an object"))
        return result

    status = str(metrics.get("status", "ok"))
    allowed = set(plan.validate.require_metric_status)
    # Steps that allow stubs may also accept status=stub when permitted.
    if any(s.allow_stub for s in steps) or allow_stub_statuses:
        allowed = set(allowed) | {"stub", "ok"}
    if status not in allowed:
        result.ok = False
        result.issues.append(
            ValidationIssue(
                "error",
                f"metrics.status={status!r} not in allowed {sorted(allowed)}",
            )
        )

    required_metrics = list(plan.validate.require_metrics)
    for step in steps:
        required_metrics.extend(step.metrics)
    # de-dupe preserve order
    seen: set[str] = set()
    ordered: list[str] = []
    for key in required_metrics:
        if key not in seen:
            seen.add(key)
            ordered.append(key)

    values = metrics.get("metrics")
    if values is None:
        values = metrics  # allow flat metrics.json
    if not isinstance(values, dict):
        result.ok = False
        result.issues.append(ValidationIssue("error", "metrics payload must be an object"))
        return result

    # Stub arms are structure-valid even without real KPI numbers.
    if status == "stub":
        return result

    # Workers record metrics they could not observe instead of defaulting them to 0.
    attrs = metrics.get("attributes")
    if isinstance(attrs, dict):
        for step_name, step_attrs in attrs.items():
            if isinstance(step_attrs, dict) and step_attrs.get("missing_metrics"):
                result.ok = False
                result.issues.append(
                    ValidationIssue(
                        "error",
                        f"{step_name} reported missing metrics: "
                        f"{', '.join(map(str, step_attrs['missing_metrics']))}",
                    )
                )

    for key in ordered:
        if key not in values:
            result.ok = False
            result.issues.append(ValidationIssue("error", f"missing metric: {key}"))

    return result


# ---------------------------------------------------------------------------
# Plan preflight: no env key may be set that nothing reads.
# ---------------------------------------------------------------------------

_ENV_TOKEN = re.compile(r"\bYAMS_[A-Z0-9_]+\b")
_PRODUCT_DIRS = ("src", "include")
_PRODUCT_SUFFIXES = {".cpp", ".cc", ".h", ".hpp", ".mm", ".inl"}
# YAMS_BENCH_* is harness vocabulary: readers live in the bench binaries/tests or in the
# xplan workers themselves, not in product code.
_HARNESS_DIRS = ("tests",)
_HARNESS_PREFIX = "YAMS_BENCH_"
# Keys the harness sets that are intentionally not read by src/ or include/. Keep tiny;
# each entry needs a reason.
ENV_READER_ALLOWLIST: dict[str, str] = {}


def _scan_tokens(root: Path, dirs: tuple[str, ...], suffixes: set[str]) -> set[str]:
    found: set[str] = set()
    for rel in dirs:
        base = root / rel
        if not base.is_dir():
            continue
        for dirpath, dirnames, filenames in os.walk(base):
            # plans only *declare* keys; they are not readers.
            dirnames[:] = [d for d in dirnames if d not in {"plans", "__pycache__", "build"}]
            for name in filenames:
                if Path(name).suffix not in suffixes:
                    continue
                try:
                    text = (Path(dirpath) / name).read_text(encoding="utf-8", errors="ignore")
                except OSError:
                    continue
                found.update(_ENV_TOKEN.findall(text))
    return found


@functools.lru_cache(maxsize=8)
def _reader_index(root_str: str) -> tuple[frozenset[str], frozenset[str]]:
    root = Path(root_str)
    product = _scan_tokens(root, _PRODUCT_DIRS, _PRODUCT_SUFFIXES)
    harness = _scan_tokens(root, _HARNESS_DIRS, _PRODUCT_SUFFIXES | {".py"})
    return frozenset(product), frozenset(harness)


def env_key_has_reader(key: str, repo_root: Path) -> bool:
    """True when `key` is read by product code (or, for YAMS_BENCH_*, by bench code)."""
    if key in ENV_READER_ALLOWLIST:
        return True
    product, harness = _reader_index(str(repo_root.resolve()))
    if key in product:
        return True
    if key.startswith(_HARNESS_PREFIX):
        return key in harness
    return False


def preflight_plan(plan: ExperimentPlan, repo_root: Path) -> list[str]:
    """Return blocking problems for a plan; empty means every knob it sets is honored."""
    # Imported lazily: workers pull in heavy modules and need the runner's sys.path.
    from workers.ablation import AblationError, apply_ablation
    from workers.retrieval_quality import PARAM_ENV_MAP

    issues: list[str] = []
    parked = plan.raw.get("parked")
    if parked:
        issues.append(f"plan is parked: {parked}")

    seen: set[tuple[str, str]] = set()

    def check(where: str, key: str) -> None:
        if not key.startswith("YAMS_") or (where, key) in seen:
            return
        seen.add((where, key))
        if not env_key_has_reader(key, repo_root):
            issues.append(f"{where}: env {key} has no reader (would be a silent no-op)")

    for arm in plan.arms:
        for step in plan.steps:
            where = f"arm {arm.name!r}"
            env = {**plan.fixed_env, **arm.env, **step.env}
            params = {**arm.factors, **plan.fixed_params, **arm.params, **step.params}
            for key in env:
                check(where, key)
            scratch = dict(env)
            try:
                apply_ablation(scratch, factors=arm.factors, params=params)
            except AblationError as exc:
                issues.append(f"{where}: {exc}")
                continue
            for key in scratch:
                if key not in env:
                    check(where, key)
            if step.worker == "retrieval_quality":
                for pkey, ekey in PARAM_ENV_MAP.items():
                    if pkey in params:
                        check(where, ekey)
    return sorted(set(issues))
