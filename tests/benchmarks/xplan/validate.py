"""Validate arm artifacts against plan contracts."""

from __future__ import annotations

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

    for key in ordered:
        if key not in values:
            result.ok = False
            result.issues.append(ValidationIssue("error", f"missing metric: {key}"))

    return result
