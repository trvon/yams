"""Worker context and result types."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from model import ArmContext, Step


@dataclass
class WorkerContext:
    arm: ArmContext
    step: Step
    step_index: int
    env: dict[str, str]
    params: dict[str, Any]

    @property
    def arm_dir(self) -> Path:
        return self.arm.arm_dir

    @property
    def build_dir(self) -> Path:
        return self.arm.build_dir

    @property
    def repo_root(self) -> Path:
        return self.arm.repo_root

    @property
    def dry_run(self) -> bool:
        return self.arm.dry_run


@dataclass
class WorkerResult:
    status: str  # ok | stub | failed | skipped
    exit_code: int
    metrics: dict[str, Any] = field(default_factory=dict)
    attributes: dict[str, Any] = field(default_factory=dict)
    message: str = ""
    raw_path: str | None = None

    def to_metrics_doc(self, *, worker: str, arm: str) -> dict[str, Any]:
        return {
            "status": self.status,
            "worker": worker,
            "arm": arm,
            "message": self.message,
            "metrics": self.metrics,
            "attributes": self.attributes,
            "raw_path": self.raw_path,
            "exit_code": self.exit_code,
        }
