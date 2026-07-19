"""Artifact layout helpers for yams xplan runs."""

from __future__ import annotations

import hashlib
import json
import os
import platform
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def try_git_sha(repo_root: Path) -> str | None:
    try:
        proc = subprocess.run(
            ["git", "-C", str(repo_root), "rev-parse", "HEAD"],
            check=False,
            capture_output=True,
            text=True,
            timeout=5,
        )
        if proc.returncode == 0:
            return proc.stdout.strip() or None
    except (OSError, subprocess.SubprocessError):
        return None
    return None


def file_sha256(path: Path) -> str | None:
    if not path.is_file():
        return None
    digest = hashlib.sha256()
    try:
        with path.open("rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(chunk)
    except OSError:
        return None
    return digest.hexdigest()


def host_info() -> dict[str, Any]:
    return {
        "system": platform.system(),
        "release": platform.release(),
        "machine": platform.machine(),
        "processor": platform.processor(),
        "python": platform.python_version(),
        "hostname": platform.node(),
        "cpu_count": os.cpu_count(),
    }


def write_mode_manifest(
    path: Path,
    *,
    runner: str,
    plan_name: str,
    mode: str,
    build_dir: str,
    arm_name: str | None = None,
    extra: dict[str, Any] | None = None,
) -> None:
    payload: dict[str, Any] = {
        "runner": runner,
        "plan": plan_name,
        "mode_label": mode,
        "build_dir": build_dir,
        "arm": arm_name,
        "host": host_info(),
    }
    if extra:
        payload.update(extra)
    write_json(path, payload)


def arm_layout(run_dir: Path, arm_safe_name: str) -> Path:
    return ensure_dir(run_dir / "arms" / arm_safe_name)


def metrics_path(arm_dir: Path) -> Path:
    return arm_dir / "metrics.json"


def timeline_path(arm_dir: Path) -> Path:
    return arm_dir / "timeline.jsonl"


def raw_worker_output_path(arm_dir: Path, step_index: int, worker: str) -> Path:
    return arm_dir / f"step{step_index:02d}_{worker}.raw.json"
