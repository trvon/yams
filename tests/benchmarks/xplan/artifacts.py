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


def capture_git_source_snapshot(repo_root: Path, run_dir: Path) -> dict[str, Any]:
    """Preserve dirty tracked changes and a deterministic untracked-file identity."""
    repo_root = repo_root.resolve()
    run_dir = run_dir.resolve()

    def run_git(*args: str) -> bytes:
        proc = subprocess.run(
            ["git", "-C", str(repo_root), *args],
            check=False,
            capture_output=True,
            timeout=30,
        )
        if proc.returncode != 0:
            detail = proc.stderr.decode("utf-8", errors="replace").strip()
            raise RuntimeError(f"git {' '.join(args)} failed: {detail}")
        return proc.stdout

    status = run_git("status", "--porcelain=v1", "-z", "--untracked-files=all")
    patch = run_git("diff", "--binary", "HEAD", "--", ".")
    patch_name = "source_snapshot.patch"
    (run_dir / patch_name).write_bytes(patch)

    untracked_raw = run_git("ls-files", "--others", "--exclude-standard", "-z")
    untracked: list[dict[str, Any]] = []
    for raw_path in untracked_raw.split(b"\0"):
        if not raw_path:
            continue
        relative = raw_path.decode("utf-8", errors="surrogateescape")
        path = repo_root / relative
        if not path.is_file():
            continue
        untracked.append(
            {
                "path": relative,
                "sha256": file_sha256(path),
                "size_bytes": path.stat().st_size,
            }
        )
    untracked_name = "source_snapshot_untracked.json"
    write_json(run_dir / untracked_name, untracked)

    patch_sha256 = hashlib.sha256(patch).hexdigest()
    status_sha256 = hashlib.sha256(status).hexdigest()
    untracked_payload = json.dumps(untracked, sort_keys=True, separators=(",", ":")).encode()
    snapshot_digest = hashlib.sha256()
    snapshot_digest.update((try_git_sha(repo_root) or "unknown").encode())
    snapshot_digest.update(b"\0")
    snapshot_digest.update(status)
    snapshot_digest.update(b"\0")
    snapshot_digest.update(patch)
    snapshot_digest.update(b"\0")
    snapshot_digest.update(untracked_payload)
    return {
        "worktree_dirty": bool(status),
        "worktree_status_sha256": status_sha256,
        "worktree_patch_sha256": patch_sha256,
        "worktree_patch_path": patch_name,
        "untracked_manifest_path": untracked_name,
        "untracked_file_count": len(untracked),
        "source_snapshot_sha256": snapshot_digest.hexdigest(),
    }


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


def source_set_sha256(root: Path, paths: list[Path]) -> str:
    """Hash a deterministic set of source paths and contents for run provenance."""
    root = root.resolve()
    digest = hashlib.sha256()
    canonical: list[tuple[str, Path]] = []
    for source in paths:
        resolved = source.resolve()
        try:
            label = resolved.relative_to(root).as_posix()
        except ValueError:
            label = resolved.as_posix()
        canonical.append((label, resolved))

    for label, source in sorted(canonical):
        if not source.is_file():
            raise FileNotFoundError(f"xplan provenance source is missing: {source}")
        digest.update(label.encode("utf-8"))
        digest.update(b"\0")
        with source.open("rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(chunk)
        digest.update(b"\0")
    return digest.hexdigest()


def host_info() -> dict[str, Any]:
    info = {
        "system": platform.system(),
        "release": platform.release(),
        "machine": platform.machine(),
        "processor": platform.processor(),
        "python": platform.python_version(),
        "hostname": platform.node(),
        "cpu_count": os.cpu_count(),
    }
    try:
        load1, load5, load15 = os.getloadavg()
    except (AttributeError, OSError):
        return info
    cpu_count = os.cpu_count() or 1
    info.update(
        {
            "load_average_1m": load1,
            "load_average_5m": load5,
            "load_average_15m": load15,
            "load_average_1m_per_cpu": load1 / cpu_count,
        }
    )
    return info


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
