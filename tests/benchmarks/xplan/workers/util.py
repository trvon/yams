"""Shared helpers for xplan workers that shell out to bench binaries."""

from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path
from typing import Any


def env_truthy(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def maybe_meson_compile(repo_root: Path, build_dir: Path, target: str, *, skip: bool) -> None:
    if skip:
        return
    subprocess.run(
        ["meson", "compile", "-C", str(build_dir), "-j4", target],
        check=False,
        cwd=str(repo_root),
    )


def resolve_binary(
    build_dir: Path,
    *relative: str,
    explicit: str | None = None,
) -> Path:
    if explicit:
        return Path(explicit)
    return build_dir.joinpath(*relative)


def run_captured(
    cmd: list[str],
    *,
    cwd: Path,
    env: dict[str, str],
    timeout: int | None,
    stdout_path: Path,
    stderr_path: Path,
) -> subprocess.CompletedProcess[str]:
    try:
        proc = subprocess.run(
            cmd,
            cwd=str(cwd),
            env=env,
            text=True,
            capture_output=True,
            timeout=timeout,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        stdout_path.write_text(
            (exc.stdout.decode("utf-8", errors="replace") if isinstance(exc.stdout, bytes) else (exc.stdout or "")),
            encoding="utf-8",
            errors="replace",
        )
        stderr_path.write_text(
            (exc.stderr.decode("utf-8", errors="replace") if isinstance(exc.stderr, bytes) else (exc.stderr or "")),
            encoding="utf-8",
            errors="replace",
        )
        raise
    stdout_path.write_text(proc.stdout or "", encoding="utf-8", errors="replace")
    stderr_path.write_text(proc.stderr or "", encoding="utf-8", errors="replace")
    return proc


def read_json_file(path: Path) -> dict[str, Any] | None:
    if not path.is_file():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None
    return data if isinstance(data, dict) else None


def read_jsonl_records(path: Path) -> list[dict[str, Any]]:
    if not path.is_file():
        return []
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(obj, dict):
            rows.append(obj)
    return rows


def last_jsonl_matching(
    path: Path, *, test_name: str | None = None, key: str = "test"
) -> dict[str, Any] | None:
    rows = read_jsonl_records(path)
    if not rows:
        return None
    if test_name:
        for row in reversed(rows):
            if str(row.get(key, "")) == test_name:
                return row
    return rows[-1]


def latency_ms_from_us_block(block: dict[str, Any] | None, field: str = "p50_us") -> float | None:
    if not isinstance(block, dict):
        return None
    if field in block:
        return float(block[field]) / 1000.0
    # Alternate naming
    alt = field.replace("_us", "_ms")
    if alt in block:
        return float(block[alt])
    return None


def nested_get(data: dict[str, Any], *keys: str, default: Any = None) -> Any:
    cur: Any = data
    for key in keys:
        if not isinstance(cur, dict) or key not in cur:
            return default
        cur = cur[key]
    return cur
