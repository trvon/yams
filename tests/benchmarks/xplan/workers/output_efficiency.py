"""Real-codebase output-efficiency measurement for search, grep, and graph."""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import shutil
import statistics
import subprocess
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Iterable, Sequence

from artifacts import raw_worker_output_path, write_json
from workers.base import WorkerContext, WorkerResult


_TASK_ID = re.compile(r"^[a-z0-9][a-z0-9_-]*$")
_ANSI_ESCAPE = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]")
_ABSOLUTE_PATH = re.compile(
    r"(?m)(?:(?<=\")|(?<=')|^)(/(?!/)(?:[^/\s:\"']+/)*[^/\s:\"',)}\]>`]+)"
)
_SUPPORTED_SURFACES = frozenset({"search", "grep", "graph"})
_SUPPORTED_FORMATS = frozenset({"human", "json"})
_DEFAULT_BYTE_BUDGETS = (1024, 2048, 4096, 8192, 16384)


@dataclass(frozen=True)
class OutputTask:
    task_id: str
    surface: str
    query: str
    expected_any: tuple[str, ...]
    limit: int = 5


@dataclass(frozen=True)
class TaskManifest:
    corpus: str
    tasks: tuple[OutputTask, ...]


@dataclass(frozen=True)
class OutputRecord:
    task_id: str
    surface: str
    output_bytes: int
    first_useful_byte: int | None
    useful: bool
    duplicate_line_bytes: int
    scope_path_count: int
    scope_leak_path_count: int
    latency_ms: float
    exit_code: int
    payload_sha256: str


@dataclass(frozen=True)
class ContractEvaluation:
    metrics: dict[str, float]
    violations: tuple[dict[str, Any], ...]


@dataclass(frozen=True)
class ShadowGrepOutput:
    payload: str
    input_matches: int
    unique_matches: int
    emitted_matches: int
    identity_duplicates_removed: int
    cap_matches_dropped: int


def _required_text(raw: Any, *, field: str, task_id: str) -> str:
    value = str(raw or "").strip()
    if not value:
        raise ValueError(f"task {task_id!r} requires non-empty {field}")
    return value


def load_tasks(path: Path) -> TaskManifest:
    """Load and validate a checked-in output-efficiency task manifest."""
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"invalid task manifest JSON: {path}: {exc}") from exc
    if not isinstance(raw, dict):
        raise ValueError(f"task manifest root must be an object: {path}")
    if int(raw.get("schema_version") or 0) != 1:
        raise ValueError("task manifest schema_version must be 1")

    corpus = str(raw.get("corpus") or "").strip()
    if not corpus:
        raise ValueError("task manifest corpus is required")
    task_rows = raw.get("tasks")
    if not isinstance(task_rows, list) or not task_rows:
        raise ValueError("task manifest tasks must be a non-empty array")

    tasks: list[OutputTask] = []
    seen_ids: set[str] = set()
    for index, row in enumerate(task_rows):
        if not isinstance(row, dict):
            raise ValueError(f"task at index {index} must be an object")
        task_id = str(row.get("id") or "").strip()
        if not _TASK_ID.fullmatch(task_id):
            raise ValueError(f"invalid task id {task_id!r}")
        if task_id in seen_ids:
            raise ValueError(f"duplicate task id: {task_id}")
        seen_ids.add(task_id)

        surface = str(row.get("surface") or "").strip()
        if surface not in _SUPPORTED_SURFACES:
            raise ValueError(f"task {task_id!r} has unknown surface {surface!r}")
        query = _required_text(row.get("query"), field="query", task_id=task_id)
        expected_raw = row.get("expected_any")
        if not isinstance(expected_raw, list):
            raise ValueError(f"task {task_id!r} expected_any must be an array")
        expected_any = tuple(
            marker for value in expected_raw if (marker := str(value).strip())
        )
        if not expected_any:
            raise ValueError(f"task {task_id!r} expected_any must not be empty")
        limit = int(row.get("limit") or 5)
        if limit <= 0:
            raise ValueError(f"task {task_id!r} limit must be positive")
        tasks.append(
            OutputTask(
                task_id=task_id,
                surface=surface,
                query=query,
                expected_any=expected_any,
                limit=limit,
            )
        )
    return TaskManifest(corpus=corpus, tasks=tuple(tasks))


def build_command(
    task: OutputTask,
    binary: Path,
    repo_root: Path,
    *,
    output_format: str,
) -> list[str]:
    """Build one non-mutating CLI command scoped to the YAMS checkout."""
    if output_format not in _SUPPORTED_FORMATS:
        raise ValueError(f"unsupported output format: {output_format}")

    if task.surface == "search":
        command = [
            str(binary),
            "search",
            task.query,
            "--cwd",
            "--limit",
            str(task.limit),
        ]
    elif task.surface == "grep":
        command = [
            str(binary),
            "grep",
            "-F",
            task.query,
            "--path",
            f"{repo_root}/**",
            "--lang",
            "cpp",
            "--limit",
            str(task.limit),
            "--color",
            "never",
        ]
    elif task.surface == "graph":
        command = [
            str(binary),
            "graph",
            "--explore",
            task.query,
            "--max-files",
            str(task.limit),
        ]
    else:  # pragma: no cover - OutputTask normally comes from validated manifests
        raise ValueError(f"unsupported surface: {task.surface}")

    if output_format == "json":
        command.append("--json")
    return command


def _normalized_payload(payload: str) -> str:
    return _ANSI_ESCAPE.sub("", payload)


def _first_useful_byte(payload: str, markers: Iterable[str]) -> int | None:
    first: int | None = None
    for marker in markers:
        index = payload.find(marker)
        if index < 0:
            continue
        marker_end = index + len(marker)
        byte_end = len(payload[:marker_end].encode("utf-8"))
        first = byte_end if first is None else min(first, byte_end)
    return first


def _duplicate_normalized_line_bytes(payload: str) -> int:
    seen: set[str] = set()
    duplicate_bytes = 0
    for raw_line in payload.splitlines():
        line = " ".join(raw_line.split())
        if not line:
            continue
        if line in seen:
            duplicate_bytes += len((line + "\n").encode("utf-8"))
        else:
            seen.add(line)
    return duplicate_bytes


def build_shadow_grep_json(
    payload: str,
    *,
    max_matches: int | None = None,
) -> ShadowGrepOutput:
    if max_matches is not None and max_matches <= 0:
        raise ValueError("max_matches must be positive")
    document = json.loads(payload)
    if not isinstance(document, dict) or not isinstance(document.get("matches"), list):
        raise ValueError("grep JSON payload must contain a matches array")

    matches = document["matches"]
    unique: list[dict[str, Any]] = []
    seen: set[tuple[str, int, str]] = set()
    for raw_match in matches:
        if not isinstance(raw_match, dict):
            raise ValueError("grep JSON matches must be objects")
        identity = (
            str(raw_match.get("file") or ""),
            int(raw_match.get("line_number") or 0),
            " ".join(str(raw_match.get("line") or "").split()),
        )
        if identity in seen:
            continue
        seen.add(identity)
        unique.append(raw_match)

    emitted = unique if max_matches is None else unique[:max_matches]
    shadow_document = dict(document)
    shadow_document["matches"] = emitted
    return ShadowGrepOutput(
        payload=json.dumps(shadow_document, indent=2, ensure_ascii=False) + "\n",
        input_matches=len(matches),
        unique_matches=len(unique),
        emitted_matches=len(emitted),
        identity_duplicates_removed=len(matches) - len(unique),
        cap_matches_dropped=len(unique) - len(emitted),
    )


def _scope_path_counts(payload: str, repo_root: Path | None) -> tuple[int, int]:
    if repo_root is None:
        return 0, 0
    normalized_root = repo_root.resolve()
    paths = {Path(match.group(0)) for match in _ABSOLUTE_PATH.finditer(payload)}
    leaks = 0
    for path in paths:
        try:
            path.resolve().relative_to(normalized_root)
        except ValueError:
            leaks += 1
    return len(paths), leaks


def analyze_output(
    task: OutputTask,
    payload: str,
    *,
    latency_ms: float,
    exit_code: int,
    repo_root: Path | None = None,
) -> OutputRecord:
    """Measure exact serialized cost and first judged-useful marker."""
    normalized = _normalized_payload(payload)
    first_useful = _first_useful_byte(normalized, task.expected_any)
    encoded = normalized.encode("utf-8")
    scope_path_count, scope_leak_path_count = _scope_path_counts(normalized, repo_root)
    return OutputRecord(
        task_id=task.task_id,
        surface=task.surface,
        output_bytes=len(encoded),
        first_useful_byte=first_useful,
        useful=first_useful is not None,
        duplicate_line_bytes=_duplicate_normalized_line_bytes(normalized),
        scope_path_count=scope_path_count,
        scope_leak_path_count=scope_leak_path_count,
        latency_ms=latency_ms,
        exit_code=exit_code,
        payload_sha256=hashlib.sha256(encoded).hexdigest(),
    )


def _percentile(values: Sequence[float], percentile: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = max(0, math.ceil(percentile * len(ordered)) - 1)
    return float(ordered[index])


def _aggregate_group(
    records: Sequence[OutputRecord],
    *,
    budgets: Sequence[int],
) -> dict[str, float]:
    count = len(records)
    if count == 0:
        metrics: dict[str, float] = {
            "task_count": 0.0,
            "command_success_rate": 0.0,
            "useful_hit_rate": 0.0,
            "output_bytes_p50": 0.0,
            "output_bytes_p95": 0.0,
            "first_useful_byte_p50": 0.0,
            "duplicate_line_fraction": 0.0,
            "scope_path_count": 0.0,
            "scope_leak_path_count": 0.0,
            "scope_leak_fraction": 0.0,
            "scope_leak_task_rate": 0.0,
            "useful_units_per_kib": 0.0,
            "command_latency_ms_p50": 0.0,
            "command_latency_ms_p95": 0.0,
        }
        metrics.update({f"useful_recall_at_{budget}_bytes": 0.0 for budget in budgets})
        return metrics

    output_bytes = [float(record.output_bytes) for record in records]
    first_useful = [
        float(record.first_useful_byte)
        for record in records
        if record.first_useful_byte is not None
    ]
    total_bytes = sum(record.output_bytes for record in records)
    scope_path_count = sum(record.scope_path_count for record in records)
    useful_count = sum(record.useful for record in records)
    metrics = {
        "task_count": float(count),
        "command_success_rate": sum(record.exit_code == 0 for record in records) / count,
        "useful_hit_rate": useful_count / count,
        "output_bytes_p50": float(statistics.median(output_bytes)),
        "output_bytes_p95": _percentile(output_bytes, 0.95),
        "first_useful_byte_p50": (
            float(statistics.median(first_useful)) if first_useful else 0.0
        ),
        "duplicate_line_fraction": (
            sum(record.duplicate_line_bytes for record in records) / total_bytes
            if total_bytes
            else 0.0
        ),
        "scope_path_count": float(scope_path_count),
        "scope_leak_path_count": float(
            sum(record.scope_leak_path_count for record in records)
        ),
        "scope_leak_fraction": (
            sum(record.scope_leak_path_count for record in records) / scope_path_count
            if scope_path_count
            else 0.0
        ),
        "scope_leak_task_rate": (
            sum(record.scope_leak_path_count > 0 for record in records) / count
        ),
        "useful_units_per_kib": (
            useful_count * 1024.0 / total_bytes if total_bytes else 0.0
        ),
        "command_latency_ms_p50": float(
            statistics.median(record.latency_ms for record in records)
        ),
        "command_latency_ms_p95": _percentile(
            [record.latency_ms for record in records], 0.95
        ),
    }
    for budget in budgets:
        within_budget = sum(
            record.first_useful_byte is not None and record.first_useful_byte <= budget
            for record in records
        )
        metrics[f"useful_recall_at_{budget}_bytes"] = within_budget / count
    return metrics


def aggregate_output_records(
    records: Sequence[OutputRecord],
    *,
    budgets: Sequence[int] = _DEFAULT_BYTE_BUDGETS,
) -> dict[str, float]:
    if not budgets or any(budget <= 0 for budget in budgets):
        raise ValueError("byte budgets must be positive")
    normalized_budgets = tuple(sorted(set(int(budget) for budget in budgets)))
    metrics = _aggregate_group(records, budgets=normalized_budgets)
    for surface in sorted(_SUPPORTED_SURFACES):
        surface_metrics = _aggregate_group(
            [record for record in records if record.surface == surface],
            budgets=normalized_budgets,
        )
        metrics.update({f"{surface}_{key}": value for key, value in surface_metrics.items()})
    return metrics


def _contract_gates(contract: dict[str, Any]) -> tuple[tuple[str, str, float], ...]:
    gates: list[tuple[str, str, float]] = []
    for key, raw_target in contract.items():
        if key == "profile":
            continue
        if key.startswith("min_"):
            direction = "min"
            metric = key[4:]
        elif key.startswith("max_"):
            direction = "max"
            metric = key[4:]
        else:
            raise ValueError(f"unknown efficiency contract gate: {key}")
        target = float(raw_target)
        if not math.isfinite(target):
            raise ValueError(f"efficiency contract gate must be finite: {key}")
        gates.append((direction, metric, target))
    if not gates:
        raise ValueError("efficiency contract requires at least one min_ or max_ gate")
    return tuple(gates)


def evaluate_efficiency_contract(
    metrics: dict[str, float],
    contract: dict[str, Any],
) -> ContractEvaluation:
    gates = _contract_gates(contract)
    violations: list[dict[str, Any]] = []
    for direction, metric, target in gates:
        actual = metrics.get(metric)
        passed = actual is not None and (
            actual >= target if direction == "min" else actual <= target
        )
        if not passed:
            violations.append(
                {
                    "metric": metric,
                    "direction": direction,
                    "target": target,
                    "actual": actual,
                }
            )
    return ContractEvaluation(
        metrics={
            "efficiency_contract_evaluated": 1.0,
            "efficiency_contract_gate_count": float(len(gates)),
            "efficiency_contract_violation_count": float(len(violations)),
            "efficiency_contract_pass": 0.0 if violations else 1.0,
        },
        violations=tuple(violations),
    )


def _absolute_executable_path(value: str, repo_root: Path) -> Path:
    path = Path(value).expanduser()
    if path.is_absolute():
        return path
    if len(path.parts) == 1:
        installed = shutil.which(value)
        if installed:
            return Path(installed)
    return (repo_root / path).resolve()


def _resolve_binary(ctx: WorkerContext) -> Path:
    explicit = ctx.params.get("binary") or os.environ.get("YAMS_BENCH_YAMS_BINARY")
    if explicit:
        return _absolute_executable_path(str(explicit), ctx.repo_root)
    built = ctx.build_dir / "tools" / "yams-cli" / "yams-cli"
    if built.is_file():
        return built.resolve()
    installed = shutil.which("yams")
    if installed:
        return Path(installed)
    return built.resolve()


def _repo_commit(repo_root: Path) -> str:
    result = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=str(repo_root),
        text=True,
        capture_output=True,
        check=False,
    )
    return result.stdout.strip() if result.returncode == 0 else "unknown"


def _command_environment(overrides: dict[str, str]) -> dict[str, str]:
    environment = os.environ.copy()
    environment.update(overrides)
    environment.pop("YAMS_BENCH_YAMS_BINARY", None)
    environment.setdefault("NO_COLOR", "1")
    return environment


def _dry_run_metrics(
    budgets: Sequence[int],
    contract: dict[str, Any],
) -> dict[str, float]:
    metrics = aggregate_output_records([], budgets=budgets)
    metrics["token_count_available"] = 0.0
    metrics.update(
        {
            "efficiency_contract_evaluated": 0.0,
            "efficiency_contract_gate_count": float(len(_contract_gates(contract))),
            "efficiency_contract_violation_count": 0.0,
            "efficiency_contract_pass": 0.0,
        }
    )
    metrics.update(_shadow_metric_defaults())
    return metrics


def _shadow_metric_defaults() -> dict[str, float]:
    return {
        "grep_shadow_available": 0.0,
        "grep_shadow_input_matches": 0.0,
        "grep_identity_shadow_removed_fraction": 0.0,
        "grep_identity_shadow_output_bytes_p95": 0.0,
        "grep_identity_shadow_grep_output_bytes_p95": 0.0,
        "grep_window_shadow_dropped_fraction": 0.0,
        "grep_window_shadow_output_bytes_p95": 0.0,
        "grep_window_shadow_useful_recall_at_512_bytes": 0.0,
        "grep_window_shadow_duplicate_line_fraction": 0.0,
        "grep_window_shadow_useful_units_per_kib": 0.0,
        "grep_window_shadow_grep_output_bytes_p95": 0.0,
        "grep_window_shadow_grep_useful_recall_at_512_bytes": 0.0,
        "grep_window_shadow_grep_duplicate_line_fraction": 0.0,
        "grep_window_shadow_grep_useful_units_per_kib": 0.0,
        "grep_identity_shadow_efficiency_contract_violation_count": 0.0,
        "grep_identity_shadow_efficiency_contract_pass": 0.0,
        "grep_window_shadow_efficiency_contract_violation_count": 0.0,
        "grep_window_shadow_efficiency_contract_pass": 0.0,
    }


def _add_shadow_metrics(
    metrics: dict[str, float],
    *,
    prefix: str,
    records: Sequence[OutputRecord],
    budgets: Sequence[int],
    contract: dict[str, Any],
) -> ContractEvaluation:
    shadow_metrics = aggregate_output_records(records, budgets=budgets)
    metrics.update({f"{prefix}_{key}": value for key, value in shadow_metrics.items()})
    evaluation = evaluate_efficiency_contract(shadow_metrics, contract)
    metrics[f"{prefix}_efficiency_contract_violation_count"] = evaluation.metrics[
        "efficiency_contract_violation_count"
    ]
    metrics[f"{prefix}_efficiency_contract_pass"] = evaluation.metrics[
        "efficiency_contract_pass"
    ]
    return evaluation


def run_output_efficiency(ctx: WorkerContext) -> WorkerResult:
    """Run read-only CLI traversals against an already indexed YAMS checkout."""
    manifest_value = ctx.params.get("manifest") or (
        "tests/benchmarks/xplan/data/yams_codebase_output_tasks.json"
    )
    manifest_path = Path(str(manifest_value))
    if not manifest_path.is_absolute():
        manifest_path = ctx.repo_root / manifest_path
    output_format = str(ctx.params.get("output_format") or "human").strip()
    if output_format not in _SUPPORTED_FORMATS:
        return WorkerResult(
            status="failed",
            exit_code=2,
            message=f"unsupported output format: {output_format}",
        )
    budgets = tuple(
        int(value)
        for value in (ctx.params.get("byte_budgets") or _DEFAULT_BYTE_BUDGETS)
    )
    contract = ctx.params.get("efficiency_contract")
    if not isinstance(contract, dict):
        return WorkerResult(
            status="failed",
            exit_code=2,
            message="efficiency_contract must be an object",
        )
    try:
        manifest = load_tasks(manifest_path)
        _contract_gates(contract)
    except (OSError, TypeError, ValueError) as exc:
        return WorkerResult(
            status="failed",
            exit_code=2,
            message=f"failed to load output-efficiency tasks: {exc}",
        )

    binary = _resolve_binary(ctx)
    raw_path = raw_worker_output_path(ctx.arm_dir, ctx.step_index, "output_efficiency")
    attributes: dict[str, Any] = {
        "binary": str(binary),
        "manifest": str(manifest_path),
        "corpus": manifest.corpus,
        "output_format": output_format,
        "repo_commit": _repo_commit(ctx.repo_root),
        "tokenizer_profile": "none",
        "byte_budgets": list(budgets),
        "efficiency_contract_profile": str(contract.get("profile") or "unnamed"),
    }
    if ctx.dry_run:
        write_json(
            raw_path,
            {
                "dry_run": True,
                "attributes": attributes,
                "tasks": [asdict(task) for task in manifest.tasks],
            },
        )
        return WorkerResult(
            status="ok",
            exit_code=0,
            metrics=_dry_run_metrics(budgets, contract),
            attributes=attributes,
            message="dry-run output_efficiency",
            raw_path=str(raw_path),
        )
    if not binary.is_file():
        return WorkerResult(
            status="failed",
            exit_code=2,
            attributes=attributes,
            message=f"yams CLI binary missing: {binary}",
        )

    output_dir = ctx.arm_dir / f"step{ctx.step_index:02d}_outputs"
    output_dir.mkdir(parents=True, exist_ok=True)
    environment = _command_environment(ctx.env)
    timeout = int(ctx.params.get("task_timeout_sec") or 60)
    shadow_grep_max_matches = int(ctx.params.get("shadow_grep_max_matches") or 20)
    if shadow_grep_max_matches <= 0:
        return WorkerResult(
            status="failed",
            exit_code=2,
            attributes=attributes,
            message="shadow_grep_max_matches must be positive",
        )
    records: list[OutputRecord] = []
    raw_records: list[dict[str, Any]] = []
    identity_shadow_records: list[OutputRecord] = []
    cap_shadow_records: list[OutputRecord] = []
    shadow_input_matches = 0
    shadow_identity_removed = 0
    shadow_cap_dropped = 0
    shadow_errors: list[str] = []

    for task in manifest.tasks:
        command = build_command(
            task,
            binary,
            ctx.repo_root,
            output_format=output_format,
        )
        start = time.perf_counter()
        try:
            result = subprocess.run(
                command,
                cwd=str(ctx.repo_root),
                env=environment,
                text=True,
                capture_output=True,
                timeout=timeout,
                check=False,
            )
            latency_ms = (time.perf_counter() - start) * 1000.0
            stdout = result.stdout or ""
            stderr = result.stderr or ""
            exit_code = result.returncode
        except subprocess.TimeoutExpired as exc:
            latency_ms = (time.perf_counter() - start) * 1000.0
            stdout = exc.stdout if isinstance(exc.stdout, str) else ""
            stderr = exc.stderr if isinstance(exc.stderr, str) else ""
            exit_code = 124

        (output_dir / f"{task.task_id}.stdout").write_text(
            stdout, encoding="utf-8", errors="replace"
        )
        (output_dir / f"{task.task_id}.stderr").write_text(
            stderr, encoding="utf-8", errors="replace"
        )
        record = analyze_output(
            task,
            stdout,
            latency_ms=latency_ms,
            exit_code=exit_code,
            repo_root=ctx.repo_root,
        )
        records.append(record)
        raw_records.append(
            {
                **asdict(record),
                "query": task.query,
                "expected_any": list(task.expected_any),
                "command": command,
            }
        )
        if output_format != "json" or task.surface != "grep":
            identity_shadow_records.append(record)
            cap_shadow_records.append(record)
            continue
        try:
            identity_shadow = build_shadow_grep_json(stdout)
            cap_shadow = build_shadow_grep_json(
                stdout,
                max_matches=shadow_grep_max_matches,
            )
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            shadow_errors.append(f"{task.task_id}: {exc}")
            identity_shadow_records.append(record)
            cap_shadow_records.append(record)
            continue
        shadow_input_matches += identity_shadow.input_matches
        shadow_identity_removed += identity_shadow.identity_duplicates_removed
        shadow_cap_dropped += cap_shadow.cap_matches_dropped
        identity_shadow_records.append(
            analyze_output(
                task,
                identity_shadow.payload,
                latency_ms=latency_ms,
                exit_code=exit_code,
                repo_root=ctx.repo_root,
            )
        )
        cap_shadow_records.append(
            analyze_output(
                task,
                cap_shadow.payload,
                latency_ms=latency_ms,
                exit_code=exit_code,
                repo_root=ctx.repo_root,
            )
        )
        (output_dir / f"{task.task_id}.shadow_identity.stdout").write_text(
            identity_shadow.payload,
            encoding="utf-8",
        )
        (output_dir / f"{task.task_id}.shadow_cap{shadow_grep_max_matches}.stdout").write_text(
            cap_shadow.payload,
            encoding="utf-8",
        )

    metrics = aggregate_output_records(records, budgets=budgets)
    metrics["token_count_available"] = 0.0
    contract_evaluation = evaluate_efficiency_contract(metrics, contract)
    metrics.update(contract_evaluation.metrics)
    attributes["efficiency_contract_violations"] = list(contract_evaluation.violations)
    metrics.update(_shadow_metric_defaults())
    if output_format == "json" and not shadow_errors:
        metrics["grep_shadow_available"] = 1.0
        metrics["grep_shadow_input_matches"] = float(shadow_input_matches)
        metrics["grep_identity_shadow_removed_fraction"] = (
            shadow_identity_removed / shadow_input_matches if shadow_input_matches else 0.0
        )
        unique_matches = shadow_input_matches - shadow_identity_removed
        metrics["grep_window_shadow_dropped_fraction"] = (
            shadow_cap_dropped / unique_matches if unique_matches else 0.0
        )
        identity_evaluation = _add_shadow_metrics(
            metrics,
            prefix="grep_identity_shadow",
            records=identity_shadow_records,
            budgets=budgets,
            contract=contract,
        )
        cap_evaluation = _add_shadow_metrics(
            metrics,
            prefix="grep_window_shadow",
            records=cap_shadow_records,
            budgets=budgets,
            contract=contract,
        )
        attributes["grep_identity_shadow_contract_violations"] = list(
            identity_evaluation.violations
        )
        attributes["grep_window_shadow_contract_violations"] = list(cap_evaluation.violations)
    attributes["grep_shadow_errors"] = shadow_errors
    write_json(
        raw_path,
        {
            "attributes": attributes,
            "records": raw_records,
            "metrics": metrics,
        },
    )
    failures = sum(record.exit_code != 0 for record in records) + len(shadow_errors)
    return WorkerResult(
        status="failed" if failures else "ok",
        exit_code=1 if failures else 0,
        metrics=metrics,
        attributes=attributes,
        message=(
            f"output_efficiency completed with {failures} command failures"
            if failures
            else f"output_efficiency completed {len(records)} YAMS codebase tasks"
        ),
        raw_path=str(raw_path),
    )
