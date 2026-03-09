#!/usr/bin/env python3

import argparse
import json
import os
import random
import re
import shutil
import statistics
import subprocess
import sys
import textwrap
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass
class CommandResult:
    duration_s: float
    stdout: str
    stderr: str
    returncode: int


def run_command(
    cmd: list[str], env: dict[str, str], capture: bool = True
) -> CommandResult:
    start = time.perf_counter()
    proc = subprocess.run(
        cmd,
        env=env,
        text=True,
        stdout=subprocess.PIPE if capture else None,
        stderr=subprocess.PIPE if capture else None,
    )
    duration_s = time.perf_counter() - start
    return CommandResult(
        duration_s=duration_s,
        stdout=proc.stdout or "",
        stderr=proc.stderr or "",
        returncode=proc.returncode,
    )


def ensure_ok(result: CommandResult, cmd: list[str], phase: str) -> None:
    if result.returncode == 0:
        return
    raise RuntimeError(
        textwrap.dedent(
            f"""
            Command failed during {phase}
            Exit: {result.returncode}
            Command: {" ".join(cmd)}
            Stdout:\n{result.stdout}
            Stderr:\n{result.stderr}
            """
        ).strip()
    )


def parse_json_payload(raw: str) -> Any:
    decoder = json.JSONDecoder()
    for idx, ch in enumerate(raw):
        if ch not in "[{":
            continue
        try:
            parsed, _ = decoder.raw_decode(raw[idx:])
            return parsed
        except json.JSONDecodeError:
            continue

    preview = raw[:400].strip()
    raise RuntimeError(
        f"Failed to parse JSON payload from command output. Preview: {preview}"
    )


def parse_list_payload(raw: str) -> tuple[list[dict[str, Any]], int]:
    data = parse_json_payload(raw)
    if isinstance(data, dict):
        docs = data.get("documents") or data.get("results") or data.get("items") or []
        total_raw = data.get("total", len(docs))
        total = int(total_raw) if isinstance(total_raw, (int, float)) else len(docs)
    elif isinstance(data, list):
        docs = data
        total = len(docs)
    else:
        docs = []
        total = 0

    rows = [row for row in docs if isinstance(row, dict)]
    return rows, total


def normalize_endpoint(endpoint: str) -> str:
    host = endpoint.strip()
    if host.startswith("https://"):
        host = host[len("https://") :]
    elif host.startswith("http://"):
        host = host[len("http://") :]
    host = host.strip("/")
    if "/" in host:
        host = host.split("/", 1)[0]
    return host


def extract_r2_account_id(endpoint: str) -> str:
    host = normalize_endpoint(endpoint).lower()
    suffixes = (
        ".r2.cloudflarestorage.com",
        ".eu.r2.cloudflarestorage.com",
        ".fedramp.r2.cloudflarestorage.com",
    )
    for suffix in suffixes:
        if not host.endswith(suffix):
            continue
        label = host[: -len(suffix)]
        if "." in label:
            return ""
        if re.fullmatch(r"[0-9a-f]{32}", label):
            return label
        return ""
    return ""


def ensure_no_storage_fallback(result: CommandResult, backend: str, phase: str) -> None:
    if backend == "local":
        return
    text = f"{result.stdout}\n{result.stderr}".lower()
    if "storage fallback activated" in text:
        raise RuntimeError(
            f"Remote backend unexpectedly fell back to local during {phase}. "
            "Check storage.s3 config and credentials."
        )


def count_local_object_files(storage_dir: Path) -> tuple[int, int]:
    objects_dir = storage_dir / "storage" / "objects"
    if not objects_dir.exists():
        return 0, 0

    count = 0
    total_bytes = 0
    for path in objects_dir.rglob("*"):
        if path.is_file():
            count += 1
            total_bytes += path.stat().st_size
    return count, total_bytes


def create_dataset(
    dataset_dir: Path, file_count: int, file_size_kb: int, seed: int
) -> None:
    random.seed(seed)
    dataset_dir.mkdir(parents=True, exist_ok=True)
    topics = ["topic_alpha", "topic_beta", "topic_gamma", "topic_delta"]
    target_chars = max(1024, file_size_kb * 1024)

    for i in range(file_count):
        group = f"group_{i % 12:02d}"
        path = dataset_dir / group
        path.mkdir(parents=True, exist_ok=True)
        file_path = path / f"doc_{i:05d}.md"
        topic = topics[i % len(topics)]
        marker = f"multi_marker_{i:05d}"

        parts = [
            f"# Document {i}",
            f"topic={topic}",
            f"marker={marker}",
            "Multi-client benchmark corpus line for local-vs-r2 read-heavy workloads.",
        ]
        body = "\n".join(parts) + "\n"
        while len(body) < target_chars:
            body += (
                f"{topic} benchmark corpus sentence {random.randint(1000, 9999)} "
                "for daemon-ipc and mcp request pressure tests.\n"
            )

        file_path.write_text(body[:target_chars], encoding="utf-8")


def configure_storage(
    yams_bin: str,
    env: dict[str, str],
    storage_dir: Path,
    backend: str,
    backend_cfg: dict[str, Any],
) -> None:
    init_cmd = [
        yams_bin,
        "--storage",
        str(storage_dir),
        "init",
        "--non-interactive",
        "--force",
        "--no-keygen",
    ]
    init_res = run_command(init_cmd, env)
    ensure_ok(init_res, init_cmd, "init")

    if backend == "local":
        cfg_cmd = [yams_bin, "config", "storage", "--engine", "local"]
        cfg_res = run_command(cfg_cmd, env)
        ensure_ok(cfg_res, cfg_cmd, "config-local")
        return

    if backend != "r2":
        raise RuntimeError(f"Unsupported backend: {backend}")

    cfg_cmd = [
        yams_bin,
        "config",
        "storage",
        "--engine",
        "s3",
        "--s3-url",
        backend_cfg["url"],
        "--s3-region",
        backend_cfg["region"],
    ]

    auth_mode = str(backend_cfg.get("auth_mode", "direct")).strip().lower()
    if auth_mode == "temp_credentials":
        cfg_cmd.extend(["--s3-r2-auth-mode", "temp_credentials"])
        cfg_cmd.extend(["--s3-r2-api-token", backend_cfg["api_token"]])

        r2_account_id = str(backend_cfg.get("r2_account_id", "")).strip()
        if r2_account_id:
            cfg_cmd.extend(["--s3-r2-account-id", r2_account_id])

        r2_parent_access_key_id = str(
            backend_cfg.get("r2_parent_access_key_id", "")
        ).strip()
        if r2_parent_access_key_id:
            cfg_cmd.extend(["--s3-r2-parent-access-key-id", r2_parent_access_key_id])

        r2_permission = str(
            backend_cfg.get("r2_permission", "object-read-write")
        ).strip()
        if r2_permission:
            cfg_cmd.extend(["--s3-r2-permission", r2_permission])

        r2_ttl_seconds = int(backend_cfg.get("r2_ttl_seconds", 3600))
        cfg_cmd.extend(["--s3-r2-ttl-seconds", str(r2_ttl_seconds)])
    else:
        cfg_cmd.extend(["--s3-access-key", backend_cfg["access_key"]])
        cfg_cmd.extend(["--s3-secret-key", backend_cfg["secret_key"]])

    endpoint = str(backend_cfg.get("endpoint", "")).strip()
    if endpoint:
        cfg_cmd.extend(["--s3-endpoint", endpoint])
    if backend_cfg.get("use_path_style", False):
        cfg_cmd.append("--s3-use-path-style")

    cfg_res = run_command(cfg_cmd, env)
    ensure_ok(cfg_res, cfg_cmd, "config-r2")


def seed_backend_corpus(
    yams_bin: str,
    env: dict[str, str],
    storage_dir: Path,
    dataset_dir: Path,
    backend: str,
    file_count: int,
) -> dict[str, int]:
    add_cmd = [
        yams_bin,
        "--storage",
        str(storage_dir),
        "add",
        str(dataset_dir),
        "--recursive",
        "--include",
        "*.md",
        "--no-embeddings",
        "--collection",
        f"multi-client-{backend}",
    ]
    add_res = run_command(add_cmd, env)
    ensure_ok(add_res, add_cmd, f"seed-add-{backend}")
    ensure_no_storage_fallback(add_res, backend, f"seed-add-{backend}")

    local_files_after_add, local_bytes_after_add = count_local_object_files(storage_dir)
    if backend == "r2" and local_files_after_add > 0:
        raise RuntimeError(
            "Remote backend isolation check failed while seeding corpus: local object files "
            f"exist in {storage_dir / 'storage' / 'objects'} "
            f"(files={local_files_after_add}, bytes={local_bytes_after_add})."
        )

    list_cmd = [
        yams_bin,
        "--storage",
        str(storage_dir),
        "list",
        "--format",
        "json",
        "--limit",
        str(file_count + 1000),
    ]
    list_res = run_command(list_cmd, env)
    ensure_ok(list_res, list_cmd, f"seed-list-{backend}")
    ensure_no_storage_fallback(list_res, backend, f"seed-list-{backend}")

    rows, total_docs = parse_list_payload(list_res.stdout)
    unique_paths = {
        str(row.get("path"))
        for row in rows
        if isinstance(row.get("path"), str) and row.get("path")
    }
    if len(unique_paths) < file_count:
        raise RuntimeError(
            f"Seed corpus validation failed for backend={backend}: expected at least {file_count} "
            f"unique docs, got {len(unique_paths)}"
        )
    return {
        "unique_docs": len(unique_paths),
        "total_docs_reported": total_docs,
        "local_object_files": local_files_after_add,
        "local_object_bytes": local_bytes_after_add,
    }


def scrub_workspace_secrets(ws: Path) -> None:
    config_path = ws / "xdg_config" / "yams" / "config.toml"
    if not config_path.exists():
        return

    sensitive_line = re.compile(
        r"^(\s*(?:storage\.)?(?:s3\.)?(?:access_key|secret_key|session_token|r2\.api_token)\s*=\s*).*$"
    )
    lines = config_path.read_text(encoding="utf-8").splitlines()
    redacted = [sensitive_line.sub(r'\1"***REDACTED***"', line) for line in lines]
    config_path.write_text("\n".join(redacted) + "\n", encoding="utf-8")


def compute_client_layout(total_clients: int) -> dict[str, int]:
    if total_clients < 4:
        raise ValueError("total_clients must be >= 4")

    grep_clients = 1
    search_clients = max(1, total_clients // 4)
    list_clients = max(1, total_clients // 4)
    status_get_clients = total_clients - search_clients - list_clients - grep_clients

    if status_get_clients < 1:
        deficit = 1 - status_get_clients
        take_search = min(deficit, max(0, search_clients - 1))
        search_clients -= take_search
        deficit -= take_search
        take_list = min(deficit, max(0, list_clients - 1))
        list_clients -= take_list
        deficit -= take_list
        status_get_clients = 1
        if deficit > 0:
            raise ValueError(
                f"Unable to allocate client layout for total={total_clients}"
            )

    return {
        "search_clients": search_clients,
        "list_clients": list_clients,
        "grep_clients": grep_clients,
        "status_get_clients": status_get_clients,
    }


def load_records(jsonl_path: Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    if not jsonl_path.exists():
        return records
    with jsonl_path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            data = json.loads(line)
            if isinstance(data, dict):
                records.append(data)
    return records


def pick_record_for_phase(
    records: list[dict[str, Any]], phase_id: str
) -> dict[str, Any]:
    for record in reversed(records):
        if str(record.get("run_phase", "")) == phase_id:
            return record
    raise RuntimeError(f"No benchmark JSONL record found for phase '{phase_id}'")


def mean(values: list[float]) -> float:
    return statistics.fmean(values) if values else 0.0


def summarize(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    buckets: dict[tuple[str, str, str, int], list[dict[str, Any]]] = {}
    for row in rows:
        key = (
            str(row["backend"]),
            str(row["usage_profile"]),
            str(row["transport"]),
            int(row["num_clients"]),
        )
        buckets.setdefault(key, []).append(row)

    output: list[dict[str, Any]] = []
    for key in sorted(buckets.keys()):
        backend, profile, transport, num_clients = key
        samples = buckets[key]
        output.append(
            {
                "backend": backend,
                "usage_profile": profile,
                "transport": transport,
                "num_clients": num_clients,
                "runs": len(samples),
                "ops_per_sec_mean": mean([float(x["ops_per_sec"]) for x in samples]),
                "fail_rate_mean": mean([float(x["fail_rate"]) for x in samples]),
                "search_p95_ms_mean": mean(
                    [float(x["search_p95_ms"]) for x in samples]
                ),
                "duration_s_mean": mean([float(x["duration_s"]) for x in samples]),
            }
        )
    return output


def render_markdown(summary_rows: list[dict[str, Any]], out_path: Path) -> None:
    lines = [
        "# Multi-client Local vs R2 Benchmark Summary",
        "",
        "| Backend | Profile | Transport | Clients | Runs | Ops/s mean | Fail rate mean | Search p95 mean (ms) | Duration mean (s) |",
        "|---|---|---|---:|---:|---:|---:|---:|---:|",
    ]
    for row in summary_rows:
        lines.append(
            "| "
            + f"{row['backend']} | {row['usage_profile']} | {row['transport']} | "
            + f"{row['num_clients']} | {row['runs']} | {row['ops_per_sec_mean']:.2f} | "
            + f"{row['fail_rate_mean']:.4f} | {row['search_p95_ms_mean']:.2f} | "
            + f"{row['duration_s_mean']:.2f} |"
        )

    out_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Benchmark multi-client read-heavy workloads across local and Cloudflare R2 "
            "for daemon-ipc and MCP transports."
        )
    )
    parser.add_argument(
        "--bench-bin", default=os.environ.get("YAMS_MULTI_CLIENT_BENCH_BIN", "")
    )
    parser.add_argument("--yams-bin", default=os.environ.get("YAMS_BENCH_BIN", "yams"))
    parser.add_argument("--iterations", type=int, default=1)
    parser.add_argument("--files", type=int, default=240)
    parser.add_argument("--file-size-kb", type=int, default=8)
    parser.add_argument("--seed", type=int, default=1337)
    parser.add_argument("--ops-per-client", type=int, default=40)
    parser.add_argument("--backends", default="local,r2")
    parser.add_argument("--profiles", default="mixed,external_agent_churn")
    parser.add_argument("--clients", default="4,8")
    parser.add_argument("--transports", default="daemon_ipc,mcp")
    parser.add_argument(
        "--output-dir",
        default="",
        help="Optional output dir (default: bench_results/storage_backends_multi_client/<timestamp>)",
    )
    parser.add_argument(
        "--require-remote",
        action="store_true",
        help="Fail if requested remote backend env is missing",
    )
    parser.add_argument(
        "--keep-workspaces",
        action="store_true",
        help="Retain backend workspaces (configs are scrubbed before keep)",
    )
    args = parser.parse_args()

    if not args.bench_bin:
        args.bench_bin = "builddir-nosan/tests/benchmarks/multi_client_ingestion_bench"

    bench_bin = str(Path(args.bench_bin))
    if shutil.which(bench_bin) is None and not Path(bench_bin).exists():
        raise RuntimeError(
            f"Could not locate multi-client benchmark binary: {bench_bin}"
        )
    if shutil.which(args.yams_bin) is None:
        raise RuntimeError(f"Could not locate yams binary: {args.yams_bin}")

    requested_backends = [x.strip() for x in args.backends.split(",") if x.strip()]
    requested_profiles = [x.strip() for x in args.profiles.split(",") if x.strip()]
    requested_clients = [int(x.strip()) for x in args.clients.split(",") if x.strip()]
    requested_transports = [x.strip() for x in args.transports.split(",") if x.strip()]

    allowed_profiles = {"mixed", "external_agent_churn"}
    for profile in requested_profiles:
        if profile not in allowed_profiles:
            raise RuntimeError(f"Unsupported profile: {profile}")

    transport_map = {"daemon_ipc": "0", "mcp": "1"}
    for transport in requested_transports:
        if transport not in transport_map:
            raise RuntimeError(f"Unsupported transport: {transport}")

    for count in requested_clients:
        if count < 4:
            raise RuntimeError("Client count must be >= 4")

    stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    run_dir = (
        Path(args.output_dir)
        if args.output_dir
        else Path("bench_results") / "storage_backends_multi_client" / stamp
    )
    run_dir.mkdir(parents=True, exist_ok=True)

    dataset_dir = run_dir / "dataset"
    create_dataset(dataset_dir, args.files, args.file_size_kb, args.seed)

    r2_auth_mode = (
        os.environ.get("YAMS_BENCH_R2_AUTH_MODE", "temp_credentials").strip().lower()
    )
    if not r2_auth_mode:
        r2_auth_mode = "temp_credentials"
    if r2_auth_mode not in {"direct", "temp_credentials"}:
        raise RuntimeError(
            "Invalid YAMS_BENCH_R2_AUTH_MODE; expected 'direct' or 'temp_credentials'"
        )

    r2_access_key = os.environ.get("YAMS_BENCH_R2_ACCESS_KEY", "")
    r2_secret_key = os.environ.get("YAMS_BENCH_R2_SECRET_KEY", "")
    r2_api_token = os.environ.get("YAMS_BENCH_R2_API_TOKEN", "")
    r2_account_id = os.environ.get("YAMS_BENCH_R2_ACCOUNT_ID", "")
    r2_parent_access_key_id = os.environ.get("YAMS_BENCH_R2_PARENT_ACCESS_KEY_ID", "")
    r2_permission = os.environ.get("YAMS_BENCH_R2_PERMISSION", "object-read-write")
    r2_ttl_seconds_raw = os.environ.get("YAMS_BENCH_R2_TTL_SECONDS", "3600")
    try:
        r2_ttl_seconds = int(r2_ttl_seconds_raw)
    except ValueError as exc:
        raise RuntimeError("YAMS_BENCH_R2_TTL_SECONDS must be an integer") from exc
    if r2_ttl_seconds <= 0:
        raise RuntimeError("YAMS_BENCH_R2_TTL_SECONDS must be > 0")

    r2_bucket = os.environ.get("YAMS_BENCH_R2_BUCKET", "")
    r2_endpoint = normalize_endpoint(os.environ.get("YAMS_BENCH_R2_ENDPOINT", ""))
    r2_region = os.environ.get("YAMS_BENCH_R2_REGION", "auto")
    r2_path_style = os.environ.get("YAMS_BENCH_R2_PATH_STYLE", "0") in (
        "1",
        "true",
        "TRUE",
    )
    r2_prefix_base = os.environ.get(
        "YAMS_BENCH_R2_PREFIX", f"bench/multi-client/{stamp}"
    )

    r2_ready_direct = all((r2_access_key, r2_secret_key, r2_bucket, r2_endpoint))
    r2_ready_temp = all((r2_api_token, r2_bucket, r2_endpoint))
    r2_ready = r2_ready_temp if r2_auth_mode == "temp_credentials" else r2_ready_direct

    if "r2" in requested_backends and not r2_ready and args.require_remote:
        if r2_auth_mode == "temp_credentials":
            raise RuntimeError(
                "R2 requested with temp_credentials mode but env is incomplete. Set "
                "YAMS_BENCH_R2_BUCKET, YAMS_BENCH_R2_ENDPOINT, YAMS_BENCH_R2_API_TOKEN "
                "(optional: YAMS_BENCH_R2_ACCOUNT_ID, YAMS_BENCH_R2_PARENT_ACCESS_KEY_ID, "
                "YAMS_BENCH_R2_PERMISSION, YAMS_BENCH_R2_TTL_SECONDS)."
            )
        raise RuntimeError(
            "R2 requested with direct mode but env is incomplete. Set "
            "YAMS_BENCH_R2_BUCKET, YAMS_BENCH_R2_ENDPOINT, YAMS_BENCH_R2_ACCESS_KEY, "
            "YAMS_BENCH_R2_SECRET_KEY"
        )

    if "r2" in requested_backends and r2_auth_mode == "temp_credentials" and r2_ready:
        endpoint_account_id = extract_r2_account_id(r2_endpoint)
        if (
            r2_account_id
            and endpoint_account_id
            and r2_account_id != endpoint_account_id
        ):
            raise RuntimeError(
                "R2 temp_credentials misconfiguration: "
                "YAMS_BENCH_R2_ACCOUNT_ID does not match account id encoded in "
                "YAMS_BENCH_R2_ENDPOINT "
                f"({r2_account_id} vs {endpoint_account_id})."
            )
        if not r2_account_id and not endpoint_account_id:
            raise RuntimeError(
                "R2 temp_credentials requires an account id. Set "
                "YAMS_BENCH_R2_ACCOUNT_ID or use a canonical endpoint like "
                "<account-id>.r2.cloudflarestorage.com."
            )

    if "r2" in requested_backends and not r2_ready and not args.require_remote:
        requested_backends = [b for b in requested_backends if b != "r2"]
        print("[bench] Skipping r2 backend: missing required YAMS_BENCH_R2_* env vars.")

    if not requested_backends:
        raise RuntimeError("No runnable backends after validation")

    results_jsonl = run_dir / "results.jsonl"
    if results_jsonl.exists():
        results_jsonl.unlink()

    all_rows: list[dict[str, Any]] = []
    backend_seed_info: dict[str, dict[str, Any]] = {}

    for backend in requested_backends:
        ws = run_dir / "workspaces" / backend
        xdg_config = ws / "xdg_config"
        xdg_data = ws / "xdg_data"
        home = ws / "home"
        storage_dir = ws / "storage"
        for p in (xdg_config, xdg_data, home, storage_dir):
            p.mkdir(parents=True, exist_ok=True)

        env_base = os.environ.copy()
        env_base["XDG_CONFIG_HOME"] = str(xdg_config)
        env_base["XDG_DATA_HOME"] = str(xdg_data)
        env_base["HOME"] = str(home)
        env_base["YAMS_EMBEDDED"] = "1"
        env_base["YAMS_DISABLE_VECTORS"] = "1"
        env_base["YAMS_SKIP_MODEL_LOADING"] = "1"
        env_base["YAMS_BENCH_OUTPUT"] = str(results_jsonl)

        backend_cfg: dict[str, Any] = {}
        auth_mode = "direct"
        if backend == "r2":
            auth_mode = r2_auth_mode
            if r2_auth_mode == "temp_credentials":
                backend_cfg = {
                    "url": f"s3://{r2_bucket}/{r2_prefix_base}/seed",
                    "region": r2_region,
                    "endpoint": r2_endpoint,
                    "auth_mode": "temp_credentials",
                    "api_token": r2_api_token,
                    "r2_account_id": r2_account_id,
                    "r2_parent_access_key_id": r2_parent_access_key_id,
                    "r2_permission": r2_permission,
                    "r2_ttl_seconds": r2_ttl_seconds,
                    "use_path_style": r2_path_style,
                }
            else:
                backend_cfg = {
                    "url": f"s3://{r2_bucket}/{r2_prefix_base}/seed",
                    "region": r2_region,
                    "endpoint": r2_endpoint,
                    "auth_mode": "direct",
                    "access_key": r2_access_key,
                    "secret_key": r2_secret_key,
                    "use_path_style": r2_path_style,
                }

        configure_storage(args.yams_bin, env_base, storage_dir, backend, backend_cfg)
        config_path = xdg_config / "yams" / "config.toml"
        if not config_path.exists():
            raise RuntimeError(f"Expected config not found: {config_path}")

        seed_info = seed_backend_corpus(
            args.yams_bin,
            env_base,
            storage_dir,
            dataset_dir,
            backend,
            args.files,
        )
        backend_seed_info[backend] = {
            **seed_info,
            "workspace": str(ws),
            "storage_dir": str(storage_dir),
            "config_path": str(config_path),
            "auth_mode": auth_mode,
        }

        for profile in requested_profiles:
            for total_clients in requested_clients:
                layout = compute_client_layout(total_clients)
                for transport in requested_transports:
                    for iteration in range(1, args.iterations + 1):
                        phase_id = (
                            f"{backend}-{profile}-c{total_clients}-{transport}-"
                            f"iter{iteration:02d}"
                        )
                        env_case = env_base.copy()
                        env_case["YAMS_BENCH_DATA_DIR"] = str(storage_dir)
                        env_case["YAMS_BENCH_CONFIG"] = str(config_path)
                        env_case["YAMS_BENCH_RUN_ID"] = stamp
                        env_case["YAMS_BENCH_PHASE"] = phase_id
                        env_case["YAMS_BENCH_CASE"] = (
                            "Multi-client ingestion: large corpus reads"
                        )
                        env_case["YAMS_BENCH_TRANSPORT"] = transport
                        env_case["YAMS_BENCH_GIT_SHA"] = ""
                        env_case["YAMS_BENCH_BACKEND"] = backend
                        env_case["YAMS_BENCH_AUTH_MODE"] = auth_mode
                        env_case["YAMS_BENCH_USE_MCP"] = transport_map[transport]
                        env_case["YAMS_BENCH_USAGE_PROFILE"] = profile
                        env_case["YAMS_BENCH_OPS_PER_CLIENT"] = str(args.ops_per_client)
                        env_case["YAMS_BENCH_SEARCH_CLIENTS"] = str(
                            layout["search_clients"]
                        )
                        env_case["YAMS_BENCH_LIST_CLIENTS"] = str(
                            layout["list_clients"]
                        )
                        env_case["YAMS_BENCH_GREP_CLIENTS"] = str(
                            layout["grep_clients"]
                        )
                        env_case["YAMS_BENCH_STATUS_GET_CLIENTS"] = str(
                            layout["status_get_clients"]
                        )

                        cmd = [
                            bench_bin,
                            "--allow-running-no-tests",
                            "Multi-client ingestion: large corpus reads",
                        ]
                        run_res = run_command(cmd, env_case)
                        ensure_ok(run_res, cmd, phase_id)
                        ensure_no_storage_fallback(run_res, backend, phase_id)

                        phase_record = pick_record_for_phase(
                            load_records(results_jsonl), phase_id
                        )
                        total_ops = float(phase_record.get("total_ops", 0.0))
                        total_failures = float(phase_record.get("total_failures", 0.0))
                        fail_rate = (
                            (total_failures / total_ops) if total_ops > 0 else 0.0
                        )
                        search_p95_ms = (
                            float(
                                phase_record.get("search", {})
                                .get("latency", {})
                                .get("p95_us", 0.0)
                            )
                            / 1000.0
                        )

                        row = {
                            "phase": phase_id,
                            "backend": backend,
                            "auth_mode": auth_mode,
                            "usage_profile": profile,
                            "transport": transport,
                            "num_clients": total_clients,
                            "iteration": iteration,
                            "ops_per_sec": float(phase_record.get("ops_per_sec", 0.0)),
                            "fail_rate": fail_rate,
                            "search_p95_ms": search_p95_ms,
                            "duration_s": run_res.duration_s,
                            "layout": layout,
                        }
                        all_rows.append(row)
                        print(
                            f"[bench] backend={backend} profile={profile} transport={transport} "
                            f"clients={total_clients} iter={iteration} "
                            f"ops={row['ops_per_sec']:.2f} fail={row['fail_rate']:.4f} "
                            f"search_p95={row['search_p95_ms']:.2f}ms"
                        )

        if not args.keep_workspaces:
            shutil.rmtree(ws, ignore_errors=True)
        else:
            scrub_workspace_secrets(ws)

    summary_rows = summarize(all_rows)
    output = {
        "timestamp": stamp,
        "params": {
            "iterations": args.iterations,
            "files": args.files,
            "file_size_kb": args.file_size_kb,
            "ops_per_client": args.ops_per_client,
            "backends": requested_backends,
            "profiles": requested_profiles,
            "clients": requested_clients,
            "transports": requested_transports,
            "r2_auth_mode": r2_auth_mode if "r2" in requested_backends else "",
        },
        "seed": backend_seed_info,
        "results": all_rows,
        "summary": summary_rows,
        "jsonl": str(results_jsonl),
    }

    results_json = run_dir / "results.json"
    results_json.write_text(json.dumps(output, indent=2), encoding="utf-8")
    summary_md = run_dir / "summary.md"
    render_markdown(summary_rows, summary_md)

    print(f"[bench] Wrote {results_json}")
    print(f"[bench] Wrote {summary_md}")
    print(f"[bench] Wrote {results_jsonl}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"[bench] ERROR: {exc}", file=sys.stderr)
        raise
