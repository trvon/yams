"""Adapter worker for tests/benchmarks/search/ingestion_e2e_bench."""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Any

from artifacts import raw_worker_output_path, write_json
from workers.ablation import apply_ablation
from workers.base import WorkerContext, WorkerResult


_METRIC_TOKEN = re.compile(r"[^a-z0-9]+")
_HASH_CACHE: dict[tuple[str, int, int], str] = {}


def _sha256_file(path: Path) -> str:
    stat = path.stat()
    key = (str(path.resolve()), stat.st_size, stat.st_mtime_ns)
    cached = _HASH_CACHE.get(key)
    if cached:
        return cached
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    value = digest.hexdigest()
    _HASH_CACHE[key] = value
    return value


def _artifact_identity(binary: Path, raw: dict[str, Any]) -> dict[str, Any]:
    cfg = raw.get("test_config") or {}
    identity: dict[str, Any] = {
        "binary_sha256": _sha256_file(binary),
    }
    model_path_raw = cfg.get("gliner_model_path") if isinstance(cfg, dict) else None
    if isinstance(cfg, dict) and cfg.get("gliner_requested") and model_path_raw:
        model_path = Path(str(model_path_raw)).expanduser()
        if model_path.is_file():
            identity["gliner_model_name"] = model_path.parent.name
            identity["gliner_model_sha256"] = _sha256_file(model_path)
            tokenizer = model_path.parent / "tokenizer.json"
            if tokenizer.is_file():
                identity["gliner_tokenizer_sha256"] = _sha256_file(tokenizer)
    return identity


def _metric_token(value: object) -> str:
    return _METRIC_TOKEN.sub("_", str(value).strip().lower()).strip("_") or "unknown"


def _truthy(value: object) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _flatten_numeric_block(
    metrics: dict[str, Any], prefix: str, block: object
) -> None:
    if not isinstance(block, dict):
        return
    for key, value in block.items():
        if isinstance(value, bool):
            metrics[f"{prefix}_{_metric_token(key)}"] = 1.0 if value else 0.0
        elif isinstance(value, (int, float)):
            metrics[f"{prefix}_{_metric_token(key)}"] = float(value)


def _flatten_phase_group(
    metrics: dict[str, Any], prefix: str, group: object
) -> None:
    if not isinstance(group, dict):
        return
    for phase, values in group.items():
        _flatten_numeric_block(metrics, f"{prefix}_{_metric_token(phase)}", values)


def _flatten_named_numeric_records(
    metrics: dict[str, Any], prefix: str, records: object, name_key: str
) -> None:
    if not isinstance(records, list):
        return
    for record in records:
        if not isinstance(record, dict) or not record.get(name_key):
            continue
        _flatten_numeric_block(
            metrics,
            f"{prefix}_{_metric_token(record[name_key])}",
            record,
        )


def _phase_total_ms(block: dict[str, Any] | None, key: str) -> float:
    if not isinstance(block, dict):
        return 0.0
    entry = block.get(key) or {}
    if not isinstance(entry, dict):
        return 0.0
    return float(entry.get("total_ms") or 0.0)


def _probe_map(raw: dict[str, Any]) -> dict[str, dict[str, Any]]:
    probes = raw.get("search_impact") or []
    out: dict[str, dict[str, Any]] = {}
    if isinstance(probes, list):
        for probe in probes:
            if isinstance(probe, dict) and probe.get("name"):
                out[str(probe["name"])] = probe
    return out


def extract_metrics(raw: dict[str, Any]) -> dict[str, Any]:
    cfg = raw.get("test_config") or {}
    status = raw.get("pipeline_status") or {}
    queues = raw.get("queues") or {}
    post = raw.get("post_ingest_phase_timings") or {}
    doc_store = raw.get("document_store_phase_timings") or {}
    probes = _probe_map(raw)
    keyword = probes.get("keyword") or {}
    semantic = probes.get("semantic") or {}
    graph = probes.get("graph_rerank_hybrid") or {}

    metrics: dict[str, Any] = {
        "total_duration_ms": float(raw.get("total_duration_ms") or 0),
        "docs_per_s": float(raw.get("throughput_docs_per_sec") or 0),
        "mib_per_s": float(raw.get("throughput_mib_per_sec") or 0),
        "pipeline_complete": 1.0 if status.get("complete") else 0.0,
        "post_ingest_coalesce_ms": float(cfg.get("post_ingest_coalesce_ms") or 0),
        "post_ingest_batch_size": float(cfg.get("post_ingest_batch_size") or 0),
        "kg_enabled": 1.0 if cfg.get("kg_enabled") else 0.0,
        "expected_kg": float(status.get("expected_kg") or 0),
        "observed_kg": float(status.get("observed_kg") or 0),
        "expected_embed": float(status.get("expected_embed") or 0),
        "observed_embed": float(status.get("observed_embed") or 0),
        "expected_post": float(status.get("expected_post") or 0),
        "observed_post": float(status.get("observed_post") or 0),
        "expected_title": float(status.get("expected_title") or 0),
        "observed_title": float(status.get("observed_title") or 0),
        "dropped_batches": float(queues.get("dropped_batches") or 0),
        "extract_ms": _phase_total_ms(post, "prepare_metadata_entry"),
        "kg_ms": _phase_total_ms(post, "kg_graph_component"),
        "dispatch_ms": _phase_total_ms(post, "dispatch_successes"),
        "embed_prepare_ms": _phase_total_ms(post, "dispatch_embed_prepare"),
        "document_store_total_ms": _phase_total_ms(doc_store, "document_store_total")
        if "document_store_total" in (doc_store or {})
        else float(
            sum(
                float((v or {}).get("total_ms") or 0)
                for v in (doc_store or {}).values()
                if isinstance(v, dict)
            )
        ),
        "keyword_ms": float(keyword.get("wall_ms") or 0),
        "keyword_total": float(keyword.get("total_count") or 0),
        "semantic_ms": float(semantic.get("wall_ms") or 0),
        "semantic_total": float(semantic.get("total_count") or 0),
        "graph_ms": float(graph.get("wall_ms") or 0),
        "graph_total": float(graph.get("total_count") or 0),
    }

    _flatten_numeric_block(metrics, "phase", raw.get("phase_timings"))
    for public_name in (
        "daemon_startup_ms",
        "admission_ms",
        "storage_ready_ms",
        "pipeline_drain_ms",
        "enrichment_ready_ms",
        "vector_index_finalize_ms",
        "searchability_ready_ms",
        "shutdown_ms",
        "lifecycle_ms",
    ):
        metrics[public_name] = float(
            ((raw.get("phase_timings") or {}).get(public_name) or 0)
            if isinstance(raw.get("phase_timings"), dict)
            else 0
        )

    stages = raw.get("stages") or {}
    if isinstance(stages, dict):
        for stage, values in stages.items():
            _flatten_numeric_block(metrics, f"stage_{_metric_token(stage)}", values)

    _flatten_numeric_block(metrics, "queue", queues)
    _flatten_numeric_block(metrics, "add_dispatch", raw.get("add_dispatch_metrics"))
    _flatten_numeric_block(
        metrics,
        "metadata_insert_writer",
        raw.get("metadata_insert_writer_metrics"),
    )
    _flatten_numeric_block(metrics, "post_ingest_batch", raw.get("post_ingest_batch_metrics"))
    _flatten_numeric_block(metrics, "write_coordinator", raw.get("write_coordinator"))
    write_coordinator = raw.get("write_coordinator") or {}
    if isinstance(write_coordinator, dict):
        _flatten_named_numeric_records(
            metrics,
            "write_coordinator_source",
            write_coordinator.get("hot_sources"),
            "source",
        )
    _flatten_numeric_block(metrics, "work_coordinator", raw.get("work_coordinator"))
    _flatten_numeric_block(metrics, "storage_oracle", raw.get("storage_oracle"))
    _flatten_numeric_block(metrics, "vector_index", raw.get("vector_index"))
    _flatten_numeric_block(metrics, "resource", raw.get("resources"))

    enrichment = raw.get("enrichment_status") or {}
    if isinstance(enrichment, dict):
        for stage, values in enrichment.items():
            _flatten_numeric_block(metrics, f"enrichment_{_metric_token(stage)}", values)

    for group_name in (
        "embedding_phase_timings",
        "document_store_phase_timings",
        "content_store_phase_timings",
        "metadata_insert_phase_timings",
        "ref_counter_commit_phase_timings",
        "ref_counter_writer_timings",
        "ref_counter_writer_value_metrics",
        "post_ingest_phase_timings",
    ):
        _flatten_phase_group(metrics, _metric_token(group_name), raw.get(group_name))

    writer_apply_total_us = metrics.get(
        "metadata_insert_writer_batch_apply_total_us"
    )
    transaction_total_us = metrics.get(
        "metadata_insert_phase_timings_transaction_total_total_us"
    )
    apply_samples = metrics.get("metadata_insert_writer_batch_apply_samples")
    if isinstance(writer_apply_total_us, (int, float)) and isinstance(
        transaction_total_us, (int, float)
    ):
        estimated_total_us = max(
            0.0, float(writer_apply_total_us) - float(transaction_total_us)
        )
        metrics[
            "metadata_insert_writer_connection_wait_estimated_total_us"
        ] = estimated_total_us
        metrics[
            "metadata_insert_writer_connection_wait_estimated_avg_us"
        ] = (
            estimated_total_us / float(apply_samples)
            if isinstance(apply_samples, (int, float)) and apply_samples > 0
            else 0.0
        )

    return metrics


def ingestion_experiment_identity(raw: dict[str, Any]) -> dict[str, Any]:
    """Return the stable workload identity; deliberately excludes timestamps and host state."""
    cfg = raw.get("test_config") or {}
    if not isinstance(cfg, dict):
        cfg = {}
    keys = (
        "corpus_size",
        "doc_size",
        "corpus_seed",
        "corpus_fingerprint",
        "ingest_mode",
        "ingest_concurrency",
        "post_ingest_coalesce_ms",
        "post_ingest_batch_size",
        "embedding_model",
        "embedding_backend",
        "embedding_provider",
        "embedding_provider_version",
        "embedding_model_uri",
        "embedding_space_identity",
        "embedding_dimension",
        "simeon_recipe",
        "kg_enabled",
        "gliner_requested",
        "plugin_autoload_enabled",
        "gliner_stage_active",
    )
    identity = {key: cfg.get(key) for key in keys}
    identity["artifacts"] = raw.get("artifact_identity") or {}
    return identity


def require_ingestion_experiment_identity(
    identity_path: Path, identity: dict[str, Any]
) -> str | None:
    observed = json.dumps(identity, sort_keys=True, separators=(",", ":"))
    if identity_path.is_file():
        expected = identity_path.read_text(encoding="utf-8").strip()
        if expected != observed:
            return (
                "ingestion experiment identity mismatch: "
                f"expected={expected} observed={observed}"
            )
        return None
    identity_path.parent.mkdir(parents=True, exist_ok=True)
    identity_path.write_text(observed + "\n", encoding="utf-8")
    return None


def validate_ingestion_contract(
    raw: dict[str, Any],
    *,
    require_full_searchability: bool = False,
    require_full_oracle: bool = False,
    require_spq_persistence: bool = False,
    minimum_vector_count: int = 0,
) -> list[str]:
    errors: list[str] = []
    cfg = raw.get("test_config") or {}
    status = raw.get("pipeline_status") or {}
    queues = raw.get("queues") or {}
    stages = raw.get("stages") or {}

    if not isinstance(status, dict) or status.get("complete") is not True:
        errors.append("pipeline incomplete")
    if isinstance(status, dict):
        for stage in ("post", "embed", "kg", "title"):
            expected = int(status.get(f"expected_{stage}") or 0)
            observed = int(status.get(f"observed_{stage}") or 0)
            if observed < expected:
                errors.append(
                    f"{stage} incomplete: observed={observed} expected={expected}"
                )
            elif observed > expected:
                errors.append(
                    f"{stage} duplicate work: observed={observed} expected={expected}"
                )
        if status.get("enrichment_drained") is False:
            errors.append("enrichment queues did not drain")
        if status.get("write_coordinator_flushed") is False:
            errors.append("WriteCoordinator did not flush")

    dropped = int(queues.get("dropped_batches") or 0) if isinstance(queues, dict) else 0
    if dropped != 0:
        errors.append(f"pipeline dropped {dropped} batches")

    if isinstance(stages, dict):
        metadata_stage = stages.get("metadata_storage") or {}
        if isinstance(metadata_stage, dict) and int(metadata_stage.get("failures") or 0) != 0:
            errors.append(
                f"document admission failures={int(metadata_stage.get('failures') or 0)}"
            )

    enrichment = raw.get("enrichment_status") or {}
    kg_enabled = bool(cfg.get("kg_enabled")) if isinstance(cfg, dict) else False
    if isinstance(enrichment, dict):
        for name, values in enrichment.items():
            if not isinstance(values, dict):
                continue
            if name == "kg" and not kg_enabled:
                continue
            queued = int(values.get("queued") or 0)
            consumed = int(values.get("consumed") or 0)
            stage_dropped = int(values.get("dropped") or 0)
            if stage_dropped:
                errors.append(f"{name} enrichment dropped={stage_dropped}")
            if consumed < queued:
                errors.append(
                    f"{name} enrichment incomplete: consumed={consumed} queued={queued}"
                )

    write_coordinator = raw.get("write_coordinator") or {}
    if isinstance(write_coordinator, dict):
        commit_errors = int(write_coordinator.get("commit_errors") or 0)
        if commit_errors:
            errors.append(f"WriteCoordinator commit_errors={commit_errors}")

    corpus_size = int(cfg.get("corpus_size") or 0) if isinstance(cfg, dict) else 0
    probes = raw.get("search_impact") or []
    if isinstance(probes, list):
        for probe in probes:
            if not isinstance(probe, dict) or not probe.get("required_by_contract"):
                continue
            name = str(probe.get("name") or "unknown")
            if probe.get("skipped") or probe.get("ok") is not True:
                errors.append(f"required searchability probe failed: {name}")
                continue
            if int(probe.get("returned_count") or 0) <= 0:
                errors.append(f"required searchability probe returned no results: {name}")
            if (
                require_full_searchability
                and name == "keyword"
                and int(probe.get("total_count") or 0) < corpus_size
            ):
                errors.append(
                    "corpus not fully searchable: "
                    f"keyword_total={int(probe.get('total_count') or 0)} "
                    f"corpus_size={corpus_size}"
                )
    if require_full_oracle:
        artifacts = raw.get("artifact_identity") or {}
        required_identity = (
            "embedding_provider",
            "embedding_provider_version",
            "embedding_model_uri",
            "embedding_space_identity",
            "embedding_dimension",
        )
        for key in required_identity:
            if not cfg.get(key):
                errors.append(f"missing ingestion identity field: {key}")
        if not artifacts.get("binary_sha256"):
            errors.append("missing benchmark binary SHA-256")

        if cfg.get("gliner_requested"):
            if cfg.get("gliner_stage_active") is not True:
                errors.append("GLiNER requested but title stage is inactive")
            if not artifacts.get("gliner_model_sha256"):
                errors.append("GLiNER requested without a hashed model artifact")
            if not artifacts.get("gliner_tokenizer_sha256"):
                errors.append("GLiNER requested without a hashed tokenizer artifact")

        storage = raw.get("storage_oracle") or {}
        corpus_size = int(cfg.get("corpus_size") or 0)
        if storage.get("stored_document_count_readable") is not True:
            errors.append("stored document count unavailable")
        elif int(storage.get("stored_documents") or 0) != corpus_size:
            errors.append(
                "stored document count mismatch: "
                f"stored={int(storage.get('stored_documents') or 0)} expected={corpus_size}"
            )

        vectors_enabled = cfg.get("embedding_backend") != "mock"
        if vectors_enabled:
            stored_vectors = int(storage.get("stored_vectors") or 0)
            if storage.get("vector_oracle_complete") is not True:
                errors.append("vector identity oracle incomplete")
            if int(storage.get("embedded_documents") or 0) != corpus_size:
                errors.append("embedded document count mismatch")
            if int(storage.get("invalid_vector_dimensions") or 0) != 0:
                errors.append("vector dimension mismatch")
            if int(storage.get("non_finite_vector_values") or 0) != 0:
                errors.append("non-finite vector values")
            if int(storage.get("non_finite_vector_search_scores") or 0) != 0:
                errors.append("non-finite vector search scores")
            if int(storage.get("vector_search_result_count") or 0) == 0:
                errors.append("empty vector search oracle")
            if not storage.get("embedding_fingerprint"):
                errors.append("missing embedding fingerprint")
            if not storage.get("vector_search_fingerprint"):
                errors.append("missing vector search fingerprint")
            if stored_vectors < minimum_vector_count:
                errors.append(
                    f"stored vector count {stored_vectors} below minimum {minimum_vector_count}"
                )

            vector_index = raw.get("vector_index") or {}
            if vector_index.get("rebuild_ok") is not True:
                errors.append("vector index rebuild/persist failed")
            if vector_index.get("coordinator_ready") is not True:
                errors.append("vector index coordinator not ready")
            if int(vector_index.get("coordinator_pending_reasons") or 0) != 0:
                errors.append("vector index coordinator has pending rebuild reasons")
            if int(vector_index.get("coordinator_active_bulk_scopes") or 0) != 0:
                errors.append("vector index coordinator retained bulk scopes")
            if require_spq_persistence:
                if vector_index.get("reusable_persisted_index") is not True:
                    errors.append("persisted SPQ index is not reusable")
                if vector_index.get("persistence_readable") is not True:
                    errors.append("persisted SPQ metadata is unavailable")
                if vector_index.get("trained") is not True:
                    errors.append("persisted SPQ index is not trained")
                authoritative = int(vector_index.get("authoritative_generation") or -1)
                source = int(vector_index.get("source_generation") or -2)
                if authoritative != source:
                    errors.append(
                        f"SPQ generation mismatch: authoritative={authoritative} source={source}"
                    )

        if not storage.get("search_result_fingerprint"):
            errors.append("missing search-result fingerprint")
        if not storage.get("search_score_fingerprint"):
            errors.append("missing search-score fingerprint")
        for probe in raw.get("search_impact") or []:
            if not isinstance(probe, dict) or not probe.get("required_by_contract"):
                continue
            paths = probe.get("top_paths") or []
            scores = probe.get("top_scores") or []
            if not paths or any(not path for path in paths):
                errors.append(
                    f"required probe lacks stable result paths: {probe.get('name') or 'unknown'}"
                )
            if probe.get("scores_finite") is not True or len(scores) != len(paths):
                errors.append(
                    f"required probe lacks finite aligned scores: {probe.get('name') or 'unknown'}"
                )

        resources = raw.get("resources") or {}
        for key in ("shutdown_succeeded", "socket_removed", "pid_removed", "sidecars_removed"):
            if resources.get(key) is not True:
                errors.append(f"unclean benchmark shutdown: {key}=false")
        for key in ("cpu_ms", "peak_rss_after_bytes", "wal_growth_bytes", "db_lock_errors"):
            if key not in resources:
                errors.append(f"missing resource metric: {key}")

    return errors


def _resolve_binary(ctx: WorkerContext) -> Path:
    explicit = ctx.params.get("binary") or os.environ.get("YAMS_BENCH_BINARY")
    if explicit:
        return Path(str(explicit))
    return ctx.build_dir / "tests" / "benchmarks" / "ingestion_e2e_bench"


def _maybe_build(ctx: WorkerContext, binary: Path) -> None:
    if binary.exists() and os.access(binary, os.X_OK):
        return
    if ctx.params.get("skip_build") or os.environ.get("YAMS_BENCH_SKIP_BUILD") == "1":
        return
    target = str(ctx.params.get("meson_target") or "bench_ingestion_e2e")
    subprocess.run(
        ["meson", "compile", "-C", str(ctx.build_dir), "-j4", target],
        check=False,
        cwd=str(ctx.repo_root),
    )


def run_ingestion_e2e(ctx: WorkerContext) -> WorkerResult:
    binary = _resolve_binary(ctx)
    raw_path = raw_worker_output_path(ctx.arm_dir, ctx.step_index, "ingestion_e2e")

    if ctx.dry_run:
        metrics = {name: 0.0 for name in ctx.step.metrics}
        metrics.update(
            {
                "total_duration_ms": 0.0,
                "docs_per_s": 0.0,
                "pipeline_complete": 0.0,
                "dropped_batches": 0.0,
            }
        )
        write_json(
            raw_path,
            {"dry_run": True, "binary": str(binary), "params": ctx.params},
        )
        return WorkerResult(
            status="ok",
            exit_code=0,
            metrics=metrics,
            attributes={"dry_run": True, "binary": str(binary)},
            message="dry-run: skipped ingestion_e2e execution",
            raw_path=str(raw_path),
        )

    _maybe_build(ctx, binary)
    if not binary.exists():
        return WorkerResult(
            status="failed",
            exit_code=2,
            metrics={},
            attributes={"binary": str(binary)},
            message=f"ingestion_e2e binary not found: {binary}",
        )

    env = os.environ.copy()
    env.update(ctx.env)
    env.setdefault("YAMS_TEST_SAFE_SINGLE_INSTANCE", "1")

    arm_factors = getattr(getattr(ctx.arm, "arm", None), "factors", None) or {}
    ablation = apply_ablation(env, factors=arm_factors, params=ctx.params)
    gliner_enabled = not _truthy(env.get("YAMS_DISABLE_GLINER_TITLES", "0"))
    if gliner_enabled:
        glint_dir = (ctx.build_dir / "plugins" / "glint").resolve()
        if glint_dir.is_dir():
            env["YAMS_BENCH_GLINT_PLUGIN_DIR"] = str(glint_dir)
        if not env.get("YAMS_GLINT_MODEL_PATH"):
            default_gliner_model = (
                Path.home()
                / ".local"
                / "share"
                / "yams"
                / "models"
                / "gliner"
                / "gliner_small-v2.1"
                / "model.onnx"
            )
            if default_gliner_model.is_file():
                env["YAMS_GLINT_MODEL_PATH"] = str(default_gliner_model)

    # Prefer explicit harness env (shell wrappers), then plan params.
    corpus_size = int(
        env.get("YAMS_BENCH_CORPUS_SIZE") or ctx.params.get("corpus_size") or 100
    )
    doc_size = int(env.get("YAMS_BENCH_DOC_SIZE") or ctx.params.get("doc_size") or 1000)
    poll_ms = int(
        env.get("YAMS_BENCH_POLL_INTERVAL_MS") or ctx.params.get("poll_interval_ms") or 100
    )
    ingest_mode = str(
        env.get("YAMS_BENCH_INGEST_MODE")
        or ctx.params.get("ingest_mode")
        or "single_file_serial"
    )
    ingest_concurrency = int(
        env.get("YAMS_BENCH_INGEST_CONCURRENCY")
        or ctx.params.get("ingest_concurrency")
        or 4
    )
    post_ingest_coalesce_raw = env.get("YAMS_BENCH_POST_INGEST_COALESCE_MS")
    if post_ingest_coalesce_raw is None:
        post_ingest_coalesce_raw = ctx.params.get("post_ingest_coalesce_ms", 2)
    post_ingest_coalesce_ms = int(post_ingest_coalesce_raw)
    post_ingest_batch_size = int(ctx.params.get("post_ingest_batch_size") or 0)

    env["YAMS_BENCH_CORPUS_SIZE"] = str(corpus_size)
    env["YAMS_BENCH_DOC_SIZE"] = str(doc_size)
    env["YAMS_BENCH_POLL_INTERVAL_MS"] = str(poll_ms)
    env["YAMS_BENCH_INGEST_MODE"] = ingest_mode
    env["YAMS_BENCH_INGEST_CONCURRENCY"] = str(max(1, ingest_concurrency))
    env["YAMS_BENCH_POST_INGEST_COALESCE_MS"] = str(
        max(0, min(20, post_ingest_coalesce_ms))
    )
    if post_ingest_batch_size > 0:
        env["YAMS_BENCH_POST_INGEST_BATCH_SIZE"] = str(
            max(1, min(256, post_ingest_batch_size))
        )
    timeout = ctx.step.timeout_sec
    if timeout is None:
        timeout = int(ctx.params.get("timeout_sec") or ctx.arm.plan.timeout_sec)

    stdout_path = ctx.arm_dir / f"step{ctx.step_index:02d}_ingestion_e2e.stdout.log"
    stderr_path = ctx.arm_dir / f"step{ctx.step_index:02d}_ingestion_e2e.stderr.log"
    configured_phases = ctx.params.get("lifecycle_phases") or []
    lifecycle_phases = [str(phase) for phase in configured_phases]
    if not lifecycle_phases:
        lifecycle_phases = [str(env.get("YAMS_BENCH_PHASE") or "initial")]

    fixture_root: Path | None = None
    if len(lifecycle_phases) > 1:
        fixture_root = Path(tempfile.mkdtemp(prefix="yams_e2e_bench_", dir="/tmp"))
        env["YAMS_BENCH_FIXTURE_ROOT"] = str(fixture_root)

    lifecycle_runs: list[dict[str, Any]] = []
    stdout_parts: list[str] = []
    stderr_parts: list[str] = []
    proc: subprocess.CompletedProcess[str] | None = None
    try:
        for phase_index, phase in enumerate(lifecycle_phases):
            phase_env = env.copy()
            phase_env["YAMS_BENCH_PHASE"] = phase
            phase_raw_path = (
                raw_path
                if len(lifecycle_phases) == 1
                else ctx.arm_dir
                / f"step{ctx.step_index:02d}_ingestion_e2e.{phase_index:02d}-{phase}.raw.json"
            )
            phase_env["YAMS_BENCH_OUTPUT"] = str(phase_raw_path)
            try:
                proc = subprocess.run(
                    [str(binary)],
                    cwd=str(ctx.repo_root),
                    env=phase_env,
                    text=True,
                    capture_output=True,
                    timeout=timeout,
                    check=False,
                )
            except subprocess.TimeoutExpired as exc:
                stdout_parts.append(exc.stdout or "")
                stderr_parts.append(exc.stderr or "")
                stdout_path.write_text("\n".join(stdout_parts), encoding="utf-8", errors="replace")
                stderr_path.write_text("\n".join(stderr_parts), encoding="utf-8", errors="replace")
                return WorkerResult(
                    status="failed",
                    exit_code=124,
                    metrics={},
                    attributes={"binary": str(binary), "timeout_sec": timeout, "phase": phase},
                    message=f"ingestion_e2e phase {phase} timed out after {timeout}s",
                    raw_path=str(phase_raw_path) if phase_raw_path.exists() else None,
                )

            stdout_parts.append(f"=== lifecycle phase {phase} ===\n{proc.stdout or ''}")
            stderr_parts.append(f"=== lifecycle phase {phase} ===\n{proc.stderr or ''}")
            if not phase_raw_path.exists():
                if proc.stdout and proc.stdout.strip().startswith("{"):
                    phase_raw_path.write_text(proc.stdout, encoding="utf-8")
                else:
                    return WorkerResult(
                        status="failed",
                        exit_code=1,
                        metrics={},
                        attributes={"binary": str(binary), "phase": phase},
                        message=f"missing worker raw output for lifecycle phase {phase}",
                    )
            try:
                phase_raw = json.loads(phase_raw_path.read_text(encoding="utf-8"))
            except json.JSONDecodeError as exc:
                return WorkerResult(
                    status="failed",
                    exit_code=1,
                    metrics={},
                    message=f"invalid JSON from ingestion_e2e phase {phase}: {exc}",
                    raw_path=str(phase_raw_path),
                )
            phase_raw["artifact_identity"] = _artifact_identity(binary, phase_raw)
            write_json(phase_raw_path, phase_raw)
            lifecycle_runs.append(
                {
                    "phase": phase,
                    "exit_code": proc.returncode,
                    "raw_path": str(phase_raw_path),
                    "result": phase_raw,
                }
            )
    finally:
        if fixture_root is not None:
            shutil.rmtree(fixture_root, ignore_errors=True)

    stdout_path.write_text("\n".join(stdout_parts), encoding="utf-8", errors="replace")
    stderr_path.write_text("\n".join(stderr_parts), encoding="utf-8", errors="replace")
    if proc is None or not lifecycle_runs:
        return WorkerResult(status="failed", exit_code=1, metrics={}, message="no lifecycle runs")

    raw = dict(lifecycle_runs[-1]["result"])
    raw["lifecycle_runs"] = lifecycle_runs
    write_json(raw_path, raw)

    if not isinstance(raw, dict):
        return WorkerResult(
            status="failed",
            exit_code=1,
            metrics={},
            message="ingestion_e2e output root must be an object",
            raw_path=str(raw_path),
        )

    raw["artifact_identity"] = _artifact_identity(binary, raw)
    write_json(raw_path, raw)

    metrics = extract_metrics(raw)
    contract_errors: list[str] = []
    require_complete = _truthy(ctx.params.get("require_complete_pipeline", True))
    require_search = _truthy(ctx.params.get("require_full_searchability", True))
    require_oracle = _truthy(ctx.params.get("require_full_oracle", False))
    require_spq = _truthy(ctx.params.get("require_spq_persistence", False))
    minimum_vectors = int(ctx.params.get("minimum_vector_count") or 0)
    if require_complete:
        contract_errors.extend(
            validate_ingestion_contract(
                raw,
                require_full_searchability=require_search,
                require_full_oracle=require_oracle,
                require_spq_persistence=require_spq,
                minimum_vector_count=minimum_vectors,
            )
        )

    if len(lifecycle_runs) > 1:
        baseline_storage = lifecycle_runs[0]["result"].get("storage_oracle") or {}
        baseline_config = lifecycle_runs[0]["result"].get("test_config") or {}
        replay_fingerprints: set[tuple[str, str]] = set()
        for lifecycle_run in lifecycle_runs:
            phase = lifecycle_run["phase"]
            phase_raw = lifecycle_run["result"]
            if int(lifecycle_run["exit_code"]) != 0:
                contract_errors.append(
                    f"lifecycle phase {phase} exited {lifecycle_run['exit_code']}"
                )
            if require_complete:
                contract_errors.extend(
                    f"lifecycle phase {phase}: {error}"
                    for error in validate_ingestion_contract(
                        phase_raw,
                        require_full_searchability=require_search,
                        require_full_oracle=require_oracle,
                        require_spq_persistence=require_spq,
                        minimum_vector_count=minimum_vectors,
                    )
                )
            phase_config = phase_raw.get("test_config") or {}
            phase_storage = phase_raw.get("storage_oracle") or {}
            if phase_config.get("corpus_fingerprint") != baseline_config.get(
                "corpus_fingerprint"
            ):
                contract_errors.append(f"lifecycle phase {phase} changed corpus fingerprint")
            if phase_storage.get("embedding_fingerprint") != baseline_storage.get(
                "embedding_fingerprint"
            ):
                contract_errors.append(f"lifecycle phase {phase} changed embedding fingerprint")
            if phase_storage.get("vector_search_fingerprint") != baseline_storage.get(
                "vector_search_fingerprint"
            ):
                contract_errors.append(f"lifecycle phase {phase} changed vector search fingerprint")
            if phase == "replay":
                restart = phase_raw.get("restart_oracle") or {}
                if restart.get("required") is not True or restart.get("complete") is not True:
                    contract_errors.append("replay restart oracle incomplete")
                if restart.get("vector_reusable") is not True:
                    contract_errors.append("replay did not reuse persisted SPQ")
                if restart.get("embedding_fingerprint") != phase_storage.get(
                    "embedding_fingerprint"
                ):
                    contract_errors.append("replay startup/final embedding fingerprints differ")
                if restart.get("vector_search_fingerprint") != phase_storage.get(
                    "vector_search_fingerprint"
                ):
                    contract_errors.append("replay startup/final vector search fingerprints differ")
                result_pair = (
                    str(restart.get("search_result_fingerprint") or ""),
                    str(restart.get("search_score_fingerprint") or ""),
                )
                if result_pair != (
                    str(phase_storage.get("search_result_fingerprint") or ""),
                    str(phase_storage.get("search_score_fingerprint") or ""),
                ):
                    contract_errors.append("replay startup/final search fingerprints differ")
                replay_fingerprints.add(result_pair)
        if len(replay_fingerprints) > 1:
            contract_errors.append("repeated replay search fingerprints differ")

    identity = ingestion_experiment_identity(raw)
    identity_json = json.dumps(identity, sort_keys=True, separators=(",", ":"))
    identity_sha256 = hashlib.sha256(identity_json.encode("utf-8")).hexdigest()
    if _truthy(ctx.params.get("require_experiment_identity", True)):
        if not identity.get("corpus_fingerprint"):
            contract_errors.append("missing ingestion corpus fingerprint")
        identity_path = (
            ctx.arm.run_dir
            / "shared_state"
            / "ingestion_identity"
            / f"{ctx.arm.arm.safe_name}.json"
        )
        identity_error = require_ingestion_experiment_identity(identity_path, identity)
        if identity_error:
            contract_errors.append(identity_error)

    if _truthy(ctx.params.get("require_stable_retrieval_outcome", False)):
        storage = raw.get("storage_oracle") or {}
        outcome = {
            "corpus_fingerprint": identity.get("corpus_fingerprint"),
            "embedding_fingerprint": storage.get("embedding_fingerprint"),
            "vector_search_fingerprint": storage.get("vector_search_fingerprint"),
            "search_result_fingerprint": storage.get("search_result_fingerprint"),
        }
        outcome_path = (
            ctx.arm.run_dir
            / "shared_state"
            / "ingestion_outcome"
            / f"{ctx.arm.arm.safe_name}.json"
        )
        outcome_error = require_ingestion_experiment_identity(outcome_path, outcome)
        if outcome_error:
            contract_errors.append("retrieval outcome " + outcome_error)

    if proc.returncode != 0:
        contract_errors.insert(0, f"ingestion_e2e exited {proc.returncode}")
    # Compatibility copy for older ablation summary path names.
    legacy_copy = ctx.arm_dir / f"{ctx.arm.arm.safe_name}.json"
    try:
        shutil.copyfile(raw_path, legacy_copy)
    except OSError:
        pass

    valid = not contract_errors
    return WorkerResult(
        status="ok" if valid else "failed",
        exit_code=0 if valid else (proc.returncode or 1),
        metrics=metrics,
        attributes={
            "binary": str(binary),
            "corpus_size": corpus_size,
            "doc_size": doc_size,
            "ingest_mode": identity.get("ingest_mode"),
            "ingest_concurrency": identity.get("ingest_concurrency"),
            "requested_ingest_mode": ingest_mode,
            "requested_ingest_concurrency": max(1, ingest_concurrency),
            "experiment_identity": identity,
            "experiment_identity_sha256": identity_sha256,
            "contract_errors": contract_errors,
            "ablation": ablation,
            "semantic_skip": (_probe_map(raw).get("semantic") or {}).get("skip_reason"),
            "graph_skip": (_probe_map(raw).get("graph_rerank_hybrid") or {}).get(
                "skip_reason"
            ),
        },
        message=(
            "ingestion_e2e completed"
            if valid
            else "ingestion_e2e contract failed: " + "; ".join(contract_errors)
        ),
        raw_path=str(raw_path),
    )
