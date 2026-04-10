# Multi-Client Optimization Loop

This runbook captures the optimization cycle for bursty CLI/MCP/daemon workloads.

It executes:

1. phase 2 (throughput-first)
2. phase 1 (stability-first)

and prepares outputs for phase 3 (tuning sweeps) and phase 4 (regression gating).

## Prerequisites

- Build benchmark target:

```bash
meson compile -C build/debug bench_multi_client
```

- Optional but recommended: point to an existing corpus for read-path tests.

## Run

From repo root:

```bash
tests/benchmarks/run_multi_client_optimization_loop.sh \
  --bench-bin build/debug/tests/benchmarks/multi_client_ingestion_bench \
  --data-dir <corpus-dir>
```

By default, `--data-dir` is copied to an isolated temporary snapshot (`--data-dir-mode snapshot`)
before benchmark execution to avoid `.yams-lock` contention with an already-running daemon.

Use the source directory directly only when you explicitly want that behavior:

```bash
tests/benchmarks/run_multi_client_optimization_loop.sh \
  --bench-bin build/debug/tests/benchmarks/multi_client_ingestion_bench \
  --data-dir <corpus-dir> \
  --data-dir-mode direct
```

The run emits per-phase JSONL, per-phase summaries, a combined summary, a case manifest, and a run manifest.

The summary schema is versioned (`multi_client_summary_v2`) and now includes:

- compatibility KPIs used by legacy regression checks
- per-op (`search/list/grep/status/get/cat`) throughput/fail-rate/p95 aggregates
- grep server telemetry aggregates (`execution_time_ms`, `phase_worker_scan_ms`,
  `content_retrieval_ms`, scanned volume counters)
- case-level breakdown keyed by phase + transport + usage profile

If `--data-dir` is omitted, large-corpus runs are skipped and contention runs still execute.

## Summarize Existing JSONL

```bash
python3 tests/scripts/summarize_multi_client_jsonl.py \
  --input <phase2.jsonl> \
  --input <phase1.jsonl> \
  --output <combined-summary.json>
```

## Regression Check

Compare current vs baseline summaries:

```bash
python3 tests/scripts/check_multi_client_regression.py \
  <current-summary.json> \
  <baseline-summary.json> \
  --output <regression-report.json> \
  --fail-on-regression
```

`check_multi_client_regression.py` is backward compatible with older baseline summaries
while using the richer v2 metrics when present.

## KPI Targets

- `large_corpus_reads.*.ops_per_sec`: non-regressing or improving
- `large_corpus_reads.*.fail_rate`: non-regressing
- `large_corpus_reads.*.connection_drop_rate`: non-regressing
- `connection_contention.fail_rate`: non-regressing
- `connection_contention.retry_after_rate`: stable or decreasing

## Current contention policy notes

- Peak-load CLI probe failures should now be classified, not treated as opaque harness failures.
- `timeout_under_load` is the only accepted CLI failure mode during the saturation window.
- Post-stress CLI recovery must pass for `status` and `list` before a contention run is considered healthy.
- In the current reduced contention scenario, `cli_search` now succeeds instead of timing out.
- Fresh ASAN validation remains the conservative safety lane; current ASAN mixed-workload runs are clean through 48 total clients and first degrade at 56.

## Mixed stress interpretation

- The mixed-client benchmark is a **closed-loop** workload with fixed per-client op budgets.
- Aggregate ops/s can rise at higher client counts even while the system is under *more* stress.
- For scaling decisions, use the first appearance of **nonzero failures** and rising **p95/p99 latencies** as the primary boundary markers.
- In the fresh Debug validation on the tuned defaults, **64 total mixed clients** is the clean tier and **68+** is the first degraded tier.

## Phase1 Daemon Scheduling Defaults

For large-corpus mixed read stability, prefer `[tuning]` keys over ad-hoc env overrides and keep the model hardware-aware:

```toml
[tuning]
grep_inflight_limit = 1
grep_admission_wait_ms = 20000
```

Notes:

- Read-path worker capacity, CLI request pool size, search concurrency, and read DB pool cap are now derived centrally in `TuneAdvisor`; avoid hard-coding machine-specific thread counts in docs or tests.
- `grep_inflight_limit = 1` is the recommended phase1 default for lower variance and stable tails.
- If your corpus is grep-heavy and stable, test `grep_inflight_limit = 2` for higher peak throughput.
- Keep timeout knobs unchanged while validating scheduler changes.
