# Metadata Write Bottleneck Diagnosis (2026-04-30, in progress)

## TL;DR

Two distinct problems were uncovered, neither fixable by replacing SQLite:

1. **Bench-harness throttle** (fixed in this branch): `PostIngestQueue` constructed its `GradientLimiter`s with `maxLimit = 1` at startup with no runtime path to raise the ceiling, so the bench's post-ingest stages couldn't drain. Resolved via a runtime-mutable `setMaxLimit` driven by `TuningManager`.
2. **Production write-pool acquire timeouts** (open): Real daemon logs show 30s pool-acquire timeouts under heavy concurrent ingest. The `maxConnections=1` write pool is intentional (WAL one-writer rule + KG ordering invariants) — the actual issue is that a single write transaction is held for ≥30s while many callers queue. Need per-holder transaction duration instrumentation to identify the slow path.

Replacing SQLite would not help either problem.

| Path | Throughput | Source |
|---|---|---|
| RPC ingestion (metadata + content store) | ~794 docs/s (1000 docs in 1.26s) | `e2e_1k.json` |
| RPC ingestion (vectors disabled) | ~877 docs/s (500 docs in 570ms) | `e2e_500_novec.log:274` |
| Post-ingest pipeline drain | 0 docs/s observed | both runs |

`build/release` runs `database-backend = libsql`, not stock SQLite. Numbers reported here are libSQL.

## Setup

- Build: `/Users/trevon/work/tools/yams/build/release` (debugoptimized -O2, NDEBUG, libSQL backend).
- Bench: `tests/benchmarks/ingestion_e2e_bench` (release flavor).
- Two runs:
  - `e2e_1k.json` — corpus=1000, doc=1500B, vectors enabled, simeon-default.
  - `e2e_500_novec.json` + `.log` — corpus=500, doc=1500B, `YAMS_DISABLE_VECTORS=1`.
- Concurrent CPU contention: `simeon_bench_vs_reference_research` running on PID 48403; release-build, library-only, no SQLite.
- Artifacts: `.artifacts/bench_baseline/`.

## What the data says

### Writes are fast

`e2e_500_novec.log:274` — `Ingestion complete: 500 succeeded, 0 failed in 570 ms`. That is the wall time from first `streamingAddDocument` to the 500th return. Path includes RPC, content-store write, metadata insert, and FK-cascaded relationship updates. **At ~877 docs/s the write path is not the bottleneck.**

### Post-ingest never drains in this harness

Both runs hit the bench's 60s pipeline-completion timeout. `Pipeline status: post=0/500 (includes FTS5) embed=0/500` repeats every second across the 60s window. Even with vectors disabled (no embedding work to do) the post counter stays at 0.

### Why post-ingest stays at 0

`e2e_500_novec.log:122`:
```
[PostIngestQueue] Stage snapshot: active={extraction=true, kg=true, symbol=false, entity=false, title=false}
                                  paused={extraction=false, kg=false, symbol=false, entity=false, title=false}
                                  limits={extraction=1, kg=1, symbol=0, entity=0, title=0}
```

The post-ingest queue starts with **in-flight limit = 1** for extraction and KG, **0** for symbol/entity/title. WorkCoordinator has 12 threads available (`log:11`), queue capacity is 1000 (`log:138`). The throttle is TuneAdvisor / Gradient limiters, not the worker pool.

Two issues compound:
1. With limits this low, even nominal post-ingest progress would barely move the counters during the 60s window.
2. `YAMS_DISABLE_VECTORS=1` skips the embedding stage entirely, but the bench's completion predicate requires both `embedComplete` AND `postComplete` (`ingestion_e2e_bench.cpp:794`). Under disabled-vectors, `embedComplete` can never become true. **This is a bench-harness bug**: the bench is unsuited to vectors-disabled measurement.

### What this rules out (and what it does not)

- **Refuted (initial premise):** SQLite write contention is the bottleneck. Writes complete at ~800–900 docs/s; pool stats are not even being exercised.
- **Refuted:** A drop-in custom SQL engine in `third_party/` would help ingestion throughput. The data says the database is not what is slow.
- **Open:** Whether the post-ingest pipeline drains in production-realistic conditions (real daemon, real workload, no test-safe-single-instance harness). The bench's `extraction=1` initial limit may rise after warmup; the bench's 60s window may not capture that warmup. Need a longer-running probe outside the SimpleDaemonHarness.
- **Open:** Where the `store_document_tasks: 984` queue depth in `e2e_1k.json` actually comes from. It is constant across all 581 samples — looks like dead state, possibly seeded by something the harness did not isolate. Worth tracing but not a contention story.

## Implications for the original proposal

The exploration plan opened with three forward paths:
- **A — Measure first.** This is that.
- **B — Shard the write queue.** Not justified by these numbers; writes are not the slow path.
- **C — libSQL/DuckDB experiment.** Partially moot: `build/release` is already libSQL.

The actionable next steps are not in those branches. They are:

1. **Diagnose post-ingest TuneAdvisor configuration.** Why does extraction start at limit=1? Does it ramp up under load, or does it get stuck low? The answer is in `TuningManager.cpp` and `Gradient limiters` referenced at `log:123`.
2. **Fix the bench's vectors-disabled completion predicate.** When `YAMS_DISABLE_VECTORS=1`, the bench should not require `embedComplete`.
3. **Run a longer-window probe.** 60s is too short to observe warmup-then-drain behaviour. Either lengthen the bench timeout to 5–10 minutes, or instrument the post-ingest pipeline to emit per-stage progress at sub-second granularity.

Each of those is days of work, not months. None requires replacing SQLite.

## Root cause for the post-ingest stall

Tracing the `extraction=1, kg=1` snapshot from the bench log up the call chain:

1. **`PostIngestQueue::initializeGradientLimiters()`** (`src/daemon/components/PostIngestQueue.cpp:2441–2512`) is called exactly once, from `start()` at `PostIngestQueue.cpp:335`.
2. For each stage it computes `stageMax`:
   - extraction: `static_cast<double>(maxExtractionConcurrent())` → `TuneAdvisor::postExtractionConcurrent()`
   - kg/symbol/entity/title/embed: same routing into `TuneAdvisor::post*Concurrent()`
3. All those eventually call **`postIngestBudgetedConcurrency(includeDynamicCaps=true)`** (`include/yams/daemon/components/TuneAdvisor.h:2350+`).
4. With activeStages=2 (extraction + kg) and `totalBudget=2` from `postIngestTotalConcurrent()` startup compute, the allocator (`TuneAdvisor.h:2380–2399`) returns `extraction=1, kg=1`.
5. Each stage's limiter is then constructed with `cfg.maxLimit = std::min(globalMax, stageMax)` (`PostIngestQueue.cpp:2490`) → **maxLimit = 1**.
6. `cfg.initialLimit > cfg.maxLimit ⇒ initialLimit = maxLimit` (`PostIngestQueue.cpp:2496–2498`) → **initialLimit = 1**.
7. **`GradientLimiter::config_` is `const` after construction.** No setter, no `setConfig()`, no `reconfigure()`. `updateLimit()` clamps every adjustment to `[minLimit, maxLimit]` = `[1, 1]`.

So the gradient limiter for extraction is permanently pinned at `limit = 1`, regardless of how throughput actually behaves. The same goes for kg.

Even if `TuneAdvisor::postExtractionConcurrent()` later returns 8 because the budget allocator changed (it does query dynamic caps), `PostIngestQueue` only reads that value once, at queue startup. The gradient limiter's `maxLimit` cannot grow.

This is the bug. **It is not SQL-related at all.** It explains slow ingestion that the user perceived as "write contention," because:
- writes succeed instantly (the database isn't backed up)
- the post-ingest pipeline drains at single-doc-in-flight × per-stage RTT
- the result looks like ingestion is "stuck" even though the daemon is mostly idle

### Concrete fixes (none touches the database)

1. **Make `GradientLimiter::maxLimit` mutable.** Add a setter, called periodically by `TuningManager` (it already runs a tick loop) with the current `postIngestBudgetedConcurrency()` value.
2. **Or recreate the limiters on stage-cap change.** `initializeGradientLimiters()` already supports replacement via `limiters_[i] = std::move(...)` and `limiterPtrs_[i].store(...)` (`PostIngestQueue.cpp:2503–2509`). Call it from a TuningManager hook when the post-ingest budget changes by more than X%.
3. **Lift the startup-time `totalBudget`.** If `postIngestTotalConcurrent()` is computing 2 on a 12-core machine because the activeStages mask is 2 at startup, that's pessimistic — a sensible floor would be `max(2, hw/4)` or so. Trace `postIngestTotalConcurrent()` in `TuneAdvisor.h:1871+` and confirm it does what the comment claims.

### Fix landed (Option 1)

Implemented in this branch:

- `include/yams/daemon/components/GradientLimiter.h` — added `setMaxLimit(double)` and `currentMaxLimit()` accessors backed by `std::atomic<double> dynamicMaxLimit_`.
- `src/daemon/components/GradientLimiter.cpp` — replaced the four `config_.maxLimit` reads in `updateLimit` and `applyPressure` with the atomic; raising the cap also bumps `limit_` and clears RTT memory so the algorithm re-explores; lowering the cap clamps `limit_` down.
- `src/daemon/components/TuningManager.cpp` — added a per-tick refresh inside the existing `enableGradientLimiters()` block that pushes `TuneAdvisor::postIngestBudgetAll(true)` values into each PostIngestQueue limiter via `setMaxLimit`.
- Tests at `tests/unit/daemon/components/gradient_limiter_catch2_test.cpp` cover ceiling-raise, ceiling-lower, minLimit floor, and growth unblocking. Suite passes 178 assertions / 32 cases.

## Production evidence (post-fix verification surfaced this)

After landing the gradient-limiter fix, real production daemon logs show repeated:

```
[error] MetadataRepository::executeQueryOnPool route='write' acquire error: Timeout acquiring connection
```

These appear in batches of ~4 at exact 30-second intervals during heavy concurrent activity (4 simultaneous `IndexingService::addDirectory` calls — 35 / 128 / 2399 / 24780 candidate files — overlapping with simeon-lexical search index build, RepairService startup, and SearchEngine build). The 30s spacing matches `withConnection`'s default `acquire(30000ms)` timeout (`include/yams/metadata/connection_pool.h:131`).

### `maxConnections=1` is load-bearing, not a bug

It's hardcoded at `src/daemon/components/DatabaseManager.cpp:243` because:
- SQLite WAL mode permits **one writer at a time** anyway. Multiple write connections would just push contention from the pool layer (timeout-error) down to the engine layer (`SQLITE_BUSY`), which has worse failure modes for KG transactions.
- KG operations have ordering invariants (node insert before edge resolution) that rely on a single serializing writer.

### The real diagnosis

Pool times out only if **one transaction holds the writer for ≥30 seconds**. Pool size is irrelevant; transaction duration is the metric.

Currently the diagnosis cannot identify *which* transaction. The bench output and the `Statement::execute` retry counter only see SQLite-internal contention, not "this app code held the connection a long time."

### Pivot for Step 3 instrumentation

What we actually need:
- **Per-call-path tx wall time** in `ConnectionPool::withConnection` and direct `acquire()` callers — record holder identity (function name / call site) plus elapsed time on release.
- **Slow-write log threshold**: emit a warning when any caller holds the write connection for > 5s, with the call site.
- **Histogram of holder durations** (1ms, 10ms, 100ms, 1s, 10s, 30s buckets) exported via DaemonMetrics.

That replaces the `dbWritePoolTimeoutCount` reading from earlier Step 2 — pool timeouts are the *symptom*, holder duration is the *cause*.

### Likely culprits to inspect

- Long `addDirectory` transactions that batch many file inserts under one writer hold.
- `RepairService` startup transactions.
- Simeon-lexical persistence/checkpoint writes during the 18s build window.
- Any SearchComponent → Metadata write during initial corpus build (the user log shows 41253 docs in search index, 38383 in BM25 — that's a real corpus, not bench).

## Open questions before this report ships

- Does the post-ingest pipeline drain in normal operation (real daemon, not bench harness)? Drive a small ingestion via the production CLI and watch `yams stats`.
- Is the `store_document_tasks: 984` constant queue depth a real backlog or a stale counter? Trace the source.
- What does TuneAdvisor's `Gradient limiters` config look like in this build, and what does it ramp to over 5 minutes?

These are the questions to answer next, not "should we replace SQLite."

## Appendix: artifacts

- `/Users/trevon/work/tools/yams/.artifacts/bench_baseline/e2e_1k.json` — corpus=1000, vectors=on
- `/Users/trevon/work/tools/yams/.artifacts/bench_baseline/e2e_500_novec.json` — corpus=500, vectors=off
- `/Users/trevon/work/tools/yams/.artifacts/bench_baseline/e2e_500_novec.log` — full bench log

Bench source: `src/search/benchmarks/ingestion_e2e_bench.cpp`.
Pipeline-completion logic: `ingestion_e2e_bench.cpp:786–794`.
Post-ingest stage snapshot emitted by: `src/daemon/components/PostIngestQueue` startup.
