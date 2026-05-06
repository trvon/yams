# Daemon runaway — architectural trace

- **Date**: 2026-04-18
- **Commit**: `6eb16351` + working-tree (post-Tracks-ABC) with the ns-clock harness edit in `tests/benchmarks/benchmark_base.h`
- **Trigger**: live snapshot taken during the Apr-17 benchmark rerun

The numbers that prompted this trace, observed on the developer's workstation while running `multi_client_ingestion_bench`:

| Process | PID | RSS | Uptime | Notes |
| --- | ---:| ---:| ---:| --- |
| `yams-daemon --socket /tmp/yams-daemon.sock ...` | 94980 | **27 GB** | 1h 36m | Single instance, normal invocation |
| `yams serve` (homebrew) | 24283 | — | ~2d 12h | MCP server, idle |
| `yams serve` (homebrew) | 26292 | — | ~2d 0h | MCP server, idle |
| `yams add ...` | 5421 | ~858 MB | — | Mid-ingest, external Flutter project |
| `yams add ...` | 75037 | ~858 MB | — | Mid-ingest, same task |
| `yams add ...` | 96500 | ~858 MB | — | Mid-ingest, same task |

None of this is a stress test. Normal usage left the daemon at 27 GB and several CLI processes parked indefinitely. Two multi-client bench cases (`[mixed-16]`, `[ux-scaling]`) regressed from 5/5+3/3 clean to failing while the bench competed with this ambient state — symptom, not cause. **The fact that this accumulation is reachable via normal usage is the architectural finding.**

This doc groups the trace into four areas (A–D) and ranks findings within each by severity / confidence. Evidence anchors are verified against HEAD. Raw Phase-1 evidence is in the Appendix.

## Systemic patterns

The audit clusters the 15 findings into three repeating patterns plus three one-offs. Each pattern has a shared fix template, which changes remediation from "12 independent bugs" to "three coordinated passes."

| Pattern | Findings | Shape | Fix template |
|---|---|---|---|
| 1. Exception-unsafe cleanup | C1, C2 | `insert(map)` → process → `erase(map)`; erase only runs on happy path | RAII scope-guard wrapping the erase so any exception / early-return path still cleans up |
| 2. Warn-but-don't-enforce | A3, C3, C4 | Soft cap checked, logged, ignored — work continues past the limit | Per-site admission decision (block with timeout, drop, or spill) |
| 3. Declared-but-unwired | A1, A2, A5✓, A6✓, A7, C5✓, D2✓ | Config flag / method / state exists; no code actually reads it | Find the call site that would make the mechanism effective (1–3 LOC each). ✓ = fixed 2026-04-18 |
| Drift (one-offs) | A4, D3, D4 | Genuinely missing mechanism or one-place inconsistency | Individual patches |

Pattern 3 is the largest and most strictly mechanical — most instances are single-line wires. Pattern 1 is the smallest but highest-confidence contributor to the 27 GB observation. Pattern 2 requires per-site semantic decisions (block vs drop vs spill).

---

## A. Daemon self-preservation is missing

The daemon has no mechanism to exit on its own. Every pressure path degrades behavior, pauses subsystems, or rejects new work — none of them tear down the process.

| # | Finding | Anchor | State | Fix sketch |
|---|---|---|---|---|
| A1 | `--max-memory` CLI flag and `DaemonConfig.maxMemoryGb` are parsed but never enforced. Nothing in the daemon compares live RSS against the cap. | `src/daemon/daemon_main.cpp:192` (parser), `DaemonConfig.maxMemoryGb` referenced nowhere in the runtime loop | **Present-but-unwired** (config reaches the struct; no reader) | Wire `ResourceGovernor` to sample RSS, compare to `maxMemoryGb`, call `daemon.requestStop()` when breached for N consecutive samples |
| A2 | `ResourceGovernor` escalates Normal→Warning→Critical→Emergency and pauses non-essential stages (KG/Symbol/Entity/Title, embedding model release), but Emergency never calls `requestStop()`. The daemon sits in Emergency indefinitely. | `src/daemon/components/ResourceGovernor.cpp` — admission logic near `:685–699` (CPU throttle returns `false`), DB-contention reporting `:701–727`. No call to `requestStop()` anywhere in this file. | **Missing** (Emergency → exit path does not exist) | Add "terminal pressure" state: if Emergency persists > T seconds and queues are empty, call `requestStop()` |
| A3 | `PostIngestQueue` capacity is advisory. `tryEnqueue` honours it; `enqueue` pushes unconditionally. | `include/yams/daemon/components/PostIngestQueue.h:144–198` | **Present-but-unwired** (cap exists; not enforced on the unconditional path) | Remove `enqueue` or make it block-with-timeout; force all producers through `tryEnqueue` |
| A4 | No watchdog / uptime bound on the main loop. `runLoop()` exits only on `stopRequested_` — nothing trips that flag from resource conditions. | `src/daemon/daemon_main.cpp:~1212` (main `runLoop()` call) | **Missing** | Periodic self-health tick inside `runLoop()` that checks RSS / queue backlog / uptime and may trip `stopRequested_` |
| A5 | `DaemonConfig::maxPendingRepairs = 1000` was declared but unread. Repair queue size was unbounded in practice. | `include/yams/daemon/daemon.h:62` → `RepairServiceHost::Config` → `RepairService::Config` → admission check at enqueue sites | **Fixed 2026-04-18**: admission cap enforced in `RepairService::onDocumentAdded` and `RepairService::enqueueEmbeddingRepair`; plumbed via `RepairServiceHost::Config::maxPendingRepairs` from `ServiceManager::startRepairService` | (done) |
| A6 | `DaemonConfig::downloadPolicy.maxFileBytes` parsed from TOML but was never applied. | `daemon.h:92` parsed at `src/daemon/daemon_main.cpp:899`; plumbed via new `app::services::DownloadServiceOptions` overload of `makeDownloadService` | **Fixed 2026-04-18**: dispatcher constructs `DownloadServiceImpl` with `opts.maxFileBytes = policy.maxFileBytes`; `downloader::DownloaderConfig.maxFileBytes` now honors the daemon policy (already enforced by `download_manager.cpp:409, 547`) | (done) |
| A7 | `DaemonConfig::downloadPolicy.rateLimitRps` parsed but never enforced. | Declared `daemon.h:93`; parsed `src/daemon/daemon_main.cpp:910`. `downloader::RateLimit` is bytes-per-second, not requests-per-second — no natural plumb target exists. | **Present-but-unwired** — deferred | Requires a separate dispatcher-level token-bucket component. Not a one-line wire; left for a follow-up design pass |

**Net**: a normal ingestion session that hits any of the memory-growth paths in §C will grow the daemon to the OOM-killer limit. The watchdog story is "the user notices and `kill -9`."

**Audit note**: `autoRepairBatchSize` (`ServiceManager.cpp:3299`) and `autoRebuildOnDimMismatch` (`RepairService.cpp:1009`) are confirmed wired. The A1/A5/A6/A7 cluster is selective drift, not a universal issue. `maxLogFiles` / `maxLogSizeMb` / `healthMonitoring` weren't flagged — likely flow through to spdlog config or a path grep missed; worth a second-pass look but not resource-critical.

---

## B. Long-lived CLI shapes — acknowledged, out of scope

Both long-lived CLI patterns in the live snapshot are **intentional**, not bugs:

- `yams serve` is an MCP stdio server. Blocking on stdin indefinitely is the MCP contract — clients (Claude IDE, etc.) own the session lifetime. Two survivors at 2d uptime is normal. `src/cli/commands/serve_command.cpp:~160–199`.
- `yams add` batch processes at 858 MB each are actively ingesting an external Flutter project. They're not zombies; they're live CLIs awaiting daemon-side batch completion. Legitimate work in progress. `src/cli/commands/add_command.cpp:343–359, 674, 748`.

Noted here for completeness. No action planned from the §B surface area.

---

## C. Ranked memory-growth candidates (daemon side)

All five are unbounded-in-practice even when there's a soft cap or TTL. Candidate #1 is the best match for the 27 GB / 1h36m growth rate.

| # | Candidate | Anchor | State | Confidence | Fix sketch |
|---|---|---|---|---|---|
| C1 | **`PostIngestQueue::activeJobs_` exception leak.** Both posted lambdas in the poller (batch path + single-item path) invoked the user process function with no try-catch; if it threw, the subsequent `cfg.completeJobFn(...)` and `inFlightCounter->fetch_sub(...)` were skipped, leaking the job in `activeJobs_` and the inflight count. | Batch lambda `include/yams/daemon/pressure_limited_poller.h:259–275`; single-item lambda `:297–306`; map declared `include/yams/daemon/components/PostIngestQueue.h:534`; complete/acquire at `src/daemon/components/PostIngestQueue.cpp:2474–2507` | **Fixed 2026-04-18**: try/catch added in both lambdas, `completeJobFn(..., false)` on throw, inflight subtract now always runs | HIGH — known-cap-missing + exception-safety gap | (done) |
| C2 | ~~`EmbeddingService::activeInferSubBatches_` no TTL.~~ **Re-verified: already protected.** An `InferFinalizeGuard` RAII struct at `EmbeddingService.cpp:2026–2033` wraps `inferFinalize("exit")` and fires on any non-success exit (exception, `continue`, early return). The guard is only disarmed after the explicit `inferFinalize("done")` call on the success branch. Map is correctly cleaned up. Phase-1 missed the guard. | — | **Working as intended** | — | — |
| C3 | **`KGWriteQueue::pendingBatches_` soft cap only.** Over-capacity warns but keeps pushing; batches carry full node / edge / doc-entity vectors. | `src/daemon/components/KGWriteQueue.cpp:32–52` (enqueue path), `:102–157` (writer loop) | **Present-but-unwired** (cap checked, not enforced) | MED | Block with timeout when full, or spill to disk |
| C4 | **`EmbeddingService::pendingJobs_` channel-poller accumulation.** Local-scope vector that holds `EmbedPreparedDoc` (chunk text) until dispatch — if dispatch lags, vector grows per poll iteration. | `src/daemon/components/EmbeddingService.cpp:634–688` (push), clear on dispatch | **Present-but-unwired** (no per-job cap) | MED | Cap total pending bytes; drop-or-block past cap |
| C5 | ~~`RetrievalSessionManager::sessions_` cleanup not scheduled.~~ | `src/daemon/ipc/retrieval_session.cpp:50–60` called from `ServiceManager::co_runSessionWatcher` per periodic tick (~2 s) with `ttl=60s` | **Fixed 2026-04-18** | LOW-MED | (done) |

Confirming which of these hold the actual 27 GB would require a heap profile against the live daemon (Instruments `malloc_history` / `leaks`, or `valgrind --tool=massif` on Linux). Not run here — the daemon has live `yams add` clients attached.

---

## D. Bench isolation is strong at socket/data-dir, weak elsewhere

`DaemonHarness` correctly randomises the socket path and data dir, so the `[mixed-16]` / `[ux-scaling]` regression is **not** "bench talked to the 27 GB daemon." But there are real isolation gaps that compound resource pressure.

| # | Finding | Anchor | State | Fix sketch |
|---|---|---|---|---|
| D1 | `DaemonHarness` randomises socket path (`/tmp/daemon_<id>.sock`) and data dir; all bench clients explicitly set `ccfg.socketPath = harness.socketPath()`. | `tests/integration/daemon/test_daemon_harness.h:69–100` (constructor), `:137` (socket assignment); bench uses at `tests/benchmarks/multi_client_ingestion_bench.cpp:2765, 3284, 3656` | **Working as intended** | — |
| D2 | ~~`isolateState = false` by default.~~ `XDG_STATE_HOME` leaked from bench to user env. | `tests/benchmarks/multi_client_ingestion_bench.cpp` — both `benchHarnessOptions()` overloads now set `.isolateState = true` | **Fixed 2026-04-18** | (done) |
| D3 | No `setrlimit` / `ulimit` management in any bench. macOS default `RLIMIT_NOFILE` is tight; the 27 GB peer daemon holds many FDs, and the bench process inherits the same per-user limit. Thread-creation pressure on macOS is a known failure mode under resource contention. | No call sites found in `tests/benchmarks/` | **Missing** | Call `setrlimit(RLIMIT_NOFILE, ...)` at bench startup; warn-and-skip if < N |
| D4 | Startup retry is asymmetric between cases. `[ux-scaling]` uses `startHarnessWithRetry` (1–3 attempts, 500 ms backoff); `[mixed-16]` uses bare `harness.start(kStartTimeout)`. First-attempt startup is the most contention-sensitive step. | `tests/benchmarks/multi_client_ingestion_bench.cpp:2762` (ux-scaling, retry), `:3627` (mixed-16, no retry); retry helper at `:2140–2149` | **Present-but-inconsistent** | Route both through `startHarnessWithRetry` |
| D5 | The observed failures are consistent with resource contention, not wrong-daemon routing. `[mixed-16]` → `getOps=0`, `failRate=0.234` (clients couldn't execute); `[ux-scaling]` → `controlHeldConnections.open()` refused (harness daemon likely didn't reach Ready or thread-create failed). | `tests/benchmarks/multi_client_ingestion_bench.cpp:3276` (control.open REQUIRE), `:4012–4014` (getOps / failRate CHECKs) | (Symptom, not fix target) | Ambient-daemon detection at bench startup: warn if a `yams-daemon` peer is holding > X GB RSS |

---

## Recommended sequencing

Scope is A, C, D (§B acknowledged as intentional).

1. **C1** — lowest-risk, highest-value: exception-safe the `PostIngestQueue` batch dispatch. One try/catch around `batchProcessFn` + `completeJobFn(..., false)` on throw.
2. **A1 + A2** — wire `maxMemoryGb` → `ResourceGovernor::Emergency` → `requestStop()`. Turns the Emergency state from a silent pause into a bounded shutdown; makes the `--max-memory` flag real.
3. **A3 + A4** — hard-enforce `PostIngestQueue` capacity (remove the unconditional `enqueue` or make it blocking) and add a periodic self-health tick in `runLoop()` so the daemon can trip its own `stopRequested_`.
4. **D2 + D4** — flip `isolateState=true` for bench harnesses and route both `[mixed-16]` and `[ux-scaling]` through `startHarnessWithRetry`. Re-run the two failing cases afterwards.
5. **C2, C3, C4, C5** — follow-up pass. Individual magnitudes are smaller but all share the "bounded-in-theory, unbounded-in-practice" pattern.
6. **D3** — `setrlimit(RLIMIT_NOFILE)` + warn-if-peer-daemon-present at bench startup. Hardening, not required for correctness.

Validation after (1)–(3) should run the daemon against the same external Flutter ingestion workload and confirm RSS plateaus instead of growing linearly with session length. A heap-profile run (Instruments / massif) is the only way to directly confirm which §C candidate is dominant today; optional follow-up.

---

## Appendix A — Raw Phase-1 evidence (daemon lifecycle)

Finding A details beyond the table:

- `src/daemon/daemon_main.cpp:58–103` — `g_shutdown_requested` atomic routes SIGTERM/SIGINT through `daemon.requestStop()` at lines 88–91. Graceful-shutdown path exists for signals; **not** wired to resource conditions.
- `src/daemon/daemon_main.cpp:192–193` — `--max-memory` parses into `config.maxMemoryGb` with default 8. The config field propagates into `DaemonConfig` but is never read by `ResourceGovernor`, `ServiceManager`, or `runLoop()` at any site grep can find.
- `src/daemon/components/ResourceGovernor.cpp` — has `admitIngest()` returning false under CPU pressure (line ~685–699), `reportDbLockContention()` logging severity (line ~701–727), and `capKgConcurrencyForDbContention()` capping KG workers (line ~730+). No `requestStop()` invocation anywhere in the file.
- `include/yams/daemon/components/PostIngestQueue.h:144–198` — capacity member + `tryEnqueue` vs `enqueue` split.
- `src/daemon/components/LifecycleComponent.cpp:117–150, 356–380` — the PID-file single-instance lock for `yams-daemon` IS enforced. It protects against two `yams-daemon` instances but is irrelevant to `yams serve`.

## Appendix B — Raw Phase-1 evidence (CLI exit discipline, reference only)

Kept for the record; §B is out of scope per user scoping:

- `src/cli/commands/serve_command.cpp:44–47` — entry; `:160–186` signal handlers set `g_running=false`; `:193` `return runStdioServer()`. Blocking on stdin is the MCP contract.
- `src/cli/commands/add_command.cpp:343–359` — per-RPC `--daemon-timeout-ms` (5 s); `:674, :748` — batch `addBatchAsync()` awaits. Long-running `yams add` are legitimate mid-ingest work.
- `src/daemon/ipc/daemon_client.cpp:107–109` — AddDocument categorised "Fast" because daemon queues-and-returns; queued work is unbounded in time from the client's perspective (this is the property that, combined with §A's lack of backpressure-exit, lets the daemon grow unbounded — not a CLI bug).
- `src/daemon/ipc/daemon_client.cpp:522–556` + `src/daemon/ipc/socket_utils.cpp:40–144` — socket-discovery fallback chain. Benches avoid it (explicit `socketPath`); noted so future client code doesn't drift into the fallback and hit the user's real daemon.

## Appendix C — Raw Phase-1 evidence (memory-growth candidates)

Map ownerships & lifecycles:

- `activeJobs_` — `include/yams/daemon/components/PostIngestQueue.h:534`. Lifetime = from `tryAcquireLimiterSlot` until `completeJob(hash, success)`. Leak when `batchProcessFn` throws in the posted lambda at `include/yams/daemon/pressure_limited_poller.h:259–275`.
- `activeInferSubBatches_` — `include/yams/daemon/components/EmbeddingService.h:144`. Keyed by `inferToken` (`uint64_t`). Insert `src/daemon/components/EmbeddingService.cpp:1993`, erase `:2013`. No error-path erase.
- `pendingBatches_` — `include/yams/daemon/components/KGWriteQueue.h:178`. Vector of `DeferredKGBatch`. Enqueue check at `src/daemon/components/KGWriteQueue.cpp:32–52` warns only.
- `pendingJobs_` — `include/yams/daemon/components/EmbeddingService.h:146`. Local to `channelPoller()` at `src/daemon/components/EmbeddingService.cpp:634–688`.
- `sessions_` — `include/yams/daemon/ipc/retrieval_session.h:38`. TTL sweep at `src/daemon/ipc/retrieval_session.cpp:50–60` (ttl default 60 s); not called from any periodic tick visible in the service manager.

## Appendix D — Raw Phase-1 evidence (bench isolation)

- Harness constructor: `tests/integration/daemon/test_daemon_harness.h:69–100`. Randomises `/tmp/daemon_<id>.sock` and `root_/data`.
- `isolateState` default: `tests/integration/daemon/test_daemon_harness.h:46` (`bool isolateState = false;`). Override branch: `:120–126`. Not set by benches.
- Bench options: `tests/benchmarks/multi_client_ingestion_bench.cpp:2114–2120` (`benchHarnessOptions()`).
- Explicit client socket bindings (sanity check that benches don't use fallback discovery): `multi_client_ingestion_bench.cpp:2765, 2838, 3284, 3656, 3684–3688`.
- Failing assertions: `[ux-scaling]` control.open at `:3276`; `[mixed-16]` `CHECK(getOps.load() > 0)` and `CHECK(failRate <= cfg.mixedMaxFailRate)` at `:4012, :4014`.
- Retry helper: `:2140–2149` (`startHarnessWithRetry`, 1–3 attempts, 500 ms backoff). Used by `:2762` but not `:3627`.

## Appendix E — Out of scope for this pass

- Heap profiling the live PID 94980 daemon (the user has `yams add` jobs attached; disruptive).
- Re-running `multi_client_ingestion_bench`. Failure cause is understood (Finding D5 + ambient peer) and is not the target of this trace.
- Process cleanup. Not touching PID 94980, the `yams serve` pair, or the three `yams add` processes without explicit approval — the `yams add` work appears to be live.
- Baseline JSON / `performance_report.md` / `rerun_post_tracks_abc.md` updates. Those wait on whichever of the §C / §A fixes land.
