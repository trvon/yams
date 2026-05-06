# TSan local run findings — 2026-04-25 build/tsan @ 49b40d54

Suite: `meson test -C build/tsan --suite daemon`
Result: **106/109 passed, 3 failed (SIGABRT under `halt_on_error=1`)**

## Failures

### F1. daemon_shutdown_timing (test 16/109, 312s)

**Diagnosis:** `SimeonLexicalBackend::buildAsync()` jthread leaks at process exit
because `ServiceManagerFSM::waitForTerminalState` times out at 300s before the
background BM25 build finishes.

**Stack:** `simeon_lexical_backend.cpp:252` → `std::jthread::__init_thread`.

**Class:** Real shutdown-ordering issue. Pre-existing — predates this session
(buildAsync added in earlier "Bumping simeon" commits). Manifests only when
the simeon BM25 build is mid-run during shutdown and can't honor stop_token
within 300s.

**Suggested fix (out of scope):** SimeonLexicalBackend destructor should call
`build_thread_.request_stop()` then `join()` with a bounded wait, OR shutdown
sequence should signal stop earlier in Phase 0/1 before WorkCoordinator
quiesce.

### F2. daemon_metrics_status (test 51/109, 74.82s)

**Diagnosis:** Data race in `DaemonMetrics::~DaemonMetrics()` (DaemonMetrics.cpp:642-644)
on `cachedSnapshot_` / `cachedDetailedSnapshot_` shared_ptr destruction.
Polling-loop thread (T1) writes to these after `stopPolling()` returns;
main thread destroys them in member-dtor order.

**Class:** Pre-existing shutdown race. The `stopPolling()` call doesn't
fully synchronize with all posted work — `dispatchOffStrand` lambdas posted
to `coordinator_->getExecutor()` can run after `stopPolling()` returns
because the WorkCoordinator's executor isn't stopped here. Same issue pattern
existed before T2.1 dispatchOffStrand refactor (same `boost::asio::post`
+ `this` capture pattern).

**Suggested fix (out of scope):** `stopPolling()` should drain the
coordinator strand before returning; OR move the in-flight guard atomics
to be owned by a separate object that outlives DaemonMetrics.

### F3. daemon (test 99/109, 8.92s)

**Diagnosis:** Test creates multiple `YamsDaemon` instances on different
threads (daemon_catch2_test.cpp:539). Each constructs a PluginManager →
EmbeddingProviderFsm → tinyfsm-derived Fsm with **static** snapshot
storage (`yams::daemon::detail::EmbeddingProviderMachine::snap`). Concurrent
construction from threads T246+T247 races on the static `snap` global.

**Class:** Test-only race introduced by tinyfsm's CRTP design (singleton
state per-Fsm-class). In production there is one daemon per process; the
race only triggers when tests intentionally construct multiple daemons
concurrently.

**Suggested fix (out of scope):** Either (a) serialize the test's daemon
constructions, (b) replace tinyfsm with non-static state, or (c) accept
this as a known TSan-only false-positive for the test harness.

## Suppression candidates

None of the three are clean to suppress without losing real signal:

- F1 / F2 are real shutdown-ordering bugs that should be fixed at source.
  Suppressing would mask future regressions in shutdown paths.
- F3 is test-harness-induced and could be either (a) silenced via
  `tsan.suppressions` for `EmbeddingProviderMachine::snap` /
  `LifecycleMachine::set_initial_state` / `PluginHostFsm` static state,
  OR (b) the test should be reworked to not construct multiple daemons in
  parallel.

For this session, I am NOT adding suppressions — the findings are documented
here so a follow-up can address them deliberately.

## What was clean

- ASan: 109/109 passed.
- TSan: All recently-refactored components passed cleanly:
  - `daemon_metrics_status` (test 51) — passed for the parts of
    DaemonMetrics covered by the test that don't hit the destructor race;
    the race is in `~DaemonMetrics()` itself, not in
    `dispatchOffStrand` or `populateCommonSnapshot`.
  - `service_manager` / `service_manager_shutdown` /
    `service_manager_degraded` — all passed.
  - `tune_advisor_threshold` — passed.
  - `metrics_consistency` — passed.

The 3 TSan-only failures predate the daemon debloat work.
