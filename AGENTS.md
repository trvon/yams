---
description: YAMS repo supplement — conventions, structure, and repo-scoped rules
argument-hint: [TASK=<description>] [MODE=<engineering|bug-bounty>] [PHASE=<start|checkpoint|complete>]
---

# AGENTS.md (Repo Supplement)

Operating model (retrieval contract, execution loop, metadata, handoff,
ask-first list): `docs/prompts/PROMPT-eng-codex.md`. C++ design/TDD/assertion/
profiling flow: `docs/prompts/PROMPT-yams-cpp-workflow.md`. This file holds
only what is specific to this repository.

## Repo Intent

Engineering on YAMS itself, plus scoped non-destructive bug-bounty research.
Dogfood YAMS as the distributed memory across sessions, agents, and handoffs —
`yams grep`/`search`/`graph` before local discovery, always (see the retrieval
contract in the prompt; the graph here is rich: function-level nodes with
`calls`/`defined_in` edges, so "who uses X" is one `yams graph --explore`).

## CLI Philosophy

- Clean default behavior over feature flags and one-off env vars.
- Add an env knob only when it materially improves usability/debugging/perf.
- Auto-select behavior when safe rather than exposing another switch.
- Remove temporary diagnostics once product behavior is clear.

## Tuning Surfaces

- Tuning lives in typed config, not env vars: new search/topology/daemon knobs
  go on the relevant config struct (`SearchEngineConfig`, `TopologyConfig`, …)
  with a TOML key resolved through `ConfigResolver`
  (`include/yams/daemon/components/ConfigResolver.h`).
- Benches/tests/callers set the typed field directly or write a per-test
  `config.toml` (`DaemonHarnessOptions::configPath`) — never a fresh `YAMS_*`
  env var. Existing `YAMS_SEARCH_TOPOLOGY_*` / `YAMS_EMBED_*` overlays are
  test-only; keep them working, do not grow them.
- Reaching for `std::getenv` for a tunable is the warning sign.

## Repo Structure (Start Here)

- CLI entry points: `src/cli/` · services: `src/app/services/`
- Search (grep/semantic): `src/search/` · metadata/indexing: `src/metadata/`
- Storage engines: `src/storage/` · vector DB/embeddings: `src/vector/`
- Daemon client/server/components: `src/daemon/` · MCP: `src/mcp/`
- Tests: `tests/` · vendored libs: `third_party/` (own git repos)

## Conventions (Condensed)

- Formatting is mechanical: `.clang-format` (LLVM base, 4-space, 100-col).
- Naming: functions/vars `camelCase`; types `PascalCase`; constants
  `kPascalCase`; members trailing `_`; files `snake_case`.
- `Result<T>` for fallible operations, explicit propagation.
- Prefer `YAMS_HAS_*` gates from `include/yams/core/cpp23_features.hpp`.

## Engineering Quality Loop (C++)

1. Design the seam first: observable behavior, boundary, smallest testable seam.
2. Red before green: failing Catch2 test through public/repo-stable APIs;
   characterize existing behavior before refactors.
3. Assertions: recoverable failures stay in `Result<T>`; `YAMS_ASSERT`/
   `YAMS_PRECONDITION`/`YAMS_POSTCONDITION` for always-on invariants;
   `YAMS_DCHECK` for debug-only consistency; expressions side-effect free.
4. Refactor under green tests; no broad rewrites without seam/test evidence.
5. Measure before optimizing: workload + KPI + baseline, change one thing,
   re-measure. `YAMS_*` profiling macros only with a clear question.

## Testing Conventions

- Catch2 v3; files end `_catch2_test.cpp`; live in `tests/unit/<subsystem>/`;
  registered in `tests/meson.build`; targets named without `_exe`.
- Compile: `meson compile -C build/debug -j4 <target>` — never run parallel
  meson compiles (build-dir lock contention).
- Run: `build/debug/tests/<target>` or `meson test -C build/debug <name>`.
- Enable tracked hooks with `git config core.hooksPath .githooks`. The default
  pre-push hook runs the blocking Linux+macOS CI gate, not coverage.
  Run coverage explicitly with `YAMS_PREPUSH_COVERAGE=1 git push`; it dispatches
  `scripts/run-local-coverage.sh`, builds `build/coverage`, prefers ccache and
  a fast linker override when available, runs the unit-suite fast path by
  default, and prints a `gcovr` text summary for pushed C/C++ / Meson changes.
  Opt into integration coverage with `YAMS_COVERAGE_INCLUDE_INTEGRATION=1`.
  Use `YAMS_PREPUSH_GATE_SELF_TEST=1 git push` to prove hook wiring without the
  expensive build/test work. Temporary bypasses: `YAMS_SKIP_PREPUSH_CI_GATE=1 git push`
  or `YAMS_SKIP_COVERAGE_HOOK=1 YAMS_PREPUSH_COVERAGE=1 git push`.
- Optimize test **overlap and suite shape**, not away correctness. Keep a fast
  default correctness lane, but move stress/soak, migration, heavy log-capture,
  and multi-second wait tests into explicit slow suites or binaries instead of
  deleting them.
- Avoid repeated expensive bootstrap in broad suites when bootstrap is not the
  behavior under test: temp DB + pool + migration + daemon/KG setup should be
  reused, pre-seeded, or split into narrower binaries where safe.
- Prefer deterministic synchronization (`promise`/`future`, latches, condition
  variables, observable state transitions) over fixed sleeps and long polling
  loops. If polling is unavoidable, keep deadlines tight and justify them.
- Non-log-assertion tests should clamp log volume; verbose daemon/service logs
  belong in focused diagnostics/logging tests, not the default fast TSAN path.
- Split monolithic Catch2 binaries when one executable bundles many unrelated
  files and blocks `meson test` parallelism; preserve behavior coverage while
  improving failure locality and runtime.
- `-DYAMS_TESTING=1` gates `testing_*` helpers in production headers.
- RAII guards for global state (`ProfileGuard`, `EnvGuard`, `HwGuard`); reset
  atomics/overrides between cases.
- DynamicCap sentinel: `UINT32_MAX` = unset (use it when resetting, not `0`).
- See `docs/developer/testing.md` for the full suite-shaping policy.

## Patterns To Reuse (High Signal)

- `TuneAdvisor`: static inline atomics, relaxed reads for advisory knobs.
- `InternalEventBus`: typed channels keyed by string names.
- `ResourceGovernor`: check admission before heavy work.
- `profiling.h`: `YAMS_*` macros, no-op without Tracy.
- Daemon async paths: Boost.Asio coroutines (`boost::asio::awaitable<…>`).

## Bug-Bounty (Repo Use)

Scoped targets only; read-only/low-impact validation first; no persistence,
destructive payloads, or exfiltration; redact secrets before `yams add`; index
evidence as short reproducible notes (`source=evidence`).

## Keep This File Lean

Generic agent behavior belongs in `docs/prompts/PROMPT-eng-codex.md`. No
session-specific skill catalogs here.
