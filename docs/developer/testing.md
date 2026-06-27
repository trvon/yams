# Testing Strategy

This repo keeps **correctness coverage** and **slow-path coverage** separate on purpose.
The goal is to make the default TSAN-backed local gate practical **without deleting hard-earned regression tests**.

## Principles

- Optimize **overlap and suite shape**, not away correctness.
- Keep coverage for stress, migrations, logging, shutdown, and soak behavior — just give each class of test an intentional home.
- Prefer a small number of focused, reproducible test patterns over one giant binary that mixes unrelated costs.
- Every test-speed change needs before/after evidence for runtime and a clear statement of what correctness signal is preserved.

## Expected test lanes

### 1. Fast correctness lane

Used by the default local pre-push TSAN coverage path.

Characteristics:

- deterministic
- bounded runtime
- no long wall-clock sleeps
- no repeated expensive bootstrap unless the bootstrap is the thing under test
- broad behavioral coverage for normal correctness

### 2. Specialized slow lanes

Still required, but not allowed to dominate the fast lane.

Typical categories:

- stress / soak / concurrency duration tests
- migration and schema-upgrade coverage
- daemon lifecycle / shutdown / queue-pressure scenarios
- log-capture / diagnostics-heavy assertions
- large fixture / multi-thousand-row maintenance tests
- integration and smoke coverage

Concrete example: the storage Catch2 binary now runs the fast default path as
`storage_submodule` and routes only explicit duration-heavy soak coverage
through `storage_submodule_slow` / `--suite slow`. Keep correctness-critical
concurrency and regression checks in the fast lane; move only the fixed-duration
30s soak (and similar wall-clock stress cases) out of the default pre-push
path.

## Rules for test refactors

### Separate long wall-clock tests from fast correctness

If a test intentionally waits seconds, schedules background work, or runs a fixed-duration soak, it should live in a dedicated slow executable, suite, or clearly scoped path rather than the default fast submodule binary.

Examples:

- 30s concurrent stress loops
- scheduled GC waits
- retry/poll loops that intentionally span seconds

Do **not** delete these tests. Move or retag them so they still run intentionally.

### Avoid repeated expensive bootstrap in broad Catch2 suites

If many cases repeatedly pay for:

- temp DB creation
- connection-pool init
- migration registration + migrate
- daemon/service bootstrap
- KG store setup

then prefer one of:

- a seeded fixture snapshot
- helper that creates a pre-migrated DB once per binary/group
- narrower binaries that keep related expensive setup together
- explicit migration-focused tests separated from CRUD/query tests

Rule of thumb: if the setup is not the behavior under test, do not pay it for every unrelated case.

### Keep migration coverage explicit

Full migration chains are correctness-critical. Preserve them in dedicated migration/schema tests.

But broad CRUD/query/path/filter tests do not all need to re-prove the full migration path on every case. Use a pre-migrated fixture when the test target is repository/query behavior rather than migration logic itself.

### Prefer deterministic synchronization over sleeps

For async/background tests, prefer:

- promises / futures
- latches / barriers
- condition variables
- explicit state transitions

Use polling sleeps only as a bounded fallback around an observable condition, and keep the deadline tight.

### Control log volume in non-log tests

TSAN plus high-volume logging can dominate runtime.

For tests that are **not** asserting on logs:

- clamp `spdlog` to `warn` or `err`
- avoid verbose daemon/service bootstrap logs

For tests that **do** assert on logging/diagnostics:

- use focused log-capture helpers
- keep those tests isolated from unrelated behavior coverage

### Split monolithic binaries when parallelism helps

If one executable aggregates many unrelated test files and becomes a TSAN bottleneck, split it by behavior cluster.

Good split dimensions:

- repository CRUD vs repository counters/tag stats
- CLI config vs CLI completion vs CLI daemon helpers
- graph maintenance vs graph query service
- storage correctness vs storage stress

Do not split just to create churn. Split when it:

- reduces repeated setup
- improves `meson test` parallelism
- makes ownership and failure triage clearer

## What to preserve during optimization

When reshaping tests, preserve all three:

1. **behavioral signal** — the same user-visible/regression behavior is still asserted
2. **failure localization** — when something breaks, the failing suite still points to the right subsystem
3. **intentional slow coverage** — stress/migration/soak/logging coverage still has an explicit place to run

## Required validation for test-suite changes

When changing suite topology, tags, or fixtures:

- capture before/after timing for the affected binaries or lanes
- run the refactored fast lane locally
- run the moved specialized slow suites in their new home
- confirm no assertion/regression coverage was silently dropped
- run `git diff --check`

## Preferred outcomes

Good outcomes:

- same assertions, less repeated bootstrap
- same stress coverage, less fast-lane wall time
- same daemon correctness, fewer sleep/poll loops
- same CLI behavior coverage, more parallel binaries

Bad outcomes:

- deleting slow tests without replacement
- moving correctness checks into docs or comments
- replacing assertions with weaker smoke-only checks
- hiding expensive tests in the fast lane under vague names

## Related docs

- `AGENTS.md`
- `docs/developer/contributing.md`
- `docs/developer/build_system.md`
- `docs/BUILD.md`
