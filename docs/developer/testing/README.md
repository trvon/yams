# Developer Testing Guide

This page shows how to build, run, and write tests for YAMS. It’s short, copy‑paste friendly, and points you at the right targets.

## TL;DR

```bash
# Configure with tests (dev profile recommended)
cmake -S . -B build -DYAMS_BUILD_PROFILE=dev -DYAMS_BUILD_TESTS=ON

# Build
cmake --build build -j

# Run all tests (verbose on failure)
ctest --test-dir build --output-on-failure

# Labels
ctest --test-dir build -L unit
ctest --test-dir build -L integration
ctest --test-dir build -L performance

# Filter by name/regex
ctest --test-dir build -R "Search.*" -j
```

## Test Types

- Unit tests
  - Fast, in‑memory, no filesystem/network where possible
  - Mock external boundaries (DB, network, OS)
  - Label: `unit`
- Integration tests
  - Exercise multiple components (e.g., CLI → metadata → content store)
  - Real SQLite and filesystem recommended; mock only external services
  - Label: `integration`
- Performance/benchmarks (optional)
  - Build with `-DYAMS_BUILD_BENCHMARKS=ON`; benchmarks live under `src/<module>/benchmarks` and run separately (not in CTest)
  - Label: `performance` (if covered via CTest harness)

## Build With Tests

```bash
# Common setup
cmake -S . -B build -DYAMS_BUILD_PROFILE=dev -DYAMS_BUILD_TESTS=ON
cmake --build build -j
ctest --test-dir build --output-on-failure
```

Notes:
- GoogleTest is pulled in automatically when tests are enabled.
- Prefer `ctest --test-dir build` so generator differences don’t matter.

## Consumer (Downstream) Test

We ship a minimal downstream consumer that verifies `find_package(Yams)` works against a staged install.

```bash
# Ensure testing is on (it is with dev profile)
cmake -S . -B build -DYAMS_BUILD_PROFILE=dev
cmake --build build -j

# Run just the consumer tests (names may vary slightly)
ctest --test-dir build -R "consumer_" --output-on-failure
```

What it does:
- Installs the current build into a staging prefix
- Configures and builds a tiny consumer project against that prefix
- Runs the consumer’s CTest suite

## Coverage (Optional)

```bash
# Configure with coverage
cmake -S . -B build \
  -DYAMS_BUILD_PROFILE=dev \
  -DYAMS_BUILD_TESTS=ON \
  -DYAMS_ENABLE_COVERAGE=ON

# Build + test
cmake --build build -j
ctest --test-dir build --output-on-failure

# Generate HTML (gcovr preferred; falls back to lcov if configured)
cmake --build build --target coverage-html

# Open report
open build/coverage/html/index.html      # macOS
xdg-open build/coverage/html/index.html  # Linux
```

Also available:
- `coverage-xml` (for CI upload)
- `coverage-summary` (quick stats)

Coverage excludes:
- `src/**/benchmarks/*`

## Writing Tests

Principles:
- One assertion path per test (clear Arrange‑Act‑Assert)
- No hidden global state; isolate filesystem and environment
- Deterministic and seedable (avoid time/non‑determinism)

Structure:
- Unit tests live near their module’s tests tree (see existing layout)
- Use GTest fixtures for setup/teardown
- Name tests clearly: `ModuleName.FunctionName_Scenario_ExpectedBehavior`

Filesystem & config:
- Use temporary, unique directories per test (e.g., under `std::filesystem::temp_directory_path()` or a test-private subdir in `build/`)
- Set `YAMS_STORAGE` to a test‑local path to avoid touching real data
- Avoid `sudo` and global user paths in tests

Mocking:
- Mock external services; prefer real SQLite/filesystem for integration
- Keep mocks small and focused; verify behavior, not implementation details

Randomness & time:
- Seed any RNG explicitly
- Fix times or inject a clock for deterministic behavior (UTC if needed)
- Normalize locale/encoding when testing string formatting (e.g., `LC_ALL=C`)

CLI tests:
- Capture stdout/stderr and exit codes
- Keep CLI tests small; leave exhaustive option coverage to docs/verbose help and a few “happy path” checks

## Running Subsets

```bash
# By label
ctest --test-dir build -L unit
ctest --test-dir build -L integration

# By regex name match
ctest --test-dir build -R "Metadata|Search"

# Parallel runs
ctest --test-dir build -j 8

# Stop on first failure
ctest --test-dir build --stop-on-failure
```

## Benchmarks (Optional)

Benchmarks are module‑local and live under `src/<module>/benchmarks`. A reusable framework is available as `yams::benchmarks` from `src/benchmarks`.

```bash
# Configure with benchmarks
cmake -S . -B build -DYAMS_BUILD_PROFILE=dev -DYAMS_BUILD_BENCHMARKS=ON
cmake --build build -j

# Run all benchmarks via discovery (Linux/macOS)
find build -type f \( -name "*_bench" -o -name "*_bench.exe" \) -print0 \
| xargs -0 -I{} sh -c '{} --benchmark_min_time=0.1 --benchmark_format=json --benchmark_out="{}.json" || true'

# Or run a single benchmark directly, e.g.:
./build/src/search/benchmarks/query_parser_bench --benchmark_min_time=0.1
```

Guidelines:
- Benchmarks are opt‑in and not part of CTest by default
- Keep runs deterministic (fixed seeds, minimal logging)
- Avoid external network/services in timed regions

## CI Notes

- Typical CI flow:
  - Configure with `-DYAMS_BUILD_PROFILE=dev -DYAMS_BUILD_TESTS=ON`
  - Build: `cmake --build build -j`
  - Test: `ctest --test-dir build --output-on-failure`
  - Optional coverage: `cmake --build build --target coverage-html` and upload artifact
- For docs consistency, install pandoc if you need manpages/embedded verbose help built in CI.

## Troubleshooting (Fast)

- Build succeeds but tests not found:
  - Ensure `-DYAMS_BUILD_TESTS=ON` at configure time and reconfigure clean
- Flaky tests:
  - Remove parallelism temporarily: `ctest --test-dir build -j1`
  - Check for shared state or temp directory collisions
- OpenSSL not found (macOS):
  - `export OPENSSL_ROOT_DIR="$(brew --prefix openssl@3)"`
- Failing CLI tests in CI only:
  - Force deterministic locale/timezone for jobs (e.g., `LC_ALL=C TZ=UTC`)
- Coverage empty:
  - Confirm compiler supports instrumentation and `YAMS_ENABLE_COVERAGE=ON` is set
  - Run tests before generating coverage targets

## Conventions Checklist

- [ ] Unit tests: no external side effects; mocks where appropriate
- [ ] Integration tests: real DB/filesystem; mock external services
- [ ] Deterministic output: fixed seeds, TZ=UTC if time is involved
- [ ] Clean temp dirs: create unique paths; delete on teardown when safe
- [ ] Meaningful names: `Module_Action_Result`
- [ ] Keep logs minimal; assert on return values and outputs

## See Also

- Developer Quick Start: `../../developer/setup.md`
- Build System: `../../developer/build_system.md`
- CLI Reference (authoritative for flags): `../../user_guide/cli.md`
- Installation: `../../user_guide/installation.md`
- Admin/Configuration: `../../admin/configuration.md`
