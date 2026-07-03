# Local GitHub Actions Validation

Use the local push lane before pushing CI, release, packaging, daemon-test, or benchmark/reporting changes:

```bash
bash scripts/local-ci/push-lane.sh --profile fast
```

Logs and summaries are written under `build/local-ci/push-lane/`. On Apple Silicon the wrapper runs Linux jobs with `--container-architecture linux/amd64`.

## Profiles

| Profile | Runs | Use when |
|---|---|---|
| `fast` | act sanity, Linux x86_64 tests via `act`, Linux-hosted release fast-mode via `act` | Default pre-push lane |
| `tests` | act sanity, Linux x86_64 tests, targeted native Meson tests | Debugging test failures |
| `release` | Linux-hosted release fast-mode via `act` | Debugging release/package shell errors |
| `packages` | Local Docker package build; install validation only on Linux | Debugging package output |
| `full` | `tests + release + packages` | High-risk pushes |

## Current status (2026-07-03)

All known local blockers found by the full lane are fixed in-tree.

| Lane | Status | Evidence |
|---|---|---|
| act sanity | Passing | `workflow-lint`, `windows-script-sanity` passed in full lane. |
| Linux tests via act | Passing | `tests-linux-x86_64-act` passed `224/224`. |
| Targeted native tests | Passing | `integration_smoke`, `daemon_background_processing`, `metadata_corruption` passed together. |
| Linux-hosted release fast-mode | Passing | Release summary generation now handles empty benchmark output. |
| Package build-only on macOS | Passing | `package-lane-build-only` passed in full lane. |

Recommended final check:

```bash
bash scripts/local-ci/push-lane.sh --profile full --keep-going
```

## Opt-in pre-push hook

```bash
YAMS_PREPUSH_GH_LANE=1 git push
YAMS_PREPUSH_GH_LANE=1 YAMS_PREPUSH_GH_PROFILE=release git push
```

## Known gaps

Local `act` does not fully reproduce hosted Windows, hosted macOS, or hosted Linux arm64. Systemd package install validation runs only on Linux; macOS runs package build-only.

## Useful commands

```bash
# Print commands without executing them
bash scripts/local-ci/push-lane.sh --profile fast --dry-run

# Continue after failures and collect all logs
bash scripts/local-ci/push-lane.sh --profile full --keep-going

# Use a specific local Meson build directory for targeted native tests
YAMS_LOCAL_CI_BUILD_DIR=build/debug bash scripts/local-ci/push-lane.sh --profile tests
```

## Appendix: field runs

### 2026-07-03 release validation after summary fix

```bash
bash scripts/local-ci/push-lane.sh --profile release
```

Result: passed. Release fast-mode built, installed/pruned/verified staged runtime assets, and generated `build/release/release_summary.md` without benchmark JSON files.

Summary: `build/local-ci/push-lane/summary-20260703T004339Z-49808.md`.

### 2026-07-02 full profile after first fixes

```bash
bash scripts/local-ci/push-lane.sh --profile full --keep-going
```

| Step | Result | Finding |
|---|---:|---|
| `act-sanity-workflow-lint` | pass | actionlint config accepted custom runner label. |
| `act-sanity-windows-script` | pass | act-only PowerShell setup worked. |
| `tests-linux-x86_64-act` | pass | Linux act test job passed `224/224`. |
| `targeted-native-known-failures` | fail | Host daemon/socket leakage made search/graph UX smoke hit IPC timeouts. Fixed by forcing embedded transport in that fixture. |
| `release-linux-hosted-fast-act` | fail | Empty `bench_results` directory reached `jq build/release/bench_results/*.json`. Fixed by using a discovered JSON-file array. |
| `package-lane-build-only` | pass | macOS package build-only lane passed. |

Summary: `build/local-ci/push-lane/summary-20260702T215649Z-27474.md`.

### 2026-07-02 targeted native validation after embedded fixture fix

```bash
meson compile -C builddir -j4 yams_integration_smoke_tests
meson test -C builddir metadata_corruption daemon_background_processing integration_smoke --print-errorlogs --timeout-multiplier 2
```

Result: passed (`Ok: 3`, `Fail: 0`).

### 2026-07-02 initial full profile before fixes

```bash
bash scripts/local-ci/push-lane.sh --profile full --keep-going
```

| Step | Result | Finding |
|---|---:|---|
| `act-sanity-workflow-lint` | fail | `actionlint` rejected custom runner label `windows-2025-vs2026`. |
| `act-sanity-windows-script` | fail | `pwsh` was missing in `catthehacker/ubuntu:act-latest`. |
| `tests-linux_x86_64-act` | fail | Conan warm reached `bzip2/1.0.8`, then failed because `cmake` was missing. |
| `targeted-native-known-failures` | fail | `integration_smoke` had grep/graph/timeout failures from fixture tokens colliding with repo-indexed test source. |
| `release-linux-hosted-fast-act` | interrupted | Cold dependency build was manually interrupted. |
| `package-lane-build-only` | interrupted | Follow-on cancellation after release interrupt. |

Summary: `build/local-ci/push-lane/summary-20260702T142323Z-18649.md`.
