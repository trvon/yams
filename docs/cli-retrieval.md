# Bounded CLI retrieval and repository freshness

These commands target the updated CLI and daemon. YAMS remains experimental;
use `yams <command> --help` for the options supported by an installed build.

## Start with bounded results

```bash
yams list --recent 3
yams list '*handoff*' --metadata task=release-review --paths-only --limit 10
yams grep -F 'resolveOriginNode' --paths-only --max-results 10
yams search 'path version history' --compact --limit 5
```

`list --recent N` limits the returned items, not just the heading. A slash-free
list pattern matches basenames before pagination. Metadata filters also apply
before pagination.

Grep's `-m N` remains a **per-file** match limit. `--max-results N` caps the
combined output across files; paths-only output counts distinct paths. Paths-only
grep defaults to 50 paths. Explicit `--max-results 0` removes that global cap.

`search --compact` and `grep --compact` produce `path:line: snippet` records
without banners. Search scores are omitted unless `--scores` is supplied.
`--quiet` suppresses retrieval advice and uses `(no results)` for an empty result.

## Bound text output

Search, grep, list, get, cat and graph accept the same payload budget:

```bash
yams grep -F 'pathOnly' --budget-chars 4000
yams graph --list-types --budget-chars 2000
```

The renderer keeps complete lines that fit and, when output is omitted, appends
one marker:

```text
[truncated: 123 more chars, 4 more results; narrow with --path/--limit/--lines]
```

The marker is additional to the payload budget; this is not a hard cap on the
entire process output. Stderr diagnostics are separate. Accounting uses text
bytes, so the payload limit is conservative for multibyte UTF-8. Unlimited output
remains binary-safe; use slices or byte caps when inspecting binary content.

Defaults, in descending precedence:

1. Explicit `--budget-chars N`.
2. `YAMS_BUDGET_CHARS=N`.
3. `budget_chars` in the `[cli]` config section.
4. Unlimited when none is configured.

```toml
[cli]
budget_chars = 4000
```

An explicit zero disables the inherited budget. Result limits and output budgets
are independent: a payload budget does not replace candidate/result limits.

## Inspect only the needed content

```bash
yams cat --hash HASH --lines 10:30
yams get --name /absolute/path/to/file.cpp --around 'handleUnlinkPathRequest' --context 3
yams cat --hash HASH --max-bytes 4096
```

Line ranges are inclusive and start at 1. Sliced lines carry their original line
numbers. `--around PATTERN --context N` selects matching neighborhoods.
`--lines` and `--around` are mutually exclusive; malformed ranges fail rather
than silently returning unrelated content. `--max-bytes` bounds the resulting
content, and zero means unlimited. Name, hash and local-file reads share these
slice rules.

## Check and update a Git worktree

```bash
yams status --repo "$PWD"
yams add --changed "$PWD" --sync --no-embeddings --no-session
yams grep -F 'worktreeneedle' --worktree "$PWD" --paths-only
```

Repository status exits 0 when fresh and 3 when stale. Freshness compares the
worktree's Git-selected files with indexed content hashes; timestamps alone do
not establish freshness. Each worktree carries its own repository/branch scope.

Incremental ingestion records repository ownership for the imported locations.
It ingests changed files and retires missing locations from that snapshot.
Retiring a location preserves shared content and immutable graph history; it is
not a hash-wide delete. Generic notes beneath a repository are not automatically
owned by its snapshot and must not be removed by reconciliation. Legacy generic
imports are recognized only at exact currently Git-tracked paths, not merely by
a common directory prefix.

The CLI checks daemon path-unlink support before incremental ingestion. An older
daemon must be upgraded or restarted. Unlink uses a distinct protocol operation
and never retries a legacy hash-delete operation after reconnecting.

No Git hook is installed automatically.

## Live grep and exclusions

```bash
yams grep -F 'cli_live_probe_only' --live --paths-only /path/to/worktree
yams grep -F 'indexed text' --no-live --paths-only
```

Explicit `--live` scans the filesystem without a daemon lookup. On supported
hosts it uses an argv-only, streaming ripgrep subprocess; unavailable ripgrep
retains the C++ fallback. Scoped stale-repository detection can select live grep
automatically; `--no-live` keeps retrieval indexed. Live grep rejects corpus-tag
filters because filesystem contents cannot establish those tags.

Default retrieval exclusions cover temporary files, Pi session scratch and
scratchpad directories. Explicit selection can include excluded content; adjust
`[search].exclude_paths` when a corpus intentionally lives there. For example,
`exclude_paths = []` disables those defaults. Avoid relying on a broad temporary
root as the permanent home of a corpus.

## Explicit model setup and initialization

Static embedding tables are opt-in. `init --auto` and `init --non-interactive` do not
download them; the interactive download prompt defaults to **No**. A table merely
present in the data directory or installed package is not a configured selection.

Choose a table explicitly with `yams model download <catalog-name>`, or configure
an existing validated table through the existing setting:

```toml
[embeddings.simeon]
static_table = "/absolute/path/to/table.smstat"
```

An explicit static-table download records the selection, including when the
validated file is already cached. Missing configured files do not silently select
another table. Existing backend, disabled, hash and profile choices are preserved.
A **selected** message means persisted configuration, not proof that a running
daemon has activated that model.

CLI selection uses a staged, same-parent replacement and checks unrelated values
before publishing. Unsupported multiline or ambiguous TOML layouts fail closed;
edit those configurations manually. This is not a general TOML editor,
cross-writer transaction or power-loss durability guarantee.

When an add reaches core services but initialization or resource pressure prevents
synchronous processing, an explicit completion request receives a retryable busy
error rather than a successful async-queue acknowledgement. Explicit async requests
retain their queued acknowledgement. This guard does **not** bound full recovery
or certify authenticated replication readiness; those remain separate checks.

## Reproduce the verification lanes

From a configured Debug build:

```bash
meson compile -C build/debug -j4 grep_service catch2_daemon_protocol yams-cli
build/debug/tests/grep_service
build/debug/tests/catch2_daemon_protocol
python3 tests/integration/cli_token_budget_test.py \
  --binary build/debug/tools/yams-cli/yams-cli
```

For process-level live-grep timing, build Release separately, then run the paired
control. The harness creates an isolated 5,000-file corpus, checks matching file
identities, rotates run order, and records all seven measured durations:

```bash
meson compile -C build/release -j4 yams-cli
python3 tests/benchmarks/scripts/cli_live_grep_smoke.py \
  --after build/release/tools/yams-cli/yams-cli \
  --before /path/to/pre-change/yams \
  --output build/benchmarks/cli-live-grep/paired.json
```

The timing harness fails when the updated CLI median exceeds three times rg's
median. Its JSON artifact is written before that check. A green CLI lane does
not establish concurrent prune/unlink safety or semantic-search alias parity;
those need their own regressions and review.

### Verification snapshot: 2026-11-03

The isolated Linux x86-64 Release build at `59dd894b` plus the retrieval fixes
produced these medians for the workload above (one warmup, seven rotating rounds):

| Process | Median wall time |
| --- | ---: |
| ripgrep 15.2.0 | 9.248 ms |
| Updated CLI | 22.660 ms |
| Installed CLI (`0aec0ad4`) | 14.965 ms |

The updated CLI meets the **2.450× rg** criterion, but is slower than the installed
CLI in this paired run. This is not evidence of an installed-CLI speedup. The
updated executable's SHA-256 is
`3c6fe9dc8843ef6f59a623247f8a0fa2d168170904b77c738f77cde77132ed1a`;
the tracked source patch used for that build had SHA-256
`fc2f7e663f83995fdab37b5d5837d610c45410554346c93d1556e0dc703096f1`.
The optional prepared-payload/RSS xplan run is separate and was not executed
because its local harness base was unavailable.

### Boost coroutine analyzer characterization

LLVM 23.1.4 with Boost 1.92 reports an uninitialized object in
`boost/asio/impl/awaitable.hpp` at lines 179 and 229 when checking the request
handler. The standalone, YAMS-free two-translation-unit probe under
`tests/diagnostics/boost_awaitable_analyzer_probe*` reproduces both reports:

```bash
clang-tidy tests/diagnostics/boost_awaitable_analyzer_probe.cpp --quiet \
  --config="{Checks: '-*,clang-analyzer-core.CallAndMessage', WarningsAsErrors: '*', HeaderFilterRegex: '.*'}" \
  -- -std=c++23 -pthread -fsanitize=thread

mkdir -p build/diagnostics
clang++ -std=c++23 -pthread -fsanitize=thread \
  tests/diagnostics/boost_awaitable_analyzer_probe.cpp \
  tests/diagnostics/boost_awaitable_analyzer_probe_impl.cpp \
  -o build/diagnostics/boost_awaitable_analyzer_probe
build/diagnostics/boost_awaitable_analyzer_probe
```

The analyzer command is a **diagnostic reproduction**, not a passing test: on
that toolchain it exits nonzero with the two reports. The compiled probe exits
zero under TSAN. This demonstrates that those diagnostics do not require YAMS
code or runtime state; it does not prove every request-handler path safe. The
raw request-handler analyzer check still reports them. No suppression was added,
and these results are not a claim of a completely clean static-analysis run.

### Local commit-packaging checks: 2026-11-03

The dependency-ordered commit split reproduced the preserved aggregate source tree
exactly. Component checks and normal pre-commit hooks ran during packaging, but
individual intermediate commit trees were not separately built. Rebuild
`catch2_cli_common` after HEAD changes before running its version-header checks:
those assertions compare generated metadata with the current Git commit.

The registered fast-smoke scope (`~[mcpdocopsfixture]`), with the existing
single-instance/vector-disabled test profile, passed **381 assertions** across
**40 cases: 32 passed and 8 skipped**. It ran with
`TSAN_OPTIONS=halt_on_error=1`, without a sanitizer suppression file. Existing
TSAN-only teardown and vector-provider skips remain coverage limitations, not
proof that those paths work.

A broader native smoke invocation without that profile remains **red**:
43 cases, 35 passed, 3 failed and 5 skipped. It reported an MCP tag-wait failure,
an embedding-fixture `Result contains value` exception, and missing CLI fallback
output. TSAN also reported `YamsDaemon::stop()` reading a thread's state while
`runLoop()` assigned it (`src/daemon/daemon.cpp:1154` and `:710` in this snapshot).
Those daemon and embedding-test files were unchanged from the review baseline;
that does not establish the first-bad revision or the cause of the failures.
Passing the fast scope does not clear the broader failures. They remain work for
the startup/lifecycle review; no assertion or sanitizer diagnostic was suppressed.

Global recovery deadlines, degraded-state read/write boundaries, checked drain,
and constrained idle CPU/RSS measurements are still unfinished. The earlier
Release timing above predates the startup/model changes and was not remeasured
for this packaged candidate. These local commits do not certify merge or release
readiness, and do not supply a human DCO sign-off.
