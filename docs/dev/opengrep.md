# OpenGrep static-analysis profile

YAMS keeps an OpenGrep-compatible local rule pack under `tools/opengrep/rules`.
OpenGrep is a Semgrep fork, so the same YAML rules can be run by OpenGrep or by
Semgrep as a local fallback.

## Install OpenGrep

Ask before installing dependencies in this repo. When approved, install with the
upstream macOS/Linux installer:

```bash
curl -fsSL https://raw.githubusercontent.com/opengrep/opengrep/main/install.sh | bash
```

OpenGrep also publishes release binaries on GitHub.

## Run

```bash
scripts/dev/run_opengrep.sh
OPENGREP_PROFILE=audit scripts/dev/run_opengrep.sh src include
OPENGREP_PROFILE=public scripts/dev/run_opengrep.sh src include tests
OPENGREP_PROFILE=all scripts/dev/run_opengrep.sh src include tests
```

Outputs:

- `.artifacts/opengrep/opengrep-yams-<profile>.json`
- `.artifacts/opengrep/opengrep-yams-<profile>.sarif`

If `opengrep` is not installed, the wrapper uses `semgrep` when available. Set
`OPENGREP_BIN=/path/to/opengrep` to force a specific binary.

Useful knobs:

```bash
OPENGREP_STRICT=1 scripts/dev/run_opengrep.sh src include
OPENGREP_TIMEOUT=120 scripts/dev/run_opengrep.sh src/wal src/api
OPENGREP_OUT_DIR=.artifacts/opengrep/pr scripts/dev/run_opengrep.sh
OPENGREP_PROFILE=audit scripts/dev/run_opengrep.sh src include
OPENGREP_PROFILE=public scripts/dev/run_opengrep.sh src include tests
OPENGREP_PROFILE=all scripts/dev/run_opengrep.sh src include tests
```

## Profiles

- `default`: low-noise CodeQL parity / serialization checks.
- `audit`: higher-noise migration and binary-codec audit checks.
- `public`: run curated online registry packs: `p/default`, `p/security-audit`, `p/trailofbits`, `p/c`.
- `all`: run every local profile.
- Any path or registry entry can be passed as `OPENGREP_PROFILE=/path/to/rules` or `OPENGREP_PROFILE=p/<pack>`.

## Curated local rules

- `yams.cpp.suspicious-add-sizeof`: CodeQL `cpp/suspicious-add-sizeof` parity/audit rule.
- `yams.cpp.raw-yams-getenv`: flags new runtime `YAMS_*` env lookups outside tests.
- `yams.cpp.size-to-u32-cast`: audits possible size truncation in binary serialization.
- `yams.cpp.vector-data-reinterpret-struct`: audits vector/span binary codec aliasing patterns.
- `yams.cpp.dynamic-sql-execute`: flags dynamic SQL passed to execute-style APIs instead of prepared statements.
- `yams.cpp.memcpy-non-pod-object`: audits memcpy into structured objects without exact-size POD copy.
- `yams.cpp.result-value-without-guard`: high-noise audit for unchecked `.value()` access on Result-like objects.

The rule pack intentionally uses mostly INFO/WARNING severity because these are
triage accelerators, not proof of vulnerability.

## Public rule sources reviewed

- OpenGrep is compatible with Semgrep rules and supports JSON/SARIF output.
- Trail of Bits publishes `p/trailofbits` / `trailofbits/semgrep-rules`.
- Semgrep community registry packs useful here: `p/default`, `p/security-audit`, `p/c`. `p/cpp` was not available in this OpenGrep registry run.


## Git hook

The tracked pre-commit hook in `.githooks/pre-commit` calls `.githooks/opengrep-pre-commit`.
It is opportunistic: if `opengrep` or local rules are missing, it skips cleanly.
When rules change, it validates the `default` and `audit` profiles. When C/C++ files are staged,
it scans only those staged files with the local `default` profile.

By default findings are advisory so legacy findings do not block commits. To make findings blocking locally:

```bash
YAMS_OPENGREP_STRICT=1 git commit
```

To use the tracked hooks in a clone:

```bash
git config core.hooksPath .githooks
```

