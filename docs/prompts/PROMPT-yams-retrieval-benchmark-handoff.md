# Prompt: YAMS retrieval benchmark handoff

Use when collecting performance numbers after search/topology/fusion/rerank changes.

## Role

Run xplan safely, preserve **generated** artifacts, verify path markers, report deltas
without overclaiming. **Do not** commit KPI tables into `docs/`.

## Rules

- Source `~/.zshenv` before build/bench commands.
- `meson compile -C <builddir> -j4 <target>` (no parallel meson compiles).
- Multi-arm matrices: **xplan only**
  `python3 tests/benchmarks/xplan/runner.py run <plan> --build-dir build/release`
- Prefer `build/release/tests/benchmarks/retrieval_quality_bench`.
- Topology-core off unless the plan arm sets it.
- Non-vector topology sources + seed: require construction certificate in `debug.jsonl`.
- Artifacts: `build/benchmarks/<plan>/<stamp>/` — open `REPORT.md` and `ablation.md`.
- Organized numbers: `docs/benchmarks/README.md`. Harness: `tests/benchmarks/xplan/README.md`.
- Repo rules: `AGENTS.md`.

## Build

```bash
zsh -lc 'source ~/.zshenv 2>/dev/null || true; meson compile -C build/release -j4 retrieval_quality_bench'
python3 tests/benchmarks/xplan/runner.py self-test
```

## Canonical runs

```bash
# classic vs topology_core
python3 tests/benchmarks/xplan/runner.py run topology_core_ab --build-dir build/release

# expansion arms
python3 tests/benchmarks/xplan/runner.py run topology_expansion --build-dir build/release

# source × expansion
python3 tests/benchmarks/xplan/runner.py run topology_source --build-dir build/release
```

Regenerate docs for a stamp:

```bash
python3 tests/benchmarks/xplan/runner.py report build/benchmarks/<plan>/<stamp>
```

Compare two stamps:

```bash
python3 tests/benchmarks/xplan/runner.py compare <dirA> <dirB>
```

## Report format (from generated REPORT.md)

- Artifact path + plan + stamp
- Validation / certificates
- Primary metrics table
- Ablation deltas vs baseline
- Verdict only if counters support the claim
