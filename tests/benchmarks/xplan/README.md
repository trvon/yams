# xplan (engineering)

Unified multi-arm experiment runner for daemon KPI work. **Not** a product surface.

## Commands

```bash
python3 tests/benchmarks/xplan/runner.py self-test
python3 tests/benchmarks/xplan/runner.py list-plans
python3 tests/benchmarks/xplan/runner.py list-workers

# BEIR corpora for quality/topology plans (auto-fetched on first run too)
python3 tests/benchmarks/xplan/runner.py download-beir            # scifact + nfcorpus
python3 tests/benchmarks/xplan/runner.py download-beir scifact
python3 tests/benchmarks/xplan/runner.py download-beir --list

# Run a plan (writes artifacts under build/benchmarks/)
python3 tests/benchmarks/xplan/runner.py run <plan> --build-dir build/release
python3 tests/benchmarks/xplan/runner.py run <plan> --dry-run
python3 tests/benchmarks/xplan/runner.py run <plan> --arm baseline --arm no_kg

# Regenerate docs for an existing artifact directory
python3 tests/benchmarks/xplan/runner.py report build/benchmarks/<plan>/<stamp>

# Compare two stamps
python3 tests/benchmarks/xplan/runner.py compare \
  build/benchmarks/ingest_pipeline/<stampA> \
  build/benchmarks/ingest_pipeline/<stampB>
```

## Generated report files (per run)

| File | Contents |
|------|----------|
| `REPORT.md` | Full engineering report: host/git, primary + all metrics, factors, per-arm detail |
| `ablation.md` | Baseline vs other arms, relative deltas, ranking hints |
| `metrics.csv` | Flat table for spreadsheets |
| `report.json` | Machine-readable full report |
| `summary.md` / `summary.json` | Compact overview |
| `arms/<arm>/metrics.json` | Per-arm KPIs + ablation attributes |

All under `build/benchmarks/<plan>/<stamp>/` (gitignored).

## Layout

```text
tests/benchmarks/xplan/
  runner.py          # CLI
  plans/*.json       # experiment definitions
  workers/           # measurement adapters + ablation.py
  report.py          # REPORT.md / ablation.md / csv
  summarize.py       # compact summary + report hook
  schema/            # plan schema
```

## Ablation

`workers/ablation.py` maps plan factors → env (one-factor-off):

- Search components: `YAMS_SEARCH_*_WEIGHT=0` + `YAMS_ENABLE_ENV_OVERRIDES=1`
- Ingest: `YAMS_BENCH_DISABLE_KG`, `YAMS_DISABLE_VECTORS`, GLiNER
- Topology mode / expansion presets (`full64`…`rerank_only`) in `retrieval_quality`

## Plans (engineering)

| Plan | Focus |
|------|--------|
| `search_component_ablation` / `subsystem_overhead` | Search subsystem one-factor-off (BEIR scifact) |
| `ingest_pipeline` | Ingest kg/vectors/gliner (synthetic docs — throughput, not ranking) |
| `retrieval_load` | Concurrent load, search_type |
| `repair_ability` | Fault-kind repair |
| `ops_timeline` | Idle / drain |
| `topology_*` / `simeon_rerank` / `topology_core_ab` / `leg_stage_ablation` | Quality / topology on BEIR scifact |
| `topology_construction_purity` | Construction/routing purity vs quality (cert + MRR) |
| `topology_optimize_v2` | Focused CC/graph follow-up on purity winners |
| `daemon_ops_core` | Multi-step chain |

## Corpora

| Kind | Used by | Notes |
|------|---------|--------|
| **BEIR scifact** (default for quality) | `search_component_*`, `topology_*`, `simeon_rerank*`, `leg_stage_*` | `~/.cache/yams/benchmarks/scifact`; auto-download on run |
| **BEIR nfcorpus** | optional plan override `dataset=nfcorpus` | same cache layout |
| **synthetic** | ingest/load/ops throughput only | **not** for ranking ablations — opt-in via `"dataset": "synthetic"` |

Default quality params: `dataset=scifact`, `corpus_size=2000`, `num_queries=50`.

## Rules

- **No new multi-arm shell scripts.** Wrappers under `scripts/` only.
- Prefer typed config for product knobs; `YAMS_BENCH_*` is harness-only.
- **Do not rank search/topology levers on synthetic** — BEIR only (or another labeled hard corpus).
- Topology claims need `debug.jsonl` counters / certificates.
- Multi-client plans need a Catch2-enabled builddir.
- Published numbers: `docs/benchmarks/README.md` (refresh after runs).
