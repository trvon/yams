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
- Topology mode / expansion presets (`full64`, `medoid64`, `cap8`, `cap2`) in
  `retrieval_quality`

## Plans (engineering)

| Plan | Focus |
|------|--------|
| `search_component_ablation` / `subsystem_overhead` | Search engine component one-factor-off (BEIR scifact) |
| `leg_stage_ablation` / `simeon_rerank*` | Search pipeline / rerank arms |
| `ingest_pipeline` | Ingest kg/vectors/gliner (synthetic — throughput, not ranking) |
| `retrieval_load` / `repair_ability` / `ops_timeline` / `daemon_ops_core` | Daemon KPIs 2–5 |
| `read_write_pressure` | Equal-budget write-heavy/balanced/read-heavy/read-only daemon load; per-op latency, DB pools, WriteCoordinator pressure, WAL growth, drain, and recovery (repeats=3) |
| `topology_purity_validate` / `topology_routing_budget_ablation` | Topology construction purity and routed-vs-global ANN budget gate (repeats=3) |
| `search_generalized_memory_topology_gate` | SciFact + NF-Corpus in one index; per-source quality, cross-source interference, and topology-vs-global cost (repeats=3) |
| `search_simeon_ann_attribution_multicorp` | Mixed-corpus text/current-PQ/exact-vec0 attribution over identical Simeon embeddings (repeats=3) |
| `plans/archive/*` | Superseded plans (historical only) |

Agent measurement loop and plan selection: repo `AGENTS.md` (Benchmarks & Experiments).

## Corpora

| Kind | Used by | Notes |
|------|---------|--------|
| **BEIR scifact** (default for quality) | `search_component_*`, `topology_*`, `simeon_rerank*`, `leg_stage_*` | `~/.cache/yams/benchmarks/scifact`; auto-download on run |
| **BEIR nfcorpus** | optional plan override `dataset=nfcorpus` | same cache layout |
| **Mixed BEIR memory** | `search_generalized_memory_topology_gate` | one daemon/index containing namespaced SciFact + NF-Corpus documents and queries |
| **synthetic** | ingest/load/ops throughput only | **not** for ranking ablations — opt-in via `"dataset": "synthetic"` |

Default quality params: `dataset=scifact`, `corpus_size=2000`, `num_queries=50`.

Plans with separate SciFact and NF-Corpus arms test transfer between isolated indexes; they do
not model generalized memory. A retrieval-quality plan can set `datasets` to two or more BEIR
names to materialize one content-deduplicated `local-manifest` for the entire run. The worker
namespaces document/query IDs, remaps duplicate-content qrels, and reports per-source MRR/recall
plus `mixed_cross_source_result_rate` and `mixed_cross_source_top1_rate` from the shared ranking.
The benchmark also exports `topology_clusters.json` and derives `mixed_cluster_overlap.json`, which
shows source counts, entropy, and purity for every cluster. Aggregate metrics distinguish the
fraction of cross-source clusters from the fraction of documents exposed to those clusters;
content duplicated across corpora is reported separately and excluded from partition purity.

For state-sensitive search ablations, `shared_warm_cache` primes one immutable YAMS store and clones
it into each measured arm/repeat. This fixes corpus, vector index, tuner state, and persisted
topology without letting one workload mutate the next workload's input. Topology plans can also set
`require_topology_construction_identity=true`; the worker pins the first construction fingerprint
and fails on within-workload, cross-repeat, or cross-arm drift. Do not point multiple measured arms
at one live warm directory: that preserves topology but carries mutable query/tuner state forward.

Evaluate ingestion changes in two lanes at the same code revision. Use
`search_generalized_memory_topology_gate` after the index settles for mixed-memory quality and
route purity. Use `retrieval_load` for concurrent adds/searches through the daemon; it reports
post-ingest backlog plus WriteCoordinator queue depth, in-flight work, apply time, queue wait, and
excess queue wait. Do not combine these into one score: promote an ingestion change only when it
preserves the per-source quality floor and improves or holds the read-under-write pressure KPIs.

Use `read_write_pressure` when the question is contention rather than search-component attribution.
Its arms execute the same operation budget per client, vary only the read/write mix, and subtract a
post-warmup infrastructure baseline from cumulative queue/WAL counters. Point-in-time queue depth
and the workload high-water delta are both retained so short bursts are not hidden by sampling.
Set harness-only `YAMS_BENCH_CORPUS_SEED_DIR` to a frozen YAMS data directory to give every
arm/repeat an isolated copy-on-write corpus; `YAMS_BENCH_SEARCH_QUERY` can select a representative
symbol or concept. The source directory is never opened by the daemon and remains unchanged.

## Rules

- **No new multi-arm shell scripts.** Wrappers under `scripts/` only.
- Prefer typed config for product knobs; `YAMS_BENCH_*` is harness-only.
- **Do not rank search levers on synthetic** — BEIR only (or another labeled hard corpus).
- Topology claims need `debug.jsonl` counters / certificates.
- Multi-client plans need a Catch2-enabled builddir.
- Published numbers: `docs/benchmarks/README.md` (default system only).

## Known infrastructure debt

- `yams graph --explore` can fail with a local index `SQL logic error` for valid readiness
  symbols (observed on `SearchEngineManager::getSnapshot` / `rebuildNow`). The fallback is a
  targeted local read after YAMS retrieval identifies the files. Follow-up: capture `yams doctor`
  diagnostics, verify graph/index schema integrity, and add a regression fixture before changing
  graph-query behavior.
