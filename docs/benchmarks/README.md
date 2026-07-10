# Benchmarks

Daemon KPI measurements via **xplan**. Tables below are the latest snapshot of the
**default system** (no experiment matrices). Per-run detail:
`build/benchmarks/<plan>/<stamp>/` (`REPORT.md`, `ablation.md`, `metrics.csv`).

| | |
|--|--|
| Harness | [`tests/benchmarks/xplan/`](../../tests/benchmarks/xplan/README.md) |
| Plans | `tests/benchmarks/xplan/plans/*.json` |
| Artifacts | `build/benchmarks/<plan>/<stamp>/` (gitignored) |
| Micro baselines (code) | `tests/benchmarks/baseline/*.json` |

```bash
python3 tests/benchmarks/xplan/runner.py list-plans
python3 tests/benchmarks/xplan/runner.py download-beir          # scifact + nfcorpus
python3 tests/benchmarks/xplan/runner.py run <plan> --build-dir build/release
python3 tests/benchmarks/xplan/runner.py compare <dirA> <dirB>
```

**Corpora:** quality plans use **BEIR scifact** (`dataset=scifact`, 2000 docs × 50
queries) by default. Cache: `~/.cache/yams/benchmarks/<name>` (auto-download).
`dataset=synthetic` is only for ingest/load/ops throughput — not ranking.

**Topology construction defaults (when topology rebuild runs):**
`min_edge_score=0.25`, `max_component_docs=64`, engine `connected`.

---

## Latest snapshot

**Stamp:** `kpi-20260709T031007Z` (search quality) · `kpi-20260709T003228Z` (ingest / load / repair / ops)\
**Host:** local macOS
**Builds:** `build/release` (ingest, repair, quality); `build/prepush-macos` (load, ops — Catch2)

### Ingest — `ingest_pipeline` (80 docs × 1 KB, synthetic throughput)

| Arm | docs/s | wall_ms | complete |
|-----|------:|--------:|----------|
| baseline | 96.6 | 828 | yes |
| no_kg | 95.6 | 837 | yes |
| no_vectors | 87.0 | 920 | yes |
| no_vectors_no_kg | 97.2 | 823 | yes |
| no_gliner | 97.8 | 818 | yes |
| minimal | 111.0 | 721 | yes |

Artifacts: `build/benchmarks/ingest_pipeline/kpi-20260709T003228Z-ingest/`

### Retrieval load — `retrieval_load`

| Arm | p50_ms | p95_ms | QPS | searches |
|-----|------:|------:|----:|---------:|
| baseline_hybrid | 28.0 | 41.3 | 123 | 189 |
| keyword_only | 1.20 | 5.45 | 362 | 189 |

Artifacts: `build/benchmarks/retrieval_load/kpi-20260709T003228Z-load/`

### Search quality — BEIR scifact (default hybrid)

2000 docs · 50 queries · topk=10 · topology routing **off** (product default)

| Metric | Value |
|--------|------:|
| MRR | 0.613 |
| nDCG | 0.634 |
| MAP | 0.608 |
| recall@10 | 0.713 |
| precision@10 | 0.076 |

Artifacts: `build/benchmarks/topology_optimize_v2/kpi-20260709T031007Z-topo-optv2b/` (`topo_off` arm)

### Repair — `repair_ability` (20 faults)

| Arm | injected | repaired | status |
|-----|--------:|---------:|--------|
| fts5 | 20 | 20 | ok |
| graph | 20 | 225* | ok |
| embed | 20 | 0 | fail (timeout ~120s) |
| embed_all_ops | 20 | 0 | fail (timeout ~120s) |

\* graph arm counts operations, not 1:1 documents.

Artifacts: `build/benchmarks/repair_ability/kpi-20260709T003228Z-repair/`

### Ops / idle — `ops_timeline`

| Arm | ingest docs/s | idle_fraction |
|-----|-------------:|--------------:|
| baseline_no_vectors | 212 | 1.0 |
| with_vectors | 212 | 1.0 |

Artifacts: `build/benchmarks/ops_timeline/kpi-20260709T003228Z-ops/`

---

## Reference material

| Topic | Link |
|-------|------|
| xplan harness | [tests/benchmarks/xplan/README.md](../../tests/benchmarks/xplan/README.md) |
| Extraction / repair | [docs/architecture/extraction-repair-pipeline.md](../architecture/extraction-repair-pipeline.md) |
| System architecture | [docs/architecture/system_architecture.md](../architecture/system_architecture.md) |
| Testing policy | [docs/developer/testing.md](../developer/testing.md) |

---

## Refresh

1. `download-beir` (or let quality worker auto-fetch).
2. Run the default plans under a common stamp.
3. Replace tables above from each `summary.md` / `REPORT.md` (default arms only).
4. Update the stamp line.
