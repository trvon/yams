# Benchmarks

Daemon KPI measurements via **xplan**. Tables below are the latest organized snapshot;
per-run detail lives next to each artifact (`REPORT.md`, `ablation.md`, `metrics.csv`).

| | |
|--|--|
| Harness | [`tests/benchmarks/xplan/`](../../tests/benchmarks/xplan/README.md) |
| Plans | `tests/benchmarks/xplan/plans/*.json` |
| Artifacts | `build/benchmarks/<plan>/<stamp>/` (gitignored) |
| Micro baselines (code) | `tests/benchmarks/baseline/*.json` |

```bash
python3 tests/benchmarks/xplan/runner.py list-plans
python3 tests/benchmarks/xplan/runner.py run <plan> --build-dir build/release
# Open ARTIFACT/REPORT.md and ARTIFACT/ablation.md
python3 tests/benchmarks/xplan/runner.py compare <dirA> <dirB>
```

---

## Latest snapshot

**Stamp:** `kpi-20260709T003228Z`  
**Host:** local macOS  
**Builds:** `build/release` (ingest, repair, quality); `build/prepush-macos` (load, ops — Catch2)

### Ingest — `ingest_pipeline` (80 docs × 1 KB)

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

### Search component ablation — `search_component_ablation` (40 docs, 8 queries)

| Arm | MRR | nDCG | MAP |
|-----|----:|-----:|----:|
| baseline | 1.0 | 1.0 | 1.0 |
| no_vector | 1.0 | 1.0 | 1.0 |
| no_kg | 1.0 | 1.0 | 1.0 |
| no_topology | 1.0 | 1.0 | 1.0 |

Synthetic corpus is too easy for ranking fanout sources — use BEIR/scifact before optimizing.

Artifacts: `build/benchmarks/search_component_ablation/kpi-20260709T003228Z-search/`

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
| Ingest hot path | [docs/architecture/ingestion-hot-path-20260705.md](../architecture/ingestion-hot-path-20260705.md) |
| Ingest ablation notes | [docs/architecture/ingestion-pipeline-ablation-20260705.md](../architecture/ingestion-pipeline-ablation-20260705.md) |
| Extraction / repair | [docs/architecture/extraction-repair-pipeline.md](../architecture/extraction-repair-pipeline.md) |
| System architecture | [docs/architecture/system_architecture.md](../architecture/system_architecture.md) |
| Agent retrieval handoff | [docs/prompts/PROMPT-yams-retrieval-benchmark-handoff.md](../prompts/PROMPT-yams-retrieval-benchmark-handoff.md) |
| Testing policy | [docs/developer/testing.md](../developer/testing.md) |

---

## Refresh

1. Run plans under a common stamp prefix (see harness README).  
2. Replace tables above from each `summary.md` / `REPORT.md`.  
3. Update the stamp line.  
4. Optional: `runner.py compare <old> <new>` for deltas.  
