# Benchmark scripts (migration status)

Canonical experiment entrypoint is **xplan**:

```bash
python3 tests/benchmarks/xplan/runner.py run <plan>
python3 tests/benchmarks/xplan/runner.py list-plans
```

Harness: `tests/benchmarks/xplan/README.md`. Numbers: `docs/benchmarks/README.md`.

## Scripts in this directory

| Script | Status | Notes |
|--------|--------|-------|
| `ingestion_pipeline_ablation.sh` | **wrapper** | plan `ingest_pipeline` |
| `topology_source_ablation.sh` | **wrapper** | plan `topology_source` |
| `topology_cluster_ablation.sh` | **wrapper** | plan `topology_cluster` |
| `topology_route_scoring_ablation.sh` | **wrapper** | plan `topology_route` |
| `topology_expansion_fusion_ablation.sh` | **wrapper** | plan `topology_expansion` |
| `simeon_rerank_ab.sh` | **wrapper** | plan `simeon_rerank` |
| `live_benchmark_bundle.sh` | **kept** | Tracy/xctrace bundle (not multi-arm) |
| `live_mirror_suite.sh` | **kept** | steady-state mirror suite |
| `search_profile.sh` / `quick_search_profile.sh` | **kept** | ad-hoc profiling |
| `retrieval_opt_loop.sh` | **kept** | single-path opt loop |
| `benchmark_mode_manifest.sh` | **kept** | still used by profilers above |
| `cli_overhead_smoke.py` | **kept** | CLI overhead smoke |
| `clusterability_diagnostic.py` | **kept** | diagnostic |

**Removed**: `topology_expansion_wrapper.sh` (redundant alias).

**Rule:** do not add new multi-arm ablation shell scripts. Expansion presets and
component ablations live in xplan workers (`ablation.py`, `retrieval_quality.py`).
