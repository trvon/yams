# Benchmark scripts

Tracked scripts are focused profiling and smoke-test entry points. The xplan
experiment harness and its compatibility wrappers are optional, gitignored
local tooling. Recorded numbers remain in `docs/benchmarks/README.md`.

## Scripts in this directory

| Script | Status | Notes |
|--------|--------|-------|
| `live_benchmark_bundle.sh` | **kept** | Tracy/xctrace bundle (not multi-arm) |
| `live_mirror_suite.sh` | **kept** | steady-state mirror suite |
| `search_profile.sh` / `quick_search_profile.sh` | **kept** | ad-hoc profiling |
| `retrieval_opt_loop.sh` | **kept** | single-path opt loop |
| `benchmark_mode_manifest.sh` | **kept** | still used by profilers above |
| `cli_overhead_smoke.py` | **kept** | CLI overhead smoke |

**Removed**: `topology_expansion_wrapper.sh` (redundant alias);
`clusterability_diagnostic.py` (one-off lab dump, not on the docs path).

**Rule:** do not add new tracked multi-arm ablation shell scripts.
