# Archived topology plans

Superseded by the decision-grade set under `plans/`:

| Active plan | Role |
|-------------|------|
| `topology_purity_validate` | Construction/routing purity + quality, `repeats=3` |
| `topology_optimize_v2` | CC / louvain / graph sweep vs `topo_off`, `repeats=3` |
| `topology_vector_seed_ablation` | Mechanism A/B for GraphNeighbors seed ANN, `repeats=3` |

Archived files are kept for historical stamp replay only. Run explicitly:

```bash
python3 tests/benchmarks/xplan/runner.py run \
  tests/benchmarks/xplan/plans/archive/topology_expansion.json \
  --build-dir build/release
```

Do not use archived plans for promote/kill decisions without re-adding `repeats>=3`
and quality×latency primary metrics.
