# Archived topology plans

The active decision-grade set under `plans/` is intentionally compact:

| Active plan | Role |
|-------------|------|
| `topology_purity_validate` | Construction/routing purity + quality, `repeats=3` |
| `topology_routing_budget_ablation` | Adaptive routed CC vs global ANN at equal recall/work budgets, `repeats=3` |

`topology_optimize_v2` and `topology_vector_seed_ablation` are retained here as
the mechanism screens that selected the active routed-CC path.

Archived files are kept for historical stamp replay only. Run explicitly:

```bash
python3 tests/benchmarks/xplan/runner.py run \
  tests/benchmarks/xplan/plans/archive/topology_expansion.json \
  --build-dir build/release
```

Do not use archived plans for promote/kill decisions without re-adding `repeats>=3`
and quality×latency primary metrics.
