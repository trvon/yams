# Benchmarks

This section is the public benchmark index for YAMS.

Each page answers three things:

- what workload or dataset was measured,
- what the latest published results are,
- what the historical comparison or regression signal is.

## Benchmark Docs

- [Performance Report](performance_report.md) - canonical local benchmark summary and historical comparisons.
- [Tree Builder](performance_report.md#tree-builder) - tree construction throughput.
- [WriteCoordinator](performance_report.md#writecoordinator) - ingest apply profile.
- [LongMemEval_S Retrieval Quality Baseline](longmemeval_s_baseline.md) - dataset statistics and retrieval-quality baselines.
- [Storage Backends Benchmark](storage_backends.md) - local vs R2 CLI CRUD and multi-client benchmark results.
- [Multi-Client Optimization Loop](multi_client_optimization_loop.md) - throughput/stability summary and regression interpretation.
- [Retrieval Precision Optimization](retrieval_precision_optimization.md) - ranking-quality tuning loop and summary workflow.

## Scope

- Public benchmark docs focus on benchmark data, workload context, and historical comparisons.
- Internal harness plumbing and generated outputs are intentionally omitted from public docs.
- `tested` means the path is validated in this repository.
- `supported` means the path exists but is not currently part of automated validation here.

Use tested paths as the default reference point.
