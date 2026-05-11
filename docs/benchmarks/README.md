# Benchmarks

This section is the public benchmark index for YAMS.

Each page should answer three things:

- what workload or dataset was measured,
- what the latest published results are,
- how to run the benchmark locally.

## Benchmark Docs

- [Performance Report](performance_report.md) - canonical ingestion, metadata, IPC, tree-builder, and write-coordinator baseline tables plus run commands.
- [Tree & Coordinator Benchmarks](performance_report.md#tree--coordinator-benchmarks) - tree builder throughput and WriteCoordinator ingest profile.
- [LongMemEval_S Retrieval Quality Baseline](longmemeval_s_baseline.md) - dataset statistics, retrieval-quality baselines, and the benchmark command.
- [Storage Backends Benchmark](storage_backends.md) - local vs R2 CLI CRUD and multi-client benchmark results.
- [Multi-Client Optimization Loop](multi_client_optimization_loop.md) - throughput/stability runbook plus summary and regression commands.
- [Retrieval Precision Optimization](retrieval_precision_optimization.md) - ranking-quality tuning loop and summary workflow.

## Scope

- Public benchmark docs focus on benchmark data, workload context, and reproducible commands.
- Internal harness plumbing and generated artifacts are intentionally omitted from public docs.
- `tested` means the path is validated in this repository.
- `supported` means the path exists but is not currently part of automated validation here.

When in doubt, treat tested paths as the default reference point.
