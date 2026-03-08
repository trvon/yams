# Storage Backends Benchmark

This page tracks local vs S3-compatible backend behavior for CLI-level CRUD workflows.

## Scope

- Compare backend behavior for `add`, `get`, `search`, and `delete`.
- Measure end-to-end CLI latency (includes command startup + daemon path), not storage microbenchmarks.
- Keep embeddings/model loading disabled for stable backend-path comparisons.

## Provider validation status

- **Cloudflare R2**: validated in this repository, including `temp_credentials` flow.
- **AWS S3**: supported by the same S3-compatible path, but not currently part of automated validation here.

## Latest validated run (2026-03-08)

- Harness: `tests/benchmarks/run_local_vs_r2_benchmark.py`
- Backends: `local,r2`
- Params: `iterations=1`, `files=60`, `file_size_kb=8`, `retrieve_count=20`
- R2 auth mode: `temp_credentials` (bearer token + account id bootstrap)

| Backend | Store (files/s) | Retrieve mean (ms) | Retrieve ops/s | Search mean (ms) | Search qps | Delete mean (ms) | Delete ops/s |
|---|---:|---:|---:|---:|---:|---:|---:|
| local | 40.34 | 1675.76 | 0.60 | 1519.15 | 0.66 | 1500.77 | 0.67 |
| r2 | 1.14 | 5126.69 | 0.20 | 4398.37 | 0.23 | 4190.88 | 0.24 |

## Validation notes

- Remote fallback guard stayed clean: R2 iterations wrote `0` local object files.
- CRUD gate passed (`PUT/GET/UPDATE/DELETE` semantics validated by harness checks).
- List output currently reports duplicate rows (`120` rows for `60` unique paths) for both backends; benchmark normalizes to unique paths for timing.

## Artifacts

- Canonical benchmark report: `docs/benchmarks/performance_report.md`
- Latest run artifacts: `bench_results/storage_backends/20260308T011745Z/`
- Generated local artifacts root: `bench_results/storage_backends/`

## Notes

- Benchmark harness scripts are internal and are not shipped as part of public docs.
- Storage backend validation remains a stabilization priority before expanding benchmark scope.
