# Vector baseline — 2026-06-02

Platform: Apple Silicon/macOS
Repo: `yams`
Purpose: pre-change baseline for the YAMS-used vector surface and owned third-party vector libraries.

## Notes

- Measurements use `/usr/bin/time -lp` for wall time, CPU time, and peak RSS.
- Existing `build/debug` test binaries emitted stale `*.gcda` merge warnings during some runs. Baseline numbers still completed successfully, but coverage-profiler noise should be cleaned before comparing instruction-level traces.
- Commands below are the exact baseline commands run.

## YAMS baselines

### Tokenizer unit suite

Command:

```bash
/usr/bin/time -lp build/debug/tests/catch2_tokenizer
```

Result:
- Pass: `95 assertions in 33 test cases`
- Real: `0.11s`
- User: `0.01s`
- Sys: `0.05s`
- Peak RSS: `25.4 MB`

### Vector submodule suite

Command:

```bash
/usr/bin/time -lp build/debug/tests/catch2_vector_submodule
```

Result:
- Pass: `2174 assertions in 194 test cases`
- Real: `3.96s`
- User: `2.87s`
- Sys: `0.25s`
- Peak RSS: `469.7 MB`

### Vector backend engine comparison

Command:

```bash
/usr/bin/time -lp \
  build/debug/tests/benchmarks/vector_backend_engine_compare \
  --corpus=2000 --queries=50 --dim=128 --k=10
```

Result:
- Real: `47.56s`
- User: `47.11s`
- Sys: `0.23s`
- Peak RSS: `551.8 MB`

Engine results:
- `simeon-pq`: build `3574.1 ms`, mean query `1501.0 us`, p95 `1553.4 us`, recall@10 `73.6%`
- `vec0-l2`: build `24.2 ms`, mean query output reported `867607.3 us`, p95 `2558.4 us`, recall@10 `99.8%`

Observation:
- The `vec0-l2` mean appears inconsistent with p95/p99 and likely reflects a benchmark accounting bug or a heavy first-query/warm-path artifact contaminating the mean. Follow-up profiling should confirm.

## sqlite-vec-cpp baselines

### Distance tests

Command:

```bash
/usr/bin/time -lp third_party/sqlite-vec-cpp/build_neon/tests/test_distances
```

Result:
- Pass
- Real: `0.03s`
- User: `0.00s`
- Sys: `0.00s`
- Peak RSS: `1.5 MB`

### SQLite function + vec0 integration tests

Command:

```bash
/usr/bin/time -lp third_party/sqlite-vec-cpp/build_neon/tests/test_sqlite_functions
```

Result:
- Pass: `46 / 46`
- Real: `0.43s`
- User: `0.36s`
- Sys: `0.00s`
- Peak RSS: `6.5 MB`

### vec0 ANN benchmark

Command:

```bash
/usr/bin/time -lp \
  third_party/sqlite-vec-cpp/build_bench/benchmarks/vec0_ann_benchmark \
  --rows=2000 --queries=50 --dim=128 --k=10
```

Result:
- Real: `1.00s`
- User: `1.00s`
- Sys: `0.00s`
- Peak RSS: `9.4 MB`

Benchmark output:
- Warm ANN first-query build: `985.4 ms`
- ANN warm search: `62.3 us/query`, `16042 QPS`, `100% recall@10`
- Exact full scan: `234.2 us/query`, `4270 QPS`

## simeon baselines

### Encoder test

Command:

```bash
/usr/bin/time -lp third_party/simeon/build-asan/tests/test_encoder
```

Result:
- Pass
- Real: `0.21s`
- User: `0.08s`
- Sys: `0.03s`
- Peak RSS: `32.7 MB`

### PQ test

Command:

```bash
/usr/bin/time -lp third_party/simeon/build-asan/tests/test_pq
```

Result:
- Pass
- Real: `7.01s`
- User: `6.87s`
- Sys: `0.04s`
- Peak RSS: `23.9 MB`
- Output: `PQ Recall@10 on mixture-of-Gaussians: 0.519`

### Tokenizer BPE test

Command:

```bash
/usr/bin/time -lp third_party/simeon/build-asan/tests/test_tokenizer_bpe
```

Result:
- Pass
- Real: `0.11s`
- User: `0.00s`
- Sys: `0.03s`
- Peak RSS: `20.2 MB`

### Microbenchmark sweep

Command:

```bash
/usr/bin/time -lp third_party/simeon/build-asan/benchmarks/simeon_microbench 200 512 \
  > /tmp/simeon-microbench-200.jsonl
```

Result:
- Real: `92.11s`
- User: `86.73s`
- Sys: `0.38s`
- Peak RSS: `414.2 MB`

Selected rows from `/tmp/simeon-microbench-200.jsonl`:
- Achlioptas 4096→384: `2593.268 us/doc`, `385.6 docs/s`
- Gaussian 4096→384: `3319.628 us/doc`, `301.2 docs/s`
- VerySparse 4096→384: `314.037 us/doc`, `3184.3 docs/s`

Observation:
- The currently built microbench binary did not emit FWHT rows even though the source now includes them. Rebuild before using this binary to justify YAMS default projection choices.

## Immediate follow-ups suggested by the baseline

1. Inspect the YAMS benchmark accounting for `vector_backend_engine_compare`, especially `vec0-l2` mean latency.
2. Rebuild `third_party/simeon` benchmark binaries before using them to justify FWHT-vs-Achlioptas defaults.
3. Profile `catch2_vector_submodule` and the engine comparison benchmark because both have disproportionately high RSS for their dataset sizes.
