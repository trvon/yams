# Vector hotspot profiling — 2026-06-02

Platform: Apple Silicon (macOS)
Tools: `sample` (CLI profiler), `xctrace record` (Time Profiler)
Build: `build/debug` (asan + coverage)

## Hotspots

### 1. vec0 first-query ANN index build (CRITICAL)

**Benchmark:** `vector_backend_engine_compare --corpus=500 --dim=128`
**Tool:** `sample` (8s, 1ms interval)

Call stack:
```
main
  runEngine (vec0_l2 path)
    SqliteVecBackend::searchSimilar          [104bf4900]
      Impl::searchSimilar                    [104bf520c]
        vec0SearchUnlocked                   [104de44a4]
          sqlite3_step → sqlite3VdbeExec
            vec0Filter
              vec0_run_ann_query
                vec0_ensure_ann_index        ← 99.2% of samples
                  HNSWIndex::insert_single_threaded
                    connect_neighbors_unlocked
                      prune_connections_unlocked
                        comparable_distance_nodes
                          compute_comparable_distance
                            l2_squared_distance (NEON SIMD)  ← inner loop
```

**Observations:**
- 4,087 out of 4,121 CPU samples (99.2%) in HNSW index construction
- The SIMD `l2_squared_distance_float_neon` kernel is the inner hot loop (~1,953 samples)
- All sample time is spent building the index; no queries are executed during the profile window
- vec0 search latency is hidden behind index-build wall time

**Recommendation:**
In production, `prepareSearchIndex()` or `buildIndex()` must be called **before** any query hits the vec0 backend. This cost is already handled in the existing API — the benchmark was profiling a cold-first-query scenario.

### 2. Simeon encoder projection comparison

**Benchmark:** `simeon_microbench 50 512`
**Tool:** `sample` (4s)

| Projection | Sketch→Output | us/doc | docs/sec | vs default |
|-----------|--------------|--------|---------|-----------|
| AchlioptasSparse | 4096→384 | 6,237 | 160 | 1.00× (current default) |
| AchlioptasSparse | 4096→256 | 3,544 | 282 | 1.76× |
| VerySparse | 4096→384 | 360 | 2,776 | **17.33×** |
| VerySparse | 4096→256 | 483 | 2,068 | 12.9× |
| DenseGaussian | 4096→384 | 5,100 | 196 | 1.22× |
| DenseGaussian | 4096→256 | 4,321 | 231 | 1.44× |

**Observations:**
- **VerySparse is ~17× faster** than AchlioptasSparse at 4096→384
- DenseGaussian is marginally faster than AchlioptasSparse at 4096→384
- FWHT (YAMS default via ConfigResolver) is not in the currently built microbench binary — needs rebuild for direct comparison

**Recommendation:**
Rebuild `simeon_microbench` with FWHT rows to compare directly against VerySparse. The FJLT literature (Ailon-Chazelle 2009, Fandina et al. 2023) suggests FWHT should be comparable or faster than VerySparse for wide sketches. Verify before changing defaults.

### 3. Simeon encoder CPU profile (asan build)

The asan build doesn't resolve symbols well — most frames appear as offset+address. The measurable throughput from JSONL output (above) is more informative than the `sample` callgraph for this build.

## Commands used

```bash
# vec0 search profiling
build/debug/tests/benchmarks/vector_backend_engine_compare --corpus=500 --queries=30 --dim=128 --k=10 &
sample $PID 8 -file /tmp/vector-search-sample.txt

# simeon encoder profiling
third_party/simeon/build-asan/benchmarks/simeon_microbench 50 512 &
sample $PID 4 -file /tmp/simeon-encoder-sample.txt
```

## Next steps

1. Build release variant of benchmarks for accurate CPU profiles
2. Add FWHT rows to simeon microbench for direct comparison
3. Profile daemon startup + repair path (requires daemon harness)
4. Profile with `xctrace record --template "Time Profiler"` on release builds for Instruments-compatible traces
