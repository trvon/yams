# TurboQuant Use Cases Implementation Plan

**Objective**: Implement TurboQuant (arXiv:2504.19874) in three YAMS subsystems with isolated development, testing, and benchmarking.

> **⚠️ Implementation Status (2026-03-25)**
> - UC1 (Storage): ✅ Packed encode/decode wired through `VectorDatabase::Impl` owned `TurboQuantMSE`. Quantized sidecar path is implemented; full compressed backend persistence (search over packed codes) is not yet implemented. HNSW still uses decoded float embeddings.
> - UC2 (HNSW Search): ⚠️ **Partial / Experimental.** `TurboQuantProd` exists with two IP estimators:
>   - `estimateInnerProduct()`: conservative sign-agreement blend (production-safe, default)
>   - `estimateInnerProductFull()`: full QJL correction (EXPERIMENTAL; benchmark shows worse than conservative on random vectors)
>   Full compressed search not yet wired to HNSW. Recall benchmarks show low @1 rates (0.10–0.15) at 4-bit for high dimensions.
> - UC3 (ONNX Pipeline): ✅ Embedding generation is transparent to TurboQuant; compression is applied at `insertVectorsBatch`.
> - **TLS Plumbing**: ⚠️ Partial. `VectorDatabase` callers use the owned member. The global `thread_local` state in `src/vector/vector_index_manager.cpp:2390` and the deprecated `configureTurboQuant()` are still present (kept for backward compatibility with the `vector_utils` free functions used by benchmarks).
> - **Tests**: 163 test cases, 2243 assertions (verified locally 2026-03-25; not committed as artifact).

## Use Cases

| # | Use Case | Description | Priority |
|---|---------|-------------|----------|
| 1 | **Vector Storage / KV Cache** | Compress stored vectors to 2-4 bits/channel vs 8-bit baseline | HIGH |
| 2 | **Vector Search (HNSW)** | Use compressed vectors for faster HNSW traversal with inner product mode | MEDIUM |
| 3 | **Embedding Pipeline** | Integrate with ONNX plugin for compressed embedding generation | LOW |

---

## Use Case 1: Vector Storage / KV Cache Compression

### Description
Compress vectors when storing in sqlite-vec backend. Target: 16-32x storage reduction (8-bit → 2-4 bits).

**⚠️ Current state**: Packed TurboQuant codes are stored as a quantized sidecar alongside float embeddings. Decompression (via `packedDecode`) is applied on retrieval before HNSW search. Full compressed-backend persistence — where HNSW traverses packed codes directly without decoding — is **not yet implemented**.

### Feasibility Assessment

| Aspect | Status | Notes |
|--------|--------|-------|
| Algorithm correctness | ✅ | FWHT + Lloyd-Max quantizer implemented |
| Storage format | ⚠️ | Need to define packed byte format for 2-4 bit indices |
| Integration point | ✅ | `quantizeVector` in `vector_index_manager.cpp` exists but unused |
| Performance | ⚠️ | FWHT is 4x slower than linear; mitigated by storage savings |
| Backward compat | ⚠️ | Need migration/versioning for existing stores |

### Implementation Tasks

- [x] **1.1** Audit `sqlite_vec_backend.cpp` for storage write path
- [x] **1.2** Define TurboQuant storage format (packed indices + rotation seed) — see `QuantizedEmbedding::packed_codes`
- [x] **1.3** Wire `quantizeVector` into `VectorIndex::add()` path — via `VectorDatabase::insertVectorsBatch`
- [x] **1.4** Add dimension-specific centroid tables (128, 384, 768, 1536) — Lloyd-Max tables in `turboquant.cpp`
- [x] **1.5** Implement lazy initialization of TurboQuantMSE — `VectorDatabase::Impl::ensureTurboQuant()`
- [x] **1.6** Add config option `enable_turboquant_storage` — `VectorDatabaseConfig::enable_turboquant_storage`

### Test Updates

- [x] **1.7** Unit test: encode/decode roundtrip MSE < threshold — `turboquant_mse_catch2_test.cpp`
- [x] **1.8** Unit test: storage size matches bitwidth calculation — `turboquant_mse_catch2_test.cpp`
- [x] **1.9** Integration test: add vectors, restart, verify retrieval — `turboquant_integration_catch2_test.cpp`
- [ ] **1.10** Integration test: upgrade path from 8-bit to TurboQuant (BLOCKED: schema migration not implemented)

### Benchmark Regression Tests

```bash
# Storage efficiency benchmark
./builddir/tests/benchmarks/turboquant_bench --dims=768,1536 --bits=2,4 --vectors=10000

# Metrics to capture:
# - storage_bytes_per_vector (should be 192 bytes @ 768dim/2bit vs 6144 @ 8bit)
# - encode_latency_us_p50 (should be < 200us for practical use)
# - decode_latency_us_p50 (should be < 150us for practical use)
# - mse (should be < 0.001 for 4-bit)
```

**Baseline thresholds** (will be captured before changes):
```json
{
  "baseline_storage_768_8bit": 6144,
  "baseline_storage_1536_8bit": 12288,
  "target_storage_768_2bit": 192,
  "target_storage_1536_2bit": 384
}
```

---

## Use Case 2: Vector Search (HNSW Traversal)

### Description
Use TurboQuant's inner product mode (TurboQuant_Prod) for faster approximate nearest neighbor search.

### Feasibility Assessment

| Aspect | Status | Notes |
|--------|--------|-------|
| Inner product mode | ⚠️ | `TurboQuantProd` implemented; conservative blend in `estimateInnerProduct()`; `estimateInnerProductFull()` is EXPERIMENTAL |
| QJL projection | ⚠️ | Implemented on residuals; `estimateInnerProductFull()` uses full arcsin correction; `estimateInnerProduct()` uses conservative blend |
| HNSW integration | ❌ | HNSW still uses decoded float embeddings; compressed-space search not wired |
| Search quality | ⚠️ | Recall@1 ≈ 0.10–0.15 at 4-bit for 768-1536d (measured via `turboquant_bench`) |

### Implementation Tasks

- [ ] **2.1** Audit HNSW implementation in `vector_index_manager.cpp`
- [ ] **2.2** Validate `TurboQuantProd::estimateInnerProduct()` correctness
- [ ] **2.3** Implement compressed-space distance computation
- [ ] **2.4** Add fallback to full vectors for final re-ranking (BLOCKED: depends on TurboQuantProd)
- [ ] **2.5** Benchmark recall@k vs baseline at various bitwidths (⚠️ PARTIAL: `turboquant_bench` measures recall ≈ 0.10–0.15; low recall is expected given MSE quantization error — see docs/user_guide/turboquant.md:83)
- [ ] **2.6** Add config option `turboquant_inner_product_mode` (BLOCKED: TurboQuantProd is experimental and not production-ready)

### Test Updates

- [ ] **2.7** Unit test: inner product estimation error < 5% (BLOCKED: TurboQuantProd QJL correction not implemented)
- [ ] **2.8** Unit test: bias correction for QJL stage (BLOCKED: QJL correction not implemented)
- [ ] **2.9** Integration test: search returns same top-k (or within epsilon) (BLOCKED: depends on 2.7-2.8)
- [ ] **2.10** Integration test: 1M vector search latency < 50ms (BLOCKED: depends on TurboQuantProd)

### Benchmark Regression Tests

```bash
# Search quality benchmark
./builddir/tests/benchmarks/turboquant_bench --dims=768 --bits=4 --vectors=10000 --search-queries=100

# Metrics to capture:
# - recall_at_1 (baseline vs TurboQuant_Prod)
# - recall_at_10
# - search_latency_us_p50
# - inner_product_estimation_error
```

**Target thresholds**:
```json
{
  "min_recall_at_1": 0.85,
  "min_recall_at_10": 0.90,
  "max_inner_product_error": 0.05
}
```

---

## Use Case 3: Embedding Pipeline (ONNX Integration)

### Description
Apply TurboQuant in the ONNX plugin's embedding generation path for KV cache compression.

### Feasibility Assessment

| Aspect | Status | Notes |
|--------|--------|-------|
| ONNX plugin path | ✅ | No direct quantization in plugin; passes to vector backend |
| E2E pipeline | ✅ | Quantization happens in `VectorIndexManager` |
| Config propagation | ⚠️ | Need to pass TurboQuant config to ONNX provider |
| Latency budget | ⚠️ | ONNX inference is dominant; quantization overhead marginal |

### Implementation Tasks

- [ ] **3.1** Audit `plugins/onnx/model_provider.cpp` embedding output path
- [ ] **3.2** Add `enable_turboquant` flag to `EmbeddingConfig`
- [ ] **3.3** Wire TurboQuant config through to `VectorIndexManager`
- [ ] **3.4** Add ONNX integration test with TurboQuant enabled
- [ ] **3.5** Document ONNX + TurboQuant usage in README

### Test Updates

- [ ] **3.6** Unit test: config propagation to VectorIndexManager
- [ ] **3.7** Integration test: ONNX generates → TurboQuant stores → retrieves correctly
- [ ] **3.8** Integration test: E2E quality with real ONNX model

### Benchmark Regression Tests

```bash
# E2E embedding + storage benchmark
./builddir/tests/benchmarks/turboquant_bench --dims=768 --bits=4 --vectors=1000

# Metrics to capture:
# - total_pipeline_latency_ms (ONNX + quantization)
# - storage_bytes
# - retrieval_mse
```

---

## Build & Test Configuration

### No-ASAN Build
```bash
./setup.sh Debug --no-asan --no-tsan -Dbuild-benchmarks=true
meson compile -C builddir
```

### Benchmark Command
```bash
./builddir/tests/benchmarks/turboquant_bench --dims=128,384,768,1536 --bits=2,3,4 --vectors=1000 > benchmark_results.json
```

### Exit Status Check
```bash
./builddir/tests/benchmarks/turboquant_bench
if [ $? -eq 0 ]; then
    echo "Benchmark passed"
else
    echo "Benchmark FAILED"
    exit 1
fi
```

---

## Implementation Isolation Strategy

| Branch/Directory | Purpose |
|-----------------|---------|
| `feature/turboquant-storage` | Use Case 1 (Vector Storage) |
| `feature/turboquant-search` | Use Case 2 (HNSW Search) |
| `feature/turboquant-onnx` | Use Case 3 (ONNX Integration) |

**Merge order**: 1 → 2 → 3

---

## Risk Mitigation

| Risk | Mitigation |
|------|------------|
| Storage format breaking change | Add version field; fall back to 8-bit for old data |
| Recall degradation in search | Make TurboQuant_Prod optional; default to baseline |
| ONNX pipeline latency | Measure overhead; ensure < 5% of total |

---

## Success Criteria

| Use Case | Criteria |
|----------|----------|
| 1: Storage | 16-32x storage reduction with MSE < 0.001 |
| 2: Search | recall@1 > 0.85 vs baseline |
| 3: ONNX | E2E pipeline works; no regression in ONNX latency |

---

## Use Case 4: Benchmark Regression Testing (Old vs New Storage)

### Description
Compare baseline 8-bit linear quantization vs TurboQuant storage retrieval to detect regressions.

### Implementation Tasks

- [ ] **4.1** Create benchmark comparison script (`scripts/bench_turboquant_compare.py`)
- [ ] **4.2** Add benchmark_json output to existing `turboquant_bench` executable
- [ ] **4.3** Capture baseline metrics with current 8-bit quantization
- [ ] **4.4** Run TurboQuant benchmarks after integration
- [ ] **4.5** Generate regression report

### Benchmark Comparison Script

```python
#!/usr/bin/env python3
"""
Compare TurboQuant vs Baseline storage retrieval benchmarks.

Usage:
    ./scripts/bench_turboquant_compare.py --baseline bench_old.json --turboquant bench_new.json
"""

import json
import argparse
from typing import Dict, List, Any

def load_benchmark_json(path: str) -> Dict[str, Any]:
    with open(path) as f:
        return json.load(f)

def compare_benchmarks(baseline: Dict, turboquant: Dict) -> List[str]:
    regressions = []
    baseline_results = {r["dimension"]: r for r in baseline["results"]}
    tq_results = {r["dimension"]: r for r in turboquant["results"]}
    
    for dim in baseline_results:
        if dim not in tq_results:
            continue
        b = baseline_results[dim]
        t = tq_results[dim]
        
        # Storage reduction
        storage_ratio = b["storage_bytes"] / t["storage_bytes"]
        
        # Encode latency comparison
        encode_diff = (t["encode_latency_us_p50"] - b["baseline_encode_us_p50"]) / b["baseline_encode_us_p50"]
        
        # Decode latency comparison
        decode_diff = (t["decode_latency_us_p50"] - b["baseline_decode_us_p50"]) / b["baseline_decode_us_p50"]
        
        # Report if encode/decode regressed by >20%
        if encode_diff > 0.2:
            regressions.append(f"REGRESSION: dim={dim}, encode +{encode_diff*100:.1f}%")
        if decode_diff > 0.2:
            regressions.append(f"REGRESSION: dim={dim}, decode +{decode_diff*100:.1f}%")
    
    return regressions

def main():
    parser = argparse.ArgumentParser(description="Compare benchmark results")
    parser.add_argument("--baseline", required=True, help="Baseline benchmark JSON")
    parser.add_argument("--turboquant", required=True, help="TurboQuant benchmark JSON")
    parser.add_argument("--output", default="benchmark_comparison.json", help="Output JSON")
    parser.add_argument("--threshold", type=float, default=0.2, help="Regression threshold (default: 0.2 = 20%)")
    args = parser.parse_args()
    
    baseline = load_benchmark_json(args.baseline)
    turboquant = load_benchmark_json(args.turboquant)
    
    regressions = compare_benchmarks(baseline, turboquant)
    
    result = {
        "regressions_found": len(regressions),
        "regressions": regressions,
        "baseline_file": args.baseline,
        "turboquant_file": args.turboquant,
        "threshold": args.threshold
    }
    
    with open(args.output, "w") as f:
        json.dump(result, f, indent=2)
    
    if regressions:
        print("REGRESSIONS FOUND:")
        for r in regressions:
            print(f"  {r}")
        return 1
    else:
        print("No regressions found. Benchmark comparison passed.")
        return 0

if __name__ == "__main__":
    exit(main())
```

### Build & Run Commands

```bash
# 1. Build with no-asan
./setup.sh Debug --no-asan --no-tsan -Dbuild-benchmarks=true
meson compile -C builddir

# 2. Capture baseline (current 8-bit implementation)
./builddir/tests/benchmarks/turboquant_bench --dims=128,384,768,1536 --bits=4 --vectors=1000 > bench_baseline.json

# 3. After TurboQuant integration, capture new benchmarks
./builddir/tests/benchmarks/turboquant_bench --dims=128,384,768,1536 --bits=4 --vectors=1000 > bench_turboquant.json

# 4. Run comparison
python3 scripts/bench_turboquant_compare.py \
    --baseline bench_baseline.json \
    --turboquant bench_turboquant.json \
    --output benchmark_comparison.json

# 5. Check exit status
if [ $? -eq 0 ]; then
    echo "✓ Benchmark comparison passed"
else
    echo "✗ REGRESSIONS DETECTED"
    cat benchmark_comparison.json
    exit 1
fi
```

### Metrics to Compare

| Metric | Baseline | TurboQuant | Expected Change |
|--------|----------|------------|-----------------|
| `storage_bytes` | 6144 (768dim×8bit) | 384 (768dim×4bit) | 16x reduction |
| `encode_latency_us_p50` | ~16μs | ~90μs | 5x increase (FWHT overhead) |
| `decode_latency_us_p50` | ~11μs | ~70μs | 6x increase (FWHT overhead) |
| `mse` | N/A | < 0.001 | Acceptable distortion |
| `recall_at_1` | 1.0 (baseline) | > 0.85 | Acceptable quality |

### Regression Thresholds

```yaml
regression_thresholds:
  storage_increase_max: 1.05  # Allow 5% tolerance
  encode_latency_increase_max: 1.50  # Allow 50% increase
  decode_latency_increase_max: 1.50  # Allow 50% increase
  mse_max: 0.01  # MSE must be < 1%
  recall_at_1_min: 0.85  # Recall must be > 85%
```

### Integration with CI

```yaml
# .github/workflows/benchmarks.yml
- name: Run Storage Benchmark Comparison
  run: |
    ./setup.sh Debug --no-asan -Dbuild-benchmarks=true
    meson compile -C builddir
    
    # Capture baseline
    ./builddir/tests/benchmarks/turboquant_bench --vectors=1000 > bench_old.json
    
    # Run comparison (after integration)
    python3 scripts/bench_turboquant_compare.py \
        --baseline bench_old.json \
        --turboquant bench_new.json
```
