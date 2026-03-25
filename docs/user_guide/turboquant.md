# TurboQuant Vector Quantization

TurboQuant is a vector quantization method using a signed Hadamard transform approximation. This guide covers its use in YAMS.

**Paper reference:** [arXiv:2504.19874](https://arxiv.org/pdf/2504.19874) (approximation)

## Overview

TurboQuant improves upon naive 8-bit linear quantization by:

1. **Distribution-aware quantization**: Uses signed Hadamard transform to approximately decorrelate vector coordinates
2. **Variable bit-width**: Supports 1-4 bits per channel (vs fixed 8-bit)
3. **Inner product support**: Two-stage quantization with QJL residuals (approximation)

**Important limitations:**
- Uses signed Hadamard (O(d log d)) instead of the paper's full random orthogonal rotation (O(d²))
- Does NOT provide the paper's theoretical distortion guarantees
- `estimateInnerProduct()` uses a conservative sign-agreement blend (default, safe for production)
- `estimateInnerProductFull()` is research-only; benchmarks show it is WORSE than the conservative
  blend on random vectors (MAE 0.28-0.36 vs 0.04-0.06 for d≥384)
- V2.2 schema persists quantized sidecars alongside float embeddings (safe dual-storage)

## Configuration

Enable TurboQuant via `VectorDatabaseConfig`:

```cpp
#include <yams/vector/vector_database.h>

VectorDatabaseConfig config;
config.database_path = ":memory:";
config.embedding_dim = 384;
config.enable_turboquant_storage = true;
config.turboquant_bits = 4;      // 1-4 bits per channel
config.turboquant_seed = 42;     // Reproducibility seed

VectorDatabase db(config);
db.initialize();
```

## Bit-width Recommendations

| Bits/Channel | Compression (vs 8-bit) | Approximate MSE | Use Case |
|--------------|------------------------|-----------------|----------|
| 4 | 50% | ~0.003 | High quality, moderate compression |
| 2 | 75% | ~0.01 | Lower storage, some quality loss |

**Note:** Actual MSE varies with dimension and vector distribution. Run benchmarks for your workload.

## Performance Characteristics

**IMPORTANT:** The following numbers are from early benchmarking and may not reflect current performance. Always run benchmarks for your use case.

| Dimension | Encode (approx.) | Decode (approx.) |
|-----------|-----------------|------------------|
| 128 | ~9μs | ~8μs |
| 384 | ~35μs | ~32μs |
| 768 | ~75μs | ~70μs |

**vs Baseline 8-bit linear:** TurboQuant is currently ~3-4x slower in encode/decode due to the Hadamard transform overhead. Storage savings depend on bit-width.

## Algorithm Details

### TurboQuant_MSE (Signed Hadamard Approximation)

This implementation differs from the paper:

1. Apply signed Hadamard: y = D · H · x  (O(d log d))
   - H is the Walsh-Hadamard matrix
   - D is a random diagonal matrix of ±1 signs
2. Scale by 1/√d
3. Apply Lloyd-Max scalar quantizer per coordinate with b bits
4. Store index vector

**Difference from paper:** The paper uses full random orthogonal rotation (O(d²)). This approximation is faster but does not provide the same theoretical guarantees.

### TurboQuant_Prod (Inner Product Approximation)

Two-stage approach:
1. Apply TurboQuant_MSE with (b-1) bits → x̃
2. Compute residual: r = x - x̃
3. Apply 1-bit QJL: s = sign(S · r) where S_ij ~ N(0, 1/d)
4. Store: (idx, s, ||r||²) — indices, QJL signs, and residual norm squared

**`estimateInnerProduct()` (default, production-safe):**
Uses a conservative sign-agreement blend. γ = matching_signs / m is the observed agreement rate:
```
est = dot_mse · (1 + 0.25 · (γ - 0.5) · 2)
```
The blend factor (0.25) is deliberately conservative to avoid over-correcting. This is the recommended estimator.

**`estimateInnerProductFull()` (experimental, research-only):**
Applies the full QJL correction using residual norms:
```
gamma = matching_signs / m                                    // raw agreement rate
rho_res = -cos(π · gamma)  = sin(π·gamma - π/2)          // correct inverse [Joint Normal Lemma]
est = dot_mse + rho_res · √(||r1||² · ||r2||²)
```
**Benchmark results show this is WORSE than the conservative blend on random vectors** (MAE 0.28-0.36 vs 0.04-0.06 for d≥384). Only use for structured-data experiments.

## Current Limitations

1. **Compressed search not yet integrated**: HNSW still indexes float embeddings. TurboQuant
   sidecars are persisted (V2.2) but not yet used for search execution.
2. **estimateInnerProductFull() experimental**: Full QJL correction is research-only.
   Benchmark before using for any production ranking.
3. **Thread-safety**: TurboQuant state is thread-local. Each thread must configure independently.
4. **QJL on residuals, not original vectors**: This implementation applies QJL to the
   post-MSE quantization residual, not to the original vector directly. This differs
   from the canonical QJL inner product estimation approach.

## Benchmarks

Run the TurboQuant benchmark:

```bash
# Build
meson compile -C build/debug -j4 turboquant_bench

# Run with defaults (128,384,768 dimensions)
./build/debug/tests/benchmarks/turboquant_bench

# Custom dimensions and bit-widths
./build/debug/tests/benchmarks/turboquant_bench \
    --dims=128,384,768 \
    --bits=2,4 \
    --vectors=100
```

## Tests

Run the TurboQuant unit tests:

```bash
# Build vector tests
meson compile -C build/debug -j4 catch2_vector_submodule

# Run TurboQuant-specific tests
./build/debug/tests/catch2_vector_submodule "[turboquant]"
```

## References

- Zandieh et al., "TurboQuant: Online Vector Quantization with Near-optimal Distortion Rate", arXiv:2504.19874, 2025

- Implementation note: This code uses a signed Hadamard approximation and does NOT claim paper-faithful implementation.
