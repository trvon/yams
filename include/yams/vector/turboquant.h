#pragma once

/**
 * TurboQuant: Vector Quantization via Signed Hadamard Transform
 *
 * Paper reference: arXiv:2504.19874 (approximation)
 * Implements:
 *   - TurboQuant_MSE: Vector quantization via signed Hadamard + Lloyd-Max
 *   - TurboQuant_Prod: Inner product approximation via two-stage MSE + QJL
 *
 * This implementation uses a signed Hadamard transform (O(d log d)) instead of
 * the paper's full random orthogonal rotation (O(d²)). This is an engineering
 * approximation that preserves the l2-norm but does not provide the same
 * theoretical distortion guarantees.
 *
 * Storage: V2.2 schema persists quantized sidecar (format, bits, seed, packed_codes).
 * Inner product: TurboQuantProd provides two estimators:
 *   - estimateInnerProduct(): conservative blend (default, production-safe)
 *   - estimateInnerProductFull(): full QJL correction (EXPERIMENTAL, research-only)
 */

#include <yams/core/types.h>

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <random>
#include <vector>

namespace yams::vector {

/**
 * Configuration for TurboQuant
 */
struct TurboQuantConfig {
    /** Dimension of vectors to quantize */
    size_t dimension = 384;

    /** Bits per channel (1-4) */
    uint8_t bits_per_channel = 4;

    /** Random seed for reproducibility (0 = random device) */
    uint64_t seed = 42;

    /** Enable inner product mode (two-stage with QJL) */
    bool inner_product_mode = false;

    /** Number of QJL hash functions for inner product (m parameter) */
    size_t qjl_m = 128;

    /** Reserved for future rotation matrix caching; currently unused */
    bool cache_rotation = false;
};

/**
 * TurboQuantProd encoding output
 *
 * Extended encoding that captures the residual norm squared alongside the
 * MSE-decoded indices and QJL signs. This enables the full QJL-corrected
 * inner product estimation.
 *
 * Mathematical components:
 *   - mse_indices: quantized Hadamard coefficients (from MSE stage)
 *   - qjl_signs: sign of (S·residual) for each of m random projections
 *   - residual_norm_sq: squared l2-norm of the post-MSE quantization residual
 *
 * QJL correction formula (full method):
 *   gamma_raw = matching_signs / m                            // raw agreement rate ∈ [0, 1]
 *   P(same sign) = 0.5 + (1/π)·arcsin(ρ)                  // [Feller Vol.2 Ch.III.4]
 *   E[sgn·sgn]  = 2·gamma_raw - 1
 *   arcsin(ρ)   = π·gamma_raw - π/2
 *   rho_res     = sin(π·gamma_raw - π/2) = -cos(π·gamma_raw) // correct inverse
 *   dot_estimate = dot_mse + rho_res · √(r1_norm_sq · r2_norm_sq)
 *
 *   Joint Normal Lemma derivation (see TurboQuantProd::estimateInnerProductFull):
 *     P(same) = 0.5 + (1/π)·arcsin(ρ)
 *     E[sgn·sgn] = 2·gamma_raw - 1 = (2/π)·arcsin(ρ)
 *     arcsin(ρ) = π·gamma_raw - π/2  →  rho_res = sin(π·gamma_raw - π/2) = -cos(π·gamma_raw)
 */
struct TurboQuantEncoded {
    /** MSE-decoded Hadamard indices */
    std::vector<uint8_t> mse_indices;

    /** QJL projection signs (one per m hash) */
    std::vector<int8_t> qjl_signs;

    /** Squared l2-norm of the residual (x - MSE_decode(mse_indices)) */
    float residual_norm_sq = 0.0f;

    /** Implicit conversion to std::pair for backward compatibility with
     *  existing code that uses (mse_indices, qjl_signs) pairs.
     *  The residual_norm_sq is dropped in the conversion. */
    operator std::pair<std::vector<uint8_t>, std::vector<int8_t>>() const {
        return {mse_indices, qjl_signs};
    }
};

/**
 * TurboQuant MSE Vector Quantizer
 *
 * Algorithm (Approximation of arXiv:2504.19874):
 *  1. Apply signed Hadamard transform with random diagonal signs: y = D · H · x
 *     (Note: This is an O(d log d) approximation to full random orthogonal rotation.
 *      Full rotation would be O(d²). The signed Hadamard preserves l2-norm and
 *      approximates the rotational property needed for quantization quality.)
 *  2. Scale coordinates by 1/√d (coordinates become approximately N(0,1) for unit vectors)
 *  3. Quantize each coordinate with Lloyd-Max scalar quantizer (b bits)
 *  4. Store index vector idx ∈ [2^b]^d
 *
 * Reconstruction:
 *  - Lookup centroids for each index
 *  - Undo scale, apply diagonal signs, inverse Hadamard: x̃ = H · D · y
 */
class TurboQuantMSE {
public:
    /**
     * Construct TurboQuant MSE quantizer
     * @param config Configuration (dimension, bits, seed)
     */
    explicit TurboQuantMSE(const TurboQuantConfig& config);

    /**
     * Encode a vector to compressed indices
     * @param vector Input vector (must be unit sphere, dim=config.dimension)
     * @return Compressed indices (one per coordinate)
     */
    std::vector<uint8_t> encode(const std::vector<float>& vector);

    /**
     * Decode compressed indices back to approximate vector
     * @param indices Compressed indices from encode()
     * @return Reconstructed vector (on unit sphere)
     */
    std::vector<float> decode(const std::vector<uint8_t>& indices);

    /**
     * Encode-decode round trip
     * @param vector Input vector
     * @return Reconstructed vector
     */
    std::vector<float> roundTrip(const std::vector<float>& vector) {
        return decode(encode(vector));
    }

    /**
     * Compute MSE between original and reconstructed
     */
    double computeMSE(const std::vector<float>& original, const std::vector<float>& reconstructed);

    /**
     * Get configuration
     */
    const TurboQuantConfig& config() const { return config_; }

    /**
     * Get storage size in bytes for a vector
     * @note Must match the byte length returned by packedEncode()
     */
    size_t storageSize() const {
        // Each coordinate needs bits_per_channel bits
        // Stored as packed bytes
        size_t total_bits = config_.dimension * config_.bits_per_channel;
        return (total_bits + 7) / 8; // Round up to bytes
    }

    /**
     * Encode a vector and return packed bytes for storage
     * @param vector Input vector (must be unit sphere, dim=config.dimension)
     * @return Packed bytes (bit-packed indices, length = storageSize())
     */
    std::vector<uint8_t> packedEncode(const std::vector<float>& vector);

    /**
     * Decode from packed bytes
     * @param packed Packed bytes from packedEncode() (length = storageSize())
     * @return Reconstructed vector (on unit sphere)
     */
    std::vector<float> packedDecode(const std::vector<uint8_t>& packed);

private:
    TurboQuantConfig config_;

    /** Random diagonal signs for Hadamard-based rotation (d entries of ±1) */
    std::vector<float> diagonal_signs_;

    /** Lloyd-Max centroids for current bit-width [centroid_index] */
    std::vector<float> centroids_;

    /** Pre-computed decision boundaries for scalar quantization */
    std::vector<float> decision_boundaries_;

    /**
     * Generate random diagonal signs for Hadamard rotation
     */
    void generateDiagonalSigns();

    /**
     * Generate Lloyd-Max optimal centroids for Beta(d/2, d/2) distribution
     * Pre-computed for bits = 1, 2, 3, 4
     */
    void generateCentroids();

    /**
     * Find nearest centroid index for a coordinate value
     */
    uint8_t findNearestCentroid(float value) const;

    /**
     * Apply scalar quantization to a single coordinate
     */
    uint8_t scalarQuantize(float value) const;

    /**
     * Apply scalar dequantization to get centroid value
     */
    float scalarDequantize(uint8_t index) const;
};

/**
 * TurboQuant Inner Product Quantizer (Two-Stage) [EXPERIMENTAL — QJL correction incomplete]
 *
 * WARNING: This class is experimental. The inner product estimation is approximate and
 * the QJL correction is not yet fully calibrated. Use only for research/evaluation.
 *
 * Current status:
 *   - TurboQuantMSE: stable for storage (79% size reduction vs float)
 *   - TurboQuantProd::estimateInnerProduct(): conservative blend, production-safe lower bound
 *   - TurboQuantProd::estimateInnerProductFull(): QJL correction is EXPERIMENTAL;
 *     requires arc-sine calibration and residual-norm-weighted decoding before production use
 *
 * Algorithm:
 *  1. Apply TurboQuant_MSE with (b-1) bits → x̃
 *  2. Compute residual: r = x - x̃
 *  3. Apply 1-bit QJL: s = sign(S · r) where S ∈ R^{m×d}
 *  4. Store: (idx_mse, s, ||r||²)
 *
 * Inner product estimation:
 *   - estimateInnerProduct(): conservative sign-agreement blend (default, production-safe)
 *   - estimateInnerProductFull(): full QJL correction using residual norms (EXPERIMENTAL)
 */
class TurboQuantProd {
public:
    /**
     * Construct TurboQuant Prod quantizer
     * @param config Configuration (dimension, bits, seed)
     */
    explicit TurboQuantProd(const TurboQuantConfig& config);

    /**
     * Encode a vector for inner product estimation
     * @param vector Input vector (dim=config.dimension)
     * @return TurboQuantEncoded with mse_indices, qjl_signs, and residual_norm_sq
     */
    TurboQuantEncoded encode(const std::vector<float>& vector);

    /**
     * Estimate inner product between two vectors using conservative sign-agreement blend.
     *
     * Uses sign agreement rate γ = matching_signs / m to compute a confidence-weighted
     * blend around the MSE-decoded dot product:
     *   est = dot_mse · (1 + 0.25 · (γ - 0.5) · 2)
     *
     * The blend factor (0.25) is deliberately conservative — it nudges the estimate
     * in the direction indicated by QJL sign agreement without over-correcting.
     * This avoids the pitfall of the full QJL correction (estimateInnerProductFull)
     * which performs WORSE than decoded-only on random vectors.
     *
     * @param enc1 Encoded vector 1 (from encode — implicit conversion to pair supported)
     * @param enc2 Encoded vector 2 (from encode)
     * @return Approximate inner product with conservative QJL-assisted blend
     */
    float estimateInnerProduct(const std::pair<std::vector<uint8_t>, std::vector<int8_t>>& enc1,
                               const std::pair<std::vector<uint8_t>, std::vector<int8_t>>& enc2);

    /* EXPERIMENTAL — for structured-data research only.
     * See turboquant.h TurboQuantEncoded docs for the full QJL correction formula.
     * Benchmarks show this is WORSE than the conservative blend on random vectors. */
    float estimateInnerProductFull(const TurboQuantEncoded& enc1, const TurboQuantEncoded& enc2);

    /**
     * Decode to approximate vector
     */
    std::vector<float> decode(const std::vector<uint8_t>& mse_indices);

    /**
     * Get configuration
     */
    const TurboQuantConfig& config() const { return config_; }

private:
    TurboQuantConfig config_;
    TurboQuantMSE mse_quantizer_;

    /** QJL random matrix S (m×d), entries ~ N(0,1) */
    std::vector<float> qjl_matrix_;

    /** Number of QJL hash functions */
    size_t qjl_m_;

    /**
     * Generate QJL random matrix
     */
    void generateQJLCMatrix();
};

/**
 * Lloyd-Max optimal centroids for Beta(α, α) with α = d/2
 * Indexed by [bits-1][centroid_index]
 *
 * Values from paper's pre-computed tables:
 *   b=1: 2 centroids
 *   b=2: 4 centroids
 *   b=3: 8 centroids
 *   b=4: 16 centroids
 */
extern const std::vector<std::vector<std::vector<float>>>& getLloydMaxCentroids();

/**
 * Decision boundaries for Lloyd-Max quantizers
 * Indexed by [bits-1][boundary_index]
 */
extern const std::vector<std::vector<float>>& getLloydMaxBoundaries();

/**
 * Compute theoretical MSE lower bound from Shannon distortion-rate function
 * D(B) >= (d/2π) * 2^(-2B/d)
 */
double computeDistortionLowerBound(size_t d, size_t total_bits);

/**
 * Beta distribution sampler (for testing)
 */
float sampleBeta(std::mt19937& rng, float alpha, float beta);

} // namespace yams::vector
