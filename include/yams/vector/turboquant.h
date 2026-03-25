#pragma once

/**
 * TurboQuant: Online Vector Quantization with Near-optimal Distortion Rate
 *
 * Paper: arXiv:2504.19874
 * Implements:
 *   - TurboQuant_MSE: MSE-optimal quantization via random rotation + Lloyd-Max
 *   - TurboQuant_Prod: Inner product quantization via two-stage MSE + QJL
 *
 * Key properties:
 *   - Near-optimal distortion (within ~2.7x of theoretical lower bound)
 *   - Data-oblivious (online capable, no fitting required)
 *   - Variable bit-width: 1-4 bits per channel
 *   - Unbiased inner product estimation (TurboQuant_Prod)
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

    /** Cache rotation matrix (saves recomputation but uses memory) */
    bool cache_rotation = true;
};

/**
 * TurboQuant MSE-Optimal Vector Quantizer
 *
 * Algorithm:
 *  1. Generate random orthogonal matrix Π (d×d)
 *  2. Rotate: y = Π · x
 *  3. Quantize each coordinate with Lloyd-Max scalar quantizer (b bits)
 *  4. Store index vector idx ∈ [2^b]^d
 *
 * Reconstruction:
 *  - Lookup centroids for each index
 *  - Inverse rotate: x̃ = Π^T · ỹ
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
     */
    size_t storageSize() const {
        // Each coordinate needs bits_per_channel bits
        // Stored as packed bytes
        size_t total_bits = config_.dimension * config_.bits_per_channel;
        return (total_bits + 7) / 8; // Round up to bytes
    }

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
 * TurboQuant Inner Product Quantizer (Two-Stage)
 *
 * Algorithm:
 *  1. Apply TurboQuant_MSE with (b-1) bits → x̃
 *  2. Compute residual: r = x - x̃
 *  3. Apply 1-bit QJL: s = sign(S · r) where S ∈ R^{m×d}
 *  4. Store: (idx_mse, s)
 *
 * Inner product estimation: E[⟨s, S·x⟩] ∝ ⟨x, y⟩
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
     * @return Pair of (MSE indices, QJL signs)
     */
    std::pair<std::vector<uint8_t>, std::vector<int8_t>> encode(const std::vector<float>& vector);

    /**
     * Estimate inner product between two vectors (both encoded)
     * @param enc1 Encoded vector 1 (from encode)
     * @param enc2 Encoded vector 2 (from encode)
     * @return Estimated inner product (unbiased)
     */
    float estimateInnerProduct(const std::pair<std::vector<uint8_t>, std::vector<int8_t>>& enc1,
                               const std::pair<std::vector<uint8_t>, std::vector<int8_t>>& enc2);

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
