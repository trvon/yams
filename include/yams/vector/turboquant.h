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
 * Current limitations:
 *   - TurboQuant_Prod QJL correction term is NOT yet applied in estimation
 *   - No persistent packed storage path (backend persists float embeddings)
 *   - Variable bit-width: 1-4 bits per channel
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
 * TurboQuant Inner Product Quantizer (Two-Stage) [EXPERIMENTAL]
 *
 * WARNING: This class is experimental. The inner product estimation is approximate
 * and does NOT provide the unbiasedness guarantee described in the paper.
 *
 * Algorithm:
 *  1. Apply TurboQuant_MSE with (b-1) bits → x̃
 *  2. Compute residual: r = x - x̃
 *  3. Apply 1-bit QJL: s = sign(S · r) where S ∈ R^{m×d}
 *  4. Store: (idx_mse, s)
 *
 * Inner product estimation: Currently returns MSE-decoded dot product only.
 * The QJL residual term is encoded but NOT applied in estimation.
 *
 * Do NOT use this for production ranking/scoring until QJL correction is implemented.
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
     * @return Approximate inner product (QJL correction NOT applied)
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
