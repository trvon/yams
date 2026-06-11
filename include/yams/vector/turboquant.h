#pragma once

/**
 * TurboQuant: Vector Quantization via Signed Hadamard Transform
 *
 * Paper reference: arXiv:2504.19874 (approximation)
 * Implements:
 *   - TurboQuant_MSE: Vector quantization via signed Hadamard + Lloyd-Max
 *
 * This implementation uses a signed Hadamard transform (O(d log d)) instead of
 * the paper's full random orthogonal rotation (O(d²)). This is an engineering
 * approximation that preserves the l2-norm but does not provide the same
 * theoretical distortion guarantees.
 *
 * Storage: V2.2 schema persists quantized sidecar (format, bits, seed, packed_codes).
 */

#include <yams/core/types.h>

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <random>
#include <span>
#include <vector>

// M_PI is not defined by default on Windows; define it for portability
#ifndef M_PI
#define M_PI 3.14159265358979323846
#endif

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

    // =========================================================================
    // Asymmetric Compressed-Space Scoring
    //
    // For compressed search-index traversal without full float reconstruction:
    //
    // Given a unit query vector q and a packed stored code C:
    //   q · C = (1/d) · sum_i y_q[i] · centroid[i][code_i]
    //
    // where y_q = (1/sqrt(d)) · D · H · q is the transformed query.
    // Pre-computing y_q once per search makes each candidate O(total_bits).
    // =========================================================================

    /**
     * Pre-compute transformed query for asymmetric packed scoring.
     * Call once per query, reuse the result for all candidates.
     * @param query Unit query vector (dim = config_.dimension)
     * @return Transformed query y_q = (1/sqrt(d)) · D · H · q
     */
    std::vector<float> transformQuery(const std::vector<float>& query) const;

    /**
     * @brief Allocation-free variant: transform query in-place into provided span.
     *
     * Avoids heap allocation in hot paths (e.g., compressed ANN traversal).
     * The output span must have size >= dimension.
     *
     * @param query Unit query vector (dim = config_.dimension)
     * @param output Pre-allocated span with at least dimension elements
     */
    void transformQueryInPlace(const std::vector<float>& query, std::span<float> output) const;

    /**
     * Asymmetric cosine score from packed codes (no full decode).
     *
     * Computes: (1/d) · sum_i y_q[i] · centroid[i][code_i]
     * where y_q is the transformed query from transformQuery(),
     * and centroid[i][code_i] is the Lloyd-Max centroid for coordinate i.
     *
     * This avoids full packedDecode() per candidate during HNSW traversal.
     *
     * @param transformed_query Output of transformQuery() for the current query
     * @param packed_codes Packed storage bytes (from packedEncode())
     * @return Approximate cosine similarity (unit vectors, so dot product = cosine)
     */
    float scoreFromPacked(const std::vector<float>& transformed_query,
                          const std::vector<uint8_t>& packed_codes) const;

    /**
     * Span-based asymmetric scoring — zero-allocation hot-path for HNSW traversal.
     * Reuses pre-allocated buffers instead of allocating per candidate.
     *
     * @param transformed_query Pre-computed transformed query (size = dim)
     * @param packed_codes Packed codes (size = (dim*bits+7)/8)
     * @param temp_decode_buffer Pre-allocated buffer of dim floats for decode (optional, unused in
     * scoring)
     * @return Approximate cosine similarity
     */
    float scoreFromPacked(std::span<const float> transformed_query,
                          std::span<const uint8_t> packed_codes) const;

    /**
     * Transform a packed code into the same space as transformQuery().
     *
     * For graph construction (buildMetricAligned), we need transformed vectors for all
     * corpus nodes. This avoids the packedDecode() → transformQuery() round-trip by
     * directly unpacking the bit indices and scaling by the same Hadamard+sign pipeline.
     *
     * Math: the packed code stores round(scale * D * H * v). We return scale * D * packed.
     * This is approximately the same as transformQuery(v) but skips the encode→decode detour.
     *
     * @param packed_code Packed TurboQuant bytes (size = (dim*bits+7)/8)
     * @param output Pre-allocated span with at least dimension elements
     */
    void transformPackedCode(std::span<const uint8_t> packed_code, std::span<float> output) const;

    /**
     * Fit per-coordinate scales from training vectors using Welford's algorithm.
     * Improves scoring quality when training data is representative of the corpus.
     * @param vectors Training vectors (unit sphere, all dim=config_.dimension)
     * @param sample_limit Max vectors to sample (0 = all)
     */
    void fitPerCoordScales(const std::vector<std::vector<float>>& vectors, size_t sample_limit = 0);

    /**
     * Set per-coordinate scales directly from an external source (e.g., loaded from DB).
     * Use this when scales were computed during index build and must be restored on open.
     * @param scales Vector of dim floats; must match config_.dimension
     */
    void setPerCoordScales(std::vector<float> scales) {
        if (scales.size() != config_.dimension) {
            return; // Ignore mismatched scales
        }
        per_coord_scales_ = std::move(scales);
    }

    /**
     * Per-coordinate scale getter (for debugging/benchmarking)
     */
    const std::vector<float>& perCoordScales() const { return per_coord_scales_; }

private:
    TurboQuantConfig config_;

    /** Random diagonal signs for Hadamard-based rotation (d entries of ±1) */
    std::vector<float> diagonal_signs_;

    /** Lloyd-Max centroids for current bit-width [centroid_index] */
    std::vector<float> centroids_;

    /**
     * Pre-computed decision boundaries for scalar quantization.
     * These are the shared centroid midpoints (shared for all coordinates).
     * After fitPerCoordScales, ONLY the per_coord_scales_ are updated (for scoring).
     * The quantization boundaries remain SHARED because:
     *   1. Encoding: h_i / scale[i] is quantized against shared boundaries
     *   2. The packed code stores only centroid indices, not scale[i]
     *   3. Dequantization: scale[i] * centroid[code] recovers magnitude
     *   4. Scoring: query_scale[i] * corpus_scale[i] * dot(product of centroids)
     *   5. query_scale[i] = mean |h_q[i]| from the query's own transform
     *
     * Using per-coord boundaries during encoding would create a corpus/query mismatch:
     * - Corpus uses corpus's per-coord scales (fitted during indexing)
     * - Queries use query's per-coord scales (not available at encode time)
     * This would make packed codes incompatible between indexing and search.
     */
    std::vector<float> decision_boundaries_;

    /**
     * Per-coordinate scales for LUT-based scoring.
     * Each coordinate i has scale[i] ≈ E[|h[i]|] for unit-sphere Hadamard coefficients.
     * Scoring: z[i] = scale[i] * centroid[code_i], where z is dequantized Hadamard coord.
     * This replaces the shared-scale assumption (same centroid for all coordinates).
     */
    std::vector<float> per_coord_scales_;

    /**
     * Generate random diagonal signs for Hadamard rotation
     */
    void generateDiagonalSigns();

    /**
     * Generate Lloyd-Max optimal centroids for Beta(d/2, d/2) distribution
     * Pre-computed for bits = 1..6 (static tables); bits 7-8 use k-means on N(0,1) samples.
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

public:
    // ========================================================================
    // Milestone 10: Persistent Fitted Quantizer Model
    //
    // Key insight: the corpus/query mismatch comes from encoding using fitted
    // scales (corpus) but scoring against unfitted centroid tables (shared table).
    // The fix is to persist the full fitted state (scales + per-coord centroids)
    // and use it consistently at both encoding and search time.
    // ========================================================================

    /**
     * Fitted quantizer model: all state needed for consistent encoding + scoring.
     *
     * Version 1 (legacy): only per_coord_scales stored. Per-coord centroids
     *   fall back to the shared Lloyd-Max centroid table (misaligned with fitted scales).
     * Version 2 (Milestone 10): per_coord_scales + per_coord_centroids stored.
     *   Scoring uses the same per-coord centroids that were used for encoding,
     *   eliminating the corpus/query mismatch.
     *
     * Schema:
     *   version: uint32_t (2 = full fitted model)
     *   bits: uint8_t
     *   per_coord_scales: dim floats (little-endian IEEE-754)
     *   per_coord_centroids: dim * num_centroids floats
     */
    struct FittedTurboQuantModel {
        static constexpr uint32_t kVersion = 2;
        uint32_t version = 0;
        uint8_t bits = 0;
        std::vector<float> per_coord_scales;    // dim floats
        std::vector<float> per_coord_centroids; // dim * num_centroids floats
        bool empty() const { return per_coord_scales.empty(); }
    };

    /**
     * Fit per-coordinate centroids from training data via k-means.
     * This is the core of Milestone 10: each Hadamard coordinate gets its own
     * Lloyd-Max centroid table fitted to that coordinate's distribution.
     *
     * Run AFTER fitPerCoordScales (or call fit() which does both).
     * @param vectors Training vectors (unit sphere)
     * @param max_iters Maximum k-means iterations per coordinate (default: 25)
     */
    void fitPerCoordCentroids(const std::vector<std::vector<float>>& vectors,
                              size_t max_iters = 25);

    /**
     * Fit both scales AND per-coord centroids in one call.
     * This is the recommended entry point for Milestone 10.
     * @param vectors Training vectors (unit sphere)
     * @param max_iters k-means iterations per coordinate (default: 25)
     */
    void fit(const std::vector<std::vector<float>>& vectors, size_t max_iters = 25);

    /**
     * Serialize fitted model to a flat binary blob suitable for DB storage.
     * @return Binary blob (empty on error)
     */
    std::vector<uint8_t> saveFittedModel() const;

    /**
     * Load fitted model from a binary blob produced by saveFittedModel().
     * @return true on success
     */
    bool loadFittedModel(std::span<const uint8_t> blob);

    /**
     * Get the fitted model currently in use.
     */
    FittedTurboQuantModel fittedModel() const;

private:
    /**
     * Per-coordinate fitted centroid tables.
     * Indexed as [coord * num_centroids + centroid_idx].
     * Only populated after fitPerCoordCentroids() is called.
     * When empty: encode/scoreFromPacked fall back to shared centroids_ table.
     * Milestone 11: encode() now uses per_coord_centroids_ when available,
     *   closing the corpus/query mismatch that blocked higher bit-depth scoring.
     */
    std::vector<float> per_coord_centroids_;
}; // end TurboQuantMSE

/**
 * Lloyd-Max optimal centroids for Beta(α, α) with α = d/2
 * Indexed by [bits-1][centroid_index]
 *
 * Values from paper's pre-computed tables + midpoint-rule approximation for higher bits:
 *   b=1: 2 centroids
 *   b=2: 4 centroids
 *   b=3: 8 centroids
 *   b=4: 16 centroids
 *   b=5: 32 centroids (midpoint-rule approximation for N(0,1))
 *   b=6: 64 centroids (midpoint-rule approximation for N(0,1))
 *   b=7–8: generated via k-means at runtime (static tables too large to embed)
 */
extern const std::vector<std::vector<std::vector<float>>>& getLloydMaxCentroids();

/**
 * Decision boundaries for Lloyd-Max quantizers
 * Indexed by [bits-1][boundary_index]
 */
extern const std::vector<std::vector<float>>& getLloydMaxBoundaries();

/**
 * Beta distribution sampler (for testing)
 */
float sampleBeta(std::mt19937& rng, float alpha, float beta);

namespace vector_utils {

inline std::vector<uint8_t> packedQuantizeVector(const std::vector<float>& vector,
                                                 TurboQuantMSE* quantizer) {
    return quantizer ? quantizer->packedEncode(vector) : std::vector<uint8_t>{};
}

inline std::vector<float> packedDequantizeVector(const std::vector<uint8_t>& packed, size_t /*dim*/,
                                                 TurboQuantMSE* quantizer) {
    return quantizer ? quantizer->packedDecode(packed) : std::vector<float>{};
}

inline std::vector<float> transformQueryForScoring(const std::vector<float>& query,
                                                   TurboQuantMSE* quantizer) {
    return quantizer ? quantizer->transformQuery(query) : std::vector<float>{};
}

inline float scoreCompressedCosine(const std::vector<float>& transformed_query,
                                   const std::vector<uint8_t>& packed_codes,
                                   TurboQuantMSE* quantizer) {
    return quantizer ? quantizer->scoreFromPacked(transformed_query, packed_codes) : 0.0f;
}

} // namespace vector_utils

} // namespace yams::vector
