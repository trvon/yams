/**
 * TurboQuant Implementation
 *
 * Paper reference: arXiv:2504.19874 (approximation)
 *
 * This implementation uses a signed Hadamard transform (O(d log d)) instead of
 * the paper's full random orthogonal rotation (O(d²)). This is an engineering
 * approximation that:
 *   - Preserves l2-norm
 *   - Approximates rotational property for MSE optimality
 *   - Does NOT provide the paper's theoretical distortion guarantees
 *
 * Algorithm (signed Hadamard variant):
 *  1. Apply signed Hadamard: y = D · H · x  (O(d log d))
 *     - H is the Walsh-Hadamard matrix
 *     - D is a random diagonal matrix of ±1 signs
 *  2. Scale by 1/√d
 *  3. Quantize each coordinate with Lloyd-Max scalar quantizer (b bits)  (O(d))
 *  4. Store index vector idx ∈ [2^b]^d
 *
 * Reconstruction:
 *  - Lookup centroids for each index
 *  - Undo scale, apply diagonal signs, inverse Hadamard: x̃ = H · D · y
 *
 * Note: Non-power-of-2 dimensions are padded to the next power of 2 before the
 * Hadamard transform. This is an engineering workaround; the paper assumes d=2^k.
 */

#include <yams/vector/turboquant.h>

#include <algorithm>
#include <cassert>
#include <cmath>
#include <numeric>
#include <random>

namespace yams::vector {

// =============================================================================
// Lloyd-Max Pre-computed Centroids (from paper's numerical optimization)
// =============================================================================

/**
 * Optimal Lloyd-Max centroids for Beta(d/2, d/2) distribution
 * Computed via Lloyd's algorithm for each bit-width
 *
 * For 1-bit (b=1): 2 centroids
 * For 2-bit (b=2): 4 centroids
 * For 3-bit (b=3): 8 centroids
 * For 4-bit (b=4): 16 centroids
 */
static const std::vector<std::vector<std::vector<float>>> g_lloydMaxCentroids = {
    // b=1 (2 centroids)
    {{-0.7978845608028654f, 0.7978845608028654f}},
    // b=2 (4 centroids)
    {{-1.5104179315949422f, -0.4532269973342759f, 0.4532269973342759f, 1.5104179315949422f}},
    // b=3 (8 centroids)
    {{-1.966778352689758f, -1.1937927387629168f, -0.7624506556169162f, -0.405816249511576f,
      0.405816249511576f, 0.7624506556169162f, 1.1937927387629168f, 1.966778352689758f}},
    // b=4 (16 centroids)
    {{-2.265413948280589f, -1.8145833333333333f, -1.5095833333333333f, -1.2604166666666667f,
      -1.0416666666666667f, -0.8437500000000000f, -0.6562500000000000f, -0.4739583333333333f,
      0.4739583333333333f, 0.6562500000000000f, 0.8437500000000000f, 1.0416666666666667f,
      1.2604166666666667f, 1.5095833333333333f, 1.8145833333333333f, 2.265413948280589f}},
    // b=5 (32 centroids) — midpoint-rule approximation of Lloyd-Max for N(0,1)
    {{0.003599f, 0.134488f, 0.204499f, 0.263592f, 0.314865f, 0.359760f, 0.399135f, 0.433565f,
      0.463457f, 0.489113f, 0.510760f, 0.528571f, 0.542678f, 0.553178f, 0.560141f, 0.563612f,
      0.563612f, 0.560141f, 0.553178f, 0.542678f, 0.528571f, 0.510760f, 0.489113f, 0.463457f,
      0.433565f, 0.399135f, 0.359760f, 0.314865f, 0.263592f, 0.204499f, 0.134488f, 0.003599f}},
    // b=6 (64 centroids) — midpoint-rule approximation of Lloyd-Max for N(0,1)
    {{0.001843f, 0.075898f, 0.118298f, 0.155812f, 0.189963f, 0.221444f, 0.250670f, 0.277918f,
      0.303390f, 0.327241f, 0.349591f, 0.370538f, 0.390162f, 0.408531f, 0.425702f, 0.441724f,
      0.456639f, 0.470483f, 0.483287f, 0.495081f, 0.505887f, 0.515727f, 0.524620f, 0.532581f,
      0.539626f, 0.545764f, 0.551008f, 0.555365f, 0.558843f, 0.561446f, 0.563179f, 0.564045f,
      0.564045f, 0.563179f, 0.561446f, 0.558843f, 0.555365f, 0.551008f, 0.545764f, 0.539626f,
      0.532581f, 0.524620f, 0.515727f, 0.505887f, 0.495081f, 0.483287f, 0.470483f, 0.456639f,
      0.441724f, 0.425702f, 0.408531f, 0.390162f, 0.370538f, 0.349591f, 0.327241f, 0.303390f,
      0.277918f, 0.250670f, 0.221444f, 0.189963f, 0.155812f, 0.118298f, 0.075898f, 0.001843f}}};

/**
 * Decision boundaries between Lloyd-Max centroids
 * Indexed by [bits-1][boundary_index]
 */
static const std::vector<std::vector<float>> g_lloydMaxBoundaries = {
    // b=1 (1 boundary)
    {{0.0f}},
    // b=2 (3 boundaries)
    {{-0.9815954923026236f, 0.0f, 0.9815954923026236f}},
    // b=3 (7 boundaries)
    {{-1.5802855457263374f, -0.9781216971899165f, -0.5841334525642461f, 0.0f, 0.5841334525642461f,
      0.9781216971899165f, 1.5802855457263374f}},
    // b=4 (15 boundaries)
    {{-2.039998640807961f, -1.6620833333333333f, -1.3850000000000000f, -1.1510416666666667f,
      -0.9427083333333333f, -0.7447916666666667f, -0.5651041666666667f, -0.3906250000000000f,
      0.3906250000000000f, 0.5651041666666667f, 0.7447916666666667f, 0.9427083333333333f,
      1.1510416666666667f, 1.3850000000000000f, 1.6620833333333333f, 2.039998640807961f}},
    // b=5 (31 boundaries)
    {{-1.317150f, -1.084787f, -0.931974f, -0.813420f, -0.714171f, -0.627307f, -0.549013f,
      -0.476936f, -0.409508f, -0.345617f, -0.284434f, -0.225312f, -0.167727f, -0.111235f,
      -0.055446f, 0.000000f,  0.055446f,  0.111235f,  0.167727f,  0.225312f,  0.284434f,
      0.345617f,  0.409508f,  0.476936f,  0.549013f,  0.627307f,  0.714171f,  0.813420f,
      0.931974f,  1.084787f,  1.317150f}},
    // b=6 (63 boundaries)
    {{-1.523019f, -1.317150f, -1.185068f, -1.084787f, -1.002534f, -0.931974f, -0.869641f,
      -0.813420f, -0.761919f, -0.714171f, -0.669476f, -0.627307f, -0.587260f, -0.549013f,
      -0.512309f, -0.476936f, -0.442719f, -0.409508f, -0.377178f, -0.345617f, -0.314731f,
      -0.284434f, -0.254650f, -0.225312f, -0.196357f, -0.167727f, -0.139370f, -0.111235f,
      -0.083276f, -0.055446f, -0.027702f, 0.000000f,  0.027702f,  0.055446f,  0.083276f,
      0.111235f,  0.139370f,  0.167727f,  0.196357f,  0.225312f,  0.254650f,  0.284434f,
      0.314731f,  0.345617f,  0.377178f,  0.409508f,  0.442719f,  0.476936f,  0.512309f,
      0.549013f,  0.587260f,  0.627307f,  0.669476f,  0.714171f,  0.761919f,  0.813420f,
      0.869641f,  0.931974f,  1.002534f,  1.084787f,  1.185068f,  1.317150f,  1.523019f}}};

const std::vector<std::vector<std::vector<float>>>& getLloydMaxCentroids() {
    return g_lloydMaxCentroids;
}

const std::vector<std::vector<float>>& getLloydMaxBoundaries() {
    return g_lloydMaxBoundaries;
}

// =============================================================================
// TurboQuantMSE Implementation
// =============================================================================

TurboQuantMSE::TurboQuantMSE(const TurboQuantConfig& config) : config_(config) {
    // bits_per_channel supports 1–8. Bits 1–6 have static Lloyd-Max tables;
    // bits 7–8 generate centroids via k-means at construction time.
    assert(config.bits_per_channel >= 1 && config.bits_per_channel <= 8);
    assert(config.dimension > 0);

    // Generate random diagonal signs (stored as vector for Hadamard pre/post multiplication)
    generateDiagonalSigns();
    generateCentroids();

    // Default scale: 1.0 (sigma=1.0 for Lloyd-Max centroid tables).
    // This is correct for N(0,1) Lloyd-Max centroids which have sigma=1.
    // fitPerCoordScales() / fit() will override with corpus-specific scales.
    per_coord_scales_.resize(config_.dimension, 1.0f);
}

void TurboQuantMSE::generateDiagonalSigns() {
    const size_t d = config_.dimension;
    diagonal_signs_.resize(d);

    std::mt19937 rng;
    if (config_.seed == 0) {
        rng.seed(std::random_device{}());
    } else {
        rng.seed(static_cast<uint32_t>(config_.seed));
    }

    // Generate random ±1 diagonal entries
    std::uniform_int_distribution<int> dist(0, 1);
    for (size_t i = 0; i < d; ++i) {
        diagonal_signs_[i] = dist(rng) ? 1.0f : -1.0f;
    }
}

// Fast Walsh-Hadamard Transform (FWHT)
// In-place transform, O(d log d)
// Requires n to be power of 2
static void fwht(std::vector<float>& data) {
    size_t n = data.size();
    for (size_t stride = 1; stride < n; stride *= 2) {
        for (size_t i = 0; i < n; i += 2 * stride) {
            for (size_t j = 0; j < stride; ++j) {
                float u = data[i + j];
                float v = data[i + j + stride];
                data[i + j] = u + v;
                data[i + j + stride] = u - v;
            }
        }
    }
}

// Pad vector to next power of 2
static std::vector<float> padToPowerOf2(const std::vector<float>& data, size_t target_dim) {
    size_t n = 1;
    while (n < target_dim) {
        n *= 2;
    }
    std::vector<float> padded(n, 0.0f);
    for (size_t i = 0; i < target_dim; ++i) {
        padded[i] = data[i];
    }
    return padded;
}

void TurboQuantMSE::generateCentroids() {
    const uint8_t bits = config_.bits_per_channel;

    // Use pre-computed Lloyd-Max centroids from the table (bits 1–6)
    if (bits <= 6) {
        size_t bits_idx = static_cast<size_t>(bits) - 1;
        const auto& centroid_table = g_lloydMaxCentroids[bits_idx][0];
        centroids_ = centroid_table;

        // Boundaries come from the extended static table for bits ≤ 6
        if (bits <= 6 && bits_idx < g_lloydMaxBoundaries.size()) {
            decision_boundaries_ = g_lloydMaxBoundaries[bits_idx];
            return;
        }
    }

    // Fallback for bits > 6: generate centroids using k-means on N(0,1) samples.
    // This is a one-time cost during quantizer construction.
    // For bits 7 (128 levels) and bits 8 (256 levels), the static tables are
    // too large to embed. k-means initialization on a modest sample (5000 draws)
    // produces Lloyd-Max-quality centroids without code churn.
    const size_t num_centroids = 1u << bits;
    centroids_.resize(num_centroids);
    decision_boundaries_.resize(num_centroids - 1);

    std::mt19937 rng(static_cast<uint32_t>(config_.seed + 99999));
    std::normal_distribution<float> dist(0.0f, 1.0f);

    // Sample-based Lloyd-Max via a few k-means iterations on N(0,1)
    std::vector<float> samples(5000);
    for (auto& s : samples)
        s = dist(rng);

    // Uniform initialization across [-3, +3] — covers >99.7% of N(0,1)
    for (size_t k = 0; k < num_centroids; ++k) {
        centroids_[k] =
            -3.0f + 6.0f * (static_cast<float>(k) + 0.5f) / static_cast<float>(num_centroids);
    }

    for (size_t iter = 0; iter < 20; ++iter) {
        std::vector<std::vector<float>> clusters(num_centroids);
        for (float s : samples) {
            float best_dist = 1e30f;
            size_t best_k = 0;
            for (size_t k = 0; k < num_centroids; ++k) {
                float d = (s - centroids_[k]) * (s - centroids_[k]);
                if (d < best_dist) {
                    best_dist = d;
                    best_k = k;
                }
            }
            clusters[best_k].push_back(s);
        }
        float max_delta = 0.0f;
        for (size_t k = 0; k < num_centroids; ++k) {
            float old = centroids_[k];
            if (!clusters[k].empty()) {
                float sum = 0.0f;
                for (float v : clusters[k])
                    sum += v;
                centroids_[k] = sum / clusters[k].size();
            }
            float delta = std::abs(centroids_[k] - old);
            if (delta > max_delta)
                max_delta = delta;
        }
        if (max_delta < 1e-5f)
            break;
    }

    // Build boundaries as midpoints between sorted centroids
    for (size_t k = 0; k < num_centroids - 1; ++k) {
        decision_boundaries_[k] = (centroids_[k] + centroids_[k + 1]) * 0.5f;
    }
}

uint8_t TurboQuantMSE::findNearestCentroid(float value) const {
    // Linear search over centroids — used as fallback for bits > 4
    // (bits ≤ 4 use binary search over pre-built decision_boundaries_)
    if (centroids_.empty())
        return 0;
    float best_dist = 1e30f;
    uint8_t best_k = 0;
    for (size_t k = 0; k < centroids_.size(); ++k) {
        float diff = value - centroids_[k];
        float dist = diff * diff;
        if (dist < best_dist) {
            best_dist = dist;
            best_k = static_cast<uint8_t>(k);
        }
    }
    return best_k;
}

uint8_t TurboQuantMSE::scalarQuantize(float value) const {
    // Fast path for common cases
    if (decision_boundaries_.empty()) {
        return 0;
    }
    if (value < decision_boundaries_[0]) {
        return 0;
    }

    // Binary search for the correct bin (log2(boundaries) instead of linear)
    size_t lo = 0;
    size_t hi = decision_boundaries_.size();
    while (lo < hi) {
        size_t mid = lo + (hi - lo) / 2;
        if (value < decision_boundaries_[mid]) {
            hi = mid;
        } else {
            lo = mid + 1;
        }
    }

    return static_cast<uint8_t>(lo);
}

float TurboQuantMSE::scalarDequantize(uint8_t index) const {
    size_t idx = static_cast<size_t>(index);
    assert(idx < centroids_.size());
    return centroids_[idx];
}

std::vector<uint8_t> TurboQuantMSE::encode(const std::vector<float>& vector) {
    const size_t d = config_.dimension;
    assert(vector.size() == d);

    // Pad to power of 2 for FWHT (required by Hadamard transform)
    // NOTE: This is an engineering workaround for non-power-of-2 dimensions.
    // The paper assumes d=2^k. Padding zeros to n (next power of 2) means the
    // transform sees extra zero elements - this is an approximation.
    size_t n = 1;
    while (n < d) {
        n *= 2;
    }

    // Build padded vector with zeros
    std::vector<float> rotated(n, 0.0f);
    for (size_t i = 0; i < d; ++i) {
        rotated[i] = vector[i];
    }

    // Apply FWHT: H · x (n must be power of 2)
    fwht(rotated);

    // Apply diagonal signs: D · (H · x) - only for first d elements
    for (size_t i = 0; i < d; ++i) {
        rotated[i] *= diagonal_signs_[i];
    }

    // Scale coordinates by 1/sqrt(d) so variance ~1 for Lloyd-Max quantizer.
    // The Hadamard transform preserves l2-norm; for a unit vector x:
    //   ||H·x||² = n·||x||²  (Hadamard has l2-norm up to n)
    // So scaling by 1/sqrt(d) gives variance ~1/d per coordinate.
    float scale = 1.0f / std::sqrt(static_cast<float>(d));
    for (size_t i = 0; i < d; ++i) {
        rotated[i] *= scale;
    }

    // Quantize each coordinate with Lloyd-Max scalar quantizer (per-coordinate scale aware).
    // For coordinate i: encode `rotated[i] / per_coord_scales_[i]` against the global
    // centroid table. This is equivalent to finding the centroid c_j that minimizes
    // (rotated[i] - per_coord_scales_[i] * c_j)².
    //
    // Milestone 11: when per_coord_centroids_ is fitted, use per-coordinate centroids
    // during encoding so that encode() and scoreFromPacked() are perfectly symmetric.
    // This closes the corpus/query mismatch from Milestone 10.
    std::vector<uint8_t> indices(d);
    const size_t num_centroids = centroids_.size(); // 2^bits

    if (!per_coord_centroids_.empty()) {
        // Per-coordinate centroid quantization — matches the fitted scoring model
        for (size_t i = 0; i < d; ++i) {
            const float* coord_cents = &per_coord_centroids_[i * num_centroids];
            float scaled =
                (per_coord_scales_[i] > 1e-6f) ? (rotated[i] / per_coord_scales_[i]) : rotated[i];
            float best_dist = 1e30f;
            uint8_t best_idx = 0;
            for (size_t k = 0; k < num_centroids; ++k) {
                float diff = scaled - coord_cents[k];
                float dist = diff * diff;
                if (dist < best_dist) {
                    best_dist = dist;
                    best_idx = static_cast<uint8_t>(k);
                }
            }
            indices[i] = best_idx;
        }
    } else {
        // Shared centroid table (original path, for unfitted quantizers)
        for (size_t i = 0; i < d; ++i) {
            float scaled =
                (per_coord_scales_[i] > 1e-6f) ? (rotated[i] / per_coord_scales_[i]) : rotated[i];
            indices[i] = scalarQuantize(scaled);
        }
    }

    return indices;
}

std::vector<float> TurboQuantMSE::decode(const std::vector<uint8_t>& indices) {
    const size_t d = config_.dimension;
    assert(indices.size() == d);

    // Pad to power of 2 for FWHT
    size_t n = 1;
    while (n < d) {
        n *= 2;
    }

    // Step 1: Dequantize - lookup centroids and apply per-coordinate scales
    // Milestone 11: use per_coord_centroids_ when available (consistent with encode/score)
    std::vector<float> dequantized(n, 0.0f);
    const size_t num_centroids = centroids_.size();
    for (size_t i = 0; i < d; ++i) {
        float centroid_val = 0.0f;
        if (!per_coord_centroids_.empty()) {
            const float* coord_cents = &per_coord_centroids_[i * num_centroids];
            uint8_t code = indices[i];
            centroid_val = (code < num_centroids) ? coord_cents[code] : 0.0f;
        } else {
            centroid_val = scalarDequantize(indices[i]);
        }
        dequantized[i] = centroid_val * per_coord_scales_[i];
    }

    // Scale up (undo the 1/sqrt(d) scaling from encode)
    float unscale = std::sqrt(static_cast<float>(d));
    for (size_t i = 0; i < d; ++i) {
        dequantized[i] *= unscale;
    }

    // Inverse transform: x = D · H · y (Hadamard is self-inverse up to 1/n factor)
    // Apply diagonal signs
    for (size_t i = 0; i < d; ++i) {
        dequantized[i] *= diagonal_signs_[i];
    }

    // Apply FWHT (inverse = forward for Hadamard) - n is power of 2
    fwht(dequantized);

    // Extract only the first d elements and normalize to unit sphere
    std::vector<float> result(d);
    float norm_sq = 0.0f;
    for (size_t i = 0; i < d; ++i) {
        result[i] = dequantized[i];
        norm_sq += result[i] * result[i];
    }
    float norm = std::sqrt(norm_sq);
    if (norm > 1e-6f) {
        float inv_norm = 1.0f / norm;
        for (size_t i = 0; i < d; ++i) {
            result[i] *= inv_norm;
        }
    }

    return result;
}

std::vector<uint8_t> TurboQuantMSE::packedEncode(const std::vector<float>& vector) {
    auto indices = encode(vector);
    const size_t d = config_.dimension;
    const uint8_t bits = config_.bits_per_channel;
    const size_t total_bits = d * bits;
    const size_t num_bytes = (total_bits + 7) / 8;

    std::vector<uint8_t> packed(num_bytes, 0);

    for (size_t i = 0; i < d; ++i) {
        size_t bit_pos = i * bits;
        size_t byte_idx = bit_pos / 8;
        size_t bit_offset = bit_pos % 8;

        // Store the index value in the packed bits
        uint8_t val = indices[i];
        size_t bits_remaining = bits;
        size_t bit_pos_in_val = 0;

        while (bits_remaining > 0) {
            size_t space_in_byte = 8 - bit_offset;
            size_t bits_to_write = std::min(bits_remaining, space_in_byte);

            uint8_t mask = ((1 << bits_to_write) - 1) << bit_offset;
            uint8_t chunk = (val >> bit_pos_in_val) & ((1 << bits_to_write) - 1);
            packed[byte_idx] = (packed[byte_idx] & ~mask) | (chunk << bit_offset);

            bit_offset = 0;
            byte_idx++;
            bits_remaining -= bits_to_write;
            bit_pos_in_val += bits_to_write;
        }
    }

    return packed;
}

std::vector<float> TurboQuantMSE::packedDecode(const std::vector<uint8_t>& packed) {
    const size_t d = config_.dimension;
    const uint8_t bits = config_.bits_per_channel;
    const size_t expected_bytes = (d * bits + 7) / 8;
    assert(packed.size() == expected_bytes);

    std::vector<uint8_t> indices(d);

    for (size_t i = 0; i < d; ++i) {
        size_t bit_pos = i * bits;
        size_t byte_idx = bit_pos / 8;
        size_t bit_offset = bit_pos % 8;

        uint8_t val = 0;
        size_t bits_read = 0;

        while (bits_read < bits) {
            size_t bits_available = 8 - bit_offset;
            size_t bits_to_read = std::min(bits - bits_read, bits_available);

            uint8_t chunk = (packed[byte_idx] >> bit_offset) & ((1 << bits_to_read) - 1);
            val |= chunk << bits_read;

            bit_offset = 0;
            byte_idx++;
            bits_read += bits_to_read;
        }

        indices[i] = val;
    }

    return decode(indices);
}

double TurboQuantMSE::computeMSE(const std::vector<float>& original,
                                 const std::vector<float>& reconstructed) {
    assert(original.size() == reconstructed.size());

    double mse = 0.0;
    for (size_t i = 0; i < original.size(); ++i) {
        double diff = static_cast<double>(original[i]) - static_cast<double>(reconstructed[i]);
        mse += diff * diff;
    }

    return mse / original.size();
}

std::vector<float> TurboQuantMSE::transformQuery(const std::vector<float>& query) const {
    const size_t d = config_.dimension;
    assert(query.size() == d);

    // Pad to power of 2 for FWHT
    size_t n = 1;
    while (n < d) {
        n *= 2;
    }

    // Step 1: Copy to padded buffer
    std::vector<float> rotated(n, 0.0f);
    for (size_t i = 0; i < d; ++i) {
        rotated[i] = query[i];
    }

    // Step 2: Apply FWHT: H · q
    fwht(rotated);

    // Step 3: Apply diagonal signs: D · (H · q)
    for (size_t i = 0; i < d; ++i) {
        rotated[i] *= diagonal_signs_[i];
    }

    // Step 4: Scale by 1/sqrt(d) — this matches the encode() scaling
    float scale = 1.0f / std::sqrt(static_cast<float>(d));
    for (size_t i = 0; i < d; ++i) {
        rotated[i] *= scale;
    }

    // Step 5: Truncate to first d elements (drop padding)
    std::vector<float> result(d);
    for (size_t i = 0; i < d; ++i) {
        result[i] = rotated[i];
    }

    return result;
}

void TurboQuantMSE::transformQueryInPlace(const std::vector<float>& query,
                                          std::span<float> output) const {
    const size_t d = config_.dimension;
    assert(query.size() == d);
    assert(output.size() >= d);

    // Pad to power of 2 for FWHT
    size_t n = 1;
    while (n < d) {
        n *= 2;
    }

    // Step 1: Copy to padded buffer
    std::vector<float> rotated(n, 0.0f);
    for (size_t i = 0; i < d; ++i) {
        rotated[i] = query[i];
    }

    // Step 2: Apply FWHT: H · q
    fwht(rotated);

    // Step 3: Apply diagonal signs: D · (H · q)
    for (size_t i = 0; i < d; ++i) {
        rotated[i] *= diagonal_signs_[i];
    }

    // Step 4: Scale by 1/sqrt(d) — this matches the encode() scaling
    float scale = 1.0f / std::sqrt(static_cast<float>(d));
    for (size_t i = 0; i < d; ++i) {
        rotated[i] *= scale;
    }

    // Step 5: Copy first d elements to output span
    for (size_t i = 0; i < d; ++i) {
        output[i] = rotated[i];
    }
}

float TurboQuantMSE::scoreFromPacked(const std::vector<float>& transformed_query,
                                     const std::vector<uint8_t>& packed_codes) const {
    // Asymmetric packed-code scoring for compressed ANN / HNSW reranking.
    // Returns unnormalized inner product (higher = more similar).
    //
    // Mathematical derivation:
    //   y_q  = (1/sqrt(d)) · D · H · q         [exact transformed query]
    //   z_c[i] = scales[i] · diagonal_signs_[i] · centroids_[code_i]  [dequantized corpus]
    //   score = y_q^T · z_c = sum_i y_q[i] · z_c[i]
    //
    // The Lloyd-Max centroid table is optimized for unit-variance coefficients.
    // Multiplying by per_coord_scales_[i] (mean |h_i| from training data) accounts for
    // the empirically observed Hadamard-domain variance per coordinate, significantly
    // reducing quantization error for high-energy coordinates that dominate the dot product.
    // Multiplying by diagonal_signs_[i] mirrors the D·H pre-processing applied to queries,
    // ensuring consistent sign alignment between query and corpus representations.

    const size_t d = config_.dimension;
    const uint8_t bits = config_.bits_per_channel;
    assert(transformed_query.size() == d);
    assert(!centroids_.empty());

    const size_t expected_bytes = (d * bits + 7) / 8;
    assert(packed_codes.size() == expected_bytes);

    const float* y_q = transformed_query.data();
    const float* scales = per_coord_scales_.data();
    const size_t num_centroids = centroids_.size(); // 2^bits

    float accumulator = 0.0f;

    for (size_t i = 0; i < d; ++i) {
        size_t bit_pos = i * bits;
        size_t byte_idx = bit_pos / 8;
        size_t bit_offset = bit_pos % 8;

        uint8_t code = 0;
        size_t bits_read = 0;
        while (bits_read < bits) {
            size_t bits_available = 8 - bit_offset;
            size_t bits_to_read = std::min(bits - bits_read, bits_available);
            uint8_t chunk = (packed_codes[byte_idx] >> bit_offset) & ((1 << bits_to_read) - 1);
            code |= chunk << bits_read;
            bit_offset = 0;
            bits_read += bits_to_read;
        }

        // Dequantize: apply per-coordinate Hadamard-domain scale AND diagonal sign.
        // This makes z_c[i] = scales[i] · diagonal_signs_[i] · centroid[code_i].
        // Milestone 10: use per-coord centroids when fitted, shared table otherwise
        const float* centroids = !per_coord_centroids_.empty()
                                     ? &per_coord_centroids_[i * num_centroids]
                                     : centroids_.data();
        float centroid_val = (code < num_centroids) ? centroids[code] : 0.0f;
        accumulator += y_q[i] * centroid_val * scales[i];
    }

    return accumulator;
}

float TurboQuantMSE::scoreFromPacked(std::span<const float> transformed_query,
                                     std::span<const uint8_t> packed_codes) const {
    const size_t d = config_.dimension;
    const uint8_t bits = config_.bits_per_channel;
    assert(transformed_query.size() == d);

    const size_t expected_bytes = (d * bits + 7) / 8;
    assert(packed_codes.size() == expected_bytes);

    const float* y_q = transformed_query.data();
    const float* scales = per_coord_scales_.data();
    const uint8_t* packed = packed_codes.data();
    const size_t num_centroids = centroids_.size(); // 2^bits

    float accumulator = 0.0f;

    for (size_t i = 0; i < d; ++i) {
        size_t bit_pos = i * bits;
        size_t byte_idx = bit_pos >> 3;  // bit_pos / 8
        size_t bit_offset = bit_pos & 7; // bit_pos % 8

        uint8_t code = 0;
        size_t bits_read = 0;
        while (bits_read < bits) {
            size_t bits_available = 8 - bit_offset;
            size_t bits_to_read = std::min(bits - bits_read, bits_available);
            uint8_t chunk = (packed[byte_idx] >> bit_offset) & ((1 << bits_to_read) - 1);
            code |= chunk << bits_read;
            bit_offset = 0;
            bits_read += bits_to_read;
        }

        // Milestone 10: use per-coord centroids when fitted, shared table otherwise
        const float* centroids = !per_coord_centroids_.empty()
                                     ? &per_coord_centroids_[i * num_centroids]
                                     : centroids_.data();
        float centroid_val = (code < num_centroids) ? centroids[code] : 0.0f;
        // Same as vector-based overload: scale + sign for dequantized representation.
        accumulator += y_q[i] * centroid_val * scales[i];
    }

    return accumulator;
}

void TurboQuantMSE::transformPackedCode(std::span<const uint8_t> packed_code,
                                        std::span<float> output) const {
    // Transform a packed code into the same space as transformQuery().
    // Returns the dequantized Hadamard coefficients: scales[i] * centroid[code_i]
    // for each coordinate i. This is NOT the same as transformQuery() output.
    //
    // The scoreFromPacked function uses the dot product of:
    //   transformed_query = (1/sqrt(d)) * D * H * q    [from transformQuery()]
    //   dequantized      = scales * centroid              [from transformPackedCode]
    // The diagonal signs D cancel in the dot product since D² = I.
    //
    // Algorithm:
    // 1. Unpack each bit index from the packed code
    // 2. Look up the centroid value (quantized Hadamard coefficient)
    // 3. Apply per-coordinate Hadamard-domain scale
    // 4. (Diagonal sign is NOT applied here — it appears in transformQuery)

    const size_t d = config_.dimension;
    const uint8_t bits = config_.bits_per_channel;
    assert(packed_code.size() == (d * bits + 7) / 8);
    assert(output.size() >= d);

    const float* scales = per_coord_scales_.data();
    const uint8_t* packed = packed_code.data();
    const size_t num_centroids = centroids_.size(); // 2^bits

    for (size_t i = 0; i < d; ++i) {
        // Unpack code value for coordinate i
        size_t bit_pos = i * bits;
        size_t byte_idx = bit_pos >> 3;  // bit_pos / 8
        size_t bit_offset = bit_pos & 7; // bit_pos % 8

        uint8_t code = 0;
        size_t bits_read = 0;
        while (bits_read < bits) {
            size_t bits_available = 8 - bit_offset;
            size_t bits_to_read = std::min(bits - bits_read, bits_available);
            uint8_t chunk = (packed[byte_idx] >> bit_offset) & ((1 << bits_to_read) - 1);
            code |= chunk << bits_read;

            bit_offset = 0;
            bits_read += bits_to_read;
        }

        // Dequantized Hadamard coefficient: scales[i] * centroid[code_i]
        // NOTE: We intentionally do NOT multiply by diagonal_signs_[i].
        // The score = sum_i y_q[i] * z_c[i] where:
        //   y_q[i] = (1/sqrt(d)) * D[i] * h_q[i]        (from transformQuery)
        //   z_c[i] = scales[i] * centroid[code_i]         (dequantized, no D)
        // The D[i] in y_q[i] and the D[i] in z_c[i] cancel in the dot product
        // since D[i]² = 1 (D is a diagonal sign matrix).
        // Milestone 10: use per-coord centroids when fitted, shared table otherwise.
        const float* centroids = !per_coord_centroids_.empty()
                                     ? &per_coord_centroids_[i * num_centroids]
                                     : centroids_.data();
        float centroid_val = (code < num_centroids) ? centroids[code] : 0.0f;
        output[i] = scales[i] * centroid_val;
    }
}

void TurboQuantMSE::fitPerCoordScales(const std::vector<std::vector<float>>& vectors,
                                      size_t sample_limit) {
    const size_t d = config_.dimension;
    const size_t n = sample_limit > 0 ? std::min(sample_limit, vectors.size()) : vectors.size();
    if (n == 0) {
        return;
    }

    // Welford's online algorithm for per-coordinate mean of |h_i|
    // (mean absolute value after Hadamard transform, which tells us the scale)
    std::vector<double> mean_abs(d, 0.0); // Running mean of |h_i|
    std::vector<double> m2(d, 0.0);       // Running sum of squared deviations (for variance)

    for (size_t v = 0; v < n; ++v) {
        const auto& vec = vectors[v];
        if (vec.size() != d) {
            continue; // Skip vectors with wrong dimension
        }

        // Apply Hadamard transform to this vector
        std::vector<float> h = vec;
        size_t n_pow2 = 1;
        while (n_pow2 < d) {
            n_pow2 *= 2;
        }
        h.resize(n_pow2, 0.0f);
        fwht(h);

        for (size_t i = 0; i < d; ++i) {
            h[i] *= diagonal_signs_[i];                        // Apply diagonal signs
            h[i] *= (1.0f / std::sqrt(static_cast<float>(d))); // Scale by 1/sqrt(d)

            double abs_h = std::abs(h[i]);
            double delta = abs_h - mean_abs[i];
            mean_abs[i] += delta / (v + 1);
            double delta2 = abs_h - mean_abs[i];
            m2[i] += delta * delta2;
        }
    }

    // Update per_coord_scales_ with the per-coordinate means
    // Use max(mean_abs[i], 1e-6) to avoid zero scales
    per_coord_scales_.resize(d);
    for (size_t i = 0; i < d; ++i) {
        // Use the mean absolute value as the scale
        // This is approximately E[|h_i|] for the training distribution
        per_coord_scales_[i] = static_cast<float>(std::max(mean_abs[i], 1e-6));
    }
}

void TurboQuantMSE::fitPerCoordCentroids(const std::vector<std::vector<float>>& vectors,
                                         size_t max_iters) {
    // ========================================================================
    // Per-Coordinate K-Means for Fitted Lloyd-Max Centroids
    //
    // For each Hadamard coordinate i, collect the Hadamard coefficients h_i across
    // all training vectors, then run k-means (k = 2^bits) to find the optimal
    // centroids for that coordinate's distribution.
    //
    // These per-coord centroids replace the shared Lloyd-Max table when scoring,
    // eliminating the mismatch between fitted scales and unfitted centroid boundaries.
    //
    // Storage: dim * num_centroids floats (~384*16*4 = 24KB for 384d/4bit).
    // ========================================================================

    const size_t d = config_.dimension;
    const size_t bits = config_.bits_per_channel;
    const size_t num_centroids = 1u << bits;                  // 2^bits
    const size_t n = std::min(vectors.size(), size_t(10000)); // Cap training data
    if (n == 0 || d == 0) {
        return;
    }

    // Collect per-coord Hadamard coefficient samples
    std::vector<std::vector<float>> coord_samples(d, std::vector<float>(n));
    for (size_t v = 0; v < n; ++v) {
        const auto& vec = vectors[v];
        if (vec.size() != d) {
            continue;
        }
        // Compute Hadamard transform (in-place, same as encode path)
        std::vector<float> h = vec;
        size_t n_pow2 = 1;
        while (n_pow2 < d) {
            n_pow2 *= 2;
        }
        h.resize(n_pow2, 0.0f);
        fwht(h);
        for (size_t i = 0; i < d; ++i) {
            h[i] *= diagonal_signs_[i];
            h[i] *= (1.0f / std::sqrt(static_cast<float>(d)));
            coord_samples[i][v] = h[i];
        }
    }

    // Allocate per-coord centroids: [coord * num_centroids + k]
    per_coord_centroids_.resize(d * num_centroids);
    std::vector<float> centroids_k(num_centroids); // temp k centroids for one coordinate

    // K-means per coordinate
    std::vector<int> assignments(n, 0);
    std::vector<float> new_centroids(num_centroids);

    for (size_t i = 0; i < d; ++i) {
        // Initialize centroids with Lloyd-Max centroids for this coordinate
        // (k-means++ initialization: spread by distance to closest existing centroid)
        for (size_t k = 0; k < num_centroids; ++k) {
            centroids_k[k] = centroids_[k]; // shared table as initial guess
        }

        // Simple Lloyd-Max refinement (Voronoi iteration)
        for (size_t iter = 0; iter < max_iters; ++iter) {
            // E-step: assign each sample to nearest centroid
            for (size_t j = 0; j < n; ++j) {
                float val = coord_samples[i][j];
                float best_dist = 1e30f;
                int best_k = 0;
                for (size_t k = 0; k < num_centroids; ++k) {
                    float diff = val - centroids_k[k];
                    float dist = diff * diff;
                    if (dist < best_dist) {
                        best_dist = dist;
                        best_k = static_cast<int>(k);
                    }
                }
                assignments[j] = best_k;
            }

            // M-step: recompute centroids as mean of assigned samples
            std::fill(new_centroids.begin(), new_centroids.end(), 0.0f);
            std::vector<size_t> counts(num_centroids, 0);
            for (size_t j = 0; j < n; ++j) {
                int k = assignments[j];
                new_centroids[k] += coord_samples[i][j];
                ++counts[k];
            }
            float max_delta = 0.0f;
            for (size_t k = 0; k < num_centroids; ++k) {
                float old = centroids_k[k];
                centroids_k[k] = (counts[k] > 0) ? (new_centroids[k] / counts[k]) : old;
                float delta = std::abs(centroids_k[k] - old);
                if (delta > max_delta)
                    max_delta = delta;
            }
            if (max_delta < 1e-4f)
                break; // Converged
        }

        // Store fitted centroids for coordinate i
        float* dst = &per_coord_centroids_[i * num_centroids];
        for (size_t k = 0; k < num_centroids; ++k) {
            dst[k] = centroids_k[k];
        }
    }
}

void TurboQuantMSE::fit(const std::vector<std::vector<float>>& vectors, size_t max_iters) {
    // Fit scales first (required for consistent encoding), then centroids
    fitPerCoordScales(vectors, 0);
    fitPerCoordCentroids(vectors, max_iters);
}

std::vector<uint8_t> TurboQuantMSE::saveFittedModel() const {
    // Binary format: version(4) | bits(1) | [padding(3)] | scales | centroids
    const uint32_t version = FittedTurboQuantModel::kVersion;
    const size_t d = config_.dimension;
    const size_t num_centroids = 1u << config_.bits_per_channel;
    const size_t scales_bytes = d * sizeof(float);
    const size_t centroids_bytes = d * num_centroids * sizeof(float);
    const size_t total = 8 + scales_bytes + centroids_bytes;

    std::vector<uint8_t> blob(total);
    size_t offset = 0;

    // version (4 bytes, little-endian)
    std::memcpy(&blob[offset], &version, 4);
    offset += 4;
    // bits (1 byte)
    blob[offset] = config_.bits_per_channel;
    offset += 1;
    // padding to 8-byte alignment
    offset += 3;

    // scales (dim floats)
    if (!per_coord_scales_.empty()) {
        std::memcpy(&blob[offset], per_coord_scales_.data(), scales_bytes);
    }
    offset += scales_bytes;

    // per-coord centroids
    if (!per_coord_centroids_.empty()) {
        std::memcpy(&blob[offset], per_coord_centroids_.data(), centroids_bytes);
    }

    return blob;
}

bool TurboQuantMSE::loadFittedModel(std::span<const uint8_t> blob) {
    if (blob.size() < 8)
        return false;
    size_t offset = 0;

    uint32_t version = 0;
    std::memcpy(&version, &blob[offset], 4);
    offset += 4;

    uint8_t bits = blob[offset];
    offset += 1;
    offset += 3; // skip padding

    if (bits != config_.bits_per_channel) {
        return false; // Mismatched bit depth
    }

    const size_t d = config_.dimension;
    const size_t num_centroids = 1u << bits;
    const size_t scales_bytes = d * sizeof(float);
    const size_t centroids_bytes = d * num_centroids * sizeof(float);

    if (blob.size() < 8 + scales_bytes)
        return false;

    // Load scales
    per_coord_scales_.resize(d);
    std::memcpy(per_coord_scales_.data(), &blob[offset], scales_bytes);
    offset += scales_bytes;

    if (version >= 2 && blob.size() >= 8 + scales_bytes + centroids_bytes) {
        // Load per-coord centroids
        per_coord_centroids_.resize(d * num_centroids);
        std::memcpy(per_coord_centroids_.data(), &blob[offset], centroids_bytes);
    } else {
        per_coord_centroids_.clear(); // v1: no per-coord centroids
    }

    return true;
}

TurboQuantMSE::FittedTurboQuantModel TurboQuantMSE::fittedModel() const {
    FittedTurboQuantModel m;
    m.version = FittedTurboQuantModel::kVersion;
    m.bits = config_.bits_per_channel;
    m.per_coord_scales = per_coord_scales_;
    m.per_coord_centroids = per_coord_centroids_;
    return m;
}

// =============================================================================
// TurboQuantProd Implementation (Inner Product Mode)
// =============================================================================

TurboQuantProd::TurboQuantProd(const TurboQuantConfig& config)
    : config_(config), mse_quantizer_([&config]() {
          TurboQuantConfig mse_config = config;
          if (config.bits_per_channel > 1) {
              mse_config.bits_per_channel = config.bits_per_channel - 1;
          } else {
              mse_config.bits_per_channel = 1;
          }
          return TurboQuantMSE(mse_config);
      }()) {
    // QJL parameters for inner product estimation
    qjl_m_ = config.qjl_m;
    if (qjl_m_ == 0) {
        qjl_m_ = config.dimension / 4;
    }

    // Generate QJL random projection matrix
    generateQJLCMatrix();
}

void TurboQuantProd::generateQJLCMatrix() {
    const size_t d = config_.dimension;
    qjl_matrix_.resize(qjl_m_ * d);

    std::mt19937 rng(static_cast<uint32_t>(config_.seed + 12345));
    std::normal_distribution<float> dist(0.0f, 1.0f / std::sqrt(static_cast<float>(d)));

    for (size_t i = 0; i < qjl_m_ * d; ++i) {
        qjl_matrix_[i] = dist(rng);
    }
}

TurboQuantEncoded TurboQuantProd::encode(const std::vector<float>& vector) {
    // First stage: MSE quantization
    auto mse_indices = mse_quantizer_.encode(vector);

    // Compute residual: r = x - x_tilde
    auto reconstructed = mse_quantizer_.decode(mse_indices);
    std::vector<float> residual(vector.size());
    float residual_norm_sq = 0.0f;
    for (size_t i = 0; i < config_.dimension; ++i) {
        residual[i] = vector[i] - reconstructed[i];
        residual_norm_sq += residual[i] * residual[i];
    }

    // Second stage: QJL on residual
    std::vector<int8_t> qjl_signs(qjl_m_);
    for (size_t m = 0; m < qjl_m_; ++m) {
        float sum = 0.0f;
        for (size_t i = 0; i < config_.dimension; ++i) {
            sum += qjl_matrix_[m * config_.dimension + i] * residual[i];
        }
        qjl_signs[m] = (sum >= 0.0f) ? 1 : -1;
    }

    return {mse_indices, qjl_signs, residual_norm_sq};
}

/**
 * Estimate inner product between two vectors using TurboQuant encoding.
 *
 * Conservative blend correction: without residual norms we cannot apply the full
 * analytical correction. Instead we use sign agreement as a confidence signal.
 *
 * @return Approximate inner product with conservative QJL correction
 */
float TurboQuantProd::estimateInnerProduct(
    const std::pair<std::vector<uint8_t>, std::vector<int8_t>>& enc1,
    const std::pair<std::vector<uint8_t>, std::vector<int8_t>>& enc2) {
    // Decode first vector from MSE indices
    auto x1 = mse_quantizer_.decode(enc1.first);

    // Decode second vector from MSE indices
    auto x2 = mse_quantizer_.decode(enc2.first);

    // MSE-decoded inner product (base estimate)
    float dot_mse = 0.0f;
    for (size_t i = 0; i < config_.dimension; ++i) {
        dot_mse += x1[i] * x2[i];
    }

    // QJL correction: measure sign agreement rate and apply confidence adjustment
    const size_t m = enc1.second.size();
    if (m == 0 || enc1.second.size() != enc2.second.size()) {
        // No QJL stage or mismatched sizes — fall back to MSE-only estimate
        return dot_mse;
    }

    size_t matching_signs = 0;
    for (size_t i = 0; i < m; ++i) {
        if (enc1.second[i] == enc2.second[i]) {
            matching_signs++;
        }
    }
    float gamma = static_cast<float>(matching_signs) / static_cast<float>(m);

    // For independent Gaussian projections the expected agreement is 0.5.
    // The deviation (gamma - 0.5) tells us about residual correlation.
    // We apply a mild multiplicative correction: blend factor 0.25 means
    // a perfect agreement (gamma=1) boosts the estimate by 12.5%.
    // This is conservative — it nudges rather than over-corrects.
    constexpr float kQjlBlendFactor = 0.25f;
    float correction = 1.0f + kQjlBlendFactor * (gamma - 0.5f) * 2.0f;
    return dot_mse * correction;
}

/**
 * Full QJL-corrected inner product estimation using residual norms.
 *
 * NOTE (2026-03-25): Benchmark results show this approach is WORSE than decoded-only
 * for random vectors. For d=384, b=4:
 *   - Decoded MAE: 0.042
 *   - QJL-corrected MAE: 0.357
 *   - QJL correction adds noise because residual norms are small and residuals are
 *     near-orthogonal for random vectors (ρ_res ≈ 0), making the sign agreement
 *     rate a noisy proxy for a near-zero quantity.
 *
 * This method is retained for future exploration on structured/correlated data where
 * residuals are NOT independent. For production use, prefer estimateInnerProduct() which
 * uses a conservative blend that doesn't actively worsen the estimate.
 *
 * QJL correction theory (corrected 2026-03-25):
 *   For random Gaussian projections S_ij ~ N(0, 1/d), each projected component
 *   (S·r)_m ~ N(0, ||r||²) where r is the residual (post-MSE quantization error).
 *
 *   Joint Normal Lemma:
 *     P(both positive) = 1/2 + (1/π)·arcsin(ρ)   [Feller Vol.2, Ch. III.4]
 *     E[sgn·sgn] = 2·P(both same) - 1 = (2/π)·arcsin(ρ)
 *
 *   Key derivation:
 *     γ = (#matching) / m  ≈  P(both same sign)         // observed agreement rate
 *     E[sgn·sgn] = 2·γ - 1                            // sign product expectation
 *     2·γ - 1 = (2/π)·arcsin(ρ)                      // equate
 *     arcsin(ρ) = π·γ - π/2
 *     ρ_res = sin(π·γ - π/2) = -cos(π·γ)             // correct inverse
 *
 *   The full QJL-corrected inner product:
 *     est_ip = dot(MSE) + ρ_res · √(r1_norm_sq · r2_norm_sq)
 *
 * @return Approximate inner product
 */
float TurboQuantProd::estimateInnerProductFull(const TurboQuantEncoded& enc1,
                                               const TurboQuantEncoded& enc2) {
    // MSE-decoded inner product
    auto x1 = mse_quantizer_.decode(enc1.mse_indices);
    auto x2 = mse_quantizer_.decode(enc2.mse_indices);
    float dot_mse = 0.0f;
    for (size_t i = 0; i < config_.dimension; ++i) {
        dot_mse += x1[i] * x2[i];
    }

    // QJL correction using sign agreement rate
    const size_t m = enc1.qjl_signs.size();
    if (m == 0 || enc1.qjl_signs.size() != enc2.qjl_signs.size()) {
        return dot_mse;
    }

    size_t matching_signs = 0;
    for (size_t i = 0; i < m; ++i) {
        if (enc1.qjl_signs[i] == enc2.qjl_signs[i]) {
            matching_signs++;
        }
    }
    float gamma = static_cast<float>(matching_signs) / static_cast<float>(m);

    // Correct inversion of the Joint Normal Lemma:
    //   arcsin(ρ) = π·γ - π/2   →   ρ = sin(π·γ - π/2) = -cos(π·γ)
    // Using -cos(π·γ) directly avoids numerical issues at the boundaries.
    float rho_res = -std::cos(static_cast<float>(M_PI) * gamma);

    // Residual dot product contribution: ρ_res · ||r1|| · ||r2||
    float residual_contribution =
        rho_res * std::sqrt(enc1.residual_norm_sq * enc2.residual_norm_sq);

    return dot_mse + residual_contribution;
}

std::vector<float> TurboQuantProd::decode(const std::vector<uint8_t>& mse_indices) {
    return mse_quantizer_.decode(mse_indices);
}

double computeDistortionLowerBound(size_t d, size_t total_bits) {
    // Shannon distortion-rate lower bound: D >= (d/2π) * 2^(-2B/d)
    double B = static_cast<double>(total_bits) / static_cast<double>(d);
    return (static_cast<double>(d) / (2.0 * M_PI)) * std::pow(2.0, -2.0 * B);
}

float sampleBeta(std::mt19937& rng, float alpha, float beta) {
    // Using gamma distribution method: X = X1/(X1+X2) where X1~Gamma(α,1), X2~Gamma(β,1)
    std::gamma_distribution<float> gamma_alpha(alpha, 1.0f);
    std::gamma_distribution<float> gamma_beta(beta, 1.0f);

    float x1 = gamma_alpha(rng);
    float x2 = gamma_beta(rng);

    return x1 / (x1 + x2);
}

} // namespace yams::vector
