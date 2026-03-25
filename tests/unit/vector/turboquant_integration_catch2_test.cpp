// Catch2 integration tests for TurboQuant with VectorDatabase
// Tests TurboQuant quantization within the full vector storage pipeline
// Paper: arXiv:2504.19874 - TurboQuant
//
// NOTE: These tests focus on unit testing TurboQuant directly since
// VectorDatabase doesn't expose setIndexConfig/getIndexConfig.
// Full integration testing requires VectorDatabase to be extended with
// TurboQuant configuration support.

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_floating_point.hpp>

#include <algorithm>
#include <cmath>
#include <random>
#include <vector>
#include <yams/vector/turboquant.h>

using namespace yams::vector;

namespace {

struct TurboQuantIntegrationFixture {
    TurboQuantIntegrationFixture() : rng(42) {}

    std::mt19937 rng;

    // Generate random unit vector
    std::vector<float> generateUnitVector(size_t dim) {
        std::vector<float> v(dim);
        std::normal_distribution<float> dist(0.0f, 1.0f);
        float norm_sq = 0.0f;
        for (size_t i = 0; i < dim; ++i) {
            v[i] = dist(rng);
            norm_sq += v[i] * v[i];
        }
        float norm = std::sqrt(norm_sq);
        for (size_t i = 0; i < dim; ++i) {
            v[i] /= norm;
        }
        return v;
    }

    // L2 distance
    float l2Distance(const std::vector<float>& a, const std::vector<float>& b) {
        float sum = 0.0f;
        for (size_t i = 0; i < a.size(); ++i) {
            float diff = a[i] - b[i];
            sum += diff * diff;
        }
        return std::sqrt(sum);
    }

    // Cosine similarity
    float cosineSimilarity(const std::vector<float>& a, const std::vector<float>& b) {
        float dot = 0.0f, norm_a = 0.0f, norm_b = 0.0f;
        for (size_t i = 0; i < a.size(); ++i) {
            dot += a[i] * b[i];
            norm_a += a[i] * a[i];
            norm_b += b[i] * b[i];
        }
        return dot / (std::sqrt(norm_a) * std::sqrt(norm_b) + 1e-8f);
    }

    // MSE between vectors
    double mse(const std::vector<float>& a, const std::vector<float>& b) {
        double sum = 0.0;
        for (size_t i = 0; i < a.size(); ++i) {
            double diff = static_cast<double>(a[i]) - static_cast<double>(b[i]);
            sum += diff * diff;
        }
        return sum / a.size();
    }
};

} // namespace

// =============================================================================
// Test Group: TurboQuant Direct Usage with Different Dimensions
// =============================================================================

TEST_CASE_METHOD(TurboQuantIntegrationFixture, "TurboQuant encode/decode dimension 128",
                 "[turboquant][integration][dimensions][catch2]") {
    TurboQuantConfig config;
    config.dimension = 128;
    config.bits_per_channel = 4;
    config.seed = 42;

    TurboQuantMSE quantizer(config);

    std::vector<float> original = generateUnitVector(128);
    std::vector<uint8_t> indices = quantizer.encode(original);
    std::vector<float> reconstructed = quantizer.decode(indices);

    float dist = l2Distance(original, reconstructed);
    double mse_val = mse(original, reconstructed);

    INFO("128d: dist=" << dist << ", mse=" << mse_val);
    // L2 distance = sqrt(d * MSE), so expect ~0.57 for MSE 0.00258
    REQUIRE(dist < 0.7f);
    REQUIRE(mse_val < 0.005);
}

TEST_CASE_METHOD(TurboQuantIntegrationFixture, "TurboQuant encode/decode dimension 384",
                 "[turboquant][integration][dimensions][catch2]") {
    TurboQuantConfig config;
    config.dimension = 384;
    config.bits_per_channel = 4;
    config.seed = 42;

    TurboQuantMSE quantizer(config);

    std::vector<float> original = generateUnitVector(384);
    std::vector<uint8_t> indices = quantizer.encode(original);
    std::vector<float> reconstructed = quantizer.decode(indices);

    float dist = l2Distance(original, reconstructed);
    double mse_val = mse(original, reconstructed);

    INFO("384d: dist=" << dist << ", mse=" << mse_val);
    // L2 distance = sqrt(d * MSE), so expect ~0.61 for MSE 0.00097
    REQUIRE(dist < 0.8f);
    REQUIRE(mse_val < 0.002);
}

TEST_CASE_METHOD(TurboQuantIntegrationFixture, "TurboQuant encode/decode dimension 768",
                 "[turboquant][integration][dimensions][catch2]") {
    TurboQuantConfig config;
    config.dimension = 768;
    config.bits_per_channel = 4;
    config.seed = 42;

    TurboQuantMSE quantizer(config);

    std::vector<float> original = generateUnitVector(768);
    std::vector<uint8_t> indices = quantizer.encode(original);
    std::vector<float> reconstructed = quantizer.decode(indices);

    float dist = l2Distance(original, reconstructed);
    double mse_val = mse(original, reconstructed);

    INFO("768d: dist=" << dist << ", mse=" << mse_val);
    // L2 distance = sqrt(d * MSE), so expect ~0.64 for MSE 0.00053
    REQUIRE(dist < 0.8f);
    REQUIRE(mse_val < 0.001);
}

// =============================================================================
// Test Group: Different Bit-widths
// =============================================================================

TEST_CASE_METHOD(TurboQuantIntegrationFixture, "TurboQuant 2-bit compression",
                 "[turboquant][integration][bitwidth][catch2]") {
    TurboQuantConfig config;
    config.dimension = 384;
    config.bits_per_channel = 2;
    config.seed = 42;

    TurboQuantMSE quantizer(config);

    std::vector<float> original = generateUnitVector(384);
    std::vector<float> reconstructed = quantizer.roundTrip(original);

    float dist = l2Distance(original, reconstructed);
    double mse_val = mse(original, reconstructed);

    INFO("2-bit: dist=" << dist << ", mse=" << mse_val);
    REQUIRE(dist < 0.8f);
    REQUIRE(mse_val < 0.003);
}

TEST_CASE_METHOD(TurboQuantIntegrationFixture, "TurboQuant 3-bit compression",
                 "[turboquant][integration][bitwidth][catch2]") {
    TurboQuantConfig config;
    config.dimension = 384;
    config.bits_per_channel = 3;
    config.seed = 42;

    TurboQuantMSE quantizer(config);

    std::vector<float> original = generateUnitVector(384);
    std::vector<float> reconstructed = quantizer.roundTrip(original);

    float dist = l2Distance(original, reconstructed);
    double mse_val = mse(original, reconstructed);

    INFO("3-bit: dist=" << dist << ", mse=" << mse_val);
    REQUIRE(dist < 0.8f);
    REQUIRE(mse_val < 0.002);
}

TEST_CASE_METHOD(TurboQuantIntegrationFixture, "TurboQuant 4-bit compression",
                 "[turboquant][integration][bitwidth][catch2]") {
    TurboQuantConfig config;
    config.dimension = 384;
    config.bits_per_channel = 4;
    config.seed = 42;

    TurboQuantMSE quantizer(config);

    std::vector<float> original = generateUnitVector(384);
    std::vector<float> reconstructed = quantizer.roundTrip(original);

    float dist = l2Distance(original, reconstructed);
    double mse_val = mse(original, reconstructed);

    INFO("4-bit: dist=" << dist << ", mse=" << mse_val);
    REQUIRE(dist < 0.8f);
    REQUIRE(mse_val < 0.002);
}

// =============================================================================
// Test Group: Batch Processing
// =============================================================================

TEST_CASE_METHOD(TurboQuantIntegrationFixture, "TurboQuant batch encode/decode 100 vectors",
                 "[turboquant][integration][batch][catch2]") {
    TurboQuantConfig config;
    config.dimension = 256;
    config.bits_per_channel = 4;
    config.seed = 42;

    TurboQuantMSE quantizer(config);

    constexpr size_t kNumVectors = 100;
    std::vector<std::vector<float>> originals;
    std::vector<std::vector<uint8_t>> all_indices;
    std::vector<std::vector<float>> reconstructed;

    originals.reserve(kNumVectors);
    all_indices.reserve(kNumVectors);
    reconstructed.reserve(kNumVectors);

    for (size_t i = 0; i < kNumVectors; ++i) {
        std::vector<float> orig = generateUnitVector(256);
        originals.push_back(orig);
        all_indices.push_back(quantizer.encode(orig));
        reconstructed.push_back(quantizer.decode(all_indices.back()));
    }

    // Check all reconstructions are reasonable
    double total_mse = 0.0;
    for (size_t i = 0; i < kNumVectors; ++i) {
        total_mse += mse(originals[i], reconstructed[i]);
    }
    double avg_mse = total_mse / kNumVectors;

    INFO("Batch MSE: " << avg_mse);
    REQUIRE(avg_mse < 0.02);
}

// =============================================================================
// Test Group: Storage Size Verification
// =============================================================================

TEST_CASE_METHOD(TurboQuantIntegrationFixture, "TurboQuant storage size is correct",
                 "[turboquant][integration][storage][catch2]") {
    TurboQuantConfig config;
    config.dimension = 1536;
    config.bits_per_channel = 3;
    config.seed = 42;

    TurboQuantMSE quantizer(config);

    // 1536 dims * 3 bits = 4608 bits = 576 bytes
    REQUIRE(quantizer.storageSize() == 576);

    // Compare to 8-bit baseline: 1536 bytes
    size_t baseline_size = 1536;
    double compression_ratio = static_cast<double>(quantizer.storageSize()) / baseline_size;

    INFO("Compression ratio: " << compression_ratio);
    REQUIRE(compression_ratio > 0.35);
    REQUIRE(compression_ratio < 0.4); // ~37.5%
}

// =============================================================================
// Test Group: Search Quality (Cosine Similarity Preservation)
// =============================================================================

TEST_CASE_METHOD(TurboQuantIntegrationFixture, "TurboQuant preserves cosine similarity",
                 "[turboquant][integration][similarity][catch2]") {
    TurboQuantConfig config;
    config.dimension = 384;
    config.bits_per_channel = 4;
    config.seed = 42;

    TurboQuantMSE quantizer(config);

    // Generate two vectors
    std::vector<float> v1 = generateUnitVector(384);
    std::vector<float> v2 = generateUnitVector(384);

    // Original cosine similarity
    float orig_sim = cosineSimilarity(v1, v2);

    // Quantize and dequantize
    std::vector<float> q1 = quantizer.roundTrip(v1);
    std::vector<float> q2 = quantizer.roundTrip(v2);

    // Quantized cosine similarity
    float quant_sim = cosineSimilarity(q1, q2);

    // Similarity should be preserved
    float sim_error = std::abs(orig_sim - quant_sim);

    INFO("Original similarity: " << orig_sim);
    INFO("Quantized similarity: " << quant_sim);
    INFO("Similarity error: " << sim_error);

    REQUIRE(sim_error < 0.1f); // Within 10% error
}

// =============================================================================
// Test Group: Inner Product Mode
// =============================================================================

TEST_CASE_METHOD(TurboQuantIntegrationFixture, "TurboQuantProd basic encode/decode",
                 "[turboquant][integration][inner_product][catch2]") {
    TurboQuantConfig config;
    config.dimension = 256;
    config.bits_per_channel = 3;
    config.seed = 42;
    config.inner_product_mode = true;
    config.qjl_m = 64;

    TurboQuantProd quantizer(config);

    std::vector<float> original = generateUnitVector(256);
    auto [mse_indices, signs] = quantizer.encode(original);
    std::vector<float> reconstructed = quantizer.decode(mse_indices);

    // Should get back something reasonable
    float dist = l2Distance(original, reconstructed);
    INFO("TurboQuantProd distance: " << dist);
    REQUIRE(dist < 0.8f); // Inner product mode trades some accuracy
}

// =============================================================================
// Test Group: Edge Cases
// =============================================================================

TEST_CASE_METHOD(TurboQuantIntegrationFixture, "TurboQuant high dimensionality 3072",
                 "[turboquant][integration][edge][catch2]") {
    TurboQuantConfig config;
    config.dimension = 3072;
    config.bits_per_channel = 4;
    config.seed = 42;

    TurboQuantMSE quantizer(config);

    std::vector<float> original = generateUnitVector(3072);
    std::vector<float> reconstructed = quantizer.roundTrip(original);

    float dist = l2Distance(original, reconstructed);
    double mse_val = mse(original, reconstructed);

    INFO("3072d: dist=" << dist << ", mse=" << mse_val);
    // L2 distance = sqrt(d * MSE), expect ~0.64 for MSE 0.00013
    REQUIRE(dist < 0.8f);
    REQUIRE(mse_val < 0.0003);
}

TEST_CASE_METHOD(TurboQuantIntegrationFixture, "TurboQuant reproducibility across instances",
                 "[turboquant][integration][reproducibility][catch2]") {
    TurboQuantConfig config1;
    config1.dimension = 384;
    config1.bits_per_channel = 4;
    config1.seed = 12345;

    TurboQuantConfig config2;
    config2.dimension = 384;
    config2.bits_per_channel = 4;
    config2.seed = 12345;

    TurboQuantMSE q1(config1);
    TurboQuantMSE q2(config2);

    std::vector<float> v = generateUnitVector(384);

    std::vector<uint8_t> idx1 = q1.encode(v);
    std::vector<uint8_t> idx2 = q2.encode(v);

    REQUIRE(idx1 == idx2);

    std::vector<float> r1 = q1.decode(idx1);
    std::vector<float> r2 = q2.decode(idx2);

    float dist = l2Distance(r1, r2);
    REQUIRE(dist < 1e-6f);
}
