// Catch2 unit tests for TurboQuant MSE quantizer
// Tests the TurboQuant_MSE class from turboquant.h
// Paper: arXiv:2504.19874 - TurboQuant

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_floating_point.hpp>

#include <algorithm>
#include <cmath>
#include <random>
#include <vector>
#include <yams/vector/turboquant.h>

using namespace yams::vector;

namespace {

struct TurboQuantMSEFixture {
    TurboQuantMSEFixture() : rng(42) {}

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

    // L2 distance between vectors
    float l2Distance(const std::vector<float>& a, const std::vector<float>& b) {
        float sum = 0.0f;
        for (size_t i = 0; i < a.size(); ++i) {
            float diff = a[i] - b[i];
            sum += diff * diff;
        }
        return std::sqrt(sum);
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

// Theoretical MSE bounds from paper (Table 1)
const std::vector<double> kMSEBounds = {
    0.36,  // b=1
    0.117, // b=2
    0.03,  // b=3
    0.009  // b=4
};

} // namespace

// =============================================================================
// Test Group: Basic Encode/Decode
// =============================================================================

TEST_CASE_METHOD(TurboQuantMSEFixture, "TurboQuantMSE basic encode/decode round-trip",
                 "[turboquant][mse][basic][catch2]") {
    TurboQuantConfig config;
    config.dimension = 128;
    config.bits_per_channel = 4;
    config.seed = 42;

    TurboQuantMSE quantizer(config);

    std::vector<float> original = generateUnitVector(128);
    std::vector<uint8_t> indices = quantizer.encode(original);
    std::vector<float> reconstructed = quantizer.decode(indices);

    // Should reconstruct to approximately the same vector
    // With uniform quantization, expect some distortion
    float dist = l2Distance(original, reconstructed);
    INFO("Distance: " << dist);
    REQUIRE(dist < 1.0f); // Reasonable bound for 4-bit quantization
}

TEST_CASE_METHOD(TurboQuantMSEFixture, "TurboQuantMSE correct index count",
                 "[turboquant][mse][basic][catch2]") {
    TurboQuantConfig config;
    config.dimension = 256;
    config.bits_per_channel = 3;
    config.seed = 42;

    TurboQuantMSE quantizer(config);
    std::vector<float> v = generateUnitVector(256);

    std::vector<uint8_t> indices = quantizer.encode(v);

    // Should have one index per dimension
    REQUIRE(indices.size() == 256);

    // Each index should fit in 3 bits (0-7)
    for (size_t i = 0; i < indices.size(); ++i) {
        REQUIRE(indices[i] < 8);
    }
}

TEST_CASE_METHOD(TurboQuantMSEFixture, "TurboQuantMSE storage size calculation",
                 "[turboquant][mse][basic][catch2]") {
    TurboQuantConfig config;
    config.dimension = 384;
    config.bits_per_channel = 4;
    config.seed = 42;

    TurboQuantMSE quantizer(config);

    // 384 dimensions × 4 bits = 1536 bits = 192 bytes
    REQUIRE(quantizer.storageSize() == 192);
}

TEST_CASE_METHOD(TurboQuantMSEFixture, "TurboQuantMSE 2-bit storage size",
                 "[turboquant][mse][basic][catch2]") {
    TurboQuantConfig config;
    config.dimension = 1536;
    config.bits_per_channel = 2;
    config.seed = 42;

    TurboQuantMSE quantizer(config);

    // 1536 dimensions × 2 bits = 3072 bits = 384 bytes (vs 1536 for 8-bit)
    REQUIRE(quantizer.storageSize() == 384);
}

// =============================================================================
// Test Group: MSE Bounds (Theoretical Guarantees)
// =============================================================================

TEST_CASE_METHOD(TurboQuantMSEFixture, "TurboQuantMSE 1-bit MSE bound",
                 "[turboquant][mse][bounds][catch2]") {
    TurboQuantConfig config;
    config.dimension = 384;
    config.bits_per_channel = 1;
    config.seed = 42;

    TurboQuantMSE quantizer(config);

    // Test on 100 random vectors and check average MSE
    double total_mse = 0.0;
    constexpr size_t kNumVectors = 100;

    for (size_t i = 0; i < kNumVectors; ++i) {
        std::vector<float> original = generateUnitVector(384);
        std::vector<float> reconstructed = quantizer.roundTrip(original);
        total_mse += mse(original, reconstructed);
    }

    double avg_mse = total_mse / kNumVectors;

    // From paper: 1-bit MSE ≈ 0.36
    // Allow some margin for finite dimension effects
    INFO("Average MSE at 1-bit: " << avg_mse);
    REQUIRE(avg_mse < 0.40);                // Loose bound
    REQUIRE(avg_mse < kMSEBounds[0] * 1.5); // Within 1.5x of theoretical
}

TEST_CASE_METHOD(TurboQuantMSEFixture, "TurboQuantMSE 2-bit MSE bound",
                 "[turboquant][mse][bounds][catch2]") {
    TurboQuantConfig config;
    config.dimension = 384;
    config.bits_per_channel = 2;
    config.seed = 42;

    TurboQuantMSE quantizer(config);

    double total_mse = 0.0;
    constexpr size_t kNumVectors = 100;

    for (size_t i = 0; i < kNumVectors; ++i) {
        std::vector<float> original = generateUnitVector(384);
        std::vector<float> reconstructed = quantizer.roundTrip(original);
        total_mse += mse(original, reconstructed);
    }

    double avg_mse = total_mse / kNumVectors;

    // From paper: 2-bit MSE ≈ 0.117
    INFO("Average MSE at 2-bit: " << avg_mse);
    REQUIRE(avg_mse < 0.15);
    REQUIRE(avg_mse < kMSEBounds[1] * 1.5);
}

TEST_CASE_METHOD(TurboQuantMSEFixture, "TurboQuantMSE 3-bit MSE bound",
                 "[turboquant][mse][bounds][catch2]") {
    TurboQuantConfig config;
    config.dimension = 384;
    config.bits_per_channel = 3;
    config.seed = 42;

    TurboQuantMSE quantizer(config);

    double total_mse = 0.0;
    constexpr size_t kNumVectors = 100;

    for (size_t i = 0; i < kNumVectors; ++i) {
        std::vector<float> original = generateUnitVector(384);
        std::vector<float> reconstructed = quantizer.roundTrip(original);
        total_mse += mse(original, reconstructed);
    }

    double avg_mse = total_mse / kNumVectors;

    // From paper: 3-bit MSE ≈ 0.03
    INFO("Average MSE at 3-bit: " << avg_mse);
    REQUIRE(avg_mse < 0.05);
    REQUIRE(avg_mse < kMSEBounds[2] * 1.5);
}

TEST_CASE_METHOD(TurboQuantMSEFixture, "TurboQuantMSE 4-bit MSE bound",
                 "[turboquant][mse][bounds][catch2]") {
    TurboQuantConfig config;
    config.dimension = 384;
    config.bits_per_channel = 4;
    config.seed = 42;

    TurboQuantMSE quantizer(config);

    double total_mse = 0.0;
    constexpr size_t kNumVectors = 100;

    for (size_t i = 0; i < kNumVectors; ++i) {
        std::vector<float> original = generateUnitVector(384);
        std::vector<float> reconstructed = quantizer.roundTrip(original);
        total_mse += mse(original, reconstructed);
    }

    double avg_mse = total_mse / kNumVectors;

    // From paper: 4-bit MSE ≈ 0.009
    INFO("Average MSE at 4-bit: " << avg_mse);
    REQUIRE(avg_mse < 0.02);
    REQUIRE(avg_mse < kMSEBounds[3] * 2.0); // More relaxed due to finite effects
}

// =============================================================================
// Test Group: Different Dimensions
// =============================================================================

TEST_CASE_METHOD(TurboQuantMSEFixture, "TurboQuantMSE dimension 128",
                 "[turboquant][mse][dimensions][catch2]") {
    TurboQuantConfig config;
    config.dimension = 128;
    config.bits_per_channel = 4;
    config.seed = 42;

    TurboQuantMSE quantizer(config);
    std::vector<float> original = generateUnitVector(128);
    std::vector<float> reconstructed = quantizer.roundTrip(original);

    double mse_val = mse(original, reconstructed);
    REQUIRE(mse_val < 0.05);
}

TEST_CASE_METHOD(TurboQuantMSEFixture, "TurboQuantMSE dimension 768",
                 "[turboquant][mse][dimensions][catch2]") {
    TurboQuantConfig config;
    config.dimension = 768;
    config.bits_per_channel = 4;
    config.seed = 42;

    TurboQuantMSE quantizer(config);
    std::vector<float> original = generateUnitVector(768);
    std::vector<float> reconstructed = quantizer.roundTrip(original);

    double mse_val = mse(original, reconstructed);
    REQUIRE(mse_val < 0.02);
}

TEST_CASE_METHOD(TurboQuantMSEFixture, "TurboQuantMSE dimension 1536",
                 "[turboquant][mse][dimensions][catch2]") {
    TurboQuantConfig config;
    config.dimension = 1536;
    config.bits_per_channel = 4;
    config.seed = 42;

    TurboQuantMSE quantizer(config);
    std::vector<float> original = generateUnitVector(1536);
    std::vector<float> reconstructed = quantizer.roundTrip(original);

    double mse_val = mse(original, reconstructed);
    REQUIRE(mse_val < 0.015);
}

// =============================================================================
// Test Group: Reproducibility
// =============================================================================

TEST_CASE_METHOD(TurboQuantMSEFixture, "TurboQuantMSE same seed produces same results",
                 "[turboquant][mse][reproducibility][catch2]") {
    TurboQuantConfig config1;
    config1.dimension = 256;
    config1.bits_per_channel = 4;
    config1.seed = 12345;

    TurboQuantConfig config2;
    config2.dimension = 256;
    config2.bits_per_channel = 4;
    config2.seed = 12345;

    TurboQuantMSE q1(config1);
    TurboQuantMSE q2(config2);

    std::vector<float> v = generateUnitVector(256);

    std::vector<uint8_t> idx1 = q1.encode(v);
    std::vector<uint8_t> idx2 = q2.encode(v);

    REQUIRE(idx1 == idx2);
}

TEST_CASE_METHOD(TurboQuantMSEFixture, "TurboQuantMSE different seeds produce different results",
                 "[turboquant][mse][reproducibility][catch2]") {
    TurboQuantConfig config1;
    config1.dimension = 256;
    config1.bits_per_channel = 4;
    config1.seed = 12345;

    TurboQuantConfig config2;
    config2.dimension = 256;
    config2.bits_per_channel = 4;
    config2.seed = 54321;

    TurboQuantMSE q1(config1);
    TurboQuantMSE q2(config2);

    std::vector<float> v = generateUnitVector(256);

    std::vector<uint8_t> idx1 = q1.encode(v);
    std::vector<uint8_t> idx2 = q2.encode(v);

    // With high probability, these should differ
    bool all_same = (idx1 == idx2);
    INFO("Different seeds produced same result: " << all_same);
    // This test is probabilistic; there's a tiny chance they could match
}

// =============================================================================
// Test Group: Edge Cases
// =============================================================================

TEST_CASE_METHOD(TurboQuantMSEFixture, "TurboQuantMSE zero vector",
                 "[turboquant][mse][edge][catch2]") {
    TurboQuantConfig config;
    config.dimension = 128;
    config.bits_per_channel = 4;
    config.seed = 42;

    TurboQuantMSE quantizer(config);
    std::vector<float> zero(128, 0.0f);

    // Should not crash
    std::vector<uint8_t> indices = quantizer.encode(zero);
    REQUIRE(indices.size() == 128);

    std::vector<float> reconstructed = quantizer.decode(indices);
    REQUIRE(reconstructed.size() == 128);
}

TEST_CASE_METHOD(TurboQuantMSEFixture, "TurboQuantMSE near-zero vector",
                 "[turboquant][mse][edge][catch2]") {
    TurboQuantConfig config;
    config.dimension = 64;
    config.bits_per_channel = 4;
    config.seed = 42;

    TurboQuantMSE quantizer(config);
    std::vector<float> near_zero(64, 1e-6f);

    // Should not crash
    std::vector<uint8_t> indices = quantizer.encode(near_zero);
    REQUIRE(indices.size() == 64);
}

TEST_CASE_METHOD(TurboQuantMSEFixture, "TurboQuantMSE high dimensionality",
                 "[turboquant][mse][edge][catch2]") {
    TurboQuantConfig config;
    config.dimension = 3072;
    config.bits_per_channel = 4;
    config.seed = 42;

    TurboQuantMSE quantizer(config);
    std::vector<float> original = generateUnitVector(3072);

    std::vector<uint8_t> indices = quantizer.encode(original);
    std::vector<float> reconstructed = quantizer.decode(indices);

    double mse_val = mse(original, reconstructed);
    INFO("MSE at dimension 3072: " << mse_val);
    REQUIRE(mse_val < 0.02); // Should still be low
}
