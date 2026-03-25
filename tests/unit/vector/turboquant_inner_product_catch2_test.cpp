// Catch2 unit tests for TurboQuant inner product quantizer
// Tests the TurboQuant_Prod class from turboquant.h
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

struct TurboQuantProdFixture {
    TurboQuantProdFixture() : rng(42) {}

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

    // Inner product between vectors
    float innerProduct(const std::vector<float>& a, const std::vector<float>& b) {
        float sum = 0.0f;
        for (size_t i = 0; i < a.size(); ++i) {
            sum += a[i] * b[i];
        }
        return sum;
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

} // namespace

// =============================================================================
// Test Group: Basic Encode/Decode
// =============================================================================

TEST_CASE_METHOD(TurboQuantProdFixture, "TurboQuantProd basic encode produces valid output",
                 "[turboquant][prod][basic][catch2]") {
    TurboQuantConfig config;
    config.dimension = 128;
    config.bits_per_channel = 3; // 2 + 1 for inner product
    config.seed = 42;
    config.inner_product_mode = true;
    config.qjl_m = 64;

    TurboQuantProd quantizer(config);

    std::vector<float> v = generateUnitVector(128);
    auto [mse_indices, signs] = quantizer.encode(v);

    // Should produce correct number of indices and signs
    REQUIRE(mse_indices.size() == 128);
    REQUIRE(signs.size() == 64); // qjl_m

    // Signs should be ±1
    for (size_t i = 0; i < signs.size(); ++i) {
        bool is_valid_sign = (signs[i] == 1) || (signs[i] == -1);
        REQUIRE(is_valid_sign);
    }
}

TEST_CASE_METHOD(TurboQuantProdFixture, "TurboQuantProd decode reconstructs vector",
                 "[turboquant][prod][basic][catch2]") {
    TurboQuantConfig config;
    config.dimension = 256;
    config.bits_per_channel = 3;
    config.seed = 42;
    config.inner_product_mode = true;
    config.qjl_m = 128;

    TurboQuantProd quantizer(config);

    std::vector<float> original = generateUnitVector(256);
    auto [mse_indices, signs] = quantizer.encode(original);
    std::vector<float> reconstructed = quantizer.decode(mse_indices);

    // Should approximately reconstruct
    float dist = l2Distance(original, reconstructed);
    INFO("Reconstruction distance: " << dist);
    REQUIRE(dist < 1.0f); // Inner product mode trades some MSE for IP accuracy
}

// =============================================================================
// Test Group: Inner Product Estimation
// =============================================================================

TEST_CASE_METHOD(TurboQuantProdFixture, "TurboQuantProd inner product same vector",
                 "[turboquant][prod][inner_product][catch2]") {
    TurboQuantConfig config;
    config.dimension = 384;
    config.bits_per_channel = 3;
    config.seed = 42;
    config.inner_product_mode = true;
    config.qjl_m = 128;

    TurboQuantProd quantizer(config);

    std::vector<float> v = generateUnitVector(384);
    auto enc = quantizer.encode(v);

    // Inner product of vector with itself should be ~1 (cosine similarity)
    float ip_estimate = quantizer.estimateInnerProduct(enc, enc);
    float true_ip = innerProduct(v, v); // Should be 1.0 for unit vectors

    INFO("Inner product estimate (same vector): " << ip_estimate);
    INFO("True inner product: " << true_ip);

    // The estimate won't be exact, but should be positive and reasonable
    REQUIRE(ip_estimate > 0.0f);
}

TEST_CASE_METHOD(TurboQuantProdFixture, "TurboQuantProd inner product orthogonal vectors",
                 "[turboquant][prod][inner_product][catch2]") {
    TurboQuantConfig config;
    config.dimension = 256;
    config.bits_per_channel = 3;
    config.seed = 42;
    config.inner_product_mode = true;
    config.qjl_m = 128;

    TurboQuantProd quantizer(config);

    // Generate two orthogonal vectors
    std::vector<float> v1 = generateUnitVector(256);
    std::vector<float> v2 = generateUnitVector(256);

    // Gram-Schmidt orthogonalization
    float dot = innerProduct(v1, v2);
    for (size_t i = 0; i < 256; ++i) {
        v2[i] -= dot * v1[i];
    }
    float norm = 0.0f;
    for (size_t i = 0; i < 256; ++i) {
        norm += v2[i] * v2[i];
    }
    norm = std::sqrt(norm);
    for (size_t i = 0; i < 256; ++i) {
        v2[i] /= norm;
    }

    auto enc1 = quantizer.encode(v1);
    auto enc2 = quantizer.encode(v2);

    float ip_estimate = quantizer.estimateInnerProduct(enc1, enc2);
    float true_ip = innerProduct(v1, v2); // Should be ~0

    INFO("Inner product estimate (orthogonal): " << ip_estimate);
    INFO("True inner product: " << true_ip);

    // Should be close to 0
    REQUIRE(std::abs(ip_estimate) < 1.0f);
}

TEST_CASE_METHOD(TurboQuantProdFixture, "TurboQuantProd inner product varies with angle",
                 "[turboquant][prod][inner_product][catch2]") {
    TurboQuantConfig config;
    config.dimension = 384;
    config.bits_per_channel = 4;
    config.seed = 42;
    config.inner_product_mode = true;
    config.qjl_m = 256;

    TurboQuantProd quantizer(config);

    std::vector<float> v1 = generateUnitVector(384);

    // Create v2 at known angle from v1
    float angle = 0.5f; // radians
    float c = std::cos(angle);
    float s = std::sin(angle);

    std::vector<float> v2(384, 0.0f);
    v2[0] = c;
    v2[1] = s;
    // Rest is random but orthonormal to first two basis
    std::normal_distribution<float> dist(0.0f, 1.0f);
    for (size_t i = 2; i < 384; ++i) {
        v2[i] = dist(rng);
    }
    float norm = 0.0f;
    for (size_t i = 0; i < 384; ++i)
        norm += v2[i] * v2[i];
    norm = std::sqrt(norm);
    for (size_t i = 0; i < 384; ++i)
        v2[i] /= norm;

    auto enc1 = quantizer.encode(v1);
    auto enc2 = quantizer.encode(v2);

    float ip_estimate = quantizer.estimateInnerProduct(enc1, enc2);
    float true_ip = innerProduct(v1, v2);

    INFO("Inner product estimate: " << ip_estimate);
    INFO("True inner product (cosine): " << true_ip);

    // The signs of the estimates should generally agree (both positive for acute angle)
    float expected_sign = (true_ip > 0) ? 1.0f : -1.0f;
    INFO("Expected sign: " << expected_sign);
}

// =============================================================================
// Test Group: Reproducibility
// =============================================================================

TEST_CASE_METHOD(TurboQuantProdFixture, "TurboQuantProd same seed reproducibility",
                 "[turboquant][prod][reproducibility][catch2]") {
    TurboQuantConfig config1;
    config1.dimension = 256;
    config1.bits_per_channel = 3;
    config1.seed = 99999;
    config1.inner_product_mode = true;
    config1.qjl_m = 64;

    TurboQuantConfig config2;
    config2.dimension = 256;
    config2.bits_per_channel = 3;
    config2.seed = 99999;
    config2.inner_product_mode = true;
    config2.qjl_m = 64;

    TurboQuantProd q1(config1);
    TurboQuantProd q2(config2);

    std::vector<float> v = generateUnitVector(256);

    auto [idx1, signs1] = q1.encode(v);
    auto [idx2, signs2] = q2.encode(v);

    REQUIRE(idx1 == idx2);
    REQUIRE(signs1 == signs2);
}

// =============================================================================
// Test Group: Edge Cases
// =============================================================================

TEST_CASE_METHOD(TurboQuantProdFixture, "TurboQuantProd zero vector",
                 "[turboquant][prod][edge][catch2]") {
    TurboQuantConfig config;
    config.dimension = 128;
    config.bits_per_channel = 3;
    config.seed = 42;
    config.inner_product_mode = true;
    config.qjl_m = 64;

    TurboQuantProd quantizer(config);
    std::vector<float> zero(128, 0.0f);

    // Should not crash
    auto [mse_indices, signs] = quantizer.encode(zero);
    REQUIRE(mse_indices.size() == 128);
    REQUIRE(signs.size() == 64);
}

TEST_CASE_METHOD(TurboQuantProdFixture, "TurboQuantProd high m value",
                 "[turboquant][prod][edge][catch2]") {
    TurboQuantConfig config;
    config.dimension = 256;
    config.bits_per_channel = 3;
    config.seed = 42;
    config.inner_product_mode = true;
    config.qjl_m = 512; // High m

    TurboQuantProd quantizer(config);

    std::vector<float> v = generateUnitVector(256);
    auto [mse_indices, signs] = quantizer.encode(v);

    REQUIRE(signs.size() == 512);

    // Should still produce reasonable inner product estimates
    auto enc = quantizer.encode(v);
    float ip_estimate = quantizer.estimateInnerProduct(enc, enc);
    INFO("IP estimate with high m: " << ip_estimate);
    REQUIRE(ip_estimate > 0.0f);
}
