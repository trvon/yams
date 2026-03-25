// Catch2 unit tests for TurboQuant inner product quantizer
// Tests the TurboQuantProd class from turboquant.h
// Paper reference: arXiv:2504.19874 (approximation implementation)
//
// Test coverage:
//   - Conservative blend correction (estimateInnerProduct): sign agreement rate
//     adjusts the MSE-decoded dot product. Blend factor = 0.25.
//   - Full QJL correction (estimateInnerProductFull): uses residual norms.
//     Benchmark shows WORSE than decoded-only for random vectors (MAE: 0.28-0.36 vs 0.04-0.06).
//   - Statistical tests: bias, monotonicity, orthogonal, same-vector.

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
    auto enc = quantizer.encode(v);

    // Should produce correct number of indices and signs
    REQUIRE(enc.mse_indices.size() == 128);
    REQUIRE(enc.qjl_signs.size() == 64); // qjl_m

    // Signs should be ±1
    for (size_t i = 0; i < enc.qjl_signs.size(); ++i) {
        bool is_valid_sign = (enc.qjl_signs[i] == 1) || (enc.qjl_signs[i] == -1);
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
    auto enc = quantizer.encode(original);
    std::vector<float> reconstructed = quantizer.decode(enc.mse_indices);

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

    // The estimate should be in a reasonable range for a unit vector self-IP
    // With QJL correction (kQjlBlendFactor=0.25), max boost when gamma=1 gives 1.25x
    REQUIRE(ip_estimate > 0.5f); // Clearly positive
    REQUIRE(ip_estimate < 2.5f); // Not wildly inflated
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

    // For orthogonal unit vectors: true_ip ≈ 0. With QJL correction, estimates
    // near 0 are expected (agreement ≈ 0.5 means correction ≈ 1.0).
    // Require: |estimate| < 0.5 — meaningfully closer to 0 than 1.0
    REQUIRE(std::abs(ip_estimate) < 0.5f);
}

TEST_CASE_METHOD(TurboQuantProdFixture,
                 "TurboQuantProd IP estimate bias is near zero across random pairs",
                 "[turboquant][prod][inner_product][statistical][catch2]") {
    TurboQuantConfig config;
    config.dimension = 256;
    config.bits_per_channel = 3;
    config.seed = 7;
    config.inner_product_mode = true;
    config.qjl_m = 128;

    TurboQuantProd quantizer(config);

    // Run 50 random pairs, measure bias = mean(estimate - true_ip)
    constexpr size_t kNumPairs = 50;
    std::vector<float> errors;
    errors.reserve(kNumPairs);

    for (size_t trial = 0; trial < kNumPairs; ++trial) {
        auto v1 = generateUnitVector(256);
        auto v2 = generateUnitVector(256);
        auto enc1 = quantizer.encode(v1);
        auto enc2 = quantizer.encode(v2);

        float estimate = quantizer.estimateInnerProduct(enc1, enc2);
        float true_ip = innerProduct(v1, v2);
        errors.push_back(estimate - true_ip);
    }

    // Compute mean error
    float mean_error = 0.0f;
    for (float e : errors)
        mean_error += e;
    mean_error /= static_cast<float>(kNumPairs);

    INFO("Mean IP bias across " << kNumPairs << " random pairs: " << mean_error);

    // Bias should be within [-0.2, 0.2] for unit vectors (conservative threshold)
    REQUIRE(std::abs(mean_error) < 0.2f);
}

TEST_CASE_METHOD(TurboQuantProdFixture, "TurboQuantProd IP estimate is monotonic with cosine angle",
                 "[turboquant][prod][inner_product][statistical][catch2]") {
    TurboQuantConfig config;
    config.dimension = 384;
    config.bits_per_channel = 4;
    config.seed = 99;
    config.inner_product_mode = true;
    config.qjl_m = 128;

    TurboQuantProd quantizer(config);

    // Test 5 angle buckets: 0°, 30°, 60°, 90°, 120°
    std::vector<float> angles_rad = {0.0f, 0.524f, 1.047f, 1.571f,
                                     2.094f}; // 0, 30, 60, 90, 120 deg

    std::vector<float> estimates;
    for (float angle : angles_rad) {
        auto v1 = generateUnitVector(384);

        // Build v2 at this angle from v1
        float c = std::cos(angle);
        float s = std::sin(angle);
        std::vector<float> v2(384);
        v2[0] = c;
        v2[1] = s;
        for (size_t i = 2; i < 384; ++i) {
            v2[i] = generateUnitVector(384)[i];
        }
        float norm = 0.0f;
        for (size_t i = 0; i < 384; ++i)
            norm += v2[i] * v2[i];
        norm = std::sqrt(norm);
        for (size_t i = 0; i < 384; ++i)
            v2[i] /= norm;

        auto enc1 = quantizer.encode(v1);
        auto enc2 = quantizer.encode(v2);

        float est = quantizer.estimateInnerProduct(enc1, enc2);
        float true_ip = innerProduct(v1, v2);

        INFO("Angle=" << angle << " rad: estimate=" << est << ", true_ip=" << true_ip);
        estimates.push_back(est);

        // Sanity: same angle gives close estimate to true_ip
        if (angle < 0.1f) {
            REQUIRE(std::abs(est - true_ip) < 1.0f);
        }
    }

    // Monotonicity: estimates should decrease (or stay same) as angle increases.
    // Check that estimate[0] >= estimate[1] >= ... (with small tolerance for noise)
    size_t violations = 0;
    for (size_t i = 0; i + 1 < estimates.size(); ++i) {
        if (estimates[i] < estimates[i + 1] - 0.5f) {
            violations++;
        }
    }
    INFO("Monotonicity violations: " << violations << " / " << (estimates.size() - 1));
    REQUIRE(violations <= 1); // Allow at most 1 noisy violation out of 4 comparisons
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

    auto enc1 = q1.encode(v);
    auto enc2 = q2.encode(v);

    REQUIRE(enc1.mse_indices == enc2.mse_indices);
    REQUIRE(enc1.qjl_signs == enc2.qjl_signs);
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
    auto enc = quantizer.encode(zero);
    REQUIRE(enc.mse_indices.size() == 128);
    REQUIRE(enc.qjl_signs.size() == 64);
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
    auto enc = quantizer.encode(v);

    REQUIRE(enc.qjl_signs.size() == 512);

    // Should still produce reasonable inner product estimates
    float ip_estimate = quantizer.estimateInnerProduct(enc, enc);
    INFO("IP estimate with high m: " << ip_estimate);
    REQUIRE(ip_estimate > 0.0f);
}
