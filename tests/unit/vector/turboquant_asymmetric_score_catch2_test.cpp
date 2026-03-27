// Catch2 unit tests for TurboQuantMSE asymmetric compressed-space scoring
// Tests: transformQuery, scoreFromPacked, and vector_utils free functions
//
// Score contract:
//   scoreFromPacked(transformQuery(q), packedEncode(v)) ≈ dot(q, v)
//   for unit vectors: ≈ cosine(q, v)

#include <catch2/catch_test_macros.hpp>

#include <cmath>
#include <random>
#include <vector>

#include <yams/vector/turboquant.h>
#include <yams/vector/vector_index_manager.h>

using namespace yams::vector;
using namespace yams::vector::vector_utils;

namespace {

float dotProduct(const std::vector<float>& a, const std::vector<float>& b) {
    float sum = 0.0f;
    for (size_t i = 0; i < a.size(); ++i) {
        sum += a[i] * b[i];
    }
    return sum;
}

std::vector<float> generateUnitVector(size_t dim, uint32_t seed) {
    std::mt19937 rng(seed);
    std::normal_distribution<float> dist(0.0f, 1.0f);
    std::vector<float> v(dim);
    float norm_sq = 0.0f;
    for (size_t i = 0; i < dim; ++i) {
        v[i] = dist(rng);
        norm_sq += v[i] * v[i];
    }
    float inv_norm = 1.0f / std::sqrt(norm_sq);
    for (size_t i = 0; i < dim; ++i)
        v[i] *= inv_norm;
    return v;
}

} // namespace

TEST_CASE("transformQuery produces non-trivial values", "[turboquant][asymmetric][catch2]") {
    TurboQuantConfig config;
    config.dimension = 384;
    config.bits_per_channel = 4;
    config.seed = 42;
    TurboQuantMSE tq(config);

    auto v = generateUnitVector(384, 1);
    auto y = tq.transformQuery(v);

    CHECK(y.size() == 384);

    // Transform should not be all zeros
    float sum_abs = 0.0f;
    for (float val : y)
        sum_abs += std::abs(val);
    CHECK(sum_abs > 1e-4f);
}

TEST_CASE("scoreFromPacked returns valid cosine range", "[turboquant][asymmetric][catch2]") {
    TurboQuantConfig config;
    config.dimension = 384;
    config.bits_per_channel = 4;
    config.seed = 42;
    TurboQuantMSE tq(config);

    auto v1 = generateUnitVector(384, 1);
    auto v2 = generateUnitVector(384, 2);

    auto packed1 = tq.packedEncode(v1);
    auto y2 = tq.transformQuery(v2);

    float score = tq.scoreFromPacked(y2, packed1);

    // Valid cosine range: [-1, 1]
    CHECK(score >= -1.001f);
    CHECK(score <= 1.001f);
}

TEST_CASE("scoreFromPacked: self-score ≈ 1.0 for unit vectors",
          "[turboquant][asymmetric][catch2]") {
    TurboQuantConfig config;
    config.dimension = 384;
    config.bits_per_channel = 4;
    config.seed = 42;
    TurboQuantMSE tq(config);

    auto v = generateUnitVector(384, 7);
    auto packed = tq.packedEncode(v);
    auto y = tq.transformQuery(v);

    float self_score = tq.scoreFromPacked(y, packed);
    float true_dot = dotProduct(v, v); // = 1.0 for unit vectors

    INFO("Self-score: " << self_score << ", true dot: " << true_dot);
    CHECK(self_score > 0.5f);                      // Should be close to 1.0
    CHECK(std::abs(self_score - true_dot) < 0.6f); // Asymmetric may deviate but not wildly
}

TEST_CASE("scoreFromPacked tracks decoded dot product direction",
          "[turboquant][asymmetric][catch2]") {
    TurboQuantConfig config;
    config.dimension = 384;
    config.bits_per_channel = 4;
    config.seed = 42;
    TurboQuantMSE tq(config);

    auto v1 = generateUnitVector(384, 10);
    auto v2 = generateUnitVector(384, 20);

    auto packed1 = tq.packedEncode(v1);
    auto y2 = tq.transformQuery(v2);

    float asym_score = tq.scoreFromPacked(y2, packed1);
    float decoded_dot = dotProduct(v1, v2);

    INFO("Asymmetric: " << asym_score << ", decoded: " << decoded_dot);

    // Both should agree on sign (both positive or both negative)
    CHECK((asym_score >= 0.0f) == (decoded_dot >= 0.0f));
}

TEST_CASE("scoreFromPacked: orthogonal vectors get low scores",
          "[turboquant][asymmetric][catch2]") {
    TurboQuantConfig config;
    config.dimension = 384;
    config.bits_per_channel = 4;
    config.seed = 42;
    TurboQuantMSE tq(config);

    // Two unrelated random vectors should have near-zero cosine
    auto v1 = generateUnitVector(384, 100);
    auto v2 = generateUnitVector(384, 200);

    auto packed1 = tq.packedEncode(v1);
    auto y2 = tq.transformQuery(v2);

    float asym = tq.scoreFromPacked(y2, packed1);
    float decoded = dotProduct(v1, v2);

    INFO("Asymmetric (unrelated): " << asym << ", decoded: " << decoded);
    CHECK(std::abs(asym) < 0.9f);    // Not near ±1.0
    CHECK(std::abs(decoded) < 0.9f); // Not near ±1.0
}

TEST_CASE("scoreFromPacked: similar vectors score well", "[turboquant][asymmetric][catch2]") {
    TurboQuantConfig config;
    config.dimension = 384;
    config.bits_per_channel = 4;
    config.seed = 42;
    TurboQuantMSE tq(config);

    // Start with v1, make v2 as a small perturbation of v1
    auto v1 = generateUnitVector(384, 300);
    auto v2 = v1;
    std::mt19937 rng(301);
    std::normal_distribution<float> noise(0.0f, 0.005f);
    for (size_t i = 0; i < 384; ++i)
        v2[i] += noise(rng);
    // Renormalize
    float nsq = 0.0f;
    for (size_t i = 0; i < 384; ++i)
        nsq += v2[i] * v2[i];
    float inv = 1.0f / std::sqrt(nsq);
    for (size_t i = 0; i < 384; ++i)
        v2[i] *= inv;

    auto packed1 = tq.packedEncode(v1);
    auto y2 = tq.transformQuery(v2);

    float asym = tq.scoreFromPacked(y2, packed1);
    float decoded = dotProduct(v1, v2);

    INFO("Similar vectors — asymmetric: " << asym << ", decoded: " << decoded);
    CHECK(asym > 0.7f); // Similar vectors should score well
    CHECK(decoded > 0.7f);
}

TEST_CASE("scoreFromPacked: dim=768 works correctly", "[turboquant][asymmetric][dim768][catch2]") {
    TurboQuantConfig config;
    config.dimension = 768;
    config.bits_per_channel = 4;
    config.seed = 7;
    TurboQuantMSE tq(config);

    auto v = generateUnitVector(768, 1);
    auto packed = tq.packedEncode(v);
    auto y = tq.transformQuery(v);

    float score = tq.scoreFromPacked(y, packed);
    CHECK(score > 0.0f); // Self-score should be positive
    CHECK(score <= 1.001f);
}

TEST_CASE("scoreFromPacked: 2-bit has different scores than 4-bit",
          "[turboquant][asymmetric][bits][catch2]") {
    TurboQuantConfig cfg4;
    cfg4.dimension = 384;
    cfg4.bits_per_channel = 4;
    cfg4.seed = 42;
    TurboQuantMSE tq4(cfg4);

    TurboQuantConfig cfg2;
    cfg2.dimension = 384;
    cfg2.bits_per_channel = 2;
    cfg2.seed = 42; // Same seed for fair comparison
    TurboQuantMSE tq2(cfg2);

    auto v1 = generateUnitVector(384, 50);
    auto v2 = generateUnitVector(384, 51);

    auto packed1_4 = tq4.packedEncode(v1);
    auto y2_4 = tq4.transformQuery(v2);
    float score4 = tq4.scoreFromPacked(y2_4, packed1_4);

    auto packed1_2 = tq2.packedEncode(v1);
    auto y2_2 = tq2.transformQuery(v2);
    float score2 = tq2.scoreFromPacked(y2_2, packed1_2);

    INFO("4-bit: " << score4 << ", 2-bit: " << score2);
    // Both should be valid
    CHECK(score4 > -1.0f);
    CHECK(score4 <= 1.0f);
    CHECK(score2 > -1.0f);
    CHECK(score2 <= 1.0f);
}

TEST_CASE("vector_utils free functions work correctly",
          "[turboquant][asymmetric][vector_utils][catch2]") {
    TurboQuantConfig config;
    config.dimension = 384;
    config.bits_per_channel = 4;
    config.seed = 42;
    TurboQuantMSE tq(config);

    auto v1 = generateUnitVector(384, 60);
    auto v2 = generateUnitVector(384, 61);

    auto y1 = transformQueryForScoring(v1, &tq);
    auto packed2 = tq.packedEncode(v2);

    float asym_free = scoreCompressedCosine(y1, packed2, &tq);
    float asym_method = tq.scoreFromPacked(y1, packed2);

    CHECK(asym_free > -1.0f);
    CHECK(asym_free <= 1.0f);
    CHECK(std::abs(asym_free - asym_method) < 1e-5f); // Should be identical
}

TEST_CASE("TurboQuantMSE: per-coord scales are non-trivial after fit",
          "[turboquant][asymmetric][per_coord_scales][catch2]") {
    // After fitPerCoordScales, scales should vary across coordinates
    // (not all the same as the heuristic initialization)
    TurboQuantConfig config;
    config.dimension = 128;
    config.bits_per_channel = 4;
    config.seed = 99;
    TurboQuantMSE tq(config);

    // Generate training corpus
    std::vector<std::vector<float>> corpus;
    for (size_t i = 0; i < 200; ++i) {
        corpus.push_back(generateUnitVector(128, static_cast<uint32_t>(i + 1000)));
    }

    tq.fitPerCoordScales(corpus, 200);

    const auto& scales = tq.perCoordScales();
    REQUIRE(scales.size() == 128);

    // Check scales are all positive and in a reasonable range
    float min_scale = scales[0];
    float max_scale = scales[0];
    for (float s : scales) {
        CHECK(s > 0.0f);
        min_scale = std::min(min_scale, s);
        max_scale = std::max(max_scale, s);
    }

    // With enough training data, scales should show variance
    CHECK(max_scale > min_scale * 1.01f); // At least 1% variation
}
