// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

#include <catch2/catch_approx.hpp>
#include <catch2/catch_test_macros.hpp>

#include <yams/vector/binary_quantization.h>

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <numbers>
#include <utility>
#include <vector>

using yams::vector::BinaryQuantizedIndex;
using yams::vector::BinaryQuantizer;
using yams::vector::BinaryVector;

TEST_CASE("BinaryQuantizer: quantize and Hamming distance", "[vector][bq][catch2]") {
    SECTION("Empty vector returns empty BinaryVector") {
        std::vector<float> empty;
        auto bq = BinaryQuantizer::quantize(empty);
        CHECK(bq.empty());
        CHECK(bq.dimension == 0);
        CHECK(bq.words.empty());
    }

    SECTION("Sign bit quantization correctly packs words") {
        // 4 elements: >= 0 is bit 1, < 0 is bit 0
        std::vector<float> v = {1.5F, -0.2F, 0.0F, -10.0F};
        auto bq = BinaryQuantizer::quantize(v);
        REQUIRE(bq.dimension == 4);
        REQUIRE(bq.words.size() == 1);
        // Bit 0 = 1, Bit 1 = 0, Bit 2 = 1, Bit 3 = 0 -> binary 0101 = 5
        CHECK(bq.words[0] == 5ULL);
    }

    SECTION("Quantization across 64-bit boundaries") {
        std::vector<float> v(130, -1.0F);
        v[0] = 1.0F;   // word 0, bit 0
        v[63] = 1.0F;  // word 0, bit 63
        v[64] = 1.0F;  // word 1, bit 0
        v[127] = 1.0F; // word 1, bit 63
        v[128] = 1.0F; // word 2, bit 0

        auto bq = BinaryQuantizer::quantize(v);
        REQUIRE(bq.dimension == 130);
        REQUIRE(bq.words.size() == 3);

        CHECK(bq.words[0] == (1ULL | (1ULL << 63)));
        CHECK(bq.words[1] == (1ULL | (1ULL << 63)));
        CHECK(bq.words[2] == 1ULL);
    }

    SECTION("Identical vectors have Hamming distance 0 and cosine similarity 1.0") {
        std::vector<float> v = {0.1F, -0.5F, 1.2F, -3.0F, 0.0F, 2.1F};
        auto bq1 = BinaryQuantizer::quantize(v);
        auto bq2 = BinaryQuantizer::quantize(v);

        CHECK(BinaryQuantizer::hammingDistance(bq1, bq2) == 0);
        CHECK(BinaryQuantizer::normalizedHammingDistance(bq1, bq2) == Catch::Approx(0.0F));
        CHECK(BinaryQuantizer::estimatedCosineSimilarity(bq1, bq2) == Catch::Approx(1.0F));
    }

    SECTION("Opposite vectors have maximum Hamming distance and cosine similarity -1.0") {
        std::vector<float> v1 = {1.0F, 2.0F, 3.0F, 4.0F};
        std::vector<float> v2 = {-1.0F, -2.0F, -3.0F, -4.0F};
        auto bq1 = BinaryQuantizer::quantize(v1);
        auto bq2 = BinaryQuantizer::quantize(v2);

        CHECK(BinaryQuantizer::hammingDistance(bq1, bq2) == 4);
        CHECK(BinaryQuantizer::normalizedHammingDistance(bq1, bq2) == Catch::Approx(1.0F));
        CHECK(BinaryQuantizer::estimatedCosineSimilarity(bq1, bq2) == Catch::Approx(-1.0F));
    }

    SECTION("Half differing bits yield orthogonal estimated similarity 0.0") {
        std::vector<float> v1 = {1.0F, 1.0F, -1.0F, -1.0F};
        std::vector<float> v2 = {1.0F, 1.0F, 1.0F, 1.0F};
        auto bq1 = BinaryQuantizer::quantize(v1);
        auto bq2 = BinaryQuantizer::quantize(v2);

        CHECK(BinaryQuantizer::hammingDistance(bq1, bq2) == 2);
        CHECK(BinaryQuantizer::normalizedHammingDistance(bq1, bq2) == Catch::Approx(0.5F));
        CHECK(std::abs(BinaryQuantizer::estimatedCosineSimilarity(bq1, bq2)) < 1e-5F);
    }
}

TEST_CASE("BinaryQuantizedIndex: build and search", "[vector][bq][index][catch2]") {
    SECTION("Validation rejects empty or mismatched inputs") {
        std::vector<std::size_t> ids = {1, 2};
        std::vector<std::vector<float>> vecs = {{1.0F, 0.0F}}; // mismatched size
        auto res = BinaryQuantizedIndex::build(ids, vecs);
        CHECK_FALSE(res.has_value());

        // Duplicate IDs
        ids = {1, 1};
        vecs = {{1.0F, 0.0F}, {0.0F, 1.0F}};
        res = BinaryQuantizedIndex::build(ids, vecs);
        CHECK_FALSE(res.has_value());

        // Mismatched dimensions
        ids = {1, 2};
        vecs = {{1.0F, 0.0F}, {1.0F, 0.0F, 1.0F}};
        res = BinaryQuantizedIndex::build(ids, vecs);
        CHECK_FALSE(res.has_value());
    }

    SECTION("Search ranks by Hamming distance and respects topK") {
        std::vector<std::size_t> ids = {10, 20, 30, 40};
        std::vector<std::vector<float>> vecs = {
            {1.0F, 1.0F, 1.0F, 1.0F},    // Identical signs: dist 0
            {1.0F, 1.0F, 1.0F, -1.0F},   // 1 bit flipped: dist 1
            {1.0F, 1.0F, -1.0F, -1.0F},  // 2 bits flipped: dist 2
            {-1.0F, -1.0F, -1.0F, -1.0F} // 4 bits flipped: dist 4
        };

        auto indexRes = BinaryQuantizedIndex::build(ids, vecs);
        REQUIRE(indexRes.has_value());
        auto index = std::move(indexRes.value());

        REQUIRE(index->size() == 4);
        REQUIRE(index->dimension() == 4);
        CHECK(index->memoryUsageBytes() > 0);

        std::vector<float> query = {2.0F, 0.5F, 1.0F, 3.0F}; // all positive signs
        auto hits = index->search(query, 3);

        REQUIRE(hits.size() == 3);
        CHECK(hits[0].id == 10);
        CHECK(hits[0].hammingDistance == 0);
        CHECK(hits[0].estimatedSimilarity == Catch::Approx(1.0F));

        CHECK(hits[1].id == 20);
        CHECK(hits[1].hammingDistance == 1);

        CHECK(hits[2].id == 30);
        CHECK(hits[2].hammingDistance == 2);
    }

    SECTION("Matryoshka prefix dimension slicing") {
        // 128-dimensional vectors, indexed with maxPrefixDimension = 64
        std::vector<std::size_t> ids = {1, 2};
        std::vector<float> v1(128, 1.0F);
        std::vector<float> v2(128, -1.0F);
        for (std::size_t i = 64; i < 128; ++i) {
            v1[i] = -1.0F;
        }

        std::vector<std::vector<float>> vecs = {v1, v2};
        auto fullIndex = BinaryQuantizedIndex::build(ids, vecs, 0).value();
        CHECK(fullIndex->dimension() == 128);

        auto prefixIndex = BinaryQuantizedIndex::build(ids, vecs, 64).value();
        CHECK(prefixIndex->dimension() == 64);

        // Full 128-D query queries 64-D prefix index seamlessly
        std::vector<float> query(128, 1.0F);
        auto hits = prefixIndex->search(query, 2);
        REQUIRE(hits.size() == 2);
        CHECK(hits[0].id == 1);
        CHECK(hits[0].hammingDistance == 0);
        CHECK(hits[1].id == 2);
        CHECK(hits[1].hammingDistance == 64);
    }
}

namespace {

// Deterministic, platform-independent pseudo-random floats in [-1, 1).
struct SplitMixFloats {
    std::uint64_t state;
    float next() {
        std::uint64_t z = (state += 0x9E3779B97F4A7C15ULL);
        z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ULL;
        z = (z ^ (z >> 27)) * 0x94D049BB133111EBULL;
        z ^= z >> 31;
        return static_cast<float>(static_cast<double>(z >> 11) * 0x1.0p-53 * 2.0 - 1.0);
    }
};

// Reference raw-coordinate sign-bit shortlist: the behaviour of the unrotated index.
std::vector<std::pair<std::size_t, std::size_t>>
rawSignShortlist(const std::vector<std::vector<float>>& vectors, const std::vector<float>& query,
                 std::size_t prefix, std::size_t topK) {
    std::vector<std::pair<std::size_t, std::size_t>> ranked; // (hamming, id)
    for (std::size_t id = 0; id < vectors.size(); ++id) {
        std::size_t hamming = 0;
        for (std::size_t i = 0; i < prefix; ++i) {
            hamming += ((vectors[id][i] >= 0.0F) != (query[i] >= 0.0F)) ? 1U : 0U;
        }
        ranked.emplace_back(hamming, id);
    }
    std::ranges::sort(ranked);
    ranked.resize(std::min(topK, ranked.size()));
    return ranked;
}

} // namespace

TEST_CASE("BinaryQuantizedIndex without rotation keeps raw-coordinate sign bits",
          "[vector][bq][index][characterization][catch2]") {
    constexpr std::size_t kVectors = 48;
    constexpr std::size_t kDimension = 96;
    SplitMixFloats rng{42};
    std::vector<std::size_t> ids(kVectors);
    std::vector<std::vector<float>> vectors(kVectors, std::vector<float>(kDimension));
    for (std::size_t id = 0; id < kVectors; ++id) {
        ids[id] = id;
        for (auto& value : vectors[id]) {
            value = rng.next();
        }
    }
    std::vector<float> query(kDimension);
    for (auto& value : query) {
        value = rng.next();
    }

    for (const std::size_t prefix : {std::size_t{0}, std::size_t{16}}) {
        auto index = BinaryQuantizedIndex::build(ids, vectors, prefix);
        REQUIRE(index.has_value());
        const auto effective = prefix == 0 ? kDimension : prefix;
        CHECK(index.value()->dimension() == effective);
        const auto hits = index.value()->search(query, 10);
        const auto expected = rawSignShortlist(vectors, query, effective, 10);
        REQUIRE(hits.size() == expected.size());
        for (std::size_t i = 0; i < hits.size(); ++i) {
            CHECK(hits[i].hammingDistance == expected[i].first);
            CHECK(hits[i].id == expected[i].second);
            CHECK(hits[i].estimatedSimilarity ==
                  Catch::Approx(
                      std::cos(std::numbers::pi_v<float> * static_cast<float>(expected[i].first) /
                               static_cast<float>(effective))));
        }
    }
}
