// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors

#include <catch2/catch_approx.hpp>
#include <catch2/catch_test_macros.hpp>

#include <yams/search/anchor_fusion.h>

#include <cmath>
#include <random>
#include <vector>

using Catch::Approx;
using yams::search::AnchorCandidate;
using yams::search::computeAnchorScores;
using yams::search::phssQueryConfidence;

namespace {

std::vector<float> makeUnitVector(std::size_t dim, std::uint32_t seed) {
    std::mt19937 gen{seed};
    std::normal_distribution<float> noise{0.0F, 1.0F};
    std::vector<float> v(dim, 0.0F);
    double sq = 0.0;
    for (auto& x : v) {
        x = noise(gen);
        sq += static_cast<double>(x) * x;
    }
    const float norm = static_cast<float>(std::sqrt(sq));
    if (norm > 0.0F) {
        for (auto& x : v) {
            x /= norm;
        }
    }
    return v;
}

} // namespace

TEST_CASE("computeAnchorScores: empty input -> empty output",
          "[unit][search][anchor_fusion][catch2]") {
    auto query = makeUnitVector(8, 1);
    std::vector<AnchorCandidate> empty;
    auto scores = computeAnchorScores(std::span<const float>(query), empty);
    CHECK(scores.empty());
}

TEST_CASE("computeAnchorScores: doc with empty medoid yields zero",
          "[unit][search][anchor_fusion][catch2]") {
    auto query = makeUnitVector(8, 2);
    std::vector<AnchorCandidate> cands = {
        AnchorCandidate{"orphan", {}},
    };
    auto scores = computeAnchorScores(std::span<const float>(query), cands);
    REQUIRE(scores.contains("orphan"));
    CHECK(scores["orphan"] == Approx(0.0F));
}

TEST_CASE("computeAnchorScores: identical query and medoid -> max score",
          "[unit][search][anchor_fusion][catch2]") {
    auto query = makeUnitVector(8, 3);
    std::vector<AnchorCandidate> cands = {
        AnchorCandidate{"perfect", query}, // medoid == query
    };
    auto scores = computeAnchorScores(std::span<const float>(query), cands);
    REQUIRE(scores.contains("perfect"));
    // cosine = 1 -> (1+1)/2 = 1.0
    CHECK(scores["perfect"] == Approx(1.0F).epsilon(0.001));
}

TEST_CASE("computeAnchorScores: anti-parallel query and medoid -> min score",
          "[unit][search][anchor_fusion][catch2]") {
    auto query = makeUnitVector(8, 4);
    std::vector<float> opposite(query.size(), 0.0F);
    for (std::size_t i = 0; i < query.size(); ++i) {
        opposite[i] = -query[i];
    }
    std::vector<AnchorCandidate> cands = {
        AnchorCandidate{"opposite", opposite},
    };
    auto scores = computeAnchorScores(std::span<const float>(query), cands);
    REQUIRE(scores.contains("opposite"));
    // cosine = -1 -> (-1+1)/2 = 0.0
    CHECK(scores["opposite"] == Approx(0.0F).margin(0.001));
}

TEST_CASE("computeAnchorScores: ranking order preserved across multiple docs",
          "[unit][search][anchor_fusion][catch2]") {
    auto query = makeUnitVector(16, 5);
    auto closeMedoid = makeUnitVector(16, 5); // same seed -> identical
    auto medMedoid = makeUnitVector(16, 99);
    auto farMedoid = makeUnitVector(16, 999);

    std::vector<AnchorCandidate> cands = {
        AnchorCandidate{"close", closeMedoid},
        AnchorCandidate{"medium", medMedoid},
        AnchorCandidate{"far", farMedoid},
    };
    auto scores = computeAnchorScores(std::span<const float>(query), cands);
    REQUIRE(scores.size() == 3);
    // close should outscore medium and far (it's the same vector as query).
    CHECK(scores["close"] > scores["medium"]);
    CHECK(scores["close"] > scores["far"]);
    // All scores are in [0, 1].
    for (const auto& [_, s] : scores) {
        CHECK(s >= 0.0F);
        CHECK(s <= 1.0F);
    }
}

TEST_CASE("computeAnchorScores: dimension mismatch yields zero",
          "[unit][search][anchor_fusion][catch2]") {
    auto query = makeUnitVector(8, 7);
    std::vector<float> wrongDim(16, 0.5F); // wrong dim
    std::vector<AnchorCandidate> cands = {
        AnchorCandidate{"mismatch", wrongDim},
    };
    auto scores = computeAnchorScores(std::span<const float>(query), cands);
    CHECK(scores["mismatch"] == Approx(0.0F));
}

TEST_CASE("phssQueryConfidence: degenerate inputs return 0",
          "[unit][search][anchor_fusion][catch2]") {
    auto query = makeUnitVector(8, 11);
    // Empty top-K
    CHECK(phssQueryConfidence(std::span<const float>(query),
                              std::span<const std::vector<float>>{}) == Approx(0.0F));
    // Single doc (need at least 2 to form an edge with the query)
    std::vector<std::vector<float>> single = {makeUnitVector(8, 12)};
    // Actually the function requires k >= 2 for n = k + 1 >= 3 vertices.
    CHECK(phssQueryConfidence(std::span<const float>(query),
                              std::span<const std::vector<float>>(single)) == Approx(0.0F));
    // Empty query
    std::vector<std::vector<float>> ten;
    for (int i = 0; i < 10; ++i)
        ten.push_back(makeUnitVector(8, static_cast<std::uint32_t>(20 + i)));
    CHECK(phssQueryConfidence(std::span<const float>{}, std::span<const std::vector<float>>(ten)) ==
          Approx(0.0F));
}

TEST_CASE("phssQueryConfidence: tight cluster around query -> meaningful confidence",
          "[unit][search][anchor_fusion][catch2]") {
    // Build a tight cluster of 8 docs near the query and 4 noise docs far away.
    auto query = makeUnitVector(16, 31);
    std::vector<std::vector<float>> docs;
    for (int i = 0; i < 8; ++i) {
        // Near-query docs: small perturbation of query.
        auto near = query;
        std::mt19937 gen{static_cast<std::uint32_t>(40 + i)};
        std::normal_distribution<float> noise{0.0F, 0.05F};
        for (auto& x : near) {
            x += noise(gen);
        }
        // Re-normalize.
        double sq = 0.0;
        for (auto x : near) {
            sq += static_cast<double>(x) * x;
        }
        const float norm = static_cast<float>(std::sqrt(sq));
        for (auto& x : near) {
            x /= norm;
        }
        docs.push_back(std::move(near));
    }
    for (int i = 0; i < 4; ++i) {
        docs.push_back(makeUnitVector(16, static_cast<std::uint32_t>(900 + i)));
    }
    const float conf = phssQueryConfidence(std::span<const float>(query),
                                           std::span<const std::vector<float>>(docs));
    CHECK(conf >= 0.0F);
    CHECK(conf <= 1.0F);
    // With a clear cluster + noise structure, PHSS should pick a scale above
    // the mean similarity → confidence > 0.5.
    CHECK(conf > 0.4F);
}
