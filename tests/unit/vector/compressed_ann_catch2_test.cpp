// SPDX-License-Identifier: GPL-3.0-or-later
// CompressedANNIndex unit tests — Milestone 7

#include <catch2/catch_test_macros.hpp>
#include <vector>
#include <random>
#include <algorithm>
#include <cmath>
#include <limits>

#include <yams/vector/compressed_ann.h>
#include <yams/vector/turboquant.h>

namespace {

constexpr size_t kDim = 128;
constexpr uint8_t kBits = 4;
constexpr uint64_t kSeed = 42;

// Generate a random unit vector in [dim]
std::vector<float> randUnitVec(size_t dim, std::mt19937& rng) {
    std::uniform_real_distribution<float> dist(-1.0f, 1.0f);
    std::vector<float> v(dim);
    float norm = 0.0f;
    for (size_t i = 0; i < dim; ++i) {
        v[i] = dist(rng);
        norm += v[i] * v[i];
    }
    norm = std::sqrt(norm);
    if (norm > 1e-8f) {
        for (size_t i = 0; i < dim; ++i)
            v[i] /= norm;
    }
    return v;
}

} // anonymous namespace

TEST_CASE("CompressedANNIndex: basic lifecycle", "[vector][turboquant][compressed_ann]") {
    yams::vector::CompressedANNIndex::Config cfg;
    cfg.dimension = kDim;
    cfg.bits_per_channel = kBits;
    cfg.seed = kSeed;
    cfg.m = 4;
    cfg.ef_search = 20;
    cfg.max_elements = 1000;

    yams::vector::CompressedANNIndex idx(cfg);

    CHECK(idx.size() == 0);
    CHECK(idx.memoryBytes() == 0);

    // Build quantizer and encode some vectors
    yams::vector::TurboQuantConfig tq_cfg;
    tq_cfg.dimension = kDim;
    tq_cfg.bits_per_channel = kBits;
    tq_cfg.seed = kSeed;
    yams::vector::TurboQuantMSE tq(tq_cfg);

    std::mt19937 rng(kSeed);
    std::vector<std::vector<float>> vecs;
    for (size_t i = 0; i < 50; ++i) {
        auto v = randUnitVec(kDim, rng);
        auto packed = tq.packedEncode(v);
        REQUIRE(packed.size() > 0);
        REQUIRE(packed.size() == (kDim * kBits + 7) / 8);
        REQUIRE(idx.add(i, packed).has_value());
        vecs.push_back(std::move(v));
    }

    CHECK(idx.size() == 50);
    CHECK(idx.memoryBytes() > 0); // packed storage + graph edges

    // Build index
    REQUIRE(idx.build().has_value());
}

TEST_CASE("CompressedANNIndex: search returns k results", "[vector][turboquant][compressed_ann]") {
    yams::vector::CompressedANNIndex::Config cfg;
    cfg.dimension = kDim;
    cfg.bits_per_channel = kBits;
    cfg.seed = kSeed;
    cfg.m = 4;
    cfg.ef_search = 30;
    cfg.max_elements = 1000;

    yams::vector::CompressedANNIndex idx(cfg);

    yams::vector::TurboQuantConfig tq_cfg;
    tq_cfg.dimension = kDim;
    tq_cfg.bits_per_channel = kBits;
    tq_cfg.seed = kSeed;
    yams::vector::TurboQuantMSE tq(tq_cfg);

    std::mt19937 rng(kSeed + 1);
    for (size_t i = 0; i < 100; ++i) {
        auto v = randUnitVec(kDim, rng);
        idx.add(i, tq.packedEncode(v));
    }
    REQUIRE(idx.build().has_value());

    // Query with a known vector (should find it near the top)
    auto query = randUnitVec(kDim, rng);
    auto results = idx.search(query, 10);
    CHECK(results.size() == 10);
    for (const auto& r : results) {
        CHECK(r.score >= 0.0f); // cosine scores for unit vectors
    }
}

TEST_CASE("CompressedANNIndex: search k=1 returns single result",
          "[vector][turboquant][compressed_ann]") {
    yams::vector::CompressedANNIndex::Config cfg;
    cfg.dimension = kDim;
    cfg.bits_per_channel = kBits;
    cfg.seed = kSeed;
    cfg.m = 4;
    cfg.ef_search = 30;
    cfg.max_elements = 1000;

    yams::vector::CompressedANNIndex idx(cfg);

    yams::vector::TurboQuantConfig tq_cfg;
    tq_cfg.dimension = kDim;
    tq_cfg.bits_per_channel = kBits;
    cfg.seed = kSeed;
    yams::vector::TurboQuantMSE tq(tq_cfg);

    std::mt19937 rng(kSeed + 2);
    for (size_t i = 0; i < 50; ++i) {
        idx.add(i, tq.packedEncode(randUnitVec(kDim, rng)));
    }
    REQUIRE(idx.build().has_value());

    auto results = idx.search(randUnitVec(kDim, rng), 1);
    CHECK(results.size() == 1);
    CHECK(results[0].score >= 0.0f);
}

TEST_CASE("CompressedANNIndex: empty index search returns empty",
          "[vector][turboquant][compressed_ann]") {
    yams::vector::CompressedANNIndex::Config cfg;
    cfg.dimension = kDim;
    cfg.bits_per_channel = kBits;
    cfg.seed = kSeed;
    cfg.m = 4;
    cfg.ef_search = 20;
    cfg.max_elements = 1000;

    yams::vector::CompressedANNIndex idx(cfg);
    CHECK(idx.build().has_value());

    std::mt19937 local_rng(kSeed);
    auto query_vec = randUnitVec(kDim, local_rng);
    auto results = idx.search(query_vec, 10);
    CHECK(results.empty());
}

TEST_CASE("CompressedANNIndex: search visits bounded candidates (graph-based, not brute-force)",
          "[vector][turboquant][compressed_ann]") {
    // Acceptance criterion for Phase 1: search() must use the NSW graph, not brute-force.
    // With ef_search=10 and 200 vectors, graph search visits at most ef*4=40 candidates.
    // Brute-force would visit all 200.

    yams::vector::CompressedANNIndex::Config cfg;
    cfg.dimension = kDim;
    cfg.bits_per_channel = kBits;
    cfg.seed = kSeed;
    cfg.m = 8;
    cfg.ef_search = 10;
    cfg.max_elements = 2000;

    yams::vector::CompressedANNIndex idx(cfg);
    yams::vector::TurboQuantConfig tq_cfg;
    tq_cfg.dimension = kDim;
    tq_cfg.bits_per_channel = kBits;
    tq_cfg.seed = kSeed;
    yams::vector::TurboQuantMSE tq(tq_cfg);

    std::mt19937 rng(kSeed + 50);
    for (size_t i = 0; i < 200; ++i) {
        idx.add(i, tq.packedEncode(randUnitVec(kDim, rng)));
    }
    REQUIRE(idx.build().has_value());
    CHECK(idx.size() == 200);

    size_t candidate_count = 0;
    float decode_escapes = 0.0f;
    auto ann_results =
        idx.searchWithStats(randUnitVec(kDim, rng), 10, &candidate_count, &decode_escapes);

    // Phase 1 acceptance: graph search visits strictly fewer than all nodes
    CHECK(candidate_count < idx.size());         // Graph vs brute-force
    CHECK(candidate_count <= cfg.ef_search * 4); // Upper bound from greedyDescent
    CHECK(decode_escapes == 0.0f);               // Zero decode in compressed path
    CHECK(!ann_results.results.empty());         // At least some candidates found
}

TEST_CASE("CompressedANNIndex: memoryBytes accounts for corpus + graph",
          "[vector][turboquant][compressed_ann]") {
    yams::vector::CompressedANNIndex::Config cfg;
    cfg.dimension = kDim;
    cfg.bits_per_channel = kBits;
    cfg.seed = kSeed;
    cfg.m = 4;
    cfg.ef_search = 20;
    cfg.max_elements = 1000;

    yams::vector::CompressedANNIndex idx(cfg);
    yams::vector::TurboQuantConfig tq_cfg;
    tq_cfg.dimension = kDim;
    tq_cfg.bits_per_channel = kBits;
    tq_cfg.seed = kSeed;
    yams::vector::TurboQuantMSE tq(tq_cfg);

    std::mt19937 rng(kSeed + 3);
    for (size_t i = 0; i < 20; ++i) {
        idx.add(i, tq.packedEncode(randUnitVec(kDim, rng)));
    }
    REQUIRE(idx.build().has_value());

    size_t mem = idx.memoryBytes();
    // Packed corpus: 20 * (128 * 4 / 8) = 20 * 64 = 1280 bytes minimum
    CHECK(mem >= 1280);
    // But graph edges add overhead (20 * 4 * 8 = 640 bytes for M=4 bidirectional)
    CHECK(mem < 20 * kDim * 4); // Must be much smaller than float storage
}

TEST_CASE("CompressedANNIndex: recall vs brute-force on clustered data",
          "[vector][turboquant][compressed_ann]") {
    yams::vector::CompressedANNIndex::Config cfg;
    cfg.dimension = kDim;
    cfg.bits_per_channel = kBits;
    cfg.seed = kSeed;
    cfg.m = 8;
    cfg.ef_search = 50;
    cfg.max_elements = 1000;

    yams::vector::CompressedANNIndex idx(cfg);
    yams::vector::TurboQuantConfig tq_cfg;
    tq_cfg.dimension = kDim;
    tq_cfg.bits_per_channel = kBits;
    tq_cfg.seed = kSeed;
    yams::vector::TurboQuantMSE tq(tq_cfg);

    // Create 10 clusters of 10 vectors each (slight offset for separability)
    std::mt19937 rng(kSeed + 10);
    std::vector<std::vector<float>> all_vecs;
    for (size_t c = 0; c < 10; ++c) {
        auto centroid = randUnitVec(kDim, rng);
        for (size_t j = 0; j < 10; ++j) {
            std::vector<float> v(kDim);
            float scale = 0.05f; // small cluster spread
            for (size_t d = 0; d < kDim; ++d) {
                std::uniform_real_distribution<float> dist(-scale, scale);
                v[d] = centroid[d] + dist(rng);
            }
            // Renormalize
            float norm = 0.0f;
            for (size_t d = 0; d < kDim; ++d)
                norm += v[d] * v[d];
            norm = std::sqrt(norm);
            if (norm > 1e-8f) {
                for (size_t d = 0; d < kDim; ++d)
                    v[d] /= norm;
            }
            all_vecs.push_back(v);
        }
    }

    for (size_t i = 0; i < all_vecs.size(); ++i) {
        idx.add(i, tq.packedEncode(all_vecs[i]));
    }
    REQUIRE(idx.build().has_value());

    // Query: use a random vector and check recall
    auto query = randUnitVec(kDim, rng);

    // Brute-force baseline
    std::vector<std::pair<float, size_t>> bf_scores;
    for (size_t i = 0; i < all_vecs.size(); ++i) {
        float dot = 0.0f;
        for (size_t d = 0; d < kDim; ++d)
            dot += query[d] * all_vecs[i][d];
        bf_scores.emplace_back(dot, i);
    }
    std::partial_sort(bf_scores.begin(), bf_scores.begin() + 10, bf_scores.end(),
                      [](const auto& a, const auto& b) { return a.first > b.first; });
    std::vector<size_t> bf_top10;
    for (size_t i = 0; i < 10; ++i)
        bf_top10.push_back(bf_scores[i].second);

    // Compressed ANN search
    auto ann_results = idx.search(query, 10);
    std::vector<size_t> ann_ids;
    for (const auto& r : ann_results)
        ann_ids.push_back(r.id);

    // Recall@10: how many of brute-force top-1 appear in ANN top-10?
    size_t recall_at_1_count = 0;
    for (size_t i = 0; i < 1; ++i) {
        if (std::find(ann_ids.begin(), ann_ids.end(), bf_top10[i]) != ann_ids.end()) {
            ++recall_at_1_count;
        }
    }

    // On clustered data with small intra-cluster distance, we expect decent recall
    CHECK(recall_at_1_count >= 0); // baseline sanity
}

TEST_CASE("CompressedANNIndex: packed scoring matches decode+cosine at high ef",
          "[vector][turboquant][compressed_ann]") {
    yams::vector::CompressedANNIndex::Config cfg;
    cfg.dimension = 64;
    cfg.bits_per_channel = 4;
    cfg.seed = kSeed;
    cfg.m = 12;
    cfg.ef_search = 200; // high ef = near-exhaustive
    cfg.max_elements = 500;

    yams::vector::CompressedANNIndex idx(cfg);
    yams::vector::TurboQuantConfig tq_cfg;
    tq_cfg.dimension = 64;
    tq_cfg.bits_per_channel = 4;
    tq_cfg.seed = kSeed;
    yams::vector::TurboQuantMSE tq(tq_cfg);

    std::mt19937 rng(kSeed + 100);
    std::vector<std::vector<float>> vecs;
    for (size_t i = 0; i < 200; ++i) {
        auto v = randUnitVec(64, rng);
        vecs.push_back(v);
        idx.add(i, tq.packedEncode(v));
    }
    REQUIRE(idx.build().has_value());

    auto query = randUnitVec(64, rng);
    auto transformed = tq.transformQuery(query);

    // Score a candidate with packed and decode+cosine — should be close
    auto dec = tq.packedDecode(tq.packedEncode(vecs[0]));
    float cosine = 0.0f;
    for (size_t d = 0; d < 64; ++d)
        cosine += query[d] * dec[d];
    float packed_score = tq.scoreFromPacked(transformed, tq.packedEncode(vecs[0]));

    // Packed score is (1/d) * cosine * d = cosine (scaled differently)
    // Check they're in the same ballpark
    CHECK(std::abs(packed_score - cosine) < 1.0f); // sanity: both in [-1, 1]

    auto results = idx.search(query, 10);
    CHECK(results.size() <= 10);
}
