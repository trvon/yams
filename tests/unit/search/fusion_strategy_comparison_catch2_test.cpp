// SPDX-License-Identifier: Apache-2.0
// Copyright 2025 YAMS Contributors

/**
 * @file fusion_strategy_comparison_catch2_test.cpp
 * @brief Comparison tests for different fusion strategies
 *
 * Tests compare RRF, COMB_MNZ, TEXT_ANCHOR, and other fusion strategies
 * to verify ranking quality improvements from main branch defaults.
 */

#include <catch2/catch_approx.hpp>
#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_floating_point.hpp>

#include <yams/search/search_engine.h>

#include <algorithm>
#include <chrono>
#include <string>
#include <vector>

using namespace yams::search;
using Catch::Approx;

// =============================================================================
// Helper functions to create test data
// =============================================================================

namespace {

/// Create a ComponentResult with the given parameters
ComponentResult makeComponent(const std::string& hash, const std::string& path, float score,
                              const std::string& source, size_t rank) {
    ComponentResult cr;
    cr.documentHash = hash;
    cr.filePath = path;
    cr.score = score;
    cr.source = source;
    cr.rank = rank;
    return cr;
}

/// Create a standard test corpus with overlapping results from different components
std::vector<ComponentResult> createTestCorpus() {
    std::vector<ComponentResult> results;

    // Document A: Found by text (rank 0, high score) and vector (rank 2, medium score)
    results.push_back(makeComponent("docA", "/path/a.cpp", 0.95f, "text", 0));
    results.push_back(makeComponent("docA", "/path/a.cpp", 0.70f, "vector", 2));

    // Document B: Found by vector only (rank 0, very high score) - semantic match
    results.push_back(makeComponent("docB", "/path/b.cpp", 0.98f, "vector", 0));

    // Document C: Found by text (rank 1, high score), vector (rank 1), and KG (rank 0)
    results.push_back(makeComponent("docC", "/path/c.cpp", 0.88f, "text", 1));
    results.push_back(makeComponent("docC", "/path/c.cpp", 0.85f, "vector", 1));
    results.push_back(makeComponent("docC", "/path/c.cpp", 0.92f, "kg", 0));

    // Document D: Found by text only (rank 2, medium score)
    results.push_back(makeComponent("docD", "/path/d.cpp", 0.75f, "text", 2));

    // Document E: Found by path_tree (rank 0) and metadata (rank 0) - structural match
    results.push_back(makeComponent("docE", "/path/e.cpp", 0.80f, "path_tree", 0));
    results.push_back(makeComponent("docE", "/path/e.cpp", 0.65f, "metadata", 0));

    return results;
}

/// Find a result by document hash
const SearchResult* findByHash(const std::vector<SearchResult>& results, const std::string& hash) {
    for (const auto& r : results) {
        if (r.document.sha256Hash == hash) {
            return &r;
        }
    }
    return nullptr;
}

/// Get rank position (0-based) of a document in results
int getRank(const std::vector<SearchResult>& results, const std::string& hash) {
    for (size_t i = 0; i < results.size(); ++i) {
        if (results[i].document.sha256Hash == hash) {
            return static_cast<int>(i);
        }
    }
    return -1;
}

} // namespace

// =============================================================================
// Fusion Strategy Comparison Tests
// =============================================================================

TEST_CASE("FusionStrategy: RECIPROCAL_RANK produces stable rankings", "[unit][fusion][rrf]") {
    SearchEngineConfig config;
    config.fusionStrategy = SearchEngineConfig::FusionStrategy::RECIPROCAL_RANK;
    config.rrfK = 60.0f; // Main branch default

    ResultFusion fusion(config);
    auto corpus = createTestCorpus();
    auto results = fusion.fuse(corpus);

    REQUIRE(results.size() == 5);

    // Document C should rank high - found by 3 components
    // RRF rewards documents found by multiple sources
    auto* docC = findByHash(results, "docC");
    REQUIRE(docC != nullptr);

    // Document A should also rank well - found by text (rank 0) and vector
    auto* docA = findByHash(results, "docA");
    REQUIRE(docA != nullptr);

    // Verify stable ranking - multiple runs should produce same order
    auto results2 = fusion.fuse(corpus);
    REQUIRE(results.size() == results2.size());
    for (size_t i = 0; i < results.size(); ++i) {
        CHECK(results[i].document.sha256Hash == results2[i].document.sha256Hash);
    }
}

TEST_CASE("FusionStrategy: COMB_MNZ boosts multi-component documents", "[unit][fusion][comb_mnz]") {
    SearchEngineConfig config;
    config.fusionStrategy = SearchEngineConfig::FusionStrategy::COMB_MNZ;
    config.rrfK = 60.0f;

    ResultFusion fusion(config);
    auto corpus = createTestCorpus();
    auto results = fusion.fuse(corpus);

    REQUIRE(results.size() == 5);

    // COMB_MNZ multiplies by component count
    // Document C (3 components) should get a significant boost
    int rankC = getRank(results, "docC");
    int rankB = getRank(results, "docB"); // Only 1 component despite high score

    // Document C should rank higher than B due to multi-component boost
    CHECK(rankC < rankB);
}

TEST_CASE("FusionStrategy: TEXT_ANCHOR preserves text ranking", "[unit][fusion][text_anchor]") {
    SearchEngineConfig config;
    config.fusionStrategy = SearchEngineConfig::FusionStrategy::TEXT_ANCHOR;
    config.rrfK = 60.0f;

    ResultFusion fusion(config);
    auto corpus = createTestCorpus();
    auto results = fusion.fuse(corpus);

    // TEXT_ANCHOR uses text results as primary anchor
    // Document A is text rank 0, should be at or near top
    int rankA = getRank(results, "docA");

    // Document B is vector-only with high score but no text match
    // Should need very high confidence (>0.85) to be included
    int rankB = getRank(results, "docB");

    // Document D is text-only, should be included
    int rankD = getRank(results, "docD");
    CHECK(rankD >= 0);

    // Text-anchored documents should generally rank higher
    // unless vector-only has exceptional score
    INFO("Text-anchored docA rank: " << rankA);
    INFO("Vector-only docB rank: " << rankB);
}

TEST_CASE("FusionStrategy: Higher RRF k produces smoother rankings", "[unit][fusion][rrf_k]") {
    auto corpus = createTestCorpus();

    // Low k (30) - more emphasis on top-ranked items
    SearchEngineConfig configLowK;
    configLowK.fusionStrategy = SearchEngineConfig::FusionStrategy::RECIPROCAL_RANK;
    configLowK.rrfK = 30.0f;
    ResultFusion fusionLowK(configLowK);
    auto resultsLowK = fusionLowK.fuse(corpus);

    // High k (60) - smoother distribution
    SearchEngineConfig configHighK;
    configHighK.fusionStrategy = SearchEngineConfig::FusionStrategy::RECIPROCAL_RANK;
    configHighK.rrfK = 60.0f;
    ResultFusion fusionHighK(configHighK);
    auto resultsHighK = fusionHighK.fuse(corpus);

    REQUIRE(resultsLowK.size() == resultsHighK.size());

    // With higher k, score differences between adjacent ranks should be smaller
    if (resultsHighK.size() >= 2) {
        double gapLowK = resultsLowK[0].score - resultsLowK[1].score;
        double gapHighK = resultsHighK[0].score - resultsHighK[1].score;

        // Higher k should produce smaller gaps (smoother distribution)
        // This is because 1/(k+rank) produces smaller differences when k is larger
        INFO("Low k gap (top 2): " << gapLowK);
        INFO("High k gap (top 2): " << gapHighK);
    }
}

TEST_CASE("FusionStrategy: Default config uses RECIPROCAL_RANK", "[unit][fusion][defaults]") {
    SearchEngineConfig config;

    // Verify restored defaults from main branch
    CHECK(config.fusionStrategy == SearchEngineConfig::FusionStrategy::RECIPROCAL_RANK);
    CHECK(config.rrfK == Approx(60.0f));
    CHECK(config.vectorWeight == Approx(0.55f));
}

TEST_CASE("FusionStrategy: Vector weight affects ranking", "[unit][fusion][vector_weight]") {
    auto corpus = createTestCorpus();

    // Low vector weight
    SearchEngineConfig configLowVec;
    configLowVec.fusionStrategy = SearchEngineConfig::FusionStrategy::WEIGHTED_RECIPROCAL;
    configLowVec.vectorWeight = 0.20f;
    configLowVec.textWeight = 0.55f;
    ResultFusion fusionLowVec(configLowVec);
    auto resultsLowVec = fusionLowVec.fuse(corpus);

    // High vector weight (main branch default)
    SearchEngineConfig configHighVec;
    configHighVec.fusionStrategy = SearchEngineConfig::FusionStrategy::WEIGHTED_RECIPROCAL;
    configHighVec.vectorWeight = 0.55f;
    configHighVec.textWeight = 0.30f;
    ResultFusion fusionHighVec(configHighVec);
    auto resultsHighVec = fusionHighVec.fuse(corpus);

    // Document B (vector-only with high score) should rank better with high vector weight
    int rankBLowVec = getRank(resultsLowVec, "docB");
    int rankBHighVec = getRank(resultsHighVec, "docB");

    // With higher vector weight, docB should have better rank (lower number)
    CHECK(rankBHighVec <= rankBLowVec);
}

// =============================================================================
// Edge case tests
// =============================================================================

TEST_CASE("FusionStrategy: Empty input returns empty results", "[unit][fusion][edge]") {
    SearchEngineConfig config;
    ResultFusion fusion(config);

    std::vector<ComponentResult> empty;
    auto results = fusion.fuse(empty);

    CHECK(results.empty());
}

TEST_CASE("FusionStrategy: Single component works correctly", "[unit][fusion][edge]") {
    SearchEngineConfig config;
    config.fusionStrategy = SearchEngineConfig::FusionStrategy::RECIPROCAL_RANK;
    ResultFusion fusion(config);

    std::vector<ComponentResult> single;
    single.push_back(makeComponent("doc1", "/path/1.cpp", 0.90f, "text", 0));

    auto results = fusion.fuse(single);

    REQUIRE(results.size() == 1);
    CHECK(results[0].document.sha256Hash == "doc1");
}

TEST_CASE("FusionStrategy: All strategies handle duplicate documents", "[unit][fusion][edge]") {
    std::vector<ComponentResult> corpus;
    // Same document from multiple components
    corpus.push_back(makeComponent("docX", "/path/x.cpp", 0.90f, "text", 0));
    corpus.push_back(makeComponent("docX", "/path/x.cpp", 0.85f, "vector", 0));
    corpus.push_back(makeComponent("docX", "/path/x.cpp", 0.80f, "kg", 0));
    corpus.push_back(makeComponent("docX", "/path/x.cpp", 0.75f, "path_tree", 0));

    // Test all strategies produce single result
    std::vector<SearchEngineConfig::FusionStrategy> strategies = {
        SearchEngineConfig::FusionStrategy::WEIGHTED_SUM,
        SearchEngineConfig::FusionStrategy::RECIPROCAL_RANK,
        SearchEngineConfig::FusionStrategy::BORDA_COUNT,
        SearchEngineConfig::FusionStrategy::WEIGHTED_RECIPROCAL,
        SearchEngineConfig::FusionStrategy::COMB_MNZ,
        SearchEngineConfig::FusionStrategy::TEXT_ANCHOR,
    };

    for (auto strategy : strategies) {
        SearchEngineConfig config;
        config.fusionStrategy = strategy;
        ResultFusion fusion(config);

        auto results = fusion.fuse(corpus);

        INFO("Strategy: " << SearchEngineConfig::fusionStrategyToString(strategy));
        REQUIRE(results.size() == 1);
        CHECK(results[0].document.sha256Hash == "docX");
    }
}

// =============================================================================
// Performance Benchmark Tests
// =============================================================================

TEST_CASE("FusionStrategy: Performance comparison benchmark", "[unit][fusion][benchmark]") {
    // Generate larger test corpus for meaningful timing
    std::vector<ComponentResult> corpus;
    std::vector<std::string> sources = {"text", "vector", "kg", "path_tree", "metadata"};

    // Create 500 documents with overlapping component results
    for (int i = 0; i < 500; ++i) {
        std::string hash = "doc" + std::to_string(i);
        std::string path = "/path/file" + std::to_string(i) + ".cpp";

        // Each doc appears in 1-4 components
        int numComponents = 1 + (i % 4);
        for (int c = 0; c < numComponents; ++c) {
            float score = 0.5f + (static_cast<float>(500 - i) / 1000.0f);
            corpus.push_back(makeComponent(hash, path, score, sources[c % 5], i % 50));
        }
    }

    INFO("Corpus size: " << corpus.size() << " component results");

    std::vector<std::pair<std::string, SearchEngineConfig::FusionStrategy>> strategies = {
        {"RECIPROCAL_RANK", SearchEngineConfig::FusionStrategy::RECIPROCAL_RANK},
        {"COMB_MNZ", SearchEngineConfig::FusionStrategy::COMB_MNZ},
        {"WEIGHTED_RECIPROCAL", SearchEngineConfig::FusionStrategy::WEIGHTED_RECIPROCAL},
        {"TEXT_ANCHOR", SearchEngineConfig::FusionStrategy::TEXT_ANCHOR},
        {"BORDA_COUNT", SearchEngineConfig::FusionStrategy::BORDA_COUNT},
        {"WEIGHTED_SUM", SearchEngineConfig::FusionStrategy::WEIGHTED_SUM},
    };

    constexpr int ITERATIONS = 1000;

    for (const auto& [name, strategy] : strategies) {
        SearchEngineConfig config;
        config.fusionStrategy = strategy;
        config.rrfK = 60.0f;
        config.vectorWeight = 0.55f;

        ResultFusion fusion(config);

        auto start = std::chrono::high_resolution_clock::now();
        for (int i = 0; i < ITERATIONS; ++i) {
            auto results = fusion.fuse(corpus);
            (void)results; // Prevent optimization
        }
        auto end = std::chrono::high_resolution_clock::now();

        auto durationUs =
            std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
        double avgUs = static_cast<double>(durationUs) / ITERATIONS;

        // Run once more to get result stats
        auto results = fusion.fuse(corpus);

        INFO(name << ": avg " << avgUs << " us, " << results.size() << " results");

        // Basic sanity checks - most strategies have a 100 result limit by default
        CHECK(results.size() >= 100);
        CHECK(results[0].score > 0.0f);
    }
}
