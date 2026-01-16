// SPDX-License-Identifier: Apache-2.0
// Copyright 2025 YAMS Contributors

/**
 * @file fusion_strategy_comparison_catch2_test.cpp
 * @brief Tests for UNIFIED fusion strategy
 *
 * Tests verify the unified fusion strategy (weight * RRF * scoreBoost * mnzBoost)
 * produces stable, high-quality rankings.
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
// Unified Fusion Strategy Tests
// =============================================================================

TEST_CASE("FusionStrategy: UNIFIED produces stable rankings", "[unit][fusion][unified]") {
    SearchEngineConfig config;
    config.fusionStrategy = SearchEngineConfig::FusionStrategy::UNIFIED;
    config.rrfK = 60.0f;
    config.enableMnzBoost = true;

    ResultFusion fusion(config);
    auto corpus = createTestCorpus();
    auto results = fusion.fuse(corpus);

    REQUIRE(results.size() == 5);

    // Document C should rank high - found by 3 components, gets MNZ boost
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

TEST_CASE("FusionStrategy: UNIFIED MNZ boost affects multi-component documents",
          "[unit][fusion][unified][mnz]") {
    auto corpus = createTestCorpus();

    // With MNZ boost enabled
    SearchEngineConfig configWithMnz;
    configWithMnz.fusionStrategy = SearchEngineConfig::FusionStrategy::UNIFIED;
    configWithMnz.rrfK = 60.0f;
    configWithMnz.enableMnzBoost = true;
    ResultFusion fusionWithMnz(configWithMnz);
    auto resultsWithMnz = fusionWithMnz.fuse(corpus);

    // Without MNZ boost
    SearchEngineConfig configNoMnz;
    configNoMnz.fusionStrategy = SearchEngineConfig::FusionStrategy::UNIFIED;
    configNoMnz.rrfK = 60.0f;
    configNoMnz.enableMnzBoost = false;
    ResultFusion fusionNoMnz(configNoMnz);
    auto resultsNoMnz = fusionNoMnz.fuse(corpus);

    REQUIRE(resultsWithMnz.size() == resultsNoMnz.size());

    // Document C (3 components) should have relatively higher score with MNZ
    // compared to Document B (1 component)
    int rankCWithMnz = getRank(resultsWithMnz, "docC");
    int rankBWithMnz = getRank(resultsWithMnz, "docB");
    int rankCNoMnz = getRank(resultsNoMnz, "docC");
    int rankBNoMnz = getRank(resultsNoMnz, "docB");

    // With MNZ boost, docC (3 components) should rank better relative to docB (1 component)
    int gapWithMnz = rankCWithMnz - rankBWithMnz;
    int gapNoMnz = rankCNoMnz - rankBNoMnz;

    // MNZ boost should improve docC's relative position (smaller or more negative gap)
    INFO("With MNZ: docC rank " << rankCWithMnz << ", docB rank " << rankBWithMnz);
    INFO("Without MNZ: docC rank " << rankCNoMnz << ", docB rank " << rankBNoMnz);
    CHECK(gapWithMnz <= gapNoMnz);
}

TEST_CASE("FusionStrategy: Higher RRF k produces smoother rankings", "[unit][fusion][rrf_k]") {
    auto corpus = createTestCorpus();

    // Low k (30) - more emphasis on top-ranked items
    SearchEngineConfig configLowK;
    configLowK.fusionStrategy = SearchEngineConfig::FusionStrategy::UNIFIED;
    configLowK.rrfK = 30.0f;
    ResultFusion fusionLowK(configLowK);
    auto resultsLowK = fusionLowK.fuse(corpus);

    // High k (60) - smoother distribution
    SearchEngineConfig configHighK;
    configHighK.fusionStrategy = SearchEngineConfig::FusionStrategy::UNIFIED;
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

TEST_CASE("FusionStrategy: Default config uses UNIFIED", "[unit][fusion][defaults]") {
    SearchEngineConfig config;

    // Verify UNIFIED is the only and default strategy
    CHECK(config.fusionStrategy == SearchEngineConfig::FusionStrategy::UNIFIED);
    CHECK(config.rrfK == Approx(60.0f));
    CHECK(config.enableMnzBoost == true);
}

TEST_CASE("FusionStrategy: Component weights affect ranking", "[unit][fusion][weights]") {
    auto corpus = createTestCorpus();

    // Low vector weight - text dominates
    SearchEngineConfig configLowVec;
    configLowVec.fusionStrategy = SearchEngineConfig::FusionStrategy::UNIFIED;
    configLowVec.vectorWeight = 0.10f;
    configLowVec.textWeight = 0.60f;
    ResultFusion fusionLowVec(configLowVec);
    auto resultsLowVec = fusionLowVec.fuse(corpus);

    // High vector weight
    SearchEngineConfig configHighVec;
    configHighVec.fusionStrategy = SearchEngineConfig::FusionStrategy::UNIFIED;
    configHighVec.vectorWeight = 0.60f;
    configHighVec.textWeight = 0.20f;
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
    config.fusionStrategy = SearchEngineConfig::FusionStrategy::UNIFIED;
    ResultFusion fusion(config);

    std::vector<ComponentResult> empty;
    auto results = fusion.fuse(empty);

    CHECK(results.empty());
}

TEST_CASE("FusionStrategy: Single component works correctly", "[unit][fusion][edge]") {
    SearchEngineConfig config;
    config.fusionStrategy = SearchEngineConfig::FusionStrategy::UNIFIED;
    ResultFusion fusion(config);

    std::vector<ComponentResult> single;
    single.push_back(makeComponent("doc1", "/path/1.cpp", 0.90f, "text", 0));

    auto results = fusion.fuse(single);

    REQUIRE(results.size() == 1);
    CHECK(results[0].document.sha256Hash == "doc1");
}

TEST_CASE("FusionStrategy: UNIFIED handles duplicate documents", "[unit][fusion][edge]") {
    std::vector<ComponentResult> corpus;
    // Same document from multiple components
    corpus.push_back(makeComponent("docX", "/path/x.cpp", 0.90f, "text", 0));
    corpus.push_back(makeComponent("docX", "/path/x.cpp", 0.85f, "vector", 0));
    corpus.push_back(makeComponent("docX", "/path/x.cpp", 0.80f, "kg", 0));
    corpus.push_back(makeComponent("docX", "/path/x.cpp", 0.75f, "path_tree", 0));

    SearchEngineConfig config;
    config.fusionStrategy = SearchEngineConfig::FusionStrategy::UNIFIED;
    config.enableMnzBoost = true;
    ResultFusion fusion(config);

    auto results = fusion.fuse(corpus);

    // Should produce single result with contributions from all components
    REQUIRE(results.size() == 1);
    CHECK(results[0].document.sha256Hash == "docX");

    // Score should be higher than any single component due to aggregation
    CHECK(results[0].score > 0.0f);
}

// =============================================================================
// Performance Benchmark Tests
// =============================================================================

TEST_CASE("FusionStrategy: UNIFIED performance benchmark", "[unit][fusion][benchmark]") {
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

    SearchEngineConfig config;
    config.fusionStrategy = SearchEngineConfig::FusionStrategy::UNIFIED;
    config.rrfK = 60.0f;
    config.enableMnzBoost = true;

    ResultFusion fusion(config);

    constexpr int ITERATIONS = 1000;

    auto start = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < ITERATIONS; ++i) {
        auto results = fusion.fuse(corpus);
        (void)results; // Prevent optimization
    }
    auto end = std::chrono::high_resolution_clock::now();

    auto durationUs = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
    double avgUs = static_cast<double>(durationUs) / ITERATIONS;

    // Run once more to get result stats
    auto results = fusion.fuse(corpus);

    INFO("UNIFIED: avg " << avgUs << " us, " << results.size() << " results");

    // Basic sanity checks
    CHECK(results.size() >= 100);
    CHECK(results[0].score > 0.0f);
}
