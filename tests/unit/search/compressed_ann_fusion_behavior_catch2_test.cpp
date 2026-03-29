// Copyright 2025 YAMS Authors
// SPDX-License-Identifier: GPL-3.0-or-later OR MIT

/**
 * @file compressed_ann_fusion_behavior_catch2_test.cpp
 * @brief Behavior tests for CompressedANN in result fusion
 *
 * Tests that verify the behavioral properties of CompressedANN when
 * mixed with other component sources in the fusion pipeline.
 *
 * Key behaviors tested:
 * 1. Doc B (compressed ANN only) remains a novel candidate
 * 2. Doc A with both vector and compressed ANN is not over-counted
 * 3. compressedAnnWeight influences ranking through the CompressedANN source path
 */

#include <catch2/catch_approx.hpp>
#include <catch2/catch_test_macros.hpp>

#include <yams/search/search_engine.h>

#include <algorithm>
#include <cmath>
#include <set>

using yams::metadata::SearchResult;
using yams::search::ComponentResult;
using yams::search::ResultFusion;
using yams::search::SearchEngineConfig;

namespace {

ComponentResult makeComponent(std::string hash, float score, ComponentResult::Source source,
                              size_t rank = 0) {
    ComponentResult c;
    c.documentHash = std::move(hash);
    c.filePath = c.documentHash;
    c.score = score;
    c.source = source;
    c.rank = rank;
    return c;
}

} // namespace

TEST_CASE("CompressedANN for doc B survives fusion as novel candidate",
          "[search][fusion][compressed_ann][behavior]") {
    SearchEngineConfig cfg;
    cfg.maxResults = 10;
    cfg.fusionStrategy = SearchEngineConfig::FusionStrategy::WEIGHTED_SUM;
    cfg.vectorWeight = 1.0f;
    cfg.textWeight = 1.0f;
    cfg.compressedAnnWeight = 0.5f;
    cfg.vectorOnlyThreshold = 0.0f; // Disable vector-only filtering for this test

    ResultFusion fusion(cfg);
    std::vector<ComponentResult> components;

    // Doc A: exact vector hit
    components.push_back(makeComponent("doc-A", 0.80f, ComponentResult::Source::Vector, 0));
    // Doc A: compressed ANN hit (same doc)
    components.push_back(makeComponent("doc-A", 0.75f, ComponentResult::Source::CompressedANN, 1));
    // Doc B: compressed ANN hit (novel - no exact vector match)
    components.push_back(makeComponent("doc-B", 0.70f, ComponentResult::Source::CompressedANN, 2));

    auto results = fusion.fuse(components);

    // Doc B should survive fusion as a unique candidate
    std::set<std::string> resultDocs;
    for (const auto& r : results) {
        resultDocs.insert(r.document.sha256Hash);
    }

    CHECK(resultDocs.count("doc-A") == 1);
    CHECK(resultDocs.count("doc-B") == 1);
}

TEST_CASE("Doc A with both vector and compressed ANN is not over-counted",
          "[search][fusion][compressed_ann][behavior]") {
    SearchEngineConfig cfg;
    cfg.maxResults = 10;
    cfg.fusionStrategy = SearchEngineConfig::FusionStrategy::WEIGHTED_SUM;
    cfg.vectorWeight = 1.0f;
    cfg.textWeight = 1.0f;
    cfg.compressedAnnWeight = 0.5f;
    cfg.vectorOnlyThreshold = 0.0f; // Disable vector-only filtering for this test

    ResultFusion fusion(cfg);
    std::vector<ComponentResult> components;

    // Doc A: exact vector hit at rank 0
    components.push_back(makeComponent("doc-A", 0.90f, ComponentResult::Source::Vector, 0));
    // Doc A: compressed ANN hit at rank 1
    components.push_back(makeComponent("doc-A", 0.85f, ComponentResult::Source::CompressedANN, 1));

    auto results = fusion.fuse(components);

    // Doc A should appear exactly once
    REQUIRE(results.size() == 1);
    CHECK(results[0].document.sha256Hash == "doc-A");
}

TEST_CASE("CompressedANN contributes to vectorScore via accumulation",
          "[search][fusion][compressed_ann][behavior]") {
    SearchEngineConfig cfg;
    cfg.maxResults = 10;
    cfg.fusionStrategy = SearchEngineConfig::FusionStrategy::WEIGHTED_SUM;
    cfg.vectorWeight = 1.0f;
    cfg.compressedAnnWeight = 0.5f;
    cfg.vectorOnlyThreshold = 0.0f; // Disable vector-only filtering for this test

    ResultFusion fusion(cfg);
    std::vector<ComponentResult> components;

    // Doc A: vector only
    components.push_back(makeComponent("doc-A", 0.80f, ComponentResult::Source::Vector, 0));
    // Doc B: compressed ANN only
    components.push_back(makeComponent("doc-B", 0.80f, ComponentResult::Source::CompressedANN, 0));

    auto results = fusion.fuse(components);

    REQUIRE(results.size() == 2);

    // Find doc A and doc B in results
    const SearchResult* docA = nullptr;
    const SearchResult* docB = nullptr;
    for (const auto& r : results) {
        if (r.document.sha256Hash == "doc-A") {
            docA = &r;
        } else if (r.document.sha256Hash == "doc-B") {
            docB = &r;
        }
    }

    REQUIRE(docA != nullptr);
    REQUIRE(docB != nullptr);

    // Both should have vectorScore populated (CompressedANN routes to vectorScore)
    CHECK(docA->vectorScore.has_value() == true);
    CHECK(docB->vectorScore.has_value() == true);
}

TEST_CASE("Higher compressedAnnWeight improves compressed ANN doc ranking",
          "[search][fusion][compressed_ann][behavior]") {
    // Config with low compressedAnnWeight
    SearchEngineConfig cfgLow;
    cfgLow.maxResults = 10;
    cfgLow.fusionStrategy = SearchEngineConfig::FusionStrategy::WEIGHTED_SUM;
    cfgLow.vectorWeight = 1.0f;
    cfgLow.compressedAnnWeight = 0.1f; // Low weight
    cfgLow.vectorOnlyThreshold = 0.0f; // Disable vector-only filtering

    ResultFusion fusionLow(cfgLow);
    std::vector<ComponentResult> componentsLow;
    // Doc A: high-scoring compressed ANN
    componentsLow.push_back(
        makeComponent("doc-A", 0.95f, ComponentResult::Source::CompressedANN, 0));
    // Doc B: medium-scoring text
    componentsLow.push_back(makeComponent("doc-B", 0.80f, ComponentResult::Source::Text, 0));

    auto resultsLow = fusionLow.fuse(componentsLow);

    // Config with high compressedAnnWeight
    SearchEngineConfig cfgHigh;
    cfgHigh.maxResults = 10;
    cfgHigh.fusionStrategy = SearchEngineConfig::FusionStrategy::WEIGHTED_SUM;
    cfgHigh.vectorWeight = 1.0f;
    cfgHigh.compressedAnnWeight = 1.0f; // High weight
    cfgHigh.textWeight = 1.0f;
    cfgHigh.vectorOnlyThreshold = 0.0f; // Disable vector-only filtering

    ResultFusion fusionHigh(cfgHigh);
    std::vector<ComponentResult> componentsHigh;
    // Same docs
    componentsHigh.push_back(
        makeComponent("doc-A", 0.95f, ComponentResult::Source::CompressedANN, 0));
    componentsHigh.push_back(makeComponent("doc-B", 0.80f, ComponentResult::Source::Text, 0));

    auto resultsHigh = fusionHigh.fuse(componentsHigh);

    REQUIRE(resultsLow.size() >= 1);
    REQUIRE(resultsHigh.size() >= 1);

    // Doc A should rank higher with higher compressedAnnWeight
    size_t rankLow = std::numeric_limits<size_t>::max();
    size_t rankHigh = std::numeric_limits<size_t>::max();

    for (size_t i = 0; i < resultsLow.size(); ++i) {
        if (resultsLow[i].document.sha256Hash == "doc-A") {
            rankLow = i;
            break;
        }
    }

    for (size_t i = 0; i < resultsHigh.size(); ++i) {
        if (resultsHigh[i].document.sha256Hash == "doc-A") {
            rankHigh = i;
            break;
        }
    }

    // Higher compressedAnnWeight should improve doc-A's relative rank
    CHECK(rankHigh <= rankLow);
}

TEST_CASE("Text hit for doc A combines with vector hits without duplication",
          "[search][fusion][compressed_ann][behavior]") {
    SearchEngineConfig cfg;
    cfg.maxResults = 10;
    cfg.fusionStrategy = SearchEngineConfig::FusionStrategy::WEIGHTED_SUM;
    cfg.vectorWeight = 1.0f;
    cfg.textWeight = 1.0f;
    cfg.compressedAnnWeight = 0.5f;
    cfg.vectorOnlyThreshold = 0.0f; // Disable vector-only filtering for this test

    ResultFusion fusion(cfg);
    std::vector<ComponentResult> components;

    // Doc A: exact vector hit
    components.push_back(makeComponent("doc-A", 0.80f, ComponentResult::Source::Vector, 0));
    // Doc A: compressed ANN hit (should NOT duplicate)
    components.push_back(makeComponent("doc-A", 0.75f, ComponentResult::Source::CompressedANN, 1));
    // Doc A: text hit
    components.push_back(makeComponent("doc-A", 0.70f, ComponentResult::Source::Text, 0));

    auto results = fusion.fuse(components);

    // Doc A should appear exactly once
    REQUIRE(results.size() == 1);
    CHECK(results[0].document.sha256Hash == "doc-A");

    // Doc A should have both vectorScore and keywordScore
    CHECK(results[0].vectorScore.has_value() == true);
    CHECK(results[0].keywordScore.has_value() == true);
}

TEST_CASE("Multiple compressed ANN hits for same doc collapse to one",
          "[search][fusion][compressed_ann][behavior]") {
    SearchEngineConfig cfg;
    cfg.maxResults = 10;
    cfg.fusionStrategy = SearchEngineConfig::FusionStrategy::WEIGHTED_SUM;
    cfg.compressedAnnWeight = 0.5f;
    cfg.vectorOnlyThreshold = 0.0f; // Disable vector-only filtering for this test

    ResultFusion fusion(cfg);
    std::vector<ComponentResult> components;

    // Doc A: multiple compressed ANN hits (ranked 0, 1, 2)
    components.push_back(makeComponent("doc-A", 0.95f, ComponentResult::Source::CompressedANN, 0));
    components.push_back(makeComponent("doc-A", 0.85f, ComponentResult::Source::CompressedANN, 1));
    components.push_back(makeComponent("doc-A", 0.75f, ComponentResult::Source::CompressedANN, 2));

    auto results = fusion.fuse(components);

    // Doc A should appear exactly once
    REQUIRE(results.size() == 1);
    CHECK(results[0].document.sha256Hash == "doc-A");
}

TEST_CASE("Compressed ANN with RRF fusion maintains vector-family semantics",
          "[search][fusion][compressed_ann][behavior]") {
    SearchEngineConfig cfg;
    cfg.maxResults = 10;
    cfg.fusionStrategy = SearchEngineConfig::FusionStrategy::WEIGHTED_RECIPROCAL;
    cfg.rrfK = 1.0f;
    cfg.vectorWeight = 1.0f;
    cfg.compressedAnnWeight = 0.5f;
    cfg.textWeight = 1.0f;
    cfg.vectorOnlyThreshold = 0.0f; // Disable vector-only filtering for this test

    ResultFusion fusion(cfg);
    std::vector<ComponentResult> components;

    // Doc A: vector at rank 0
    components.push_back(makeComponent("doc-A", 0.90f, ComponentResult::Source::Vector, 0));
    // Doc A: compressed ANN at rank 1
    components.push_back(makeComponent("doc-A", 0.85f, ComponentResult::Source::CompressedANN, 1));
    // Doc B: compressed ANN at rank 0
    components.push_back(makeComponent("doc-B", 0.80f, ComponentResult::Source::CompressedANN, 0));

    auto results = fusion.fuse(components);

    // Both docs should appear
    REQUIRE(results.size() == 2);

    std::set<std::string> resultDocs;
    for (const auto& r : results) {
        resultDocs.insert(r.document.sha256Hash);
    }

    CHECK(resultDocs.count("doc-A") == 1);
    CHECK(resultDocs.count("doc-B") == 1);
}
