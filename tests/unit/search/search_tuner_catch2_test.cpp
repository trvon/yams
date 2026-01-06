// SPDX-License-Identifier: Apache-2.0
// Copyright 2025 YAMS Contributors

/**
 * @file search_tuner_catch2_test.cpp
 * @brief Unit tests for SearchTuner FSM (Phase 2 of Epic yams-7ez4)
 *
 * Tests cover:
 * - TuningState enum and string conversion
 * - TunedParams for each state (parameter values from matrix)
 * - SearchTuner FSM state transitions based on corpus stats
 * - SearchEngineConfig generation from tuned params
 * - JSON serialization for observability
 */

#include <catch2/catch_approx.hpp>
#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_floating_point.hpp>

#include <yams/search/search_tuner.h>
#include <yams/storage/corpus_stats.h>

using namespace yams::search;
using namespace yams::storage;
using Catch::Approx;

// =============================================================================
// TuningState enum and string conversion tests
// =============================================================================

TEST_CASE("TuningState: string conversion", "[unit][search_tuner]") {
    CHECK(std::string(tuningStateToString(TuningState::SMALL_CODE)) == "SMALL_CODE");
    CHECK(std::string(tuningStateToString(TuningState::LARGE_CODE)) == "LARGE_CODE");
    CHECK(std::string(tuningStateToString(TuningState::SMALL_PROSE)) == "SMALL_PROSE");
    CHECK(std::string(tuningStateToString(TuningState::LARGE_PROSE)) == "LARGE_PROSE");
    CHECK(std::string(tuningStateToString(TuningState::SCIENTIFIC)) == "SCIENTIFIC");
    CHECK(std::string(tuningStateToString(TuningState::MIXED)) == "MIXED");
    CHECK(std::string(tuningStateToString(TuningState::MINIMAL)) == "MINIMAL");
}

// =============================================================================
// TunedParams tests - verify parameter matrix values
// =============================================================================

TEST_CASE("TunedParams: SMALL_CODE parameters", "[unit][search_tuner][params]") {
    auto params = getTunedParams(TuningState::SMALL_CODE);

    CHECK(params.rrfK == 20);
    CHECK(params.textWeight == Approx(0.50f));
    CHECK(params.vectorWeight == Approx(0.15f));
    CHECK(params.pathTreeWeight == Approx(0.20f));
    CHECK(params.kgWeight == Approx(0.10f));
    CHECK(params.tagWeight == Approx(0.03f));
    CHECK(params.metadataWeight == Approx(0.02f));
}

TEST_CASE("TunedParams: LARGE_CODE parameters", "[unit][search_tuner][params]") {
    auto params = getTunedParams(TuningState::LARGE_CODE);

    CHECK(params.rrfK == 60);
    CHECK(params.textWeight == Approx(0.45f));
    CHECK(params.vectorWeight == Approx(0.20f));
    CHECK(params.pathTreeWeight == Approx(0.15f));
    CHECK(params.kgWeight == Approx(0.10f));
    CHECK(params.tagWeight == Approx(0.05f));
    CHECK(params.metadataWeight == Approx(0.05f));
}

TEST_CASE("TunedParams: SMALL_PROSE parameters", "[unit][search_tuner][params]") {
    auto params = getTunedParams(TuningState::SMALL_PROSE);

    CHECK(params.rrfK == 25);
    CHECK(params.textWeight == Approx(0.50f));
    CHECK(params.vectorWeight == Approx(0.40f));
    CHECK(params.pathTreeWeight == Approx(0.00f));
    CHECK(params.kgWeight == Approx(0.00f));
    CHECK(params.tagWeight == Approx(0.05f));
    CHECK(params.metadataWeight == Approx(0.05f));
}

TEST_CASE("TunedParams: LARGE_PROSE parameters", "[unit][search_tuner][params]") {
    auto params = getTunedParams(TuningState::LARGE_PROSE);

    CHECK(params.rrfK == 60);
    CHECK(params.textWeight == Approx(0.40f));
    CHECK(params.vectorWeight == Approx(0.45f));
    CHECK(params.pathTreeWeight == Approx(0.05f));
    CHECK(params.kgWeight == Approx(0.00f));
    CHECK(params.tagWeight == Approx(0.05f));
    CHECK(params.metadataWeight == Approx(0.05f));
}

TEST_CASE("TunedParams: SCIENTIFIC parameters", "[unit][search_tuner][params]") {
    auto params = getTunedParams(TuningState::SCIENTIFIC);

    CHECK(params.rrfK == 30);
    CHECK(params.textWeight == Approx(0.55f));
    CHECK(params.vectorWeight == Approx(0.40f));
    CHECK(params.pathTreeWeight == Approx(0.00f));
    CHECK(params.kgWeight == Approx(0.00f));
    CHECK(params.tagWeight == Approx(0.00f));
    CHECK(params.metadataWeight == Approx(0.05f));
}

TEST_CASE("TunedParams: MIXED parameters", "[unit][search_tuner][params]") {
    auto params = getTunedParams(TuningState::MIXED);

    CHECK(params.rrfK == 45);
    CHECK(params.textWeight == Approx(0.45f));
    CHECK(params.vectorWeight == Approx(0.25f));
    CHECK(params.pathTreeWeight == Approx(0.10f));
    CHECK(params.kgWeight == Approx(0.10f));
    CHECK(params.tagWeight == Approx(0.05f));
    CHECK(params.metadataWeight == Approx(0.05f));
}

TEST_CASE("TunedParams: MINIMAL parameters", "[unit][search_tuner][params]") {
    auto params = getTunedParams(TuningState::MINIMAL);

    CHECK(params.rrfK == 15);
    CHECK(params.textWeight == Approx(0.60f));
    CHECK(params.vectorWeight == Approx(0.30f));
    CHECK(params.pathTreeWeight == Approx(0.05f));
    CHECK(params.kgWeight == Approx(0.00f));
    CHECK(params.tagWeight == Approx(0.03f));
    CHECK(params.metadataWeight == Approx(0.02f));
}

TEST_CASE("TunedParams: JSON serialization", "[unit][search_tuner][params]") {
    auto params = getTunedParams(TuningState::SMALL_CODE);
    auto json = params.toJson();

    CHECK(json["rrf_k"] == 20);
    CHECK(json["text_weight"].get<float>() == Approx(0.50f));
    CHECK(json["vector_weight"].get<float>() == Approx(0.15f));
    CHECK(json["path_tree_weight"].get<float>() == Approx(0.20f));
    CHECK(json["kg_weight"].get<float>() == Approx(0.10f));
    CHECK(json["tag_weight"].get<float>() == Approx(0.03f));
    CHECK(json["metadata_weight"].get<float>() == Approx(0.02f));
}

TEST_CASE("TunedParams: applyTo SearchEngineConfig", "[unit][search_tuner][params]") {
    auto params = getTunedParams(TuningState::LARGE_PROSE);

    SearchEngineConfig config;
    params.applyTo(config);

    CHECK(config.textWeight == Approx(0.40f));
    CHECK(config.vectorWeight == Approx(0.45f));
    CHECK(config.pathTreeWeight == Approx(0.05f));
    CHECK(config.kgWeight == Approx(0.00f));
    CHECK(config.tagWeight == Approx(0.05f));
    CHECK(config.metadataWeight == Approx(0.05f));
    CHECK(config.similarityThreshold == Approx(0.65f));
}

// =============================================================================
// SearchTuner FSM state transition tests
// =============================================================================

TEST_CASE("SearchTuner: MINIMAL state for empty corpus", "[unit][search_tuner][fsm]") {
    CorpusStats stats;
    stats.docCount = 0;

    auto state = SearchTuner::computeState(stats);
    CHECK(state == TuningState::MINIMAL);
}

TEST_CASE("SearchTuner: MINIMAL state for very small corpus", "[unit][search_tuner][fsm]") {
    CorpusStats stats;
    stats.docCount = 50; // < 100

    auto state = SearchTuner::computeState(stats);
    CHECK(state == TuningState::MINIMAL);
}

TEST_CASE("SearchTuner: MINIMAL state boundary (99 docs)", "[unit][search_tuner][fsm]") {
    CorpusStats stats;
    stats.docCount = 99;

    auto state = SearchTuner::computeState(stats);
    CHECK(state == TuningState::MINIMAL);
}

TEST_CASE("SearchTuner: SMALL_CODE state", "[unit][search_tuner][fsm]") {
    CorpusStats stats;
    stats.docCount = 500;   // 100 <= docs < 1000
    stats.codeRatio = 0.8f; // > 0.7 (code dominant)
    stats.proseRatio = 0.15f;
    stats.binaryRatio = 0.05f;

    auto state = SearchTuner::computeState(stats);
    CHECK(state == TuningState::SMALL_CODE);
}

TEST_CASE("SearchTuner: LARGE_CODE state", "[unit][search_tuner][fsm]") {
    CorpusStats stats;
    stats.docCount = 5000;   // >= 1000
    stats.codeRatio = 0.85f; // > 0.7 (code dominant)
    stats.proseRatio = 0.10f;
    stats.binaryRatio = 0.05f;

    auto state = SearchTuner::computeState(stats);
    CHECK(state == TuningState::LARGE_CODE);
}

TEST_CASE("SearchTuner: SMALL_PROSE state", "[unit][search_tuner][fsm]") {
    CorpusStats stats;
    stats.docCount = 500; // 100 <= docs < 1000
    stats.codeRatio = 0.1f;
    stats.proseRatio = 0.85f; // > 0.7 (prose dominant)
    stats.binaryRatio = 0.05f;
    stats.pathDepthAvg = 3.0f; // Not flat (avoid SCIENTIFIC)
    stats.tagCoverage = 0.3f;  // Has tags (avoid SCIENTIFIC)

    auto state = SearchTuner::computeState(stats);
    CHECK(state == TuningState::SMALL_PROSE);
}

TEST_CASE("SearchTuner: LARGE_PROSE state", "[unit][search_tuner][fsm]") {
    CorpusStats stats;
    stats.docCount = 5000; // >= 1000
    stats.codeRatio = 0.1f;
    stats.proseRatio = 0.85f; // > 0.7 (prose dominant)
    stats.binaryRatio = 0.05f;
    stats.pathDepthAvg = 3.0f; // Not flat
    stats.tagCoverage = 0.3f;  // Has tags

    auto state = SearchTuner::computeState(stats);
    CHECK(state == TuningState::LARGE_PROSE);
}

TEST_CASE("SearchTuner: SCIENTIFIC state", "[unit][search_tuner][fsm]") {
    CorpusStats stats;
    stats.docCount = 2000; // >= 100
    stats.codeRatio = 0.05f;
    stats.proseRatio = 0.90f; // > 0.7 (prose dominant)
    stats.binaryRatio = 0.05f;
    stats.pathDepthAvg = 1.0f; // Flat paths (< 1.5)
    stats.tagCoverage = 0.02f; // No tags (< 0.1)

    auto state = SearchTuner::computeState(stats);
    CHECK(state == TuningState::SCIENTIFIC);
}

TEST_CASE("SearchTuner: MIXED state for balanced corpus", "[unit][search_tuner][fsm]") {
    CorpusStats stats;
    stats.docCount = 1000;
    stats.codeRatio = 0.4f;  // Neither > 0.7
    stats.proseRatio = 0.4f; // Neither > 0.7
    stats.binaryRatio = 0.2f;

    auto state = SearchTuner::computeState(stats);
    CHECK(state == TuningState::MIXED);
}

TEST_CASE("SearchTuner: MIXED state boundary (code at 0.7)", "[unit][search_tuner][fsm]") {
    CorpusStats stats;
    stats.docCount = 1000;
    stats.codeRatio = 0.7f; // Exactly at threshold, not > 0.7
    stats.proseRatio = 0.2f;
    stats.binaryRatio = 0.1f;

    auto state = SearchTuner::computeState(stats);
    CHECK(state == TuningState::MIXED);
}

TEST_CASE("SearchTuner: state reason contains explanation", "[unit][search_tuner][fsm]") {
    CorpusStats stats;
    stats.docCount = 500;
    stats.codeRatio = 0.8f;
    stats.proseRatio = 0.15f;

    std::string reason;
    auto state = SearchTuner::computeState(stats, reason);

    CHECK(state == TuningState::SMALL_CODE);
    CHECK(reason.find("code_dominant") != std::string::npos);
    CHECK(reason.find("80%") != std::string::npos);
    CHECK(reason.find("small") != std::string::npos);
}

// =============================================================================
// SearchTuner class tests
// =============================================================================

TEST_CASE("SearchTuner: construction and state access", "[unit][search_tuner]") {
    CorpusStats stats;
    stats.docCount = 500;
    stats.codeRatio = 0.8f;
    stats.proseRatio = 0.15f;

    SearchTuner tuner(stats);

    CHECK(tuner.currentState() == TuningState::SMALL_CODE);
    CHECK(tuner.getRrfK() == 20);
    CHECK_FALSE(tuner.stateReason().empty());
}

TEST_CASE("SearchTuner: getParams returns correct params", "[unit][search_tuner]") {
    CorpusStats stats;
    stats.docCount = 5000;
    stats.codeRatio = 0.85f;

    SearchTuner tuner(stats);
    const auto& params = tuner.getParams();

    // Should be LARGE_CODE
    CHECK(params.rrfK == 60);
    CHECK(params.textWeight == Approx(0.45f));
    CHECK(params.vectorWeight == Approx(0.20f));
}

TEST_CASE("SearchTuner: getConfig returns valid SearchEngineConfig", "[unit][search_tuner]") {
    CorpusStats stats;
    stats.docCount = 500;
    stats.proseRatio = 0.85f;
    stats.pathDepthAvg = 3.0f;
    stats.tagCoverage = 0.3f;

    SearchTuner tuner(stats);
    auto config = tuner.getConfig();

    // Should be SMALL_PROSE
    CHECK(config.textWeight == Approx(0.50f));
    CHECK(config.vectorWeight == Approx(0.40f));
    CHECK(config.pathTreeWeight == Approx(0.00f));
    CHECK(config.kgWeight == Approx(0.00f));
    CHECK(config.corpusProfile == SearchEngineConfig::CorpusProfile::CUSTOM);
    CHECK(config.fusionStrategy == SearchEngineConfig::FusionStrategy::WEIGHTED_RECIPROCAL);
}

TEST_CASE("SearchTuner: toJson serialization", "[unit][search_tuner]") {
    CorpusStats stats;
    stats.docCount = 1000;
    stats.codeRatio = 0.4f;
    stats.proseRatio = 0.4f;
    stats.pathDepthAvg = 3.0f;
    stats.tagCoverage = 0.3f;
    stats.embeddingCoverage = 0.8f;
    stats.symbolDensity = 2.0f;

    SearchTuner tuner(stats);
    auto json = tuner.toJson();

    CHECK(json["state"] == "MIXED");
    CHECK_FALSE(json["reason"].get<std::string>().empty());
    CHECK(json["rrf_k"] == 45);
    CHECK(json["params"]["text_weight"].get<float>() == Approx(0.45f));
    CHECK(json["corpus"]["doc_count"] == 1000);
    CHECK(json["corpus"]["code_ratio"].get<float>() == Approx(0.4f));
    CHECK(json["corpus"]["prose_ratio"].get<float>() == Approx(0.4f));
}

// =============================================================================
// Edge cases and feature availability tests
// =============================================================================

TEST_CASE("SearchTuner: handles missing embeddings", "[unit][search_tuner][edge]") {
    CorpusStats stats;
    stats.docCount = 500;
    stats.codeRatio = 0.8f;
    stats.embeddingCoverage = 0.0f; // No embeddings

    std::string reason;
    auto state = SearchTuner::computeState(stats, reason);

    CHECK(state == TuningState::SMALL_CODE);
    CHECK(reason.find("no_embeddings") != std::string::npos);
}

TEST_CASE("SearchTuner: handles missing KG", "[unit][search_tuner][edge]") {
    CorpusStats stats;
    stats.docCount = 500;
    stats.codeRatio = 0.8f;
    stats.symbolDensity = 0.0f; // No KG/symbols

    std::string reason;
    auto state = SearchTuner::computeState(stats, reason);

    CHECK(state == TuningState::SMALL_CODE);
    CHECK(reason.find("no_kg") != std::string::npos);
}

TEST_CASE("SearchTuner: priority order - MINIMAL takes precedence", "[unit][search_tuner][edge]") {
    // Even with code-dominant ratio, minimal size wins
    CorpusStats stats;
    stats.docCount = 50;    // MINIMAL takes precedence
    stats.codeRatio = 0.9f; // Would be SMALL_CODE if not minimal

    auto state = SearchTuner::computeState(stats);
    CHECK(state == TuningState::MINIMAL);
}

TEST_CASE("SearchTuner: priority order - SCIENTIFIC before PROSE", "[unit][search_tuner][edge]") {
    // Scientific detection comes before prose classification
    CorpusStats stats;
    stats.docCount = 500;
    stats.proseRatio = 0.9f;
    stats.pathDepthAvg = 1.0f; // Flat paths
    stats.tagCoverage = 0.02f; // No tags

    auto state = SearchTuner::computeState(stats);
    CHECK(state == TuningState::SCIENTIFIC);
}

TEST_CASE("SearchTuner: size threshold boundary (1000 docs)", "[unit][search_tuner][edge]") {
    // 999 docs -> small, 1000 docs -> large
    CorpusStats small_stats;
    small_stats.docCount = 999;
    small_stats.codeRatio = 0.8f;

    CorpusStats large_stats;
    large_stats.docCount = 1000;
    large_stats.codeRatio = 0.8f;

    CHECK(SearchTuner::computeState(small_stats) == TuningState::SMALL_CODE);
    CHECK(SearchTuner::computeState(large_stats) == TuningState::LARGE_CODE);
}

// =============================================================================
// Weight sum validation (sanity check)
// =============================================================================

TEST_CASE("TunedParams: weights sum to approximately 1.0", "[unit][search_tuner][validation]") {
    std::vector<TuningState> states = {TuningState::SMALL_CODE,  TuningState::LARGE_CODE,
                                       TuningState::SMALL_PROSE, TuningState::LARGE_PROSE,
                                       TuningState::SCIENTIFIC,  TuningState::MIXED,
                                       TuningState::MINIMAL};

    for (auto state : states) {
        auto params = getTunedParams(state);
        float sum = params.textWeight + params.vectorWeight + params.pathTreeWeight +
                    params.kgWeight + params.tagWeight + params.metadataWeight;

        INFO("State: " << tuningStateToString(state));
        CHECK(sum == Approx(1.0f).margin(0.01f));
    }
}
