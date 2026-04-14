// SPDX-License-Identifier: GPL-3.0-or-later
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

#include <yams/search/search_engine_builder.h>
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
    CHECK(std::string(tuningStateToString(TuningState::MIXED_PRECISION)) == "MIXED_PRECISION");
    CHECK(std::string(tuningStateToString(TuningState::MINIMAL)) == "MINIMAL");
}

// =============================================================================
// TunedParams tests - verify parameter matrix values
// =============================================================================

TEST_CASE("TunedParams: SMALL_CODE parameters", "[unit][search_tuner][params]") {
    auto params = getTunedParams(TuningState::SMALL_CODE);

    CHECK(params.zoomLevel == SearchEngineConfig::NavigationZoomLevel::Street);
    CHECK(params.rrfK == 20);
    CHECK(params.weights.text.value == Approx(0.45f));
    CHECK(params.weights.vector.value == Approx(0.15f));
    CHECK(params.weights.entityVector.value == Approx(0.15f));
    CHECK(params.weights.pathTree.value == Approx(0.15f));
    CHECK(params.weights.kg.value == Approx(0.05f));
    CHECK(params.weights.tag.value == Approx(0.03f));
    CHECK(params.weights.metadata.value == Approx(0.02f));
}

TEST_CASE("TunedParams: LARGE_CODE parameters", "[unit][search_tuner][params]") {
    auto params = getTunedParams(TuningState::LARGE_CODE);

    CHECK(params.rrfK == 60);
    CHECK(params.weights.text.value == Approx(0.40f));
    CHECK(params.weights.vector.value == Approx(0.20f));
    CHECK(params.weights.entityVector.value == Approx(0.15f));
    CHECK(params.weights.pathTree.value == Approx(0.10f));
    CHECK(params.weights.kg.value == Approx(0.05f));
    CHECK(params.weights.tag.value == Approx(0.05f));
    CHECK(params.weights.metadata.value == Approx(0.05f));
}

TEST_CASE("TunedParams: SMALL_PROSE parameters", "[unit][search_tuner][params]") {
    auto params = getTunedParams(TuningState::SMALL_PROSE);

    CHECK(params.rrfK == 25);
    CHECK(params.weights.text.value == Approx(0.50f));
    CHECK(params.weights.vector.value == Approx(0.40f));
    CHECK(params.weights.entityVector.value == Approx(0.00f));
    CHECK(params.weights.pathTree.value == Approx(0.00f));
    CHECK(params.weights.kg.value == Approx(0.00f));
    CHECK(params.weights.tag.value == Approx(0.05f));
    CHECK(params.weights.metadata.value == Approx(0.05f));
    CHECK(params.lexicalFloorTopN == 12);
    CHECK(params.lexicalFloorBoost == Approx(0.20f));
    CHECK(params.enableLexicalTieBreak);
    CHECK(params.lexicalTieBreakEpsilon == Approx(0.010f));
    CHECK(params.fusionEvidenceRescueSlots == 1);
    CHECK(params.fusionEvidenceRescueMinScore == Approx(0.012f));
}

TEST_CASE("TunedParams: LARGE_PROSE parameters", "[unit][search_tuner][params]") {
    auto params = getTunedParams(TuningState::LARGE_PROSE);

    CHECK(params.rrfK == 60);
    CHECK(params.weights.text.value == Approx(0.40f));
    CHECK(params.weights.vector.value == Approx(0.45f));
    CHECK(params.weights.entityVector.value == Approx(0.00f));
    CHECK(params.weights.pathTree.value == Approx(0.05f));
    CHECK(params.weights.kg.value == Approx(0.00f));
    CHECK(params.weights.tag.value == Approx(0.05f));
    CHECK(params.weights.metadata.value == Approx(0.05f));
}

TEST_CASE("TunedParams: SCIENTIFIC parameters", "[unit][search_tuner][params]") {
    auto params = getTunedParams(TuningState::SCIENTIFIC);

    CHECK(params.zoomLevel == SearchEngineConfig::NavigationZoomLevel::Auto);
    CHECK(params.rrfK == 12);
    CHECK(params.weights.text.value == Approx(0.60f));
    CHECK(params.weights.vector.value == Approx(0.35f));
    CHECK(params.weights.entityVector.value == Approx(0.00f));
    CHECK(params.weights.pathTree.value == Approx(0.00f));
    CHECK(params.weights.kg.value == Approx(0.00f));
    CHECK(params.weights.tag.value == Approx(0.00f));
    CHECK(params.weights.metadata.value == Approx(0.05f));
}

TEST_CASE("TunedParams: MIXED parameters", "[unit][search_tuner][params]") {
    auto params = getTunedParams(TuningState::MIXED);

    CHECK(params.rrfK == 45);
    CHECK(params.weights.text.value == Approx(0.40f));
    CHECK(params.weights.vector.value == Approx(0.25f));
    CHECK(params.weights.entityVector.value == Approx(0.10f));
    CHECK(params.weights.pathTree.value == Approx(0.10f));
    CHECK(params.weights.kg.value == Approx(0.05f));
    CHECK(params.weights.tag.value == Approx(0.05f));
    CHECK(params.weights.metadata.value == Approx(0.05f));
}

TEST_CASE("TunedParams: MIXED_PRECISION parameters", "[unit][search_tuner][params]") {
    auto params = getTunedParams(TuningState::MIXED_PRECISION);

    CHECK(params.zoomLevel == SearchEngineConfig::NavigationZoomLevel::Neighborhood);
    CHECK(params.rrfK == 45);
    CHECK(params.weights.text.value == Approx(0.40f));
    CHECK(params.weights.vector.value == Approx(0.25f));
    CHECK(params.weights.entityVector.value == Approx(0.10f));
    CHECK(params.weights.pathTree.value == Approx(0.10f));
    CHECK(params.weights.kg.value == Approx(0.05f));
    CHECK(params.weights.tag.value == Approx(0.05f));
    CHECK(params.weights.metadata.value == Approx(0.05f));
    CHECK(params.vectorOnlyThreshold == Approx(0.94f));
    CHECK(params.vectorOnlyPenalty == Approx(0.70f));
    CHECK(params.vectorOnlyNearMissReserve == 2);
    CHECK(params.enablePathDedupInFusion);
    CHECK(params.lexicalFloorTopN == 12);
    CHECK(params.lexicalFloorBoost == Approx(0.20f));
    CHECK(params.enableLexicalTieBreak);
    CHECK(params.lexicalTieBreakEpsilon == Approx(0.010f));
    CHECK(params.semanticRescueSlots == 1);
    CHECK(params.semanticRescueMinVectorScore == Approx(0.0f));
    CHECK(params.fusionEvidenceRescueSlots == 1);
    CHECK(params.fusionEvidenceRescueMinScore == Approx(0.012f));
}

TEST_CASE("TunedParams: MINIMAL parameters", "[unit][search_tuner][params]") {
    auto params = getTunedParams(TuningState::MINIMAL);

    CHECK(params.rrfK == 15);
    CHECK(params.weights.text.value == Approx(0.55f));
    CHECK(params.weights.vector.value == Approx(0.30f));
    CHECK(params.weights.entityVector.value == Approx(0.05f));
    CHECK(params.weights.pathTree.value == Approx(0.05f));
    CHECK(params.weights.kg.value == Approx(0.00f));
    CHECK(params.weights.tag.value == Approx(0.03f));
    CHECK(params.weights.metadata.value == Approx(0.02f));
}

TEST_CASE("TunedParams: JSON serialization", "[unit][search_tuner][params]") {
    auto params = getTunedParams(TuningState::SMALL_CODE);
    auto json = params.toJson();

    CHECK(json["zoom_level"] == "STREET");
    CHECK(json["rrf_k"] == 20);
    CHECK(json["text_weight"].get<float>() == Approx(0.45f));
    CHECK(json["vector_weight"].get<float>() == Approx(0.15f));
    CHECK(json["entity_vector_weight"].get<float>() == Approx(0.15f));
    CHECK(json["path_tree_weight"].get<float>() == Approx(0.15f));
    CHECK(json["kg_weight"].get<float>() == Approx(0.05f));
    CHECK(json["tag_weight"].get<float>() == Approx(0.03f));
    CHECK(json["metadata_weight"].get<float>() == Approx(0.02f));

    // Provenance source fields
    CHECK(json["text_weight_source"] == "Profile");
    CHECK(json["vector_weight_source"] == "Profile");
    CHECK(json["kg_weight_source"] == "Profile");
    CHECK(json["similarity_threshold_source"] == "Default");
    CHECK(json["semantic_rescue_slots_source"] == "Default");
}

TEST_CASE("TunedParams: applyTo SearchEngineConfig", "[unit][search_tuner][params]") {
    auto params = getTunedParams(TuningState::LARGE_PROSE);

    SearchEngineConfig config;
    params.applyTo(config);

    CHECK(config.zoomLevel == SearchEngineConfig::NavigationZoomLevel::Neighborhood);
    CHECK(config.textWeight == Approx(0.40f));
    CHECK(config.vectorWeight == Approx(0.45f));
    CHECK(config.entityVectorWeight == Approx(0.00f));
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
    stats.pathDepthAvg = 3.0f;
    stats.pathRelativeDepthAvg = 3.0; // Not flat (>= 1.5 avoids SCIENTIFIC)
    stats.tagCoverage = 0.3f;         // Has tags (avoid SCIENTIFIC)

    auto state = SearchTuner::computeState(stats);
    CHECK(state == TuningState::SMALL_PROSE);
}

TEST_CASE("SearchTuner: LARGE_PROSE state", "[unit][search_tuner][fsm]") {
    CorpusStats stats;
    stats.docCount = 5000; // >= 1000
    stats.codeRatio = 0.1f;
    stats.proseRatio = 0.85f; // > 0.7 (prose dominant)
    stats.binaryRatio = 0.05f;
    stats.pathDepthAvg = 3.0f;
    stats.pathRelativeDepthAvg = 3.0; // Not flat (>= 1.5 avoids SCIENTIFIC)
    stats.tagCoverage = 0.3f;         // Has tags

    auto state = SearchTuner::computeState(stats);
    CHECK(state == TuningState::LARGE_PROSE);
}

TEST_CASE("SearchTuner: SCIENTIFIC state", "[unit][search_tuner][fsm]") {
    CorpusStats stats;
    stats.docCount = 2000; // >= 100
    stats.codeRatio = 0.05f;
    stats.proseRatio = 0.90f; // > 0.7 (prose dominant)
    stats.binaryRatio = 0.05f;
    stats.pathDepthAvg = 6.0f; // Deep absolute paths are allowed
    stats.tagCoverage = 0.02f; // No tags (< 0.1)
    stats.symbolDensity = 0.0f;

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

TEST_CASE("SearchTuner: MIXED_PRECISION state when mixed corpus has embeddings",
          "[unit][search_tuner][fsm]") {
    CorpusStats stats;
    stats.docCount = 1000;
    stats.codeRatio = 0.4f;
    stats.proseRatio = 0.4f;
    stats.binaryRatio = 0.2f;
    stats.embeddingCoverage = 0.65f;

    std::string reason;
    auto state = SearchTuner::computeState(stats, reason);
    CHECK(state == TuningState::MIXED_PRECISION);
    CHECK(reason.find("mixed_precision") != std::string::npos);
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
    stats.symbolDensity = 0.0f; // No KG: kgWeight is zeroed and remaining weights are normalized

    SearchTuner tuner(stats);
    const auto& params = tuner.getParams();

    // Should be LARGE_CODE
    CHECK(params.rrfK == 60);
    CHECK(stats.hasKnowledgeGraph() == false);
    CHECK(params.weights.kg.value == Approx(0.0f));
    CHECK(params.weights.text.value == Approx(0.42105263f));
    CHECK(params.weights.vector.value == Approx(0.21052632f));
}

TEST_CASE("SearchTuner: getConfig returns valid SearchEngineConfig", "[unit][search_tuner]") {
    CorpusStats stats;
    stats.docCount = 500;
    stats.proseRatio = 0.85f;
    stats.pathDepthAvg = 3.0f;
    stats.pathRelativeDepthAvg = 3.0; // Not flat (>= 1.5 avoids SCIENTIFIC)
    stats.tagCoverage = 0.3f;

    SearchTuner tuner(stats);
    auto config = tuner.getConfig();

    // Should be SMALL_PROSE
    CHECK(config.zoomLevel == SearchEngineConfig::NavigationZoomLevel::Neighborhood);
    CHECK(config.textWeight == Approx(0.50f));
    CHECK(config.vectorWeight == Approx(0.40f));
    CHECK(config.pathTreeWeight == Approx(0.00f));
    CHECK(config.kgWeight == Approx(0.00f));
    CHECK(config.corpusProfile == SearchEngineConfig::CorpusProfile::CUSTOM);
    CHECK(config.fusionStrategy == SearchEngineConfig::FusionStrategy::COMB_MNZ);
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

    CHECK(json["state"] == "MIXED_PRECISION");
    CHECK_FALSE(json["reason"].get<std::string>().empty());
    CHECK(json["rrf_k"] == 45);
    CHECK(json["params"]["zoom_level"] == "NEIGHBORHOOD");
    // When KG is present, graph-aware adjustments shift weights toward KG
    // (multiplicative scaling preserves profile ratios).
    CHECK(json["params"]["text_weight"].get<float>() == Approx(0.356f).epsilon(0.01));
    CHECK(json["params"]["text_weight_source"] == "Corpus");
    CHECK(json["params"]["kg_weight"].get<float>() == Approx(0.131f).epsilon(0.01));
    CHECK(json["params"]["kg_weight_source"] == "Corpus");
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

TEST_CASE("SearchTuner: seedRuntimeConfig preserves explicit graph overrides without KG",
          "[unit][search_tuner][edge]") {
    CorpusStats stats;
    stats.docCount = 500;
    stats.proseRatio = 0.90f;
    stats.pathDepthAvg = 6.0f;
    stats.tagCoverage = 0.02f;
    stats.symbolDensity = 0.0f;

    SearchTuner tuner(stats);

    SearchEngineConfig config = tuner.getConfig();
    config.enableGraphRerank = true;
    config.kgWeight = 0.04f;
    config.graphRerankTopN = 30;
    config.graphRerankWeight = 0.18f;
    config.graphRerankMaxBoost = 0.22f;
    config.graphRerankMinSignal = 0.01f;
    config.graphCommunityWeight = 0.10f;
    config.kgMaxResults = 60;
    config.graphScoringBudgetMs = 8;

    tuner.seedRuntimeConfig(config);
    const auto seeded = tuner.getConfig();

    CHECK_FALSE(stats.hasKnowledgeGraph());
    CHECK(seeded.enableGraphRerank);
    CHECK(seeded.kgWeight > 0.0f);
    CHECK(seeded.kgWeight == Approx(0.04f / 1.04f));
    CHECK(seeded.graphRerankTopN == 30);
    CHECK(seeded.graphRerankWeight == Approx(0.18f));
    CHECK(seeded.graphRerankMaxBoost == Approx(0.22f));
    CHECK(seeded.graphRerankMinSignal == Approx(0.01f));
    CHECK(seeded.graphCommunityWeight == Approx(0.10f));
    CHECK(seeded.kgMaxResults == 60);
    CHECK(seeded.graphScoringBudgetMs == 8);
}

TEST_CASE("SearchTuner: priority order - MINIMAL takes precedence", "[unit][search_tuner][edge]") {
    // Even with code-dominant ratio, minimal size wins
    CorpusStats stats;
    stats.docCount = 50;    // MINIMAL takes precedence
    stats.codeRatio = 0.9f; // Would be SMALL_CODE if not minimal

    auto state = SearchTuner::computeState(stats);
    CHECK(state == TuningState::MINIMAL);
}

TEST_CASE("SearchTuner: small scientific-like prose gets SCIENTIFIC",
          "[unit][search_tuner][edge]") {
    CorpusStats stats;
    stats.docCount = 500;
    stats.proseRatio = 0.9f;
    stats.pathDepthAvg = 6.0f; // Deep absolute paths, but relative depth defaults to 0 (flat)
    stats.tagCoverage = 0.02f; // No tags
    stats.symbolDensity = 0.0f;

    auto state = SearchTuner::computeState(stats);
    CHECK(state == TuningState::SCIENTIFIC);
}

TEST_CASE("SearchTuner: large scientific-like prose still uses SCIENTIFIC",
          "[unit][search_tuner][edge]") {
    CorpusStats stats;
    stats.docCount = 2000;
    stats.proseRatio = 0.9f;
    stats.pathDepthAvg = 6.0f;
    stats.tagCoverage = 0.02f;
    stats.symbolDensity = 0.0f;

    auto state = SearchTuner::computeState(stats);
    CHECK(state == TuningState::SCIENTIFIC);
}

TEST_CASE("SearchTuner: SCIENTIFIC falls back to prose when structured",
          "[unit][search_tuner][edge]") {
    CorpusStats stats;
    stats.docCount = 500;
    stats.codeRatio = 0.05f;
    stats.proseRatio = 0.90f;
    stats.binaryRatio = 0.05f;
    stats.pathDepthAvg = 6.0f;        // Deep paths
    stats.pathRelativeDepthAvg = 4.0; // Not flat (>= 1.5 avoids SCIENTIFIC)
    stats.tagCoverage = 0.25f;        // Structured corpus
    stats.symbolDensity = 0.2f;

    auto state = SearchTuner::computeState(stats);
    CHECK(state == TuningState::SMALL_PROSE);
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
    std::vector<TuningState> states = {TuningState::SMALL_CODE,      TuningState::LARGE_CODE,
                                       TuningState::SMALL_PROSE,     TuningState::LARGE_PROSE,
                                       TuningState::SCIENTIFIC,      TuningState::MIXED,
                                       TuningState::MIXED_PRECISION, TuningState::MINIMAL};

    for (auto state : states) {
        auto params = getTunedParams(state);
        float sum = params.weights.text.value + params.weights.vector.value +
                    params.weights.entityVector.value + params.weights.pathTree.value +
                    params.weights.kg.value + params.weights.tag.value +
                    params.weights.metadata.value;

        INFO("State: " << tuningStateToString(state));
        CHECK(sum == Approx(1.0f).margin(0.01f));
    }
}

TEST_CASE("SearchEngineBuilder: default options align with MIXED_PRECISION fallback",
          "[unit][search_tuner][builder]") {
    auto opts = SearchEngineBuilder::BuildOptions::makeDefault();
    auto expectedParams = getTunedParams(TuningState::MIXED_PRECISION);
    SearchEngineConfig expectedConfig;
    expectedParams.applyTo(expectedConfig);

    CHECK(opts.autoTune);
    CHECK(opts.config.enableParallelExecution);
    CHECK(opts.config.includeDebugInfo);
    CHECK(opts.config.maxResults == 100);
    CHECK(opts.config.corpusProfile == SearchEngineConfig::CorpusProfile::CUSTOM);

    CHECK(opts.config.textWeight == Approx(expectedConfig.textWeight));
    CHECK(opts.config.vectorWeight == Approx(expectedConfig.vectorWeight));
    CHECK(opts.config.entityVectorWeight == Approx(expectedConfig.entityVectorWeight));
    CHECK(opts.config.pathTreeWeight == Approx(expectedConfig.pathTreeWeight));
    CHECK(opts.config.kgWeight == Approx(expectedConfig.kgWeight));
    CHECK(opts.config.tagWeight == Approx(expectedConfig.tagWeight));
    CHECK(opts.config.metadataWeight == Approx(expectedConfig.metadataWeight));
    CHECK(opts.config.rrfK == Approx(expectedConfig.rrfK));
    CHECK(opts.config.fusionStrategy == expectedConfig.fusionStrategy);
    CHECK(opts.config.vectorOnlyThreshold == Approx(expectedConfig.vectorOnlyThreshold));
    CHECK(opts.config.vectorOnlyPenalty == Approx(expectedConfig.vectorOnlyPenalty));
    CHECK(opts.config.enablePathDedupInFusion == expectedConfig.enablePathDedupInFusion);
    CHECK(opts.config.lexicalFloorTopN == expectedConfig.lexicalFloorTopN);
    CHECK(opts.config.lexicalFloorBoost == Approx(expectedConfig.lexicalFloorBoost));
    CHECK(opts.config.enableLexicalTieBreak == expectedConfig.enableLexicalTieBreak);
    CHECK(opts.config.lexicalTieBreakEpsilon == Approx(expectedConfig.lexicalTieBreakEpsilon));
    CHECK(opts.config.semanticRescueSlots == expectedConfig.semanticRescueSlots);
}

TEST_CASE("SearchTuner: adaptive observation trims KG under latency pressure",
          "[unit][search_tuner][adaptive]") {
    CorpusStats stats;
    stats.docCount = 5000;
    stats.codeRatio = 0.4f;
    stats.proseRatio = 0.4f;
    stats.embeddingCoverage = 0.8f;
    stats.symbolDensity = 2.0f;

    SearchTuner tuner(stats);
    const auto before = tuner.getParams();

    SearchTuner::RuntimeTelemetry telemetry;
    telemetry.latencyMs = 120.0;
    telemetry.topWindow = 25;
    telemetry.stages["kg"] = {.enabled = true,
                              .attempted = true,
                              .contributed = true,
                              .skipped = false,
                              .durationMs = 55.0,
                              .rawHitCount = 20,
                              .uniqueDocCount = 12};
    telemetry.stages["graph_rerank"] = {.enabled = true,
                                        .attempted = true,
                                        .contributed = false,
                                        .skipped = true,
                                        .durationMs = 18.0,
                                        .rawHitCount = 0,
                                        .uniqueDocCount = 0};
    telemetry.fusionSources["kg"] = {.enabled = true,
                                     .contributedToFinal = false,
                                     .configuredWeight = before.weights.kg.value,
                                     .finalScoreMass = 0.02,
                                     .finalTopDocCount = 0,
                                     .rawHitCount = 20,
                                     .uniqueDocCount = 12};

    for (int i = 0; i < 9; ++i) {
        tuner.observe(telemetry);
    }

    const auto after = tuner.getParams();
    CHECK(after.kgMaxResults <= before.kgMaxResults);
    CHECK(after.graphScoringBudgetMs <= before.graphScoringBudgetMs);
    CHECK(after.graphRerankTopN <= before.graphRerankTopN);
    CHECK(after.weights.kg.value >= Approx(0.02f));

    const auto adaptive = tuner.adaptiveStateToJson();
    CHECK(adaptive["changed_last_observation"].get<bool>());
    CHECK(adaptive["last_decision"].get<std::string>().find("kg_latency_pressure") !=
          std::string::npos);
}

TEST_CASE("SearchTuner: adaptive observation keeps KG floor and cools flapping",
          "[unit][search_tuner][adaptive]") {
    CorpusStats stats;
    stats.docCount = 5000;
    stats.codeRatio = 0.4f;
    stats.proseRatio = 0.4f;
    stats.embeddingCoverage = 0.8f;
    stats.symbolDensity = 1.5f;

    SearchTuner tuner(stats);

    SearchTuner::RuntimeTelemetry badTelemetry;
    badTelemetry.latencyMs = 140.0;
    badTelemetry.topWindow = 25;
    badTelemetry.stages["kg"] = {.enabled = true,
                                 .attempted = true,
                                 .contributed = false,
                                 .skipped = false,
                                 .durationMs = 70.0,
                                 .rawHitCount = 10,
                                 .uniqueDocCount = 8};
    badTelemetry.stages["graph_rerank"] = {.enabled = true,
                                           .attempted = true,
                                           .contributed = false,
                                           .skipped = true,
                                           .durationMs = 12.0,
                                           .rawHitCount = 0,
                                           .uniqueDocCount = 0};
    badTelemetry.fusionSources["kg"] = {.enabled = true,
                                        .contributedToFinal = false,
                                        .configuredWeight = tuner.getParams().weights.kg.value,
                                        .finalScoreMass = 0.01,
                                        .finalTopDocCount = 0,
                                        .rawHitCount = 10,
                                        .uniqueDocCount = 8};

    for (int i = 0; i < 10; ++i) {
        tuner.observe(badTelemetry);
    }

    const auto pressured = tuner.getParams();
    CHECK(pressured.weights.kg.value >= Approx(0.02f));

    SearchTuner::RuntimeTelemetry goodTelemetry;
    goodTelemetry.latencyMs = 40.0;
    goodTelemetry.topWindow = 25;
    goodTelemetry.stages["kg"] = {.enabled = true,
                                  .attempted = true,
                                  .contributed = true,
                                  .skipped = false,
                                  .durationMs = 5.0,
                                  .rawHitCount = 12,
                                  .uniqueDocCount = 8};
    goodTelemetry.stages["graph_rerank"] = {.enabled = true,
                                            .attempted = true,
                                            .contributed = true,
                                            .skipped = false,
                                            .durationMs = 4.0,
                                            .rawHitCount = 0,
                                            .uniqueDocCount = 0};
    goodTelemetry.fusionSources["kg"] = {.enabled = true,
                                         .contributedToFinal = true,
                                         .configuredWeight = pressured.weights.kg.value,
                                         .finalScoreMass = 0.18,
                                         .finalTopDocCount = 4,
                                         .rawHitCount = 12,
                                         .uniqueDocCount = 8};

    tuner.observe(goodTelemetry);
    const auto cooled = tuner.adaptiveStateToJson();
    CHECK(cooled["last_decision"].get<std::string>().find("cooldown_active") != std::string::npos);
}

// =============================================================================
// Community layer blending tests (Phase 6)
// =============================================================================

#include <yams/search/tuning_pipeline.h>

TEST_CASE("seedTunedParamsFromConfig preserves explicit config fields",
          "[unit][search_tuner][policy]") {
    SearchEngineConfig config;
    config.zoomLevel = SearchEngineConfig::NavigationZoomLevel::Map;
    config.textWeight = 0.21f;
    config.vectorWeight = 0.22f;
    config.entityVectorWeight = 0.03f;
    config.pathTreeWeight = 0.04f;
    config.kgWeight = 0.15f;
    config.tagWeight = 0.05f;
    config.metadataWeight = 0.30f;
    config.similarityThreshold = 0.47f;
    config.vectorBoostFactor = 1.35f;
    config.rrfK = 17.0f;
    config.fusionStrategy = SearchEngineConfig::FusionStrategy::WEIGHTED_SUM;
    config.vectorOnlyThreshold = 0.81f;
    config.vectorOnlyPenalty = 0.66f;
    config.vectorOnlyNearMissReserve = 3;
    config.vectorOnlyNearMissSlack = 0.07f;
    config.vectorOnlyNearMissPenalty = 0.44f;
    config.enablePathDedupInFusion = true;
    config.lexicalFloorTopN = 9;
    config.lexicalFloorBoost = 0.23f;
    config.enableLexicalTieBreak = true;
    config.lexicalTieBreakEpsilon = 0.015f;
    config.semanticRescueSlots = 4;
    config.semanticRescueMinVectorScore = 0.73f;
    config.fusionEvidenceRescueSlots = 2;
    config.fusionEvidenceRescueMinScore = 0.021f;
    config.enableAdaptiveVectorFallback = true;
    config.adaptiveVectorSkipMinTier1Hits = 7;
    config.adaptiveVectorSkipRequireTextSignal = false;
    config.adaptiveVectorSkipMinTextHits = 5;
    config.adaptiveVectorSkipMinTopTextScore = 0.42f;
    config.enableSubPhraseRescoring = true;
    config.subPhraseScoringPenalty = 0.61f;
    config.rerankTopK = 11;
    config.rerankAnchoredMinRelativeScore = 0.29f;
    config.chunkAggregation = SearchEngineConfig::ChunkAggregation::SUM;
    config.enableGraphRerank = true;
    config.graphRerankTopN = 33;
    config.graphRerankWeight = 0.17f;
    config.graphRerankMaxBoost = 0.27f;
    config.graphRerankMinSignal = 0.013f;
    config.graphCommunityWeight = 0.19f;
    config.kgMaxResults = 77;
    config.graphScoringBudgetMs = 18;
    config.graphEnablePathEnumeration = true;
    config.enableGraphQueryExpansion = true;
    config.graphEntitySignalWeight = 0.31f;
    config.graphStructuralSignalWeight = 0.18f;
    config.graphCoverageSignalWeight = 0.22f;
    config.graphPathSignalWeight = 0.14f;
    config.graphCorroborationFloor = 0.39f;

    const auto params = seedTunedParamsFromConfig(config);
    SearchEngineConfig roundTrip;
    params.applyTo(roundTrip);

    CHECK(roundTrip.zoomLevel == config.zoomLevel);
    CHECK(roundTrip.textWeight == Approx(config.textWeight));
    CHECK(roundTrip.vectorWeight == Approx(config.vectorWeight));
    CHECK(roundTrip.entityVectorWeight == Approx(config.entityVectorWeight));
    CHECK(roundTrip.pathTreeWeight == Approx(config.pathTreeWeight));
    CHECK(roundTrip.kgWeight == Approx(config.kgWeight));
    CHECK(roundTrip.tagWeight == Approx(config.tagWeight));
    CHECK(roundTrip.metadataWeight == Approx(config.metadataWeight));
    CHECK(roundTrip.similarityThreshold == Approx(config.similarityThreshold));
    CHECK(roundTrip.vectorBoostFactor == Approx(config.vectorBoostFactor));
    CHECK(roundTrip.rrfK == Approx(config.rrfK));
    CHECK(roundTrip.fusionStrategy == config.fusionStrategy);
    CHECK(roundTrip.semanticRescueSlots == config.semanticRescueSlots);
    CHECK(roundTrip.semanticRescueMinVectorScore == Approx(config.semanticRescueMinVectorScore));
    CHECK(roundTrip.fusionEvidenceRescueSlots == config.fusionEvidenceRescueSlots);
    CHECK(roundTrip.fusionEvidenceRescueMinScore == Approx(config.fusionEvidenceRescueMinScore));
    CHECK(roundTrip.enableSubPhraseRescoring == config.enableSubPhraseRescoring);
    CHECK(roundTrip.subPhraseScoringPenalty == Approx(config.subPhraseScoringPenalty));
    CHECK(roundTrip.chunkAggregation == config.chunkAggregation);
    CHECK(roundTrip.graphEnablePathEnumeration == config.graphEnablePathEnumeration);
    CHECK(roundTrip.enableGraphQueryExpansion == config.enableGraphQueryExpansion);
    CHECK(roundTrip.graphEntitySignalWeight == Approx(config.graphEntitySignalWeight));
    CHECK(roundTrip.graphStructuralSignalWeight == Approx(config.graphStructuralSignalWeight));
    CHECK(roundTrip.graphCoverageSignalWeight == Approx(config.graphCoverageSignalWeight));
    CHECK(roundTrip.graphPathSignalWeight == Approx(config.graphPathSignalWeight));
    CHECK(roundTrip.graphCorroborationFloor == Approx(config.graphCorroborationFloor));
}

TEST_CASE("resolveQueryPolicy preserves no-tuner config fields unaffected by query layers",
          "[unit][search_tuner][policy]") {
    SearchEngineConfig config;
    config.zoomLevel = SearchEngineConfig::NavigationZoomLevel::Neighborhood;
    config.enableIntentAdaptiveWeighting = false;
    config.textWeight = 0.50f;
    config.vectorWeight = 0.30f;
    config.entityVectorWeight = 0.00f;
    config.pathTreeWeight = 0.00f;
    config.kgWeight = 0.10f;
    config.tagWeight = 0.05f;
    config.metadataWeight = 0.05f;
    config.semanticRescueMinVectorScore = 0.73f;
    config.fusionEvidenceRescueSlots = 2;
    config.fusionEvidenceRescueMinScore = 0.021f;
    config.enableSubPhraseRescoring = true;
    config.subPhraseScoringPenalty = 0.61f;
    config.chunkAggregation = SearchEngineConfig::ChunkAggregation::SUM;
    config.graphEnablePathEnumeration = true;
    config.enableGraphQueryExpansion = true;
    config.graphEntitySignalWeight = 0.31f;
    config.graphStructuralSignalWeight = 0.18f;
    config.graphCoverageSignalWeight = 0.22f;
    config.graphPathSignalWeight = 0.14f;
    config.graphCorroborationFloor = 0.39f;

    const auto resolution = resolveQueryPolicy(
        "documents and files", config, seedTunedParamsFromConfig(config), std::nullopt, false);

    CHECK(resolution.routeDecision.intent.label == QueryIntent::Prose);
    CHECK_FALSE(resolution.communityOverride.has_value());
    CHECK(resolution.config.zoomLevel == SearchEngineConfig::NavigationZoomLevel::Neighborhood);
    CHECK(resolution.config.semanticRescueMinVectorScore == Approx(0.73f));
    CHECK(resolution.config.fusionEvidenceRescueSlots == 2);
    CHECK(resolution.config.fusionEvidenceRescueMinScore == Approx(0.021f));
    CHECK(resolution.config.enableSubPhraseRescoring == true);
    CHECK(resolution.config.subPhraseScoringPenalty == Approx(0.61f));
    CHECK(resolution.config.chunkAggregation == SearchEngineConfig::ChunkAggregation::SUM);
    CHECK(resolution.config.graphEnablePathEnumeration == true);
    CHECK(resolution.config.enableGraphQueryExpansion == true);
    CHECK(resolution.config.graphEntitySignalWeight == Approx(0.31f));
    CHECK(resolution.config.graphStructuralSignalWeight == Approx(0.18f));
    CHECK(resolution.config.graphCoverageSignalWeight == Approx(0.22f));
    CHECK(resolution.config.graphPathSignalWeight == Approx(0.14f));
    CHECK(resolution.config.graphCorroborationFloor == Approx(0.39f));
}

TEST_CASE("applyCommunityLayer: MIXED_PRECISION → SCIENTIFIC blend",
          "[unit][search_tuner][community]") {
    auto params = getTunedParams(TuningState::MIXED_PRECISION);

    applyCommunityLayer(TuningState::SCIENTIFIC, TuningState::MIXED_PRECISION, params);

    // Weights: 60% toward SCIENTIFIC, 40% current (MIXED_PRECISION)
    // MIXED_PRECISION text=0.40, SCIENTIFIC text=0.60 → 0.40 + 0.60*(0.60-0.40) = 0.52
    CHECK(params.weights.text.value == Approx(0.52f).epsilon(0.01));
    CHECK(params.weights.text.source == TuningLayer::Community);
    // MIXED_PRECISION vector=0.25, SCIENTIFIC vector=0.35 → 0.25 + 0.60*(0.35-0.25) = 0.31
    CHECK(params.weights.vector.value == Approx(0.31f).epsilon(0.01));

    // semanticRescueSlots: lerp(1, 2, 0.60) = round(1.6) = 2
    CHECK(params.semanticRescueSlots.value == 2);
    CHECK(params.semanticRescueSlots.source == TuningLayer::Community);

    // semanticRescueMinVectorScore: lerp(0.0, 0.65, 0.60) = 0.39
    CHECK(params.semanticRescueMinVectorScore == Approx(0.39f).epsilon(0.01));

    // enableSubPhraseRescoring: false || true = true
    CHECK(params.enableSubPhraseRescoring == true);

    // fusionStrategy: adopted from SCIENTIFIC
    CHECK(params.fusionStrategy == SearchEngineConfig::FusionStrategy::WEIGHTED_RECIPROCAL);

    // chunkAggregation: adopted from SCIENTIFIC
    CHECK(params.chunkAggregation == SearchEngineConfig::ChunkAggregation::SUM);

    // rerankTopK: lerp(5, 12, 0.60) = round(9.2) = 9
    CHECK(params.rerankTopK == 9);

    // rerankAnchoredMinRelativeScore: lerp(0.0, 0.70, 0.60) = 0.42
    CHECK(params.rerankAnchoredMinRelativeScore == Approx(0.42f).epsilon(0.01));

    // similarityThreshold: lerp(0.65, 0.40, 0.60) = 0.50
    CHECK(params.similarityThreshold.value == Approx(0.50f).epsilon(0.01));
}

TEST_CASE("applyCommunityLayer: no-op when already in target state",
          "[unit][search_tuner][community]") {
    auto params = getTunedParams(TuningState::SCIENTIFIC);
    const auto before = params;

    applyCommunityLayer(TuningState::SCIENTIFIC, TuningState::SCIENTIFIC, params);

    // Nothing should change
    CHECK(params.weights.text.value == Approx(before.weights.text.value));
    CHECK(params.weights.vector.value == Approx(before.weights.vector.value));
    CHECK(params.fusionStrategy == before.fusionStrategy);
    CHECK(params.semanticRescueSlots.value == before.semanticRescueSlots.value);
}

TEST_CASE("applyCommunityLayer: env-pinned weight survives blend",
          "[unit][search_tuner][community]") {
    auto params = getTunedParams(TuningState::MIXED_PRECISION);

    // Pin vector weight via env override
    params.weights.vector.forceSet(0.35f, TuningLayer::Env);

    applyCommunityLayer(TuningState::SCIENTIFIC, TuningState::MIXED_PRECISION, params);

    // Pinned weight must not change
    CHECK(params.weights.vector.value == Approx(0.35f));
    CHECK(params.weights.vector.source == TuningLayer::Env);
    CHECK(params.weights.vector.pinned == true);

    // Other weights still blended
    CHECK(params.weights.text.source == TuningLayer::Community);
}

TEST_CASE("applyCommunityLayer: SCIENTIFIC → Code blend preserves sub-phrase rescoring",
          "[unit][search_tuner][community]") {
    auto params = getTunedParams(TuningState::SCIENTIFIC);
    CHECK(params.enableSubPhraseRescoring == true); // SCIENTIFIC enables it

    applyCommunityLayer(TuningState::SMALL_CODE, TuningState::SCIENTIFIC, params);

    // OR semantics: current=true || target=false → true (preserved)
    CHECK(params.enableSubPhraseRescoring == true);
}

TEST_CASE("applyCommunityLayer: nullopt is no-op", "[unit][search_tuner][community]") {
    auto params = getTunedParams(TuningState::MIXED);
    const auto before = params;

    applyCommunityLayer(std::nullopt, TuningState::MIXED, params);

    CHECK(params.weights.text.value == Approx(before.weights.text.value));
    CHECK(params.fusionStrategy == before.fusionStrategy);
}

TEST_CASE("lerpValue: float and integral types", "[unit][search_tuner][community]") {
    // Float lerp
    CHECK(lerpValue(0.0f, 1.0f, 0.6f) == Approx(0.6f));
    CHECK(lerpValue(10.0f, 20.0f, 0.5f) == Approx(15.0f));

    // Integral lerp (rounds to nearest)
    CHECK(lerpValue(size_t{1}, size_t{2}, 0.6f) == size_t{2});  // round(1.6) = 2
    CHECK(lerpValue(size_t{5}, size_t{12}, 0.6f) == size_t{9}); // round(9.2) = 9
    CHECK(lerpValue(size_t{0}, size_t{0}, 0.6f) == size_t{0});  // 0 stays 0

    // Int lerp
    CHECK(lerpValue(0, 10, 0.3f) == 3); // round(3.0) = 3
}

// =============================================================================
// Phase 7: Graph activation tests (validates downstream path works)
// =============================================================================

TEST_CASE("SearchTuner: graph features activate when symbolDensity > 0.1",
          "[unit][search_tuner][graph]") {
    CorpusStats stats;
    stats.docCount = 940;
    stats.proseRatio = 0.90f;
    stats.codeRatio = 0.05f;
    stats.binaryRatio = 0.05f;
    stats.symbolDensity = 0.25f; // hasKnowledgeGraph() = true
    stats.embeddingCoverage = 0.80f;

    SearchTuner tuner(stats);
    const auto& p = tuner.getParams();

    CHECK(p.enableGraphRerank == true);
    CHECK(p.weights.kg.value > 0.0f);
    CHECK(p.kgMaxResults > 0);
    CHECK(p.graphScoringBudgetMs > 0);
}

TEST_CASE("SearchTuner: graph features disabled when symbolDensity = 0",
          "[unit][search_tuner][graph]") {
    CorpusStats stats;
    stats.docCount = 940;
    stats.proseRatio = 0.90f;
    stats.codeRatio = 0.05f;
    stats.binaryRatio = 0.05f;
    stats.symbolDensity = 0.0f; // hasKnowledgeGraph() = false
    stats.embeddingCoverage = 0.80f;

    SearchTuner tuner(stats);
    const auto& p = tuner.getParams();

    CHECK(p.enableGraphRerank == false);
    CHECK(p.weights.kg.value == 0.0f);
    CHECK(p.kgMaxResults == 0);
}

TEST_CASE("SearchTuner: graph richness scales budget and results", "[unit][search_tuner][graph]") {
    // Low richness: symbolDensity=0.2 → graphRichness=(0.2-0.1)/1.5≈0.067
    CorpusStats low;
    low.docCount = 2000;
    low.codeRatio = 0.50f;
    low.proseRatio = 0.40f;
    low.symbolDensity = 0.2f;
    low.embeddingCoverage = 0.80f;

    // High richness: symbolDensity=1.0 → graphRichness=(1.0-0.1)/1.5=0.6
    CorpusStats high;
    high.docCount = 2000;
    high.codeRatio = 0.50f;
    high.proseRatio = 0.40f;
    high.symbolDensity = 1.0f;
    high.embeddingCoverage = 0.80f;

    SearchTuner lowTuner(low);
    SearchTuner highTuner(high);

    // Higher richness → more budget and results (these don't normalize away)
    CHECK(highTuner.getParams().graphScoringBudgetMs > lowTuner.getParams().graphScoringBudgetMs);
    CHECK(highTuner.getParams().kgMaxResults > lowTuner.getParams().kgMaxResults);
    CHECK(highTuner.getParams().graphRerankWeight > lowTuner.getParams().graphRerankWeight);
}

TEST_CASE("SearchTuner: overlay-backed stats damp graph-heavy tuning",
          "[unit][search_tuner][graph]") {
    CorpusStats stats;
    stats.docCount = 2000;
    stats.codeRatio = 0.50f;
    stats.proseRatio = 0.40f;
    stats.symbolDensity = 1.0f;
    stats.embeddingCoverage = 0.80f;
    stats.usedOnlineOverlay = true;
    stats.pathDepthMaxApproximate = true;

    SearchTuner tuner(stats);
    const auto& p = tuner.getParams();

    CHECK(p.weights.kg.value <= 0.10f + 1e-6f);
    CHECK(p.kgMaxResults <= size_t{48});
    CHECK(p.graphScoringBudgetMs <= 8);
    CHECK(p.graphRerankTopN <= size_t{24});
    CHECK(p.enableGraphQueryExpansion == false);
    CHECK(p.graphEnablePathEnumeration == false);
    CHECK(tuner.toJson()["corpus"]["used_online_overlay"].get<bool>() == true);
}

TEST_CASE("SearchTuner: adaptive runtime tuning stays steady on overlay-backed stats",
          "[unit][search_tuner][adaptive]") {
    CorpusStats stats;
    stats.docCount = 2000;
    stats.codeRatio = 0.50f;
    stats.proseRatio = 0.40f;
    stats.symbolDensity = 0.8f;
    stats.embeddingCoverage = 0.80f;
    stats.usedOnlineOverlay = true;

    SearchTuner tuner(stats);
    SearchTuner::RuntimeTelemetry telemetry;
    telemetry.latencyMs = 120.0;
    telemetry.zoomLevel = SearchEngineConfig::NavigationZoomLevel::Neighborhood;
    telemetry.topWindow = 20;
    telemetry.stages["kg"] = {
        .enabled = true, .durationMs = 60.0, .skipped = false, .contributed = true};
    telemetry.stages["graph_rerank"] = {
        .enabled = true, .durationMs = 20.0, .skipped = false, .contributed = true};
    telemetry.fusionSources["kg"] = {
        .enabled = true, .contributedToFinal = true, .finalScoreMass = 0.4, .finalTopDocCount = 5};

    const auto before = tuner.getParams();
    tuner.observe(telemetry);
    const auto after = tuner.getParams();

    CHECK(after.kgMaxResults == before.kgMaxResults);
    CHECK(after.graphScoringBudgetMs == before.graphScoringBudgetMs);
    CHECK(after.graphRerankTopN == before.graphRerankTopN);
    CHECK(tuner.adaptiveStateToJson()["last_decision"] == "steady_overlay_stats");
}

// =============================================================================
// Phase 8: SCIENTIFIC size guard removal tests
// =============================================================================

TEST_CASE("SearchTuner: 940-doc scientific corpus gets SCIENTIFIC state",
          "[unit][search_tuner][scientific]") {
    CorpusStats stats;
    stats.docCount = 940; // < 1000 (isSmall)
    stats.proseRatio = 0.90f;
    stats.codeRatio = 0.05f;
    stats.binaryRatio = 0.05f;
    stats.pathRelativeDepthAvg = 0.5; // < 1.5 (flat paths)
    stats.tagCoverage = 0.02f;
    stats.nativeSymbolDensity = 0.0f;
    stats.symbolDensity = 0.25f;
    stats.embeddingCoverage = 0.80f;

    auto state = SearchTuner::computeState(stats);
    CHECK(state == TuningState::SCIENTIFIC);

    // Verify SCIENTIFIC params activate
    SearchTuner tuner(stats);
    const auto& p = tuner.getParams();
    CHECK(p.semanticRescueSlots.value == 2);
    CHECK(p.enableSubPhraseRescoring == true);
    CHECK(p.fusionStrategy == SearchEngineConfig::FusionStrategy::WEIGHTED_RECIPROCAL);
}

TEST_CASE("SearchTuner: 50-doc scientific corpus still gets MINIMAL",
          "[unit][search_tuner][scientific]") {
    CorpusStats stats;
    stats.docCount = 50; // < 100 (isMinimal takes priority)
    stats.proseRatio = 0.90f;
    stats.pathRelativeDepthAvg = 0.5;
    stats.tagCoverage = 0.02f;
    stats.nativeSymbolDensity = 0.0f;

    auto state = SearchTuner::computeState(stats);
    CHECK(state == TuningState::MINIMAL);
}

// =============================================================================
// Phase 9: Graph feature activation tests
// =============================================================================

TEST_CASE("SearchTuner: path enumeration activates for rich KG",
          "[unit][search_tuner][graph_features]") {
    CorpusStats stats;
    stats.docCount = 2000;
    stats.proseRatio = 0.90f;
    stats.codeRatio = 0.05f;
    stats.symbolDensity = 0.8f; // graphRichness = (0.8-0.1)/1.5 ≈ 0.47 > 0.3
    stats.embeddingCoverage = 0.80f;
    stats.pathRelativeDepthAvg = 0.5;
    stats.tagCoverage = 0.02f;
    stats.nativeSymbolDensity = 0.0f;

    SearchTuner tuner(stats);
    const auto& p = tuner.getParams();
    CHECK(p.graphEnablePathEnumeration == true);
    CHECK(p.enableGraphQueryExpansion == true);
}

TEST_CASE("SearchTuner: path enumeration off for sparse KG",
          "[unit][search_tuner][graph_features]") {
    CorpusStats stats;
    stats.docCount = 2000;
    stats.proseRatio = 0.90f;
    stats.codeRatio = 0.05f;
    stats.symbolDensity = 0.2f; // graphRichness = (0.2-0.1)/1.5 ≈ 0.067 < 0.3
    stats.embeddingCoverage = 0.80f;
    stats.pathRelativeDepthAvg = 0.5;
    stats.tagCoverage = 0.02f;
    stats.nativeSymbolDensity = 0.0f;

    SearchTuner tuner(stats);
    const auto& p = tuner.getParams();
    CHECK(p.graphEnablePathEnumeration == false);
    CHECK(p.enableGraphQueryExpansion == false);
}

TEST_CASE("SearchTuner: path enumeration off when no KG", "[unit][search_tuner][graph_features]") {
    CorpusStats stats;
    stats.docCount = 2000;
    stats.proseRatio = 0.90f;
    stats.codeRatio = 0.05f;
    stats.symbolDensity = 0.0f; // no KG
    stats.embeddingCoverage = 0.80f;

    SearchTuner tuner(stats);
    const auto& p = tuner.getParams();
    CHECK(p.graphEnablePathEnumeration == false);
    CHECK(p.enableGraphQueryExpansion == false);
}

// =============================================================================
// Phase 10: Graph signal weight tuning tests
// =============================================================================

TEST_CASE("TunedParams: SCIENTIFIC profile has entity-heavy graph signals",
          "[unit][search_tuner][graph_signals]") {
    auto p = getTunedParams(TuningState::SCIENTIFIC);
    CHECK(p.graphEntitySignalWeight == Approx(0.50f));
    CHECK(p.graphStructuralSignalWeight == Approx(0.15f));
    CHECK(p.graphCoverageSignalWeight == Approx(0.15f));
    CHECK(p.graphPathSignalWeight == Approx(0.10f));
    CHECK(p.graphCorroborationFloor == Approx(0.25f));
}

TEST_CASE("TunedParams: SMALL_CODE profile has structure-heavy graph signals",
          "[unit][search_tuner][graph_signals]") {
    auto p = getTunedParams(TuningState::SMALL_CODE);
    CHECK(p.graphEntitySignalWeight == Approx(0.25f));
    CHECK(p.graphStructuralSignalWeight == Approx(0.35f));
    CHECK(p.graphCoverageSignalWeight == Approx(0.20f));
    CHECK(p.graphPathSignalWeight == Approx(0.15f));
    CHECK(p.graphCorroborationFloor == Approx(0.40f));
}

TEST_CASE("TunedParams: default profiles use balanced graph signals",
          "[unit][search_tuner][graph_signals]") {
    auto p = getTunedParams(TuningState::MIXED_PRECISION);
    CHECK(p.graphEntitySignalWeight == Approx(0.40f));
    CHECK(p.graphStructuralSignalWeight == Approx(0.20f));
    CHECK(p.graphCoverageSignalWeight == Approx(0.20f));
    CHECK(p.graphPathSignalWeight == Approx(0.10f));
    CHECK(p.graphCorroborationFloor == Approx(0.35f));
}

TEST_CASE("Community blend lerps graph signal weights", "[unit][search_tuner][graph_signals]") {
    auto params = getTunedParams(TuningState::MIXED_PRECISION);
    applyCommunityLayer(TuningState::SCIENTIFIC, TuningState::MIXED_PRECISION, params);
    // 60% toward SCIENTIFIC: 0.4*0.40 + 0.6*0.50 = 0.46
    CHECK(params.graphEntitySignalWeight == Approx(0.46f).margin(0.01f));
    // 60% toward SCIENTIFIC: 0.4*0.20 + 0.6*0.15 = 0.17
    CHECK(params.graphStructuralSignalWeight == Approx(0.17f).margin(0.01f));
    // 60% toward SCIENTIFIC: 0.4*0.35 + 0.6*0.25 = 0.29
    CHECK(params.graphCorroborationFloor == Approx(0.29f).margin(0.01f));
}

TEST_CASE("Graph signal weights propagate to SearchEngineConfig",
          "[unit][search_tuner][graph_signals]") {
    auto p = getTunedParams(TuningState::SCIENTIFIC);
    SearchEngineConfig config;
    p.applyTo(config);
    CHECK(config.graphEntitySignalWeight == Approx(0.50f));
    CHECK(config.graphStructuralSignalWeight == Approx(0.15f));
    CHECK(config.graphCorroborationFloor == Approx(0.25f));
}

TEST_CASE("TunedParams: toJson includes graph signal fields",
          "[unit][search_tuner][serialization]") {
    auto p = getTunedParams(TuningState::SCIENTIFIC);
    auto j = p.toJson();
    CHECK(j.at("graph_entity_signal_weight").get<float>() == Approx(0.50f));
    CHECK(j.at("graph_corroboration_floor").get<float>() == Approx(0.25f));
    CHECK(j.at("graph_enable_path_enumeration").get<bool>() == false);
    CHECK(j.at("enable_graph_query_expansion").get<bool>() == false);
    CHECK(j.at("graph_structural_signal_weight").get<float>() == Approx(0.15f));
    CHECK(j.at("graph_coverage_signal_weight").get<float>() == Approx(0.15f));
    CHECK(j.at("graph_path_signal_weight").get<float>() == Approx(0.10f));
}
