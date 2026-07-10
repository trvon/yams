#include <catch2/catch_test_macros.hpp>

#include "src/search/search_vector_pipeline_internal.h"
#include <yams/vector/vector_database.h>

using yams::search::SearchEngineConfig;

TEST_CASE("Vector pipeline over-fetches chunks for aggregating unique docs",
          "[search][vector][catch2]") {
    SearchEngineConfig cfg;
    cfg.chunkAggregation = SearchEngineConfig::ChunkAggregation::WEIGHTED_TOP_K_AVG;
    cfg.chunkAggregationTopK = 3;

    CHECK(yams::search::detail::testingVectorRawCandidateLimit(cfg, 10, false) == 30U);
    CHECK(yams::search::detail::testingVectorRawCandidateLimit(cfg, 10, true) == 10U);

    cfg.chunkAggregation = SearchEngineConfig::ChunkAggregation::MAX;
    CHECK(yams::search::detail::testingVectorRawCandidateLimit(cfg, 10, false) == 10U);
}

TEST_CASE("Vector pipeline propagates exact routed work diagnostics",
          "[search][vector][topology][catch2]") {
    yams::vector::VectorDatabaseConfig dbConfig;
    dbConfig.database_path = ":memory:";
    dbConfig.embedding_dim = 2;
    dbConfig.use_in_memory = true;
    auto vectorDb = std::make_shared<yams::vector::VectorDatabase>(dbConfig);
    REQUIRE(vectorDb->initialize());

    for (const auto& [hash, embedding] :
         std::vector<std::pair<std::string, std::vector<float>>>{{"a", {1.0F, 0.0F}},
                                                                 {"b", {0.8F, 0.2F}},
                                                                 {"c", {0.0F, 1.0F}}}) {
        yams::vector::VectorRecord record;
        record.chunk_id = "chunk-" + hash;
        record.document_hash = hash;
        record.embedding = embedding;
        record.level = yams::vector::EmbeddingLevel::DOCUMENT;
        REQUIRE(vectorDb->insertVector(record));
    }

    SearchEngineConfig cfg;
    cfg.similarityThreshold = -1.0F;
    cfg.chunkAggregation = SearchEngineConfig::ChunkAggregation::MAX;
    yams::vector::VectorSearchDiagnostics diagnostics;
    const std::unordered_set<std::string> routed{"a", "b"};

    const auto result = yams::search::detail::queryVectorIndexPipeline(
        nullptr, vectorDb, {1.0F, 0.0F}, cfg, 2, routed, &diagnostics);

    REQUIRE(result.has_value());
    CHECK(result.value().size() == 2U);
    CHECK(diagnostics.usedExactScan);
    CHECK_FALSE(diagnostics.usedAnn);
    CHECK(diagnostics.rowsVisited == 2U);
    CHECK(diagnostics.exactDistanceEvaluations == 2U);
}

TEST_CASE("Vector pipeline reuses precomputed routed scores only with full coverage",
          "[search][vector][topology][catch2]") {
    using yams::search::ComponentResult;
    const std::vector<ComponentResult> precomputed{
        {.documentHash = "b", .score = 0.7F, .source = ComponentResult::Source::Vector, .rank = 2},
        {.documentHash = "a", .score = 0.9F, .source = ComponentResult::Source::Vector, .rank = 1},
        {.documentHash = "a", .score = 0.8F, .source = ComponentResult::Source::Vector, .rank = 0}};

    const auto reused = yams::search::detail::reusePrecomputedVectorResults(
        precomputed, std::unordered_set<std::string>{"a", "b"}, 2);
    REQUIRE(reused.has_value());
    REQUIRE(reused->size() == 2U);
    CHECK(reused->at(0).documentHash == "a");
    CHECK(reused->at(0).score == 0.9F);
    CHECK(reused->at(0).rank == 0U);
    CHECK(reused->at(0).debugInfo.at("vector_score_reused") == "1");

    CHECK_FALSE(yams::search::detail::reusePrecomputedVectorResults(
                    precomputed, std::unordered_set<std::string>{"a", "missing"}, 2)
                    .has_value());
}
