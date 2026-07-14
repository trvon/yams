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
    CHECK(yams::search::detail::testingVectorRawCandidateLimit(cfg, 10, true) == 30U);

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

    for (const auto& [hash, embedding] : std::vector<std::pair<std::string, std::vector<float>>>{
             {"a", {1.0F, 0.0F}}, {"b", {0.8F, 0.2F}}, {"c", {0.0F, 1.0F}}}) {
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
        nullptr, vectorDb, {1.0F, 0.0F}, cfg, 2, routed, yams::vector::CandidateFilterMode::Exact,
        &diagnostics);

    REQUIRE(result.has_value());
    CHECK(result.value().size() == 2U);
    CHECK(diagnostics.usedExactScan);
    CHECK_FALSE(diagnostics.usedAnn);
    CHECK(diagnostics.rowsVisited == 2U);
    CHECK(diagnostics.exactDistanceEvaluations == 2U);
}

TEST_CASE("Vector pipeline filters global hits by routed membership with zero-overlap fallback",
          "[search][vector][topology][narrowing][catch2]") {
    using yams::search::ComponentResult;
    std::vector<ComponentResult> global{{.documentHash = "distractor", .score = 0.95F, .rank = 0},
                                        {.documentHash = "routed-a", .score = 0.90F, .rank = 1},
                                        {.documentHash = "routed-b", .score = 0.80F, .rank = 2}};

    auto filtered = yams::search::detail::filterVectorResultsByAllowedDocuments(
        global, std::unordered_set<std::string>{"routed-a", "routed-b"});

    REQUIRE(filtered.applied);
    CHECK_FALSE(filtered.fellBackToGlobal);
    CHECK(filtered.matched == 2U);
    CHECK(filtered.removed == 1U);
    REQUIRE(filtered.results.size() == 2U);
    CHECK(filtered.results[0].documentHash == "routed-a");
    CHECK(filtered.results[0].rank == 0U);
    CHECK(filtered.results[0].debugInfo.at("topology_route_filter") == "1");
    CHECK(filtered.results[1].documentHash == "routed-b");
    CHECK(filtered.results[1].rank == 1U);

    auto fallback = yams::search::detail::filterVectorResultsByAllowedDocuments(
        std::move(global), std::unordered_set<std::string>{"route-miss"});
    CHECK_FALSE(fallback.applied);
    REQUIRE(fallback.fellBackToGlobal);
    CHECK(fallback.matched == 0U);
    CHECK(fallback.removed == 0U);
    REQUIRE(fallback.results.size() == 3U);
    CHECK(fallback.results[0].documentHash == "distractor");
}
