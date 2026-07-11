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

TEST_CASE("Vector pipeline unions topology members into the global vector candidate stream",
          "[search][vector][topology][augmentation][catch2]") {
    using yams::search::ComponentResult;
    std::vector<ComponentResult> global{
        {.documentHash = "global", .score = 0.95F, .source = ComponentResult::Source::Vector},
        {.documentHash = "shared", .score = 0.70F, .source = ComponentResult::Source::Vector}};
    std::vector<ComponentResult> topology{
        {.documentHash = "routed", .score = 0.90F, .source = ComponentResult::Source::Vector},
        {.documentHash = "shared", .score = 0.80F, .source = ComponentResult::Source::Vector}};

    const auto merged =
        yams::search::detail::mergeVectorCandidateResults(std::move(global), std::move(topology));

    REQUIRE(merged.size() == 3U);
    CHECK(merged[0].documentHash == "global");
    CHECK(merged[1].documentHash == "routed");
    CHECK(merged[2].documentHash == "shared");
    CHECK_FALSE(merged[0].debugInfo.contains("topology_augmentation"));
    CHECK(merged[1].debugInfo.at("topology_augmentation") == "1");
    CHECK(merged[2].debugInfo.at("topology_augmentation") == "1");
    CHECK(merged[2].score == 0.80F);
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
