#include <catch2/catch_test_macros.hpp>

#include "src/search/search_vector_pipeline_internal.h"
#include "tests/common/test_helpers_catch2.h"
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
    yams::test::ScopedEnvVar enableVectors("YAMS_DISABLE_VECTORS", std::string("0"));
    yams::test::ScopedEnvVar enableSqliteVecInit("YAMS_SQLITE_VEC_SKIP_INIT", std::string("0"));

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

TEST_CASE("Vector pipeline candidate rescue preserves baseline and appends novel routed hits",
          "[search][vector][candidate-rescue][catch2]") {
    using yams::search::ComponentResult;
    std::vector<ComponentResult> baseline{
        {.documentHash = "baseline-a", .score = 0.90F, .rank = 0},
        {.documentHash = "shared", .score = 0.70F, .rank = 1},
    };
    std::vector<ComponentResult> expansion{
        {.documentHash = "rescued-hash",
         .filePath = "/corpus/rescued-doc.txt",
         .score = 0.95F,
         .rank = 0},
        {.documentHash = "shared", .score = 0.70F, .rank = 1},
    };

    auto merged = yams::search::detail::mergeVectorCandidateRescues(std::move(baseline),
                                                                    std::move(expansion));

    CHECK(merged.added == 1U);
    CHECK(merged.novelDocuments == 1U);
    CHECK(merged.evidenceRescues == 0U);
    CHECK(merged.duplicates == 1U);
    CHECK(merged.novelDocumentHashes == std::vector<std::string>{"rescued-hash"});
    CHECK(merged.novelDocumentIds == std::vector<std::string>{"rescued-doc"});
    CHECK(merged.evidenceRescueDocumentHashes.empty());
    CHECK(merged.evidenceRescueDocumentIds.empty());
    REQUIRE(merged.addedDocumentHashes.size() == 1U);
    CHECK(merged.addedDocumentHashes[0] == "rescued-hash");
    REQUIRE(merged.addedDocumentIds.size() == 1U);
    CHECK(merged.addedDocumentIds[0] == "rescued-doc");
    REQUIRE(merged.results.size() == 3U);
    CHECK(merged.results[0].documentHash == "rescued-hash");
    CHECK(merged.results[0].rank == 0U);
    CHECK(merged.results[0].debugInfo.at("candidate_rescue") == "1");
    CHECK(merged.results[1].documentHash == "baseline-a");
    CHECK(merged.results[1].rank == 1U);
    CHECK(merged.results[2].documentHash == "shared");
    CHECK(merged.results[2].rank == 2U);
}

TEST_CASE("Vector pipeline candidate rescue enriches an existing lexical candidate",
          "[search][vector][candidate-rescue][evidence][catch2]") {
    using yams::search::ComponentResult;
    std::vector<ComponentResult> baseline{
        {.documentHash = "global-vector", .score = 0.90F, .rank = 0},
    };
    std::vector<ComponentResult> expansion{
        {.documentHash = "buried-lexical",
         .filePath = "/corpus/buried-lexical.txt",
         .score = 0.80F,
         .rank = 0},
    };

    auto merged = yams::search::detail::mergeVectorCandidateRescues(
        std::move(baseline), std::move(expansion),
        std::unordered_set<std::string>{"buried-lexical"});

    CHECK(merged.added == 1U);
    CHECK(merged.novelDocuments == 0U);
    CHECK(merged.evidenceRescues == 1U);
    CHECK(merged.duplicates == 0U);
    CHECK(merged.novelDocumentHashes.empty());
    CHECK(merged.novelDocumentIds.empty());
    CHECK(merged.evidenceRescueDocumentHashes == std::vector<std::string>{"buried-lexical"});
    CHECK(merged.evidenceRescueDocumentIds == std::vector<std::string>{"buried-lexical"});
    REQUIRE(merged.results.size() == 2U);
    CHECK(merged.results[1].documentHash == "buried-lexical");
    CHECK(merged.results[1].debugInfo.at("candidate_rescue") == "1");
    CHECK(merged.results[1].debugInfo.at("candidate_rescue_kind") == "evidence");
}
